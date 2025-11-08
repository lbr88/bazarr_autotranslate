import os
import sys
import httpx
import queue
import signal
import asyncio
import logging
import threading
import time
from dotenv import load_dotenv
from typing import List, Optional
from unique_queue import UniqueQueue
from logging.handlers import TimedRotatingFileHandler
from class_types import Serie, Movie, SubtitleTranslate
from web_interface import init_web_interface, run_web_interface, WebLogHandler, add_processed_item, add_failed_item, get_failed_items, update_item_state

def get_env_or_default(env, default):
    val = os.getenv(env)
    return val if val is not None else default

def get_attr_or_key(obj, name):
    if hasattr(obj, name):
        return getattr(obj, name)
    elif isinstance(obj, dict) and name in obj:
        return obj[name]
    else:
        raise AttributeError(f"Missing attribute or key '{name}'")

# Get configuration and setup things
load_dotenv()
base_languages_env = os.getenv("BASE_LANGUAGES")
if base_languages_env is not None:
    base_languages = [lang.strip() for lang in base_languages_env.split(",")]
else:
    base_languages = []

to_languges_env = os.getenv("TO_LANGUAGES")
if to_languges_env is not None:
    to_languges = [lang.strip() for lang in to_languges_env.split(",")]
else:
    to_languges = []

translation_request_timeout = int(get_env_or_default("TRANSLATION_REQUEST_TIMEOUT", 15 * 60))
num_workers = int(get_env_or_default("NUM_WORKERS", 1))
interval_between_scans = int(get_env_or_default("INTERVAL_BETWEEN_SCANS", 5 * 60))
batch_size = int(get_env_or_default("BATCH_SIZE", 50))
dual_queue_mode = os.getenv("DUAL_QUEUE", "false").lower() in ("true", "1", "yes")
web_ui_enabled = os.getenv("WEB_UI", "true").lower() in ("true", "1", "yes")
web_ui_port = int(get_env_or_default("WEB_UI_PORT", 6700))
log_level = get_env_or_default("LOG_LEVEL", "INFO")
log_directory = get_env_or_default("LOG_DIRECTORY", "")
series_scan = bool(get_env_or_default("SERIES_SCAN", True))
movies_scan = bool(get_env_or_default("MOVIES_SCAN", True))
max_retries = int(get_env_or_default("MAX_RETRIES", 3))
min_translation_time = float(get_env_or_default("MIN_TRANSLATION_TIME", 10.0))  # Minimum expected translation time in seconds

key_fn = lambda x: f" {"s" if get_attr_or_key(x, "is_serie") else "m"} {get_attr_or_key(x, "video_id")}_{get_attr_or_key(x, "to_language")}"

# Create queues based on dual_queue_mode
if dual_queue_mode:
    series_queue = UniqueQueue(key_fn=key_fn)
    movies_queue = UniqueQueue(key_fn=key_fn)
    task_queue = None  # Not used in dual queue mode
else:
    task_queue = UniqueQueue(key_fn=key_fn)
    series_queue = None
    movies_queue = None

shutdown_event = asyncio.Event()
manual_scan_event = asyncio.Event()
manual_scan_series_event = asyncio.Event()
manual_scan_movies_event = asyncio.Event()
logger = logging.getLogger("bazarr_lingarr")

async def get_episodes_metadata(
    base_url: str,
    api_key: str,
    series_ids: Optional[List[int]] = None,
    episode_ids: Optional[List[int]] = None,
    batch_size: int = 50,
) -> List[Serie] | None:
    """
    Get metadata for episodes/series

    Args:
        base_url (str): Base URL of Bazarr API
        api_key (str): API key for authentication
        series_ids (list[int], optional): List of series IDs to get metadata for
        episode_ids (list[int], optional): List of episode IDs to get metadata for
        batch_size (int): Number of IDs to process per request (default: 50)
    """

    # Determine which list to batch
    ids_to_batch = episode_ids if episode_ids else series_ids
    id_param_key = "episodeid[]" if episode_ids else "seriesid[]"
    
    if not ids_to_batch:
        logger.debug("No episode/series IDs provided")
        return None
    
    logger.debug(f"Getting metadata for {len(ids_to_batch)} episodes/series in batches of {batch_size}")
    endpoint = f"{base_url}/api/episodes"
    headers = {"X-API-KEY": api_key}
    
    all_results = []
    total_batches = (len(ids_to_batch) + batch_size - 1) // batch_size
    
    try:
        async with httpx.AsyncClient() as client:
            # Process in batches to avoid URL length limits
            for i in range(0, len(ids_to_batch), batch_size):
                batch = ids_to_batch[i:i + batch_size]
                batch_num = i // batch_size + 1
                logger.debug(f"Fetching episodes metadata batch {batch_num}/{total_batches} ({len(batch)} items)")
                
                params = {id_param_key: batch}
                
                response = await client.get(endpoint, headers=headers, params=params)
                response.raise_for_status()
                json = response.json()["data"]
                
                logger.debug(f"Received {len(json)} episodes in batch {batch_num}/{total_batches}")
                
                # Parse each episode individually to handle errors gracefully
                for episode_data in json:
                    try:
                        episode = Serie.from_dict(episode_data)
                        all_results.append(episode)
                    except Exception as parse_error:
                        episode_title = episode_data.get('title', 'Unknown')
                        episode_id = episode_data.get('sonarrEpisodeId', 'Unknown')
                        logger.debug(f"Failed to parse episode '{episode_title}' (ID: {episode_id}): {type(parse_error).__name__}: {str(parse_error)}")
                        continue
            
            logger.debug(f"Successfully fetched metadata for {len(all_results)} episodes/series total")
            return all_results
    except Exception as e:
        logger.error(f"Error while getting metada: {e}", exc_info=True)
        return None

async def get_wanted_episodes(
    base_url: str,
    api_key: str,
    start: int = 0,
    length: int = -1,
) -> List[Serie] | None:
    """
    Get wanted subtitles for episodes

    Args:
        base_url (str): Base URL of Bazarr API
        api_key (str): API key for authentication
        start (int): Paging start integer (default: 0)
        length (int): Paging length integer (default: -1)
        episode_ids (list[int], optional): List of specific episode IDs to check
    """

    logger.debug(f"Getting wanted episodes")
    endpoint = f"{base_url}/api/episodes/wanted"
    headers = {"X-API-KEY": api_key}
    params = {"start": start, "length": length}

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(endpoint, headers=headers, params=params)
            response.raise_for_status()
            json = response.json()["data"]
            
            logger.debug(f"received: {json}")
            return [Serie.from_dict(obj) for obj in json]
    except Exception as e:
        logger.error(f"Error while getting wanted episodes: {e}")

async def get_movies_metadata(
    base_url: str,
    api_key: str,
    movie_ids: Optional[List[int]] = None,
    batch_size: int = 50,
) -> List[Movie] | None:
    """
    Get metadata for movies

    Args:
        base_url (str): Base URL of Bazarr API
        api_key (str): API key for authentication
        movie_ids (list[int], optional): List of movie IDs to get metadata for
        batch_size (int): Number of IDs to process per request (default: 50)
    """

    if not movie_ids:
        logger.debug("No movie IDs provided")
        return None
    
    logger.debug(f"Getting metadata for {len(movie_ids)} movies in batches of {batch_size}")
    endpoint = f"{base_url}/api/movies"
    headers = {"X-API-KEY": api_key}
    
    all_results = []
    total_batches = (len(movie_ids) + batch_size - 1) // batch_size

    try:
        async with httpx.AsyncClient() as client:
            # Process in batches to avoid URL length limits
            for i in range(0, len(movie_ids), batch_size):
                batch = movie_ids[i:i + batch_size]
                batch_num = i // batch_size + 1
                logger.debug(f"Fetching movies metadata batch {batch_num}/{total_batches} ({len(batch)} items)")
                
                params = {"radarrid[]": batch}
                
                response = await client.get(endpoint, headers=headers, params=params)
                response.raise_for_status()
                json = response.json()["data"]
                
                logger.debug(f"Received {len(json)} movies in batch {batch_num}/{total_batches}")
                
                # Parse each movie individually to handle errors gracefully
                for movie_data in json:
                    try:
                        movie = Movie.from_dict(movie_data)
                        all_results.append(movie)
                    except Exception as parse_error:
                        movie_title = movie_data.get('title', 'Unknown')
                        movie_id = movie_data.get('radarrId', 'Unknown')
                        logger.debug(f"Failed to parse movie '{movie_title}' (ID: {movie_id}): {type(parse_error).__name__}: {str(parse_error)}")
                        continue
            
            logger.debug(f"Successfully fetched metadata for {len(all_results)} movies total")
            return all_results
    except Exception as e:
        logger.error(f"Error while getting movies metada: {e}", exc_info=True)
        return None

async def get_wanted_movies(
    base_url: str,
    api_key: str,
    start: int = 0,
    length: int = -1,
) -> List[Movie] | None:
    """
    Get wanted subtitles for movies

    Args:
        base_url (str): Base URL of Bazarr API
        api_key (str): API key for authentication
        start (int): Paging start integer (default: 0)
        length (int): Paging length integer (default: -1)
    """

    logger.debug(f"Getting wanted movies")
    endpoint = f"{base_url}/api/movies/wanted"
    headers = {"X-API-KEY": api_key}
    params = {"start": start, "length": length}

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(endpoint, headers=headers, params=params)
            response.raise_for_status()
            json = response.json()["data"]
            
            logger.debug(f"received: {json}")
            return [Movie.from_dict(obj) for obj in json]
    except Exception as e:
        logger.error(f"Error while getting metada for movies: {e}")

async def find_base_language_subtitles_from_missing_sutitles(base_url, api_key, videos: List[Serie] | List[Movie], batch_size: int = 50) -> List[SubtitleTranslate] | None:
    # Making a video id to language map, useful later on
    video_id_language_map = {}
    for video in videos:
        # Get video id from correct property depending the video instance
        video_id = video.sonarr_episode_id if isinstance(video, Serie) else video.radarr_id
        for missing_sub in video.missing_subtitles:
            # Validate language code
            if not is_valid_language_code(missing_sub.code2):
                logger.warning(f"Invalid language code '{missing_sub.code2}' found in missing subtitles for video {video_id}. Skipping. This may be a Bazarr data issue.")
                continue
                
            # Check if the missing subtitle is in the list for language to be translated in
            if missing_sub.code2 in to_languges:
                # Check if that subtitle is already in the translation list
                check_dict = {"is_serie": isinstance(video, Serie), "video_id": video_id, "to_language": missing_sub.code2}
                
                # Check appropriate queue based on mode
                if dual_queue_mode:
                    check_queue = series_queue if isinstance(video, Serie) else movies_queue
                else:
                    check_queue = task_queue
                
                if check_queue.check(check_dict):
                    logger.debug(f"Skipping subtitle, already in translation queue, {missing_sub.to_dict()}")
                    continue

                video_id_language_map[video_id] = missing_sub.code2

    if len(video_id_language_map) == 0:
        logger.info("No missing subtitles found that is in list of languages to be translated")
        return

    metadata: List[Serie] | List[Movie] | None = None
    if isinstance(videos[0], Serie):
        metadata = await get_episodes_metadata(
            base_url, api_key, episode_ids=list(video_id_language_map.keys()), batch_size=batch_size
        )
    else:
        metadata = await get_movies_metadata(base_url, api_key, movie_ids=list(video_id_language_map.keys()), batch_size=batch_size)

    if metadata is None:
        logger.info("No metadata returned, couldn't find already existing subtitles")
        return
    
    video_id_to_video_map: dict[int, Serie | Movie] = {}
    for video in metadata:
        video_id = video.sonarr_episode_id if isinstance(video, Serie) else video.radarr_id
        video_id_to_video_map[video_id] = video
    
    # Check the metadata for already existing subtitles
    # Match the existing subtitles from base language list to the missing ones for translation
    subtitles_to_translate = []
    for video_id, language in video_id_language_map.items():
        # Get the video associated
        video = video_id_to_video_map.get(video_id)
        
        # Skip if video wasn't found in metadata (could be due to parsing error)
        if video is None:
            logger.debug(f"skipping video: {video_id} not found in metadata (may have failed to parse)")
            continue

        # Check if there is subtitles
        if video.subtitles is None:
            logger.debug(f"skipping video: {video_id} no current existing subtitles found")
            continue

        found = False
        for sub in video.subtitles:
            # Skip subtitles without a valid path
            if sub.path is None:
                continue
                
            if language == sub.code2:
                continue # Skip metadata for subtitle if it's in the same language to for the translation
                # I don't think this should happen but better safe than sorry

            # Validate the language code before processing
            if not is_valid_language_code(sub.code2):
                logger.warning(f"Invalid language code '{sub.code2}' found for subtitle '{sub.name}' on video {video_id}. Skipping. This may be a Bazarr data issue.")
                continue

            # If the subtitle is in the base language list, associate it with the language to translate in
            if sub.code2 in base_languages:
                # Get video title
                if isinstance(video, Serie):
                    # Build series title from available fields
                    series_name = video.series_title or video.title or f"Series {video.sonarr_series_id}"
                    episode_num = video.episode_number or "??"
                    episode_name = video.episode_title or "Unknown Episode"
                    video_title = f"{series_name} - {episode_num} - {episode_name}"
                else:
                    video_title = video.title if hasattr(video, 'title') and video.title else f"Movie {video.radarr_id}"
                
                subtitles_to_translate.append(SubtitleTranslate(sub, language, video_id, isinstance(video, Serie), video_title))
                found = True
                break

        if not found:
            logger.debug(f"No matching existing subtitle found for: {language} for video: {video_id}")
    
    if len(subtitles_to_translate) == 0:
        logger.info("No already existing subtitles matched with requested translation subs")
        return

    logger.debug(f"Matching subtitles: {[w.to_dict() for w in subtitles_to_translate]}")
    logger.info(f"Found {len(subtitles_to_translate)} matching subtitles to translate")
    return subtitles_to_translate

# Track all seen items across scans to identify new ones
seen_items = set()
seen_items_lock = threading.Lock()

# Track retry counts persistently (survives across scanner runs)
retry_counts = {}  # key: item_key, value: retry_count
retry_counts_lock = threading.Lock()

def is_valid_language_code(code: str) -> bool:
    """Validate that a language code is a proper 2-character ISO 639-1 code."""
    if not isinstance(code, str):
        return False
    # Must be exactly 2 characters and only letters
    if len(code) != 2:
        return False
    if not code.isalpha():
        return False
    return True

def queue_subtitles_for_translation(subtitles: List[SubtitleTranslate], priority: bool = False):
    """Queue subtitles for translation. If priority=True, new items go to front of queue."""
    new_count = 0
    existing_count = 0
    failed_skipped = 0
    in_queue_count = 0
    current_time = time.time()
    
    # Get current failed items if web UI is enabled
    failed_items = get_failed_items() if web_ui_enabled else {}
    
    for sub in subtitles:
        # Set queued timestamp
        sub.queued_at = current_time
        
        # Create a unique key for this subtitle
        item_key = f"{sub.video_id}_{sub.to_language}_{sub.base_subtitle.code2}"
        
        # Restore retry count from persistent storage
        with retry_counts_lock:
            sub.retry_count = retry_counts.get(item_key, 0)
        
        # Skip if item is in failed state (awaiting manual retry)
        if item_key in failed_items:
            failed_skipped += 1
            logger.debug(f"Skipping failed item awaiting manual retry: {sub.base_subtitle.path}")
            continue
        
        # Check if item is already in the appropriate queue
        check_dict = {"is_serie": sub.is_serie, "video_id": sub.video_id, "to_language": sub.to_language}
        if dual_queue_mode:
            check_queue = series_queue if sub.is_serie else movies_queue
        else:
            check_queue = task_queue
        
        if check_queue.check(check_dict):
            in_queue_count += 1
            logger.debug(f"Skipping item already in queue: {sub.base_subtitle.path}")
            continue
        
        with seen_items_lock:
            is_new = item_key not in seen_items
            if is_new:
                seen_items.add(item_key)
        
        # Add to appropriate queue based on mode
        use_priority = priority and is_new
        
        if dual_queue_mode:
            # Use separate queues for series and movies
            target_queue = series_queue if sub.is_serie else movies_queue
            target_queue.put(sub, priority=use_priority)
        else:
            # Use single queue
            task_queue.put(sub, priority=use_priority)
        
        # Track item as queued in web UI
        if web_ui_enabled:
            update_item_state(item_key, 'queued', {
                'title': sub.video_title,
                'video_id': sub.video_id,
                'is_serie': sub.is_serie,
                'from_language': sub.base_subtitle.code2,
                'to_language': sub.to_language,
                'retry_count': sub.retry_count
            })
        
        if is_new:
            new_count += 1
            if use_priority:
                logger.debug(f"Queued (PRIORITY): {sub.base_subtitle.path} to be translated to: {sub.to_language}")
            else:
                logger.debug(f"Queued: {sub.base_subtitle.path} to be translated to: {sub.to_language}")
        else:
            existing_count += 1
    
    if new_count > 0:
        logger.info(f"Added {new_count} new items to translation queue" + (" with priority" if priority else ""))
    if existing_count > 0:
        logger.debug(f"Skipped {existing_count} items already in queue")
    if failed_skipped > 0:
        logger.info(f"Skipped {failed_skipped} failed items awaiting manual retry")
    if in_queue_count > 0:
        logger.debug(f"Skipped {in_queue_count} items currently being processed")

def translation_worker(worker_id, base_url, api_key, queue_type="combined"):
    """Worker thread that processes translation requests from queue.
    
    Args:
        queue_type: 'combined', 'series', or 'movies'
    """
    endpoint = f"{base_url}/api/subtitles"
    headers = {"X-API-KEY": api_key}
    
    # Determine which queue to use
    if dual_queue_mode:
        if queue_type == "series":
            work_queue = series_queue
            worker_label = f"Series Worker {worker_id}"
        else:  # movies
            work_queue = movies_queue
            worker_label = f"Movies Worker {worker_id}"
    else:
        work_queue = task_queue
        worker_label = f"Worker {worker_id}"
    
    with httpx.Client(timeout=translation_request_timeout) as client:
        while True:
            sub: SubtitleTranslate | None = None
            try:
                sub = work_queue.get()
                if sub is None:
                    continue
                
                # Mark start time
                sub.started_at = time.time()
                queue_time = sub.started_at - sub.queued_at if sub.queued_at > 0 else 0
                
                # Track item as processing
                item_key = f"{sub.video_id}_{sub.to_language}_{sub.base_subtitle.code2}"
                if web_ui_enabled:
                    logger.debug(f"[{worker_label}] Updating item to 'processing' state: {item_key}")
                    update_item_state(item_key, 'processing', {
                        'title': sub.video_title,
                        'video_id': sub.video_id,
                        'is_serie': sub.is_serie,
                        'from_language': sub.base_subtitle.code2,
                        'to_language': sub.to_language,
                        'retry_count': sub.retry_count,
                        'queue_time': queue_time
                    })
                
                # Check if subtitle filename contains language code
                filename = sub.base_subtitle.path
                has_lang_code = f".{sub.base_subtitle.code2}." in filename.lower() or filename.lower().endswith(f".{sub.base_subtitle.code2}.srt")
                if not has_lang_code:
                    logger.warning(f"[{worker_label}] Subtitle file missing language code in filename: {filename} (expected .{sub.base_subtitle.code2}. - this may cause Lingarr to fail)")
                
                logger.info(f"[{worker_label}] Translating: {sub.base_subtitle.path} ({sub.base_subtitle.code2} → {sub.to_language}) (queued for {queue_time:.1f}s)")

                params = {
                    "action": "translate",
                    "language": sub.to_language,
                    "path": sub.base_subtitle.path,
                    "type": "episode" if sub.is_serie else "movie",
                    "id": sub.video_id,
                    "forced": sub.base_subtitle.forced,
                    "hi": sub.base_subtitle.hi,
                    "original_format": True,
                }

                response = client.patch(endpoint, headers=headers, params=params)
                response.raise_for_status()
                
                # Mark completion time
                sub.completed_at = time.time()
                translation_time = sub.completed_at - sub.started_at
                total_time = sub.completed_at - sub.queued_at if sub.queued_at > 0 else translation_time
                
                # Check if translation completed suspiciously fast (likely a failure)
                is_failed = False
                item_key = f"{sub.video_id}_{sub.to_language}_{sub.base_subtitle.code2}"
                
                if translation_time < min_translation_time:
                    if sub.retry_count < max_retries:
                        sub.retry_count += 1
                        
                        # Update persistent retry count
                        with retry_counts_lock:
                            retry_counts[item_key] = sub.retry_count
                        
                        logger.warning(f"[{worker_label}] Translation completed too quickly ({translation_time:.1f}s < {min_translation_time}s) - likely failed. Retry {sub.retry_count}/{max_retries}")
                        
                        # Update item state to retrying
                        if web_ui_enabled:
                            logger.debug(f"[{worker_label}] Updating item to retrying state - Title: {sub.video_title}, Retry: {sub.retry_count}/{max_retries}")
                            update_item_state(item_key, 'retrying', {
                                'title': sub.video_title,
                                'video_id': sub.video_id,
                                'is_serie': sub.is_serie,
                                'from_language': sub.base_subtitle.code2,
                                'to_language': sub.to_language,
                                'queue_time': queue_time,
                                'translation_time': translation_time,
                                'total_time': total_time,
                                'retry_count': sub.retry_count
                            })
                        
                        # Re-queue the item with priority
                        if dual_queue_mode:
                            target_queue = series_queue if sub.is_serie else movies_queue
                        else:
                            target_queue = task_queue
                        
                        # Reset timestamps for retry
                        sub.queued_at = time.time()
                        sub.started_at = 0.0
                        sub.completed_at = 0.0
                        
                        target_queue.put(sub, priority=True)
                        work_queue.done(sub)
                        continue  # Skip the rest - don't add completed item since we're retrying
                    else:
                        is_failed = True
                        logger.error(f"[{worker_label}] Translation failed after {max_retries} retries - giving up on {sub.base_subtitle.path}")
                        
                        # Remove from retry counts (permanently failed)
                        with retry_counts_lock:
                            retry_counts.pop(item_key, None)
                        
                        # Remove from seen_items so it won't be blocked from future manual retry
                        with seen_items_lock:
                            seen_items.discard(item_key)
                        
                        # Store failed item for manual retry via web interface
                        if web_ui_enabled:
                            add_failed_item(item_key, sub)
                            logger.debug(f"[{worker_label}] Stored failed item with key: {item_key}")
                else:
                    # Success - clear retry count
                    with retry_counts_lock:
                        retry_counts.pop(item_key, None)

                # Only add to final processed list if we're done (not retrying)
                logger.info(f"[{worker_label}] Translation finished (translation: {translation_time:.1f}s, total: {total_time:.1f}s){' - FAILED' if is_failed else ''}")
                
                # Update item state to final result
                if web_ui_enabled:
                    item_key = f"{sub.video_id}_{sub.to_language}_{sub.base_subtitle.code2}"
                    logger.debug(f"[{worker_label}] Updating item to final state - Title: {sub.video_title}, Failed: {is_failed}")
                    update_item_state(item_key, 'failed' if is_failed else 'completed', {
                        'title': sub.video_title,
                        'video_id': sub.video_id,
                        'is_serie': sub.is_serie,
                        'from_language': sub.base_subtitle.code2,
                        'to_language': sub.to_language,
                        'queue_time': queue_time,
                        'translation_time': translation_time,
                        'total_time': total_time,
                        'retry_count': sub.retry_count
                    })
                    
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"[{worker_label}] Error while translating {sub.base_subtitle.path if sub else 'unknown'} ({sub.base_subtitle.code2 if sub else '?'} → {sub.to_language if sub else '?'}): {e}")
                if sub:
                    logger.error(f"[{worker_label}] Failed subtitle details - Video ID: {sub.video_id}, Path: {sub.base_subtitle.path}")
            
            work_queue.done(sub)

async def scan_series(base_url, api_key):
    """Scan for episodes and return subtitles to translate."""
    logger.info("Scanning for episodes")
    series = await get_wanted_episodes(base_url, api_key)
    if series is None or len(series) == 0:
        logger.info("Found no missing subtitles for episodes")
        return None
    
    logger.info(f"Found {len(series)} missing subtitles for episodes")
    subtitles_to_translate = await find_base_language_subtitles_from_missing_sutitles(base_url, api_key, series, batch_size)
    return subtitles_to_translate

async def scan_movies(base_url, api_key):
    """Scan for movies and return subtitles to translate."""
    logger.info("Scanning for movies")
    movies = await get_wanted_movies(base_url, api_key)
    if movies is None or len(movies) == 0:
        logger.info("Found no missing subtitles for movies")
        return None
    
    logger.info(f"Found {len(movies)} missing subtitles for movies")
    subtitles_to_translate = await find_base_language_subtitles_from_missing_sutitles(base_url, api_key, movies, batch_size)
    return subtitles_to_translate

async def scanner_task(base_url, api_key):
    """Continuously scan for new items and add them to queue with priority."""
    first_scan = True
    
    while not shutdown_event.is_set():
        try:
            # Determine what to scan based on triggered events
            scan_series_now = series_scan
            scan_movies_now = movies_scan
            
            all_subtitles = []
            
            # In dual queue mode, scan both simultaneously
            if dual_queue_mode and scan_series_now and scan_movies_now:
                results = await asyncio.gather(
                    scan_series(base_url, api_key),
                    scan_movies(base_url, api_key),
                    return_exceptions=True
                )
                
                series_subs = results[0] if not isinstance(results[0], Exception) else None
                movies_subs = results[1] if not isinstance(results[1], Exception) else None
                
                if series_subs:
                    all_subtitles.extend(series_subs)
                if movies_subs:
                    all_subtitles.extend(movies_subs)
            else:
                # Sequential scanning for single queue mode or when only one type is enabled
                if scan_series_now:
                    series_subs = await scan_series(base_url, api_key)
                    if series_subs:
                        all_subtitles.extend(series_subs)
                
                if scan_movies_now:
                    movies_subs = await scan_movies(base_url, api_key)
                    if movies_subs:
                        all_subtitles.extend(movies_subs)
            
            if all_subtitles:
                # First scan: add all to queue normally
                # Subsequent scans: add new items with priority
                queue_subtitles_for_translation(all_subtitles, priority=not first_scan)
            
            # Always show queue status after each scan
            if dual_queue_mode:
                series_size = series_queue.qsize()
                movies_size = movies_queue.qsize()
                logger.info(f"Queue sizes - Series: {series_size}, Movies: {movies_size}")
                
                # Show next items from both queues
                if series_size > 0:
                    next_series = series_queue.peek(3)
                    logger.info(f"Next {len(next_series)} series in queue:")
                    for idx, item in enumerate(next_series, 1):
                        logger.info(f"  {idx}. {item.video_title} - {item.base_subtitle.code2} → {item.to_language}")
                
                if movies_size > 0:
                    next_movies = movies_queue.peek(3)
                    logger.info(f"Next {len(next_movies)} movies in queue:")
                    for idx, item in enumerate(next_movies, 1):
                        logger.info(f"  {idx}. {item.video_title} - {item.base_subtitle.code2} → {item.to_language}")
            else:
                queue_size = task_queue.qsize()
                logger.info(f"Queue size: {queue_size} items pending")
                
                # Show next 5 items in queue
                next_items = task_queue.peek(5)
                if next_items:
                    logger.info(f"Next {len(next_items)} items in queue:")
                    for idx, item in enumerate(next_items, 1):
                        logger.info(f"  {idx}. {item.video_title} - {item.base_subtitle.code2} → {item.to_language}")
            
            first_scan = False
            
        except Exception as e:
            logger.error(f"Error in scanner task: {e}", exc_info=True)
        
        # Wait for either the interval or manual scan trigger
        try:
            # Check for specific queue scans or general scan
            done, pending = await asyncio.wait(
                [
                    asyncio.create_task(manual_scan_event.wait()),
                    asyncio.create_task(manual_scan_series_event.wait()),
                    asyncio.create_task(manual_scan_movies_event.wait())
                ],
                timeout=interval_between_scans,
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Cancel pending tasks
            for task in pending:
                task.cancel()
            
            # Check which event was triggered
            if manual_scan_event.is_set():
                manual_scan_event.clear()
                logger.info("Manual scan triggered from web UI (all types)")
            if manual_scan_series_event.is_set():
                manual_scan_series_event.clear()
                logger.info("Manual scan triggered from web UI (series only)")
            if manual_scan_movies_event.is_set():
                manual_scan_movies_event.clear()
                logger.info("Manual scan triggered from web UI (movies only)")
                
        except asyncio.TimeoutError:
            pass  # Normal timeout, continue with next scan

def trigger_manual_scan():
    """Trigger a manual scan from web UI."""
    manual_scan_event.set()

def trigger_manual_scan_series():
    """Trigger a manual scan for series only from web UI."""
    manual_scan_series_event.set()

def trigger_manual_scan_movies():
    """Trigger a manual scan for movies only from web UI."""
    manual_scan_movies_event.set()

async def main(base_url, api_key):
    # Start web interface if enabled
    if web_ui_enabled:
        logger.info(f"Starting web interface on port {web_ui_port}")
        init_web_interface(
            dual_queue_mode, 
            task_queue, 
            series_queue, 
            movies_queue, 
            trigger_manual_scan,
            trigger_manual_scan_series,
            trigger_manual_scan_movies,
            seen_items,
            seen_items_lock
        )
        web_thread = threading.Thread(
            target=run_web_interface, 
            args=('0.0.0.0', web_ui_port), 
            daemon=True
        )
        web_thread.start()
        logger.info(f"Web interface available at http://localhost:{web_ui_port}")
    
    # Start translation worker threads
    if dual_queue_mode:
        # Split workers between series and movies (at least 1 for each if num_workers >= 2)
        series_workers = max(1, num_workers // 2)
        movies_workers = max(1, num_workers - series_workers)
        
        logger.info(f"Starting {series_workers} series worker(s) and {movies_workers} movies worker(s) (dual queue mode)")
        
        for i in range(series_workers):
            threading.Thread(target=translation_worker, args=(i, base_url, api_key, "series"), daemon=True).start()
        
        for i in range(movies_workers):
            threading.Thread(target=translation_worker, args=(i, base_url, api_key, "movies"), daemon=True).start()
    else:
        logger.info(f"Starting {num_workers} translation worker(s) (single queue mode)")
        for i in range(num_workers):
            threading.Thread(target=translation_worker, args=(i, base_url, api_key, "combined"), daemon=True).start()
    
    # Run scanner task concurrently (it runs independently and continuously)
    logger.info(f"Starting scanner task (interval: {interval_between_scans}s)")
    await scanner_task(base_url, api_key)

def handle_shutdown():
    logger.info("Received exit signal")
    sys.exit(1)



if __name__ == "__main__":
    # Do verification on arguments
    base_url = os.getenv("BAZARR_BASE_URL")
    api_key = os.getenv("BAZARR_API_KEY")

    if base_url is None:
        print("BAZARR_BASE_URL is missing")
        sys.exit(1)

    if api_key is None:
        print("BAZARR_API_KEY is missing")
        sys.exit(1)

    if len(base_languages) == 0:
        print("Missing BASE_LANGUAGES")
        sys.exit(1)
    
    wrong_languages = [lang for lang in base_languages if len(lang) > 2 or len(lang) < 2]
    if len(wrong_languages) > 0:
        print(f"Wrong languages given in BASE_LANGUAGES, wrong ones: {wrong_languages}, expected to be 2 characters long (code2)")
        sys.exit(1)

    if len(to_languges) == 0:
        print("Missing TO_LANGUAGES")
        sys.exit(1)

    wrong_languages = [lang for lang in to_languges if len(lang) > 2 or len(lang) < 2]
    if len(wrong_languages) > 0:
        print(f"Wrong languages given in TO_LANGUAGES, wrong ones: {wrong_languages}, expected to be 2 characters long (code2)")
        sys.exit(1)

    if not series_scan and not movies_scan:
        print("Both series and movies scan are disabled, nothing will be done")
        sys.exit(1) 

    # Setup logger
    logger.propagate = False
    
    # Use file logging if LOG_DIRECTORY is specified, otherwise log to stdout
    if log_directory:
        trailing_slash = "/" if not log_directory.endswith("/") else ""
        os.makedirs(log_directory, exist_ok=True)
        handler = TimedRotatingFileHandler(
            f"{log_directory}{trailing_slash}bazarr_lingarr_autotranslate.log", when="midnight", interval=1, backupCount=4
        )
    else:
        handler = logging.StreamHandler(sys.stdout)
    
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    # Add web log handler if web UI is enabled
    if web_ui_enabled:
        web_handler = logging.StreamHandler(WebLogHandler())
        web_handler.setFormatter(formatter)
        logger.addHandler(web_handler)

    match log_level.lower():
        case "info":
            logger.setLevel(logging.INFO)
        case "debug":
            logger.setLevel(logging.DEBUG)
            logger.debug("Configuration: --------------------")
            logger.debug(f"bazarr_base_url: {base_url}")
            logger.debug(f"base_languages: {base_languages}")
            logger.debug(f"to_languages: {to_languges}")
            logger.debug(f"translation_request_timeout: {translation_request_timeout}")
            logger.debug(f"num_workers: {num_workers}")
            logger.debug(f"interval_between_scans: {interval_between_scans}")
            logger.debug(f"batch_size: {batch_size}")
            logger.debug(f"dual_queue_mode: {dual_queue_mode}")
            logger.debug(f"web_ui_enabled: {web_ui_enabled}")
            logger.debug(f"web_ui_port: {web_ui_port}")
            logger.debug(f"log_level: {log_level}")
            logger.debug(f"log_directory: {log_directory}")
            logger.debug(f"series_scan: {series_scan}")
            logger.debug(f"movies_scan: {movies_scan}")
            logger.debug("End Configuration: ----------------")
        case "error":
            logger.setLevel(logging.ERROR)

    # Start running things
    loop = asyncio.new_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown)

    loop.run_until_complete(main(base_url, api_key))