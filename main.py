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
from logging.handlers import TimedRotatingFileHandler
from class_types import Serie, Movie, SubtitleTranslate
from queue_manager import QueueManager
from web_interface import init_web_interface, run_web_interface, WebLogHandler, broadcast_update

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

# Create queue managers based on dual_queue_mode
if dual_queue_mode:
    queue_manager_series = QueueManager(
        max_retries=max_retries,
        state_file='/config/queue_state_series.json',
        on_state_change=lambda: broadcast_update()
    )
    queue_manager_movies = QueueManager(
        max_retries=max_retries,
        state_file='/config/queue_state_movies.json',
        on_state_change=lambda: broadcast_update()
    )
    queue_manager = None  # Not used in dual queue mode
else:
    queue_manager = QueueManager(
        max_retries=max_retries,
        state_file='/config/queue_state.json',
        on_state_change=lambda: broadcast_update()
    )
    queue_manager_series = None
    queue_manager_movies = None

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

async def find_base_language_subtitles_from_missing_sutitles(base_url, api_key, videos: List[Serie] | List[Movie], batch_size: int = 50, priority: bool = False) -> int:
    """Process missing subtitles and add them to queue during fetch. Returns count of items added."""
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
                # QueueManager handles deduplication internally, so just add to map
                video_id_language_map[video_id] = missing_sub.code2

    if len(video_id_language_map) == 0:
        logger.info("No missing subtitles found that is in list of languages to be translated")
        return 0

    metadata: List[Serie] | List[Movie] | None = None
    if isinstance(videos[0], Serie):
        metadata = await get_episodes_metadata(
            base_url, api_key, episode_ids=list(video_id_language_map.keys()), batch_size=batch_size
        )
    else:
        metadata = await get_movies_metadata(base_url, api_key, movie_ids=list(video_id_language_map.keys()), batch_size=batch_size)

    if metadata is None:
        logger.info("No metadata returned, couldn't find already existing subtitles")
        return 0
    
    video_id_to_video_map: dict[int, Serie | Movie] = {}
    for video in metadata:
        video_id = video.sonarr_episode_id if isinstance(video, Serie) else video.radarr_id
        video_id_to_video_map[video_id] = video
    
    # Determine which queue manager to use
    if dual_queue_mode:
        qm = queue_manager_series if isinstance(videos[0], Serie) else queue_manager_movies
    else:
        qm = queue_manager
    
    # Check the metadata for already existing subtitles and add them to queue immediately
    items_added = 0
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

        for sub in video.subtitles:
            # Skip subtitles without a valid path
            if sub.path is None:
                continue
                
            if language == sub.code2:
                continue # Skip metadata for subtitle if it's in the same language to for the translation

            # Validate the language code before processing
            if not is_valid_language_code(sub.code2):
                logger.warning(f"Invalid language code '{sub.code2}' found for subtitle '{sub.name}' on video {video_id}. Skipping. This may be a Bazarr data issue.")
                continue

            # If the subtitle is in the base language list, add it to queue immediately
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
                
                subtitle_item = SubtitleTranslate(sub, language, video_id, isinstance(video, Serie), video_title)
                qm.add_item(subtitle_item, priority=priority)
                items_added += 1
                break

    if items_added == 0:
        logger.info("No already existing subtitles matched with requested translation subs")
    else:
        logger.info(f"Added {items_added} matching subtitles to queue")
    
    return items_added

# Track all seen items across scans to identify new ones
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
    skipped_count = 0
    
    for sub in subtitles:
        # Get the appropriate queue manager
        if dual_queue_mode:
            qm = queue_manager_series if sub.is_serie else queue_manager_movies
        else:
            qm = queue_manager
        
        # Add to queue - QueueManager handles deduplication
        if qm.add_item(sub, priority=priority):
            new_count += 1
        else:
            skipped_count += 1
    
    if new_count > 0:
        logger.info(f"Queued {new_count} new subtitles for translation{' (priority)' if priority else ''}")
    if skipped_count > 0:
        logger.debug(f"Skipped {skipped_count} subtitles already in system")

def translation_worker(worker_id, base_url, api_key, queue_type="combined"):
    """Worker thread that processes translation requests from queue.
    
    Args:
        queue_type: 'combined', 'series', or 'movies'
    """
    endpoint = f"{base_url}/api/subtitles"
    headers = {"X-API-KEY": api_key}
    
    # Get the appropriate queue manager
    if dual_queue_mode:
        if queue_type == "series":
            qm = queue_manager_series
            worker_label = f"Series Worker {worker_id}"
        else:  # movies
            qm = queue_manager_movies
            worker_label = f"Movies Worker {worker_id}"
    else:
        qm = queue_manager
        worker_label = f"Worker {worker_id}"
    
    with httpx.Client(timeout=translation_request_timeout) as client:
        while True:
            sub: SubtitleTranslate | None = None
            try:
                # Get next item - this blocks until available and auto-marks as 'processing'
                sub = qm.get_next_item()
                if sub is None:
                    continue
                
                # Check if subtitle filename contains language code
                filename = sub.base_subtitle.path
                has_lang_code = f".{sub.base_subtitle.code2}." in filename.lower() or filename.lower().endswith(f".{sub.base_subtitle.code2}.srt")
                if not has_lang_code:
                    logger.warning(f"[{worker_label}] Subtitle file missing language code in filename: {filename}")
                
                logger.info(f"[{worker_label}] Translating: {sub.base_subtitle.path} ({sub.base_subtitle.code2} → {sub.to_language})")

                # Execute translation
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

                start_time = time.time()
                response = client.patch(endpoint, headers=headers, params=params)
                response.raise_for_status()
                translation_time = time.time() - start_time
                
                # Check if translation completed suspiciously fast (likely failed)
                if translation_time < min_translation_time:
                    logger.warning(f"[{worker_label}] Translation completed too quickly ({translation_time:.1f}s < {min_translation_time}s) - marking as failed")
                    qm.mark_completed(sub, success=False)  # This will retry or mark failed based on retry count
                else:
                    logger.info(f"[{worker_label}] Translation completed successfully ({translation_time:.1f}s)")
                    qm.mark_completed(sub, success=True)
                    
            except httpx.HTTPStatusError as e:
                logger.error(f"[{worker_label}] HTTP error translating {sub.base_subtitle.path if sub else 'unknown'}: {e}")
                if sub:
                    qm.mark_completed(sub, success=False)
            except Exception as e:
                logger.error(f"[{worker_label}] Error translating {sub.base_subtitle.path if sub else 'unknown'}: {e}")
                if sub:
                    qm.mark_completed(sub, success=False)

async def scan_series(base_url, api_key, priority: bool = False):
    """Scan for episodes and add them to queue during fetch."""
    logger.info("Scanning for episodes")
    series = await get_wanted_episodes(base_url, api_key)
    if series is None or len(series) == 0:
        logger.info("Found no missing subtitles for episodes")
        return
    
    logger.info(f"Found {len(series)} missing subtitles for episodes")
    await find_base_language_subtitles_from_missing_sutitles(base_url, api_key, series, batch_size, priority=priority)

async def scan_movies(base_url, api_key, priority: bool = False):
    """Scan for movies and add them to queue during fetch."""
    logger.info("Scanning for movies")
    movies = await get_wanted_movies(base_url, api_key)
    if movies is None or len(movies) == 0:
        logger.info("Found no missing subtitles for movies")
        return
    
    logger.info(f"Found {len(movies)} missing subtitles for movies")
    await find_base_language_subtitles_from_missing_sutitles(base_url, api_key, movies, batch_size, priority=priority)

async def scanner_task(base_url, api_key):
    """Continuously scan for new items and add them to queue with priority."""
    first_scan = True
    
    while not shutdown_event.is_set():
        try:
            # Determine what to scan based on triggered events
            scan_series_now = series_scan
            scan_movies_now = movies_scan
            
            # Items are added to queue during scan, so we just trigger the scans
            # In dual queue mode, scan both simultaneously
            if dual_queue_mode and scan_series_now and scan_movies_now:
                await asyncio.gather(
                    scan_series(base_url, api_key, priority=not first_scan),
                    scan_movies(base_url, api_key, priority=not first_scan),
                    return_exceptions=True
                )
            else:
                # Sequential scanning for single queue mode or when only one type is enabled
                if scan_series_now:
                    await scan_series(base_url, api_key, priority=not first_scan)
                
                if scan_movies_now:
                    await scan_movies(base_url, api_key, priority=not first_scan)
            
            # Always show queue status after each scan
            if dual_queue_mode:
                series_stats = queue_manager_series.get_stats()
                movies_stats = queue_manager_movies.get_stats()
                logger.info(f"Queue sizes - Series: {series_stats['queued']} queued, {series_stats['processing']} processing | Movies: {movies_stats['queued']} queued, {movies_stats['processing']} processing")
            else:
                stats = queue_manager.get_stats()
                logger.info(f"Queue status - Queued: {stats['queued']}, Processing: {stats['processing']}, Completed: {stats['completed']}, Failed: {stats['failed']}")
            
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
            queue_manager,
            queue_manager_series,
            queue_manager_movies,
            trigger_manual_scan,
            trigger_manual_scan_series,
            trigger_manual_scan_movies
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