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
lingarr_url = os.getenv("LINGARR_URL", "").rstrip("/")
lingarr_check_interval = int(get_env_or_default("LINGARR_CHECK_INTERVAL", 30))  # Check for duplicates every N seconds
lingarr_clear_queue_on_startup = os.getenv("LINGARR_CLEAR_QUEUE_ON_STARTUP", "false").lower() in ("true", "1", "yes")

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

async def get_lingarr_active_translations():
    """Get all active translation requests from Lingarr."""
    if not lingarr_url:
        return []
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Get all translation requests with a large page size to catch everything
            response = await client.get(
                f"{lingarr_url}/api/TranslationRequest/requests",
                params={"pageSize": 1000, "pageNumber": 1}
            )
            response.raise_for_status()
            data = response.json()
            
            # Filter for active translations (Pending or InProgress)
            active = [
                item for item in data.get("items", [])
                if item.get("status") in ["Pending", "InProgress"]
            ]
            
            logger.debug(f"Found {len(active)} active translation requests in Lingarr")
            return active
    except Exception as e:
        logger.warning(f"Failed to get Lingarr translation requests: {e}")
        return []

async def cancel_lingarr_translation(translation_request):
    """Cancel a translation request in Lingarr."""
    if not lingarr_url:
        return False
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{lingarr_url}/api/TranslationRequest/cancel",
                json=translation_request
            )
            response.raise_for_status()
            return True
    except Exception as e:
        logger.error(f"Failed to cancel Lingarr translation request {translation_request.get('id')}: {e}")
        return False

async def remove_lingarr_translation(translation_request):
    """Remove a translation request from Lingarr."""
    if not lingarr_url:
        return False
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{lingarr_url}/api/TranslationRequest/remove",
                json=translation_request
            )
            response.raise_for_status()
            return True
    except Exception as e:
        logger.error(f"Failed to remove Lingarr translation request {translation_request.get('id')}: {e}")
        return False

async def clear_lingarr_queue():
    """Remove all translation requests (active, failed, cancelled) from Lingarr."""
    if not lingarr_url:
        return
    
    logger.info("Clearing Lingarr queue on startup...")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Fetch all pages of translation requests
            all_translations = []
            page_number = 1
            page_size = 100
            
            while True:
                response = await client.get(
                    f"{lingarr_url}/api/TranslationRequest/requests",
                    params={"pageSize": page_size, "pageNumber": page_number}
                )
                response.raise_for_status()
                data = response.json()
                
                items = data.get("items", [])
                if not items:
                    break
                
                all_translations.extend(items)
                total_count = data.get("totalCount", 0)
                
                logger.info(f"Fetched page {page_number}: {len(items)} items (total so far: {len(all_translations)}/{total_count})")
                
                # Check if we've fetched everything
                if len(all_translations) >= total_count:
                    break
                
                page_number += 1
            
            if not all_translations:
                logger.info("Lingarr queue is already empty")
                return
            
            logger.info(f"Found {len(all_translations)} translation request(s) to remove")
            removed_count = 0
            
            for trans in all_translations:
                title = trans.get("title", "Unknown")
                trans_id = trans.get("id")
                status = trans.get("status", "Unknown")
                logger.debug(f"  Removing: {title} (ID: {trans_id}, Status: {status})")
                
                # Cancel first if active, then remove
                if status in ["Pending", "InProgress"]:
                    await cancel_lingarr_translation(trans)
                
                if await remove_lingarr_translation(trans):
                    removed_count += 1
            
            logger.info(f"Removed {removed_count}/{len(all_translations)} translation(s) from Lingarr queue")
    except Exception as e:
        logger.error(f"Failed to clear Lingarr queue: {e}")

async def cleanup_duplicate_lingarr_translations():
    """Find and cancel duplicate translation requests in Lingarr, keeping only the oldest one per unique subtitle+language combo."""
    if not lingarr_url:
        return
    
    active_translations = await get_lingarr_active_translations()
    if not active_translations:
        return
    
    # Group by subtitle path + target language
    groups = {}
    for trans in active_translations:
        subtitle_path = trans.get("subtitleToTranslate", "")
        target_lang = trans.get("targetLanguage", "")
        key = f"{subtitle_path}|{target_lang}"
        
        if key not in groups:
            groups[key] = []
        groups[key].append(trans)
    
    # Find duplicates and cancel newer ones
    cancelled_count = 0
    removed_count = 0
    for key, translations in groups.items():
        if len(translations) <= 1:
            continue
        
        # Sort by creation time (oldest first)
        translations.sort(key=lambda x: x.get("createdAt", ""))
        
        # Keep the oldest, cancel and remove the rest
        oldest = translations[0]
        duplicates = translations[1:]
        
        logger.info(f"Found {len(duplicates)} duplicate translation(s) for {oldest.get('title', 'Unknown')} ({oldest.get('targetLanguage', '')})")
        
        for dup in duplicates:
            logger.info(f"  Cancelling and removing duplicate translation ID {dup.get('id')} (created at {dup.get('createdAt')})")
            if await cancel_lingarr_translation(dup):
                cancelled_count += 1
            if await remove_lingarr_translation(dup):
                removed_count += 1
    
    if cancelled_count > 0:
        logger.info(f"Cancelled {cancelled_count} and removed {removed_count} duplicate translation request(s) in Lingarr")

async def lingarr_cleanup_loop():
    """Background loop that periodically checks and cancels duplicate translations in Lingarr."""
    if not lingarr_url:
        logger.info("Lingarr duplicate cleanup disabled (LINGARR_URL not set)")
        return
    
    logger.info(f"Starting Lingarr duplicate cleanup loop (checking every {lingarr_check_interval}s)")
    
    while not shutdown_event.is_set():
        try:
            await cleanup_duplicate_lingarr_translations()
        except Exception as e:
            logger.error(f"Error in Lingarr cleanup loop: {e}")
        
        # Wait for interval or shutdown
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=lingarr_check_interval)
            break  # Shutdown event was set
        except asyncio.TimeoutError:
            pass  # Normal timeout, continue loop

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
    """Process missing subtitles and add them to queue IMMEDIATELY as they're found. Returns count of items added."""
    # Build video id to language map AND preserve original video data for titles
    video_id_language_map = {}
    video_id_to_original = {}  # Store original video objects with their titles
    
    for video in videos:
        video_id = video.sonarr_episode_id if isinstance(video, Serie) else video.radarr_id
        for missing_sub in video.missing_subtitles:
            if not is_valid_language_code(missing_sub.code2):
                logger.warning(f"Invalid language code '{missing_sub.code2}' found in missing subtitles for video {video_id}. Skipping.")
                continue
                
            if missing_sub.code2 in to_languges:
                video_id_language_map[video_id] = missing_sub.code2
                video_id_to_original[video_id] = video  # Store original with title data

    if len(video_id_language_map) == 0:
        logger.info("No missing subtitles found that is in list of languages to be translated")
        return 0

    # Determine which queue manager to use
    is_series = isinstance(videos[0], Serie)
    if dual_queue_mode:
        qm = queue_manager_series if is_series else queue_manager_movies
    else:
        qm = queue_manager

    # Fetch metadata in batches and process IMMEDIATELY
    ids_to_fetch = list(video_id_language_map.keys())
    items_added = 0
    
    logger.debug(f"Fetching metadata for {len(ids_to_fetch)} items in batches of {batch_size}")
    
    # Process in batches - add to queue as soon as each batch is fetched
    for batch_start in range(0, len(ids_to_fetch), batch_size):
        batch_ids = ids_to_fetch[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        total_batches = (len(ids_to_fetch) + batch_size - 1) // batch_size
        
        logger.debug(f"Processing batch {batch_num}/{total_batches} ({len(batch_ids)} items)")
        
        # Fetch this batch
        if is_series:
            batch_metadata = await get_episodes_metadata(base_url, api_key, episode_ids=batch_ids, batch_size=len(batch_ids))
        else:
            batch_metadata = await get_movies_metadata(base_url, api_key, movie_ids=batch_ids, batch_size=len(batch_ids))
        
        if batch_metadata is None:
            logger.warning(f"Batch {batch_num} returned no metadata")
            continue
        
        # Process this batch IMMEDIATELY - add matching items to queue
        for video in batch_metadata:
            video_id = video.sonarr_episode_id if isinstance(video, Serie) else video.radarr_id
            target_language = video_id_language_map.get(video_id)
            
            if target_language is None:
                continue
            
            if video.subtitles is None:
                logger.debug(f"Video {video_id} has no existing subtitles")
                continue

            # Find base language subtitle and add to queue immediately
            for sub in video.subtitles:
                if sub.path is None:
                    continue
                    
                if target_language == sub.code2:
                    continue  # Skip if same language as target
                
                if not is_valid_language_code(sub.code2):
                    logger.warning(f"Invalid language code '{sub.code2}' for subtitle on video {video_id}. Skipping.")
                    continue

                # Found a base language subtitle - add it NOW
                if sub.code2 in base_languages:
                    # Get original video data for proper title
                    original_video = video_id_to_original.get(video_id)
                    
                    # Build video title from WANTED episodes data (has full metadata)
                    if isinstance(video, Serie) and original_video:
                        series_name = original_video.series_title or f"Series {original_video.sonarr_series_id}"
                        episode_num = original_video.episode_number or "?x?"
                        episode_name = original_video.episode_title or "Unknown"
                        video_title = f"{series_name} {episode_num} - {episode_name}"
                    elif original_video and hasattr(original_video, 'title'):
                        video_title = original_video.title or f"Movie {video_id}"
                    else:
                        video_title = f"Video {video_id}"
                    
                    subtitle_item = SubtitleTranslate(sub, target_language, video_id, is_series, video_title)
                    # Only increment counter if item was actually added (not a duplicate)
                    if qm.add_item(subtitle_item, priority=priority):
                        items_added += 1
                        logger.debug(f"✓ Added to queue: {video_title} ({sub.code2} → {target_language})")
                    else:
                        logger.debug(f"⊘ Skipped duplicate: {video_title} ({sub.code2} → {target_language})")
                    break  # Only add one base subtitle per video

    if items_added == 0:
        logger.info("No existing base language subtitles found for translation")
    else:
        logger.info(f"✓ Added {items_added} items to queue during scan")
    
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

async def find_lingarr_media_id(subtitle_path: str, media_type: str, client: httpx.AsyncClient) -> Optional[int]:
    """
    Find the Lingarr media ID for a given subtitle path.
    Returns the Lingarr internal media ID or None if not found.
    """
    try:
        # Get subtitles for this path from Lingarr
        response = await client.post(
            f"{lingarr_url}/api/Subtitle/all",
            json={"path": subtitle_path}
        )
        response.raise_for_status()
        subtitles = response.json()
        
        if not subtitles or len(subtitles) == 0:
            return None
        
        # Extract the directory path (parent of subtitle file)
        import os
        media_dir = os.path.dirname(subtitle_path)
        
        # Search for the media in Lingarr's database by path
        if media_type == "Episode":
            # For shows, we need to find by the show path
            response = await client.get(
                f"{lingarr_url}/api/Media/shows",
                params={"searchQuery": media_dir, "pageSize": 100}
            )
        else:  # Movie
            response = await client.get(
                f"{lingarr_url}/api/Media/movies",
                params={"searchQuery": media_dir, "pageSize": 100}
            )
        
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])
        
        # Find media that matches the path
        for item in items:
            item_path = item.get("path", "")
            if media_dir.startswith(item_path) or item_path in media_dir:
                if media_type == "Episode":
                    # For episodes, we need to find the specific episode
                    # Return the sonarrId as mediaId for episodes
                    return item.get("sonarrId")
                else:
                    # For movies, return the radarrId
                    return item.get("radarrId")
        
        return None
        
    except Exception as e:
        logger.debug(f"Failed to find Lingarr media ID for {subtitle_path}: {e}")
        return None

def translate_via_lingarr_sync(sub: SubtitleTranslate, client: httpx.Client) -> bool:
    """
    Translate subtitle directly via Lingarr API, bypassing Bazarr.
    Synchronous wrapper for async function.
    Returns True if successful, False otherwise.
    """
    import asyncio
    
    async def _translate():
        async with httpx.AsyncClient(timeout=60.0) as async_client:
            media_type = "Episode" if sub.is_serie else "Movie"
            
            # Try to find the media ID in Lingarr
            lingarr_media_id = await find_lingarr_media_id(sub.base_subtitle.path, media_type, async_client)
            
            if lingarr_media_id is None:
                raise Exception(f"Media not found in Lingarr database for path: {sub.base_subtitle.path}")
            
            # Prepare the request payload
            payload = {
                "mediaId": lingarr_media_id,
                "subtitlePath": sub.base_subtitle.path,
                "sourceLanguage": sub.base_subtitle.code2,
                "targetLanguage": sub.to_language,
                "mediaType": media_type,
                "subtitleFormat": "srt"
            }
            
            response = await async_client.post(
                f"{lingarr_url}/api/Translate/subtitle",
                json=payload
            )
            response.raise_for_status()
            
            # Response should contain jobId
            data = response.json()
            job_id = data.get("jobId")
            
            if job_id:
                return True
            else:
                raise Exception("No jobId returned from Lingarr")
    
    # Run the async function in the current thread's event loop
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(_translate())
        loop.close()
        return result
    except Exception as e:
        raise e

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
                
                # Decide whether to use Lingarr directly or go through Bazarr
                use_lingarr_direct = not has_lang_code and lingarr_url
                
                if use_lingarr_direct:
                    logger.info(f"[{worker_label}] Translating via Lingarr (missing language code): {sub.base_subtitle.path} ({sub.base_subtitle.code2} → {sub.to_language})")
                else:
                    if not has_lang_code:
                        logger.warning(f"[{worker_label}] Subtitle file missing language code in filename: {filename}")
                        logger.warning(f"[{worker_label}] This may cause Lingarr to misdetect source language. Consider renaming to include '.{sub.base_subtitle.code2}.' in filename")
                    logger.info(f"[{worker_label}] Translating: {sub.base_subtitle.path} ({sub.base_subtitle.code2} → {sub.to_language})")

                start_time = time.time()
                
                if use_lingarr_direct:
                    # Translate directly via Lingarr API
                    try:
                        translate_via_lingarr_sync(sub, client)
                        translation_time = time.time() - start_time
                        logger.info(f"[{worker_label}] Translation submitted to Lingarr successfully ({translation_time:.1f}s)")
                        qm.mark_completed(sub, success=True)
                    except Exception as e:
                        translation_time = time.time() - start_time
                        logger.error(f"[{worker_label}] Failed to translate via Lingarr: {e}")
                        logger.info(f"[{worker_label}] Falling back to Bazarr API")
                        # Fall back to Bazarr
                        use_lingarr_direct = False
                
                if not use_lingarr_direct:
                    # Execute translation via Bazarr
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
                    translation_time = time.time() - start_time
                    
                    # Log response for debugging
                    if response.status_code != 204:
                        logger.warning(f"[{worker_label}] Unexpected status code: {response.status_code}")
                        try:
                            logger.debug(f"[{worker_label}] Response body: {response.text}")
                        except:
                            pass
                    
                    response.raise_for_status()
                    
                    # Check if translation completed suspiciously fast (likely failed)
                    if translation_time < min_translation_time:
                        logger.warning(f"[{worker_label}] Translation completed too quickly ({translation_time:.1f}s < {min_translation_time}s) - marking as failed")
                        qm.mark_completed(sub, success=False)  # This will retry or mark failed based on retry count
                    else:
                        logger.info(f"[{worker_label}] Translation completed successfully ({translation_time:.1f}s)")
                        qm.mark_completed(sub, success=True)
                    
            except httpx.HTTPStatusError as e:
                logger.error(f"[{worker_label}] HTTP {e.response.status_code} error translating {sub.base_subtitle.path if sub else 'unknown'}: {e}")
                try:
                    logger.error(f"[{worker_label}] Error response body: {e.response.text}")
                except:
                    pass
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
    
    # Clear Lingarr queue if requested
    if lingarr_clear_queue_on_startup:
        await clear_lingarr_queue()
    
    # Start Lingarr duplicate cleanup task
    lingarr_cleanup_task = asyncio.create_task(lingarr_cleanup_loop())
    
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