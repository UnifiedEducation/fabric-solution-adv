# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb-av01-api-tools-youtube
#
# **Purpose**: YouTube-specific helper functions for API extraction.
#
# **Usage**: `%run nb-av01-api-tools-youtube`
#
# **Dependencies**: Requires `requests` and `json` (available via nb-av01-generic-functions)
#
# **Functions**:
# - `ingest_youtube()` - Entry-point handler for YouTube API ingestion (called by nb-0 via metadata dispatch)
# - `extract_video_ids()` - Extract video IDs from playlist items response
# - `fetch_video_stats_batched()` - Fetch video statistics in batches
# - `fetch_with_pagination()` - Handle YouTube API pagination

# CELL ********************

import time


def _request_with_retry(url, max_retries=3, base_delay=1.0, **kwargs):
    """
    Make HTTP GET request with retry and exponential backoff for transient errors.

    Args:
        url: Request URL
        max_retries: Maximum retry attempts (default: 3)
        base_delay: Base delay in seconds between retries (default: 1.0)
        **kwargs: Additional arguments passed to requests.get

    Returns:
        Response object

    Raises:
        requests.HTTPError: After all retries exhausted
    """
    retryable_codes = {429, 500, 502, 503}

    for attempt in range(max_retries + 1):
        response = requests.get(url, **kwargs)

        if response.status_code not in retryable_codes or attempt == max_retries:
            response.raise_for_status()
            return response

        delay = base_delay * (2 ** attempt)
        print(f"  -> HTTP {response.status_code}, retrying in {delay:.1f}s (attempt {attempt + 1}/{max_retries})")
        time.sleep(delay)


def extract_video_ids(playlist_items: list) -> list:
    """
    Extract video IDs from playlist items response.

    Args:
        playlist_items: List of playlist item dicts from YouTube API

    Returns:
        List of video ID strings (may be empty if no valid IDs found)
    """
    if not playlist_items:
        return []

    video_ids = []
    for item in playlist_items:
        # Primary location: contentDetails.videoId
        video_id = item.get('contentDetails', {}).get('videoId')

        # Fallback: snippet.resourceId.videoId
        if not video_id:
            video_id = item.get('snippet', {}).get('resourceId', {}).get('videoId')

        if video_id:
            video_ids.append(video_id)

    return video_ids


def fetch_video_stats_batched(base_url: str, api_key: str, video_ids: list,
                               part: str = "statistics", batch_size: int = 40) -> list:
    """
    Fetch video statistics in batches (YouTube API max 50 IDs per request).

    Args:
        base_url: YouTube API base URL (e.g., 'https://www.googleapis.com/youtube/v3')
        api_key: API key for authentication
        video_ids: List of video IDs to fetch stats for
        part: API part parameter (default: 'statistics')
        batch_size: Number of IDs per batch (default: 40, max: 50)

    Returns:
        List of video items with statistics

    Raises:
        ValueError: If required parameters are missing
        requests.HTTPError: If API request fails after retries
    """
    if not base_url or not api_key:
        raise ValueError("base_url and api_key are required")
    if not video_ids:
        return []

    # Ensure batch_size doesn't exceed YouTube API limit
    batch_size = min(batch_size, 50)

    all_videos = []
    total_batches = (len(video_ids) + batch_size - 1) // batch_size

    for i in range(0, len(video_ids), batch_size):
        batch = video_ids[i:i+batch_size]
        batch_num = (i // batch_size) + 1
        print(f"  Processing batch {batch_num}/{total_batches}: {len(batch)} videos...")

        params = {
            "part": part,
            "id": ",".join(batch),
            "key": api_key
        }

        response = _request_with_retry(f"{base_url}/videos", params=params)
        data = response.json()

        if "items" in data:
            all_videos.extend(data["items"])

        # Rate limiting: brief pause between batches
        if i + batch_size < len(video_ids):
            time.sleep(0.2)

    print(f"  Fetched stats for {len(all_videos)} videos total")
    return all_videos


def fetch_with_pagination(url: str, params: dict, api_key: str) -> list:
    """
    Fetch all pages from a YouTube API endpoint that supports pagination.

    Args:
        url: Full endpoint URL
        params: Request parameters dict
        api_key: API key for authentication

    Returns:
        List of all items from all pages

    Raises:
        ValueError: If required parameters are missing
        requests.HTTPError: If API request fails after retries
    """
    if not url or not api_key:
        raise ValueError("url and api_key are required")

    all_items = []
    page_token = None
    params = (params or {}).copy()
    params["key"] = api_key

    while True:
        if page_token:
            params["pageToken"] = page_token

        response = _request_with_retry(url, params=params)
        data = response.json()

        if "items" in data:
            all_items.extend(data["items"])

        page_token = data.get("nextPageToken")
        if not page_token:
            break

        # Rate limiting: brief pause between pages
        time.sleep(0.2)

    print(f"  Fetched {len(all_items)} items total")
    return all_items


def ingest_youtube(source_meta, instr, api_key, context):
    """
    YouTube API ingestion entry-point. Routes to the appropriate fetch strategy
    based on endpoint_path.

    Handles endpoint dependencies (e.g., /videos requires /playlistItems data).
    Stores intermediate results in context for downstream endpoints.

    Args:
        source_meta: Source metadata dict (base_url, key_vault_url, etc.)
        instr: Ingestion instruction dict (endpoint_path, request_params, etc.)
        api_key: API key for authentication
        context: Shared dict for cross-instruction state (stores intermediate results)

    Returns:
        List of items from API response
    """
    base_url = source_meta["base_url"].rstrip("/")
    request_params = json.loads(instr["request_params"]) if instr.get("request_params") else {}
    endpoint = instr["endpoint_path"]

    if endpoint == "/videos":
        # /videos requires video IDs extracted from a prior /playlistItems call
        playlist_items = context.get("/playlistItems", [])
        if not playlist_items:
            raise ValueError("No /playlistItems data in context - /playlistItems must run first")

        video_ids = extract_video_ids(playlist_items)
        print(f"  -> Extracted {len(video_ids)} video IDs from /playlistItems")
        items = fetch_video_stats_batched(base_url, api_key, video_ids)

    elif endpoint == "/playlistItems":
        # Paginated endpoint - fetch all pages
        url = f"{base_url}{endpoint}"
        items = fetch_with_pagination(url, request_params, api_key)
        # Store for downstream dependencies (e.g., /videos needs these)
        context[endpoint] = items

    else:
        # Standard single-call endpoint (e.g., /channels)
        url = f"{base_url}{endpoint}"
        request_params["key"] = api_key
        response = _request_with_retry(url, params=request_params)
        data = response.json()
        items = data.get("items", [data])

    return items

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
