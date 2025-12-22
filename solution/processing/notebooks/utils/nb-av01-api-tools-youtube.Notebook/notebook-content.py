# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb-av01-api-tools-youtube
# # **Purpose**: YouTube-specific helper functions for API extraction.
# # **Usage**: `%run nb-av01-api-tools-youtube`
# # **Dependencies**: Requires `requests` library (available via nb-av01-generic-functions)
# # **Functions**:
# - `extract_video_ids()` - Extract video IDs from playlist items response
# - `fetch_video_stats_batched()` - Fetch video statistics in batches
# - `fetch_with_pagination()` - Handle YouTube API pagination

# CELL ********************

# Note: 'requests' is imported via %run nb-av01-generic-functions in calling notebooks

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
        requests.HTTPError: If API request fails
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
        
        response = requests.get(f"{base_url}/videos", params=params)
        response.raise_for_status()
        data = response.json()
        
        if "items" in data:
            all_videos.extend(data["items"])
    
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
        requests.HTTPError: If API request fails
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
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if "items" in data:
            all_items.extend(data["items"])
        
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    
    print(f"  Fetched {len(all_items)} items total")
    return all_items

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
