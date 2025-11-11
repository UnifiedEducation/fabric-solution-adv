# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# #### PRJ104 ⬛ ADV Project (Sprint 4): Initial data extraction (YouTube)  
# 
# > The code in this notebook is written as part of Week 4 of the Advanced Project, in [Fabric Dojo](https://skool.com/fabricdojo/about). The intention is first to get the extraction functionality working. In future weeks, we'll be refactoring this into a metadata-driven extraction process. 
# 
# #### Data extraction strategy
# In this notebook we will: 
# - Step 0: Solution Step Up - importing packages, helper functions, defining metadata 
# - Step 1: get AKV secret from Azure Key Vault (secure storage of Google Developers project key for querying YouTube Data V3 API)
# - Step 2: get overall channel information for a YouTube channel, and write it to a Lakehouse Files area (our Bronze layer)
# - Step 3: get all videos on a channel (and write to Bronze layer) 
# - Step 4: get statistics for all videos on a channel (and write to Bronze layer) 
# 
# This notebook is dynamic: it can be run in DEV, TEST and PROD, thanks to the use of Variable libraries. 
# 
# #### Step 0: Solution Set up
# 
# Import packages: 


# CELL ********************

import notebookutils 
import requests 
from datetime import datetime
import os
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# In this solution, we're using Variable Libraries for store different variable values that we need in different deployment workspaces (DEV, TEST, PROD). 
# 
# Let's get the ABFS path for the Lakehouse we are going to write our RAW data to... 
# 
# This will vary depending on the current deployment stage. 


# CELL ********************

variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Define some helper functions (that we'll use across multiple steps): 

# CELL ********************

def get_data_from_endpoint(BASE_URL, base_params, additional_params = ""): 
    """Construct a GET request and return the JSON results"""
    
    params = base_params + additional_params

    full_URI = f'{BASE_URL}?key={api_key}&{params}'

    response = requests.get(full_URI) 

    json_data = response.json()

    return json_data

def get_data_with_pagination(BASE_URL, base_params, additional_params = ""):
    """Construct a GET request and return the JSON results, with Pagination"""
    all_items = []
    next_page_token = None
    params = base_params + additional_params

    while True:
        if next_page_token:
            additional_params_plus_token = additional_params + f'&pageToken={next_page_token}'
        else: 
            additional_params_plus_token = additional_params
        
        json_data = get_data_from_endpoint(BASE_URL, base_params, additional_params_plus_token)

        json_data_items = json_data.get("items", {})
        
        all_items.extend(json_data_items)

        next_page_token = json_data.get('nextPageToken', None)
                
        if not next_page_token:
            break

    print("Total items: ", len(all_items))
    return all_items


def construct_abfs_write_path(write_location): 
    """Constructs an ABFS path to write RAW JSON files into a Lakehouse
    It uses variables (from the Variable Library), that the path is dynamic across deployment environments
    """
    
    # 'variables' is the values from the Variable Library 
    ws_name = variables.LH_WORKSPACE_NAME
    lh_name = variables.BRONZE_LH_NAME

    formatted_date = datetime.now().strftime("%Y%m%d")

    file_name = f"{formatted_date}-{id}.json" 

    abfs_path = f"abfss://{ws_name}@onelake.dfs.fabric.microsoft.com/{lh_name}.Lakehouse/Files/{write_location}{file_name}"
    
    return abfs_path

def write_json_to_location(json_data, location, id):  
    """Write JSON files to RAW Lakehouse area"""

    abfs_path = construct_abfs_write_path(location)

    json_string = json.dumps(json_data, indent=2)

    notebookutils.fs.put(abfs_path, json_string, overwrite=True)
        
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Defining some metadata for scalability
# For now, we will just use a Python object to store the metadata, but in later weeks, we will store this metadata (& add to it!)

# CELL ********************

METADATA = {
    "yt-channels": {
        "base_url": "https://www.googleapis.com/youtube/v3/channels", 
        "base_params": "part=snippet,statistics,contentDetails&id=UCrvoIYkzS-RvCEb0x7wfmwQ",  
        "write_location": "youtube_data_v3/channels/"
  
    }, 
    "yt-playlistItems": {
        "base_url": "https://www.googleapis.com/youtube/v3/playlistItems", 
        "base_params": "part=snippet&maxResults=50", 
        "write_location": "youtube_data_v3/playlistItems/"
    },
    "yt-videos": {
        "base_url": "https://www.googleapis.com/youtube/v3/videos", 
        "base_params": "part=statistics&maxresults=50", 
        "write_location": "youtube_data_v3/videos/"
    }
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Step 1: Get secret from Azure Key Vault

# CELL ********************

def get_secret_from_akv()-> str: 
    """Get API key from Azure Key Vault"""

    akv_name= 'https://av01-akv-restapi-keys.vault.azure.net/' 
    secret_name = 'data-v3-api-key'
    
    api_key = notebookutils.credentials.getSecret(akv_name,secret_name)

    return api_key

api_key = get_secret_from_akv()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Step 2: Get Channel Statistics

# CELL ********************

id = 'yt-channels'

md = METADATA.get(id)

channel_json_data = get_data_from_endpoint(md["base_url"], md["base_params"])

write_json_to_location(channel_json_data, md["write_location"], id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Step 3: Get Playlist Items (all videos on the channel)

# CELL ********************

def extract_uploads_playlist_id(channel_json_data): 
    return channel_json_data.get("items")[0].get("contentDetails").get("relatedPlaylists").get("uploads")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

playlist_id = extract_uploads_playlist_id(channel_json_data) 

id = 'yt-playlistItems'

md = METADATA.get(id)

additional_params = f"&playlistId={playlist_id}"

playlist_json_data = get_data_with_pagination(md["base_url"], md["base_params"], additional_params)

write_json_to_location(playlist_json_data, md["write_location"], id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Step 4: Get Video Statistics for all videos
# 
# First, we'll define a few helper functions for this extraction. 

# CELL ********************

def extract_video_ids(playlist_json): 
    """Input: JSON from previous step (raw playlist items)
       Output: list of video_ids
    """
    video_ids = [] 
    for item in playlist_json:
        # Primary location: contentDetails.videoId
        video_id = item.get('contentDetails', {}).get('videoId')
        
        # Fallback: snippet.resourceId.videoId
        if not video_id:
            video_id = (item.get('snippet', {})
                           .get('resourceId', {})
                           .get('videoId'))
        
        if video_id:
            video_ids.append(video_id)

    return video_ids

def get_video_stats_batched(metadata, video_ids):
    """Batches long list of video_ids into smaller batches to get under the 
    50 id maximum set by the API

    """

    all_videos = []
    batch_size = 40
    md = metadata 
    
    # Split into batches of {batch_size} 
    for i in range(0, len(video_ids), batch_size):
        batch = video_ids[i:i+batch_size]
        batch_count = (i // batch_size) + 1
        total_batches = (len(video_ids) + batch_size - 1) // batch_size
        
        print(f"  Processing batch {batch_count}/{total_batches}: {len(batch)} videos...")
        
        # Convert list to comma-separated string
        additional_params = f"&id={','.join(batch)}"

        results = get_data_from_endpoint(md["base_url"], md["base_params"], additional_params)
                
        all_videos.extend(results["items"])
    
    print(f"Extracted stats for {len(all_videos)} videos")
    return  all_videos


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

id = 'yt-videos'

md = METADATA.get(id)

video_ids = extract_video_ids(playlist_json_data)

video_json_data = get_video_stats_batched(md, video_ids)

write_json_to_location(video_json_data, md["write_location"], id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
