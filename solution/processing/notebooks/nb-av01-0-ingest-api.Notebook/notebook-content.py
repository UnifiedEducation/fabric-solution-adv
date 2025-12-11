# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "8e42a676-c1b7-8c84-4def-63a50b9c5c90",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb-av01-0-ingest-api

# MARKDOWN ********************

# Import Generic & Specific Tools

# CELL ********************

%run nb-av01-generic-functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb-av01-api-tools-youtube

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Load variables from Variable Library, and construct the ABFS path to the Raw Layer Lakehouse

# CELL ********************

# Load workspace-specific variables from Variable Library
# Provides: LH_WORKSPACE_NAME, BRONZE_LH_NAME, SILVER_LH_NAME, GOLD_LH_NAME, METADATA_SERVER, METADATA_DB
variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

# Build base path for raw files landing zone (Files area of Bronze LH)
RAW_BASE_PATH = construct_abfs_path(variables.LH_WORKSPACE_NAME, variables.BRONZE_LH_NAME, area="Files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Load Metadata from the Fabric SQL

# CELL ********************

# Configure connection to metadata SQL database
set_metadata_db_url(
    server=variables.METADATA_SERVER,
    database=variables.METADATA_DB
)

# Load source store for API connection details
source_lookup = load_source_store(spark)

# Load log store for logging function lookup
log_lookup = load_log_store(spark)

# Get all active ingestion instructions
ingestion_instructions = get_active_instructions(spark, "ingestion")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 6: API Ingestion Functions
# 
# Source-specific ingestion logic. These handle pagination, authentication, and API quirks.

# CELL ********************


def ingest_rest_api(source_meta: dict, endpoint_path: str, 
                    request_params: dict, landing_path: str) -> int:
    """
    Generic REST API ingestion with pagination support.
    
    Returns: Total items fetched
    """
    # Get authentication
    if source_meta["auth_method"] == "api_key":
        api_key = get_api_key_from_keyvault(
            source_meta["key_vault_url"],
            source_meta["secret_name"]
        )
        request_params["key"] = api_key
    
    # Build full URL
    base_url = source_meta["base_url"].rstrip("/")
    url = f"{base_url}{endpoint_path}"
    
    # Fetch with pagination
    all_items = []
    page_token = None
    
    while True:
        params = request_params.copy()
        if page_token:
            params["pageToken"] = page_token
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Collect items
        if "items" in data:
            all_items.extend(data["items"])
        else:
            # Single item response (e.g., channel details)
            all_items.append(data)
        
        # Check for next page
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    
    # Save to landing zone
    output_path = f"{RAW_BASE_PATH}{landing_path}"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"{output_path}{timestamp}.json"
    
    # Write JSON file
    output_data = {"items": all_items} if len(all_items) > 1 else all_items[0]
    json_content = json.dumps(output_data, indent=2)
    notebookutils.fs.put(file_path, json_content, overwrite=True)
    
    return len(all_items)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cell 7: Execute Ingestion Instructions

# CELL ********************

NOTEBOOK_NAME = "nb-av01-0-ingest-api"
PIPELINE_NAME = "youtube_pipeline"  # Or get from pipeline parameter

# Store responses for dependency resolution between endpoints
ingestion_responses = {}

for instr in ingestion_instructions:
    # Capture start time for accurate duration tracking
    start_time = datetime.now()
    source_meta = None

    try:
        # Get source metadata
        source_meta = source_lookup.get(instr["source_id"])
        if not source_meta:
            raise ValueError(f"Source ID {instr['source_id']} not found")

        # Parse request params from JSON
        request_params = json.loads(instr["request_params"]) if instr.get("request_params") else {}

        print(f"Ingesting: {source_meta['source_name']}{instr['endpoint_path']}")

        # Get API key
        api_key = get_api_key_from_keyvault(
            source_meta["key_vault_url"],
            source_meta["secret_name"]
        )
        base_url = source_meta["base_url"].rstrip("/")

        # Execute ingestion based on endpoint type
        if instr["endpoint_path"] == "/videos":
            # Videos endpoint needs video IDs from playlistItems response
            playlist_items = ingestion_responses.get("/playlistItems", [])
            if not playlist_items:
                raise ValueError("No playlistItems response found - must run playlistItems first")

            video_ids = extract_video_ids(playlist_items)
            print(f"  -> Extracted {len(video_ids)} video IDs from playlistItems")

            items = fetch_video_stats_batched(base_url, api_key, video_ids)

        elif instr["endpoint_path"] == "/playlistItems":
            # Paginated endpoint - use pagination helper
            url = f"{base_url}{instr['endpoint_path']}"
            items = fetch_with_pagination(url, request_params, api_key)
            # Store response for downstream dependencies
            ingestion_responses[instr["endpoint_path"]] = items

        else:
            # Standard single-call endpoint (channels, etc.)
            url = f"{base_url}{instr['endpoint_path']}"
            request_params["key"] = api_key
            response = requests.get(url, params=request_params)
            response.raise_for_status()
            data = response.json()
            items = data.get("items", [data])

        # Save to landing zone
        item_count = len(items)
        output_path = f"{RAW_BASE_PATH}{instr['landing_path']}"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = f"{output_path}{timestamp}.json"

        output_data = {"items": items} if item_count > 1 else (items[0] if items else {})
        json_content = json.dumps(output_data, indent=2)
        notebookutils.fs.put(file_path, json_content, overwrite=True)

        print(f"  -> Saved {item_count} items to {instr['landing_path']}")

        # Log success using metadata-driven function lookup
        log_meta = log_lookup.get(instr["log_function_id"])
        if log_meta:
            log_func = globals().get(log_meta["function_name"])
            if log_func:
                log_func(
                    spark=spark,
                    pipeline_name=PIPELINE_NAME,
                    notebook_name=NOTEBOOK_NAME,
                    status="success",
                    rows_processed=item_count,
                    action_type="ingestion",
                    source_name=source_meta["source_name"],
                    instruction_detail=instr["endpoint_path"],
                    started_at=start_time
                )

    except Exception as e:
        print(f"  -> ERROR: {str(e)}")

        # Log failure using metadata-driven function lookup
        log_meta = log_lookup.get(instr["log_function_id"])
        if log_meta:
            log_func = globals().get(log_meta["function_name"])
            if log_func:
                log_func(
                    spark=spark,
                    pipeline_name=PIPELINE_NAME,
                    notebook_name=NOTEBOOK_NAME,
                    status="failed",
                    rows_processed=0,
                    error_message=str(e),
                    action_type="ingestion",
                    source_name=source_meta["source_name"] if source_meta else None,
                    instruction_detail=instr["endpoint_path"],
                    started_at=start_time
                )
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
