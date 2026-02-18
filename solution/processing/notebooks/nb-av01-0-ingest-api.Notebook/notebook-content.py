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
#
# **Purpose**: Ingest data from external REST APIs to the Raw landing zone.
#
# **Stage**: External APIs → Raw (Files in Bronze Lakehouse)
#
# **Dependencies**: nb-av01-generic-functions, nb-av01-api-tools-youtube

# MARKDOWN ********************

# ## Imports & Setup

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

# ## Configuration

# CELL ********************

# Parameters - can be passed via REST API execution
# These are optional; if not provided, notebookutils.credentials.getSecret() is used (requires user credentials)
spn_tenant_id = ""
spn_client_id = ""
spn_client_secret = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configure SPN credentials for Key Vault access if provided
if spn_tenant_id and spn_client_id and spn_client_secret:
    set_spn_credentials(spn_tenant_id, spn_client_id, spn_client_secret)

# Load workspace-specific variables from Variable Library
variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

# Build base path for raw files landing zone (Files area of Bronze LH)
RAW_BASE_PATH = construct_abfs_path(variables.LH_WORKSPACE_NAME, variables.BRONZE_LH_NAME, area="Files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Metadata

# CELL ********************

# Configure connection to metadata SQL database
set_metadata_db_url(
    server=variables.METADATA_SERVER,
    database=variables.METADATA_DB
)

# Load source store for API connection details (source_id -> base_url, key_vault_url, handler_function, etc.)
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

# ## Execute Ingestion
#
# Expected fields in each instruction from `instructions.ingestion`:
# - `source_id` (int, required): Lookup key in metadata.source_store
# - `endpoint_path` (str, required): API endpoint path (e.g., '/channels')
# - `request_params` (JSON str, optional): Query parameters for the API call
# - `landing_path` (str, required): Subfolder in Raw landing zone
# - `log_function_id` (int, required): Lookup key in metadata.log_store
# - `pipeline_name` (str, optional): Pipeline name for logging
# - `notebook_name` (str, optional): Notebook name for logging
#
# Expected fields in `metadata.source_store`:
# - `source_name` (str): Human-readable source name
# - `base_url` (str): API base URL
# - `key_vault_url` (str): Azure Key Vault URL
# - `secret_name` (str): Secret name in Key Vault
# - `handler_function` (str): Ingestion handler function name (e.g., 'ingest_youtube')

# CELL ********************

# Read pipeline/notebook identity from instruction metadata
first_instr = ingestion_instructions[0] if ingestion_instructions else {}
PIPELINE_NAME = first_instr.get("pipeline_name", "data_pipeline")
NOTEBOOK_NAME = first_instr.get("notebook_name", "nb-av01-0-ingest-api")

# Shared context for cross-instruction dependencies
# (e.g., /videos endpoint needs data from /playlistItems endpoint)
ingestion_context = {}

for instr in ingestion_instructions:
    start_time = datetime.now()
    source_meta = None

    try:
        # Resolve source metadata
        source_meta = source_lookup.get(instr["source_id"])
        if not source_meta:
            raise ValueError(f"Source ID {instr['source_id']} not found in source_store")

        print(f"Ingesting: {source_meta['source_name']}{instr['endpoint_path']}")

        # Get API key from Key Vault
        api_key = get_api_key_from_keyvault(
            source_meta["key_vault_url"],
            source_meta["secret_name"]
        )

        # Dispatch to handler function defined in source metadata
        handler_name = source_meta.get("handler_function")
        if not handler_name:
            raise ValueError(f"No handler_function defined for source '{source_meta['source_name']}'")

        handler_func = globals().get(handler_name)
        if not handler_func:
            raise ValueError(f"Handler function '{handler_name}' not found")

        items = handler_func(source_meta, instr, api_key, ingestion_context)

        # Save to landing zone
        item_count = len(items)
        output_path = f"{RAW_BASE_PATH}{instr['landing_path']}"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = f"{output_path}{timestamp}.json"

        # Wrap multiple items in {"items": [...]} for consistent downstream parsing
        output_data = {"items": items} if item_count > 1 else (items[0] if items else {})
        json_content = json.dumps(output_data, indent=2)
        notebookutils.fs.put(file_path, json_content, overwrite=True)

        print(f"  -> Saved {item_count} items to {instr['landing_path']}")

        # Log success
        log_meta = log_lookup.get(instr["log_function_id"])
        if log_meta:
            log_func = globals().get(log_meta["function_name"])
            if log_func:
                log_func(
                    spark=spark,
                    pipeline_name=PIPELINE_NAME,
                    notebook_name=NOTEBOOK_NAME,
                    status=STATUS_SUCCESS,
                    rows_processed=item_count,
                    action_type=ACTION_INGESTION,
                    source_name=source_meta["source_name"],
                    instruction_detail=instr["endpoint_path"],
                    started_at=start_time
                )
            else:
                print(f"  -> WARNING: Log function '{log_meta['function_name']}' not found")
        else:
            print(f"  -> WARNING: log_function_id '{instr['log_function_id']}' not found in log_store")

    except Exception as e:
        print(f"  -> ERROR: {str(e)}")

        # Log failure
        log_meta = log_lookup.get(instr.get("log_function_id"))
        if log_meta:
            log_func = globals().get(log_meta["function_name"])
            if log_func:
                log_func(
                    spark=spark,
                    pipeline_name=PIPELINE_NAME,
                    notebook_name=NOTEBOOK_NAME,
                    status=STATUS_FAILED,
                    rows_processed=0,
                    error_message=str(e),
                    action_type=ACTION_INGESTION,
                    source_name=source_meta["source_name"] if source_meta else None,
                    instruction_detail=instr.get("endpoint_path"),
                    started_at=start_time
                )
            else:
                print(f"  -> WARNING: Could not log failure - log function not found")
        else:
            print(f"  -> WARNING: Could not log failure - log_function_id not found")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
