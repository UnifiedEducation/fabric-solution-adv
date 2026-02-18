# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# # nb-av01-0-ingest-api
# **Purpose**: Ingest data from external REST APIs to the Raw landing zone.
# 
# **Stage**: External APIs → Raw (Files in Bronze Lakehouse)
# 
# **Dependencies**: nb-av01-generic-functions, nb-av01-api-tools-youtube

# MARKDOWN ********************

# ## Imports & Setup

# PARAMETERS CELL ********************

# Parameters - passed via REST API execution
spn_tenant_id = ""
spn_client_id = ""
spn_client_secret = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# CELL ********************

df = spark.read.option("url", METADATA_DB_URL).mssql("metadata.source_store")
df.printSchema()                                                               

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

  print(source_lookup)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Ingestion
# Expected fields in each instruction from `instructions.ingestion`:
# - `source_id` (int, required): Lookup key in metadata.source_store
# - `endpoint_path` (str, required): API endpoint path (e.g., '/channels')
# - `request_params` (JSON str, optional): Query parameters for the API call
# - `landing_path` (str, required): Subfolder in Raw landing zone
# - `log_function_id` (int, required): Lookup key in metadata.log_store
# - `pipeline_name` (str, optional): Pipeline name for logging
# - `notebook_name` (str, optional): Notebook name for logging
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


def ingest_executor(spark, instr):
    """Execute a single ingestion instruction. Returns (row_count, source_name, detail)."""
    source_meta = source_lookup.get(instr["source_id"])
    if not source_meta:
        raise ValueError(f"Source ID {instr['source_id']} not found in source_store")

    print(f"Ingesting: {source_meta['source_name']}{instr['endpoint_path']}")

    api_key = get_api_key_from_keyvault(source_meta["key_vault_url"], source_meta["secret_name"])
    handler_func = resolve_ingestion_handler(source_meta)
    items = handler_func(source_meta, instr, api_key, ingestion_context)

    item_count = write_to_landing_zone(items, RAW_BASE_PATH, instr["landing_path"])
    print(f"  -> Saved {item_count} items to {instr['landing_path']}")

    return (item_count, source_meta["source_name"], instr["endpoint_path"])


execute_pipeline_stage(
    spark=spark,
    instructions=ingestion_instructions,
    stage_executor=ingest_executor,
    notebook_name=NOTEBOOK_NAME,
    pipeline_name=PIPELINE_NAME,
    action_type=ACTION_INGESTION,
    log_lookup=log_lookup
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
