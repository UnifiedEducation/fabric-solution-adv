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

# # nb-av01-1-load
#
# **Purpose**: Load raw JSON files into Bronze Delta tables using column mappings.
#
# **Stage**: Raw (Files) → Bronze (Delta tables)
#
# **Dependencies**: nb-av01-generic-functions
#
# **Metadata**: instructions.loading, metadata.loading_store, metadata.column_mappings

# MARKDOWN ********************

# ## Imports & Setup

# CELL ********************

%run nb-av01-generic-functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuration

# CELL ********************

# Load workspace-specific variables from Variable Library
variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

# Build base paths
RAW_BASE_PATH = construct_abfs_path(variables.LH_WORKSPACE_NAME, variables.BRONZE_LH_NAME, area="Files")
BRONZE_BASE_PATH = construct_abfs_path(variables.LH_WORKSPACE_NAME, variables.BRONZE_LH_NAME, area="Tables")

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

# Load loading store for function lookup (loading_id -> function_name)
loading_lookup = load_loading_store(spark)

# Load log store for logging
log_lookup = load_log_store(spark)

# Get all active loading instructions for bronze layer
loading_instructions = get_active_instructions(spark, "loading", layer="bronze")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Loading
#
# Expected fields in each instruction from `instructions.loading`:
# - `loading_id` (int, required): Lookup key in metadata.loading_store
# - `source_path` (str, required): Path to raw JSON files in landing zone
# - `target_table` (str, required): Delta table name in Bronze (e.g., 'youtube/channels')
# - `merge_condition` (str, required): SQL MERGE condition (e.g., 'target.id = source.id')
# - `merge_type` (str, required): 'update_all' or 'specific_columns'
# - `merge_columns` (JSON str, optional): Column lists for specific_columns merge
# - `load_params` (JSON str, optional): Additional parameters (e.g., column_mapping_id)
# - `log_function_id` (int, required): Lookup key in metadata.log_store
# - `pipeline_name` (str, optional): Pipeline name for logging
# - `notebook_name` (str, optional): Notebook name for logging

# CELL ********************

# Read pipeline/notebook identity from instruction metadata
first_instr = loading_instructions[0] if loading_instructions else {}
PIPELINE_NAME = first_instr.get("pipeline_name", "data_pipeline")
NOTEBOOK_NAME = first_instr.get("notebook_name", "nb-av01-1-load")


def load_executor(spark, instr):
    """Execute a single loading instruction. Returns (row_count, source_name, detail)."""
    # Resolve loading function from metadata
    loading_meta = loading_lookup.get(instr["loading_id"])
    if not loading_meta:
        raise ValueError(f"Loading ID {instr['loading_id']} not found in loading_store")

    function_name = loading_meta["function_name"]
    loading_func = globals().get(function_name)
    if not loading_func:
        raise ValueError(f"Loading function '{function_name}' not found")

    # Build paths (build_source_path normalizes any 'Files/' prefix in metadata)
    source_path = build_source_path(RAW_BASE_PATH, instr["source_path"])
    target_path = f"{BRONZE_BASE_PATH}{instr['target_table']}"

    # Parse optional JSON fields
    load_params = json.loads(instr["load_params"]) if instr.get("load_params") else {}
    merge_columns = json.loads(instr["merge_columns"]) if instr.get("merge_columns") else None

    if not instr.get("merge_type"):
        raise ValueError(f"merge_type is required in loading instruction for {instr['target_table']}")

    print(f"Loading: {instr['source_path']} -> {instr['target_table']}")

    row_count = loading_func(
        spark=spark,
        source_path=source_path,
        target_path=target_path,
        column_mapping_id=load_params.get("column_mapping_id"),
        merge_condition=instr["merge_condition"],
        merge_type=instr["merge_type"],
        merge_columns=merge_columns
    )

    print(f"  -> Loaded {row_count} rows")
    return (row_count, instr["source_path"], instr["target_table"])


execute_pipeline_stage(
    spark=spark,
    instructions=loading_instructions,
    stage_executor=load_executor,
    notebook_name=NOTEBOOK_NAME,
    pipeline_name=PIPELINE_NAME,
    action_type=ACTION_LOADING,
    log_lookup=log_lookup
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
