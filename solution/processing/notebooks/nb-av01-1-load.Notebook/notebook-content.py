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
# Provides: LH_WORKSPACE_NAME, BRONZE_LH_NAME, SILVER_LH_NAME, GOLD_LH_NAME, METADATA_SERVER, METADATA_DB
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

# Load loading store for function lookup
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

# CELL ********************

NOTEBOOK_NAME = "nb-av01-1-load"
PIPELINE_NAME = "data_pipeline"

for instr in loading_instructions:
    # Capture start time for accurate duration tracking
    start_time = datetime.now()

    try:
        # Get loading function metadata
        loading_meta = loading_lookup.get(instr["loading_id"])
        if not loading_meta:
            raise ValueError(f"Loading ID {instr['loading_id']} not found")

        # Get the loading function by name from metadata
        function_name = loading_meta["function_name"]
        loading_func = globals().get(function_name)
        if not loading_func:
            raise ValueError(f"Loading function '{function_name}' not implemented")

        # Build paths
        source_path = f"{RAW_BASE_PATH}{instr['source_path'].replace('Files/', '')}"
        target_path = f"{BRONZE_BASE_PATH}{instr['target_table']}"

        # Parse load params
        load_params = json.loads(instr["load_params"]) if instr.get("load_params") else {}
        merge_columns = json.loads(instr["merge_columns"]) if instr.get("merge_columns") else None

        print(f"Loading: {instr['source_path']} -> {instr['target_table']}")

        # Execute loading function
        row_count = loading_func(
            spark=spark,
            source_path=source_path,
            target_path=target_path,
            column_mapping_id=load_params.get("column_mapping_id"),
            merge_condition=instr["merge_condition"],
            merge_type=instr.get("merge_type", "update_all"),
            merge_columns=merge_columns
        )

        print(f"  -> Loaded {row_count} rows")

        # Log success using metadata-driven function lookup
        log_meta = log_lookup.get(instr["log_function_id"])
        if log_meta:
            log_func = globals().get(log_meta["function_name"])
            if log_func:
                log_func(
                    spark=spark,
                    pipeline_name=PIPELINE_NAME,
                    notebook_name=NOTEBOOK_NAME,
                    status=STATUS_SUCCESS,
                    rows_processed=row_count,
                    action_type=ACTION_LOADING,
                    source_name=instr["source_path"],
                    instruction_detail=instr["target_table"],
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
                    status=STATUS_FAILED,
                    rows_processed=0,
                    error_message=str(e),
                    action_type=ACTION_LOADING,
                    source_name=instr["source_path"],
                    instruction_detail=instr["target_table"],
                    started_at=start_time
                )
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
