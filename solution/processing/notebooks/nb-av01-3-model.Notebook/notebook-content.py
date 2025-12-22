# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": []
# META     },
# META     "environment": {
# META       "environmentId": "8e42a676-c1b7-8c84-4def-63a50b9c5c90",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb-av01-3-model
#
# **Purpose**: Transform Silver data to Gold using business modeling rules.
#
# **Stage**: Silver → Gold
#
# **Dependencies**: nb-av01-generic-functions
#
# **Metadata**: instructions.transformations (dest_layer='gold'), metadata.transform_store

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

# Build base paths for Silver and Gold lakehouses
SILVER_BASE_PATH = construct_abfs_path(variables.LH_WORKSPACE_NAME, variables.SILVER_LH_NAME, area="Tables")
GOLD_BASE_PATH = construct_abfs_path(variables.LH_WORKSPACE_NAME, variables.GOLD_LH_NAME, area="Tables")

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

# Load transform store for function lookup (transform_id -> function_name)
transform_lookup = load_transform_store(spark)

# Load log store for logging
log_lookup = load_log_store(spark)

# Get all active transformation instructions for gold layer (Silver -> Gold)
transform_instructions = get_active_instructions(spark, "transformations", layer="gold")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Transformations

# CELL ********************

NOTEBOOK_NAME = "nb-av01-3-model"
PIPELINE_NAME = "data_pipeline"

for instr in transform_instructions:
    start_time = datetime.now()

    try:
        # Build paths using pre-built base paths
        source_path = SILVER_BASE_PATH + instr["source_table"]
        dest_path = GOLD_BASE_PATH + instr["dest_table"]

        print(f"Modeling: {instr['source_table']} -> {instr['dest_table']}")

        # Read source data
        df = spark.read.format("delta").load(source_path)
        print(f"  -> Read {df.count()} rows from source")

        # Parse transform pipeline and params from JSON
        pipeline = json.loads(instr["transform_pipeline"])
        params = json.loads(instr["transform_params"]) if instr.get("transform_params") else {}

        # Execute transform pipeline using metadata lookup
        result_df = execute_transform_pipeline(
            spark=spark,
            df=df,
            pipeline=pipeline,
            params=params,
            transform_lookup=transform_lookup,
            dest_base_path=GOLD_BASE_PATH
        )

        row_count = result_df.count()
        print(f"  -> Transformed to {row_count} rows")

        # Parse merge columns if present
        merge_columns = json.loads(instr["merge_columns"]) if instr.get("merge_columns") else None

        # Merge to destination
        merge_to_delta(
            spark=spark,
            source_df=result_df,
            target_path=dest_path,
            merge_condition=instr["merge_condition"],
            merge_type=instr.get("merge_type", "update_all"),
            merge_columns=merge_columns
        )

        print(f"  -> Merged to {instr['dest_table']}")

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
                    action_type=ACTION_TRANSFORMATION,
                    source_name=instr["source_table"],
                    instruction_detail=instr["dest_table"],
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
                    action_type=ACTION_TRANSFORMATION,
                    source_name=instr["source_table"],
                    instruction_detail=instr["dest_table"],
                    started_at=start_time
                )
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
