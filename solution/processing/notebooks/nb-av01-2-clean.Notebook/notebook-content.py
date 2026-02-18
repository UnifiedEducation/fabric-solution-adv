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

# # nb-av01-2-clean
#
# **Purpose**: Transform Bronze data to Silver using metadata-driven cleansing rules.
#
# **Stage**: Bronze → Silver
#
# **Dependencies**: nb-av01-generic-functions
#
# **Metadata**: instructions.transformations (dest_layer='silver'), metadata.transform_store

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

# Build base paths for Bronze and Silver lakehouses
BRONZE_BASE_PATH = construct_abfs_path(variables.LH_WORKSPACE_NAME, variables.BRONZE_LH_NAME, area="Tables")
SILVER_BASE_PATH = construct_abfs_path(variables.LH_WORKSPACE_NAME, variables.SILVER_LH_NAME, area="Tables")

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

# Get all active transformation instructions for silver layer (Bronze -> Silver)
transform_instructions = get_active_instructions(spark, "transformations", layer="silver")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Transformations
#
# Expected fields in each instruction from `instructions.transformations`:
# - `source_table` (str, required): Delta table name in Bronze (e.g., 'youtube/channels')
# - `dest_table` (str, required): Delta table name in Silver
# - `transform_pipeline` (JSON str, required): Ordered array of transform_id values (e.g., [1, 2])
# - `transform_params` (JSON str, optional): Parameters keyed by transform_id (e.g., {"1": {...}})
# - `merge_condition` (str, required): SQL MERGE condition
# - `merge_type` (str, required): 'update_all' or 'specific_columns'
# - `merge_columns` (JSON str, optional): Column lists for specific_columns merge
# - `log_function_id` (int, required): Lookup key in metadata.log_store
# - `pipeline_name` (str, optional): Pipeline name for logging
# - `notebook_name` (str, optional): Notebook name for logging

# CELL ********************

# Read pipeline/notebook identity from instruction metadata
first_instr = transform_instructions[0] if transform_instructions else {}
PIPELINE_NAME = first_instr.get("pipeline_name", "data_pipeline")
NOTEBOOK_NAME = first_instr.get("notebook_name", "nb-av01-2-clean")


def clean_executor(spark, instr):
    """Execute a single clean/transform instruction. Returns (row_count, source_name, detail)."""
    source_path = BRONZE_BASE_PATH + instr["source_table"]
    dest_path = SILVER_BASE_PATH + instr["dest_table"]

    print(f"Transforming: {instr['source_table']} -> {instr['dest_table']}")

    df = spark.read.format("delta").load(source_path)

    # Parse transform pipeline (ordered list of transform_ids) and params
    pipeline = json.loads(instr["transform_pipeline"])
    params = json.loads(instr["transform_params"]) if instr.get("transform_params") else {}

    result_df = execute_transform_pipeline(
        spark=spark,
        df=df,
        pipeline=pipeline,
        params=params,
        transform_lookup=transform_lookup
    )

    merge_columns = json.loads(instr["merge_columns"]) if instr.get("merge_columns") else None

    if not instr.get("merge_type"):
        raise ValueError(f"merge_type is required in transformation instruction for {instr['dest_table']}")

    row_count = merge_to_delta(
        spark=spark,
        source_df=result_df,
        target_path=dest_path,
        merge_condition=instr["merge_condition"],
        merge_type=instr["merge_type"],
        merge_columns=merge_columns
    )

    print(f"  -> Merged to {instr['dest_table']}")
    return (row_count, instr["source_table"], instr["dest_table"])


execute_pipeline_stage(
    spark=spark,
    instructions=transform_instructions,
    stage_executor=clean_executor,
    notebook_name=NOTEBOOK_NAME,
    pipeline_name=PIPELINE_NAME,
    action_type=ACTION_TRANSFORMATION,
    log_lookup=log_lookup
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
