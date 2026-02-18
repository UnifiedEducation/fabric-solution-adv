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

# # nb-av01-4-validate
# **Purpose**: Run Great Expectations validations on Gold layer tables.
# **Stage**: Gold (validation only)
# **Dependencies**: nb-av01-generic-functions
# **Metadata**: instructions.validations, metadata.expectation_store

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

# Build base path for Gold layer
GOLD_BASE_PATH = construct_abfs_path(variables.LH_WORKSPACE_NAME, variables.GOLD_LH_NAME, area="Tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Metadata

# CELL ********************

set_metadata_db_url(
    server=variables.METADATA_SERVER,
    database=variables.METADATA_DB
)

# Load expectation store for GX method lookup (expectation_id -> gx_method)
expectation_lookup = load_expectation_store(spark)

# Load log store for logging
log_lookup = load_log_store(spark)

validation_instructions = get_active_instructions(spark, "validations", layer="gold")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Initialize Great Expectations

# CELL ********************

# Create ephemeral GX context for Fabric (no persistent store needed)
context = gx.get_context(mode="ephemeral")

# Add Spark datasource
datasource = context.data_sources.add_spark(name="spark_datasource")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Validations
# Expected fields in each instruction from `instructions.validations`:
# - `target_table` (str, required): Table to validate (e.g., 'marketing/channels')
# - `expectation_id` (int, required): Lookup key in metadata.expectation_store
# - `column_name` (str, optional): Column to validate
# - `validation_params` (JSON str, optional): Additional GX expectation parameters
# - `severity` (str, optional): 'error' or 'warning' (default: 'error')
# - `validation_instr_id` (int, required): Instruction identifier for logging
# - `log_function_id` (int, required): Lookup key in metadata.log_store
# - `pipeline_name` (str, optional): Pipeline name for logging
# - `notebook_name` (str, optional): Notebook name for logging

# CELL ********************

# Read pipeline/notebook identity from instruction metadata (for logging)
first_instr = validation_instructions[0] if validation_instructions else {}
PIPELINE_NAME = first_instr.get("pipeline_name", "data_pipeline")
NOTEBOOK_NAME = first_instr.get("notebook_name", "nb-av01-4-validate")

# Group validations by target table to minimize table reads
validations_by_table = {}
for v in validation_instructions:
    validations_by_table.setdefault(v["target_table"], []).append(v)

# Build table-level instructions for execute_pipeline_stage
# log_function_id=1 (log_standard) handles pipeline_runs logging via execute_pipeline_stage
table_instructions = [
    {"target_table": t, "validations": vals, "log_function_id": 1}
    for t, vals in validations_by_table.items()
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_executor(spark, instr):
    """Execute validations for a single table. Returns (row_count, source_name, detail)."""
    table_name = instr["target_table"]
    table_path = f"{GOLD_BASE_PATH}{table_name}"

    df = spark.read.format("delta").load(table_path)
    row_count = df.count()
    print(f"  Loaded {row_count} rows, running validations...")

    validation_result, expectations = run_table_validations(
        datasource, df, table_name, instr["validations"], expectation_lookup
    )

    # Detail log: per-expectation results to log.validation_results
    log_validation(spark=spark, validation_result=validation_result,
                   target_table=table_name, lakehouse_name=variables.GOLD_LH_NAME,
                   started_at=datetime.now())

    if not validation_result.success:
        raise ValueError(f"Validation failed for {table_name}")

    return (row_count, table_name, table_name)



execute_pipeline_stage(
    spark=spark,
    instructions=table_instructions,
    stage_executor=validate_executor,
    notebook_name=NOTEBOOK_NAME,
    pipeline_name=PIPELINE_NAME,
    action_type=ACTION_VALIDATION,
    log_lookup=log_lookup
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
