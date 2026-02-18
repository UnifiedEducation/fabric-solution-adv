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

# # nb-av01-4-validate
#
# **Purpose**: Run Great Expectations validations on Gold layer tables.
#
# **Stage**: Gold (validation only)
#
# **Dependencies**: nb-av01-generic-functions
#
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
#
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

# Read pipeline/notebook identity from instruction metadata
first_instr = validation_instructions[0] if validation_instructions else {}
PIPELINE_NAME = first_instr.get("pipeline_name", "data_pipeline")
NOTEBOOK_NAME = first_instr.get("notebook_name", "nb-av01-4-validate")

# Group validations by target table to minimize table reads
validations_by_table = {}
for v in validation_instructions:
    table = v["target_table"]
    if table not in validations_by_table:
        validations_by_table[table] = []
    validations_by_table[table].append(v)

all_results = {}
all_passed = True

for table_name, table_validations in validations_by_table.items():
    start_time = datetime.now()

    try:
        print(f"\nValidating: {table_name}")

        # Build table path
        table_path = f"{GOLD_BASE_PATH}{table_name}"

        # Read table data
        df = spark.read.format("delta").load(table_path)
        row_count = df.count()
        print(f"  -> Loaded {row_count} rows")

        # Build expectations from metadata
        expectations = []
        for v in table_validations:
            exp_meta = expectation_lookup.get(v["expectation_id"])
            if not exp_meta:
                print(f"  -> WARNING: expectation_id {v['expectation_id']} not found in expectation_store, skipping")
                continue

            # Parse validation params if present
            params = json.loads(v["validation_params"]) if v.get("validation_params") else {}

            # Build expectation using metadata
            exp = build_expectation(
                gx_method=exp_meta["gx_method"],
                column_name=v.get("column_name"),
                validation_params=params
            )
            expectations.append({
                "expectation": exp,
                "severity": v.get("severity", "error"),
                "column": v.get("column_name"),
                "expectation_name": exp_meta["expectation_name"],
                "validation_instr_id": v["validation_instr_id"]
            })

        print(f"  -> Running {len(expectations)} expectations")

        # Create expectation suite
        suite_name = f"{table_name.replace('/', '_')}_suite"
        suite = gx.ExpectationSuite(name=suite_name)
        for e in expectations:
            suite.add_expectation(e["expectation"])

        # Get or create dataframe asset (idempotent - reuses if already created)
        asset_name = f"{table_name.replace('/', '_')}_asset"
        try:
            asset = datasource.get_asset(asset_name)
        except LookupError:
            asset = datasource.add_dataframe_asset(name=asset_name)

        # Get or create batch definition (unique per table to avoid cross-contamination)
        batch_def_name = f"batch_def_{table_name.replace('/', '_')}"
        try:
            batch_definition = asset.get_batch_definition(batch_def_name)
        except LookupError:
            batch_definition = asset.add_batch_definition_whole_dataframe(name=batch_def_name)

        batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

        # Run validation
        validation_result = batch.validate(suite)

        # Add metadata for downstream logging
        validation_result.meta["table_name"] = table_name
        validation_result.meta["validation_instructions"] = expectations

        # Process results
        passed = validation_result.success
        all_results[table_name] = validation_result

        if passed:
            print(f"  -> PASSED all validations")
        else:
            all_passed = False
            print(f"  -> FAILED some validations")

            # Show failed expectations
            for result in validation_result.results:
                if not result.success:
                    exp_type = result.expectation_config.type
                    column = result.expectation_config.kwargs.get("column", "N/A")
                    print(f"     FAILED: {exp_type} on column '{column}'")

        # Log to pipeline_runs (standard logging)
        log_standard(
            spark=spark,
            pipeline_name=PIPELINE_NAME,
            notebook_name=NOTEBOOK_NAME,
            status=STATUS_SUCCESS if passed else STATUS_FAILED,
            rows_processed=row_count,
            action_type=ACTION_VALIDATION,
            instruction_detail=table_name,
            started_at=start_time
        )

        # Log detailed per-expectation results
        # All validations for a table share the same log_function_id; use the first instruction's
        log_meta = log_lookup.get(table_validations[0]["log_function_id"])
        if log_meta:
            log_func = globals().get(log_meta["function_name"])
            if log_func:
                log_func(
                    spark=spark,
                    validation_result=validation_result,
                    target_table=table_name,
                    lakehouse_name=variables.GOLD_LH_NAME,
                    started_at=start_time
                )
            else:
                print(f"  -> WARNING: Log function '{log_meta['function_name']}' not found")
        else:
            print(f"  -> WARNING: log_function_id '{table_validations[0]['log_function_id']}' not found in log_store")

    except Exception as e:
        print(f"  -> ERROR: {str(e)}")
        all_passed = False
        # Log failure to pipeline_runs
        log_standard(
            spark=spark,
            pipeline_name=PIPELINE_NAME,
            notebook_name=NOTEBOOK_NAME,
            status=STATUS_FAILED,
            rows_processed=0,
            error_message=str(e),
            action_type=ACTION_VALIDATION,
            instruction_detail=table_name,
            started_at=start_time
        )
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
