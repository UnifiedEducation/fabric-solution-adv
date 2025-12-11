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

# # PRJ106 Generic Functions Library
# 
# Metadata-driven utility functions for the PRJ106 orchestration framework.
# 
# **Design Principle**: All function names come from metadata. Python implements the functions; metadata controls which ones get called.
# 
# - Transform functions: `metadata.transform_store.function_name` → `globals().get(function_name)`
# - GX expectations: `metadata.expectation_store.gx_method` → `getattr(gxe, gx_method)`

# MARKDOWN ********************

# Imports & Setup

# CELL ********************

# Standard library
import json
import os
from datetime import datetime

# Fabric/Spark
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType
from delta.tables import DeltaTable
import notebookutils

# Great Expectations
import great_expectations as gx
import great_expectations.expectations as gxe

# HTTP
import requests

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Fabric SQL connector - enables .mssql() method for reading/writing
import com.microsoft.sqlserver.jdbc.spark

# SQL Database connection - must be set via set_metadata_db_url() before use
METADATA_DB_URL = None


def set_metadata_db_url(server: str, database: str):
    """
    Configure the metadata database URL for the Fabric SQL connector.
    Call this once at notebook startup.
    
    Args:
        server: SQL server name (without .database.fabric.microsoft.com suffix)
        database: Database name
    """
    global METADATA_DB_URL
    # Fabric SQL Database format - note curly braces around database name
    METADATA_DB_URL = f"jdbc:sqlserver://{server}.database.fabric.microsoft.com:1433;database={{{database}}};encrypt=true;trustServerCertificate=false"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Helper Functions**
# 
# Path construction and file discovery utilities.

# CELL ********************

def get_layer_lakehouse(layer: str, variables) -> str:
    """
    Map layer name to lakehouse from Variable Library.
    Layer values come from instructions tables (source_layer, target_layer, dest_layer).
    """
    mapping = {
        "raw": variables.BRONZE_LH_NAME,  # Raw files stored in Bronze LH Files area
        "bronze": variables.BRONZE_LH_NAME,
        "silver": variables.SILVER_LH_NAME,
        "gold": variables.GOLD_LH_NAME
    }
    return mapping.get(layer)


def construct_abfs_path(workspace: str, lakehouse: str, area: str = "Tables") -> str:
    """
    Build ABFS base path for a lakehouse.
    area: 'Tables' for Delta tables, 'Files' for raw files
    """
    return f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/{area}/"


def get_most_recent_file(base_path: str, folder: str):
    """
    Find most recent file in folder by modifyTime.
    Returns file object with .path attribute.
    """
    full_path = f"{base_path}{folder}"
    files = notebookutils.fs.ls(full_path)
    return max(files, key=lambda f: f.modifyTime)

def get_api_key_from_keyvault(key_vault_url: str, secret_name: str) -> str:
    """
    Retrieve API key from Azure Key Vault using notebookutils.
    Generic utility for any source needing API key authentication.

    Args:
        key_vault_url: Key Vault URL (e.g., 'https://my-vault.vault.azure.net/')
        secret_name: Name of the secret to retrieve

    Returns: Secret value as string
    """
    return notebookutils.credentials.getSecret(key_vault_url, secret_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# **Logging Functions**
# 
# Function names must match `metadata.log_store.function_name`.

# CELL ********************

def log_standard(spark, pipeline_name: str, notebook_name: str, status: str,
                 rows_processed: int = 0, error_message: str = None,
                 action_type: str = None, source_name: str = None,
                 instruction_detail: str = None, started_at: datetime = None, **ctx) -> int:
    """
    Standard logging - records pipeline runs to SQL metadata database.
    Writes to: [log].[pipeline_runs]

    Args:
        pipeline_name: Name of the pipeline (e.g., 'youtube_pipeline')
        notebook_name: Name of the notebook executing this action
        status: 'running', 'success', or 'failed'
        rows_processed: Number of records processed
        error_message: Error details if status='failed'
        action_type: 'ingestion', 'loading', 'transformation', 'validation'
        source_name: Source system name (e.g., 'youtube_api')
        instruction_detail: Specific instruction info (e.g., '/playlistItems', 'youtube/channel')
        started_at: When the action started (for accurate duration tracking)
    """
    completed_at = datetime.now()
    # Use provided started_at or default to completed_at (same time)
    started_at = started_at or completed_at

    # Schema must match table exactly (11 columns)
    schema = StructType([
        StructField("run_id", LongType(), nullable=False),
        StructField("pipeline_name", StringType(), nullable=False),
        StructField("started_at", TimestampType(), nullable=True),
        StructField("completed_at", TimestampType(), nullable=True),
        StructField("status", StringType(), nullable=False),
        StructField("records_processed", IntegerType(), nullable=True),
        StructField("error_message", StringType(), nullable=True),
        StructField("action_type", StringType(), nullable=True),
        StructField("source_name", StringType(), nullable=True),
        StructField("instruction_detail", StringType(), nullable=True),
        StructField("notebook_name", StringType(), nullable=True)
    ])

    # For 'running' status, completed_at should be None
    if status == "running":
        completed_at = None

    log_data = [(0, pipeline_name, started_at, completed_at, status, rows_processed,
                 error_message, action_type, source_name, instruction_detail, notebook_name)]
    log_df = spark.createDataFrame(log_data, schema)

    log_df.write.mode("append").option("url", METADATA_DB_URL).mssql("log.pipeline_runs")

    # Build descriptive log message
    detail = f"{action_type or 'action'}: {source_name or ''}{instruction_detail or ''}"
    print(f"  -> Logged: {detail} - {status} ({rows_processed} rows)")
    return rows_processed



def log_validation(spark, validation_result, **ctx) -> int:
    """
    Validation-specific logging - logs one row per expectation result.
    
    Corresponds to: metadata.log_store.function_name = 'log_validation' (log_id=2)
    Writes to: [log].[validation_results] in metadata database
    
    Args:
        spark: SparkSession
        validation_result: GX ValidationResult object from batch.validate()
    
    Returns: number of expectation results logged
    """
    results_flattened = [
        {
            "validation_id": validation_result.meta['validation_id'],
            "expectation_type": result.expectation_config.type,
            "column": result.expectation_config.kwargs.get("column", None),
            "success": result.success,
            "test_timestamp": datetime.strptime(
                validation_result.meta['batch_markers']['ge_load_time'],
                "%Y%m%dT%H%M%S.%fZ"
            )
        } for result in validation_result.results
    ]

    pandas_df = pd.DataFrame(results_flattened)
    spark_results_df = spark.createDataFrame(pandas_df)

    # Use .mssql() for writing (same connector as reading)
    spark_results_df.write.mode("append").option("url", METADATA_DB_URL).mssql("log.validation_results")

    print(f"  -> Logged {len(results_flattened)} validation results")
    return len(results_flattened)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# **Loading Functions**
# 
# Function names must match `metadata.loading_store.function_name`.

# CELL ********************

def load_json_to_delta(spark, source_path: str, target_path: str,
                       column_mapping_id: str, merge_condition: str,
                       merge_type: str = "update_all",
                       merge_columns: dict = None, **ctx) -> int:
    """
    Load JSON files from Raw zone to Delta table with column mapping and MERGE.
    
    Corresponds to: metadata.loading_store.function_name = 'load_json_to_delta'
    
    Args:
        source_path: ABFS path to raw JSON files folder
        target_path: ABFS path to target Delta table
        column_mapping_id: Key into COLUMN_MAPPINGS dict
        merge_condition: SQL condition for MERGE (e.g., 'target.id = source.id')
        merge_type: 'update_all' or 'specific_columns'
        merge_columns: Dict with 'update' and 'insert' column lists if merge_type='specific_columns'
    
    Returns: row count processed
    """
    # Get column mapping
    mapping = COLUMN_MAPPINGS.get(column_mapping_id, [])
    if not mapping:
        raise ValueError(f"Column mapping '{column_mapping_id}' not found")

    # Read most recent JSON file
    most_recent = get_most_recent_file(source_path, "")
    raw_df = spark.read.option("multiLine", "true").json(most_recent.path)

    # Check if needs explosion (has "items" array)
    if "items" in raw_df.columns:
        raw_df = raw_df.select(F.explode(F.col("items")).alias("item"))
        prefix = "item."
    else:
        prefix = ""

    # Apply column mapping
    select_exprs = []
    for col_map in mapping:
        source = col_map["source"]
        target = col_map["target"]
        col_type = col_map["type"]

        if source == "_loading_ts":
            select_exprs.append(F.current_timestamp().alias(target))
        elif col_type == "timestamp":
            select_exprs.append(F.to_timestamp(F.col(f"{prefix}{source}")).alias(target))
        elif col_type == "int":
            select_exprs.append(F.col(f"{prefix}{source}").cast("int").alias(target))
        else:
            select_exprs.append(F.col(f"{prefix}{source}").alias(target))

    source_df = raw_df.select(*select_exprs)
    row_count = source_df.count()

    # MERGE to target
    delta_table = DeltaTable.forPath(spark, target_path)
    merge_builder = delta_table.alias("target").merge(
        source_df.alias("source"), merge_condition
    )

    if merge_type == "update_all":
        merge_builder.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    elif merge_type == "specific_columns" and merge_columns:
        update_cols = {c: F.col(f"source.{c}") for c in merge_columns.get("update", [])}
        insert_cols = {c: F.col(f"source.{c}") for c in merge_columns.get("insert", [])}
        merge_builder.whenMatchedUpdate(set=update_cols).whenNotMatchedInsert(values=insert_cols).execute()

    return row_count

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# **Transform Functions**
# 
# Function names must match `metadata.transform_store.function_name`.
# 
# The orchestrator queries metadata to get `function_name` for each `transform_id`, then calls `globals().get(function_name)` to resolve the Python function.

# CELL ********************

def filter_nulls(df, columns: list, **ctx):
    """
    Remove rows where specified columns are null.
    
    Corresponds to: metadata.transform_store.function_name = 'filter_nulls'
    Expected params: {"columns": ["col1", "col2"]}
    """
    condition = F.col(columns[0]).isNotNull()
    for col_name in columns[1:]:
        condition = condition & F.col(col_name).isNotNull()
    return df.filter(condition)


def dedupe_by_window(df, partition_cols: list, order_col: str, order_desc: bool = True, **ctx):
    """
    Deduplicate using window function - keeps most recent by order column.
    
    Corresponds to: metadata.transform_store.function_name = 'dedupe_by_window'
    Expected params: {"partition_cols": [...], "order_col": "...", "order_desc": true}
    
    partition_cols can include expressions like "to_date(loading_TS)" which will be parsed.
    """
    partition_exprs = []
    for col in partition_cols:
        if "to_date(" in col:
            inner_col = col.replace("to_date(", "").replace(")", "")
            partition_exprs.append(F.to_date(F.col(inner_col)))
        else:
            partition_exprs.append(F.col(col))

    order_expr = F.col(order_col).desc() if order_desc else F.col(order_col)
    window_spec = Window.partitionBy(*partition_exprs).orderBy(order_expr)

    return (df
        .withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num"))


def rename_columns(df, column_mapping: dict, **ctx):
    """
    Rename columns according to mapping.
    
    Corresponds to: metadata.transform_store.function_name = 'rename_columns'
    Expected params: {"column_mapping": {"old_name": "new_name", ...}}
    """
    select_exprs = []
    for old_name, new_name in column_mapping.items():
        select_exprs.append(F.col(old_name).alias(new_name))
    return df.select(*select_exprs)


def add_literal_columns(df, columns: dict, **ctx):
    """
    Add columns with literal/static values.
    
    Corresponds to: metadata.transform_store.function_name = 'add_literal_columns'
    Expected params: {"columns": {"col_name": value, ...}}
    """
    result_df = df
    for col_name, value in columns.items():
        result_df = result_df.withColumn(col_name, F.lit(value))
    return result_df


def generate_surrogate_key(df, key_column_name: str, order_by_col: str,
                           max_from_table: str = None, **ctx):
    """
    Generate surrogate key using row_number + max existing ID.
    
    Corresponds to: metadata.transform_store.function_name = 'generate_surrogate_key'
    Expected params: {"key_column_name": "...", "order_by_col": "...", "max_from_table": "..."}
    """
    spark = ctx.get("spark")
    max_id = 0
    
    if max_from_table and spark:
        try:
            target_df = DeltaTable.forPath(spark, max_from_table).toDF()
            max_id = target_df.agg(
                F.coalesce(F.max(key_column_name), F.lit(0))
            ).collect()[0][0]
        except:
            max_id = 0

    window_spec = Window.orderBy(order_by_col)
    return df.withColumn(key_column_name, F.row_number().over(window_spec) + max_id)


def lookup_join(df, lookup_table: str, source_key: str,
                lookup_key: str, select_cols: list, **ctx):
    """
    Join to lookup/dimension table to get surrogate key or other columns.
    
    Corresponds to: metadata.transform_store.function_name = 'lookup_join'
    Expected params: {"lookup_table": "...", "source_key": "...", "lookup_key": "...", "select_cols": [...]}
    """
    spark = ctx.get("spark")
    lookup_df = DeltaTable.forPath(spark, lookup_table).toDF()
    lookup_select = lookup_df.select(lookup_key, *select_cols)

    return df.join(
        lookup_select,
        df[source_key] == lookup_select[lookup_key],
        "left"
    ).drop(lookup_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

def get_transform_function(function_name: str):
    """
    Resolve transform function by name using globals().
    function_name comes from metadata.transform_store.function_name column.
    The function must be defined in this module with a matching name.
    """
    return globals().get(function_name)


def execute_transform_pipeline(spark, df, pipeline: list, params: dict,
                               transform_lookup: dict):
    """
    Execute ordered transform pipeline using metadata lookup.

    Args:
        spark: SparkSession
        df: Input DataFrame
        pipeline: List of transform_ids in execution order (e.g., [1, 2])
                  From: instructions.transformations.transform_pipeline JSON
        params: Params keyed by transform_id (e.g., {"1": {...}, "2": {...}})
                From: instructions.transformations.transform_params JSON
        transform_lookup: Dict from load_transform_store()
                         {transform_id: {"function_name": "...", ...}}

    Returns: Transformed DataFrame
    """
    result_df = df
    ctx = {"spark": spark}  # Context for functions that need spark

    for transform_id in pipeline:
        # Get function_name from metadata lookup
        transform_meta = transform_lookup.get(transform_id)
        if not transform_meta:
            raise ValueError(f"Transform ID {transform_id} not found in metadata")

        function_name = transform_meta["function_name"]
        transform_func = get_transform_function(function_name)
        if not transform_func:
            raise ValueError(f"Function '{function_name}' not implemented")

        # Get params for this transform
        transform_params = params.get(str(transform_id), {})

        # Execute transform (pass ctx for functions that need spark)
        result_df = transform_func(result_df, **transform_params, **ctx)

    return result_df


def merge_to_delta(spark, source_df, target_path: str, merge_condition: str,
                   merge_type: str = "update_all", merge_columns: dict = None):
    """
    Generic MERGE operation for any layer transition.
    
    Args:
        source_df: DataFrame to merge into target
        target_path: ABFS path to target Delta table
        merge_condition: SQL condition (e.g., 'target.id = source.id')
        merge_type: 'update_all' or 'specific_columns'
        merge_columns: Dict with 'update' and 'insert' lists if merge_type='specific_columns'
    
    Returns: row count
    """
    delta_table = DeltaTable.forPath(spark, target_path)
    merge_builder = delta_table.alias("target").merge(
        source_df.alias("source"), merge_condition
    )

    if merge_type == "update_all":
        merge_builder.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    elif merge_type == "specific_columns" and merge_columns:
        update_set = {c: F.col(f"source.{c}") for c in merge_columns.get("update", [])}
        insert_vals = {c: F.col(f"source.{c}") for c in merge_columns.get("insert", [])}
        merge_builder.whenMatchedUpdate(set=update_set).whenNotMatchedInsert(values=insert_vals).execute()

    return source_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Validation Functions**
# 
# Uses `metadata.expectation_store.gx_method` to dynamically resolve GX expectation classes via `getattr(gxe, gx_method)`.
# 
# No hardcoded expectation mapping - the gx_method column stores the actual class name.

# CELL ********************

def get_expectation_class(gx_method: str):
    """
    Dynamically resolve GX expectation class from method name.
    gx_method comes from metadata.expectation_store.gx_method column.
    """
    return getattr(gxe, gx_method)


def build_expectation(gx_method: str, column_name: str = None, validation_params: dict = None):
    """
    Build a GX expectation instance from metadata.
    
    Args:
        gx_method: Class name from metadata.expectation_store.gx_method
        column_name: Column to validate (from instructions.validations.column_name)
        validation_params: Additional params (from instructions.validations.validation_params JSON)
    """
    exp_class = get_expectation_class(gx_method)

    # Build kwargs based on expectation type
    kwargs = {}
    if column_name:
        # Check if it's a compound columns expectation
        if "Compound" in gx_method:
            kwargs["column_list"] = [column_name]
        else:
            kwargs["column"] = column_name

    # Merge any additional params from validation_params JSON
    if validation_params:
        kwargs.update(validation_params)

    return exp_class(**kwargs)


def run_validations(context, df, validation_rows: list, expectation_lookup: dict) -> dict:
    """
    Run GX validations based on joined instruction + expectation data.

    Args:
        context: GX context
        df: DataFrame to validate
        validation_rows: List of dicts from instructions.validations
        expectation_lookup: Dict mapping expectation_id -> {gx_method, ...}
                           (from load_expectation_store)

    Returns: dict with validation results per table
    """
    results = {}

    # Group validations by target_table
    by_table = {}
    for row in validation_rows:
        table = row["target_table"]
        if table not in by_table:
            by_table[table] = []
        by_table[table].append(row)

    for table_name, rows in by_table.items():
        # Build expectations from metadata
        expectations = []
        for row in rows:
            exp_meta = expectation_lookup.get(row["expectation_id"])
            if exp_meta:
                params = json.loads(row["validation_params"]) if row.get("validation_params") else {}
                exp = build_expectation(
                    gx_method=exp_meta["gx_method"],
                    column_name=row.get("column_name"),
                    validation_params=params
                )
                expectations.append(exp)

        # Create suite and validate
        suite = gx.ExpectationSuite(name=f"{table_name.replace('/', '_')}_suite")
        for exp in expectations:
            suite.add_expectation(exp)

        batch = context.get_batch(batch_parameters={"dataframe": df})
        results[table_name] = batch.validate(suite)

    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# **Metadata queries**
# 
# Functions to query the SQL metadata store using the native Fabric Spark SQL connector.
# 
# **Note**: Uses `mssql()` method with automatic Microsoft Entra ID authentication - no credentials needed.

# CELL ********************

def query_metadata_table(spark, schema_table: str) -> list:
    """
    Query a metadata table using the native Fabric SQL connector.
    Uses automatic Microsoft Entra ID authentication.
    
    Args:
        spark: SparkSession
        schema_table: Table in schema.table format (e.g., 'metadata.transform_store')
    
    Returns: List of row dicts
    """
    df = spark.read.option("url", METADATA_DB_URL).mssql(schema_table)
    return [row.asDict() for row in df.collect()]


def query_metadata_sql(spark, query: str) -> list:
    """
    Execute a custom SQL query against the metadata database.
    Uses JDBC format for custom queries since mssql() only takes table names.
    
    Args:
        spark: SparkSession
        query: SQL query to execute
    
    Returns: List of row dicts
    """
    df = (spark.read
        .format("jdbc")
        .option("url", METADATA_DB_URL)
        .option("query", query)
        .load())
    return [row.asDict() for row in df.collect()]


def load_source_store(spark) -> dict:
    """
    Load metadata.source_store as lookup dict by source_id.
    Used by ingestion notebook to get API connection details.
    """
    rows = query_metadata_table(spark, "metadata.source_store")
    return {row["source_id"]: row for row in rows}


def load_transform_store(spark) -> dict:
    """
    Load metadata.transform_store as lookup dict by transform_id.
    Used by execute_transform_pipeline() to resolve function_name.
    """
    rows = query_metadata_table(spark, "metadata.transform_store")
    return {row["transform_id"]: row for row in rows}


def load_expectation_store(spark) -> dict:
    """
    Load metadata.expectation_store as lookup dict by expectation_id.
    Used by run_validations() to resolve gx_method.
    """
    rows = query_metadata_table(spark, "metadata.expectation_store")
    return {row["expectation_id"]: row for row in rows}


def load_loading_store(spark) -> dict:
    """
    Load metadata.loading_store as lookup dict by loading_id.
    Used to resolve loading function_name.
    """
    rows = query_metadata_table(spark, "metadata.loading_store")
    return {row["loading_id"]: row for row in rows}


def load_log_store(spark) -> dict:
    """
    Load metadata.log_store as lookup dict by log_id.
    Used to resolve logging function_name.
    """
    rows = query_metadata_table(spark, "metadata.log_store")
    return {row["log_id"]: row for row in rows}


def get_active_instructions(spark, instruction_type: str, layer: str = None) -> list:
    """
    Get active instructions from the appropriate instruction table.
    
    Args:
        spark: SparkSession
        instruction_type: 'loading', 'transformations', 'validations', 'ingestion'
        layer: Optional filter by source_layer, target_layer, or dest_layer
    
    Returns: List of instruction row dicts
    """
    table = f"instructions.{instruction_type}"
    
    if layer:
        # Use SQL query for filtered results
        if instruction_type == "loading":
            query = f"SELECT * FROM {table} WHERE is_active = 1 AND target_layer = '{layer}'"
        elif instruction_type == "transformations":
            query = f"SELECT * FROM {table} WHERE is_active = 1 AND dest_layer = '{layer}'"
        elif instruction_type == "validations":
            query = f"SELECT * FROM {table} WHERE is_active = 1 AND target_layer = '{layer}'"
        else:
            query = f"SELECT * FROM {table} WHERE is_active = 1"
        return query_metadata_sql(spark, query)
    else:
        # Read full table and filter in Spark
        rows = query_metadata_table(spark, table)
        return [r for r in rows if r.get("is_active") == 1]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
