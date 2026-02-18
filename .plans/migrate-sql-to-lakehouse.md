# Plan: Migrate Metadata Store from SQL Database to Lakehouse

## Context

Fabric SQL Database has a known issue (1170) preventing reliable capacity pause/resume, blocking the ability to turn infrastructure off and on quickly. Migrating the metadata store to a Lakehouse eliminates the JDBC/.mssql() connector layer entirely — Delta tables are just files on OneLake and survive capacity pause/resume with zero issues. This also simplifies the architecture by removing an entire Fabric item type.

## Approach

Create a dedicated **admin Lakehouse** (`lh_av01_admin`) with schemas `metadata`, `instructions`, and `log`. Replace all JDBC reads/writes with native Spark `spark.sql()` / `.saveAsTable()`. Drop the audit trail (`log.metadata_changes` + 9 triggers) — metadata changes infrequently and is source-controlled.

## Housekeeping

The seed schemas in `nb-av01-init-sql-database` are missing columns added via the migration script (`handler_function`, `pipeline_name`, `notebook_name`). These will be incorporated into the new Delta table definitions.

---

## Step 1: Variable Library — remove SQL vars, add admin LH

**Files:**
- `solution/processing/vl-av01-variables.VariableLibrary/variables.json`
- `solution/processing/vl-av01-variables.VariableLibrary/valueSets/DEV.json`
- `solution/processing/vl-av01-variables.VariableLibrary/valueSets/TEST.json`
- `solution/processing/vl-av01-variables.VariableLibrary/valueSets/PROD.json`

**Changes:**
- Remove `METADATA_SERVER` and `METADATA_DB` entries from `variables.json` and all valueSets
- Add `ADMIN_LH_NAME` with value `lh_av01_admin` to `variables.json` (default)
- No environment-specific overrides needed for `ADMIN_LH_NAME` (same name across environments, like the other LH names)

---

## Step 2: Lakehouse creation — add admin lakehouse

**File:** `solution/processing/notebooks/setup/nb-av01-lhcreate-all.Notebook/notebook-content.py`

**Changes:** Add an `"admin"` entry to the `LAKEHOUSE_METADATA` dict with:
- `name_variable`: `"ADMIN_LH_NAME"`
- `schemas`: `["metadata", "instructions", "log"]`
- `tables`: All 12 tables (dropping `log.metadata_changes`) as Spark SQL DDL

Table DDL (columns derived from existing StructType schemas + migration columns):

| Table | Columns |
|-------|---------|
| `metadata.source_store` | source_id INT, source_name STRING, source_type STRING, auth_method STRING, key_vault_url STRING, secret_name STRING, base_url STRING, handler_function STRING, description STRING, created_date TIMESTAMP, modified_date TIMESTAMP |
| `metadata.loading_store` | loading_id INT, function_name STRING, description STRING, expected_params STRING |
| `metadata.transform_store` | transform_id INT, function_name STRING, description STRING, expected_params STRING |
| `metadata.expectation_store` | expectation_id INT, expectation_name STRING, gx_method STRING, description STRING, expected_params STRING |
| `metadata.log_store` | log_id INT, function_name STRING, description STRING, expected_params STRING |
| `metadata.column_mappings` | mapping_id STRING, column_order INT, source_column STRING, target_column STRING, data_type STRING, description STRING |
| `instructions.ingestion` | ingestion_id INT, source_id INT, endpoint_path STRING, landing_path STRING, request_params STRING, is_active BOOLEAN, log_function_id INT, pipeline_name STRING, notebook_name STRING, created_date TIMESTAMP, modified_date TIMESTAMP |
| `instructions.loading` | loading_instr_id INT, loading_id INT, source_path STRING, source_layer STRING, target_table STRING, target_layer STRING, key_columns STRING, load_params STRING, merge_condition STRING, merge_type STRING NOT NULL, merge_columns STRING, is_active BOOLEAN, log_function_id INT, pipeline_name STRING, notebook_name STRING, created_date TIMESTAMP, modified_date TIMESTAMP |
| `instructions.transformations` | transform_instr_id INT, source_table STRING, source_layer STRING, dest_table STRING, dest_layer STRING, transform_pipeline STRING, transform_params STRING, merge_condition STRING, merge_type STRING NOT NULL, merge_columns STRING, is_active BOOLEAN, log_function_id INT, pipeline_name STRING, notebook_name STRING, created_date TIMESTAMP, modified_date TIMESTAMP |
| `instructions.validations` | validation_instr_id INT, target_table STRING, target_layer STRING, expectation_id INT, column_name STRING, validation_params STRING, severity STRING, is_active BOOLEAN, log_function_id INT, pipeline_name STRING, notebook_name STRING, created_date TIMESTAMP, modified_date TIMESTAMP |
| `log.pipeline_runs` | run_id LONG, pipeline_name STRING, started_at TIMESTAMP, completed_at TIMESTAMP, status STRING, records_processed INT, error_message STRING, action_type STRING, source_name STRING, instruction_detail STRING, notebook_name STRING |
| `log.validation_results` | result_id LONG, run_id LONG, validation_instr_id INT, expectation_type STRING, column_name STRING, passed BOOLEAN, observed_value STRING, executed_at TIMESTAMP, lakehouse_name STRING, schema_name STRING, table_name STRING |

---

## Step 3: Generic functions — replace JDBC with native Spark

**File:** `solution/processing/notebooks/utils/nb-av01-generic-functions.Notebook/notebook-content.py`

### 3a: Connection setup cell (lines 92-133)

**Remove:**
- `import com.microsoft.sqlserver.jdbc.spark` (line 95)
- `METADATA_DB_URL = None` (line 98)
- `set_metadata_db_url()` function (lines 115-126)

**Add:**
```python
# Admin Lakehouse connection - set via set_admin_lakehouse() before use
ADMIN_LH_WORKSPACE = None
ADMIN_LH_NAME = None

def set_admin_lakehouse(workspace: str, lakehouse: str):
    """
    Configure the admin lakehouse for metadata reads/writes.
    Call this once at notebook startup.
    """
    global ADMIN_LH_WORKSPACE, ADMIN_LH_NAME
    ADMIN_LH_WORKSPACE = workspace
    ADMIN_LH_NAME = lakehouse
```

### 3b: Markdown header (lines 1031-1037)

Update from "SQL metadata store" / "mssql()" references to "admin lakehouse" / "Delta" references.

### 3c: `query_metadata_table()` (lines 1041-1053)

**Before:**
```python
df = spark.read.option("url", METADATA_DB_URL).mssql(schema_table)
return [row.asDict() for row in df.collect()]
```

**After:**
```python
qualified = f"`{ADMIN_LH_WORKSPACE}`.`{ADMIN_LH_NAME}`.{schema_table}"
df = spark.sql(f"SELECT * FROM {qualified}")
return [row.asDict() for row in df.collect()]
```

All `load_*_store()` and `get_active_instructions()` functions (lines 1056-1144) are **unchanged** — they call `query_metadata_table()` internally.

### 3d: `log_standard()` (lines 329-380)

**Key changes:**
- Replace `.mssql()` write with `.saveAsTable()` append
- Generate `run_id` via `COALESCE(MAX(run_id), 0) + 1` before writing (Delta has no IDENTITY)

```python
# Get next run_id
qualified = f"`{ADMIN_LH_WORKSPACE}`.`{ADMIN_LH_NAME}`.log.pipeline_runs"
max_id = spark.sql(f"SELECT COALESCE(MAX(run_id), 0) FROM {qualified}").collect()[0][0]
new_run_id = max_id + 1

# ... build log_df with new_run_id instead of 0 ...

log_df.write.mode("append").saveAsTable(qualified)
```

### 3e: `log_validation()` (lines 383-464)

Same pattern — generate `result_id` via MAX+1, replace `.mssql()` with `.saveAsTable()`.

```python
qualified = f"`{ADMIN_LH_WORKSPACE}`.`{ADMIN_LH_NAME}`.log.validation_results"
max_id = spark.sql(f"SELECT COALESCE(MAX(result_id), 0) FROM {qualified}").collect()[0][0]
# Assign sequential IDs starting from max_id + 1
# ... build results_data with incrementing result_id ...
log_df.write.mode("append").saveAsTable(qualified)
```

---

## Step 4: Seed notebook — rewrite for Delta

**File:** `solution/processing/notebooks/setup/nb-av01-init-sql-database.Notebook/notebook-content.py`

**Rename:** Directory from `nb-av01-init-sql-database.Notebook` to `nb-av01-init-metadata.Notebook`

**Changes:**

### 4a: Header markdown
- Update title, purpose, and dependency description to reference Lakehouse instead of SQL

### 4b: Setup cell (lines 34-63)
- Keep `%run nb-av01-generic-functions`
- Replace `set_metadata_db_url(server=..., database=...)` with `set_admin_lakehouse(workspace=..., lakehouse=...)`
- Reference `variables.LH_WORKSPACE_NAME` and `variables.ADMIN_LH_NAME` instead of `variables.METADATA_SERVER` / `variables.METADATA_DB`

### 4c: Seed schemas — add missing migration columns
- `source_store_schema`: add `StructField("handler_function", StringType(), True)`
- `source_store_data`: add `"ingest_youtube"` to the tuple
- `ingestion_schema`: add `pipeline_name` and `notebook_name` fields
- `loading_schema`: add `pipeline_name` and `notebook_name` fields
- `transformations_schema`: add `pipeline_name` and `notebook_name` fields
- `validations_schema`: add `pipeline_name` and `notebook_name` fields
- All seed data tuples: add `None, None` for the new pipeline_name/notebook_name columns

### 4d: `write_seed_table()` function (lines 481-500)
```python
def write_seed_table(table_name: str, schema: StructType, data: list) -> int:
    if not data:
        print(f"  Skipping {table_name} - no data to write")
        return 0
    qualified = f"`{lh_workspace_name}`.`{admin_lh_name}`.{table_name}"
    df = spark.createDataFrame(data, schema)
    df.write.mode("append").saveAsTable(qualified)
    print(f"  Wrote {len(data)} rows to {table_name}")
    return len(data)
```

### 4e: `table_has_data()` function (lines 503-506)
```python
def table_has_data(table_name: str) -> bool:
    qualified = f"`{lh_workspace_name}`.`{admin_lh_name}`.{table_name}"
    return spark.sql(f"SELECT 1 FROM {qualified} LIMIT 1").count() > 0
```

---

## Step 5: Pipeline notebooks — update setup cells

Each notebook replaces 3-4 lines in its "Load Metadata" cell. The metadata loader calls after that are unchanged.

### nb-av01-0-ingest-api (lines 92-96 + debug cell at 117-118)

**Before:**
```python
set_metadata_db_url(
    server=variables.METADATA_SERVER,
    database=variables.METADATA_DB
)
```

**After:**
```python
set_admin_lakehouse(
    workspace=variables.LH_WORKSPACE_NAME,
    lakehouse=variables.ADMIN_LH_NAME
)
```

Also update the debug cell (line 117-118) that directly uses `METADATA_DB_URL` and `.mssql()`:
```python
# Before: df = spark.read.option("url", METADATA_DB_URL).mssql("metadata.source_store")
# After:  df = spark.sql(f"SELECT * FROM `{ADMIN_LH_WORKSPACE}`.`{ADMIN_LH_NAME}`.metadata.source_store")
df.printSchema()
```

### nb-av01-1-load (lines 67-70)
Same `set_metadata_db_url` → `set_admin_lakehouse` swap.

### nb-av01-2-clean (lines 67-70)
Same swap.

### nb-av01-3-model (lines 72-75)
Same swap.

### nb-av01-4-validate (lines 62-65)
Same swap.

**Files:**
- `solution/processing/notebooks/nb-av01-0-ingest-api.Notebook/notebook-content.py`
- `solution/processing/notebooks/nb-av01-1-load.Notebook/notebook-content.py`
- `solution/processing/notebooks/nb-av01-2-clean.Notebook/notebook-content.py`
- `solution/processing/notebooks/nb-av01-3-model.Notebook/notebook-content.py`
- `solution/processing/notebooks/nb-av01-4-validate.Notebook/notebook-content.py`

---

## Step 6: Delete SQL Database artifacts

**Delete entire directory:** `solution/processing/orchestration/fs-av01-admin.SQLDatabase/`

This removes:
- 27 files: DDL scripts, triggers, indexes, security roles, migration scripts, .sqlproj, .platform, .gitignore

---

## Files unchanged

- `nb-av01-api-tools-youtube.Notebook` — no SQL database references
- `nb-av01-unit-tests.Notebook` — tests pure functions, no SQL dependencies
- All Bronze/Silver/Gold lakehouse definitions — unaffected

---

## Summary of changes

| # | File | Nature |
|---|------|--------|
| 1 | `vl-av01-variables/variables.json` | Remove 2 vars, add 1 |
| 2 | `vl-av01-variables/valueSets/DEV.json` | Remove 2 overrides |
| 3 | `vl-av01-variables/valueSets/TEST.json` | Remove 2 overrides |
| 4 | `vl-av01-variables/valueSets/PROD.json` | Remove 2 overrides |
| 5 | `nb-av01-lhcreate-all` | Add admin lakehouse + 12 tables |
| 6 | `nb-av01-generic-functions` | Replace JDBC with Spark SQL (3 functions + setup) |
| 7 | `nb-av01-init-sql-database` → `nb-av01-init-metadata` | Rename + rewrite for Delta |
| 8-12 | `nb-av01-{0,1,2,3,4}-*` | 3-line swap in each (set_metadata_db_url → set_admin_lakehouse) |
| 13 | `fs-av01-admin.SQLDatabase/` | Delete entire directory (27 files) |

**Total: 13 file changes (4 variable lib + 7 notebooks + 1 rename + 1 directory delete)**

---

## Key technical notes

- **`is_active` filtering**: Current code checks `r.get("is_active") == 1`. This works with Delta (BooleanType returns True/False) because `True == 1` in Python. No change needed.
- **IDENTITY columns**: Delta has no auto-increment. Log tables use `MAX(id) + 1` pattern. Safe because pipelines run sequentially.
- **Four-part naming**: Lakehouse tables use `` `workspace`.`lakehouse`.schema.table `` — consistent with existing lhcreate pattern.
- **`saveAsTable` with append**: Respects existing Delta table schema. Creates table if not exists (but lhcreate should run first).

---

## Verification

After deployment to a Fabric workspace:
1. Run `nb-av01-lhcreate-all` — verify `lh_av01_admin` lakehouse created with 3 schemas and 12 tables
2. Run `nb-av01-init-metadata` — verify seed data written to all 10 metadata/instruction tables
3. Run `nb-av01-0-ingest-api` — verify metadata reads work and ingestion logs to `log.pipeline_runs`
4. Run `nb-av01-1-load` through `nb-av01-4-validate` — verify full pipeline completes
5. Query `log.pipeline_runs` and `log.validation_results` via the Lakehouse SQL analytics endpoint to confirm log data
6. Pause and resume Fabric capacity — verify metadata tables survive with no issues
