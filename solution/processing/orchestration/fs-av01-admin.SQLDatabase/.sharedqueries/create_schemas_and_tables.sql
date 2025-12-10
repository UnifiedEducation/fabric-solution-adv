-- ============================================================================
-- STEP 1: CREATE SCHEMAS
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'metadata')
    EXEC('CREATE SCHEMA metadata');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'instructions')
    EXEC('CREATE SCHEMA instructions');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'log')
    EXEC('CREATE SCHEMA log');
GO

-- ============================================================================
-- STEP 2: CREATE METADATA CATALOG TABLES
-- ============================================================================

-- Source Store: Registry of all data sources and their authentication details
CREATE TABLE metadata.source_store (
    source_id           INT PRIMARY KEY,
    source_name         VARCHAR(100) NOT NULL,          -- 'youtube_api', 'github_api'
    source_type         VARCHAR(50) NOT NULL,           -- 'rest_api', 'file', 'database'
    auth_method         VARCHAR(50),                    -- 'api_key', 'oauth', 'connection_string'
    key_vault_url       VARCHAR(500),                   -- AKV URL
    secret_name         VARCHAR(100),                   -- Secret name in AKV
    base_url            VARCHAR(500),                   -- Base URL for APIs
    description         VARCHAR(1000),
    created_date        DATETIME2 DEFAULT GETDATE(),
    modified_date       DATETIME2 DEFAULT GETDATE()
);
GO

-- Loading Store: Catalog of available loading functions
CREATE TABLE metadata.loading_store (
    loading_id          INT PRIMARY KEY,
    function_name       VARCHAR(100) NOT NULL,          -- 'load_json_to_delta', 'load_parquet_to_delta'
    description         VARCHAR(1000),                  -- When to use this function
    expected_params     JSON                            -- Parameters the function expects
);
GO

-- Transform Store: Catalog of available transform functions
CREATE TABLE metadata.transform_store (
    transform_id        INT PRIMARY KEY,
    function_name       VARCHAR(100) NOT NULL,          -- 'dedupe_by_window', 'filter_nulls', 'generate_surrogate_key'
    description         VARCHAR(1000),                  -- When to use this function
    expected_params     JSON                            -- Parameters the function expects
);
GO

-- Expectation Store: Catalog of available GX expectations
CREATE TABLE metadata.expectation_store (
    expectation_id      INT PRIMARY KEY,
    expectation_name    VARCHAR(100) NOT NULL,          -- 'not_null', 'unique', 'value_range'
    gx_method           VARCHAR(100) NOT NULL,          -- Actual GX method name
    description         VARCHAR(1000),
    expected_params     JSON                            -- Parameters the expectation expects
);
GO

-- Log Store: Catalog of available logging functions
CREATE TABLE metadata.log_store (
    log_id              INT PRIMARY KEY,
    function_name       VARCHAR(100) NOT NULL,          -- 'log_standard', 'log_verbose', 'log_minimal'
    description         VARCHAR(1000),
    expected_params     JSON
);
GO

-- ============================================================================
-- STEP 3: CREATE INSTRUCTION TABLES
-- ============================================================================

-- Ingestion Instructions: What to ingest from external sources
CREATE TABLE instructions.ingestion (
    ingestion_id        INT PRIMARY KEY,
    source_id           INT NOT NULL,
    endpoint_path       VARCHAR(500),                   -- API endpoint or file pattern
    landing_path        VARCHAR(500) NOT NULL,          -- Where to write raw data
    request_params      JSON,                           -- API request parameters
    is_active           BIT DEFAULT 1,
    log_function_id     INT,
    created_date        DATETIME2 DEFAULT GETDATE(),
    modified_date       DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_ingestion_source FOREIGN KEY (source_id)
        REFERENCES metadata.source_store(source_id),
    CONSTRAINT FK_ingestion_log FOREIGN KEY (log_function_id)
        REFERENCES metadata.log_store(log_id)
);
GO

-- Loading Instructions: What to load from Raw to Bronze
CREATE TABLE instructions.loading (
    loading_instr_id    INT PRIMARY KEY,
    loading_id          INT NOT NULL,
    source_path         VARCHAR(500) NOT NULL,          -- Raw file location (Files area)
    source_layer        VARCHAR(20) NOT NULL DEFAULT 'raw', -- 'raw' for Files area
    target_table        VARCHAR(200) NOT NULL,          -- e.g., 'youtube/channel'
    target_layer        VARCHAR(20) NOT NULL DEFAULT 'bronze', -- Layer for Variable Library lookup
    key_columns         JSON NOT NULL,                  -- e.g., ["channel_id"]
    load_params         JSON,                           -- Additional params for the function
    merge_condition     VARCHAR(500),                   -- e.g., 'target.video_id = source.video_id'
    merge_type          VARCHAR(20) DEFAULT 'update_all', -- 'update_all', 'specific_columns'
    merge_columns       JSON,                           -- Specific columns if merge_type = 'specific_columns'
    is_active           BIT DEFAULT 1,
    log_function_id     INT,
    created_date        DATETIME2 DEFAULT GETDATE(),
    modified_date       DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_loading_store FOREIGN KEY (loading_id)
        REFERENCES metadata.loading_store(loading_id),
    CONSTRAINT FK_loading_log FOREIGN KEY (log_function_id)
        REFERENCES metadata.log_store(log_id)
);
GO

-- Transformation Instructions: What transforms to apply (Bronze->Silver, Silver->Gold)
CREATE TABLE instructions.transformations (
    transform_instr_id  INT PRIMARY KEY,
    source_table        VARCHAR(200) NOT NULL,          -- e.g., 'youtube/channel'
    source_layer        VARCHAR(20) NOT NULL,           -- 'bronze', 'silver' - for Variable Library lookup
    dest_table          VARCHAR(200) NOT NULL,          -- e.g., 'youtube/channel_stats'
    dest_layer          VARCHAR(20) NOT NULL,           -- 'silver', 'gold' - for Variable Library lookup
    transform_pipeline  JSON NOT NULL,                  -- Ordered array: [1, 4, 8]
    transform_params    JSON,                           -- Params per transform in pipeline
    merge_condition     VARCHAR(500),                   -- Merge condition for destination
    merge_type          VARCHAR(20) DEFAULT 'update_all',
    merge_columns       JSON,
    is_active           BIT DEFAULT 1,
    log_function_id     INT,
    created_date        DATETIME2 DEFAULT GETDATE(),
    modified_date       DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_transform_log FOREIGN KEY (log_function_id)
        REFERENCES metadata.log_store(log_id)
);
GO

-- Validation Instructions: What validations to run on tables
CREATE TABLE instructions.validations (
    validation_instr_id INT PRIMARY KEY,
    target_table        VARCHAR(200) NOT NULL,          -- e.g., 'marketing/channels'
    target_layer        VARCHAR(20) NOT NULL,           -- 'gold' typically - for Variable Library lookup
    expectation_id      INT NOT NULL,
    column_name         VARCHAR(100),                   -- Column to validate (null for table-level)
    validation_params   JSON,                           -- e.g., {"min_value": 0, "max_value": 100}
    severity            VARCHAR(20) DEFAULT 'error',    -- 'error', 'warning'
    is_active           BIT DEFAULT 1,
    log_function_id     INT,
    created_date        DATETIME2 DEFAULT GETDATE(),
    modified_date       DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_validation_expectation FOREIGN KEY (expectation_id)
        REFERENCES metadata.expectation_store(expectation_id),
    CONSTRAINT FK_validation_log FOREIGN KEY (log_function_id)
        REFERENCES metadata.log_store(log_id)
);
GO

-- ============================================================================
-- STEP 4: CREATE LOG TABLES
-- ============================================================================

-- Pipeline Runs: Execution history of pipelines
CREATE TABLE log.pipeline_runs (
    run_id              BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name       VARCHAR(100) NOT NULL,
    started_at          DATETIME2 DEFAULT GETDATE(),
    completed_at        DATETIME2,
    status              VARCHAR(20) NOT NULL,           -- 'running', 'success', 'failed'
    records_processed   INT,
    error_message       NVARCHAR(MAX)
);
GO

-- Validation Results: Outcomes of validation runs
CREATE TABLE log.validation_results (
    result_id           BIGINT IDENTITY(1,1) PRIMARY KEY,
    run_id              BIGINT,
    validation_instr_id INT,
    expectation_type    VARCHAR(100),
    column_name         VARCHAR(100),
    passed              BIT NOT NULL,
    observed_value      JSON,
    executed_at         DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT FK_validation_run FOREIGN KEY (run_id)
        REFERENCES log.pipeline_runs(run_id)
);
GO

-- Metadata Changes: Audit trail for metadata table changes
CREATE TABLE log.metadata_changes (
    change_id           BIGINT IDENTITY(1,1) PRIMARY KEY,
    table_name          VARCHAR(100) NOT NULL,
    record_id           INT NOT NULL,
    change_type         VARCHAR(20) NOT NULL,           -- 'insert', 'update', 'delete'
    changed_by          VARCHAR(100) DEFAULT SYSTEM_USER,
    changed_at          DATETIME2 DEFAULT GETDATE(),
    old_values          JSON,
    new_values          JSON
);
GO



