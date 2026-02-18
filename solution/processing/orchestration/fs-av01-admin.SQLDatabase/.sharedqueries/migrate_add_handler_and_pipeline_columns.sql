-- ============================================================================
-- Migration: Add handler_function, pipeline_name, notebook_name columns
-- ============================================================================
-- Run this on existing DEV, TEST, and PROD databases to align with the
-- refactored notebook code (feature-repo-tidy branch).
--
-- Changes:
--   1. metadata.source_store: Add handler_function column
--   2. instructions.ingestion: Add pipeline_name, notebook_name columns
--   3. instructions.loading: Add pipeline_name, notebook_name columns;
--      make merge_type NOT NULL
--   4. instructions.transformations: Add pipeline_name, notebook_name columns;
--      make merge_type NOT NULL
--   5. instructions.validations: Add pipeline_name, notebook_name columns
--
-- Safe to re-run (uses IF NOT EXISTS / conditional checks).
-- ============================================================================

-- ============================================================================
-- 1. metadata.source_store: Add handler_function
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'metadata' AND TABLE_NAME = 'source_store'
    AND COLUMN_NAME = 'handler_function'
)
BEGIN
    ALTER TABLE [metadata].[source_store]
        ADD [handler_function] VARCHAR(100) NULL;
    PRINT 'Added handler_function to metadata.source_store';
END
GO

-- Populate handler_function for existing YouTube source
UPDATE [metadata].[source_store]
SET [handler_function] = 'ingest_youtube'
WHERE [source_id] = 1 AND [handler_function] IS NULL;
GO

-- ============================================================================
-- 2. instructions.ingestion: Add pipeline_name, notebook_name
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'instructions' AND TABLE_NAME = 'ingestion'
    AND COLUMN_NAME = 'pipeline_name'
)
BEGIN
    ALTER TABLE [instructions].[ingestion]
        ADD [pipeline_name] VARCHAR(100) NULL;
    PRINT 'Added pipeline_name to instructions.ingestion';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'instructions' AND TABLE_NAME = 'ingestion'
    AND COLUMN_NAME = 'notebook_name'
)
BEGIN
    ALTER TABLE [instructions].[ingestion]
        ADD [notebook_name] VARCHAR(100) NULL;
    PRINT 'Added notebook_name to instructions.ingestion';
END
GO

UPDATE [instructions].[ingestion]
SET [pipeline_name] = 'data_pipeline', [notebook_name] = 'nb-av01-0-ingest-api'
WHERE [pipeline_name] IS NULL;
GO

-- ============================================================================
-- 3. instructions.loading: Add pipeline_name, notebook_name; NOT NULL merge_type
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'instructions' AND TABLE_NAME = 'loading'
    AND COLUMN_NAME = 'pipeline_name'
)
BEGIN
    ALTER TABLE [instructions].[loading]
        ADD [pipeline_name] VARCHAR(100) NULL;
    PRINT 'Added pipeline_name to instructions.loading';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'instructions' AND TABLE_NAME = 'loading'
    AND COLUMN_NAME = 'notebook_name'
)
BEGIN
    ALTER TABLE [instructions].[loading]
        ADD [notebook_name] VARCHAR(100) NULL;
    PRINT 'Added notebook_name to instructions.loading';
END
GO

UPDATE [instructions].[loading]
SET [pipeline_name] = 'data_pipeline', [notebook_name] = 'nb-av01-1-load'
WHERE [pipeline_name] IS NULL;
GO

-- Ensure merge_type has no NULLs before making NOT NULL
UPDATE [instructions].[loading]
SET [merge_type] = 'update_all'
WHERE [merge_type] IS NULL;
GO

ALTER TABLE [instructions].[loading]
    ALTER COLUMN [merge_type] VARCHAR(20) NOT NULL;
PRINT 'Made merge_type NOT NULL on instructions.loading';
GO

-- ============================================================================
-- 4. instructions.transformations: Add pipeline_name, notebook_name; NOT NULL merge_type
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'instructions' AND TABLE_NAME = 'transformations'
    AND COLUMN_NAME = 'pipeline_name'
)
BEGIN
    ALTER TABLE [instructions].[transformations]
        ADD [pipeline_name] VARCHAR(100) NULL;
    PRINT 'Added pipeline_name to instructions.transformations';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'instructions' AND TABLE_NAME = 'transformations'
    AND COLUMN_NAME = 'notebook_name'
)
BEGIN
    ALTER TABLE [instructions].[transformations]
        ADD [notebook_name] VARCHAR(100) NULL;
    PRINT 'Added notebook_name to instructions.transformations';
END
GO

-- Silver-layer transforms -> nb-av01-2-clean
UPDATE [instructions].[transformations]
SET [pipeline_name] = 'data_pipeline', [notebook_name] = 'nb-av01-2-clean'
WHERE [dest_layer] = 'silver' AND [pipeline_name] IS NULL;
GO

-- Gold-layer transforms -> nb-av01-3-model
UPDATE [instructions].[transformations]
SET [pipeline_name] = 'data_pipeline', [notebook_name] = 'nb-av01-3-model'
WHERE [dest_layer] = 'gold' AND [pipeline_name] IS NULL;
GO

-- Ensure merge_type has no NULLs before making NOT NULL
UPDATE [instructions].[transformations]
SET [merge_type] = 'update_all'
WHERE [merge_type] IS NULL;
GO

ALTER TABLE [instructions].[transformations]
    ALTER COLUMN [merge_type] VARCHAR(20) NOT NULL;
PRINT 'Made merge_type NOT NULL on instructions.transformations';
GO

-- ============================================================================
-- 5. instructions.validations: Add pipeline_name, notebook_name
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'instructions' AND TABLE_NAME = 'validations'
    AND COLUMN_NAME = 'pipeline_name'
)
BEGIN
    ALTER TABLE [instructions].[validations]
        ADD [pipeline_name] VARCHAR(100) NULL;
    PRINT 'Added pipeline_name to instructions.validations';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'instructions' AND TABLE_NAME = 'validations'
    AND COLUMN_NAME = 'notebook_name'
)
BEGIN
    ALTER TABLE [instructions].[validations]
        ADD [notebook_name] VARCHAR(100) NULL;
    PRINT 'Added notebook_name to instructions.validations';
END
GO

UPDATE [instructions].[validations]
SET [pipeline_name] = 'data_pipeline', [notebook_name] = 'nb-av01-4-validate'
WHERE [pipeline_name] IS NULL;
GO

-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================
PRINT 'Migration complete: handler_function, pipeline_name, notebook_name columns added.';
GO
