
-- ============================================================================
-- CREATE INDEXES FOR PERFORMANCE
-- ============================================================================

-- Indexes on instruction tables for common queries
CREATE INDEX IX_ingestion_source ON instructions.ingestion(source_id, is_active);
CREATE INDEX IX_loading_active ON instructions.loading(is_active);
CREATE INDEX IX_transformations_active ON instructions.transformations(is_active);
CREATE INDEX IX_validations_table ON instructions.validations(target_table, is_active);

-- Indexes on log tables for querying
CREATE INDEX IX_pipeline_runs_status ON log.pipeline_runs(status, started_at);
CREATE INDEX IX_validation_results_run ON log.validation_results(run_id);
CREATE INDEX IX_metadata_changes_table ON log.metadata_changes(table_name, changed_at);
GO