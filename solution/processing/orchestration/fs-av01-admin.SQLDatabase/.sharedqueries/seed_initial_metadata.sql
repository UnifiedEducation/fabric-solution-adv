-- ============================================================================
-- PRJ106 Metadata Framework - Seed Initial Data
-- ============================================================================
-- This script populates the metadata store with data based on the actual
-- intermediate project notebooks (nb-int-0 through nb-int-4).
--
-- Run this AFTER 01_create_schemas_and_tables.sql
-- ============================================================================

-- ============================================================================
-- STEP 1: SEED LOGGING FUNCTIONS
-- ============================================================================

INSERT INTO metadata.log_store (log_id, function_name, description, expected_params)
VALUES
(1, 'log_standard', 'Standard logging - records start, end, row counts, and status',
    '{"params": ["pipeline_name", "notebook_name", "status", "rows_processed"]}'),
(2, 'log_validation', 'Validation logging - one row per expectation result with GX metadata',
    '{"params": ["validation_result"]}');
GO

-- ============================================================================
-- STEP 2: SEED SOURCE STORE
-- ============================================================================

INSERT INTO metadata.source_store (source_id, source_name, source_type, auth_method, key_vault_url, secret_name, base_url, handler_function, description)
VALUES
(1, 'youtube_api', 'rest_api', 'api_key',
    'https://av01-akv-restapi-keys.vault.azure.net/',
    'data-v3-api-key',
    'https://www.googleapis.com/youtube/v3',
    'ingest_youtube',
    'YouTube Data API v3 - Channel stats, videos, playlists');
GO

-- ============================================================================
-- STEP 3: SEED LOADING FUNCTIONS
-- ============================================================================

INSERT INTO metadata.loading_store (loading_id, function_name, description, expected_params)
VALUES
(1, 'load_json_to_delta',
    'Load JSON files from Raw zone to Delta table with column mapping and MERGE',
    '{"params": ["source_path", "target_table", "key_columns", "column_mapping_id"]}');
GO

-- ============================================================================
-- STEP 4: SEED TRANSFORM FUNCTIONS
-- ============================================================================
-- Only the transforms actually used in the notebooks

INSERT INTO metadata.transform_store (transform_id, function_name, description, expected_params)
VALUES
(1, 'filter_nulls',
    'Remove rows where specified columns are null',
    '{"params": ["columns"]}'),
(2, 'dedupe_by_window',
    'Deduplicate using window function - keeps most recent by order column',
    '{"params": ["partition_cols", "order_col", "order_desc"]}'),
(3, 'rename_columns',
    'Rename columns according to mapping',
    '{"params": ["column_mapping"]}'),
(4, 'add_literal_columns',
    'Add columns with literal/static values',
    '{"params": ["columns"]}'),
(5, 'generate_surrogate_key',
    'Generate surrogate key using row_number over window, starting from max existing ID',
    '{"params": ["key_column_name", "order_by_col", "max_from_table"]}'),
(6, 'lookup_join',
    'Join to lookup/dimension table to get surrogate key or other columns',
    '{"params": ["lookup_table", "source_key", "lookup_key", "select_cols"]}');
GO

-- ============================================================================
-- STEP 5: SEED GX EXPECTATIONS
-- ============================================================================
-- Based on nb_int_setup_gx.ipynb actual expectations

INSERT INTO metadata.expectation_store (expectation_id, expectation_name, gx_method, description, expected_params)
VALUES
(1, 'not_null',
    'ExpectColumnValuesToNotBeNull',
    'Validate that column contains no null values',
    '{"params": ["column"]}'),
(2, 'unique',
    'ExpectColumnValuesToBeUnique',
    'Validate that column contains only unique values',
    '{"params": ["column"]}'),
(3, 'value_in_set',
    'ExpectColumnValuesToBeInSet',
    'Validate that column values are within a defined set',
    '{"params": ["column", "value_set"]}'),
(4, 'values_increasing',
    'ExpectColumnValuesToBeIncreasing',
    'Validate that column values are strictly increasing',
    '{"params": ["column"]}'),
(5, 'compound_unique',
    'ExpectCompoundColumnsToBeUnique',
    'Validate that combination of columns is unique',
    '{"params": ["column_list"]}'),
(6, 'is_null',
    'ExpectColumnValuesToBeNull',
    'Validate that column contains only null values',
    '{"params": ["column"]}');
GO

-- ============================================================================
-- STEP 6: SEED INGESTION INSTRUCTIONS
-- ============================================================================
-- From nb-int-0-ingest-youtube.ipynb METADATA dict

INSERT INTO instructions.ingestion (ingestion_id, source_id, endpoint_path, landing_path, request_params, is_active, log_function_id, pipeline_name, notebook_name)
VALUES
(1, 1, '/channels', 'youtube_data_v3/channels/',
    '{"part": "snippet,statistics,contentDetails", "id": "UCrvoIYkzS-RvCEb0x7wfmwQ"}',
    1, 1, 'data_pipeline', 'nb-av01-0-ingest-api'),
(2, 1, '/playlistItems', 'youtube_data_v3/playlistItems/',
    '{"part": "snippet", "maxResults": 50, "playlistId": "UUrvoIYkzS-RvCEb0x7wfmwQ"}',
    1, 1, 'data_pipeline', 'nb-av01-0-ingest-api'),
(3, 1, '/videos', 'youtube_data_v3/videos/',
    '{"part": "statistics", "maxResults": 50}',
    1, 1, 'data_pipeline', 'nb-av01-0-ingest-api');
GO

-- ============================================================================
-- STEP 7: SEED LOADING INSTRUCTIONS (Raw -> Bronze)
-- ============================================================================
-- From nb-int-1-load-youtube.ipynb

INSERT INTO instructions.loading (loading_instr_id, loading_id, source_path, source_layer, target_table, target_layer, key_columns, load_params, merge_condition, merge_type, merge_columns, is_active, log_function_id, pipeline_name, notebook_name)
VALUES
-- Channel: merge on channel_id + date
(1, 1, 'Files/youtube_data_v3/channels/', 'raw', 'youtube/channel', 'bronze',
    '["channel_id"]',
    '{"column_mapping_id": "youtube_channels"}',
    'target.channel_id = source.channel_id AND to_date(target.loading_TS) = to_date(source.loading_TS)',
    'update_all', NULL, 1, 1, 'data_pipeline', 'nb-av01-1-load'),

-- Playlist Items: merge on video_id only
(2, 1, 'Files/youtube_data_v3/playlistItems/', 'raw', 'youtube/playlist_items', 'bronze',
    '["video_id"]',
    '{"column_mapping_id": "youtube_playlist_items"}',
    'target.video_id = source.video_id',
    'update_all', NULL, 1, 1, 'data_pipeline', 'nb-av01-1-load'),

-- Videos: merge on video_id only
(3, 1, 'Files/youtube_data_v3/videos/', 'raw', 'youtube/videos', 'bronze',
    '["video_id"]',
    '{"column_mapping_id": "youtube_videos"}',
    'target.video_id = source.video_id',
    'update_all', NULL, 1, 1, 'data_pipeline', 'nb-av01-1-load');
GO

-- ============================================================================
-- STEP 8: SEED TRANSFORMATION INSTRUCTIONS (Bronze -> Silver)
-- ============================================================================
-- From nb-int-2-clean-youtube.ipynb

INSERT INTO instructions.transformations (transform_instr_id, source_table, source_layer, dest_table, dest_layer, transform_pipeline, transform_params, merge_condition, merge_type, merge_columns, is_active, log_function_id, pipeline_name, notebook_name)
VALUES
-- Channel: filter nulls on channel_id, dedupe by channel_id+date
(1, 'youtube/channel', 'bronze', 'youtube/channel_stats', 'silver',
    '[1, 2]',
    '{"1": {"columns": ["channel_id"]}, "2": {"partition_cols": ["channel_id", "to_date(loading_TS)"], "order_col": "loading_TS", "order_desc": true}}',
    'target.channel_id = source.channel_id AND to_date(target.loading_TS) = to_date(source.loading_TS)',
    'update_all', NULL, 1, 1, 'data_pipeline', 'nb-av01-2-clean'),

-- Playlist Items -> Videos: filter nulls on video_id+video_title, dedupe by video_id
(2, 'youtube/playlist_items', 'bronze', 'youtube/videos', 'silver',
    '[1, 2]',
    '{"1": {"columns": ["video_id", "video_title"]}, "2": {"partition_cols": ["video_id"], "order_col": "loading_TS", "order_desc": true}}',
    'target.video_id = source.video_id',
    'update_all', NULL, 1, 1, 'data_pipeline', 'nb-av01-2-clean'),

-- Videos -> Video Statistics: filter nulls on video_id, dedupe by video_id+date
(3, 'youtube/videos', 'bronze', 'youtube/video_statistics', 'silver',
    '[1, 2]',
    '{"1": {"columns": ["video_id"]}, "2": {"partition_cols": ["video_id", "to_date(loading_TS)"], "order_col": "loading_TS", "order_desc": true}}',
    'target.video_id = source.video_id',
    'update_all', NULL, 1, 1, 'data_pipeline', 'nb-av01-2-clean');
GO

-- ============================================================================
-- STEP 9: SEED TRANSFORMATION INSTRUCTIONS (Silver -> Gold)
-- ============================================================================
-- From nb-int-3-model-youtube.ipynb

INSERT INTO instructions.transformations (transform_instr_id, source_table, source_layer, dest_table, dest_layer, transform_pipeline, transform_params, merge_condition, merge_type, merge_columns, is_active, log_function_id, pipeline_name, notebook_name)
VALUES
-- Channel Stats -> Marketing Channels: add literals, rename columns
(10, 'youtube/channel_stats', 'silver', 'marketing/channels', 'gold',
    '[4, 3]',
    '{"4": {"columns": {"channel_surrogate_id": 1, "channel_platform": "youtube"}}, "3": {"column_mapping": {"channel_name": "channel_account_name", "channel_description": "channel_account_description", "subscriber_count": "channel_total_subscribers", "video_count": "channel_total_assets", "view_count": "channel_total_views", "loading_TS": "modified_TS"}}}',
    'target.channel_surrogate_id = source.channel_surrogate_id AND to_date(target.modified_TS) = to_date(source.modified_TS)',
    'update_all', NULL, 1, 1, 'data_pipeline', 'nb-av01-3-model'),

-- Videos -> Marketing Assets: add literals, rename columns, generate surrogate key
(11, 'youtube/videos', 'silver', 'marketing/assets', 'gold',
    '[4, 3, 5]',
    '{"4": {"columns": {"channel_surrogate_id": 1}}, "3": {"column_mapping": {"video_id": "asset_natural_id", "video_title": "asset_title", "video_description": "asset_text", "video_publish_TS": "asset_publish_date", "loading_TS": "modified_TS"}}, "5": {"key_column_name": "asset_surrogate_id", "order_by_col": "asset_publish_date", "natural_key": "asset_natural_id", "max_from_table": "marketing/assets"}}',
    'target.asset_natural_id = source.asset_natural_id',
    'specific_columns',
    '{"update": ["asset_title", "asset_text", "asset_publish_date", "modified_TS"], "insert": ["asset_surrogate_id", "asset_natural_id", "channel_surrogate_id", "asset_title", "asset_text", "asset_publish_date", "modified_TS"]}',
    1, 1, 'data_pipeline', 'nb-av01-3-model'),

-- Video Statistics -> Marketing Asset Stats: lookup join, rename columns
(12, 'youtube/video_statistics', 'silver', 'marketing/asset_stats', 'gold',
    '[6, 3, 4]',
    '{"6": {"lookup_table": "marketing/assets", "source_key": "video_id", "lookup_key": "asset_natural_id", "select_cols": ["asset_surrogate_id"]}, "3": {"column_mapping": {"video_view_count": "asset_total_views", "video_like_count": "asset_total_likes", "video_comment_count": "asset_total_comments", "loading_TS": "modified_TS"}}, "4": {"columns": {"asset_total_impressions": null}}}',
    'target.asset_surrogate_id = source.asset_surrogate_id AND to_date(target.modified_TS) = to_date(source.modified_TS)',
    'specific_columns',
    '{"update": ["asset_total_views", "asset_total_impressions", "asset_total_likes", "asset_total_comments", "modified_TS"], "insert": ["asset_surrogate_id", "asset_total_views", "asset_total_impressions", "asset_total_likes", "asset_total_comments", "modified_TS"]}',
    1, 1, 'data_pipeline', 'nb-av01-3-model');
GO

-- ============================================================================
-- STEP 10: SEED VALIDATION INSTRUCTIONS
-- ============================================================================
-- From nb-int-4-validate-youtube.ipynb VALIDATION_METADATA
-- Validations run on Gold layer: marketing/channels, marketing/assets, marketing/asset_stats

INSERT INTO instructions.validations (validation_instr_id, target_table, target_layer, expectation_id, column_name, validation_params, severity, is_active, log_function_id, pipeline_name, notebook_name)
VALUES
-- marketing/channels validations (from nb_int_setup_gx.ipynb)
(1, 'marketing/channels', 'gold', 3, 'channel_surrogate_id', '{"value_set": [1]}', 'error', 1, 2, 'data_pipeline', 'nb-av01-4-validate'),
(2, 'marketing/channels', 'gold', 4, 'channel_total_views', NULL, 'error', 1, 2, 'data_pipeline', 'nb-av01-4-validate'),

-- marketing/assets validations (from nb_int_setup_gx.ipynb)
(3, 'marketing/assets', 'gold', 2, 'asset_surrogate_id', NULL, 'error', 1, 2, 'data_pipeline', 'nb-av01-4-validate'),
(4, 'marketing/assets', 'gold', 1, 'asset_natural_id', NULL, 'error', 1, 2, 'data_pipeline', 'nb-av01-4-validate'),
(5, 'marketing/assets', 'gold', 1, 'asset_publish_date', NULL, 'error', 1, 2, 'data_pipeline', 'nb-av01-4-validate'),
(6, 'marketing/assets', 'gold', 5, NULL, '{"column_list": ["asset_title", "asset_surrogate_id"]}', 'error', 1, 2, 'data_pipeline', 'nb-av01-4-validate'),

-- marketing/asset_stats validations (from nb_int_setup_gx.ipynb)
(7, 'marketing/asset_stats', 'gold', 6, 'asset_total_impressions', NULL, 'error', 1, 2, 'data_pipeline', 'nb-av01-4-validate');
GO

-- ============================================================================
-- STEP 11: SEED COLUMN MAPPINGS
-- ============================================================================
-- Maps source JSON paths to target Delta columns for loading functions.
-- Note: Do NOT include 'item.' prefix - the code adds it dynamically when exploding "items" array

-- YouTube Channels mapping
INSERT INTO metadata.column_mappings (mapping_id, column_order, source_column, target_column, data_type, description) VALUES
('youtube_channels', 1, 'id', 'channel_id', 'string', 'YouTube channel ID'),
('youtube_channels', 2, 'snippet.title', 'channel_name', 'string', 'Channel display name'),
('youtube_channels', 3, 'snippet.description', 'channel_description', 'string', 'Channel description'),
('youtube_channels', 4, 'statistics.viewCount', 'view_count', 'int', 'Total channel views'),
('youtube_channels', 5, 'statistics.subscriberCount', 'subscriber_count', 'int', 'Subscriber count'),
('youtube_channels', 6, 'statistics.videoCount', 'video_count', 'int', 'Number of videos'),
('youtube_channels', 7, '_loading_ts', 'loading_TS', 'current_timestamp', 'Load timestamp');
GO

-- YouTube Playlist Items mapping
INSERT INTO metadata.column_mappings (mapping_id, column_order, source_column, target_column, data_type, description) VALUES
('youtube_playlist_items', 1, 'snippet.channelId', 'channel_id', 'string', 'Channel ID'),
('youtube_playlist_items', 2, 'snippet.resourceId.videoId', 'video_id', 'string', 'Video ID'),
('youtube_playlist_items', 3, 'snippet.title', 'video_title', 'string', 'Video title'),
('youtube_playlist_items', 4, 'snippet.description', 'video_description', 'string', 'Video description'),
('youtube_playlist_items', 5, 'snippet.thumbnails.high.url', 'thumbnail_url', 'string', 'Thumbnail URL'),
('youtube_playlist_items', 6, 'snippet.publishedAt', 'video_publish_TS', 'timestamp', 'Video publish timestamp'),
('youtube_playlist_items', 7, '_loading_ts', 'loading_TS', 'current_timestamp', 'Load timestamp');
GO

-- YouTube Videos mapping
INSERT INTO metadata.column_mappings (mapping_id, column_order, source_column, target_column, data_type, description) VALUES
('youtube_videos', 1, 'id', 'video_id', 'string', 'Video ID'),
('youtube_videos', 2, 'statistics.viewCount', 'video_view_count', 'int', 'Video view count'),
('youtube_videos', 3, 'statistics.likeCount', 'video_like_count', 'int', 'Video like count'),
('youtube_videos', 4, 'statistics.commentCount', 'video_comment_count', 'int', 'Video comment count'),
('youtube_videos', 5, '_loading_ts', 'loading_TS', 'current_timestamp', 'Load timestamp');
GO

-- ============================================================================
-- SEED DATA COMPLETE
-- ============================================================================
