# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-init-sql-database
# 
# **Purpose**: Seed the metadata SQL database with initial configuration data.
# 
# **Usage**: Run once per new workspace/environment to populate metadata tables.
# 
# Uses the Spark - MSSQL Connector, [read more here](). 
# 
# **Dependencies**: Requires nb-av01-generic-functions (provides TimestampType, BooleanType, notebookutils)
# 
# **Tables Seeded**:
# - metadata.log_store, source_store, loading_store, transform_store, expectation_store, column_mappings
# - instructions.ingestion, loading, transformations, validations

# MARKDOWN ********************

# ## Imports & Setup

# CELL ********************

%run nb-av01-generic-functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Additional imports (TimestampType, BooleanType available via generic-functions)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

set_metadata_db_url(
    server=variables.METADATA_SERVER,
    database=variables.METADATA_DB
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Metadata Store Definitions
# # Configuration tables that define available functions, sources, and mappings.

# CELL ********************

# Log Store: Available logging functions (referenced by log_function_id)
log_store_schema = StructType([
    StructField("log_id", IntegerType(), False),
    StructField("function_name", StringType(), False),
    StructField("description", StringType(), True),
    StructField("expected_params", StringType(), True)
])

log_store_data = [
    (1, "log_standard", "Standard logging - records start, end, row counts, and status",
     '{"params": ["pipeline_name", "notebook_name", "status", "rows_processed"]}'),
    (2, "log_validation", "Validation logging - one row per expectation result with GX metadata",
     '{"params": ["validation_result"]}')
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Source Store: External data sources with connection details
source_store_schema = StructType([
    StructField("source_id", IntegerType(), False),
    StructField("source_name", StringType(), False),
    StructField("source_type", StringType(), False),
    StructField("auth_method", StringType(), True),
    StructField("key_vault_url", StringType(), True),
    StructField("secret_name", StringType(), True),
    StructField("base_url", StringType(), True),
    StructField("description", StringType(), True),
    StructField("created_date", TimestampType(), True),
    StructField("modified_date", TimestampType(), True)
])

source_store_data = [
    (1, "youtube_api", "rest_api", "api_key",
     "https://av01-akv-restapi-keys.vault.azure.net/",
     "data-v3-api-key",
     "https://www.googleapis.com/youtube/v3",
     "YouTube Data API v3 - Channel stats, videos, playlists",
     None, None)  # Let SQL DEFAULT handle these
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Loading Store: Available loading functions for Raw->Bronze
loading_store_schema = StructType([
    StructField("loading_id", IntegerType(), False),
    StructField("function_name", StringType(), False),
    StructField("description", StringType(), True),
    StructField("expected_params", StringType(), True)
])

loading_store_data = [
    (1, "load_json_to_delta",
     "Load JSON files from Raw zone to Delta table with column mapping and MERGE",
     '{"params": ["source_path", "target_table", "key_columns", "column_mapping_id"]}')
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform Store: Available transform functions (referenced by transform_pipeline)
transform_store_schema = StructType([
    StructField("transform_id", IntegerType(), False),
    StructField("function_name", StringType(), False),
    StructField("description", StringType(), True),
    StructField("expected_params", StringType(), True)
])

transform_store_data = [
    (1, "filter_nulls",
     "Remove rows where specified columns are null",
     '{"params": ["columns"]}'),
    (2, "dedupe_by_window",
     "Deduplicate using window function - keeps most recent by order column",
     '{"params": ["partition_cols", "order_col", "order_desc"]}'),
    (3, "rename_columns",
     "Rename columns according to mapping",
     '{"params": ["column_mapping"]}'),
    (4, "add_literal_columns",
     "Add columns with literal/static values",
     '{"params": ["columns"]}'),
    (5, "generate_surrogate_key",
     "Generate surrogate key using row_number over window, starting from max existing ID",
     '{"params": ["key_column_name", "order_by_col", "max_from_table"]}'),
    (6, "lookup_join",
     "Join to lookup/dimension table to get surrogate key or other columns",
     '{"params": ["lookup_table", "source_key", "lookup_key", "select_cols"]}')
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Expectation Store: GX expectation types for validation (referenced by expectation_id)
expectation_store_schema = StructType([
    StructField("expectation_id", IntegerType(), False),
    StructField("expectation_name", StringType(), False),
    StructField("gx_method", StringType(), False),
    StructField("description", StringType(), True),
    StructField("expected_params", StringType(), True)
])

expectation_store_data = [
    (1, "not_null", "ExpectColumnValuesToNotBeNull",
     "Validate that column contains no null values",
     '{"params": ["column"]}'),
    (2, "unique", "ExpectColumnValuesToBeUnique",
     "Validate that column contains only unique values",
     '{"params": ["column"]}'),
    (3, "value_in_set", "ExpectColumnValuesToBeInSet",
     "Validate that column values are within a defined set",
     '{"params": ["column", "value_set"]}'),
    (4, "values_increasing", "ExpectColumnValuesToBeIncreasing",
     "Validate that column values are strictly increasing",
     '{"params": ["column"]}'),
    (5, "compound_unique", "ExpectCompoundColumnsToBeUnique",
     "Validate that combination of columns is unique",
     '{"params": ["column_list"]}'),
    (6, "is_null", "ExpectColumnValuesToBeNull",
     "Validate that column contains only null values",
     '{"params": ["column"]}')
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

column_mappings_schema = StructType([
    StructField("mapping_id", StringType(), False),
    StructField("column_order", IntegerType(), False),
    StructField("source_column", StringType(), False),
    StructField("target_column", StringType(), False),
    StructField("data_type", StringType(), False),  
    StructField("description", StringType(), True)
])

column_mappings_data = [
    # YouTube Channels mapping
    ("youtube_channels", 1, "id", "channel_id", "string", "YouTube channel ID"),
    ("youtube_channels", 2, "snippet.title", "channel_name", "string", "Channel display name"),
    ("youtube_channels", 3, "snippet.description", "channel_description", "string", "Channel description"),
    ("youtube_channels", 4, "statistics.viewCount", "view_count", "int", "Total channel views"),
    ("youtube_channels", 5, "statistics.subscriberCount", "subscriber_count", "int", "Subscriber count"),
    ("youtube_channels", 6, "statistics.videoCount", "video_count", "int", "Number of videos"),
    ("youtube_channels", 7, "_loading_ts", "loading_TS", "current_timestamp", "Load timestamp"),
    # YouTube Playlist Items mapping
    ("youtube_playlist_items", 1, "snippet.channelId", "channel_id", "string", "Channel ID"),
    ("youtube_playlist_items", 2, "snippet.resourceId.videoId", "video_id", "string", "Video ID"),
    ("youtube_playlist_items", 3, "snippet.title", "video_title", "string", "Video title"),
    ("youtube_playlist_items", 4, "snippet.description", "video_description", "string", "Video description"),
    ("youtube_playlist_items", 5, "snippet.thumbnails.high.url", "thumbnail_url", "string", "Thumbnail URL"),
    ("youtube_playlist_items", 6, "snippet.publishedAt", "video_publish_TS", "timestamp", "Video publish timestamp"),
    ("youtube_playlist_items", 7, "_loading_ts", "loading_TS", "current_timestamp", "Load timestamp"),
    # YouTube Videos mapping
    ("youtube_videos", 1, "id", "video_id", "string", "Video ID"),
    ("youtube_videos", 2, "statistics.viewCount", "video_view_count", "int", "Video view count"),
    ("youtube_videos", 3, "statistics.likeCount", "video_like_count", "int", "Video like count"),
    ("youtube_videos", 4, "statistics.commentCount", "video_comment_count", "int", "Video comment count"),
    ("youtube_videos", 5, "_loading_ts", "loading_TS", "current_timestamp", "Load timestamp")
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Instruction Table Definitions
# # Runtime instructions that control pipeline behavior.

# CELL ********************

# Ingestion Instructions: API endpoints to call during ingestion
ingestion_schema = StructType([
    StructField("ingestion_id", IntegerType(), False),
    StructField("source_id", IntegerType(), False),
    StructField("endpoint_path", StringType(), True),
    StructField("landing_path", StringType(), False),
    StructField("request_params", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("log_function_id", IntegerType(), True),
    StructField("created_date", TimestampType(), True),
    StructField("modified_date", TimestampType(), True)
])

ingestion_data = [
    (1, 1, "/channels", "youtube_data_v3/channels/",
     '{"part": "snippet,statistics,contentDetails", "id": "UCrvoIYkzS-RvCEb0x7wfmwQ"}',
     True, 1, None, None),
    (2, 1, "/playlistItems", "youtube_data_v3/playlistItems/",
     '{"part": "snippet", "maxResults": 50, "playlistId": "UUrvoIYkzS-RvCEb0x7wfmwQ"}',
     True, 1, None, None),
    (3, 1, "/videos", "youtube_data_v3/videos/",
     '{"part": "statistics", "maxResults": 50}',
     True, 1, None, None)
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Loading Instructions: Raw->Bronze table loads
loading_schema = StructType([
    StructField("loading_instr_id", IntegerType(), False),
    StructField("loading_id", IntegerType(), False),
    StructField("source_path", StringType(), False),
    StructField("source_layer", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("target_layer", StringType(), False),
    StructField("key_columns", StringType(), False),
    StructField("load_params", StringType(), True),
    StructField("merge_condition", StringType(), True),
    StructField("merge_type", StringType(), True),
    StructField("merge_columns", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("log_function_id", IntegerType(), True),
    StructField("created_date", TimestampType(), True),
    StructField("modified_date", TimestampType(), True)
])

loading_data = [
    # Channel: merge on channel_id + date
    (1, 1, "Files/youtube_data_v3/channels/", "raw", "youtube/channel", "bronze",
     '["channel_id"]',
     '{"column_mapping_id": "youtube_channels"}',
     "target.channel_id = source.channel_id AND to_date(target.loading_TS) = to_date(source.loading_TS)",
     "update_all", None, True, 1, None, None),
    # Playlist Items: merge on video_id only
    (2, 1, "Files/youtube_data_v3/playlistItems/", "raw", "youtube/playlist_items", "bronze",
     '["video_id"]',
     '{"column_mapping_id": "youtube_playlist_items"}',
     "target.video_id = source.video_id",
     "update_all", None, True, 1, None, None),
    # Videos: merge on video_id only
    (3, 1, "Files/youtube_data_v3/videos/", "raw", "youtube/videos", "bronze",
     '["video_id"]',
     '{"column_mapping_id": "youtube_videos"}',
     "target.video_id = source.video_id",
     "update_all", None, True, 1, None, None)
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transformation Instructions: Bronze->Silver and Silver->Gold transforms
transformations_schema = StructType([
    StructField("transform_instr_id", IntegerType(), False),
    StructField("source_table", StringType(), False),
    StructField("source_layer", StringType(), False),
    StructField("dest_table", StringType(), False),
    StructField("dest_layer", StringType(), False),
    StructField("transform_pipeline", StringType(), False),
    StructField("transform_params", StringType(), True),
    StructField("merge_condition", StringType(), True),
    StructField("merge_type", StringType(), True),
    StructField("merge_columns", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("log_function_id", IntegerType(), True),
    StructField("created_date", TimestampType(), True),
    StructField("modified_date", TimestampType(), True)
])

transformations_data = [
    # Bronze -> Silver transformations
    # Channel: filter nulls on channel_id, dedupe by channel_id+date
    (1, "youtube/channel", "bronze", "youtube/channel_stats", "silver",
     "[1, 2]",
     '{"1": {"columns": ["channel_id"]}, "2": {"partition_cols": ["channel_id", "to_date(loading_TS)"], "order_col": "loading_TS", "order_desc": true}}',
     "target.channel_id = source.channel_id AND to_date(target.loading_TS) = to_date(source.loading_TS)",
     "update_all", None, True, 1, None, None),
    # Playlist Items -> Videos: filter nulls on video_id+video_title, dedupe by video_id
    (2, "youtube/playlist_items", "bronze", "youtube/videos", "silver",
     "[1, 2]",
     '{"1": {"columns": ["video_id", "video_title"]}, "2": {"partition_cols": ["video_id"], "order_col": "loading_TS", "order_desc": true}}',
     "target.video_id = source.video_id",
     "update_all", None, True, 1, None, None),
    # Videos -> Video Statistics: filter nulls on video_id, dedupe by video_id+date
    (3, "youtube/videos", "bronze", "youtube/video_statistics", "silver",
     "[1, 2]",
     '{"1": {"columns": ["video_id"]}, "2": {"partition_cols": ["video_id", "to_date(loading_TS)"], "order_col": "loading_TS", "order_desc": true}}',
     "target.video_id = source.video_id",
     "update_all", None, True, 1, None, None),
    # Silver -> Gold transformations
    # Channel Stats -> Marketing Channels: add literals, rename columns
    (10, "youtube/channel_stats", "silver", "marketing/channels", "gold",
     "[4, 3]",
     '{"4": {"columns": {"channel_surrogate_id": 1, "channel_platform": "youtube"}}, "3": {"column_mapping": {"channel_name": "channel_account_name", "channel_description": "channel_account_description", "subscriber_count": "channel_total_subscribers", "video_count": "channel_total_assets", "view_count": "channel_total_views", "loading_TS": "modified_TS"}}}',
     "target.channel_surrogate_id = source.channel_surrogate_id AND to_date(target.modified_TS) = to_date(source.modified_TS)",
     "update_all", None, True, 1, None, None),
    # Videos -> Marketing Assets: add literals, rename columns, generate surrogate key
    (11, "youtube/videos", "silver", "marketing/assets", "gold",
     "[4, 3, 5]",
     '{"4": {"columns": {"channel_surrogate_id": 1}}, "3": {"column_mapping": {"video_id": "asset_natural_id", "video_title": "asset_title", "video_description": "asset_text", "video_publish_TS": "asset_publish_date", "loading_TS": "modified_TS"}}, "5": {"key_column_name": "asset_surrogate_id", "order_by_col": "asset_publish_date", "natural_key": "asset_natural_id", "max_from_table": "marketing/assets"}}',
     "target.asset_natural_id = source.asset_natural_id",
     "specific_columns",
     '{"update": ["asset_title", "asset_text", "asset_publish_date", "modified_TS"], "insert": ["asset_surrogate_id", "asset_natural_id", "channel_surrogate_id", "asset_title", "asset_text", "asset_publish_date", "modified_TS"]}',
     True, 1, None, None),
    # Video Statistics -> Marketing Asset Stats: lookup join, rename columns
    (12, "youtube/video_statistics", "silver", "marketing/asset_stats", "gold",
     "[6, 3, 4]",
     '{"6": {"lookup_table": "marketing/assets", "source_key": "video_id", "lookup_key": "asset_natural_id", "select_cols": ["asset_surrogate_id"]}, "3": {"column_mapping": {"video_view_count": "asset_total_views", "video_like_count": "asset_total_likes", "video_comment_count": "asset_total_comments", "loading_TS": "modified_TS"}}, "4": {"columns": {"asset_total_impressions": null}}}',
     "target.asset_surrogate_id = source.asset_surrogate_id AND to_date(target.modified_TS) = to_date(source.modified_TS)",
     "specific_columns",
     '{"update": ["asset_total_views", "asset_total_impressions", "asset_total_likes", "asset_total_comments", "modified_TS"], "insert": ["asset_surrogate_id", "asset_total_views", "asset_total_impressions", "asset_total_likes", "asset_total_comments", "modified_TS"]}',
     True, 1, None, None)
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validation Instructions: GX expectations to run on Gold tables
validations_schema = StructType([
    StructField("validation_instr_id", IntegerType(), False),
    StructField("target_table", StringType(), False),
    StructField("target_layer", StringType(), False),
    StructField("expectation_id", IntegerType(), False),
    StructField("column_name", StringType(), True),
    StructField("validation_params", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("log_function_id", IntegerType(), True),
    StructField("created_date", TimestampType(), True),
    StructField("modified_date", TimestampType(), True)
])

validations_data = [
    # marketing/channels validations
    (1, "marketing/channels", "gold", 3, "channel_surrogate_id", '{"value_set": [1]}', "error", True, 2, None, None),
    (2, "marketing/channels", "gold", 4, "channel_total_views", None, "error", True, 2, None, None),
    # marketing/assets validations
    (3, "marketing/assets", "gold", 2, "asset_surrogate_id", None, "error", True, 2, None, None),
    (4, "marketing/assets", "gold", 1, "asset_natural_id", None, "error", True, 2, None, None),
    (5, "marketing/assets", "gold", 1, "asset_publish_date", None, "error", True, 2, None, None),
    (6, "marketing/assets", "gold", 5, None, '{"column_list": ["asset_title", "asset_surrogate_id"]}', "error", True, 2, None, None),
    # marketing/asset_stats validations
    (7, "marketing/asset_stats", "gold", 6, "asset_total_impressions", None, "error", True, 2, None, None)
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seed Execution

# CELL ********************

def write_seed_table(table_name: str, schema: StructType, data: list) -> int:
    """
    Write seed data to a SQL metadata table.

    Args:
        table_name: Fully qualified table name (schema.table)
        schema: PySpark StructType defining the table schema
        data: List of tuples containing row data

    Returns:
        Number of rows written
    """
    if not data:
        print(f"  Skipping {table_name} - no data to write")
        return 0

    df = spark.createDataFrame(data, schema)
    df.write.mode("append").option("url", METADATA_DB_URL).mssql(table_name)
    print(f"  Wrote {len(data)} rows to {table_name}")
    return len(data)


def table_has_data(table_name: str) -> bool:
    """Check if a table already has data."""
    df = spark.read.option("url", METADATA_DB_URL).mssql(table_name)
    return df.count() > 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if data already exists (use metadata.log_store as indicator)
if table_has_data("metadata.log_store"):
    print("Seed data already exists - skipping seeding")
else:
    # Seed instruction tables
    instr_rows = 0
    instr_rows += write_seed_table("instructions.ingestion", ingestion_schema, ingestion_data)
    instr_rows += write_seed_table("instructions.loading", loading_schema, loading_data)
    instr_rows += write_seed_table("instructions.transformations", transformations_schema, transformations_data)
    instr_rows += write_seed_table("instructions.validations", validations_schema, validations_data)

    # Seed metadata store tables
    total_rows = 0
    total_rows += write_seed_table("metadata.log_store", log_store_schema, log_store_data)
    total_rows += write_seed_table("metadata.source_store", source_store_schema, source_store_data)
    total_rows += write_seed_table("metadata.loading_store", loading_store_schema, loading_store_data)
    total_rows += write_seed_table("metadata.transform_store", transform_store_schema, transform_store_data)
    total_rows += write_seed_table("metadata.expectation_store", expectation_store_schema, expectation_store_data)
    total_rows += write_seed_table("metadata.column_mappings", column_mappings_schema, column_mappings_data)

    print(f"Seeded {instr_rows} instruction rows and {total_rows} metadata rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
