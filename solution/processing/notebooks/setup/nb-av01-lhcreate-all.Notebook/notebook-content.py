# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-lhcreate-all
# 
# **Purpose**: Create all lakehouse schemas and tables for Bronze, Silver, and Gold layers.
# 
# **Usage**: Run via nb-av01-new-workspace-setup or directly for lakehouse initialization.
# 
# **Naming**: Uses Spark-SQL four-part naming: `workspace`.`lakehouse`.`schema`.`table`

# MARKDOWN ********************

# ## Setup

# CELL ********************

import notebookutils

variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")
lh_workspace_name = variables.LH_WORKSPACE_NAME


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Lakehouse Schema Definitions
# # Table definitions for each layer (Bronze, Silver, Gold).

# CELL ********************

LAKEHOUSE_METADATA = {
    "bronze": {
        "name_variable": "BRONZE_LH_NAME",
        "schemas": ["youtube"],
        "tables": {
            "youtube.channel": """
                channel_id STRING, 
                channel_name STRING, 
                channel_description STRING, 
                view_count INT, 
                subscriber_count INT, 
                video_count INT, 
                loading_TS TIMESTAMP""",
            "youtube.playlist_items": """
                channel_id STRING, 
                video_id STRING, 
                video_title STRING, 
                video_description STRING,
                thumbnail_url STRING,
                video_publish_TS TIMESTAMP,
                loading_TS TIMESTAMP""",
            "youtube.videos": """
                video_id STRING, 
                video_view_count INT, 
                video_like_count INT, 
                video_comment_count INT,
                loading_TS TIMESTAMP""",
        }
    },
    "silver": {
        "name_variable": "SILVER_LH_NAME",
        "schemas": ["youtube"],
        "tables": {
            "youtube.channel_stats": """
                channel_id STRING, 
                channel_name STRING, 
                channel_description STRING, 
                view_count INT, 
                subscriber_count INT, 
                video_count INT, 
                loading_TS TIMESTAMP""",
            "youtube.videos": """
                channel_id STRING, 
                video_id STRING, 
                video_title STRING, 
                video_description STRING,
                thumbnail_url STRING,
                video_publish_TS TIMESTAMP,
                loading_TS TIMESTAMP""",
            "youtube.video_statistics": """
                video_id STRING, 
                video_view_count INT, 
                video_like_count INT, 
                video_comment_count INT,
                loading_TS TIMESTAMP""",
        }
    },
    "gold": {
        "name_variable": "GOLD_LH_NAME",
        "schemas": ["marketing"],
        "tables": {
            "marketing.channels": """
                channel_surrogate_id INT, 
                channel_platform STRING,
                channel_account_name STRING,
                channel_account_description STRING,
                channel_total_subscribers INT,
                channel_total_assets INT,
                channel_total_views INT,
                modified_TS TIMESTAMP""",
            "marketing.assets": """
                asset_surrogate_id INT,
                asset_natural_id STRING,
                channel_surrogate_id INT,
                asset_title STRING,
                asset_text STRING, 
                asset_publish_date TIMESTAMP,
                modified_TS TIMESTAMP""",
            "marketing.asset_stats": """
                asset_surrogate_id INT, 
                asset_total_impressions INT,
                asset_total_views INT, 
                asset_total_likes INT,
                asset_total_comments INT,
                modified_TS TIMESTAMP""",
        }
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create Objects

# CELL ********************

def create_lakehouse_objects(layer_name: str, lakehouse_config: dict):
    """
    Create schemas and tables for a lakehouse based on configuration.

    Args:
        layer_name: Layer identifier (bronze, silver, gold) for logging
        lakehouse_config: Dict with name_variable, schemas, and tables keys
    """
    lh_name = getattr(variables, lakehouse_config["name_variable"])
    print(f"Creating objects for {layer_name} layer ({lh_name})...")

    # Create schemas
    for schema_name in lakehouse_config["schemas"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{lh_workspace_name}`.`{lh_name}`.`{schema_name}`")
        print(f"  Schema: {schema_name}")

    # Create tables
    for table, ddl in lakehouse_config["tables"].items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS `{lh_workspace_name}`.`{lh_name}`.{table} ({ddl});")
        print(f"  Table: {table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Process all lakehouses
for layer_name, lakehouse_config in LAKEHOUSE_METADATA.items():
    create_lakehouse_objects(layer_name, lakehouse_config)

print("\nLakehouse creation complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
