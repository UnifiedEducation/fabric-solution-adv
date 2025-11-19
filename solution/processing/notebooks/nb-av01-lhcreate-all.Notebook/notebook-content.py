# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# #### PRJ105 ⬛ ADV Project (Sprint 5): Lakehouse Development - ALL TABLES
# > The code in this notebook is written as part of Week 5 of the Advanced Project, in [Fabric Dojo](https://skool.com/fabricdojo/about).
# 
# #### Step 1: Getting our variables
# Firstly, we need to get the variables from the variable library, so that our code will function no matter which workspace it is being called from. 
# 
# In this context, we are using the Spark-SQL four-part naming structure: 
# \`workspace_name\`.lakehouse_name.schema_name.table_name 
# 
# _Note: if your workspace name contains hyphens (rather than underscore), then you must use \` backtick to wrap around the workspace name._

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

# #### Step 2: Define some metadata
# I just took the metadata from each of the three notebooks, and combined it into one Python object. 
# 
# We will build this into our metadata framework in a later Sprint - for now - this is good enough! 

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

# #### Step 3: Define function
# Our function takes the lakehouse config as input (from the METADATA), and performs the Schema Creation and Table Creation, as per the metadata config. 

# CELL ********************

def create_lakehouse_objects(lakehouse_config):
    """
    Creates schemas and tables for a lakehouse based on configuration.
    
    Args:
        lakehouse_config: Dictionary containing lakehouse metadata
    """
    # Get lakehouse name from variables
    lh_name = getattr(variables, lakehouse_config["name_variable"])
    
    # Create schemas
    for schema_name in lakehouse_config["schemas"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{lh_workspace_name}`.`{lh_name}`.`{schema_name}`")
    
    # Create tables
    for table, ddl in lakehouse_config["tables"].items():
        create_script = f"CREATE TABLE IF NOT EXISTS `{lh_workspace_name}`.`{lh_name}`.{table} ({ddl});"
        spark.sql(create_script)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 4: Loop through the metadata, and run the function 

# CELL ********************

# Process all lakehouses
for lakehouse_name, lakehouse_config in LAKEHOUSE_METADATA.items():
    create_lakehouse_objects(lakehouse_config)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
