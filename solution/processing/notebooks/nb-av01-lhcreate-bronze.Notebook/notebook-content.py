# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# #### PRJ105 ⬛ ADV Project (Sprint 5): Lakehouse Development - BRONZE LAKEHOUSE
# > The code in this notebook is written as part of Week 5 of the Advanced Project, in [Fabric Dojo](https://skool.com/fabricdojo/about).
# 
# #### Step 1: Getting our variables
# Firstly, we need to get the variables from the variable library (again), so that our code will function no matter which workspace it is being called from. 
# In this context, we are using the Spark-SQL four-part naming structure: 
# \`workspace_name\`.lakehouse_name.schema_name.table_name 
# 
# _Note: if your workspace name contains hyphens (rather than underscore), then you must use \` backtick to wrap around the workspace name._

# CELL ********************

import notebookutils 

variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")

lh_workspace_name = variables.LH_WORKSPACE_NAME

bronze_lh_name = variables.BRONZE_LH_NAME


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 2: Create Lakehouse Schemas

# CELL ********************

BRONZE_LH_SCHEMAS_TO_CREATE = ["youtube"] 

for schema_name in BRONZE_LH_SCHEMAS_TO_CREATE: 
    
    # create a dynmamic Spark SQL script, reading from the Variable Library variables, and the BRONZE_LH_SCHEMAS_TO_CREATE metadata
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{lh_workspace_name}`.`{bronze_lh_name}`.`{schema_name}`")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 3: Create Lakehouse Tables


# CELL ********************

# table names from the requirements diagram
# table schemas from raw data

BRONZE_TABLE_SCHEMAS = {
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

# for each key,value in the metadata object BRONZE_TABLE_SCHEMAS 
for table, ddl in BRONZE_TABLE_SCHEMAS.items(): 
    
    # create a dynmamic Spark SQL script, reading from the Variable Library variables, and the BRONZE_TABLE_SCHEMAS metadata
    # we use IF NOT EXISTS to avoid overwrite - all updates will be done through migrations
    create_script = f"CREATE TABLE IF NOT EXISTS `{lh_workspace_name}`.`{bronze_lh_name}`.{table} ({ddl});" 
    
    # run the SQL statement to create the table
    spark.sql(create_script)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
