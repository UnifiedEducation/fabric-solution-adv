# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# #### PRJ105 ⬛ ADV Project (Sprint 5): Lakehouse Development - GOLD LAKEHOUSE
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

gold_lh_name = variables.GOLD_LH_NAME


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 2: Create Lakehouse Schemas

# CELL ********************

GOLD_LH_SCHEMAS_TO_CREATE = ["marketing"] 

for schema_name in GOLD_LH_SCHEMAS_TO_CREATE: 
    
    # create a dynmamic Spark SQL script, reading from the Variable Library variables, and the GOLD_LH_SCHEMAS_TO_CREATE metadata
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{lh_workspace_name}`.`{gold_lh_name}`.`{schema_name}`")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 3: Create Lakehouse Tables


# CELL ********************

# table names from the requirements diagram
# table schemas from SILVER tables
GOLD_TABLE_SCHEMAS = {
    "marketing.channels": """
        channel_surrogate_id INT, 
        channel_platform STRING,
        channel_account_name STRING,
        channel_account_description STRING,
        channel_total_subscribers INT,
        channel_total_assets INT,
        channel_total_views INT,
        modified_TS TIMESTAMP
        """,
    "marketing.assets": """
        asset_surrogate_id INT,
        asset_natural_id STRING,
        channel_surrogate_id INT,
        asset_title STRING,
        asset_text STRING, 
        asset_publish_date TIMESTAMP,
        modified_TS TIMESTAMP
        """, 
    "marketing.asset_stats": """
        asset_surrogate_id INT, 
        asset_total_impressions INT,
        asset_total_views INT, 
        asset_total_likes INT,
        asset_total_comments INT,
        modified_TS TIMESTAMP
        """, 
}

# for each key,value in the metadata object GOLD_TABLE_SCHEMAS 
for table, ddl in GOLD_TABLE_SCHEMAS.items(): 
    
    # create a dynmamic Spark SQL script, reading from the Variable Library variables, and the GOLD_TABLE_SCHEMAS metadata
    #create_script = f"CREATE TABLE IF NOT EXISTS `{lh_workspace_name}`.`{gold_lh_name}`.{table} ({ddl});" 
    
    # CREATE OR REPLACE, useful for development for iterating on table schema design
    create_script = f"CREATE OR REPLACE TABLE `{lh_workspace_name}`.`{gold_lh_name}`.{table} ({ddl});" 

    # run the SQL statement to create the table
    spark.sql(create_script)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
