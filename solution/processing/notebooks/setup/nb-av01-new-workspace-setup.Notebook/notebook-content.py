# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-new-workspace-setup
# 
# **Purpose**: Initialize a new workspace/environment, after deployment. This includes populating Lakehosues, publishing the Environment, and initializing the Metadata database with metadata. 
# 
# Optional toggles to turn steps on/off. (These are also notebook params, should you wish to parameterize the notebook).
# 
# **Trigger: ** this notebook is nearly always triggered automatically as part of the GitHub Actions. 
# 
# **Usage**: Run once when creating a new workspace (e.g., for feature development).
# 
# **Steps**:
# - Create Lakehouse schemas and tables
# - Publish environment (required after Git branch-out)
# - Seed metadata SQL database
# - Run the full pipeline all the way through. 

# MARKDOWN ********************

# #### Runtime Environment Configuration


# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC     "environment": {
# MAGIC         "id": {"variableName": "$(/**/vl-av01-variables/ENVIRONMENT_ID)"},
# MAGIC         "name": {"variableName": "$(/**/vl-av01-variables/ENVIRONMENT_NAME)"}
# MAGIC     }
# MAGIC }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

init_lakehouses = True
init_metadata_sql = False
run_pipeline = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1: Populate Lakehouses 
if init_lakehouses == True: 
    notebookutils.notebook.run("nb-av01-lhcreate-all")
    
# Step 2: publish environment 
notebookutils.notebook.run("nb-av01-publish-environment")

# Step 3: Init SQL Metadata Database
if init_lakehouses == True: 
    notebookutils.notebook.run("nb-av01-init-sql-database")

# Step 4: Run full ETL pipeline notebook 
if run_pipeline == True: 
    notebookutils.notebook.run("nb-av01-run")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
