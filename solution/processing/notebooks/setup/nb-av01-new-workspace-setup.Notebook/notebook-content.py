# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

init_lakehouses = True
init_metadata_sql = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Runtime Environment configuration: 


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

# MARKDOWN ********************

# This notebook should be run each time you create a new workspace/ environment (for example, when developing a new feature). 
#  
# 1. Run Lakehouse Creation Scripts
# 2. Publish environments (in this notebook)
# 3. Run Metadata SQL seeding notebook

# MARKDOWN ********************

# #### Setup Step 1. Run Lakehouse Creation Scripts

# CELL ********************

import notebookutils
if init_lakehouses == True: 
    notebookutils.notebook.run("nb-av01-lhcreate-all")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Setup Step 2. Publish environments
# When you use the Branching out feature, or use Git integration for your Deployment, Environments become unpublished in the new Workspace. I don't know why, but that's the decision Microsoft have made. To get around this, we need to re-publish our environment. Luckily they have recently announced a REST API endpoint to do this, so we'll use the Sempy Fabric REST API Client to call it. 


# CELL ********************

import sempy.fabric as fabric

# intialize a Fabric Rest API Client 
client = fabric.FabricRestClient()

# get our variables from the VL
variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")
workspace_id = variables.PROCESSING_WORKSPACE_ID
environment_id = variables.ENVIRONMENT_ID
beta_flag=True

# create the full endpoint 
# from: https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/publish-environment(preview)
endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments/{environment_id}/staging/publish?beta={beta_flag}"

#send the POST request 
client.post(path_or_url=endpoint)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Setup Step 3: (Optional) Initialize SQL Database 

# CELL ********************

if init_metadata_sql == True: 
    notebookutils.notebook.run("nb-av01-init-sql-database")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
