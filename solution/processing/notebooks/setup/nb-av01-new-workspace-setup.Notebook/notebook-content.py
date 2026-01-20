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
# # **Purpose**: Initialize a new workspace/environment with all required objects.
# # **Usage**: Run once when creating a new workspace (e.g., for feature development).
# # **Steps**:
# 1. Create Lakehouse schemas and tables
# 2. Publish environment (required after Git branch-out)
# 3. Seed metadata SQL database

# MARKDOWN ********************

# ## Parameters

# PARAMETERS CELL ********************

init_lakehouses = True
init_metadata_sql = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Runtime Environment Configuration


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

# ## Step 1: Create Lakehouse Objects

# CELL ********************

import notebookutils

if init_lakehouses:
    print("Creating lakehouse schemas and tables...")
    notebookutils.notebook.run("nb-av01-lhcreate-all")
    print("Lakehouse creation complete.")
else:
    print("Skipping lakehouse creation (init_lakehouses=False)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Publish Environment
# # When using Git branch-out or deployment, environments become unpublished in the new workspace.
# This step re-publishes the environment using the Fabric REST API.

# CELL ********************

import sempy.fabric as fabric

# Initialize Fabric REST API client
client = fabric.FabricRestClient()

# Get workspace and environment IDs from Variable Library
variables = notebookutils.variableLibrary.getLibrary("vl-av01-variables")
workspace_id = variables.PROCESSING_WORKSPACE_ID
environment_id = variables.ENVIRONMENT_ID

# Build endpoint URL (preview API requires beta flag)
# Ref: https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/publish-environment
FABRIC_API_BETA = True
endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments/{environment_id}/staging/publish?beta={FABRIC_API_BETA}"

# Publish environment with error handling
try:
    response = client.post(path_or_url=endpoint)
    print(f"Environment published successfully (status: {response.status_code})")
except Exception as e:
    # This may fail if environment is already published or doesn't need publishing
    print(f"Note: Environment publish returned: {e}")
    print("This is often expected if the environment is already published.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Seed Metadata SQL Database

# CELL ********************

if init_metadata_sql:
    print("Seeding metadata SQL database...")
    notebookutils.notebook.run("nb-av01-init-sql-database")
    print("Metadata seeding complete.")
else:
    print("Skipping SQL seeding (init_metadata_sql=False)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
