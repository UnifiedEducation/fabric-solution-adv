# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Publish Environment
# 
# When using Git branch-out or deployment, environments become unpublished when you deploy to a new Workspaace (like Test/ Prod). 
# 
# This script:
# - gets variables from the variable library 
# - uses Semantic Link to send a POST request to the Fabric REST API - the endpoint Publishes an Environment. 
# - polls the Get Environment Endpoint to check the status of the publishing (as this can take up to 10 minutes!). When the publishing is successful, the notebook completes. 

# CELL ********************

import sempy.fabric as fabric
import time

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

# #### Polling the status

# CELL ********************

# Poll for publish completion
# Ref: https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/get-environment

get_env_endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments/{environment_id}"
poll_interval_seconds = 30
max_wait_minutes = 15
max_attempts = (max_wait_minutes * 60) // poll_interval_seconds

print(f"Waiting for environment to publish (polling every {poll_interval_seconds}s, max {max_wait_minutes} min)...")

for attempt in range(1, max_attempts + 1):
    try:
        status_response = client.get(path_or_url=get_env_endpoint)
        env_data = status_response.json()

        publish_state = env_data.get("properties", {}).get("publishDetails", {}).get("state")
        print(f"  Attempt {attempt}/{max_attempts}: publishDetails.state = {publish_state}")

        if publish_state == "Success":
            print("Environment published successfully!")
            break
        elif publish_state == "Failed":
            raise Exception(f"Environment publish failed. Response: {env_data}")

        # Still running or other state - wait and retry
        time.sleep(poll_interval_seconds)

    except Exception as e:
        print(f"  Error checking status: {e}")
        time.sleep(poll_interval_seconds)
else:
    raise Exception(f"Environment publish did not complete within {max_wait_minutes} minutes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
