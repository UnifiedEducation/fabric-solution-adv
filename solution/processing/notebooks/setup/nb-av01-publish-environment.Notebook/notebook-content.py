# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-publish-environment
# 
# When using Git branch-out or deployment, environments become unpublished when you deploy to a new Workspace (like Test/ Prod). 
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

# Check if environment is already published before attempting to publish
get_env_endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments/{environment_id}"
status_response = client.get(path_or_url=get_env_endpoint)
env_data = status_response.json()
current_state = env_data.get("properties", {}).get("publishDetails", {}).get("state")

print(f"Current environment publish state: {current_state}")

if current_state == "Success":
    print("Environment is already published - skipping publish step")
    needs_polling = False
else:
    # Attempt to publish
    needs_polling = True
    try:
        response = client.post(path_or_url=endpoint)
        print(f"Environment publish initiated (status: {response.status_code})")
    except Exception as e:
        print(f"Note: Environment publish returned: {e}")
        print("This may be expected if fabric-cicd already published the environment.")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Polling the status

# CELL ********************

# Poll for publish completion (only if we initiated a publish)
# Ref: https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/get-environment

if needs_polling:
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
else:
    print("Skipping polling - environment already published")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
