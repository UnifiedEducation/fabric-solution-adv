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
# 1. Configure variable library with workspace-specific values (MUST run first!)
# 2. Create Lakehouse schemas and tables
# 3. Publish environment (required after Git branch-out)
# 4. Seed metadata SQL database

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

# ## Step 1: Configure Variable Library
# # MUST run first! This updates the variable library with workspace-specific item IDs
# and sets the active value set to the current environment (TEST/PROD).
# Other steps depend on reading correct values from the variable library.

# CELL ********************

import notebookutils

print("Configuring variable library with workspace-specific values...")
notebookutils.notebook.run("nb-av01-configure-variables")
print("Variable library configuration complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Create Lakehouse Objects
# # Note: Variable library must be configured first (Step 1) so this step reads correct values.
# Child notebooks handle their own environment attachment.

# CELL ********************

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

# ## Step 3: Publish Environment
# # When using Git branch-out or deployment, environments become unpublished in the new workspace.
# This step re-publishes the environment using the Fabric REST API.

# CELL ********************

import sempy.fabric as fabric
import time

# Initialize Fabric REST API client
client = fabric.FabricRestClient()

# Get workspace ID from runtime context (works for fresh deployments)
workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
print(f"Workspace ID: {workspace_id}")

# Find environment ID by querying workspace items
ENV_NAME = "env-av01-dataeng"
response = client.get(f"v1/workspaces/{workspace_id}/items")
items = response.json().get("value", [])
environment_id = None
for item in items:
    if item.get("displayName") == ENV_NAME and item.get("type") == "Environment":
        environment_id = item.get("id")
        break

if not environment_id:
    print(f"Warning: Environment '{ENV_NAME}' not found in workspace. Skipping publish.")
else:
    print(f"Environment ID: {environment_id}")

    # Build endpoint URL (preview API requires beta flag)
    # Ref: https://learn.microsoft.com/en-us/rest/api/fabric/environment/items/publish-environment
    FABRIC_API_BETA = True
    endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments/{environment_id}/staging/publish?beta={FABRIC_API_BETA}"

    # Publish environment with error handling
    try:
        response = client.post(path_or_url=endpoint)
        print(f"Environment publish initiated (status: {response.status_code})")

        # If 202 Accepted, poll for completion (async operation)
        if response.status_code == 202:
            location = response.headers.get("Location")
            retry_after = int(response.headers.get("Retry-After", 10))
            print(f"Polling for environment publish completion...")

            max_wait = 600  # 10 minutes max (environment publish can be slow)
            elapsed = 0
            while elapsed < max_wait:
                time.sleep(retry_after)
                elapsed += retry_after

                if location:
                    poll_resp = client.get(location)
                    if poll_resp.status_code == 200:
                        poll_data = poll_resp.json()
                        status = poll_data.get("status", "")
                        percent = poll_data.get("percentComplete", 0)

                        if status == "Succeeded":
                            print(f"Environment published successfully!")
                            break
                        elif status == "Failed":
                            error = poll_data.get("error", {}).get("message", "Unknown error")
                            print(f"Warning: Environment publish failed: {error}")
                            break
                        else:
                            print(f"  Publishing... status={status}, {percent}% complete ({elapsed}s)")
                else:
                    # No location header, assume it's done
                    break
            else:
                print(f"Warning: Timeout waiting for environment publish after {max_wait}s")
        elif response.status_code == 200:
            print("Environment published successfully!")
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

# ## Step 4: Seed Metadata SQL Database

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
