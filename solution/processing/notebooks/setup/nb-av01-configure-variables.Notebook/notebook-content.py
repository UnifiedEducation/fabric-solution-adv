# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb-av01-configure-variables
# # **Purpose**: Auto-configure variable library with workspace-specific item IDs.
# # **Usage**: Run after deploying items to a new environment (TEST/PROD).
# # **What it does**:
# 1. Discovers all deployed item IDs from the current workspace
# 2. Gets SQL Database connection info (server and database identifiers)
# 3. Updates the variable library value set for the current environment
#
# # **Note**: Uses Fabric REST API via Sempy. No authentication required inside Fabric.

# CELL ********************

import json
import base64
import time
import notebookutils
import sempy.fabric as fabric

# Initialize Fabric REST API client (handles auth automatically inside Fabric)
client = fabric.FabricRestClient()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1: Detect Environment and Workspace

# CELL ********************

# Get current workspace ID from runtime context
workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
print(f"Current Workspace ID: {workspace_id}")

# Get workspace name to detect environment
response = client.get(f"v1/workspaces/{workspace_id}")
workspace_name = response.json()["displayName"]
print(f"Workspace Name: {workspace_name}")

# Detect environment from workspace name pattern
if "-dev-" in workspace_name:
    environment = "DEV"
elif "-test-" in workspace_name:
    environment = "TEST"
elif "-prod-" in workspace_name:
    environment = "PROD"
else:
    # Default to DEV for feature branches or unknown patterns
    environment = "DEV"
    print(f"Warning: Could not detect environment from workspace name. Defaulting to DEV.")

print(f"Detected Environment: {environment}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2: Discover All Workspace Items

# CELL ********************

# Get all items in the workspace
response = client.get(f"v1/workspaces/{workspace_id}/items")
items = response.json().get("value", [])

print(f"Found {len(items)} items in workspace")

# Build lookup by displayName
items_by_name = {item["displayName"]: item for item in items}

# Map notebook displayNames to variable names
NOTEBOOK_MAPPING = {
    "nb-av01-0-ingest-api": "_0_INGEST_NOTEBOOK_ID",
    "nb-av01-1-load": "_1_LOAD_NOTEBOOK_ID",
    "nb-av01-2-clean": "_2_CLEAN_NOTEBOOK_ID",
    "nb-av01-3-model": "_3_MODEL_NOTEBOOK_ID",
    "nb-av01-4-validate": "_4_VALIDATE_NOTEBOOK_ID",
}

# Discover notebook IDs
discovered_values = {}
for notebook_name, var_name in NOTEBOOK_MAPPING.items():
    if notebook_name in items_by_name:
        discovered_values[var_name] = items_by_name[notebook_name]["id"]
        print(f"  Found {notebook_name}: {discovered_values[var_name]}")
    else:
        print(f"  Warning: Notebook not found: {notebook_name}")

# Discover environment ID
ENV_NAME = "env-av01-dataeng"
if ENV_NAME in items_by_name:
    discovered_values["ENVIRONMENT_ID"] = items_by_name[ENV_NAME]["id"]
    discovered_values["ENVIRONMENT_NAME"] = ENV_NAME
    print(f"  Found {ENV_NAME}: {discovered_values['ENVIRONMENT_ID']}")
else:
    print(f"  Warning: Environment not found: {ENV_NAME}")

# Add workspace ID
discovered_values["PROCESSING_WORKSPACE_ID"] = workspace_id
print(f"  Workspace ID: {workspace_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Get SQL Database Connection Info

# CELL ********************

# Find SQL Database
SQL_DB_NAME = "fs-av01-admin"
if SQL_DB_NAME in items_by_name:
    sql_db_item = items_by_name[SQL_DB_NAME]
    sql_db_id = sql_db_item["id"]
    print(f"Found SQL Database: {SQL_DB_NAME} ({sql_db_id})")

    # Get SQL Database details to extract connection info
    response = client.get(f"v1/workspaces/{workspace_id}/sqlDatabases/{sql_db_id}")
    sql_db_details = response.json()

    # Extract connection properties
    properties = sql_db_details.get("properties", {})
    connection_string = properties.get("connectionString", "")
    database_name = properties.get("databaseName", "")
    server_fqdn = properties.get("serverFqdn", "")

    print(f"  Server FQDN: {server_fqdn}")
    print(f"  Database Name: {database_name}")

    # Extract server identifier from FQDN (part before .datawarehouse.fabric.microsoft.com)
    if server_fqdn:
        server_identifier = server_fqdn.replace(".datawarehouse.fabric.microsoft.com", "")
        discovered_values["METADATA_SERVER"] = server_identifier
        print(f"  Server Identifier: {server_identifier}")

    # Use database name for METADATA_DB
    if database_name:
        discovered_values["METADATA_DB"] = database_name
        print(f"  Database: {database_name}")
else:
    print(f"Warning: SQL Database not found: {SQL_DB_NAME}")

print(f"\nDiscovered {len(discovered_values)} values to configure")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4: Get Current Variable Library Definition

# CELL ********************

# Find Variable Library
VL_NAME = "vl-av01-variables"
if VL_NAME not in items_by_name:
    raise ValueError(f"Variable Library not found: {VL_NAME}")

vl_item = items_by_name[VL_NAME]
vl_id = vl_item["id"]
print(f"Found Variable Library: {VL_NAME} ({vl_id})")

# Helper function to handle Fabric's async getDefinition API
def get_definition_with_retry(api_client, ws_id, item_id, max_retries=5):
    """
    Get item definition, handling Fabric's async API pattern.
    The getDefinition endpoint returns 202, requiring polling.
    After operation completes, fetch result from {location}/result.
    """
    for retry in range(max_retries):
        print(f"  Fetching definition (attempt {retry + 1}/{max_retries})...")

        # Try using sempy's built-in LRO handling first
        try:
            resp = api_client.post(
                f"v1/workspaces/{ws_id}/items/{item_id}/getDefinition",
                lro_wait=True
            )
            print(f"    Response with lro_wait: {resp.status_code}")
            if resp.status_code == 200:
                result = resp.json()
                if result and result.get("definition", {}).get("parts"):
                    print(f"    Got definition via lro_wait!")
                    return result
        except Exception as e:
            print(f"    lro_wait approach failed: {e}")

        # Manual approach: POST to start the getDefinition operation
        resp = api_client.post(f"v1/workspaces/{ws_id}/items/{item_id}/getDefinition")
        print(f"    Initial response: {resp.status_code}")

        # Handle 200 - definition returned directly
        if resp.status_code == 200:
            result = resp.json()
            if result and result.get("definition", {}).get("parts"):
                print(f"    Got definition directly!")
                return result

        # Handle 202 - async operation, need to poll
        if resp.status_code == 202:
            location = resp.headers.get("Location")
            retry_after = int(resp.headers.get("Retry-After", 2))
            print(f"    Location: {location}")
            print(f"    Retry-After: {retry_after}")

            # Poll for this operation to complete
            for attempt in range(20):  # Max 40 seconds per retry
                time.sleep(retry_after)

                if location:
                    poll_resp = api_client.get(location)
                    poll_data = poll_resp.json() if poll_resp.status_code == 200 else {}
                else:
                    poll_data = {}

                status = poll_data.get("status", "")
                percent = poll_data.get("percentComplete", 0)

                if status == "Succeeded":
                    print(f"    Operation succeeded! Keys: {list(poll_data.keys())}")

                    # Check if definition is in the poll response
                    if "definition" in poll_data and poll_data.get("definition", {}).get("parts"):
                        return poll_data

                    # Try fetching from {location}/result
                    try:
                        print(f"    Trying {location}/result...")
                        result_resp = api_client.get(f"{location}/result")
                        if result_resp.status_code == 200:
                            result_data = result_resp.json()
                            if result_data and result_data.get("definition", {}).get("parts"):
                                return result_data
                            print(f"    /result keys: {list(result_data.keys()) if result_data else 'None'}")
                    except Exception as e:
                        print(f"    /result failed: {e}")

                    # Check for resultUri or result field
                    result_uri = poll_data.get("resultUri") or poll_data.get("result")
                    if result_uri:
                        print(f"    Fetching from resultUri: {result_uri}")
                        try:
                            result_resp = api_client.get(result_uri)
                            if result_resp.status_code == 200:
                                return result_resp.json()
                        except Exception as e:
                            print(f"    resultUri failed: {e}")

                    # Wait and retry POST
                    print(f"    Waiting before retry...")
                    time.sleep(5)
                    break

                elif status == "Failed":
                    error = poll_data.get("error", "Unknown error")
                    raise ValueError(f"Definition operation failed: {error}")

                print(f"    Polling... status={status}, percent={percent} (poll {attempt + 1}/20)")
            else:
                raise ValueError("Timeout waiting for definition operation")

        elif resp.status_code != 200 and resp.status_code != 202:
            raise ValueError(f"Unexpected status code: {resp.status_code} - {resp.text}")

    raise ValueError(f"Failed to get definition after {max_retries} retries")

# Get current definition (handles async API pattern with retries)
definition_response = get_definition_with_retry(client, workspace_id, vl_id)

# Extract parts from definition
definition = definition_response.get("definition") or {}
parts = definition.get("parts", [])

if not parts:
    print(f"Warning: No definition parts. Response keys: {list(definition_response.keys())}")
    raise ValueError("Variable Library has no definition parts - cannot proceed")
print(f"Variable Library has {len(parts)} definition parts")

# Decode parts into a dict for easy manipulation
decoded_parts = {}
for part in parts:
    path = part["path"]
    payload = part["payload"]
    payload_type = part.get("payloadType", "InlineBase64")

    if payload_type == "InlineBase64":
        content = base64.b64decode(payload).decode("utf-8")
        try:
            decoded_parts[path] = json.loads(content)
        except json.JSONDecodeError:
            decoded_parts[path] = content
    else:
        decoded_parts[path] = payload

print(f"Decoded parts: {list(decoded_parts.keys())}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5: Update Value Set for Current Environment

# CELL ********************

# Determine which value set file to update
value_set_path = f"valueSets/{environment}.json"

if value_set_path not in decoded_parts:
    print(f"Warning: Value set {value_set_path} not found. Creating new one.")
    decoded_parts[value_set_path] = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/valueSet/1.0.0/schema.json",
        "name": environment,
        "variableOverrides": []
    }

value_set = decoded_parts[value_set_path]
current_overrides = {vo["name"]: vo for vo in value_set.get("variableOverrides", [])}

print(f"\nUpdating value set: {value_set_path}")
print(f"Current overrides: {list(current_overrides.keys())}")

# Update or add each discovered value
for var_name, value in discovered_values.items():
    if var_name in current_overrides:
        old_value = current_overrides[var_name]["value"]
        current_overrides[var_name]["value"] = value
        if old_value != value:
            print(f"  Updated {var_name}: {old_value[:20] if old_value else '(empty)'}... -> {value[:20]}...")
        else:
            print(f"  Unchanged {var_name}")
    else:
        current_overrides[var_name] = {"name": var_name, "value": value}
        print(f"  Added {var_name}: {value[:20]}...")

# Rebuild variable overrides list
value_set["variableOverrides"] = list(current_overrides.values())
decoded_parts[value_set_path] = value_set

print(f"\nValue set now has {len(value_set['variableOverrides'])} overrides")

# Also update the active value set to match current environment
props_path = "variableLibraryProperties.json"
if props_path in decoded_parts:
    props = decoded_parts[props_path]
    old_active = props.get("activeValueSetName", "")
    props["activeValueSetName"] = environment
    decoded_parts[props_path] = props
    print(f"\nActive value set: {old_active} -> {environment}")
else:
    print(f"Warning: {props_path} not found in definition")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 6: Push Updated Definition

# CELL ********************

# Re-encode parts for API
encoded_parts = []
for path, content in decoded_parts.items():
    if isinstance(content, dict):
        content_str = json.dumps(content, indent=2)
    else:
        content_str = content

    encoded_parts.append({
        "path": path,
        "payload": base64.b64encode(content_str.encode("utf-8")).decode("utf-8"),
        "payloadType": "InlineBase64"
    })

# Build update request
update_request = {
    "definition": {
        "parts": encoded_parts
    }
}

print(f"Updating Variable Library with {len(encoded_parts)} parts...")

# Push updated definition
response = client.post(
    f"v1/workspaces/{workspace_id}/items/{vl_id}/updateDefinition?updateMetadata=true",
    json=update_request
)

if response.status_code in [200, 202]:
    print(f"Variable Library updated successfully (status: {response.status_code})")
else:
    print(f"Warning: Update returned status {response.status_code}")
    print(response.text)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary

# CELL ********************

print("=" * 60)
print("Variable Library Configuration Complete")
print("=" * 60)
print(f"Environment: {environment}")
print(f"Workspace: {workspace_name}")
print(f"Values configured: {len(discovered_values)}")
print()
for var_name, value in sorted(discovered_values.items()):
    # Truncate long values for display
    display_value = value[:40] + "..." if len(value) > 40 else value
    print(f"  {var_name}: {display_value}")
print("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
