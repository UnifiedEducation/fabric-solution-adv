"""Item deployment module for deploying Fabric items via Item Definition API.

This module implements Microsoft's "Option 2" CI/CD pattern, allowing items
to be deployed to workspaces that are not connected to Git. Items are read
from the Git folder structure and pushed via the Item Definition API.

Uses direct REST API calls instead of the Fabric CLI for better reliability.

Supported item types:
- Notebook (.Notebook/)
- Lakehouse (.Lakehouse/)
- DataPipeline (.DataPipeline/)
- Environment (.Environment/)
- VariableLibrary (.VariableLibrary/)
- SQLDatabase (.SQLDatabase/)
"""

import base64
import json
import os
import time
from pathlib import Path

import requests

# Fabric API base URL
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"


def _get_fabric_access_token():
    """
    Get an access token for the Fabric API using service principal credentials.

    Returns:
        str: Access token, or None if failed
    """
    tenant_id = os.getenv('AZURE_TENANT_ID')
    client_id = os.getenv('SPN_CLIENT_ID')
    client_secret = os.getenv('SPN_CLIENT_SECRET')

    if not all([tenant_id, client_id, client_secret]):
        print("  Missing required environment variables for authentication")
        return None

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://api.fabric.microsoft.com/.default',
        'grant_type': 'client_credentials'
    }

    try:
        response = requests.post(token_url, data=token_data)
        response.raise_for_status()
        return response.json().get('access_token')
    except requests.RequestException as e:
        print(f"  Failed to get access token: {e}")
        return None


def _fabric_api_request(method, endpoint, json_body=None):
    """
    Make a request to the Fabric REST API.

    Args:
        method: HTTP method ('GET', 'POST', etc.)
        endpoint: API endpoint (e.g., 'workspaces/{id}/items')
        json_body: Optional request body dict

    Returns:
        tuple: (success: bool, status_code: int, response_json: dict)
    """
    token = _get_fabric_access_token()
    if not token:
        return False, 0, {'error': 'Failed to get access token'}

    url = f"{FABRIC_API_BASE}/{endpoint}"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.request(method, url, headers=headers, json=json_body)

        # Parse response body if present
        try:
            response_json = response.json() if response.text else {}
        except json.JSONDecodeError:
            response_json = {'raw_response': response.text}

        return True, response.status_code, response_json

    except requests.RequestException as e:
        return False, 0, {'error': str(e)}


def list_workspace_items(workspace_id):
    """
    List all items in a workspace.

    Args:
        workspace_id: Workspace UUID

    Returns:
        dict: Mapping of {displayName: {id, type}} for all items in workspace
    """
    success, status_code, response = _fabric_api_request(
        'GET', f'workspaces/{workspace_id}/items'
    )

    if not success:
        print(f"  Failed to list workspace items: {response.get('error')}")
        return {}

    if status_code != 200:
        print(f"  API returned status {status_code}")
        return {}

    items = response.get('value', [])
    return {
        item['displayName']: {'id': item['id'], 'type': item['type']}
        for item in items
    }


def read_item_from_git(item_folder_path):
    """
    Read item definition from Git folder structure.

    Args:
        item_folder_path: Path to item folder (e.g., 'solution/processing/notebooks/nb-av01-1-load.Notebook')

    Returns:
        dict: {type, displayName, parts: [{path, content}]} or None if failed
    """
    folder_path = Path(item_folder_path)

    if not folder_path.exists():
        print(f"  Item folder not found: {folder_path}")
        return None

    # Read .platform file for metadata
    platform_file = folder_path / '.platform'
    if not platform_file.exists():
        print(f"  .platform file not found in: {folder_path}")
        return None

    try:
        with open(platform_file, 'r', encoding='utf-8') as f:
            platform_content = f.read()
            platform_data = json.loads(platform_content)
    except (json.JSONDecodeError, IOError) as e:
        print(f"  Failed to read .platform: {e}")
        return None

    item_type = platform_data.get('metadata', {}).get('type')
    display_name = platform_data.get('metadata', {}).get('displayName')

    if not item_type or not display_name:
        print(f"  Invalid .platform file - missing type or displayName")
        return None

    # Collect all files as parts
    parts = []
    for file_path in folder_path.rglob('*'):
        if file_path.is_file():
            relative_path = file_path.relative_to(folder_path).as_posix()

            try:
                # Read file content
                with open(file_path, 'rb') as f:
                    content = f.read()

                parts.append({
                    'path': relative_path,
                    'content': content
                })
            except IOError as e:
                print(f"  Warning: Failed to read {relative_path}: {e}")

    return {
        'type': item_type,
        'displayName': display_name,
        'parts': parts
    }


def _encode_parts_for_api(parts):
    """
    Encode parts for the Fabric API (Base64 encoding).

    Args:
        parts: List of {path, content} dicts

    Returns:
        List of {path, payload, payloadType} dicts ready for API
    """
    return [
        {
            'path': part['path'],
            'payload': base64.b64encode(part['content']).decode('utf-8'),
            'payloadType': 'InlineBase64'
        }
        for part in parts
    ]


def create_item_with_definition(workspace_id, item_type, display_name, parts):
    """
    Create a new item with definition.

    Args:
        workspace_id: Workspace UUID
        item_type: Item type (e.g., 'Notebook', 'Lakehouse')
        display_name: Display name for the item
        parts: List of {path, content} dicts

    Returns:
        str: Item ID if successful, None otherwise
    """
    request_body = {
        'displayName': display_name,
        'type': item_type,
        'definition': {
            'parts': _encode_parts_for_api(parts)
        }
    }

    success, status_code, response = _fabric_api_request(
        'POST', f'workspaces/{workspace_id}/items', request_body
    )

    if not success:
        print(f"  Failed to create item: {response.get('error', 'Unknown error')}")
        return None

    # 201 = created, 202 = accepted (long-running operation)
    if status_code in [201, 202]:
        item_id = response.get('id')

        if status_code == 202:
            # Long-running operation - wait for completion
            print(f"  Item creation in progress...")
            time.sleep(5)  # Brief wait for async creation

        return item_id
    else:
        print(f"  API returned status {status_code}: {response}")
        return None


def update_item_definition(workspace_id, item_id, parts):
    """
    Update an existing item's definition.

    Args:
        workspace_id: Workspace UUID
        item_id: Item UUID
        parts: List of {path, content} dicts

    Returns:
        bool: True if successful, False otherwise
    """
    request_body = {
        'definition': {
            'parts': _encode_parts_for_api(parts)
        }
    }

    # Include updateMetadata=true to update .platform metadata
    success, status_code, response = _fabric_api_request(
        'POST',
        f'workspaces/{workspace_id}/items/{item_id}/updateDefinition?updateMetadata=true',
        request_body
    )

    if not success:
        print(f"  Failed to update item definition: {response.get('error', 'Unknown error')}")
        return False

    # 200 = success, 202 = accepted (long-running operation)
    if status_code in [200, 202]:
        if status_code == 202:
            print(f"  Update in progress...")
            time.sleep(2)
        return True
    else:
        print(f"  API returned status {status_code}: {response}")
        return False


def deploy_item(workspace_id, item_folder_path, existing_items):
    """
    Deploy a single item to a workspace (create if new, update if exists).

    Args:
        workspace_id: Workspace UUID
        item_folder_path: Path to item folder in Git
        existing_items: Dict mapping displayName to {id, type}

    Returns:
        dict: {success, action, displayName, error}
    """
    # Read item from Git
    item_data = read_item_from_git(item_folder_path)
    if not item_data:
        return {
            'success': False,
            'action': 'read',
            'displayName': Path(item_folder_path).name,
            'error': 'Failed to read item from Git'
        }

    display_name = item_data['displayName']
    item_type = item_data['type']
    parts = item_data['parts']

    # Check if item exists
    existing = existing_items.get(display_name)

    if existing:
        # Update existing item
        print(f"  Updating existing {item_type}: {display_name}")
        success = update_item_definition(workspace_id, existing['id'], parts)
        return {
            'success': success,
            'action': 'update',
            'displayName': display_name,
            'error': None if success else 'Update failed'
        }
    else:
        # Create new item
        print(f"  Creating new {item_type}: {display_name}")
        item_id = create_item_with_definition(workspace_id, item_type, display_name, parts)
        return {
            'success': item_id is not None,
            'action': 'create',
            'displayName': display_name,
            'error': None if item_id else 'Create failed'
        }


def find_item_folders(base_path):
    """
    Find all Fabric item folders in a directory.

    Args:
        base_path: Base path to search (e.g., 'solution/processing')

    Returns:
        list: List of paths to item folders
    """
    base = Path(base_path)
    if not base.exists():
        return []

    # Fabric item folders end with type suffix
    item_suffixes = [
        '.Notebook', '.Lakehouse', '.DataPipeline',
        '.Environment', '.VariableLibrary', '.SQLDatabase',
        '.SemanticModel', '.Report', '.Warehouse'
    ]

    item_folders = []
    for suffix in item_suffixes:
        # Find all folders matching the pattern
        for folder in base.rglob(f'*{suffix}'):
            if folder.is_dir() and (folder / '.platform').exists():
                item_folders.append(folder)

    return item_folders


def deploy_items_to_workspace(workspace_id, workspace_name, items_source_path):
    """
    Deploy all items from a source path to a workspace.

    Args:
        workspace_id: Workspace UUID
        workspace_name: Workspace name (for logging)
        items_source_path: Path to folder containing items (e.g., 'solution/processing')

    Returns:
        dict: {succeeded: [], failed: [], total}
    """
    print(f"\n--- Deploying items to {workspace_name} ---")
    print(f"  Source path: {items_source_path}")

    # List existing items in workspace
    print(f"  Listing existing items...")
    existing_items = list_workspace_items(workspace_id)
    print(f"  Found {len(existing_items)} existing items")

    # Find all item folders in source path
    item_folders = find_item_folders(items_source_path)
    print(f"  Found {len(item_folders)} items to deploy")

    if not item_folders:
        print(f"  No items found in {items_source_path}")
        return {'succeeded': [], 'failed': [], 'total': 0}

    # Deploy each item
    succeeded = []
    failed = []

    for folder in item_folders:
        result = deploy_item(workspace_id, folder, existing_items)

        if result['success']:
            print(f"  + {result['action'].capitalize()}d: {result['displayName']}")
            succeeded.append(result['displayName'])
        else:
            print(f"  x {result['action'].capitalize()} failed: {result['displayName']} - {result['error']}")
            failed.append(result['displayName'])

    print(f"\n  Deployment complete: {len(succeeded)} succeeded, {len(failed)} failed")

    return {
        'succeeded': succeeded,
        'failed': failed,
        'total': len(item_folders)
    }
