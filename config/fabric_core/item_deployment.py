"""Item deployment module for deploying Fabric items via Item Definition API.

This module implements Microsoft's "Option 2" CI/CD pattern, allowing items
to be deployed to workspaces that are not connected to Git. Items are read
from the Git folder structure and pushed via the Item Definition API.

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
import time
from pathlib import Path
from .utils import get_fabric_cli_path, run_command


def list_workspace_items(workspace_id):
    """
    List all items in a workspace.

    Args:
        workspace_id: Workspace UUID

    Returns:
        dict: Mapping of {displayName: {id, type}} for all items in workspace
    """
    result = run_command([
        get_fabric_cli_path(), 'api', '-X', 'get',
        f'workspaces/{workspace_id}/items'
    ])

    if result.returncode != 0:
        print(f"  Failed to list workspace items: {result.stderr}")
        return {}

    try:
        response_json = json.loads(result.stdout)
        if response_json.get('status_code') != 200:
            print(f"  API returned status {response_json.get('status_code')}")
            return {}

        items = response_json.get('text', {}).get('value', [])
        return {
            item['displayName']: {'id': item['id'], 'type': item['type']}
            for item in items
        }
    except (json.JSONDecodeError, KeyError) as e:
        print(f"  Failed to parse items response: {e}")
        return {}


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

    result = run_command([
        get_fabric_cli_path(), 'api', '-X', 'post',
        f'workspaces/{workspace_id}/items',
        '-i', json.dumps(request_body)
    ])

    if result.returncode != 0:
        print(f"  Failed to create item: {result.stderr}")
        return None

    try:
        response_json = json.loads(result.stdout)
        status_code = response_json.get('status_code', 0)

        # 201 = created, 202 = accepted (long-running operation)
        if status_code in [201, 202]:
            response_text = response_json.get('text', {})
            item_id = response_text.get('id')

            if status_code == 202:
                # Long-running operation - wait for completion
                print(f"  Item creation in progress...")
                time.sleep(5)  # Brief wait for async creation

            return item_id
        else:
            error = response_json.get('text', {})
            print(f"  API returned status {status_code}: {error}")
            return None

    except json.JSONDecodeError as e:
        print(f"  Failed to parse response: {e}")
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
    result = run_command([
        get_fabric_cli_path(), 'api', '-X', 'post',
        f'workspaces/{workspace_id}/items/{item_id}/updateDefinition?updateMetadata=true',
        '-i', json.dumps(request_body)
    ])

    if result.returncode != 0:
        print(f"  Failed to update item definition: {result.stderr}")
        return False

    try:
        response_json = json.loads(result.stdout)
        status_code = response_json.get('status_code', 0)

        # 200 = success, 202 = accepted (long-running operation)
        if status_code in [200, 202]:
            if status_code == 202:
                print(f"  Update in progress...")
                time.sleep(2)
            return True
        else:
            error = response_json.get('text', {})
            print(f"  API returned status {status_code}: {error}")
            return False

    except json.JSONDecodeError as e:
        print(f"  Failed to parse response: {e}")
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
            print(f"  ✓ {result['action'].capitalize()}d: {result['displayName']}")
            succeeded.append(result['displayName'])
        else:
            print(f"  ✗ {result['action'].capitalize()} failed: {result['displayName']} - {result['error']}")
            failed.append(result['displayName'])

    print(f"\n  Deployment complete: {len(succeeded)} succeeded, {len(failed)} failed")

    return {
        'succeeded': succeeded,
        'failed': failed,
        'total': len(item_folders)
    }
