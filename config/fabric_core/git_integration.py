"""Git integration module for connecting Fabric workspaces to GitHub."""

import os
import json
from .utils import get_fabric_cli_path, run_command


def get_or_create_git_connection(workspace_id, git_config):
    """
    Get existing or create new GitHub connection.

    Args:
        workspace_id: Workspace UUID (used for connection creation context)
        git_config: Dict with 'organization' and 'repository' keys

    Returns:
        str: Connection ID if successful, None otherwise
    """
    owner_name = git_config.get('organization')
    repo_name = git_config.get('repository')
    connection_name = f"GitHub-{owner_name}-{repo_name}"

    # Check for existing connection
    list_response = run_command([get_fabric_cli_path(), 'api', '-X', 'get', 'connections'])
    list_json = json.loads(list_response.stdout)

    if list_json.get('status_code') == 200:
        connections = list_json.get('text', {}).get('value', [])
        for conn in connections:
            if conn.get('displayName') == connection_name:
                print(f"✓ Using existing connection: {connection_name}")
                return conn.get('id')

    # Create new connection
    github_url = f"https://github.com/{owner_name}/{repo_name}"
    request_body = {
        "connectivityType": "ShareableCloud",
        "displayName": connection_name,
        "connectionDetails": {
            "type": "GitHubSourceControl",
            "creationMethod": "GitHubSourceControl.Contents",
            "parameters": [{"dataType": "Text", "name": "url", "value": github_url}]
        },
        "credentialDetails": {
            "credentials": {"credentialType": "Key", "key": os.getenv('GITHUB_PAT')}
        }
    }

    create_response = run_command([
        get_fabric_cli_path(), 'api', '-X', 'post', 'connections',
        '-i', json.dumps(request_body)
    ])
    create_json = json.loads(create_response.stdout)

    if create_json.get('status_code') in [200, 201]:
        connection_id = create_json.get('text', {}).get('id')
        print(f"✓ Created connection: {connection_name}")
        return connection_id

    return None


def update_workspace_from_git(workspace_id, workspace_name):
    """
    Update workspace content from Git (pull from Git).

    This syncs the Git repository content into the workspace.
    """
    # Get Git status to retrieve remoteCommitHash
    status_response = run_command([
        get_fabric_cli_path(), 'api', '-X', 'get',
        f'workspaces/{workspace_id}/git/status'
    ])

    if not status_response.stdout.strip():
        print(f"  ⚠ Failed to get Git status")
        return False

    try:
        status_json = json.loads(status_response.stdout)
        status_code = status_json.get('status_code')

        # Handle uninitialized connection
        if status_code == 400:
            error_text = status_json.get('text', {})
            if error_text.get('errorCode') == 'WorkspaceGitConnectionNotInitialized':
                print(f"  → Initializing Git connection")
                run_command([
                    get_fabric_cli_path(), 'api', '-X', 'post',
                    f'workspaces/{workspace_id}/git/initializeConnection',
                    '-i', '{}'
                ])
                # Retry getting status after initialization
                status_response = run_command([
                    get_fabric_cli_path(), 'api', '-X', 'get',
                    f'workspaces/{workspace_id}/git/status'
                ])
                status_json = json.loads(status_response.stdout)
                status_code = status_json.get('status_code')

        if status_code != 200:
            error_text = status_json.get('text', {})
            print(f"  ⚠ Failed to get Git status: {status_code}")
            print(f"     Error: {error_text}")
            return False

        remote_commit_hash = status_json.get('text', {}).get('remoteCommitHash')
        if not remote_commit_hash:
            print(f"  ⚠ No remoteCommitHash found in status")
            return False
    except json.JSONDecodeError:
        print(f"  ⚠ Failed to parse Git status")
        return False

    # Update from Git using the remoteCommitHash
    update_request = {
        "remoteCommitHash": remote_commit_hash,
        "conflictResolution": {
            "conflictResolutionType": "Workspace",
            "conflictResolutionPolicy": "PreferWorkspace"
        },
        "options": {"allowOverrideItems": True}
    }

    update_response = run_command([
        get_fabric_cli_path(), 'api', '-X', 'post',
        f'workspaces/{workspace_id}/git/updateFromGit',
        '-i', json.dumps(update_request)
    ])

    if not update_response.stdout.strip():
        return True  # Empty response is acceptable

    try:
        response_json = json.loads(update_response.stdout)
        if response_json.get('status_code') in [200, 201, 202]:
            print(f"  ✓ Updated {workspace_name} from Git")
            return True
    except json.JSONDecodeError:
        pass

    print(f"  ⚠ Update from Git may have failed")
    return False


def connect_workspace_to_git(workspace_id, workspace_name, directory_name, git_config, connection_id):
    """
    Connect a Fabric workspace to a Git repository.

    Args:
        workspace_id: Workspace UUID
        workspace_name: Workspace display name (for logging)
        directory_name: Directory path in the repository
        git_config: Dict with 'organization', 'provider', 'repository', 'branch'
        connection_id: GitHub connection ID

    Returns:
        bool: True if successful, False otherwise
    """
    # Check if workspace is already connected to Git
    status_response = run_command([
        get_fabric_cli_path(), 'api', '-X', 'get',
        f'workspaces/{workspace_id}/git/status'
    ])

    if status_response.stdout.strip():
        try:
            status_json = json.loads(status_response.stdout)
            if status_json.get('status_code') == 200:
                git_status = status_json.get('text', {})
                # Check for connection indicators (either field means already connected)
                if git_status.get('gitConnectionState') or git_status.get('remoteCommitHash'):
                    print(f"✓ {workspace_name} already connected to Git")
                    return True
        except json.JSONDecodeError:
            pass  # Continue with connection attempt if parsing fails

    # Connect workspace to Git
    request_body = {
        "gitProviderDetails": {
            "ownerName": git_config.get('organization'),
            "gitProviderType": git_config.get('provider'),
            "repositoryName": git_config.get('repository'),
            "branchName": git_config.get('branch'),
            "directoryName": directory_name
        },
        "myGitCredentials": {
            "source": "ConfiguredConnection",
            "connectionId": connection_id
        }
    }

    connect_response = run_command([
        get_fabric_cli_path(), 'api', '-X', 'post',
        f'workspaces/{workspace_id}/git/connect',
        '-i', json.dumps(request_body)
    ])

    if not connect_response.stdout.strip():
        print(f"✗ Failed to connect {workspace_name} to Git: Empty response")
        return False

    try:
        connect_json = json.loads(connect_response.stdout)
    except json.JSONDecodeError:
        print(f"✗ Failed to connect {workspace_name} to Git: Invalid JSON")
        return False

    if connect_json.get('status_code') in [200, 201]:
        print(f"✓ Connected {workspace_name} to Git: {git_config.get('branch')}/{directory_name}")
        return True

    print(f"✗ Failed to connect {workspace_name} to Git")
    print(f"  Status: {connect_json.get('status_code')}")
    print(f"  Response: {connect_json.get('text', {})}")
    return False
