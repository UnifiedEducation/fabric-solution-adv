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

    list_response = run_command(
        [get_fabric_cli_path(), 'api', '-X', 'get', 'connections'])
    list_json = json.loads(list_response.stdout)

    if list_json.get('status_code') == 200:
        connections = list_json.get('text', {}).get('value', [])
        for conn in connections:
            if conn.get('displayName') == connection_name:
                print(f"✓ Using existing connection: {connection_name}")
                return conn.get('id')

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

    create_response = run_command([get_fabric_cli_path(
    ), 'api', '-X', 'post', 'connections', '-i', json.dumps(request_body)])
    create_json = json.loads(create_response.stdout)

    if create_json.get('status_code') in [200, 201]:
        connection_id = create_json.get('text', {}).get('id')
        print(f"✓ Created connection: {connection_name}")
        return connection_id

    return None


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

    connect_response = run_command([get_fabric_cli_path(), 'api', '-X', 'post', f'workspaces/{workspace_id}/git/connect',
                                    '-i', json.dumps(request_body)])

    # Handle empty or invalid JSON response
    if not connect_response.stdout.strip():
        print(f"✗ Failed to connect {workspace_name} to Git: Empty response")
        print(f"  stderr: {connect_response.stderr}")
        return False

    try:
        connect_json = json.loads(connect_response.stdout)
    except json.JSONDecodeError:
        print(f"✗ Failed to connect {workspace_name} to Git: Invalid JSON")
        print(f"  stdout: {connect_response.stdout}")
        print(f"  stderr: {connect_response.stderr}")
        return False

    if connect_json.get('status_code') in [200, 201]:
        print(f"✓ Connected {workspace_name} to Git: {git_config.get('branch')}/{directory_name}")
        return True

    print(f"✗ Failed to connect {workspace_name} to Git")
    print(f"  Status: {connect_json.get('status_code')}")
    print(f"  Response: {connect_json.get('text', {})}")
    return False
