# Please note: the code in this Python file has intentionally been written WITHOUT things like:
# testing, logging, error-handling, validation, documentation, comments etc
# for now I'm trying to make it as simple as possible to follow the logic
# In future weeks, we'll refactor the code to make it more robust!
# Also, it's purposefully in one Python file for simplicity.
# In the following weeks, we'll refactor, and split into modules.

import os
import sys
import yaml
import json
import subprocess
import time
import shutil
import re
from string import Template
from pathlib import Path
from dotenv import load_dotenv

# Ensure UTF-8 encoding for stdout to support Unicode characters (like checkmarks)
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


def get_fabric_cli_path():
    """Get the path to the Fabric CLI executable."""
    return str(Path(sys.prefix) / 'Scripts' / 'fab.exe') if hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix and (Path(sys.prefix) / 'Scripts' / 'fab.exe').exists() else shutil.which('fab') or 'fab'


def run_command(cmd):
    """Execute a subprocess command and return the result."""
    return subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')


def call_azure_api(endpoint, method='get', body=None):
    """Call Azure ARM REST API via Fabric CLI and return status code and response body."""
    cmd = [get_fabric_cli_path(), 'api', endpoint, '-X', method, '-A', 'azure']
    if body:
        cmd.extend(['-i', json.dumps(body)])
    response = run_command(cmd)
    try:
        return json.loads(response.stdout).get('status_code', 0), json.loads(response.stdout).get('text', {})
    except:
        return 0, {}


def load_config(config_file_path):
    """Load YAML configuration file with environment variable substitution."""
    with open(config_file_path) as file:
        yaml_content = file.read()
    temp_config = yaml.safe_load(yaml_content)
    yaml_content = yaml_content.replace(
        '{{SOLUTION_VERSION}}', temp_config.get('solution_version', 'AV01'))
    return yaml.safe_load(Template(yaml_content).safe_substitute(os.environ))


def auth():
    """Authenticate with Azure using service principal credentials."""
    run_command([get_fabric_cli_path(), 'auth', 'login', '-u', os.getenv('SPN_CLIENT_ID'),
                '-p', os.getenv('SPN_CLIENT_SECRET'), '--tenant', os.getenv('AZURE_TENANT_ID')])


def capacity_exists(capacity_name, subscription_id, resource_group):
    """Check if a Fabric capacity exists in Azure."""
    status, _ = call_azure_api(
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version=2023-11-01")
    return status == 200


def create_capacity(capacity_config, subscription_id, resource_group, defaults):
    capacity_name = capacity_config['name']

    if capacity_exists(capacity_name, subscription_id, resource_group):
        print(f"✓ {capacity_name} exists")
        call_azure_api(
            f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/resume?api-version=2023-11-01", 'post')
        return

    admin_members = capacity_config.get(
        'admin_members', defaults.get('capacity_admins', ''))
    admin_members = admin_members if isinstance(admin_members, list) else [
        admin_id.strip() for admin_id in admin_members.split(',') if admin_id.strip()]

    request_body = {
        "location": capacity_config.get('region', defaults.get('region')),
        "sku": {"name": capacity_config.get('sku', defaults.get('sku')), "tier": "Fabric"},
        "properties": {"administration": {"members": admin_members}}
    }

    status, response = call_azure_api(
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version=2023-11-01", 'put', request_body)

    if status in [200, 201]:
        print(f"✓ Created {capacity_name}")
        time.sleep(30)


def workspace_exists(workspace_name):
    """Check if a Fabric workspace exists."""
    return run_command([get_fabric_cli_path(), 'ls', f'{workspace_name}.Workspace']).returncode == 0


def get_workspace_id(workspace_name):
    """Get the UUID of a workspace by name."""
    response = run_command(
        [get_fabric_cli_path(), 'get', f'{workspace_name}.Workspace', '-q', 'id'])
    if response.returncode == 0:
        workspace_id = response.stdout.strip()
        if workspace_id:
            return workspace_id
    uuid_match = re.search(
        r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', response.stdout.lower())
    return uuid_match.group() if uuid_match else None


def create_workspace(workspace_config):
    workspace_name = workspace_config['name']

    if workspace_exists(workspace_name):
        print(f"✓ {workspace_name} exists")
        return get_workspace_id(workspace_name)

    run_command([get_fabric_cli_path(), 'create',
                f'{workspace_name}.Workspace', '-P', f'capacityname={workspace_config["capacity"]}'])
    print(f"✓ Created {workspace_name}")

    time.sleep(5)
    return get_workspace_id(workspace_name)


def assign_permissions(workspace_id, permissions, security_groups):
    time.sleep(10)

    for permission in permissions:
        group_id = security_groups.get(permission.get('group'))

        request_body = {
            "principal": {
                "id": group_id,
                "type": "Group",
                "groupDetails": {"groupType": "SecurityGroup"}
            },
            "role": permission.get('role')
        }

        assign_response = run_command([get_fabric_cli_path(
        ), 'api', '-X', 'post', f'workspaces/{workspace_id}/roleAssignments', '-i', json.dumps(request_body)])
        response_json = json.loads(assign_response.stdout)

        if response_json.get('status_code') in [200, 201]:
            print(
                f"  ✓ Assigned {permission['role']} to {permission['group']}")


def get_or_create_git_connection(workspace_id, git_config):
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
    connect_json = json.loads(connect_response.stdout)

    if connect_json.get('status_code') in [200, 201]:
        print(f"✓ Connected {workspace_name} to Git: {directory_name}")
        return True

    return False


def suspend_capacity(capacity_name, subscription_id, resource_group):
    """Suspend a Fabric capacity to stop billing."""
    for _ in range(5):
        status, _ = call_azure_api(
            f"subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/suspend?api-version=2023-11-01", 'post')
        if status in [200, 202]:
            print(f"✓ Suspended {capacity_name}")
            return True
        time.sleep(60)
    print(f"✗ Failed to suspend {capacity_name}")
    return False


def main():
    if not os.getenv('GITHUB_ACTIONS'):
        load_dotenv(Path(__file__).parent.parent / '.env')

    config = load_config(
        os.getenv('CONFIG_FILE', 'config/templates/v01/v01-template.yml'))

    print("=== AUTHENTICATING ===")
    auth()

    azure_config = config['azure']
    subscription_id = azure_config['subscription_id']
    capacity_defaults = azure_config.get('capacity_defaults', {})
    security_groups = azure_config.get('security_groups', {})
    git_config = config.get('github', {})

    print("\n=== CREATING CAPACITIES ===")
    for capacity_config in config.get('capacities', []):
        resource_group = capacity_config.get(
            'resource_group', capacity_defaults.get('resource_group'))
        create_capacity(capacity_config, subscription_id,
                        resource_group, capacity_defaults)

    print("\n=== CREATING WORKSPACES ===")
    github_connection_id = None

    for workspace_config in config.get('workspaces', []):
        workspace_id = create_workspace(workspace_config)

        if 'permissions' in workspace_config and workspace_id:
            assign_permissions(
                workspace_id, workspace_config['permissions'], security_groups)

        if 'connect_to_git_folder' in workspace_config and workspace_id and git_config:
            if not github_connection_id:
                github_connection_id = get_or_create_git_connection(
                    workspace_id, git_config)

            if github_connection_id:
                connect_workspace_to_git(workspace_id, workspace_config['name'],
                                         workspace_config['connect_to_git_folder'],
                                         git_config, github_connection_id)

    print("\n=== SUSPENDING CAPACITIES ===")
    time.sleep(20)
    for capacity_config in config.get('capacities', []):
        resource_group = capacity_config.get(
            'resource_group', capacity_defaults.get('resource_group'))
        suspend_capacity(capacity_config['name'],
                         subscription_id, resource_group)

    print("\n✓ Done")


if __name__ == "__main__":
    main()
