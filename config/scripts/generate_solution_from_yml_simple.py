# Please note: the code in this Python file has intentionally been written WITHOUT things like:
# testing, logging, error-handling, validation, documentation, comments etc
# for now I'm trying to make it as simple as possible to follow the logic
# In future weeks, we'll refactor the code to make it more robust!

import os
import sys
import time
from pathlib import Path
from dotenv import load_dotenv

# Add config directory to Python path to find fabric_core module
config_dir = Path(__file__).parent.parent
if str(config_dir) not in sys.path:
    sys.path.insert(0, str(config_dir))

# Import from fabric_core modules
from fabric_core import auth, create_workspace, assign_permissions
from fabric_core import get_or_create_git_connection, connect_workspace_to_git
from fabric_core import create_capacity, suspend_capacity
from fabric_core.utils import load_config

# Ensure UTF-8 encoding for stdout to support Unicode characters (like checkmarks)
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


def main():
    if not os.getenv('GITHUB_ACTIONS'):
        # Load .env from project root (2 levels up from scripts/)
        load_dotenv(Path(__file__).parent.parent.parent / '.env')

    config = load_config(
        os.getenv('CONFIG_FILE', 'config/templates/v01/v01-template.yml'))

    print("=== AUTHENTICATING ===")
    if not auth():
        print("\nERROR: Authentication failed. Cannot proceed.")
        return

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

    print("\n Done")


if __name__ == "__main__":
    main()
