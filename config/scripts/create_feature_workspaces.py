"""
Create feature workspaces for development branches.

This script is designed to be called from GitHub Actions workflow_dispatch.
It creates workspaces for a feature branch and connects them to Git.
"""

# fmt: off
# isort: skip_file
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add config directory to Python path to find fabric_core module
config_dir = Path(__file__).parent.parent
if str(config_dir) not in sys.path:
    sys.path.insert(0, str(config_dir))

# Import from fabric_core modules (must be after sys.path modification)
from fabric_core import auth, create_workspace, assign_permissions
from fabric_core import get_or_create_git_connection, connect_workspace_to_git, update_workspace_from_git
from fabric_core.utils import load_config, run_command, get_fabric_cli_path
import json
# fmt: on


# Ensure UTF-8 encoding for stdout
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


def get_capacity_for_workspace_type(workspace_type, solution_version):
    """Determine which capacity to use based on workspace type."""
    capacity_map = {
        'processing': f'fc{solution_version}devengineering',
        'datastores': f'fc{solution_version}devengineering',
        'consumption': f'fc{solution_version}devconsumption'
    }
    return capacity_map.get(workspace_type)


def main():
    # Load environment variables if not in GitHub Actions
    if not os.getenv('GITHUB_ACTIONS'):
        # Load .env from project root (2 levels up from scripts/)
        load_dotenv(Path(__file__).parent.parent.parent / '.env')

    # Get inputs from environment (set by GitHub Actions workflow)
    feature_branch = os.getenv('FEATURE_BRANCH_NAME')
    workspaces_input = os.getenv(
        'WORKSPACES_TO_CREATE', 'processing,datastores')

    # Parse workspace types (comma-separated)
    workspace_types = [ws.strip()
                       for ws in workspaces_input.split(',') if ws.strip()]

    # Load config to get solution version, security groups, and git config
    config = load_config(
        os.getenv('CONFIG_FILE', 'config/templates/v01/v01-template.yml'))

    solution_version = config.get('solution_version', 'av01')
    azure_config = config['azure']
    security_groups = azure_config.get('security_groups', {})
    git_config = config.get('github', {})

    # Override git branch to use feature branch
    git_config['branch'] = feature_branch

    print("=== AUTHENTICATING ===")
    if not auth():
        print("\n✗ Authentication failed. Cannot proceed with workspace creation.")
        return

    print(
        f"\n=== CREATING FEATURE WORKSPACES FOR BRANCH: {feature_branch} ===")
    github_connection_id = None

    for workspace_type in workspace_types:
        # Construct workspace name: <solution_version>-<branch>-<type>
        workspace_name = f"{solution_version}-{feature_branch}-{workspace_type}"

        # Get capacity for this workspace type
        capacity_name = get_capacity_for_workspace_type(
            workspace_type, solution_version)

        if not capacity_name:
            print(f"✗ Unknown workspace type: {workspace_type}")
            continue

        print(f"\n--- Creating {workspace_name} ---")

        # Create workspace
        workspace_config = {
            'name': workspace_name,
            'capacity': capacity_name
        }
        workspace_id = create_workspace(workspace_config)

        if workspace_id:
            # Assign Engineers as Contributors
            permissions = [{'group': 'SG_AV_Engineers', 'role': 'Admin'}]
            assign_permissions(workspace_id, permissions, security_groups)

            # Connect to Git (feature branch, solution/<type>/ folder)
            if not github_connection_id:
                github_connection_id = get_or_create_git_connection(
                    workspace_id, git_config)

            if github_connection_id:
                git_directory = f"solution/{workspace_type}/"
                success = connect_workspace_to_git(
                    workspace_id,
                    workspace_name,
                    git_directory,
                    git_config,
                    github_connection_id
                )

                if success:
                    # Initialize the connection
                    init_response = run_command([
                        get_fabric_cli_path(), 'api', '-X', 'post',
                        f'workspaces/{workspace_id}/git/initializeConnection',
                        '-i', '{}'
                    ])
                    print(f"  ✓ Initialized Git connection")

                    # Pull content from Git into the workspace
                    update_workspace_from_git(workspace_id, workspace_name)

    print("\n✓ Feature workspace creation complete")


if __name__ == "__main__":
    main()
