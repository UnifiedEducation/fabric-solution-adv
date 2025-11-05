"""
Sync dev workspaces from main branch after PR merge.

This script is triggered by GitHub Actions when a PR is merged into main.
It updates the dev workspaces (av01-dev-processing, av01-dev-datastores, etc.)
with the latest content from the main branch.
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
from fabric_core import auth, get_workspace_id, update_workspace_from_git
from fabric_core.utils import load_config
# fmt: on


# Ensure UTF-8 encoding for stdout
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


def main():
    # Load environment variables if not in GitHub Actions
    if not os.getenv('GITHUB_ACTIONS'):
        # Load .env from project root (2 levels up from scripts/)
        load_dotenv(Path(__file__).parent.parent.parent / '.env')

    # Load config to get solution version and workspace configuration
    config = load_config(
        os.getenv('CONFIG_FILE', 'config/templates/v01/v01-template.yml'))

    solution_version = config.get('solution_version', 'av01')
    workspaces_config = config.get('workspaces', [])

    # Filter for dev workspaces that are connected to Git
    dev_workspaces = [
        ws for ws in workspaces_config
        if '-dev-' in ws.get('name', '') and ws.get('connect_to_git_folder')
    ]

    if not dev_workspaces:
        print("⚠ No dev workspaces configured with Git integration found")
        return

    print("=== AUTHENTICATING ===")
    if not auth():
        print("\n✗ Authentication failed. Cannot proceed with workspace sync.")
        return

    print(f"\n=== SYNCING DEV WORKSPACES FROM MAIN ===")
    print(f"Solution version: {solution_version}")
    print(f"Workspaces to sync: {len(dev_workspaces)}\n")

    for workspace_config in dev_workspaces:
        workspace_name = workspace_config['name'].replace(
            '{{SOLUTION_VERSION}}', solution_version)

        print(f"--- Syncing {workspace_name} ---")

        # Get workspace ID
        workspace_id = get_workspace_id(workspace_name)

        if not workspace_id:
            print(f"  ⚠ Workspace not found: {workspace_name}")
            continue

        # Update workspace from Git (pull latest from main branch)
        success = update_workspace_from_git(workspace_id, workspace_name)

        if not success:
            print(f"  ⚠ Failed to update {workspace_name} from Git")

    print("\n✓ Dev workspace sync complete")


if __name__ == "__main__":
    main()
