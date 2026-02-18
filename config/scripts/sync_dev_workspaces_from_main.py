"""
Sync dev workspaces from main branch after PR merge.

This script is triggered by GitHub Actions when a PR is merged into main.
It updates the dev workspaces (av01-dev-processing, av01-dev-datastores, etc.)
with the latest content from the main branch.
"""

import os
import sys

from fabric_core import auth, get_workspace_id, update_workspace_from_git, bootstrap
from fabric_core.utils import load_config


def main():
    bootstrap()

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
        print("Warning: No dev workspaces configured with Git integration found")
        return

    print("=== AUTHENTICATING ===")
    if not auth():
        print("\nERROR: Authentication failed. Cannot proceed with workspace sync.")
        sys.exit(1)

    print(f"\n=== SYNCING DEV WORKSPACES FROM MAIN ===")
    print(f"Solution version: {solution_version}")
    print(f"Workspaces to sync: {len(dev_workspaces)}\n")

    failures = 0
    for workspace_config in dev_workspaces:
        workspace_name = workspace_config['name'].replace(
            '{{SOLUTION_VERSION}}', solution_version)

        print(f"--- Syncing {workspace_name} ---")

        # Get workspace ID
        workspace_id = get_workspace_id(workspace_name)

        if not workspace_id:
            print(f"  Warning: Workspace not found: {workspace_name}")
            failures += 1
            continue

        # Update workspace from Git (pull latest from main branch)
        success = update_workspace_from_git(workspace_id, workspace_name)

        if not success:
            print(f"  Warning: Failed to update {workspace_name} from Git")
            failures += 1

    print(f"\n Dev workspace sync complete ({len(dev_workspaces) - failures}/{len(dev_workspaces)} succeeded)")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
