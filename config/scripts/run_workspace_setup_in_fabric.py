"""
Run workspace setup notebook in a Fabric workspace.

This script is called from GitHub Actions after deploying items to TEST/PROD
workspaces. It executes the nb-av01-new-workspace-setup notebook which:
1. Creates lakehouse schemas and tables
2. Publishes the environment
3. Seeds metadata SQL database
4. Configures variable library with workspace-specific values
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
from fabric_core import get_workspace_id, run_notebook, get_item_id
# fmt: on


# Ensure UTF-8 encoding for stdout
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


# Configuration
SETUP_NOTEBOOK_PATH = "nb-av01-new-workspace-setup.Notebook"
NOTEBOOK_TIMEOUT = 900  # 15 minutes (setup takes longer than unit tests)


def main():
    # Load environment variables if not in GitHub Actions
    if not os.getenv('GITHUB_ACTIONS'):
        load_dotenv(Path(__file__).parent.parent.parent / '.env')

    # Get workspace name from environment (set by GitHub Actions)
    workspace_name = os.getenv('SETUP_WORKSPACE_NAME')
    if not workspace_name:
        print("Error: SETUP_WORKSPACE_NAME environment variable not set")
        sys.exit(1)

    print("=== RUNNING WORKSPACE SETUP IN FABRIC ===")
    print(f"Workspace: {workspace_name}")
    print(f"Notebook: {SETUP_NOTEBOOK_PATH}")

    # Get workspace ID (REST API handles authentication internally)
    print("\n--- Getting workspace ID ---")
    workspace_id = get_workspace_id(workspace_name)
    if not workspace_id:
        print(f"✗ Workspace not found: {workspace_name}")
        sys.exit(1)
    print(f"✓ Workspace ID: {workspace_id}")

    # Get notebook ID
    print("\n--- Getting notebook ID ---")
    notebook_id = get_item_id(workspace_name, SETUP_NOTEBOOK_PATH)
    if not notebook_id:
        print(f"✗ Notebook not found: {SETUP_NOTEBOOK_PATH}")
        sys.exit(1)
    print(f"✓ Notebook ID: {notebook_id}")

    # Run the notebook
    print(f"\n--- Running notebook (timeout: {NOTEBOOK_TIMEOUT}s) ---")
    result = run_notebook(
        workspace_id=workspace_id,
        notebook_id=notebook_id,
        timeout_seconds=NOTEBOOK_TIMEOUT,
        poll_interval=15
    )

    # Report results
    print(f"\n--- Results ---")
    print(f"Status: {result['status']}")
    print(f"Job ID: {result['job_id']}")

    if result['success']:
        print("\n✓ Workspace setup COMPLETED")
        sys.exit(0)
    else:
        print(f"\n✗ Workspace setup FAILED")
        if result['error']:
            print(f"Error: {result['error']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
