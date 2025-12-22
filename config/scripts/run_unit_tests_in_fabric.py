"""
Run unit tests in a Fabric workspace.

This script is called from GitHub Actions to execute the unit test notebook
in a specified workspace and return success/failure based on test results.
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
from fabric_core import auth, get_workspace_id, run_notebook, get_item_id
# fmt: on


# Ensure UTF-8 encoding for stdout
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


# Configuration
UNIT_TEST_NOTEBOOK_PATH = "utils/nb-av01-unit-tests.Notebook"
NOTEBOOK_TIMEOUT = 600  # 10 minutes


def main():
    # Load environment variables if not in GitHub Actions
    if not os.getenv('GITHUB_ACTIONS'):
        load_dotenv(Path(__file__).parent.parent.parent / '.env')

    # Get workspace name from environment (set by GitHub Actions)
    workspace_name = os.getenv('TEST_WORKSPACE_NAME', 'av01-test-processing')

    print("=== RUNNING UNIT TESTS IN FABRIC ===")
    print(f"Workspace: {workspace_name}")
    print(f"Notebook: {UNIT_TEST_NOTEBOOK_PATH}")

    # Authenticate
    print("\n--- Authenticating ---")
    if not auth():
        print("\n✗ Authentication failed")
        sys.exit(1)

    # Get workspace ID
    print("\n--- Getting workspace ID ---")
    workspace_id = get_workspace_id(workspace_name)
    if not workspace_id:
        print(f"✗ Workspace not found: {workspace_name}")
        sys.exit(1)
    print(f"✓ Workspace ID: {workspace_id}")

    # Get notebook ID
    print("\n--- Getting notebook ID ---")
    notebook_id = get_item_id(workspace_name, UNIT_TEST_NOTEBOOK_PATH)
    if not notebook_id:
        print(f"✗ Notebook not found: {UNIT_TEST_NOTEBOOK_PATH}")
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
        print("\n✓ Unit tests PASSED")
        sys.exit(0)
    else:
        print(f"\n✗ Unit tests FAILED")
        if result['error']:
            print(f"Error: {result['error']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
