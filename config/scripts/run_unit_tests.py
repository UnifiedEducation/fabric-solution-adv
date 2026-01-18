"""
Run unit tests notebook and report results for CI/CD gates.

Usage:
    python run_unit_tests.py --environment TEST
"""

import os
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Add config directory to Python path for fabric_core imports
config_dir = Path(__file__).parent.parent
if str(config_dir) not in sys.path:
    sys.path.insert(0, str(config_dir))

from fabric_core import auth
from fabric_core.utils import load_config
from run_workspace_setup import get_notebook_id_by_name, run_notebook


def run_unit_tests(environment: str, workspace_id: str, config: dict) -> bool:
    """Execute the unit test notebook. Returns True if tests pass."""
    deployment = config.get('deployment', {})
    env_config = deployment.get('environments', {}).get(environment, {})
    test_notebook_name = env_config.get('unit_test_notebook', 'nb-av01-unit-tests')

    print(f"\n--- Running Unit Tests ({environment}) ---")
    print(f"  Notebook: {test_notebook_name}")

    notebook_id = get_notebook_id_by_name(workspace_id, test_notebook_name)
    if not notebook_id:
        print(f"  ERROR: Notebook '{test_notebook_name}' not found")
        return False

    result = run_notebook(workspace_id, notebook_id, test_notebook_name)

    if result['status'] == 'Completed':
        print(f"  PASSED: All tests completed successfully")
        return True
    else:
        print(f"  FAILED: {result.get('error', result['status'])}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Run unit tests notebook and report results')
    parser.add_argument('--environment', '-e', required=True,
                        choices=['TEST', 'PROD'],
                        help='Target environment')
    parser.add_argument('--workspace-id', '-w',
                        help='Processing workspace ID (overrides environment variable)')
    parser.add_argument('--config', '-c', default='config/templates/v01/v01-template.yml',
                        help='Path to configuration file')

    args = parser.parse_args()

    # Load .env for local testing
    if not os.getenv('GITHUB_ACTIONS'):
        env_file = Path(__file__).parent.parent.parent / '.env'
        if env_file.exists():
            load_dotenv(env_file)

    repository_root = Path(__file__).parent.parent.parent
    config_path = repository_root / args.config

    if not config_path.exists():
        print(f"ERROR: Config not found: {config_path}")
        sys.exit(1)

    config = load_config(str(config_path))

    # Get workspace ID
    workspace_id = args.workspace_id
    if not workspace_id:
        env_var = f"{args.environment}_PROCESSING_WORKSPACE_ID"
        workspace_id = os.getenv(env_var)

    if not workspace_id:
        print(f"ERROR: No workspace ID. Set {args.environment}_PROCESSING_WORKSPACE_ID or use --workspace-id")
        sys.exit(1)

    # Authenticate
    if not auth():
        print("ERROR: Authentication failed!")
        sys.exit(1)

    # Run tests
    success = run_unit_tests(args.environment, workspace_id, config)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
