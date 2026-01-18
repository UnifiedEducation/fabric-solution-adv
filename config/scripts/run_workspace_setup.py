"""
Run workspace setup notebooks after fabric-cicd deployment.

This script executes setup notebooks (like nb-av01-new-workspace-setup)
via the Fabric REST API to initialize lakehouses, publish environments,
and seed metadata after items are deployed.

Usage:
    python run_workspace_setup.py --environment TEST
    python run_workspace_setup.py --environment PROD --workspace-id <uuid>
"""

import os
import sys
import time
import json
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Add config directory to Python path for fabric_core imports
config_dir = Path(__file__).parent.parent
if str(config_dir) not in sys.path:
    sys.path.insert(0, str(config_dir))

from fabric_core import auth
from fabric_core.utils import get_fabric_cli_path, run_command, load_config


def get_notebook_id_by_name(workspace_id: str, notebook_name: str) -> str:
    """
    Find notebook ID by name within a workspace.

    Args:
        workspace_id: Fabric workspace UUID
        notebook_name: Display name of the notebook (without .Notebook suffix)

    Returns:
        str: Notebook ID if found, None otherwise
    """
    response = run_command([
        get_fabric_cli_path(), 'api', '-X', 'get',
        f'workspaces/{workspace_id}/notebooks'
    ])

    try:
        result = json.loads(response.stdout)
        if result.get('status_code') == 200:
            notebooks = result.get('text', {}).get('value', [])
            for nb in notebooks:
                display_name = nb.get('displayName', '')
                # Match with or without .Notebook suffix
                if display_name == notebook_name or display_name == f"{notebook_name}.Notebook":
                    return nb.get('id')
    except json.JSONDecodeError:
        print(f"  Warning: Could not parse notebook list response")

    return None


def run_notebook(workspace_id: str, notebook_id: str, notebook_name: str, parameters: dict = None) -> dict:
    """
    Execute a notebook and wait for completion.

    Uses the Fabric REST API to run a notebook on-demand and polls for completion.

    Args:
        workspace_id: Fabric workspace UUID
        notebook_id: Notebook UUID to execute
        notebook_name: Display name (for logging)
        parameters: Optional dictionary of notebook parameters

    Returns:
        dict: Execution result with 'status' and 'error' keys
    """
    # Start notebook execution using Jobs API
    # POST /workspaces/{workspaceId}/items/{itemId}/jobs/instances?jobType=RunNotebook
    request_body = {}
    if parameters:
        request_body["executionData"] = {"parameters": parameters}

    print(f"  Starting notebook execution...")

    start_response = run_command([
        get_fabric_cli_path(), 'api', '-X', 'post',
        f'workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook',
        '-i', json.dumps(request_body) if request_body else '{}'
    ])

    try:
        start_result = json.loads(start_response.stdout)
        status_code = start_result.get('status_code', 0)

        if status_code not in [200, 201, 202]:
            error_text = start_result.get('text', {})
            return {
                'status': 'Failed',
                'error': f"Failed to start notebook (HTTP {status_code}): {error_text}"
            }

        # Get job instance ID from response
        job_instance_id = start_result.get('text', {}).get('id')

        if not job_instance_id:
            # Check if we got a Location header or different response format
            print(f"  Response: {start_result}")
            return {
                'status': 'Unknown',
                'error': 'Could not determine job instance ID from response'
            }

        print(f"  Job started with ID: {job_instance_id}")

        # Poll for completion
        max_wait_minutes = 30
        poll_interval_seconds = 15
        max_polls = (max_wait_minutes * 60) // poll_interval_seconds

        for poll_num in range(max_polls):
            time.sleep(poll_interval_seconds)

            status_response = run_command([
                get_fabric_cli_path(), 'api', '-X', 'get',
                f'workspaces/{workspace_id}/items/{notebook_id}/jobs/instances/{job_instance_id}'
            ])

            try:
                status_result = json.loads(status_response.stdout)
                if status_result.get('status_code') == 200:
                    job_status = status_result.get('text', {}).get('status')
                    elapsed = (poll_num + 1) * poll_interval_seconds

                    if job_status == 'Completed':
                        print(f"  Notebook completed successfully ({elapsed}s)")
                        return {'status': 'Completed', 'error': None}
                    elif job_status == 'Failed':
                        error_info = status_result.get('text', {}).get('failureReason', {})
                        error_msg = error_info.get('message', 'Unknown error')
                        return {'status': 'Failed', 'error': error_msg}
                    elif job_status == 'Cancelled':
                        return {'status': 'Cancelled', 'error': 'Job was cancelled'}
                    else:
                        # Still running (InProgress, NotStarted, etc.)
                        print(f"  Status: {job_status} ({elapsed}s elapsed)")

            except json.JSONDecodeError:
                pass

        return {
            'status': 'Timeout',
            'error': f'Job did not complete within {max_wait_minutes} minutes'
        }

    except json.JSONDecodeError as e:
        return {'status': 'Failed', 'error': f'Invalid response from API: {e}'}


def run_setup_notebooks(environment: str, workspace_id: str, config: dict) -> bool:
    """
    Run all setup notebooks defined for an environment.

    Args:
        environment: Target environment (TEST, PROD)
        workspace_id: Processing workspace ID
        config: Loaded workspace configuration (v01-template.yml)

    Returns:
        bool: True if all setup notebooks completed successfully
    """
    deployment = config.get('deployment', {})
    env_config = deployment.get('environments', {}).get(environment, {})
    setup_notebooks = env_config.get('setup_notebooks', [])

    if not setup_notebooks:
        print(f"\nNo setup notebooks defined for {environment}")
        return True

    print(f"\n{'='*60}")
    print(f"Running Setup Notebooks for {environment}")
    print(f"{'='*60}")
    print(f"  Workspace ID: {workspace_id}")
    print(f"  Notebooks: {setup_notebooks}")

    all_succeeded = True

    for notebook_name in setup_notebooks:
        print(f"\n--- {notebook_name} ---")

        notebook_id = get_notebook_id_by_name(workspace_id, notebook_name)
        if not notebook_id:
            print(f"  WARNING: Notebook '{notebook_name}' not found in workspace")
            all_succeeded = False
            continue

        print(f"  Found notebook ID: {notebook_id}")

        result = run_notebook(workspace_id, notebook_id, notebook_name)

        if result['status'] == 'Completed':
            print(f"  SUCCESS: {notebook_name} completed")
        else:
            print(f"  FAILED: {notebook_name}")
            print(f"    Status: {result['status']}")
            if result['error']:
                print(f"    Error: {result['error']}")
            all_succeeded = False

    return all_succeeded


def main():
    parser = argparse.ArgumentParser(
        description='Run workspace setup notebooks after deployment',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Run setup notebooks for TEST:
    python run_workspace_setup.py -e TEST

  Run setup with explicit workspace ID:
    python run_workspace_setup.py -e PROD -w xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        """
    )
    parser.add_argument('--environment', '-e', required=True,
                        choices=['TEST', 'PROD'],
                        help='Target environment')
    parser.add_argument('--workspace-id', '-w',
                        help='Processing workspace ID (overrides environment variable)')
    parser.add_argument('--config', '-c', default='config/templates/v01/v01-template.yml',
                        help='Path to solution template configuration file')

    args = parser.parse_args()

    # Load environment variables from .env file (for local testing)
    if not os.getenv('GITHUB_ACTIONS'):
        env_file = Path(__file__).parent.parent.parent / '.env'
        if env_file.exists():
            load_dotenv(env_file)
            print(f"Loaded environment from: {env_file}")

    # Determine repository root
    repository_root = Path(__file__).parent.parent.parent

    # Load configuration
    config_path = repository_root / args.config
    if not config_path.exists():
        print(f"ERROR: Configuration file not found: {config_path}")
        sys.exit(1)

    config = load_config(str(config_path))

    # Get workspace ID from args or environment
    workspace_id = args.workspace_id
    if not workspace_id:
        env_var = f"{args.environment}_PROCESSING_WORKSPACE_ID"
        workspace_id = os.getenv(env_var)

    if not workspace_id:
        print(f"ERROR: No workspace ID provided")
        print(f"  Either pass --workspace-id or set {args.environment}_PROCESSING_WORKSPACE_ID")
        sys.exit(1)

    # Authenticate
    print("="*60)
    print("Workspace Setup")
    print("="*60)
    print(f"  Environment: {args.environment}")
    print(f"  Workspace:   {workspace_id}")
    print()
    print("Authenticating...")

    if not auth():
        print("ERROR: Authentication failed!")
        sys.exit(1)

    # Run setup notebooks
    success = run_setup_notebooks(args.environment, workspace_id, config)

    if success:
        print("\n" + "="*60)
        print("Setup Complete!")
        print("="*60)
        sys.exit(0)
    else:
        print("\n" + "="*60)
        print("Setup completed with errors!")
        print("="*60)
        sys.exit(1)


if __name__ == "__main__":
    main()
