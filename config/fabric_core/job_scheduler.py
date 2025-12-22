"""Job scheduler module for running Fabric notebooks and tracking job status."""

import json
import time
from .utils import get_fabric_cli_path, run_command


def run_notebook(workspace_id, notebook_id, timeout_seconds=600, poll_interval=15):
    """
    Run a Fabric notebook and wait for completion.

    Args:
        workspace_id: Workspace UUID
        notebook_id: Notebook item UUID
        timeout_seconds: Maximum time to wait for job completion (default 10 minutes)
        poll_interval: Seconds between status checks (default 15)

    Returns:
        dict: Result with 'success' (bool), 'status' (str), 'job_id' (str), 'error' (str or None)
    """
    # Trigger notebook execution
    endpoint = f"workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"

    result = run_command([
        get_fabric_cli_path(), 'api', '-X', 'post', endpoint
    ])

    if result.returncode != 0:
        return {
            'success': False,
            'status': 'TriggerFailed',
            'job_id': None,
            'error': f"Failed to trigger notebook: {result.stderr}"
        }

    try:
        response_json = json.loads(result.stdout)
        status_code = response_json.get('status_code', 0)

        if status_code not in [200, 202]:
            error_text = response_json.get('text', {})
            return {
                'success': False,
                'status': 'TriggerFailed',
                'job_id': None,
                'error': f"API returned status {status_code}: {error_text}"
            }

        # Extract job instance ID from response
        response_text = response_json.get('text', {})
        job_id = response_text.get('id')

        if not job_id:
            return {
                'success': False,
                'status': 'TriggerFailed',
                'job_id': None,
                'error': "No job ID returned in response"
            }

        print(f"  Job triggered: {job_id}")

    except json.JSONDecodeError as e:
        return {
            'success': False,
            'status': 'TriggerFailed',
            'job_id': None,
            'error': f"Invalid JSON response: {e}"
        }

    # Poll for job completion
    elapsed = 0
    while elapsed < timeout_seconds:
        status_result = get_job_status(workspace_id, notebook_id, job_id)

        if status_result['status'] in ['Completed', 'Failed', 'Cancelled']:
            return {
                'success': status_result['status'] == 'Completed',
                'status': status_result['status'],
                'job_id': job_id,
                'error': status_result.get('failure_reason')
            }

        print(f"  Job status: {status_result['status']} (elapsed: {elapsed}s)")
        time.sleep(poll_interval)
        elapsed += poll_interval

    return {
        'success': False,
        'status': 'Timeout',
        'job_id': job_id,
        'error': f"Job did not complete within {timeout_seconds} seconds"
    }


def get_job_status(workspace_id, item_id, job_id):
    """
    Get the status of a job instance.

    Args:
        workspace_id: Workspace UUID
        item_id: Item UUID (notebook, pipeline, etc.)
        job_id: Job instance UUID

    Returns:
        dict: Result with 'status' (str), 'failure_reason' (str or None)
    """
    endpoint = f"workspaces/{workspace_id}/items/{item_id}/jobs/instances/{job_id}"

    result = run_command([
        get_fabric_cli_path(), 'api', '-X', 'get', endpoint
    ])

    if result.returncode != 0:
        return {'status': 'Unknown', 'failure_reason': f"API call failed: {result.stderr}"}

    try:
        response_json = json.loads(result.stdout)
        status_code = response_json.get('status_code', 0)

        if status_code != 200:
            return {'status': 'Unknown', 'failure_reason': f"API returned status {status_code}"}

        response_text = response_json.get('text', {})
        return {
            'status': response_text.get('status', 'Unknown'),
            'failure_reason': response_text.get('failureReason')
        }

    except json.JSONDecodeError:
        return {'status': 'Unknown', 'failure_reason': "Invalid JSON response"}


def get_item_id(workspace_name, item_path):
    """
    Get the UUID of a Fabric item by path.

    Args:
        workspace_name: Name of the workspace
        item_path: Path to item (e.g., 'utils/nb-av01-unit-tests.Notebook')

    Returns:
        str: Item UUID or None if not found
    """
    result = run_command([
        get_fabric_cli_path(), 'get', f'{workspace_name}.Workspace/{item_path}', '-q', 'id'
    ])

    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()

    return None
