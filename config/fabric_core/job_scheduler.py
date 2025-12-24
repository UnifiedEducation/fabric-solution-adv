"""Job scheduler module for running Fabric notebooks and tracking job status.

Uses direct REST API calls instead of the Fabric CLI for better reliability.
"""

import json
import os
import time

import requests

# Fabric API base URL
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"


def _get_fabric_access_token():
    """
    Get an access token for the Fabric API using service principal credentials.

    Returns:
        str: Access token, or None if failed
    """
    tenant_id = os.getenv('AZURE_TENANT_ID')
    client_id = os.getenv('SPN_CLIENT_ID')
    client_secret = os.getenv('SPN_CLIENT_SECRET')

    if not all([tenant_id, client_id, client_secret]):
        print("  Missing required environment variables for authentication")
        return None

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://api.fabric.microsoft.com/.default',
        'grant_type': 'client_credentials'
    }

    try:
        response = requests.post(token_url, data=token_data)
        response.raise_for_status()
        return response.json().get('access_token')
    except requests.RequestException as e:
        print(f"  Failed to get access token: {e}")
        return None


def _fabric_api_request(method, endpoint, json_body=None, return_headers=False):
    """
    Make a request to the Fabric REST API.

    Args:
        method: HTTP method ('GET', 'POST', etc.)
        endpoint: API endpoint (e.g., 'workspaces/{id}/items')
        json_body: Optional request body dict
        return_headers: If True, include response headers in return

    Returns:
        tuple: (success: bool, status_code: int, response_json: dict, headers: dict or None)
    """
    token = _get_fabric_access_token()
    if not token:
        return False, 0, {'error': 'Failed to get access token'}, None

    url = f"{FABRIC_API_BASE}/{endpoint}"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.request(method, url, headers=headers, json=json_body)

        # Parse response body if present
        try:
            response_json = response.json() if response.text else {}
            if response_json is None:
                response_json = {}
        except json.JSONDecodeError:
            response_json = {'raw_response': response.text}

        if return_headers:
            return True, response.status_code, response_json, dict(response.headers)
        return True, response.status_code, response_json, None

    except requests.RequestException as e:
        return False, 0, {'error': str(e)}, None


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

    success, status_code, response, headers = _fabric_api_request('POST', endpoint, return_headers=True)

    if not success:
        return {
            'success': False,
            'status': 'TriggerFailed',
            'job_id': None,
            'error': f"Failed to trigger notebook: {response.get('error', 'Unknown error')}"
        }

    if status_code not in [200, 202]:
        return {
            'success': False,
            'status': 'TriggerFailed',
            'job_id': None,
            'error': f"API returned status {status_code}: {response}"
        }

    # Extract job instance ID from response body or Location header
    job_id = response.get('id')

    # For 202 Accepted, job ID may be in Location header
    if not job_id and headers:
        location = headers.get('Location', '')
        # Location format: .../jobs/instances/{job_id}
        if '/jobs/instances/' in location:
            job_id = location.split('/jobs/instances/')[-1].split('?')[0]

    if not job_id:
        return {
            'success': False,
            'status': 'TriggerFailed',
            'job_id': None,
            'error': f"No job ID returned. Response: {response}, Headers: {headers}"
        }

    print(f"  Job triggered: {job_id}")

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

    success, status_code, response, _ = _fabric_api_request('GET', endpoint)

    if not success:
        return {'status': 'Unknown', 'failure_reason': f"API call failed: {response.get('error')}"}

    if status_code != 200:
        return {'status': 'Unknown', 'failure_reason': f"API returned status {status_code}"}

    return {
        'status': response.get('status', 'Unknown'),
        'failure_reason': response.get('failureReason')
    }


def get_item_id(workspace_name, item_name):
    """
    Get the UUID of a Fabric item by name.

    Args:
        workspace_name: Name of the workspace
        item_name: Display name of the item (e.g., 'nb-av01-unit-tests.Notebook')

    Returns:
        str: Item UUID or None if not found
    """
    # First get the workspace ID
    workspace_id = get_workspace_id(workspace_name)
    if not workspace_id:
        return None

    # Strip the type suffix if present (e.g., '.Notebook')
    display_name = item_name
    for suffix in ['.Notebook', '.Lakehouse', '.DataPipeline', '.Environment', '.VariableLibrary', '.SQLDatabase']:
        if display_name.endswith(suffix):
            display_name = display_name[:-len(suffix)]
            break

    # List all items in workspace
    success, status_code, response, _ = _fabric_api_request('GET', f'workspaces/{workspace_id}/items')

    if not success or status_code != 200:
        return None

    items = response.get('value', [])
    for item in items:
        if item.get('displayName') == display_name:
            return item.get('id')

    return None


def get_workspace_id(workspace_name):
    """
    Get the UUID of a workspace by name.

    Args:
        workspace_name: Name of the workspace

    Returns:
        str: Workspace UUID or None if not found
    """
    success, status_code, response, _ = _fabric_api_request('GET', 'workspaces')

    if not success or status_code != 200:
        return None

    workspaces = response.get('value', [])
    for workspace in workspaces:
        if workspace.get('displayName') == workspace_name:
            return workspace.get('id')

    return None
