"""
Run a notebook in Microsoft Fabric.

A general-purpose script to execute any Fabric notebook via the REST API.
Waits for completion and returns success/failure based on notebook execution result.

Usage:
    python run_fabric_notebook.py --workspace-id <id> --notebook-id <id>
    python run_fabric_notebook.py -w <id> -n <id> --timeout 60
"""

import os
import sys
import time
import argparse
import requests
from azure.identity import ClientSecretCredential

DEFAULT_TIMEOUT_MINUTES = 30
POLL_INTERVAL_SECONDS = 15


def get_fabric_token() -> str:
    """Get a Fabric API access token using Azure credentials."""
    credential = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )
    token = credential.get_token("https://api.fabric.microsoft.com/.default")
    return token.token


def run_notebook(workspace_id: str, notebook_id: str, token: str, timeout_minutes: int) -> bool:
    """
    Execute a notebook via the Fabric REST API and wait for completion.

    Returns True if notebook completed successfully, False otherwise.
    """
    base_url = "https://api.fabric.microsoft.com/v1"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Start the notebook
    start_url = f"{base_url}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"
    response = requests.post(start_url, headers=headers, json={})

    if response.status_code not in [200, 201, 202]:
        print(f"Failed to start notebook (HTTP {response.status_code})")
        print(f"  {response.text}")
        return False

    # Extract job instance ID from response
    job_instance_id = response.json().get("id")
    if not job_instance_id:
        print("Failed to get job instance ID from response")
        print(f"  Response: {response.text}")
        return False

    print(f"Notebook started (job ID: {job_instance_id})")

    # Poll for completion
    status_url = f"{base_url}/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances/{job_instance_id}"
    max_polls = (timeout_minutes * 60) // POLL_INTERVAL_SECONDS

    for poll_num in range(max_polls):
        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed = (poll_num + 1) * POLL_INTERVAL_SECONDS

        status_response = requests.get(status_url, headers=headers)
        if status_response.status_code != 200:
            print(f"  Warning: Failed to get job status (HTTP {status_response.status_code})")
            continue

        job_status = status_response.json().get("status")

        if job_status == "Completed":
            print(f"Notebook completed successfully ({elapsed}s)")
            return True
        elif job_status == "Failed":
            failure_reason = status_response.json().get("failureReason", {})
            error_msg = failure_reason.get("message", "Unknown error")
            print(f"Notebook failed ({elapsed}s)")
            print(f"  Error: {error_msg}")
            return False
        elif job_status == "Cancelled":
            print(f"Notebook was cancelled ({elapsed}s)")
            return False
        else:
            # Still running (InProgress, NotStarted, etc.)
            print(f"  Status: {job_status} ({elapsed}s)")

    print(f"Timeout: notebook did not complete within {timeout_minutes} minutes")
    return False


def main():
    parser = argparse.ArgumentParser(description="Run a Fabric notebook")
    parser.add_argument("--workspace-id", "-w", required=True, help="Fabric workspace ID")
    parser.add_argument("--notebook-id", "-n", required=True, help="Notebook ID to execute")
    parser.add_argument("--timeout", "-t", type=int, default=DEFAULT_TIMEOUT_MINUTES,
                        help=f"Timeout in minutes (default: {DEFAULT_TIMEOUT_MINUTES})")
    args = parser.parse_args()

    print(f"Workspace: {args.workspace_id}")
    print(f"Notebook:  {args.notebook_id}")
    print(f"Timeout:   {args.timeout} minutes")
    print()

    token = get_fabric_token()
    success = run_notebook(args.workspace_id, args.notebook_id, token, args.timeout)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
