"""
Run a notebook in Microsoft Fabric.

A simple, general-purpose script to execute any Fabric notebook via the REST API.
Fire-and-forget - does not wait for completion.

Usage:
    python run_fabric_notebook.py --workspace-id <id> --notebook-id <id>
"""

import os
import sys
import argparse
import requests
from azure.identity import ClientSecretCredential


def get_fabric_token() -> str:
    """Get a Fabric API access token using Azure credentials."""
    credential = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )
    token = credential.get_token("https://api.fabric.microsoft.com/.default")
    return token.token


def run_notebook(workspace_id: str, notebook_id: str, token: str) -> bool:
    """Execute a notebook via the Fabric REST API. Returns True if started successfully."""
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"

    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={},
    )

    if response.status_code in [200, 201, 202]:
        print(f"Notebook started successfully")
        return True
    else:
        print(f"Failed to start notebook (HTTP {response.status_code})")
        print(f"  {response.text}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Run a Fabric notebook")
    parser.add_argument("--workspace-id", "-w", required=True, help="Fabric workspace ID")
    parser.add_argument("--notebook-id", "-n", required=True, help="Notebook ID to execute")
    args = parser.parse_args()

    print(f"Workspace: {args.workspace_id}")
    print(f"Notebook:  {args.notebook_id}")

    token = get_fabric_token()
    success = run_notebook(args.workspace_id, args.notebook_id, token)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
