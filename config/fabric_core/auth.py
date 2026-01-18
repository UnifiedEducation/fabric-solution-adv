"""Authentication module for Fabric CLI."""

import os
from .utils import get_fabric_cli_path, run_command


def auth():
    """
    Authenticate with Azure using service principal credentials.

    Returns:
        bool: True if authentication succeeded, False otherwise
    """
    result = run_command([get_fabric_cli_path(), 'auth', 'login', '-u', os.getenv('SPN_CLIENT_ID'),
                         '-p', os.getenv('SPN_CLIENT_SECRET'), '--tenant', os.getenv('AZURE_TENANT_ID')])

    if result.returncode != 0:
        print("Authentication failed")
        print(f"  stdout: {result.stdout}")
        print(f"  stderr: {result.stderr}")
        return False

    print("Authenticated successfully")
    return True
