"""
Fabric Core - Reusable modules for Microsoft Fabric CLI operations.

This package contains common functionality for:
- Authentication
- Workspace management (including permissions)
- Capacity management
- Git integration
- Utility functions
"""

import os
from pathlib import Path

from .auth import auth
from .workspace import workspace_exists, get_workspace_id, create_workspace, assign_permissions, get_workspace_role_assignments
from .capacity import capacity_exists, create_capacity, suspend_capacity
from .git_integration import get_or_create_git_connection, connect_workspace_to_git, update_workspace_from_git
from .utils import get_fabric_cli_path, run_command, call_azure_api, load_config


def bootstrap():
    """Load .env file for local development (skipped in GitHub Actions)."""
    if not os.getenv('GITHUB_ACTIONS'):
        from dotenv import load_dotenv
        # Walk up from fabric_core package to find project root .env
        env_file = Path(__file__).parent.parent.parent / '.env'
        if env_file.exists():
            load_dotenv(env_file)


__all__ = [
    'bootstrap',
    'auth',
    'workspace_exists',
    'get_workspace_id',
    'create_workspace',
    'assign_permissions',
    'capacity_exists',
    'create_capacity',
    'suspend_capacity',
    'get_or_create_git_connection',
    'connect_workspace_to_git',
    'update_workspace_from_git',
    'get_fabric_cli_path',
    'run_command',
    'call_azure_api',
    'load_config'
]
