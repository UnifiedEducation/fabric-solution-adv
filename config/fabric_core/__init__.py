"""
Fabric Core - Reusable modules for Microsoft Fabric CLI operations.

This package contains common functionality for:
- Authentication
- Workspace management (including permissions)
- Capacity management
- Git integration
- Item deployment (via Item Definition API)
- Utility functions
"""

from .auth import auth
from .workspace import workspace_exists, create_workspace, assign_permissions, get_workspace_role_assignments
from .capacity import capacity_exists, create_capacity, suspend_capacity
from .git_integration import get_or_create_git_connection, connect_workspace_to_git, update_workspace_from_git
from .item_deployment import deploy_items_to_workspace, list_workspace_items, deploy_item, find_item_folders
# job_scheduler uses direct REST API (no Fabric CLI) - prefer its get_workspace_id
from .job_scheduler import run_notebook, get_job_status, get_item_id, get_workspace_id
from .utils import get_fabric_cli_path, run_command, call_azure_api, load_config

__all__ = [
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
    'deploy_items_to_workspace',
    'list_workspace_items',
    'deploy_item',
    'find_item_folders',
    'run_notebook',
    'get_job_status',
    'get_item_id',
    'get_fabric_cli_path',
    'run_command',
    'call_azure_api',
    'load_config'
]
