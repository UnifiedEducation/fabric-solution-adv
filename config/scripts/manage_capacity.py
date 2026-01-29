"""
Manage Fabric capacity state for CI/CD cost optimization.

This script resumes or suspends Azure Fabric capacities before/after deployments
to minimize costs when capacities are not in use.

Usage:
    python manage_capacity.py --environment TEST --action resume
    python manage_capacity.py --environment PROD --action suspend
    python manage_capacity.py --environment TEST --action resume --wait
"""

import os
import sys
import time
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Add config directory to Python path for fabric_core imports
config_dir = Path(__file__).parent.parent
if str(config_dir) not in sys.path:
    sys.path.insert(0, str(config_dir))

from fabric_core import auth
from fabric_core.capacity import suspend_capacity
from fabric_core.utils import call_azure_api, load_config


def get_capacities_for_environment(environment: str, config: dict,
                                    workspace_types: list = None) -> list:
    """
    Get capacity names for a given environment from config.

    Searches workspaces for matching environment and collects unique capacity names.

    Args:
        environment: Target environment (TEST, PROD)
        config: Loaded workspace configuration (v01-template.yml)
        workspace_types: Optional list of workspace types to filter by (e.g., ['processing', 'datastores'])

    Returns:
        list: Unique capacity names for the environment
    """
    env_lower = environment.lower()
    capacities = set()

    for workspace in config.get('workspaces', []):
        ws_name = workspace.get('name', '').lower()
        # Match workspaces for this environment (e.g., "-test-" in name)
        if f"-{env_lower}-" in ws_name:
            # Filter by workspace type if specified
            if workspace_types:
                ws_type = workspace.get('type', '').lower()
                if ws_type not in [t.lower() for t in workspace_types]:
                    continue

            capacity = workspace.get('capacity', '')
            if capacity:
                # Resolve template variables
                solution_version = config.get('solution_version', 'av01')
                capacity = capacity.replace('{{SOLUTION_VERSION}}', solution_version)
                capacities.add(capacity)

    return list(capacities)


def resume_capacity(capacity_name: str, subscription_id: str, resource_group: str) -> bool:
    """
    Resume a suspended Fabric capacity.

    Args:
        capacity_name: Name of the capacity to resume
        subscription_id: Azure subscription ID
        resource_group: Azure resource group name

    Returns:
        bool: True if resumed successfully or already running, False otherwise
    """
    # First check if capacity exists and its state
    status, response = call_azure_api(
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version=2023-11-01"
    )

    if status != 200:
        print(f"  Warning: Capacity {capacity_name} not found or error checking status")
        return False

    current_state = response.get('properties', {}).get('state', 'Unknown')
    print(f"  Current state: {current_state}")

    if current_state == 'Active':
        print(f"  {capacity_name} is already active")
        return True

    # Resume the capacity
    status, _ = call_azure_api(
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
        f"/providers/Microsoft.Fabric/capacities/{capacity_name}/resume?api-version=2023-11-01",
        'post'
    )

    if status in [200, 202]:
        print(f"  Resume initiated for {capacity_name}")
        return True
    else:
        print(f"  Failed to resume {capacity_name} (HTTP {status})")
        return False


def wait_for_capacity_active(capacity_name: str, subscription_id: str, resource_group: str,
                              timeout_minutes: int = 10) -> bool:
    """
    Wait for a capacity to become active.

    Args:
        capacity_name: Name of the capacity
        subscription_id: Azure subscription ID
        resource_group: Azure resource group name
        timeout_minutes: Maximum time to wait

    Returns:
        bool: True if capacity became active, False if timeout
    """
    print(f"  Waiting for {capacity_name} to become active...")
    poll_interval = 15
    max_polls = (timeout_minutes * 60) // poll_interval

    for poll_num in range(max_polls):
        status, response = call_azure_api(
            f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
            f"/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version=2023-11-01"
        )

        if status == 200:
            current_state = response.get('properties', {}).get('state', 'Unknown')
            elapsed = (poll_num + 1) * poll_interval

            if current_state == 'Active':
                print(f"  {capacity_name} is now active ({elapsed}s)")
                return True
            else:
                print(f"  State: {current_state} ({elapsed}s elapsed)")

        time.sleep(poll_interval)

    print(f"  Timeout waiting for {capacity_name} to become active")
    return False


def manage_capacities(environment: str, action: str, config: dict,
                      subscription_id: str, resource_group: str,
                      wait: bool = False,
                      workspace_types: list = None) -> bool:
    """
    Resume or suspend all capacities for an environment.

    Args:
        environment: Target environment (TEST, PROD)
        action: Action to perform ('resume' or 'suspend')
        config: Loaded workspace configuration
        subscription_id: Azure subscription ID
        resource_group: Azure resource group name
        wait: Wait for capacities to reach target state (resume only)
        workspace_types: Optional list of workspace types to filter capacities by

    Returns:
        bool: True if all operations succeeded
    """
    capacities = get_capacities_for_environment(environment, config, workspace_types)

    if not capacities:
        print(f"No capacities found for {environment}")
        return True

    print(f"\n{'='*60}")
    print(f"Capacity Management: {action.upper()} for {environment}")
    print(f"{'='*60}")
    print(f"  Capacities: {capacities}")

    all_succeeded = True

    for capacity_name in capacities:
        print(f"\n--- {capacity_name} ---")

        if action == 'resume':
            success = resume_capacity(capacity_name, subscription_id, resource_group)
            if success and wait:
                success = wait_for_capacity_active(capacity_name, subscription_id, resource_group)
        elif action == 'suspend':
            success = suspend_capacity(capacity_name, subscription_id, resource_group)
        else:
            print(f"  Unknown action: {action}")
            success = False

        if not success:
            all_succeeded = False

    return all_succeeded


def main():
    parser = argparse.ArgumentParser(
        description='Manage Fabric capacity state for CI/CD',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Resume TEST capacities before deployment:
    python manage_capacity.py -e TEST -a resume --wait

  Suspend PROD capacities after deployment:
    python manage_capacity.py -e PROD -a suspend
        """
    )
    parser.add_argument('--environment', '-e', required=True,
                        choices=['TEST', 'PROD'],
                        help='Target environment')
    parser.add_argument('--action', '-a', required=True,
                        choices=['resume', 'suspend'],
                        help='Action to perform')
    parser.add_argument('--wait', '-w', action='store_true',
                        help='Wait for capacities to reach target state (resume only)')
    parser.add_argument('--workspace-type', nargs='+', default=None,
                        help='Filter capacities to those used by these workspace types (e.g., processing datastores)')
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

    # Get Azure configuration
    subscription_id = os.getenv('AZURE_SUBSCRIPTION_ID')
    if not subscription_id:
        # Try to get from config
        azure_config = config.get('azure', {})
        subscription_id = azure_config.get('subscription_id', '')
        # Remove ${...} wrapper if env var substitution didn't happen
        if subscription_id.startswith('${'):
            subscription_id = None

    if not subscription_id:
        print("ERROR: AZURE_SUBSCRIPTION_ID not set")
        sys.exit(1)

    # Get resource group from config
    azure_config = config.get('azure', {})
    capacity_defaults = azure_config.get('capacity_defaults', {})
    resource_group = capacity_defaults.get('resource_group', 'rg-av01')

    # Authenticate
    print("="*60)
    print("Capacity Manager")
    print("="*60)
    print(f"  Environment:     {args.environment}")
    print(f"  Action:          {args.action}")
    print(f"  Wait:            {args.wait}")
    print(f"  Workspace types: {args.workspace_type or 'all'}")
    print()
    print("Authenticating...")

    if not auth():
        print("ERROR: Authentication failed!")
        sys.exit(1)

    # Manage capacities
    success = manage_capacities(
        environment=args.environment,
        action=args.action,
        config=config,
        subscription_id=subscription_id,
        resource_group=resource_group,
        wait=args.wait,
        workspace_types=args.workspace_type
    )

    if success:
        print(f"\n{'='*60}")
        print(f"Capacity {args.action} completed successfully!")
        print("="*60)
        sys.exit(0)
    else:
        print(f"\n{'='*60}")
        print(f"Capacity {args.action} completed with errors!")
        print("="*60)
        sys.exit(1)


if __name__ == "__main__":
    main()
