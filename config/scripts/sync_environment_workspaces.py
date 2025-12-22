"""
Sync workspaces for a given environment (test or prod) from Git.

This script reads the workspace configuration and syncs workspaces
that belong to the target environment. It supports selective syncing
based on workspace types (processing, datastores, consumption).

Environment variables:
    TARGET_ENVIRONMENT: Environment to sync (e.g., 'test', 'prod')
    WORKSPACES_TO_SYNC: Comma-separated workspace types (e.g., 'processing,datastores')
                        If not set, syncs all workspace types.
    CONFIG_FILE: Path to YAML config (default: config/templates/v01/v01-template.yml)
"""

# fmt: off
# isort: skip_file
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add config directory to Python path to find fabric_core module
config_dir = Path(__file__).parent.parent
if str(config_dir) not in sys.path:
    sys.path.insert(0, str(config_dir))

# Import from fabric_core modules (must be after sys.path modification)
from fabric_core import auth, get_workspace_id, update_workspace_from_git
from fabric_core.utils import load_config
# fmt: on


# Ensure UTF-8 encoding for stdout
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


def get_environment_workspaces(workspaces_config, environment, solution_version, workspace_types=None):
    """
    Filter workspaces for a specific environment and optional workspace types.

    Args:
        workspaces_config: List of workspace configurations from YAML
        environment: Target environment ('test' or 'prod')
        solution_version: Solution version prefix (e.g., 'av01')
        workspace_types: Optional list of workspace types to include (e.g., ['processing', 'datastores'])
                        If None, includes all workspace types.

    Returns:
        List of workspace configs matching the environment and types
    """
    env_pattern = f"-{environment}-"
    result = []

    for ws in workspaces_config:
        ws_name = ws.get('name', '').replace('{{SOLUTION_VERSION}}', solution_version)

        # Check if workspace belongs to target environment
        if env_pattern not in ws_name:
            continue

        # If workspace_types filter is specified, check if this workspace matches
        if workspace_types:
            # Extract workspace type from name (e.g., 'av01-test-processing' -> 'processing')
            ws_type = ws_name.split('-')[-1] if '-' in ws_name else None
            if ws_type not in workspace_types:
                continue

        result.append(ws)

    return result


def get_unique_capacities(workspaces, solution_version):
    """
    Get unique capacity names from workspace configurations.

    Args:
        workspaces: List of workspace configurations
        solution_version: Solution version prefix

    Returns:
        Set of unique capacity names
    """
    capacities = set()
    for ws in workspaces:
        capacity = ws.get('capacity', '').replace('{{SOLUTION_VERSION}}', solution_version)
        if capacity:
            capacities.add(capacity)
    return capacities


def main():
    # Load environment variables if not in GitHub Actions
    if not os.getenv('GITHUB_ACTIONS'):
        load_dotenv(Path(__file__).parent.parent.parent / '.env')

    # Get target environment
    environment = os.getenv('TARGET_ENVIRONMENT', '').lower()
    if environment not in ['test', 'prod']:
        print(f"✗ Invalid TARGET_ENVIRONMENT: '{environment}' (must be 'test' or 'prod')")
        sys.exit(1)

    # Load config
    config_file = os.getenv('CONFIG_FILE', 'config/templates/v01/v01-template.yml')
    config = load_config(config_file)

    solution_version = config.get('solution_version', 'av01')
    workspaces_config = config.get('workspaces', [])

    # Parse workspace types filter (if specified)
    workspaces_to_sync = os.getenv('WORKSPACES_TO_SYNC', '')
    workspace_types = [ws.strip() for ws in workspaces_to_sync.split(',') if ws.strip()] if workspaces_to_sync else None

    # Filter workspaces for target environment and types
    env_workspaces = get_environment_workspaces(workspaces_config, environment, solution_version, workspace_types)

    if not env_workspaces:
        print(f"⚠ No workspaces found for environment: {environment}")
        sys.exit(1)

    # Get unique capacities for informational purposes
    capacities = get_unique_capacities(env_workspaces, solution_version)

    print(f"=== SYNCING {environment.upper()} ENVIRONMENT ===")
    print(f"Solution version: {solution_version}")
    if workspace_types:
        print(f"Workspace types filter: {', '.join(workspace_types)}")
    print(f"Workspaces to sync: {len(env_workspaces)}")
    print(f"Capacities involved: {', '.join(sorted(capacities))}\n")

    # Authenticate
    print("--- Authenticating ---")
    if not auth():
        print("\n✗ Authentication failed")
        sys.exit(1)

    # Sync each workspace
    failed = []
    succeeded = []

    for ws_config in env_workspaces:
        workspace_name = ws_config['name'].replace('{{SOLUTION_VERSION}}', solution_version)
        print(f"\n--- Syncing {workspace_name} ---")

        # Get workspace ID
        workspace_id = get_workspace_id(workspace_name)
        if not workspace_id:
            print(f"  ✗ Workspace not found: {workspace_name}")
            failed.append(workspace_name)
            continue

        print(f"  Workspace ID: {workspace_id}")

        # Update from Git
        success = update_workspace_from_git(workspace_id, workspace_name)

        if success:
            succeeded.append(workspace_name)
        else:
            failed.append(workspace_name)

    # Summary
    print(f"\n=== SYNC SUMMARY ===")
    print(f"Succeeded: {len(succeeded)}")
    print(f"Failed: {len(failed)}")

    if failed:
        print(f"\nFailed workspaces:")
        for ws in failed:
            print(f"  - {ws}")
        sys.exit(1)

    print(f"\n✓ All {environment.upper()} workspaces synced successfully")
    sys.exit(0)


if __name__ == "__main__":
    main()
