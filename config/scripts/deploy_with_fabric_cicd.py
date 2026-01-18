"""
Deploy Fabric items using Microsoft's fabric-cicd package.

Usage:
    python deploy_with_fabric_cicd.py --environment TEST --all-workspaces
    python deploy_with_fabric_cicd.py --environment PROD --all-workspaces --no-cleanup
    python deploy_with_fabric_cicd.py --environment TEST --workspace-type processing
"""

import os
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv

# Add config directory to Python path for fabric_core imports
config_dir = Path(__file__).parent.parent
if str(config_dir) not in sys.path:
    sys.path.insert(0, str(config_dir))

from fabric_core.utils import load_config

try:
    from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items
except ImportError:
    print("ERROR: fabric-cicd package not installed. Run: pip install fabric-cicd")
    sys.exit(1)


def get_workspace_id(environment: str, workspace_type: str) -> str:
    """Get workspace ID from environment variable."""
    env_var_name = f"{environment}_{workspace_type.upper()}_WORKSPACE_ID"
    return os.getenv(env_var_name, '')


def get_item_types_for_workspace(workspace_type: str, config: dict) -> list:
    """
    Get the list of item types to deploy for a workspace type.

    Args:
        workspace_type: Workspace type (processing, datastores, consumption)
        config: Loaded workspace configuration (v01-template.yml)

    Returns:
        list: Item types to deploy (e.g., ['Notebook', 'DataPipeline'])
    """
    deployment = config.get('deployment', {})
    return deployment.get('item_types', {}).get(workspace_type, [])


def deploy_workspace(
    environment: str,
    workspace_type: str,
    config: dict,
    repository_root: Path,
    cleanup_orphans: bool = True
) -> bool:
    """Deploy items to a single workspace using fabric-cicd."""
    workspace_id = get_workspace_id(environment, workspace_type)

    if not workspace_id:
        print(f"ERROR: {environment}_{workspace_type.upper()}_WORKSPACE_ID not set")
        return False

    repository_directory = (repository_root / "solution" / workspace_type).resolve()
    item_types = get_item_types_for_workspace(workspace_type, config)

    print(f"\n--- Deploying {workspace_type} to {environment} ---")
    print(f"  Workspace: {workspace_id}")
    print(f"  Items: {item_types}")

    try:
        target_workspace = FabricWorkspace(
            workspace_id=workspace_id,
            environment=environment,
            repository_directory=str(repository_directory),
            item_type_in_scope=item_types
        )

        publish_all_items(target_workspace)
        print(f"  Published successfully")

        if cleanup_orphans:
            try:
                unpublish_all_orphan_items(target_workspace)
                print(f"  Orphan cleanup complete")
            except Exception as e:
                print(f"  Warning: Orphan cleanup issue: {e}")

        return True

    except Exception as e:
        print(f"  ERROR: {type(e).__name__}: {e}")
        return False


def deploy_all_workspaces(
    environment: str,
    config: dict,
    repository_root: Path,
    cleanup_orphans: bool = True
) -> bool:
    """Deploy all workspace types in dependency order."""
    deployment = config.get('deployment', {})
    deployment_order = deployment.get('order', ['datastores', 'processing', 'consumption'])

    print(f"\nDeploying to {environment}: {' -> '.join(deployment_order)}")

    results = {}
    for workspace_type in deployment_order:
        results[workspace_type] = deploy_workspace(
            environment=environment,
            workspace_type=workspace_type,
            config=config,
            repository_root=repository_root,
            cleanup_orphans=cleanup_orphans
        )

    # Summary
    print(f"\n--- Summary ---")
    for ws_type, success in results.items():
        print(f"  {ws_type}: {'OK' if success else 'FAILED'}")

    return all(results.values())


def main():
    parser = argparse.ArgumentParser(description='Deploy Fabric items using fabric-cicd')
    parser.add_argument('--environment', '-e', required=True,
                        choices=['TEST', 'PROD'],
                        help='Target environment')
    parser.add_argument('--workspace-type', '-w',
                        choices=['processing', 'datastores', 'consumption'],
                        help='Specific workspace type to deploy')
    parser.add_argument('--all-workspaces', '-a', action='store_true',
                        help='Deploy all workspace types in order')
    parser.add_argument('--no-cleanup', action='store_true',
                        help='Skip orphan item cleanup')
    parser.add_argument('--config', '-c', default='config/templates/v01/v01-template.yml',
                        help='Path to configuration file')

    args = parser.parse_args()

    if not args.workspace_type and not args.all_workspaces:
        parser.error("Either --workspace-type or --all-workspaces is required")

    # Load .env for local testing
    if not os.getenv('GITHUB_ACTIONS'):
        env_file = Path(__file__).parent.parent.parent / '.env'
        if env_file.exists():
            load_dotenv(env_file)

    repository_root = Path(__file__).parent.parent.parent
    config_path = repository_root / args.config

    if not config_path.exists():
        print(f"ERROR: Config not found: {config_path}")
        sys.exit(1)

    config = load_config(str(config_path))
    cleanup_orphans = not args.no_cleanup

    if args.all_workspaces:
        success = deploy_all_workspaces(
            environment=args.environment,
            config=config,
            repository_root=repository_root,
            cleanup_orphans=cleanup_orphans
        )
    else:
        success = deploy_workspace(
            environment=args.environment,
            workspace_type=args.workspace_type,
            config=config,
            repository_root=repository_root,
            cleanup_orphans=cleanup_orphans
        )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
