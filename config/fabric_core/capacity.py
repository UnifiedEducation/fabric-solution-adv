"""Capacity management module for Azure Fabric capacities."""

import time
from .utils import call_azure_api


def capacity_exists(capacity_name, subscription_id, resource_group):
    """Check if a Fabric capacity exists in Azure."""
    status, _ = call_azure_api(
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version=2023-11-01")
    return status == 200


def create_capacity(capacity_config, subscription_id, resource_group, defaults):
    """
    Create a Fabric capacity in Azure.

    Args:
        capacity_config: Dict with capacity configuration (name, region, sku, admin_members)
        subscription_id: Azure subscription ID
        resource_group: Azure resource group name
        defaults: Dict with default values for capacity settings

    Returns:
        None
    """
    capacity_name = capacity_config['name']

    if capacity_exists(capacity_name, subscription_id, resource_group):
        print(f"  {capacity_name} exists")
        call_azure_api(
            f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/resume?api-version=2023-11-01", 'post')
        return

    admin_members = capacity_config.get(
        'admin_members', defaults.get('capacity_admins', ''))
    admin_members = admin_members if isinstance(admin_members, list) else [
        admin_id.strip() for admin_id in admin_members.split(',') if admin_id.strip()]

    request_body = {
        "location": capacity_config.get('region', defaults.get('region')),
        "sku": {"name": capacity_config.get('sku', defaults.get('sku')), "tier": "Fabric"},
        "properties": {"administration": {"members": admin_members}}
    }

    status, response = call_azure_api(
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}?api-version=2023-11-01", 'put', request_body)

    if status in [200, 201]:
        print(f"  Created {capacity_name}")
        time.sleep(40)


def suspend_capacity(capacity_name, subscription_id, resource_group):
    """
    Suspend a Fabric capacity to stop billing.

    Args:
        capacity_name: Name of the capacity to suspend
        subscription_id: Azure subscription ID
        resource_group: Azure resource group name

    Returns:
        bool: True if suspended successfully, False otherwise
    """
    for _ in range(5):
        status, _ = call_azure_api(
            f"subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Fabric/capacities/{capacity_name}/suspend?api-version=2023-11-01", 'post')
        if status in [200, 202]:
            print(f"  Suspended {capacity_name}")
            return True
        time.sleep(60)
    print(f"  ERROR: Failed to suspend {capacity_name}")
    return False
