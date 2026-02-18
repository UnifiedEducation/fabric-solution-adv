"""Utility functions for Fabric CLI operations."""

import os
import sys
import yaml
import json
import shutil
import subprocess
from pathlib import Path
from string import Template


def get_fabric_cli_path():
    """Get the path to the Fabric CLI executable."""
    return str(Path(sys.prefix) / 'Scripts' / 'fab.exe') if hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix and (Path(sys.prefix) / 'Scripts' / 'fab.exe').exists() else shutil.which('fab') or 'fab'


def run_command(cmd):
    """Execute a subprocess command and return the result."""
    return subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')


def call_azure_api(endpoint, method='get', body=None):
    """Call Azure ARM REST API via Fabric CLI and return status code and response body."""
    cmd = [get_fabric_cli_path(), 'api', endpoint, '-X', method, '-A', 'azure']
    if body:
        cmd.extend(['-i', json.dumps(body)])
    response = run_command(cmd)
    try:
        parsed = json.loads(response.stdout)
        return parsed.get('status_code', 0), parsed.get('text', {})
    except (json.JSONDecodeError, KeyError):
        return 0, {}


def load_config(config_file_path):
    """Load YAML configuration file with environment variable substitution."""
    with open(config_file_path) as file:
        yaml_content = file.read()
    temp_config = yaml.safe_load(yaml_content)
    yaml_content = yaml_content.replace(
        '{{SOLUTION_VERSION}}', temp_config.get('solution_version', 'AV01'))
    return yaml.safe_load(Template(yaml_content).safe_substitute(os.environ))
