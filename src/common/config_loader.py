"""YAML configuration loader with minimal validation."""

from pathlib import Path
from typing import Any, Dict

import yaml


class ConfigError(Exception):
    """Raised when config loading or validation fails."""


def load_yaml_config(config_path: str) -> Dict[str, Any]:
    """Load a YAML file into a dictionary."""
    path = Path(config_path)
    if not path.exists():
        raise ConfigError(f"Config file not found: {config_path}")

    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}

    if not isinstance(data, dict):
        raise ConfigError(f"Config must be a mapping: {config_path}")
    return data


def get_required(config: Dict[str, Any], key: str) -> Any:
    """Get a required key from config or raise a clear exception."""
    if key not in config:
        raise ConfigError(f"Missing required config key: {key}")
    return config[key]
