from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml


class ConfigError(Exception):
    """Raised when configuration is missing or invalid."""


def load_config(path: str | Path) -> Dict[str, Any]:
    """
    Load YAML config and validate minimal contract for Stage 0.

    Required:
      - io.runs_dir
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at: {config_path}")
    try:
        with config_path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in config file: {config_path}") from e

    # Validate presence of io.runs_dir
    io_section = data.get("io")
    if not isinstance(io_section, dict):
        raise ConfigError("Missing required section: io")
    runs_dir = io_section.get("runs_dir")
    if not runs_dir or not isinstance(runs_dir, (str, Path)):
        raise ConfigError("Missing required key: io.runs_dir")

    return data
