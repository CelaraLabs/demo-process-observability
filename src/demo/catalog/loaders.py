from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import yaml

from .models import ClientsCatalog, ProcessCatalog, RolesCatalog


def load_yaml(path: Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"YAML file not found: {p}")
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
        if data is None:
            data = {}
        if not isinstance(data, dict):
            raise ValueError(f"Top-level YAML must be a mapping: {p}")
        return data
    except Exception as e:
        raise ValueError(f"Failed to parse YAML at {p}: {e}") from e


def load_process_catalog(path: Path) -> ProcessCatalog:
    data = load_yaml(path)
    try:
        return ProcessCatalog.model_validate(data)
    except Exception as e:
        raise ValueError(f"Invalid process catalog at {path}: {e}") from e


def load_clients_catalog(path: Path) -> ClientsCatalog:
    data = load_yaml(path)
    try:
        return ClientsCatalog.model_validate(data)
    except Exception as e:
        raise ValueError(f"Invalid clients catalog at {path}: {e}") from e


def load_roles_catalog(path: Path) -> RolesCatalog:
    data = load_yaml(path)
    try:
        return RolesCatalog.model_validate(data.get("roles") or data)
    except Exception as e:
        raise ValueError(f"Invalid roles catalog at {path}: {e}") from e
