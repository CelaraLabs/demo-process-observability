from __future__ import annotations

from pathlib import Path
import pytest

from demo.catalog.loaders import load_clients_catalog, load_process_catalog, load_roles_catalog


def test_load_valid_catalogs(tmp_path: Path):
    base = Path("tests/fixtures")
    pc = load_process_catalog(base / "process_catalog.valid.yml")
    assert "hiring" in pc.processes
    cc = load_clients_catalog(base / "clients.valid.yml")
    assert any(c.name == "Altum" for c in cc.clients)
    rc = load_roles_catalog(base / "roles.valid.yml")
    assert "Other" in rc.canonical and "Unknown" in rc.canonical


def test_invalid_process_catalog_raises(tmp_path: Path):
    base = Path("tests/fixtures")
    with pytest.raises(ValueError):
        load_process_catalog(base / "process_catalog.invalid.yml")
