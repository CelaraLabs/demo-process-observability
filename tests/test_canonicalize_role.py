from __future__ import annotations

from pathlib import Path

from demo.catalog.loaders import load_roles_catalog
from demo.catalog.canonicalize import canonicalize_role


def test_canonicalize_role_rules():
    rc = load_roles_catalog(Path("tests/fixtures/roles.valid.yml"))
    assert canonicalize_role("ML Engineer", rc) == "AI Engineer"
    assert canonicalize_role(None, rc) == "Unknown"
    assert canonicalize_role("Totally Random", rc) == "Other"
