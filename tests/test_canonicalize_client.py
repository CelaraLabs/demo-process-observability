from __future__ import annotations

from pathlib import Path

from demo.catalog.loaders import load_clients_catalog
from demo.catalog.canonicalize import canonicalize_client


def test_canonicalize_client_alias_and_fallback():
    cc = load_clients_catalog(Path("tests/fixtures/clients.valid.yml"))
    assert canonicalize_client("Altum.ai", cc) == "Altum"
    assert canonicalize_client("ForwardFinancing", cc) == "Forward Financing"
    # Unknown returns cleaned original Title Case
    assert canonicalize_client("foo corp", cc) == "Foo Corp"
