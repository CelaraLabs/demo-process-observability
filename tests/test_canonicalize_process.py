from __future__ import annotations

from pathlib import Path

from demo.catalog.loaders import load_process_catalog
from demo.catalog.canonicalize import canonicalize_process


def test_canonicalize_process_basic():
    pc = load_process_catalog(Path("tests/fixtures/process_catalog.valid.yml"))
    assert canonicalize_process("hiring", pc) == "hiring"
    assert canonicalize_process("Hiring", pc) == "hiring"
    assert canonicalize_process("recruiting pipeline", pc) == "hiring"
    assert canonicalize_process("Recruiting", pc) == "hiring"
    # Unknown returns None
    assert canonicalize_process("foo", pc) is None
