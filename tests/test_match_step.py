from __future__ import annotations

from pathlib import Path

from demo.catalog.loaders import load_process_catalog
from demo.catalog.canonicalize import match_step


def test_match_step_alias_and_direct():
    pc = load_process_catalog(Path("tests/fixtures/process_catalog.valid.yml"))
    assert match_step("phone screen", "hiring", pc) == "screening"
    assert match_step("interview loop", "hiring", pc) == "interviews"
    assert match_step("close", "hiring", pc) == "close"
    assert match_step("unknown", "hiring", pc) is None
    assert match_step("screen", "hiring", pc) is None
