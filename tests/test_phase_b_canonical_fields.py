from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from demo.catalog.loaders import load_clients_catalog, load_process_catalog, load_roles_catalog
from demo.pipeline.stage3_postprocess import enrich_instances


def _now():
    return datetime(2026, 1, 20, 12, 0, 0, tzinfo=timezone.utc)


def test_phase_b_canonical_and_owner():
    pc = load_process_catalog(Path("tests/fixtures/process_catalog.valid.yml"))
    cc = load_clients_catalog(Path("tests/fixtures/clients.valid.yml"))
    rc = load_roles_catalog(Path("tests/fixtures/roles.valid.yml"))
    inst = [
        {
            "instance_key": "thread:1",
            "candidate_client": "Altum.ai",
            "candidate_process": "Recruiting",
            "candidate_role": "ML Engineer",
            "state": {"status": "in_progress", "step": "phone screen", "confidence": 0.7, "last_updated_at": "2026-01-19T00:00:00Z"},
            "evidence": [],
        }
    ]
    enriched, phase_b = enrich_instances(inst, pc, cc, rc, _now())
    e = enriched[0]
    assert e["candidate_client_raw"] == "Altum.ai"
    assert e["canonical_client"] == "Altum"
    assert e["canonical_process"] == "hiring"
    assert e["canonical_role"] == "AI Engineer"
    assert e["owner"] == "HR"
    assert "coverage" in phase_b and "counts" in phase_b
