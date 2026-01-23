from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from demo.catalog.loaders import load_process_catalog, load_clients_catalog, load_roles_catalog
from demo.pipeline.stage3_postprocess import enrich_instances


def _now():
    return datetime(2026, 1, 20, 12, 0, 0, tzinfo=timezone.utc)


def test_phase_b_steps_state_match_and_unknown():
    pc = load_process_catalog(Path("tests/fixtures/process_catalog.valid.yml"))
    cc = load_clients_catalog(Path("tests/fixtures/clients.valid.yml"))
    rc = load_roles_catalog(Path("tests/fixtures/roles.valid.yml"))

    inst = [
        {
            "instance_key": "thread:1",
            "candidate_client": "Altum",
            "candidate_process": "Hiring",
            "candidate_role": "AI Engineer",
            "state": {"status": "in_progress", "step": "interview loop", "confidence": 0.7, "last_updated_at": "2026-01-19T00:00:00Z"},
            "evidence": [],
        },
        {
            "instance_key": "thread:2",
            "candidate_client": "Altum",
            "candidate_process": "Hiring",
            "candidate_role": "AI Engineer",
            "state": {"status": "in_progress", "step": "mystery", "confidence": 0.7, "last_updated_at": "2026-01-19T00:00:00Z"},
            "evidence": [],
        },
    ]
    enriched, _ = enrich_instances(inst, pc, cc, rc, _now())
    e1 = enriched[0]
    assert e1["steps_total"] == 5
    assert e1["steps_state"]["intake"] == "completed"
    assert e1["steps_state"]["interviews"] == "in_progress"
    e2 = enriched[1]
    assert e2["steps_state"]["intake"] == "unknown"
