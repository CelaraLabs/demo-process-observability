from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

from demo.catalog.loaders import load_process_catalog, load_clients_catalog, load_roles_catalog
from demo.pipeline.stage3_postprocess import enrich_instances


def test_phase_b_health_thresholds_and_blocked():
    pc = load_process_catalog(Path("tests/fixtures/process_catalog.valid.yml"))
    cc = load_clients_catalog(Path("tests/fixtures/clients.valid.yml"))
    rc = load_roles_catalog(Path("tests/fixtures/roles.valid.yml"))
    now = datetime(2026, 1, 20, 12, 0, 0, tzinfo=timezone.utc)

    # Fresh update -> on_track
    inst1 = {
        "instance_key": "t1",
        "candidate_client": "Altum",
        "candidate_process": "Hiring",
        "candidate_role": "AI Engineer",
        "state": {"status": "in_progress", "step": "intake", "confidence": 0.7, "last_updated_at": "2026-01-19T12:00:00Z"},
        "evidence": [],
    }
    # Blocked status -> at_risk even if fresh
    inst2 = {
        "instance_key": "t2",
        "candidate_client": "Altum",
        "candidate_process": "Hiring",
        "candidate_role": "AI Engineer",
        "state": {"status": "blocked", "step": "intake", "confidence": 0.7, "last_updated_at": "2026-01-19T12:00:00Z"},
        "evidence": [],
    }
    # Stale beyond overdue threshold (14 days)
    inst3 = {
        "instance_key": "t3",
        "candidate_client": "Altum",
        "candidate_process": "Hiring",
        "candidate_role": "AI Engineer",
        "state": {"status": "in_progress", "step": "intake", "confidence": 0.7, "last_updated_at": "2025-12-31T12:00:00Z"},
        "evidence": [],
    }
    enriched, _ = enrich_instances([inst1, inst2, inst3], pc, cc, rc, now)
    h = {e["instance_key"]: e["health"] for e in enriched}
    assert h["t1"] == "on_track"
    assert h["t2"] == "at_risk"
    assert h["t3"] == "overdue"
