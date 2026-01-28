from __future__ import annotations

from demo.pipeline.reconciliation import (
    build_workflow_definition,
    generate_workflow_id,
    infer_phase_id,
    infer_steps_from_position,
    reconcile_instances,
)


def _definition():
    return build_workflow_definition(
        {
            "processes": [
                {
                    "id": "hiring",
                    "name": "Hiring",
                    "phases": [
                        {
                            "id": "phase-1",
                            "name": "Phase 1",
                            "steps": [
                                {"id": "s1", "name": "Step 1"},
                                {"id": "s2", "name": "Step 2"},
                            ],
                        },
                        {
                            "id": "phase-2",
                            "name": "Phase 2",
                            "steps": [
                                {"id": "s3", "name": "Step 3"},
                            ],
                        },
                    ],
                }
            ]
        }
    )


def test_workflow_id_stability():
    wid1 = generate_workflow_id("hiring", "Altum", "AI Engineer", "inst-1", "Altum", "AI Engineer")
    wid2 = generate_workflow_id("hiring", "Altum", "AI Engineer", "inst-1", "Altum", "AI Engineer")
    assert wid1 == wid2

    wid3 = generate_workflow_id(None, None, None, "inst-1", "Altum", "AI Engineer")
    wid4 = generate_workflow_id(None, None, None, "inst-1", "Altum", "AI Engineer")
    assert wid3 == wid4


def test_phase_derivation():
    definition = _definition()
    assert infer_phase_id("hiring", "s3", definition) == "phase-2"


def test_positional_inference():
    definition = _definition()
    steps = infer_steps_from_position("hiring", definition, "s2", "blocked", "completed_inferred")
    assert steps[0]["status"] == "completed_inferred"
    assert steps[1]["status"] == "blocked"
    assert steps[2]["status"] == "not_started"


def test_reconciliation_update_merges_evidence_and_overwrites_steps():
    definition = _definition()
    cfg = {"reconciliation": {"scope": {"hiring_only": True, "hiring_process_keys": ["hiring"]}}}
    instances = [
        {
            "instance_key": "thread:1",
            "candidate_client_raw": "Altum",
            "candidate_role_raw": "AI Engineer",
            "candidate_process_raw": "Hiring",
            "canonical_client": "Altum",
            "canonical_role": "AI Engineer",
            "canonical_process": "hiring",
            "state": {
                "status": "in_progress",
                "step": "Step 2",
                "last_updated_at": "2026-01-02T00:00:00Z",
                "confidence": 0.9,
            },
            "health": "on_track",
            "evidence": [{"message_id": "m2"}],
        }
    ]
    store = [
        {
            "workflow_id": "wf_123",
            "process_id": "hiring",
            "client": "Altum",
            "role": "AI Engineer",
            "steps": [{"id": "s1", "status": "completed"}],
            "observability": {"evidence_message_ids": ["m1"], "last_updated_at": "2026-01-01T00:00:00Z"},
        }
    ]
    workflows, _, _, _ = reconcile_instances(instances, {}, store, definition, cfg)
    updated = workflows[0]
    assert set(updated["observability"]["evidence_message_ids"]) == {"m1", "m2"}
    assert updated["steps"][1]["status"] == "in_progress"
    assert updated["observability"]["last_updated_at"] == "2026-01-02T00:00:00Z"


def test_hiring_only_scope_filters_store_but_counts_global():
    definition = _definition()
    cfg = {"reconciliation": {"scope": {"hiring_only": True, "hiring_process_keys": ["hiring"]}}}
    instances = [
        {
            "instance_key": "thread:1",
            "candidate_client_raw": "Altum",
            "candidate_role_raw": "AI Engineer",
            "candidate_process_raw": "Hiring",
            "canonical_client": "Altum",
            "canonical_role": "AI Engineer",
            "canonical_process": "hiring",
            "state": {"status": "in_progress", "step": "Step 1"},
            "health": "on_track",
            "evidence": [],
        },
        {
            "instance_key": "thread:2",
            "candidate_client_raw": "Globex",
            "candidate_role_raw": "PM",
            "candidate_process_raw": "Delivery",
            "canonical_client": "Globex",
            "canonical_role": "PM",
            "canonical_process": "delivery",
            "state": {"status": "in_progress", "step": "Step 1"},
            "health": "on_track",
            "evidence": [],
        },
    ]
    workflows, coverage, _, _ = reconcile_instances(instances, {}, [], definition, cfg)
    assert len(workflows) == 1
    assert coverage["global"]["incoming_total"] == 2
    assert coverage["hiring_funnel"]["incoming_hiring_total"] == 1
