from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..catalog.loaders import (
    load_clients_catalog,
    load_process_catalog,
    load_roles_catalog,
)
from ..catalog.canonicalize import (
    canonicalize_client,
    canonicalize_process,
    canonicalize_role,
    match_step,
)


def _safe_parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(ts)
        # If naive, assume UTC; always return timezone-aware
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _attach_raw_fields(instance: Dict[str, Any]) -> None:
    # Preserve raw candidate fields if not already present
    if "candidate_process_raw" not in instance:
        instance["candidate_process_raw"] = instance.get("candidate_process")
    if "candidate_client_raw" not in instance:
        instance["candidate_client_raw"] = instance.get("candidate_client")
    if "candidate_role_raw" not in instance:
        instance["candidate_role_raw"] = instance.get("candidate_role")


def _canonicalize_fields(
    instance: Dict[str, Any],
    catalogs: Tuple[Any, Any, Any],
) -> None:
    process_catalog, clients_catalog, roles_catalog = catalogs
    cp_raw = instance.get("candidate_process_raw")
    cc_raw = instance.get("candidate_client_raw")
    cr_raw = instance.get("candidate_role_raw")

    canon_process = None
    canon_client = None
    canon_role = None
    if process_catalog:
        canon_process = canonicalize_process(cp_raw, process_catalog)
    if clients_catalog:
        canon_client = canonicalize_client(cc_raw, clients_catalog)
    if roles_catalog:
        canon_role = canonicalize_role(cr_raw, roles_catalog)

    instance["canonical_process"] = canon_process
    instance["canonical_client"] = canon_client
    instance["canonical_role"] = canon_role

    # Owner from process catalog
    owner = None
    if canon_process and process_catalog and canon_process in process_catalog.processes:
        owner = process_catalog.processes[canon_process].owner
    instance["owner"] = owner


def _compute_steps(
    instance: Dict[str, Any],
    process_catalog,
) -> None:
    canon_process = instance.get("canonical_process")
    if not canon_process or not process_catalog or canon_process not in process_catalog.processes:
        instance["steps_total"] = None
        instance["steps_done"] = None
        instance["steps_state"] = None
        return
    spec = process_catalog.processes[canon_process]
    steps = list(spec.steps or [])
    steps_state: Dict[str, str] = {}
    matched = match_step(
        ((instance.get("state") or {}).get("step")),
        canon_process,
        process_catalog,
    )
    if matched is None:
        for s in steps:
            steps_state[s] = "unknown"
        instance["steps_total"] = len(steps)
        instance["steps_done"] = 0
        instance["steps_state"] = steps_state
        return
    status = ((instance.get("state") or {}).get("status")) or "unknown"
    # Fill states by position
    seen_matched = False
    steps_done = 0
    for s in steps:
        if s == matched:
            if status == "blocked":
                steps_state[s] = "blocked"
            elif status == "done":
                steps_state[s] = "completed"
                steps_done += 1
            else:
                steps_state[s] = "in_progress"
            seen_matched = True
        else:
            if not seen_matched:
                steps_state[s] = "completed"
                steps_done += 1
            else:
                steps_state[s] = "not_started"
    # If matched was not in steps (shouldn't happen), fall back to unknowns
    if matched not in steps:
        steps_state = {s: "unknown" for s in steps}
        steps_done = 0
    instance["steps_total"] = len(steps)
    instance["steps_done"] = steps_done
    instance["steps_state"] = steps_state


def _compute_health(instance: Dict[str, Any], process_catalog, now: datetime) -> None:
    canon_process = instance.get("canonical_process")
    state = instance.get("state") or {}
    last_updated = _safe_parse_iso(state.get("last_updated_at"))
    if not last_updated:
        instance["health"] = "unknown"
        return
    # Compute days since update
    delta_days = (now - last_updated).total_seconds() / 86400.0
    # Load thresholds if available
    at_risk_days = None
    overdue_days = None
    if canon_process and process_catalog and canon_process in process_catalog.processes:
        health = process_catalog.processes[canon_process].health
        if health:
            at_risk_days = getattr(health, "at_risk_after_days", None)
            overdue_days = getattr(health, "overdue_after_days", None)
    status = (state.get("status") or "").lower()
    if overdue_days is not None and delta_days >= float(overdue_days):
        instance["health"] = "overdue"
    elif status == "blocked":
        instance["health"] = "at_risk"
    elif at_risk_days is not None and delta_days >= float(at_risk_days):
        instance["health"] = "at_risk"
    else:
        instance["health"] = "on_track"


def enrich_instances(
    instances: List[Dict[str, Any]],
    process_catalog,
    clients_catalog,
    roles_catalog,
    now: datetime,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns enriched instances and phase_b stats dict for run_meta.
    """
    total = len(instances or [])
    counts_by_health: Dict[str, int] = {"on_track": 0, "at_risk": 0, "overdue": 0, "unknown": 0}
    cov_process = 0
    cov_steps = 0
    cov_health = 0
    enriched: List[Dict[str, Any]] = []
    catalogs = (process_catalog, clients_catalog, roles_catalog)

    for inst in instances or []:
        obj = dict(inst)
        _attach_raw_fields(obj)
        _canonicalize_fields(obj, catalogs)
        # Steps & health
        _compute_steps(obj, process_catalog)
        _compute_health(obj, process_catalog, now=now)
        # Coverage accounting
        if obj.get("canonical_process") is not None:
            cov_process += 1
        if obj.get("steps_state") is not None:
            cov_steps += 1
        h = obj.get("health") or "unknown"
        if h in counts_by_health:
            counts_by_health[h] += 1
        if h != "unknown":
            cov_health += 1
        enriched.append(obj)

    def pct(n: int) -> float:
        return round((n / total) if total else 0.0, 4)

    phase_b = {
        "coverage": {
            "canonical_process_pct": pct(cov_process),
            "steps_state_pct": pct(cov_steps),
            "health_pct": pct(cov_health),
        },
        "counts": {"by_health": counts_by_health},
    }
    return enriched, phase_b
