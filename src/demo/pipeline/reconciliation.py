from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml
from rich.console import Console

from ..utils.json_utils import write_json
from ..utils.time import utc_now_iso

console = Console()


@dataclass
class WorkflowDefinition:
    processes_by_id: Dict[str, Dict[str, Any]]
    process_steps_in_order: Dict[str, List[str]]
    step_to_phase: Dict[Tuple[str, str], str]
    phase_to_steps: Dict[Tuple[str, str], List[str]]
    phases_in_order: Dict[str, List[str]]
    phase_info: Dict[Tuple[str, str], Dict[str, Any]]
    step_info: Dict[Tuple[str, str], Dict[str, Any]]


@dataclass
class ReconciliationResult:
    workflows_written: int
    match_counts: Dict[str, int]
    coverage_report: Dict[str, Any]
    reconciliation_report: Dict[str, Any]
    drift_report: Dict[str, Any]
    output_files: Dict[str, str]


def _norm_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return " ".join(str(value).replace("-", " ").replace("_", " ").split()).lower()


def _load_yaml(path: Path) -> Dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def build_workflow_definition(data: Dict[str, Any]) -> WorkflowDefinition:
    processes = data.get("processes") or []
    processes_by_id: Dict[str, Dict[str, Any]] = {}
    process_steps_in_order: Dict[str, List[str]] = {}
    step_to_phase: Dict[Tuple[str, str], str] = {}
    phase_to_steps: Dict[Tuple[str, str], List[str]] = {}
    phases_in_order: Dict[str, List[str]] = {}
    phase_info: Dict[Tuple[str, str], Dict[str, Any]] = {}
    step_info: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for proc in processes:
        process_id = proc.get("id")
        if not process_id:
            continue
        processes_by_id[process_id] = proc
        phases = proc.get("phases") or []
        phases_in_order[process_id] = []
        steps_flat: List[str] = []
        for phase in phases:
            phase_id = phase.get("id")
            if not phase_id:
                continue
            phases_in_order[process_id].append(phase_id)
            phase_info[(process_id, phase_id)] = {
                "id": phase_id,
                "name": phase.get("name"),
            }
            phase_steps: List[str] = []
            for step in (phase.get("steps") or []):
                step_id = step.get("id")
                if not step_id:
                    continue
                step_info[(process_id, step_id)] = {
                    "id": step_id,
                    "name": step.get("name"),
                }
                step_to_phase[(process_id, step_id)] = phase_id
                phase_steps.append(step_id)
                steps_flat.append(step_id)
            phase_to_steps[(process_id, phase_id)] = phase_steps
        process_steps_in_order[process_id] = steps_flat

    return WorkflowDefinition(
        processes_by_id=processes_by_id,
        process_steps_in_order=process_steps_in_order,
        step_to_phase=step_to_phase,
        phase_to_steps=phase_to_steps,
        phases_in_order=phases_in_order,
        phase_info=phase_info,
        step_info=step_info,
    )


def load_workflow_definition(path: Path) -> WorkflowDefinition:
    return build_workflow_definition(_load_yaml(path))


def resolve_definition_process_id(
    canonical_process: Optional[str],
    definition: WorkflowDefinition,
    recruiting_process_keys: Iterable[str],
) -> Optional[str]:
    if not canonical_process:
        return None
    if canonical_process in definition.processes_by_id:
        return canonical_process
    canon_norm = _norm_text(canonical_process)
    for pid, proc in definition.processes_by_id.items():
        if _norm_text(pid) == canon_norm or _norm_text(proc.get("name")) == canon_norm:
            return pid
    # If a process exists in definition that is part of recruiting keys, prefer it.
    for pid in definition.processes_by_id.keys():
        if pid in set(recruiting_process_keys):
            return pid
    return None


def match_step_in_definition(
    raw_step: Optional[str],
    process_id: Optional[str],
    definition: WorkflowDefinition,
) -> Optional[str]:
    if not raw_step or not process_id:
        return None
    steps = definition.process_steps_in_order.get(process_id)
    if not steps:
        return None
    raw_norm = _norm_text(raw_step)
    if not raw_norm:
        return None
    candidates: List[str] = []
    for step_id in steps:
        step_meta = definition.step_info.get((process_id, step_id), {})
        step_name = step_meta.get("name") or ""
        if _norm_text(step_id) == raw_norm or _norm_text(step_name) == raw_norm:
            return step_id
        step_norm = _norm_text(step_id)
        name_norm = _norm_text(step_name)
        if step_norm and (step_norm in raw_norm or raw_norm in step_norm):
            candidates.append(step_id)
        elif name_norm and (name_norm in raw_norm or raw_norm in name_norm):
            candidates.append(step_id)
    if len(set(candidates)) == 1:
        return candidates[0]
    return None


def derive_current_step_id(
    instance: Dict[str, Any],
    process_id: Optional[str],
    definition: WorkflowDefinition,
) -> Optional[str]:
    if not process_id:
        return None
    steps = definition.process_steps_in_order.get(process_id)
    if not steps:
        return None

    state = instance.get("state") or {}
    for key in ("current_step_id", "step_id", "canonical_step_id"):
        cand = state.get(key) or instance.get(key)
        if cand in steps:
            return cand

    cand = instance.get("canonical_current_step_id")
    if cand in steps:
        return cand

    steps_state = instance.get("steps_state")
    if isinstance(steps_state, dict):
        for step_id in steps:
            if steps_state.get(step_id) in {"in_progress", "blocked"}:
                return step_id
        completed = [
            step_id
            for step_id in steps
            if steps_state.get(step_id) in {"completed", "completed_inferred"}
        ]
        if completed:
            return completed[-1]

    return match_step_in_definition(state.get("step"), process_id, definition)


def infer_steps_from_position(
    process_id: Optional[str],
    definition: WorkflowDefinition,
    current_step_id: Optional[str],
    status: Optional[str],
    completed_label: str,
) -> Optional[List[Dict[str, Any]]]:
    if not process_id or not current_step_id:
        return None
    steps = definition.process_steps_in_order.get(process_id)
    if not steps or current_step_id not in steps:
        return None

    status_norm = (status or "").lower()
    index = steps.index(current_step_id)
    steps_out: List[Dict[str, Any]] = []
    for idx, step_id in enumerate(steps):
        step_meta = definition.step_info.get((process_id, step_id), {})
        if idx < index:
            step_status = completed_label
        elif idx == index:
            if status_norm == "blocked":
                step_status = "blocked"
            elif status_norm in {"done", "completed"}:
                step_status = "completed"
            else:
                step_status = "in_progress"
        else:
            step_status = "not_started"
        steps_out.append(
            {"id": step_id, "name": step_meta.get("name"), "status": step_status}
        )
    return steps_out


def infer_phase_id(
    process_id: Optional[str],
    current_step_id: Optional[str],
    definition: WorkflowDefinition,
) -> str:
    if not process_id or not current_step_id:
        return "unknown"
    return definition.step_to_phase.get((process_id, current_step_id), "unknown")


def infer_phases_from_steps(
    process_id: Optional[str],
    definition: WorkflowDefinition,
    steps: Optional[List[Dict[str, Any]]],
    completed_label: str,
) -> Optional[List[Dict[str, Any]]]:
    if not process_id or not steps:
        return None
    phases = definition.phases_in_order.get(process_id)
    if not phases:
        return None
    status_by_step = {s["id"]: s.get("status") for s in steps}
    phases_out: List[Dict[str, Any]] = []
    for phase_id in phases:
        phase_steps = definition.phase_to_steps.get((process_id, phase_id), [])
        statuses = [status_by_step.get(step_id) for step_id in phase_steps]
        if not statuses:
            phase_status = "unknown"
        elif all(s in {"completed", completed_label} for s in statuses):
            phase_status = completed_label
        elif any(s in {"in_progress", "blocked"} for s in statuses):
            phase_status = "in_progress"
        else:
            phase_status = "not_started"
        phase_meta = definition.phase_info.get((process_id, phase_id), {})
        phases_out.append(
            {"id": phase_id, "name": phase_meta.get("name"), "status": phase_status}
        )
    return phases_out


def generate_workflow_id(
    canonical_process: Optional[str],
    canonical_client: Optional[str],
    canonical_role: Optional[str],
    instance_key: Optional[str],
    raw_client: Optional[str],
    raw_role: Optional[str],
) -> str:
    if canonical_process and canonical_client and canonical_role:
        base = f"{canonical_process}|{canonical_client}|{canonical_role}"
    else:
        base = f"{instance_key}|{canonical_process or ''}|{raw_client or ''}|{raw_role or ''}"
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]
    return f"wf_{digest}"


def _display_name(role: Optional[str], client: Optional[str]) -> str:
    role = role or "Unknown Role"
    client = client or "Unknown Client"
    return f"{role} - {client}"


def _display_key(role: Optional[str], client: Optional[str], process_id: Optional[str]) -> str:
    role = role or "Unknown Role"
    client = client or "Unknown Client"
    process_id = process_id or "unknown"
    return f"{role} - {client} - {process_id}"


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _merge_evidence_ids(
    existing: Iterable[str], incoming: Iterable[str], max_ids: int
) -> List[str]:
    seen = set()
    merged: List[str] = []
    for mid in list(existing) + list(incoming):
        if not mid or mid in seen:
            continue
        seen.add(mid)
        merged.append(mid)
        if len(merged) >= max_ids:
            break
    return merged


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _choose_latest_ts(existing: Optional[str], incoming: Optional[str]) -> Optional[str]:
    if not incoming:
        return existing
    if not existing:
        return incoming
    ex = _parse_iso(existing)
    inc = _parse_iso(incoming)
    if not ex or not inc:
        return incoming
    return incoming if inc >= ex else existing


def _get_reconciliation_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    recon_cfg = cfg.get("reconciliation", {}) if cfg else {}
    scope = recon_cfg.get("scope", {})
    reconcile = recon_cfg.get("reconcile", {})
    inference = recon_cfg.get("inference", {})
    reports = recon_cfg.get("reports", {})
    store = recon_cfg.get("store", {})
    return {
        "enabled": bool(recon_cfg.get("enabled", True)),
        "store": {
            "persistent_path": store.get("persistent_path", "data/workflow_store.json"),
            "snapshot_name": store.get("snapshot_name", "workflow_store.snapshot.json"),
        },
        "scope": {
            "recruiting_only": bool(scope.get("recruiting_only", scope.get("hiring_only", True))),
            "recruiting_process_keys": list(
                scope.get("recruiting_process_keys", scope.get("hiring_process_keys", ["recruiting"]))
            ),
        },
        "reconcile": {
            "match": {
                "method": reconcile.get("match", {}).get("method", "key_then_fuzzy"),
                "exact_key_fields": reconcile.get("match", {}).get(
                    "exact_key_fields", ["canonical_client", "canonical_role", "canonical_process"]
                ),
                "fuzzy_threshold": float(reconcile.get("match", {}).get("fuzzy_threshold", 0.88)),
            },
            "evidence": {
                "max_ids_per_instance": int(reconcile.get("evidence", {}).get("max_ids_per_instance", 200)),
                "timeline_fallback_max": int(reconcile.get("evidence", {}).get("timeline_fallback_max", 30)),
            },
        },
        "inference": {
            "positional": {
                "enabled": bool(inference.get("positional", {}).get("enabled", True)),
                "completed_label": inference.get("positional", {}).get("completed_label", "completed_inferred"),
            }
        },
        "reports": {
            "coverage_name": reports.get("coverage_name", "coverage_report.json"),
            "reconciliation_name": reports.get("reconciliation_name", "reconciliation_report.json"),
            "drift_name": reports.get("drift_name", "mapping_drift_report.json"),
        },
    }


def _extract_evidence_ids(
    instance: Dict[str, Any],
    timeline_by_instance: Optional[Dict[str, List[Dict[str, Any]]]],
    max_ids: int,
    fallback_max: int,
) -> List[str]:
    ids = [mid for mid in (instance.get("evidence_message_ids") or []) if mid]
    if not ids:
        for ev in instance.get("evidence") or []:
            mid = ev.get("message_id")
            if mid:
                ids.append(mid)
    if not ids and timeline_by_instance is not None:
        instance_key = instance.get("instance_key")
        timeline = (timeline_by_instance or {}).get(instance_key) or []
        timeline_ids = [t.get("message_id") for t in timeline if t.get("message_id")]
        if timeline_ids:
            ids = timeline_ids[-fallback_max:]
    return _merge_evidence_ids([], ids, max_ids)


def _coverage_pct(n: int, total: int) -> float:
    return round((n / total) if total else 0.0, 4)


def _is_known_role(canon_role: Optional[str]) -> bool:
    return canon_role not in (None, "", "Other", "Unknown")


def reconcile_instances(
    instances: List[Dict[str, Any]],
    timeline_by_instance: Optional[Dict[str, List[Dict[str, Any]]]],
    store_workflows: List[Dict[str, Any]],
    definition: WorkflowDefinition,
    cfg: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    settings = _get_reconciliation_config(cfg)
    recruiting_keys = settings["scope"]["recruiting_process_keys"]
    recruiting_only = settings["scope"]["recruiting_only"]
    exact_fields = settings["reconcile"]["match"]["exact_key_fields"]
    fuzzy_threshold = settings["reconcile"]["match"]["fuzzy_threshold"]
    max_ids = settings["reconcile"]["evidence"]["max_ids_per_instance"]
    fallback_max = settings["reconcile"]["evidence"]["timeline_fallback_max"]
    positional_enabled = settings["inference"]["positional"]["enabled"]
    completed_label = settings["inference"]["positional"]["completed_label"]

    # Coverage and drift counters
    total = len(instances)
    cov_process = cov_client = cov_step = cov_health = cov_evidence = 0
    recruiting_cov_process = 0
    recruiting_cov_client = 0
    recruiting_cov_role = 0
    recruiting_cov_step = 0
    role_detected = 0
    canonical_step_present = 0
    step_match_failures = Counter()
    recruiting_total = 0
    non_recruiting_missing_process = 0
    non_recruiting_not_recruiting = 0
    drift_client = Counter()
    drift_role = Counter()
    drift_process = Counter()
    drift_step = Counter()

    # Start from recruiting-only view if configured
    workflows = list(store_workflows or [])
    if recruiting_only:
        workflows = [wf for wf in workflows if wf.get("process_id") in recruiting_keys]

    def workflow_exact_key(wf: Dict[str, Any]) -> Optional[Tuple[str, ...]]:
        obs = wf.get("observability") or {}
        vals: List[str] = []
        for field in exact_fields:
            if field == "canonical_process":
                val = obs.get("canonical_process") or wf.get("process_id")
            elif field == "canonical_client":
                val = obs.get("canonical_client") or wf.get("client")
            elif field == "canonical_role":
                val = obs.get("canonical_role") or wf.get("role")
            else:
                val = obs.get(field) or wf.get(field)
            if not val:
                return None
            vals.append(str(val))
        return tuple(vals)

    existing_exact: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    for wf in workflows:
        key = workflow_exact_key(wf)
        if key:
            existing_exact[key] = wf
    existing_fuzzy = [
        (wf, _display_key(wf.get("role"), wf.get("client"), wf.get("process_id")))
        for wf in workflows
    ]

    match_counts = {"exact": 0, "fuzzy": 0, "created": 0}
    recruiting_written_total = 0
    recruiting_steps_populated = 0
    recruiting_phase_known = 0
    recruiting_evidence = 0

    for inst in instances:
        canon_process = inst.get("canonical_process")
        canon_client = inst.get("canonical_client")
        canon_role = inst.get("canonical_role")
        if canon_process:
            cov_process += 1
        if canon_client:
            cov_client += 1
        canon_role_norm = (canon_role or "").strip()
        if canon_role_norm and canon_role_norm != "Unknown":
            role_detected += 1

        instance_key = inst.get("instance_key")
        evidence_ids = _extract_evidence_ids(inst, timeline_by_instance, max_ids, fallback_max)
        if evidence_ids:
            cov_evidence += 1

        health = inst.get("health")
        if health in {"on_track", "at_risk", "overdue"}:
            cov_health += 1

        process_id = canon_process or "unknown"
        if process_id == "hiring":
            process_id = "recruiting"
        definition_pid = resolve_definition_process_id(canon_process, definition, recruiting_keys)
        current_step_id = derive_current_step_id(inst, definition_pid, definition)
        if current_step_id:
            cov_step += 1

        canonical_step_id = inst.get("canonical_current_step_id")
        if canonical_step_id is not None:
            canonical_step_present += 1
        match_type = inst.get("canonical_current_step_match_type")
        if match_type == "none":
            raw_step = (inst.get("state") or {}).get("step")
            if raw_step:
                step_match_failures[str(raw_step)] += 1

        if canon_process in recruiting_keys:
            recruiting_total += 1
            if canon_process:
                recruiting_cov_process += 1
            if canon_client:
                recruiting_cov_client += 1
            if canon_role_norm and canon_role_norm != "Unknown":
                recruiting_cov_role += 1
            if current_step_id:
                recruiting_cov_step += 1
        else:
            if not canon_process:
                non_recruiting_missing_process += 1
            else:
                non_recruiting_not_recruiting += 1

        if recruiting_only and canon_process not in recruiting_keys:
            # skip writes but still count drift and coverage
            if not canon_client and inst.get("candidate_client_raw"):
                drift_client[inst.get("candidate_client_raw")] += 1
            if not _is_known_role(canon_role) and inst.get("candidate_role_raw"):
                drift_role[inst.get("candidate_role_raw")] += 1
            if not canon_process and inst.get("candidate_process_raw"):
                drift_process[inst.get("candidate_process_raw")] += 1
            raw_step = (inst.get("state") or {}).get("step")
            if raw_step and not current_step_id:
                drift_step[str(raw_step)] += 1
            continue

        # Match existing workflow
        exact_key = None
        if all(inst.get(field) for field in exact_fields):
            exact_key = tuple(str(inst.get(field)) for field in exact_fields)
        matched = None
        match_type = "created"
        match_score = 0.0
        if exact_key and exact_key in existing_exact:
            matched = existing_exact[exact_key]
            match_type = "exact"
            match_score = 1.0
        else:
            display = _display_key(
                canon_role or inst.get("candidate_role_raw"),
                canon_client or inst.get("candidate_client_raw"),
                process_id,
            )
            best = None
            best_score = 0.0
            for wf, wf_display in existing_fuzzy:
                score = _similarity(display, wf_display)
                if score > best_score:
                    best = wf
                    best_score = score
            if best and best_score >= fuzzy_threshold:
                matched = best
                match_type = "fuzzy"
                match_score = best_score

        if matched is None:
            workflow_id = generate_workflow_id(
                canon_process,
                canon_client,
                canon_role,
                instance_key,
                inst.get("candidate_client_raw"),
                inst.get("candidate_role_raw"),
            )
            matched = {"workflow_id": workflow_id}
            workflows.append(matched)
            existing_fuzzy.append(
                (
                    matched,
                    _display_key(
                        canon_role or inst.get("candidate_role_raw"),
                        canon_client or inst.get("candidate_client_raw"),
                        process_id,
                    ),
                )
            )
            if exact_key:
                existing_exact[exact_key] = matched
            match_type = "created"
            match_score = 1.0

        match_counts[match_type] += 1
        recruiting_written_total += 1

        # Infer steps & phases
        steps = None
        if positional_enabled:
            steps = infer_steps_from_position(
                definition_pid,
                definition,
                current_step_id,
                (inst.get("state") or {}).get("status"),
                completed_label,
            )
        phase_id = infer_phase_id(definition_pid, current_step_id, definition)
        phases = None
        if positional_enabled:
            phases = infer_phases_from_steps(
                definition_pid,
                definition,
                steps,
                completed_label,
            )

        if steps:
            recruiting_steps_populated += 1
        if phase_id != "unknown":
            recruiting_phase_known += 1
        if evidence_ids:
            recruiting_evidence += 1

        # Update workflow fields
        matched["process_id"] = process_id
        matched["phase_id"] = phase_id
        matched["client"] = (
            canon_client or inst.get("candidate_client_raw") or inst.get("candidate_client")
        )
        matched["role"] = canon_role or inst.get("candidate_role_raw") or inst.get("candidate_role")
        matched["display_name"] = _display_name(matched.get("role"), matched.get("client"))
        matched["steps"] = steps
        matched["phases"] = phases

        obs = matched.get("observability") or {}
        obs["source_instance_key"] = instance_key
        obs["last_updated_at"] = _choose_latest_ts(
            obs.get("last_updated_at"), (inst.get("state") or {}).get("last_updated_at")
        )
        obs["confidence"] = (inst.get("state") or {}).get("confidence")
        obs["health"] = inst.get("health") or "unknown"
        obs["evidence_message_ids"] = _merge_evidence_ids(
            obs.get("evidence_message_ids") or [],
            evidence_ids,
            max_ids,
        )
        obs["canonical_process"] = canon_process
        obs["canonical_client"] = canon_client
        obs["canonical_role"] = canon_role
        obs["reconciliation"] = {
            "match_type": match_type,
            "match_score": round(float(match_score), 4),
            "matched_workflow_id": matched.get("workflow_id"),
        }
        matched["observability"] = obs

        # Drift tracking (for unmatched canonicalization)
        if not canon_client and inst.get("candidate_client_raw"):
            drift_client[inst.get("candidate_client_raw")] += 1
        if not _is_known_role(canon_role) and inst.get("candidate_role_raw"):
            drift_role[inst.get("candidate_role_raw")] += 1
        if not canon_process and inst.get("candidate_process_raw"):
            drift_process[inst.get("candidate_process_raw")] += 1
        raw_step = (inst.get("state") or {}).get("step")
        if raw_step and not current_step_id:
            drift_step[str(raw_step)] += 1

    # Coverage report
    coverage_report = {
        "global": {
            "incoming_total": total,
            "canonical_process_pct": _coverage_pct(cov_process, total),
            "canonical_client_pct": _coverage_pct(cov_client, total),
            "canonical_role_pct": _coverage_pct(role_detected, total),
            "current_step_pct": _coverage_pct(cov_step, total),
            "health_known_pct": _coverage_pct(cov_health, total),
            "evidence_ids_pct": _coverage_pct(cov_evidence, total),
            "canonical_current_step_id_pct": _coverage_pct(canonical_step_present, total),
        },
        "recruiting_funnel": {
            "incoming_recruiting_total": recruiting_total,
            "incoming_non_recruiting_total": total - recruiting_total,
            "pct_recruiting_among_total": _coverage_pct(recruiting_total, total),
            "pct_recruiting_among_known_process": _coverage_pct(recruiting_total, cov_process),
            "missing_canonical_process": non_recruiting_missing_process,
            "canonical_process_not_recruiting": non_recruiting_not_recruiting,
            "canonical_process_pct": _coverage_pct(recruiting_cov_process, recruiting_total),
            "canonical_client_pct": _coverage_pct(recruiting_cov_client, recruiting_total),
            "canonical_role_pct": _coverage_pct(recruiting_cov_role, recruiting_total),
            "current_step_pct": _coverage_pct(recruiting_cov_step, recruiting_total),
        },
        "recruiting_reconciliation": {
            "recruiting_written_total": recruiting_written_total,
            "match_counts": match_counts,
            "steps_list_pct": _coverage_pct(recruiting_steps_populated, recruiting_written_total),
            "phase_known_pct": _coverage_pct(recruiting_phase_known, recruiting_written_total),
            "evidence_ids_pct": _coverage_pct(recruiting_evidence, recruiting_written_total),
        },
    }

    reconciliation_report = {
        "workflows_written": recruiting_written_total,
        "match_counts": match_counts,
        "updated_at": utc_now_iso(),
    }

    def top(counter: Counter, n: int = 10) -> List[Dict[str, Any]]:
        return [{"value": k, "count": v} for k, v in counter.most_common(n)]

    drift_report = {
        "candidate_client_raw": top(drift_client),
        "candidate_role_raw": top(drift_role),
        "candidate_process_raw": top(drift_process),
        "raw_steps_unmatched": top(drift_step),
        "canonical_step_match_failures": top(step_match_failures),
    }

    return workflows, coverage_report, reconciliation_report, drift_report


def run_reconciliation(run_dir: Path, cfg: Dict[str, Any]) -> ReconciliationResult:
    settings = _get_reconciliation_config(cfg)
    if not settings["enabled"]:
        raise RuntimeError("Reconciliation disabled in config")

    run_dir = Path(run_dir)
    stage3_cfg = cfg.get("stage3", {})
    out_cfg = stage3_cfg.get("output", {})
    instances_name = out_cfg.get("instances", "instances.json")
    timeline_name = out_cfg.get("timeline", "timeline.json")

    instances_path = run_dir / instances_name
    timeline_path = run_dir / timeline_name
    if not instances_path.exists():
        raise FileNotFoundError(f"Missing instances file: {instances_path}")

    instances_data = json.loads(instances_path.read_text(encoding="utf-8"))
    instances = instances_data.get("instances") or []

    timeline_by_instance = None
    if timeline_path.exists():
        timeline_data = json.loads(timeline_path.read_text(encoding="utf-8"))
        timeline_by_instance = timeline_data.get("by_instance") or {}

    # workflow definition
    wf_def_path = run_dir / "workflow_definition.yml"
    if not wf_def_path.exists():
        alt = run_dir / "workflow_definition.yaml"
        if alt.exists():
            wf_def_path = alt
        else:
            for fallback in ("config/workflow_definition.yaml", "config/workflow_definition.yml"):
                if Path(fallback).exists():
                    wf_def_path = Path(fallback)
                    break
    if not wf_def_path.exists():
        raise FileNotFoundError("Missing workflow_definition.yml")

    definition = load_workflow_definition(wf_def_path)

    # Load existing store
    persistent_path = Path(settings["store"]["persistent_path"])
    store_obj = {"version": 1, "updated_at": utc_now_iso(), "workflows": []}
    if persistent_path.exists():
        try:
            store_obj = json.loads(persistent_path.read_text(encoding="utf-8"))
        except Exception:
            store_obj = {"version": 1, "updated_at": utc_now_iso(), "workflows": []}
    store_workflows = store_obj.get("workflows") or []
    # Migrate legacy process_id values in-memory
    for wf in store_workflows:
        if wf.get("process_id") == "hiring":
            wf["process_id"] = "recruiting"

    workflows, coverage_report, reconciliation_report, drift_report = reconcile_instances(
        instances, timeline_by_instance, store_workflows, definition, cfg
    )

    # Persist store and snapshot
    store_obj["version"] = store_obj.get("version", 1)
    store_obj["updated_at"] = utc_now_iso()
    store_obj["workflows"] = workflows
    write_json(persistent_path, store_obj)

    snapshot_name = settings["store"]["snapshot_name"]
    snapshot_path = run_dir / snapshot_name
    write_json(snapshot_path, store_obj)

    coverage_name = settings["reports"]["coverage_name"]
    reconciliation_name = settings["reports"]["reconciliation_name"]
    drift_name = settings["reports"]["drift_name"]
    coverage_path = run_dir / coverage_name
    reconciliation_path = run_dir / reconciliation_name
    drift_path = run_dir / drift_name
    write_json(coverage_path, coverage_report)
    write_json(reconciliation_path, reconciliation_report)
    write_json(drift_path, drift_report)

    console.print(
        "[green]Reconciliation complete[/green]: "
        f"workflows_written={reconciliation_report.get('workflows_written')} "
        f"matches={reconciliation_report.get('match_counts')}"
    )

    output_files = {
        "workflow_store_snapshot": snapshot_name,
        "coverage_report": coverage_name,
        "reconciliation_report": reconciliation_name,
        "mapping_drift_report": drift_name,
        "workflow_store_persistent": str(persistent_path),
    }
    return ReconciliationResult(
        workflows_written=reconciliation_report.get("workflows_written", 0),
        match_counts=reconciliation_report.get("match_counts", {}),
        coverage_report=coverage_report,
        reconciliation_report=reconciliation_report,
        drift_report=drift_report,
        output_files=output_files,
    )
