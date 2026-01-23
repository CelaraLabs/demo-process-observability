from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from rich.console import Console

from ..llm.client import OpenAIClient, LLMClientError
from ..llm.types import Pass1Event
from ..utils.json_utils import read_jsonl, write_json
from .stage3_postprocess import enrich_instances
from ..catalog.loaders import load_process_catalog, load_clients_catalog, load_roles_catalog
from datetime import datetime, timezone

console = Console()


def _slug(text: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in text).strip("-")


def _event_filter(ev: Dict[str, Any], min_conf: float) -> bool:
    if ev.get("event_type") == "unrelated":
        return False
    try:
        conf = float(ev.get("confidence", 0.0))
    except Exception:
        conf = 0.0
    return conf >= min_conf


def canonicalize_process(process: Optional[str]) -> Optional[str]:
    """
    Normalize candidate_process values to reduce synonym drift.
    Rules:
      - None/empty -> None
      - lowercase, strip, collapse spaces
      - map common synonyms to canonical labels
      - else return cleaned original in Title Case
    """
    if not process or not isinstance(process, str):
        return None
    cleaned = " ".join(process.strip().lower().split())
    if not cleaned:
        return None
    # Synonym maps
    if cleaned in {"recruiting", "hiring"}:
        return "hiring"
    if "recruiting pipeline" in cleaned or "ai search" in cleaned or "ai searching" in cleaned:
        return "hiring"
    if cleaned in {"software delivery", "delivery"}:
        return "delivery"
    if cleaned in {"operations", "ops"}:
        return "ops"
    # Keep cleaned original but normalize casing to title
    return cleaned.title()


def _event_timestamp_str(ev: Dict[str, Any]) -> Optional[str]:
    """
    Choose timestamp from Pass1Event with priority:
      1) evidence.timestamp
      2) event.timestamp (top-level string if present)
      3) None
    """
    evd = ev.get("evidence") or {}
    ts = evd.get("timestamp")
    if ts:
        return ts
    return ev.get("timestamp")


def _group_by_thread(events: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for ev in events:
        thread_id = ev.get("thread_id")
        key = f"thread:{thread_id}" if thread_id else f"msg:{ev.get('message_id')}"
        groups[key].append(ev)
    return groups


def _maybe_split_by_process(events: List[Dict[str, Any]]) -> List[Tuple[str, List[Dict[str, Any]]]]:
    # Compute distinct candidate_process values
    # Canonicalize for counting/splitting
    canon_vals: List[Optional[str]] = []
    for e in events:
        e["_canon_proc"] = canonicalize_process(e.get("candidate_process"))
        if e["_canon_proc"]:
            canon_vals.append(e["_canon_proc"])
    non_null = canon_vals
    counts = Counter(non_null)
    if len([p for p, c in counts.items() if c >= 2]) >= 2:
        # Split
        result: List[Tuple[str, List[Dict[str, Any]]]] = []
        for proc, _ in counts.items():
            cluster_events = [e for e in events if e.get("_canon_proc") == proc]
            if cluster_events:
                result.append((f"proc:{_slug(proc)}", cluster_events))
        # Also include unassigned (null) if any
        unassigned = [e for e in events if not e.get("_canon_proc")]
        if unassigned:
            result.append(("proc:unknown", unassigned))
        return result
    else:
        return [("", events)]


def _det_instance_key(base_key: str, suffix: str) -> str:
    return f"{base_key}|{suffix}" if suffix else base_key


def _build_prompt_input(instance_key: str, thread_id: Optional[str], events: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Sort by timestamp (string ISO asc)
    sorted_events = sorted(
        events,
        key=lambda e: (_event_timestamp_str(e) or "", e.get("message_id") or ""),
    )
    compact = []
    for e in sorted_events:
        compact.append(
            {
                "message_id": e.get("message_id"),
                "timestamp": _event_timestamp_str(e),
                "event_type": e.get("event_type"),
                "confidence": e.get("confidence"),
                "candidate_client": e.get("candidate_client"),
                "candidate_process": canonicalize_process(e.get("candidate_process")),
                "candidate_role": e.get("candidate_role"),
                "snippet": ((e.get("evidence") or {}).get("snippet")),
            }
        )
    return {
        "instance_key": instance_key,
        "thread_id": thread_id,
        "events": compact,
    }


def _most_frequent_non_null(events: List[Dict[str, Any]], key: str) -> Optional[str]:
    vals = [e.get(key) for e in events if e.get(key)]
    if not vals:
        return None
    return Counter(vals).most_common(1)[0][0]


@dataclass
class Stage3Result:
    instances: List[Dict[str, Any]]
    by_instance_timeline: Dict[str, List[Dict[str, Any]]]
    stats: Dict[str, Any]


def run_stage3(run_dir: Path, cfg: Dict[str, Any]) -> Stage3Result:
    stage3_cfg = cfg.get("stage3", {})
    input_cfg = stage3_cfg.get("input", {})
    output_cfg = stage3_cfg.get("output", {})
    clustering_cfg = stage3_cfg.get("clustering", {})
    evidence_cfg = stage3_cfg.get("evidence", {})
    pass2_cfg = stage3_cfg.get("pass2", {})

    events_path = run_dir / input_cfg.get("events_pass1", "events.pass1.jsonl")
    if not events_path.exists():
        raise FileNotFoundError(f"Missing events file: {events_path}")

    # Load and filter events
    min_conf = float(clustering_cfg.get("min_event_confidence", 0.30))
    raw_events: List[Dict[str, Any]] = [obj for obj in read_jsonl(events_path)]
    events = [e for e in raw_events if _event_filter(e, min_conf)]
    console.print(
        f"[cyan]Stage3[/cyan]: loaded {len(raw_events)} events, "
        f"filtered to {len(events)} with min_conf={min_conf}"
    )

    # Group by thread_id with fallback
    groups = _group_by_thread(events)
    console.print(f"[cyan]Stage3[/cyan]: formed {len(groups)} base thread groups")

    allow_split = bool(clustering_cfg.get("allow_split_by_process", True))
    instances: List[Dict[str, Any]] = []
    by_instance_timeline: Dict[str, List[Dict[str, Any]]] = {}

    # Initialize LLM client
    # Resolve model (supports ${ENV} pattern)
    model_cfg = pass2_cfg.get("model")
    if isinstance(model_cfg, str) and model_cfg.startswith("${") and model_cfg.endswith("}"):
        env_name = model_cfg[2:-1]
        pass2_model = os.getenv(env_name, "")
    else:
        pass2_model = model_cfg or os.getenv("OPENAI_MODEL", "")
    client = OpenAIClient(
        api_key_env=cfg.get("llm", {}).get("api_key_env", "OPENAI_API_KEY"),
        model=pass2_model,
        temperature=float(pass2_cfg.get("temperature", 0)),
        max_output_tokens=int(pass2_cfg.get("max_output_tokens", 900)),
        timeout_s=int(pass2_cfg.get("timeout_s", 90)),
        max_retries=int(pass2_cfg.get("max_retries", 3)),
        retry_backoff_s=float(cfg.get("llm", {}).get("retry_backoff_s", 2.0)),
    )

    # Load pass2 prompt
    prompt_path = Path("src/demo/llm/prompts/pass2_state_inference.md")
    prompt_template = prompt_path.read_text(encoding="utf-8")

    # Build cluster plan (so we can report total)
    cluster_plan: List[Tuple[str, Optional[str], str, List[Dict[str, Any]]]] = []
    for base_key, evs in groups.items():
        thread_id = evs[0].get("thread_id") if evs else None
        if allow_split:
            for suffix, ev_list in _maybe_split_by_process(evs):
                cluster_plan.append((base_key, thread_id, suffix, ev_list))
        else:
            cluster_plan.append((base_key, thread_id, "", evs))
    total_clusters = len(cluster_plan)
    console.print(
        f"[cyan]Stage3[/cyan]: starting {total_clusters} clusters, "
        f"model={pass2_model or 'unset'}, allow_split={allow_split}"
    )
    progress_every = 1

    processed = 0
    for base_key, thread_id, suffix, ev_list in cluster_plan:
        instance_key = _det_instance_key(base_key, suffix)
        # Build prompt input
        prompt_input = _build_prompt_input(instance_key, thread_id, ev_list)
        prompt_text = prompt_template.replace("{{INPUT_JSON}}", json.dumps(prompt_input, ensure_ascii=False))

        # Call LLM
        try:
            raw = client.chat(prompt_text)
            data = json.loads(raw)
        except LLMClientError:
            # Degrade gracefully: unknown state
            data = {
                "candidate_client": _most_frequent_non_null(ev_list, "candidate_client"),
                "candidate_process": _most_frequent_non_null(ev_list, "candidate_process"),
                "candidate_role": _most_frequent_non_null(ev_list, "candidate_role"),
                "status": "unknown",
                "step": None,
                "summary": "LLM error; defaulting to unknown state.",
                "last_updated_at": None,
                "open_questions": [],
                "confidence": 0.0,
                "evidence_message_ids": [],
            }
        except Exception:
            # Bad JSON from model; degrade
            data = {
                "candidate_client": _most_frequent_non_null(ev_list, "candidate_client"),
                "candidate_process": _most_frequent_non_null(ev_list, "candidate_process"),
                "candidate_role": _most_frequent_non_null(ev_list, "candidate_role"),
                "status": "unknown",
                "step": None,
                "summary": "Invalid model output; defaulting to unknown state.",
                "last_updated_at": None,
                "open_questions": [],
                "confidence": 0.0,
                "evidence_message_ids": [],
            }

        # Build instance object
        max_items = int(evidence_cfg.get("max_items_per_instance", 7))
        min_ev_conf = float(evidence_cfg.get("min_confidence", 0.30))
        ev_by_id = {e["message_id"]: e for e in ev_list}
        # Select evidence: include model picks first
        selected_ids: List[str] = []
        for mid in (data.get("evidence_message_ids") or []):
            if mid in ev_by_id and mid not in selected_ids:
                selected_ids.append(mid)
        # Fill remaining by confidence then recency
        remaining = [
            e for e in ev_list if e.get("message_id") not in selected_ids and float(e.get("confidence", 0.0)) >= min_ev_conf
        ]
        remaining.sort(key=lambda e: (float(e.get("confidence", 0.0)), e.get("timestamp") or ""), reverse=True)
        for e in remaining:
            if len(selected_ids) >= max_items:
                break
            selected_ids.append(e["message_id"])
        # Ensure at least 1
        if not selected_ids and ev_list:
            selected_ids = [ev_list[-1]["message_id"]]

        # Canonicalize candidate_process for output
        out_candidate_process = canonicalize_process(
            data.get("candidate_process") or _most_frequent_non_null(ev_list, "candidate_process")
        )
        instance = {
            "instance_key": instance_key,
            "thread_ids": [thread_id] if thread_id else None,
            "candidate_client": data.get("candidate_client") or _most_frequent_non_null(ev_list, "candidate_client"),
            "candidate_process": out_candidate_process,
            "candidate_role": data.get("candidate_role") or _most_frequent_non_null(ev_list, "candidate_role"),
            "state": {
                "status": data.get("status", "unknown"),
                "step": data.get("step"),
                "summary": data.get("summary"),
                "last_updated_at": None,  # will compute below from evidence timestamps
                "confidence": data.get("confidence", 0.0),
            },
            "evidence": [
                {
                    "message_id": mid,
                    "timestamp": _event_timestamp_str(ev_by_id[mid]),
                    "event_type": ev_by_id[mid].get("event_type"),
                    "confidence": ev_by_id[mid].get("confidence"),
                    "snippet": ((ev_by_id[mid].get("evidence") or {}).get("snippet")),
                }
                for mid in selected_ids
            ],
        }
        # Compute last_updated_at as max evidence timestamp
        ev_timestamps = [evi.get("timestamp") for evi in instance["evidence"] if evi.get("timestamp")]
        if ev_timestamps:
            instance["state"]["last_updated_at"] = max(ev_timestamps)
        instances.append(instance)

        # Timeline for this instance: all events, chronological
        timeline_items = sorted(
            [
                {
                    "message_id": e.get("message_id"),
                    "timestamp": _event_timestamp_str(e),
                    "event_type": e.get("event_type"),
                    "confidence": e.get("confidence"),
                    "snippet": ((e.get("evidence") or {}).get("snippet")),
                }
                for e in ev_list
            ],
            key=lambda x: (x.get("timestamp") or "", x.get("message_id") or ""),
        )
        by_instance_timeline[instance_key] = timeline_items

        processed += 1
        if processed % progress_every == 0 or processed == total_clusters:
            console.print(f"[cyan]Stage3[/cyan]: {processed}/{total_clusters} clusters processed")

    # Stats
    statuses = Counter((inst.get("state") or {}).get("status", "unknown") for inst in instances)
    mean_conf = 0.0
    if instances:
        mean_conf = sum((inst.get("state") or {}).get("confidence", 0.0) for inst in instances) / len(instances)
    stats = {
        "instances_by_status": dict(statuses),
        "mean_instance_confidence": round(mean_conf, 4),
    }
    console.print(
        f"[green]Stage3 complete[/green]: instances={len(instances)} "
        f"mean_conf={stats['mean_instance_confidence']}"
    )

    # Phase B post-processing (deterministic enrichment)
    try:
        process_catalog = load_process_catalog(Path("config/process_catalog.yml"))
    except Exception:
        process_catalog = None
    try:
        clients_catalog = load_clients_catalog(Path("config/clients.yml"))
    except Exception:
        clients_catalog = None
    try:
        roles_catalog = load_roles_catalog(Path("config/roles.yml"))
    except Exception:
        roles_catalog = None
    now = datetime.now(timezone.utc)
    enriched_instances, phase_b = enrich_instances(
        instances, process_catalog, clients_catalog, roles_catalog, now
    )

    # Write outputs
    out_instances = run_dir / output_cfg.get("instances", "instances.json")
    out_timeline = run_dir / output_cfg.get("timeline", "timeline.json")
    out_review = run_dir / output_cfg.get("review_template", "review_template.json")
    out_eval = run_dir / output_cfg.get("eval_report", "eval_report.json")

    write_json(out_instances, {"instances": enriched_instances})
    write_json(out_timeline, {"by_instance": by_instance_timeline})

    # Review template: one row per instance
    review_rows = []
    for inst in instances:
        review_rows.append(
            {
                "instance_key": inst["instance_key"],
                "predicted": inst["state"],
                "candidate_client": inst["candidate_client"],
                "candidate_process": inst["candidate_process"],
                "candidate_role": inst["candidate_role"],
                "evidence_message_ids": [e["message_id"] for e in inst["evidence"]],
                "human_label": None,
                "human_notes": None,
            }
        )
    write_json(out_review, {"rows": review_rows})

    # Eval report: if review.json exists compute coverage-style metrics only (placeholder)
    review_input_path = run_dir / stage3_cfg.get("eval", {}).get("review_filename", "review.json")
    if review_input_path.exists():
        try:
            review_data = json.loads(review_input_path.read_text(encoding="utf-8"))
            labels = [row.get("human_label") for row in (review_data.get("rows") or []) if row.get("human_label")]
            label_counts = Counter(labels)
            write_json(out_eval, {"labels": dict(label_counts), "instances": len(instances), "stats": stats})
        except Exception:
            write_json(out_eval, {"error": "Failed to read/parse review.json", "instances": len(instances), "stats": stats})
    else:
        write_json(out_eval, {"coverage": {"instances": len(instances)}, "stats": stats})

    # attach phase_b summary into stats for upstream run_meta inclusion
    stats["phase_b"] = phase_b
    return Stage3Result(instances=enriched_instances, by_instance_timeline=by_instance_timeline, stats=stats)
