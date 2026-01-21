from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone, date
from typing import Any, Dict, Iterable, List, Tuple

from .models import RawMessage


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _json_safe(obj: Any) -> Any:
    """
    Recursively convert objects to JSON-serializable forms.
    - date/datetime -> ISO strings
    - dict/list -> walk recursively
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    return obj


def build_manifest(
    dataset_id: str,
    start_date: str,
    end_date: str,
    gmail_mailboxes: List[str],
    slack_strategy: Dict[str, Any],
    items: List[RawMessage],
    rules_counter: Counter,
    cfg_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    counts_by_source = Counter(rm.source for rm in items)
    return {
        "dataset_id": dataset_id,
        "time_window": {"start": start_date, "end": end_date},
        "gmail_mailboxes": gmail_mailboxes,
        "slack_strategy": slack_strategy,
        "counts": {
            "total": len(items),
            "by_source": dict(counts_by_source),
        },
        "counts_by_rule": dict(rules_counter),
        "config_snapshot": _json_safe(cfg_snapshot),
        "created_at": _iso_now(),
    }


def build_stats(
    read_counts: Dict[str, int],
    kept_items: List[RawMessage],
    dropped_by_reason: Dict[str, int],
    per_channel_counts: Dict[str, int] | None = None,
) -> Dict[str, Any]:
    kept_by_source = Counter(rm.source for rm in kept_items)
    return {
        "read_counts": read_counts,
        "kept_counts": dict(kept_by_source),
        "dropped_by_reason": dropped_by_reason,
        "per_channel_counts": per_channel_counts or {},
    }
