from __future__ import annotations

from datetime import datetime, timezone


def new_run_id() -> str:
    """
    Generate a new run id: YYYYMMDD_HHMMSS (UTC), filesystem-safe.
    """
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
