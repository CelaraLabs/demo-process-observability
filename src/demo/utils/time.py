from __future__ import annotations

from datetime import datetime, timezone


def utc_now_iso() -> str:
    """
    Return current UTC time in ISO 8601 format with seconds precision.
    Example: 2024-01-01T00:00:00+00:00
    """
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
