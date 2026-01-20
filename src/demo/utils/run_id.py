from __future__ import annotations

import random
import string
from datetime import datetime, timezone


def _rand4() -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(random.choice(alphabet) for _ in range(4))


def new_run_id() -> str:
    """
    Generate a new run id: YYYYMMDD_HHMMSS_<rand4>, filesystem-safe.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{ts}_{_rand4()}"
