from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import yaml


@dataclass
class IngestionConfig:
    raw: Dict[str, Any]

    @property
    def dataset_id(self) -> str:
        return str(self.raw.get("dataset", {}).get("dataset_id"))

    @property
    def window_mode(self) -> str:
        return str(self.raw.get("dataset", {}).get("window", {}).get("mode", "relative"))

    @property
    def weeks(self) -> int:
        return int(self.raw.get("dataset", {}).get("window", {}).get("weeks", 12))

    @property
    def end_date(self) -> Optional[str]:
        return self.raw.get("dataset", {}).get("window", {}).get("end_date")

    @property
    def start_date(self) -> Optional[str]:
        return self.raw.get("dataset", {}).get("window", {}).get("start_date")

    @property
    def credentials_file(self) -> Path:
        return Path(str(self.raw.get("credentials", {}).get("file", "secrets/credentials.json")))

    def gmail_enabled(self) -> bool:
        return bool(self.raw.get("gmail", {}).get("enabled", True))

    def slack_enabled(self) -> bool:
        return bool(self.raw.get("slack", {}).get("enabled", True))


def load_config(path: Path) -> IngestionConfig:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return IngestionConfig(raw=data)


def compute_window(cfg: IngestionConfig) -> Tuple[datetime, datetime, str, str]:
    """
    Returns (start_dt, end_dt, start_date, end_date)
    start_date/end_date are YYYY-MM-DD strings (inclusive window boundary semantics for our usage).
    """
    now = datetime.now(timezone.utc)
    mode = cfg.window_mode
    if mode == "relative":
        end_dt = now
        start_dt = now - timedelta(weeks=cfg.weeks)
    elif mode == "absolute":
        end_str = cfg.end_date or now.strftime("%Y-%m-%d")
        # inclusive end for ingestion selection; convert to end of day
        end_dt = datetime.fromisoformat(end_str + "T23:59:59+00:00")
        start_str = cfg.start_date
        if not start_str:
            raise ValueError("absolute window requires dataset.window.start_date")
        start_dt = datetime.fromisoformat(start_str + "T00:00:00+00:00")
    else:
        raise ValueError(f"Unknown window mode: {mode}")
    start_date = start_dt.strftime("%Y-%m-%d")
    end_date = end_dt.strftime("%Y-%m-%d")
    return start_dt, end_dt, start_date, end_date
