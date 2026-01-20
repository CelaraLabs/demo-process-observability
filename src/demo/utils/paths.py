from __future__ import annotations

from pathlib import Path


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_run_dir(runs_dir: str | Path, run_id: str) -> Path:
    return Path(runs_dir) / run_id
