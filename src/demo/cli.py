from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict

from rich.console import Console

from .config import ConfigError, load_config
from .utils.json_utils import write_json
from .utils.paths import ensure_dir, get_run_dir
from .utils.run_id import new_run_id
from .utils.time import utc_now_iso

console = Console()


def _load_config_or_exit(config_path: Path) -> Dict[str, Any]:
    try:
        return load_config(config_path)
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(2)
    except ConfigError as e:
        console.print(f"[red]Config error:[/red] {e}")
        sys.exit(2)
    except Exception as e:  # unexpected
        console.print(f"[red]Unexpected error loading config:[/red] {e}")
        sys.exit(1)


def cmd_run(args: argparse.Namespace) -> int:
    config_path = Path(args.config or "config.yml")
    cfg = _load_config_or_exit(config_path)

    # Determine run_id
    run_id = args.run_id or new_run_id()

    # Determine input path
    input_path = Path(args.input) if args.input else Path(cfg["io"]["input_path"])

    runs_dir = Path(cfg["io"]["runs_dir"])
    run_dir = get_run_dir(runs_dir, run_id)
    ensure_dir(run_dir)

    run_meta = {
        "run_id": run_id,
        "created_at": utc_now_iso(),
        "stage": 0,
        "config_path": str(config_path),
        "input_path": str(input_path),
        "output_files": {},
        "counts": {},
        "notes": "Stage 0 scaffold: no parsing/LLM yet.",
    }

    meta_path = run_dir / "run_meta.json"
    try:
        write_json(meta_path, run_meta)
    except Exception as e:
        console.print(f"[red]Failed to write run_meta.json:[/red] {e}")
        return 1

    console.print(f"[bold green]Run ID:[/bold green] {run_id}")
    console.print(f"[bold]Run directory:[/bold] {run_dir}")
    console.print(f"[bold]Input path:[/bold] {input_path}")
    console.print(f"[bold]Meta file:[/bold] {meta_path}")

    return 0


def cmd_eval(args: argparse.Namespace) -> int:
    config_path = Path(args.config or "config.yml")
    cfg = _load_config_or_exit(config_path)

    runs_dir = Path(cfg["io"]["runs_dir"])
    run_dir = get_run_dir(runs_dir, args.run_id)
    review_path = run_dir / "review.json"

    if not review_path.exists():
        console.print(
            "[yellow]No review file found.[/yellow] "
            "This will be produced in Stage 3.5. "
            f"Expected at: {review_path}"
        )
        return 0
    else:
        console.print(
            "[cyan]Eval not implemented yet (Stage 3.5).[/cyan] "
            f"Found review file at: {review_path}"
        )
        return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="demo",
        description="Demo process observability CLI (Stage 0).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # run
    p_run = sub.add_parser("run", help="Create run folder and write run_meta.json")
    p_run.add_argument("--config", type=str, default="config.yml", help="Path to config.yml")
    p_run.add_argument("--input", type=str, help="Override input path")
    p_run.add_argument("--run-id", type=str, help="Provide a specific run id")
    p_run.set_defaults(func=cmd_run)

    # eval (stub)
    p_eval = sub.add_parser("eval", help="Eval stub (Stage 3.5 will implement)")
    p_eval.add_argument("--config", type=str, default="config.yml", help="Path to config.yml")
    p_eval.add_argument("--run-id", type=str, required=True, help="Run ID to evaluate")
    p_eval.set_defaults(func=cmd_eval)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except SystemExit:
        raise
    except Exception as e:
        console.print(f"[red]Unexpected error:[/red] {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
