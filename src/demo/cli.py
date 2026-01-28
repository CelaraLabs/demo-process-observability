from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict

from collections import Counter
from dotenv import load_dotenv
from rich.console import Console

from .config import ConfigError, load_config
from .io.load_raw import load_raw_dataset, extract_messages
from .io.normalize import normalize_messages
from .pipeline.pass1 import run_pass1
from .pipeline.pass2 import run_stage3
from .pipeline.reconciliation import run_reconciliation
from .ingestion.run import run_ingestion
from .ingestion.config import load_config as load_ingest_config
from .schemas.messages import NormalizedMessage
from .utils.json_utils import write_json, write_jsonl, read_jsonl
from .utils.paths import ensure_dir, get_run_dir
from .utils.run_id import new_run_id
from .utils.time import utc_now_iso

load_dotenv()  # automatically load variables from .env if present
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

    # Determine input path – optionally run ingestion first if ingest_config provided
    input_path: Path
    if getattr(args, "ingest_config", None):
        ingest_out_dir = Path(args.ingest_out or "data")
        console.print(f"[bold]Starting ingestion...[/bold] config={args.ingest_config} out_dir={ingest_out_dir}")
        raw_path = run_ingestion(Path(args.ingest_config), ingest_out_dir, auto_subdir=bool(getattr(args, "ingest_auto_subdir", False)))
        input_path = raw_path
    else:
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

    # Stage 1 pipeline
    try:
        dataset = load_raw_dataset(input_path)
        raw_messages = extract_messages(dataset)

        normalize_cfg = cfg.get("normalize", {})
        sort_by = bool(normalize_cfg.get("sort_by_timestamp", True))
        keep_raw = bool(normalize_cfg.get("keep_raw", True))
        allow_empty_text = bool(normalize_cfg.get("allow_empty_text", True))

        normalized_messages, empty_text_count = normalize_messages(
            raw_messages, sort_by_timestamp=sort_by, keep_raw=keep_raw
        )
        if not allow_empty_text and empty_text_count > 0:
            raise ValueError(f"Found {empty_text_count} messages with empty text but allow_empty_text=false")

        out_filename = cfg["io"]["output"]["normalized_messages"]
        out_path = run_dir / out_filename
        write_jsonl(out_path, (m.model_dump() for m in normalized_messages))

        by_source_counts = Counter(m.source for m in normalized_messages)
        thread_counts = Counter(m.thread_id for m in normalized_messages if m.thread_id)
        top_threads = [
            {"thread_id": thread_id, "message_count": count}
            for thread_id, count in thread_counts.most_common(10)
        ]

        # Update meta to stage 1 and overwrite file
        run_meta.update(
            {
                "stage": 1,
                "counts": {
                    "raw_messages": len(raw_messages),
                    "normalized_messages": len(normalized_messages),
                    "empty_text": empty_text_count,
                },
                "stats": {
                    "by_source": dict(by_source_counts),
                    "top_threads": top_threads,
                },
                "output_files": {
                    **run_meta.get("output_files", {}),
                    "normalized_messages": out_filename,
                },
                "notes": "Stage 1: loaded dataset, normalized messages, wrote JSONL.",
            }
        )
        write_json(meta_path, run_meta)

        console.print(f"[bold]Normalized messages:[/bold] {out_path}")
        console.print(f"[bold]Raw messages:[/bold] {len(raw_messages)}")
        console.print(f"[bold]Normalized:[/bold] {len(normalized_messages)}")
        console.print(f"[bold]Empty text:[/bold] {empty_text_count}")
        # Stage 2: Pass 1 LLM extraction (optional)
        pass1_cfg = cfg.get("pass1", {})
        if pass1_cfg.get("enabled", True):
            try:
                # Use in-memory normalized messages
                console.print("[bold]Starting Stage 2 (pass1)...[/bold]")
                pass1_result = run_pass1(normalized_messages, cfg, run_dir)
                # Update meta to stage 2
                run_meta.update(
                    {
                        "stage": 2,
                        "counts": {
                            **run_meta.get("counts", {}),
                            "pass1_success": pass1_result.success,
                            "pass1_errors": pass1_result.errors,
                        },
                        "stats": {
                            **run_meta.get("stats", {}),
                            "pass1_by_event_type": pass1_result.by_event_type,
                        },
                        "output_files": {
                            **run_meta.get("output_files", {}),
                            "events_pass1": pass1_cfg.get("output_events", "events.pass1.jsonl"),
                            "events_pass1_errors": pass1_cfg.get("output_errors", "events.pass1.errors.jsonl"),
                        },
                        "notes": "Stage 2: pass1 LLM extraction completed.",
                    }
                )
                write_json(meta_path, run_meta)
                console.print(
                    f"[bold]Pass1:[/bold] success={pass1_result.success} errors={pass1_result.errors}"
                )
            except Exception as e:
                console.print(f"[red]Stage 2 failed:[/red] {e}")
                # Keep meta at stage 1 if Stage 2 fails
                return 1

        # Stage 3: cluster + pass2 (optional)
        stage3_cfg = cfg.get("stage3", {})
        if stage3_cfg.get("enabled", True):
            try:
                console.print("[bold]Starting Stage 3...[/bold]")
                result3 = run_stage3(run_dir, cfg)
                # Update meta to stage 3
                run_meta.update(
                    {
                        "stage": 3,
                        "counts": {
                            **run_meta.get("counts", {}),
                            "instances": len(result3.instances),
                            "events_used": sum(len(v) for v in result3.by_instance_timeline.values()),
                            "threads": len({k for k in result3.by_instance_timeline.keys()}),
                        },
                        "stats": {
                            **run_meta.get("stats", {}),
                            "instances_by_status": result3.stats.get("instances_by_status"),
                            "mean_instance_confidence": result3.stats.get("mean_instance_confidence"),
                            "phase_b": result3.stats.get("phase_b"),
                        },
                        "output_files": {
                            **run_meta.get("output_files", {}),
                            "instances": stage3_cfg.get("output", {}).get("instances", "instances.json"),
                            "timeline": stage3_cfg.get("output", {}).get("timeline", "timeline.json"),
                            "review_template": stage3_cfg.get("output", {}).get("review_template", "review_template.json"),
                            "eval_report": stage3_cfg.get("output", {}).get("eval_report", "eval_report.json"),
                        },
                        "notes": "Stage 3: clustering + state inference completed.",
                    }
                )
                write_json(meta_path, run_meta)
                console.print(
                    f"[bold]Stage 3:[/bold] instances={len(result3.instances)} "
                    f"mean_conf={result3.stats.get('mean_instance_confidence')}"
                )
            except Exception as e:
                console.print(f"[red]Stage 3 failed:[/red] {e}")
                return 1

        # Reconciliation (required)
        try:
            console.print("[bold]Starting reconciliation...[/bold]")
            reconciliation_result = run_reconciliation(run_dir, cfg)
            # Update run_meta
            run_meta.update(
                {
                    "reconciliation": {
                        "counts": {
                            "workflows_written": reconciliation_result.workflows_written,
                            "match_counts": reconciliation_result.match_counts,
                        },
                        "coverage": reconciliation_result.coverage_report,
                        "output_files": {
                            **reconciliation_result.output_files,
                        },
                    }
                }
            )
            run_meta["output_files"] = {
                **run_meta.get("output_files", {}),
                **reconciliation_result.output_files,
            }
            write_json(meta_path, run_meta)
        except Exception as e:
            console.print(f"[red]Reconciliation failed:[/red] {e}")
            return 1

        return 0
    except SystemExit:
        raise
    except Exception as e:
        console.print(f"[red]Stage 1 failed:[/red] {e}")
        # Keep Stage 0 meta file as-is
        return 1


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
        description="Demo process observability CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # run
    p_run = sub.add_parser("run", help="Create run folder and execute Stage 1 + Stage 2")
    p_run.add_argument("--config", type=str, default="config.yml", help="Path to config.yml")
    p_run.add_argument("--input", type=str, help="Override input path")
    p_run.add_argument("--ingest-config", type=str, help="Path to ingestion config (to run Stage 0 ingestion first)")
    p_run.add_argument("--ingest-out", type=str, help="Output directory for ingestion (default: data/)")
    p_run.add_argument("--run-id", type=str, help="Provide a specific run id")
    p_run.add_argument("--ingest-auto-subdir", action="store_true", help="When using --ingest-config, create subfolder data/<dataset_id>_<start>_<end>")
    p_run.set_defaults(func=cmd_run)

    # ingest
    def cmd_ingest(args: argparse.Namespace) -> int:
        try:
            out_dir = Path(args.out or "data")
            raw_path = run_ingestion(Path(args.config), out_dir, auto_subdir=bool(args.auto_subdir))
            console.print(f"[bold green]Ingestion outputs written to:[/bold green] {raw_path.parent}")
            return 0
        except SystemExit:
            raise
        except Exception as e:
            console.print(f"[red]Ingestion failed:[/red] {e}")
            return 1

    p_ingest = sub.add_parser("ingest", help="Run Stage 0 ingestion (Gmail + Slack) to produce raw_messages.jsonl")
    p_ingest.add_argument("--config", type=str, required=True, help="Path to ingestion.yml")
    p_ingest.add_argument("--out", type=str, default="data", help="Output directory (default: data/)")
    p_ingest.add_argument("--auto-subdir", action="store_true", help="Create subfolder data/<dataset_id>_<start>_<end> to avoid overwrites")
    p_ingest.set_defaults(func=cmd_ingest)

    # eval (stub)
    p_eval = sub.add_parser("eval", help="Eval stub (Stage 3.5 will implement)")
    p_eval.add_argument("--config", type=str, default="config.yml", help="Path to config.yml")
    p_eval.add_argument("--run-id", type=str, required=True, help="Run ID to evaluate")
    p_eval.set_defaults(func=cmd_eval)

    # pass1 (rerun Stage 2 only)
    def cmd_pass1(args: argparse.Namespace) -> int:
        config_path = Path(args.config or "config.yml")
        cfg = _load_config_or_exit(config_path)
        runs_dir = Path(cfg["io"]["runs_dir"])
        run_dir = get_run_dir(runs_dir, args.run_id)

        meta_path = run_dir / "run_meta.json"
        if not meta_path.exists():
            console.print(f"[red]Missing run_meta.json at:[/red] {meta_path}")
            return 2

        # Load normalized messages JSONL
        norm_name = cfg["io"]["output"]["normalized_messages"]
        norm_path = run_dir / norm_name
        if not norm_path.exists():
            console.print(f"[red]Missing normalized messages at:[/red] {norm_path}")
            return 2
        try:
            messages = [
                NormalizedMessage.model_validate(obj) for obj in read_jsonl(norm_path)
            ]
        except Exception as e:
            console.print(f"[red]Failed to read normalized messages:[/red] {e}")
            return 1

        try:
            result = run_pass1(messages, cfg, run_dir)
            # Update run_meta
            try:
                from json import load
                with meta_path.open("r", encoding="utf-8") as f:
                    run_meta = load(f)
            except Exception:
                run_meta = {}
            pass1_cfg = cfg.get("pass1", {})
            run_meta.update(
                {
                    "stage": 2,
                    "counts": {
                        **run_meta.get("counts", {}),
                        "pass1_success": result.success,
                        "pass1_errors": result.errors,
                    },
                    "stats": {
                        **run_meta.get("stats", {}),
                        "pass1_by_event_type": result.by_event_type,
                    },
                    "output_files": {
                        **run_meta.get("output_files", {}),
                        "events_pass1": pass1_cfg.get("output_events", "events.pass1.jsonl"),
                        "events_pass1_errors": pass1_cfg.get("output_errors", "events.pass1.errors.jsonl"),
                    },
                    "notes": "Stage 2: pass1 LLM extraction completed.",
                }
            )
            write_json(meta_path, run_meta)
            console.print(f"[bold]Pass1:[/bold] success={result.success} errors={result.errors}")
            return 0
        except SystemExit:
            raise
        except Exception as e:
            console.print(f"[red]Pass1 failed:[/red] {e}")
            return 1

    p_pass1 = sub.add_parser("pass1", help="Run Stage 2 (pass1) for an existing run")
    p_pass1.add_argument("--config", type=str, default="config.yml", help="Path to config.yml")
    p_pass1.add_argument("--run-id", type=str, required=True, help="Run ID to process")
    p_pass1.set_defaults(func=cmd_pass1)

    # stage3 (rerun Stage 3 only)
    def cmd_stage3(args: argparse.Namespace) -> int:
        config_path = Path(args.config or "config.yml")
        cfg = _load_config_or_exit(config_path)
        runs_dir = Path(cfg["io"]["runs_dir"])
        run_dir = get_run_dir(runs_dir, args.run_id)
        try:
            result3 = run_stage3(run_dir, cfg)
            meta_path = run_dir / "run_meta.json"
            try:
                from json import load
                with meta_path.open("r", encoding="utf-8") as f:
                    run_meta = load(f)
            except Exception:
                run_meta = {}
            stage3_cfg = cfg.get("stage3", {})
            run_meta.update(
                {
                    "stage": 3,
                    "counts": {
                        **run_meta.get("counts", {}),
                        "instances": len(result3.instances),
                        "events_used": sum(len(v) for v in result3.by_instance_timeline.values()),
                        "threads": len({k for k in result3.by_instance_timeline.keys()}),
                    },
                    "stats": {
                        **run_meta.get("stats", {}),
                        "instances_by_status": result3.stats.get("instances_by_status"),
                        "mean_instance_confidence": result3.stats.get("mean_instance_confidence"),
                        "phase_b": result3.stats.get("phase_b"),
                    },
                    "output_files": {
                        **run_meta.get("output_files", {}),
                        "instances": stage3_cfg.get("output", {}).get("instances", "instances.json"),
                        "timeline": stage3_cfg.get("output", {}).get("timeline", "timeline.json"),
                        "review_template": stage3_cfg.get("output", {}).get("review_template", "review_template.json"),
                        "eval_report": stage3_cfg.get("output", {}).get("eval_report", "eval_report.json"),
                    },
                    "notes": "Stage 3: clustering + state inference completed.",
                }
            )
            write_json(meta_path, run_meta)
            console.print(
                f"[bold]Stage 3:[/bold] instances={len(result3.instances)} "
                f"mean_conf={result3.stats.get('mean_instance_confidence')}"
            )
            console.print("[bold]Starting reconciliation...[/bold]")
            reconciliation_result = run_reconciliation(run_dir, cfg)
            run_meta.update(
                {
                    "reconciliation": {
                        "counts": {
                            "workflows_written": reconciliation_result.workflows_written,
                            "match_counts": reconciliation_result.match_counts,
                        },
                        "coverage": reconciliation_result.coverage_report,
                        "output_files": {
                            **reconciliation_result.output_files,
                        },
                    }
                }
            )
            run_meta["output_files"] = {
                **run_meta.get("output_files", {}),
                **reconciliation_result.output_files,
            }
            write_json(meta_path, run_meta)
            return 0
        except SystemExit:
            raise
        except Exception as e:
            console.print(f"[red]Stage 3 failed:[/red] {e}")
            return 1

    p_stage3 = sub.add_parser("stage3", help="Run Stage 3 clustering + state inference")
    p_stage3.add_argument("--config", type=str, default="config.yml", help="Path to config.yml")
    p_stage3.add_argument("--run-id", type=str, required=True, help="Run ID to process")
    p_stage3.set_defaults(func=cmd_stage3)

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
