"""
Pass4: Reconcile normalized instances with the celara dataset.

This pass takes instances.normalized.json (output of Pass3) and updates
the celara_sample_data.json by:
- Matching normalized instances to existing entries
- Updating matched entries with new status/phase
- Creating new entries for unmatched instances
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from rich.console import Console

from ..reconcile.matcher import find_matching_instance
from ..reconcile.transformer import create_new_instance, update_existing_instance
from ..utils.json_utils import write_json

console = Console()


@dataclass
class Pass4Result:
    total_processed: int
    matched_and_updated: int
    new_created: int
    skipped: int
    updates: List[Dict[str, Any]] = field(default_factory=list)
    new_instances: List[Dict[str, Any]] = field(default_factory=list)
    skipped_instances: List[Dict[str, Any]] = field(default_factory=list)


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_yaml(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def run_pass4(
    normalized_instances_path: Path,
    dataset_path: Path,
    process_def_path: Path,
    output_path: Path,
    config: Dict[str, Any],
) -> Pass4Result:
    """
    Run Pass4: reconcile normalized instances with celara dataset.

    Args:
        normalized_instances_path: Path to instances.normalized.json
        dataset_path: Path to celara_sample_data.json
        process_def_path: Path to process_definition.yml
        output_path: Path for output (can be same as dataset_path to overwrite)
        config: Configuration dict (pass4 section from config.yml)

    Returns:
        Pass4Result with statistics and details
    """
    pass4_cfg = config.get("pass4", {})

    min_confidence = float(pass4_cfg.get("min_confidence", 0.7))
    match_threshold = float(pass4_cfg.get("match_threshold", 0.8))
    create_new = bool(pass4_cfg.get("create_new_instances", True))
    update_existing = bool(pass4_cfg.get("update_existing", True))

    # Load inputs
    normalized_data = _load_json(normalized_instances_path)
    normalized_instances = normalized_data.get("instances", [])

    dataset = _load_json(dataset_path)
    existing_instances = dataset.get("instances", [])

    process_def = _load_yaml(process_def_path)

    console.print(
        f"[cyan]Pass4[/cyan]: starting ({len(normalized_instances)} normalized instances, "
        f"{len(existing_instances)} existing dataset entries)"
    )
    console.print(
        f"[cyan]Pass4[/cyan]: min_confidence={min_confidence}, match_threshold={match_threshold}"
    )

    # Track changes
    updates: List[Dict[str, Any]] = []
    new_instances: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    # Generate IDs for new instances
    today = datetime.utcnow().strftime("%Y%m%d")
    existing_ids = {i.get("instance_id") for i in existing_instances if i.get("instance_id")}
    next_num = 1

    progress_every = max(1, len(normalized_instances) // 10)

    for idx, norm in enumerate(normalized_instances):
        norm_step = norm.get("normalized_step", {})
        norm_confidence = float(norm_step.get("confidence", 0.0))

        # Skip low confidence
        if norm_confidence < min_confidence:
            skipped.append({
                "instance_key": norm.get("instance_key"),
                "reason": f"confidence {norm_confidence:.2f} < {min_confidence}",
            })
            continue

        # Skip unknown process
        if norm_step.get("process_id") == "unknown":
            skipped.append({
                "instance_key": norm.get("instance_key"),
                "reason": "process_id is unknown",
            })
            continue

        # Try to match existing
        match = find_matching_instance(norm, existing_instances, threshold=match_threshold)

        if match and update_existing:
            # Update existing instance
            updated = update_existing_instance(match, norm, process_def)
            updates.append({
                "instance_id": match.get("instance_id"),
                "instance_name": match.get("instance_name"),
                "before_phase": match.get("phase_id"),
                "after_phase": updated.get("phase_id"),
                "normalized_step": norm_step,
            })
            # Replace in list
            for i, inst in enumerate(existing_instances):
                if inst.get("instance_id") == match.get("instance_id"):
                    existing_instances[i] = updated
                    break

        elif not match and create_new:
            # Generate unique ID
            while f"{today}_{next_num:03d}" in existing_ids:
                next_num += 1
            new_id = f"{today}_{next_num:03d}"
            existing_ids.add(new_id)
            next_num += 1

            # Create new instance
            new_entry = create_new_instance(norm, process_def, new_id)
            new_instances.append({
                "instance_id": new_id,
                "instance_name": new_entry.get("instance_name"),
                "process_id": new_entry.get("process_id"),
                "phase_id": new_entry.get("phase_id"),
                "normalized_step": norm_step,
            })
            existing_instances.append(new_entry)

        else:
            # Skipped due to config (match found but update_existing=false, or no match and create_new=false)
            skipped.append({
                "instance_key": norm.get("instance_key"),
                "reason": "match found but update disabled" if match else "no match and create disabled",
            })

        # Progress
        processed = idx + 1
        if processed % progress_every == 0 or processed == len(normalized_instances):
            console.print(
                f"[cyan]Pass4[/cyan]: {processed}/{len(normalized_instances)} processed "
                f"(updated={len(updates)}, new={len(new_instances)}, skipped={len(skipped)})"
            )

    # Write output
    dataset["instances"] = existing_instances
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(output_path, dataset)

    console.print(
        f"[green]Pass4 complete[/green]: updated={len(updates)} new={len(new_instances)} "
        f"skipped={len(skipped)}"
    )
    console.print(f"[green]Output written to:[/green] {output_path}")

    return Pass4Result(
        total_processed=len(normalized_instances),
        matched_and_updated=len(updates),
        new_created=len(new_instances),
        skipped=len(skipped),
        updates=updates,
        new_instances=new_instances,
        skipped_instances=skipped,
    )


def print_reconciliation_report(result: Pass4Result, dry_run: bool = False) -> None:
    """Print a formatted reconciliation report."""
    console.print()
    console.print("=" * 60)
    console.print("              RECONCILIATION REPORT")
    if dry_run:
        console.print("                  [DRY RUN MODE]")
    console.print("=" * 60)
    console.print()

    console.print(f"Total processed:    {result.total_processed}")
    console.print(f"Matched & updated:  {result.matched_and_updated}")
    console.print(f"New created:        {result.new_created}")
    console.print(f"Skipped:            {result.skipped}")
    console.print()

    if result.updates:
        console.print("-" * 60)
        console.print(f"UPDATES ({len(result.updates)}):")
        console.print("-" * 60)
        for i, update in enumerate(result.updates[:10], 1):
            console.print(f"  [{i}] {update.get('instance_name')} ({update.get('instance_id')})")
            console.print(f"      Phase: {update.get('before_phase')} -> {update.get('after_phase')}")
            norm_step = update.get("normalized_step", {})
            console.print(f"      Step: {norm_step.get('step_id')} ({norm_step.get('step_status')})")
            console.print(f"      Confidence: {norm_step.get('confidence', 0):.2f}")
            console.print()
        if len(result.updates) > 10:
            console.print(f"  ... and {len(result.updates) - 10} more")
        console.print()

    if result.new_instances:
        console.print("-" * 60)
        console.print(f"NEW INSTANCES ({len(result.new_instances)}):")
        console.print("-" * 60)
        for i, new_inst in enumerate(result.new_instances[:10], 1):
            console.print(f"  [{i}] {new_inst.get('instance_name')}")
            console.print(f"      ID: {new_inst.get('instance_id')}")
            console.print(f"      Process: {new_inst.get('process_id')}")
            console.print(f"      Phase: {new_inst.get('phase_id')}")
            norm_step = new_inst.get("normalized_step", {})
            console.print(f"      Step: {norm_step.get('step_id')} ({norm_step.get('step_status')})")
            console.print(f"      Confidence: {norm_step.get('confidence', 0):.2f}")
            if norm_step.get("reasoning"):
                console.print(f"      Reasoning: {norm_step.get('reasoning')[:60]}...")
            console.print()
        if len(result.new_instances) > 10:
            console.print(f"  ... and {len(result.new_instances) - 10} more")
        console.print()

    if result.skipped_instances:
        console.print("-" * 60)
        console.print(f"SKIPPED ({len(result.skipped_instances)}):")
        console.print("-" * 60)
        for skip in result.skipped_instances[:5]:
            console.print(f"  - {skip.get('instance_key')}: {skip.get('reason')}")
        if len(result.skipped_instances) > 5:
            console.print(f"  ... and {len(result.skipped_instances) - 5} more")
        console.print()

    console.print("=" * 60)
