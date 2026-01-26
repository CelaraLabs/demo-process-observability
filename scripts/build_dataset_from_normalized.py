#!/usr/bin/env python3
"""
Build dataset from normalized instances (Pass3 output).

This script processes instances.normalized.json (output from Pass3) and builds
a clean dataset using the normalized step information. It processes instances
in chronological order, creating or updating process instances.

Instances are skipped if:
- Client is unknown or empty
- Process ID is unknown
- Step ID is unknown or None
- Role/project is unknown or empty

Usage:
    python scripts/build_dataset_from_normalized.py \
        --run-id 2025_5 \
        --output data/process_instances.json \
        --process-def config/process_definition.yml
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from rich.console import Console
from rich.progress import track

console = Console()


def load_json(path: Path) -> Dict[str, Any]:
    """Load JSON file."""
    return json.loads(path.read_text(encoding="utf-8"))


def load_yaml(path: Path) -> Dict[str, Any]:
    """Load YAML file."""
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def save_json(path: Path, data: Dict[str, Any]) -> None:
    """Save JSON file with pretty formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def normalize_string(s: Optional[str]) -> str:
    """Normalize a string for comparison."""
    if not s:
        return ""
    return " ".join(s.strip().lower().split())


def normalize_company_name_canonical(company: str) -> str:
    """
    Normalize company names to canonical forms (for matching and display).
    Maps abbreviations and case variations to standard names.
    """
    if not company:
        return company

    # Common company name normalizations (case-insensitive mapping)
    company_mappings = {
        'newton': 'Newton',
        'nw': 'Newton',
        'altum': 'Altum',
        'celara': 'CelaraLabs',
        'celaralabs': 'CelaraLabs',
        'chromatics': 'Chromatics',
        'chromatics ai': 'Chromatics',
        'chr': 'Chromatics',
        'qu': 'QU',
        'lrn': 'LRN',
        'podifi': 'Podifi',
        'public relay': 'Public Relay',
        'ideal prediction': 'Ideal Prediction',
        'newton labs': 'Newton Labs',
    }

    normalized = company.strip()
    normalized_lower = normalized.lower()

    # Check for exact match in mappings
    if normalized_lower in company_mappings:
        return company_mappings[normalized_lower]

    # Return with proper casing
    return normalized


def normalize_client_name(name: Optional[str]) -> str:
    """Normalize client company name for fuzzy matching (lowercase, no suffixes)."""
    if not name:
        return ""
    normalized = normalize_string(name)
    suffixes = ["inc", "inc.", "llc", "ltd", "ltd.", "corp", "corp.", "company", "co", "co."]
    words = normalized.split()
    if words and words[-1] in suffixes:
        words = words[:-1]
    return " ".join(words)


def fuzzy_match(s1: str, s2: str) -> float:
    """Simple fuzzy matching using sequence matching."""
    from difflib import SequenceMatcher
    return SequenceMatcher(None, s1, s2).ratio()


def find_matching_instance(
    normalized: Dict[str, Any],
    existing_instances: List[Dict[str, Any]],
    threshold: float = 0.8,
) -> Optional[Dict[str, Any]]:
    """
    Find an existing instance that matches the normalized instance.

    Match criteria:
    - process_id must match exactly
    - client_company fuzzy match >= threshold
    - role/project fuzzy match >= threshold (if both available)
    """
    norm_step = normalized.get("normalized_step", {})
    norm_process = norm_step.get("process_id")
    norm_client = normalize_client_name(normalized.get("candidate_client"))
    norm_role = normalize_string(normalized.get("candidate_role"))

    if not norm_client or not norm_process:
        return None

    best_match: Optional[Dict[str, Any]] = None
    best_score = 0.0

    for existing in existing_instances:
        # Process must match
        if existing.get("process_id") != norm_process:
            continue

        # Get existing client
        context = existing.get("context", {})
        existing_client = normalize_client_name(context.get("client_company"))

        if not existing_client:
            continue

        # Client match
        client_score = fuzzy_match(norm_client, existing_client)
        if client_score < threshold:
            continue

        # Role/project match
        existing_role = normalize_string(
            context.get("job_title") or context.get("project_name") or ""
        )

        role_score = 1.0
        if norm_role and existing_role:
            role_score = fuzzy_match(norm_role, existing_role)
            if role_score < threshold:
                continue

        # Combined score
        combined_score = (client_score * 0.6) + (role_score * 0.4)

        if combined_score > best_score:
            best_score = combined_score
            best_match = existing

    return best_match


def get_process_by_id(process_def: Dict[str, Any], process_id: str) -> Optional[Dict[str, Any]]:
    """Find a process definition by ID."""
    for proc in process_def.get("process", []):
        if proc.get("id") == process_id:
            return proc
    return None


def get_phase_by_id(process: Dict[str, Any], phase_id: str) -> Optional[Dict[str, Any]]:
    """Find a phase definition by ID within a process."""
    for phase in process.get("phases", []):
        if phase.get("id") == phase_id:
            return phase
    return None


def get_step_definition(phase: Dict[str, Any], step_id: str) -> Optional[Dict[str, Any]]:
    """Find a step definition within a phase."""
    for step in phase.get("steps", []):
        if step.get("id") == step_id:
            return step
    return None


def create_new_instance(
    normalized: Dict[str, Any],
    process_def: Dict[str, Any],
    instance_id: str,
) -> Dict[str, Any]:
    """Create a new instance from normalized data."""
    norm_step = normalized.get("normalized_step", {})
    state = normalized.get("state", {})

    # Extract normalized step information
    process_id = norm_step.get("process_id")
    phase_id = norm_step.get("phase_id")
    step_id = norm_step.get("step_id")
    step_status = norm_step.get("step_status", "in_progress")
    timestamp = state.get("last_updated_at")

    # Get process and phase definitions
    process = get_process_by_id(process_def, process_id)
    phase = get_phase_by_id(process, phase_id) if process else None
    step_def = get_step_definition(phase, step_id) if phase and step_id else None

    # Build instance name from candidate info
    candidate_role = normalized.get("candidate_role") or "Unknown Role"
    candidate_client = normalized.get("candidate_client") or "Unknown Client"

    # Build context based on process type
    if process_id == "recruiting":
        instance_name = f"{candidate_role} - {candidate_client}"
        context = {
            "job_title": normalized.get("candidate_role"),
            "client_company": normalized.get("candidate_client"),
            "department": "Engineering",
            "seniority_level": None,
            "location": "Remote",
            "employment_type": "Full-time",
            "urgency": "medium",
        }
    elif process_id == "project-management":
        instance_name = f"{candidate_role} - {candidate_client}"
        context = {
            "project_name": normalized.get("candidate_role"),
            "client_company": normalized.get("candidate_client"),
            "department": "Engineering",
            "project_type": None,
        }
    else:
        instance_name = f"{candidate_role} - {candidate_client}"
        context = {
            "client_company": normalized.get("candidate_client"),
            "description": normalized.get("candidate_role"),
        }

    # Create initial step from normalized data
    step = {
        "step_number": 1,
        "step_id": step_id,
        "step_name": step_def.get("name") if step_def else (step_id or "Unknown Step"),
        "step_description": step_def.get("name") if step_def else state.get("step", "Unknown step"),
        "status": step_status,
        "completion_status": step_status if step_status in ("completed", "pending") else step_status,
        "timing": {
            "started_at": timestamp,
            "completed_at": timestamp if step_status == "completed" else None,
            "deadline": None,
        },
        "owner": {},
    }

    return {
        "instance_id": instance_id,
        "instance_name": instance_name,
        "process_id": process_id,
        "phase_id": phase_id,
        "context": context,
        "steps": [step],
        "metadata": {
            "source_instance_key": normalized.get("instance_key"),
            "source_thread_ids": normalized.get("thread_ids", []),
            "parsed_confidence": state.get("confidence"),
            "normalization_confidence": norm_step.get("confidence"),
            "normalization_reasoning": norm_step.get("reasoning"),
            "last_reconciled": datetime.utcnow().isoformat() + "Z",
        },
    }


def update_existing_instance(
    existing: Dict[str, Any],
    normalized: Dict[str, Any],
    process_def: Dict[str, Any],
) -> Dict[str, Any]:
    """Update an existing instance with new step information from normalized data."""
    norm_step = normalized.get("normalized_step", {})
    state = normalized.get("state", {})

    # Extract normalized step information
    new_step_id = norm_step.get("step_id")
    new_phase_id = norm_step.get("phase_id")
    new_step_status = norm_step.get("step_status", "in_progress")
    timestamp = state.get("last_updated_at")

    # Update phase if it changed
    if new_phase_id and new_phase_id != existing.get("phase_id"):
        existing["phase_id"] = new_phase_id

    # Get phase and step definition
    process = get_process_by_id(process_def, existing.get("process_id"))
    phase = get_phase_by_id(process, existing.get("phase_id")) if process else None
    step_def = get_step_definition(phase, new_step_id) if phase and new_step_id else None

    # Find if step already exists
    existing_step = None
    for step in existing.get("steps", []):
        if step.get("step_id") == new_step_id:
            existing_step = step
            break

    if existing_step:
        # Update existing step status and timing
        existing_step["status"] = new_step_status
        existing_step["completion_status"] = new_step_status
        if not existing_step["timing"].get("started_at"):
            existing_step["timing"]["started_at"] = timestamp
        if new_step_status == "completed" and not existing_step["timing"].get("completed_at"):
            existing_step["timing"]["completed_at"] = timestamp
    else:
        # Add new step
        new_step = {
            "step_number": len(existing.get("steps", [])) + 1,
            "step_id": new_step_id,
            "step_name": step_def.get("name") if step_def else (new_step_id or "Unknown Step"),
            "step_description": step_def.get("name") if step_def else state.get("step", "Unknown step"),
            "status": new_step_status,
            "completion_status": new_step_status,
            "timing": {
                "started_at": timestamp,
                "completed_at": timestamp if new_step_status == "completed" else None,
                "deadline": None,
            },
            "owner": {},
        }
        existing["steps"].append(new_step)

    # Update metadata
    existing.setdefault("metadata", {})
    existing["metadata"]["last_reconciled"] = datetime.utcnow().isoformat() + "Z"
    existing["metadata"]["latest_source_key"] = normalized.get("instance_key")
    existing["metadata"]["latest_normalization_confidence"] = norm_step.get("confidence")

    return existing


def main():
    parser = argparse.ArgumentParser(
        description="Build dataset from normalized instances (Pass3 output)"
    )
    parser.add_argument("--run-id", required=True, help="Run ID to process")
    parser.add_argument("--output", required=True, help="Output dataset file path")
    parser.add_argument(
        "--process-def",
        default="config/process_definition.yml",
        help="Path to process definition YAML",
    )
    parser.add_argument(
        "--runs-dir",
        default="runs",
        help="Base directory for runs",
    )
    parser.add_argument(
        "--match-threshold",
        type=float,
        default=0.8,
        help="Fuzzy matching threshold (0-1)",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.0,
        help="Minimum normalization confidence to include (0-1)",
    )
    args = parser.parse_args()

    # Paths
    run_dir = Path(args.runs_dir) / args.run_id
    normalized_path = run_dir / "instances.normalized.json"
    output_path = Path(args.output)
    process_def_path = Path(args.process_def)

    # Check inputs exist
    if not normalized_path.exists():
        console.print(f"[red]Error:[/red] {normalized_path} does not exist")
        console.print("[yellow]Run pass3 first to generate normalized instances[/yellow]")
        return 1

    if not process_def_path.exists():
        console.print(f"[red]Error:[/red] {process_def_path} does not exist")
        return 1

    # Load inputs
    console.print(f"[cyan]Loading normalized instances from:[/cyan] {normalized_path}")
    normalized_data = load_json(normalized_path)
    normalized_instances = normalized_data.get("instances", [])

    console.print(f"[cyan]Loading process definition from:[/cyan] {process_def_path}")
    process_def = load_yaml(process_def_path)

    # Load or initialize output
    if output_path.exists():
        console.print(f"[cyan]Loading existing dataset from:[/cyan] {output_path}")
        dataset = load_json(output_path)
    else:
        console.print("[cyan]Initializing new dataset[/cyan]")
        dataset = {"instances": []}

    existing_instances = dataset.get("instances", [])

    # Sort normalized instances by timestamp (chronological order)
    console.print("[cyan]Sorting instances by timestamp...[/cyan]")
    normalized_instances.sort(
        key=lambda x: x.get("state", {}).get("last_updated_at") or "1970-01-01T00:00:00Z"
    )

    # Track statistics
    stats = {
        "total": len(normalized_instances),
        "skipped_unknown_client": 0,
        "skipped_unknown_process": 0,
        "skipped_unknown_step": 0,
        "skipped_no_role": 0,
        "skipped_low_confidence": 0,
        "created": 0,
        "updated": 0,
    }
    skipped_instances = []

    # Generate IDs for new instances
    today = datetime.utcnow().strftime("%Y%m%d")
    existing_ids = {i.get("instance_id") for i in existing_instances if i.get("instance_id")}
    next_num = 1

    console.print(f"\n[bold]Processing {stats['total']} instances in chronological order...[/bold]\n")

    # Process each normalized instance
    for idx, norm in enumerate(track(normalized_instances, description="Processing instances")):
        norm_step = norm.get("normalized_step", {})

        # Apply canonical normalization to company name
        candidate_client = norm.get("candidate_client")
        if candidate_client:
            candidate_client = normalize_company_name_canonical(candidate_client)
            norm["candidate_client"] = candidate_client

        candidate_role = norm.get("candidate_role")

        # Skip low confidence normalizations
        norm_confidence = norm_step.get("confidence", 0.0)
        if norm_confidence < args.min_confidence:
            stats["skipped_low_confidence"] += 1
            skipped_instances.append({
                "instance_key": norm.get("instance_key"),
                "reason": f"low confidence ({norm_confidence:.2f})",
                "timestamp": norm.get("state", {}).get("last_updated_at"),
            })
            continue

        # Skip if client is unknown or empty
        if not candidate_client or candidate_client.lower() in ("unknown", "unknown client", ""):
            stats["skipped_unknown_client"] += 1
            skipped_instances.append({
                "instance_key": norm.get("instance_key"),
                "reason": "unknown client",
                "timestamp": norm.get("state", {}).get("last_updated_at"),
            })
            continue

        # Skip if process is unknown
        process_id = norm_step.get("process_id") or ""
        if not process_id or process_id.lower() == "unknown":
            stats["skipped_unknown_process"] += 1
            skipped_instances.append({
                "instance_key": norm.get("instance_key"),
                "reason": "unknown process",
                "timestamp": norm.get("state", {}).get("last_updated_at"),
            })
            continue

        # Skip if step is None or unknown
        step_id = norm_step.get("step_id")
        if step_id is None or (isinstance(step_id, str) and step_id.lower() == "unknown"):
            stats["skipped_unknown_step"] += 1
            skipped_instances.append({
                "instance_key": norm.get("instance_key"),
                "reason": "unknown or null step",
                "timestamp": norm.get("state", {}).get("last_updated_at"),
            })
            continue

        # Skip if no role/project identified
        if not candidate_role or candidate_role.lower() in ("unknown", "unknown role", ""):
            stats["skipped_no_role"] += 1
            skipped_instances.append({
                "instance_key": norm.get("instance_key"),
                "reason": "no role/project identified",
                "timestamp": norm.get("state", {}).get("last_updated_at"),
            })
            continue

        # Try to find matching existing instance
        match = find_matching_instance(norm, existing_instances, threshold=args.match_threshold)

        if match:
            # Update existing instance with new step info
            update_existing_instance(match, norm, process_def)
            stats["updated"] += 1
        else:
            # Create new instance
            while f"{today}_{next_num:03d}" in existing_ids:
                next_num += 1
            new_id = f"{today}_{next_num:03d}"
            existing_ids.add(new_id)
            next_num += 1

            new_instance = create_new_instance(norm, process_def, new_id)
            existing_instances.append(new_instance)
            stats["created"] += 1

    # Save final dataset
    dataset["instances"] = existing_instances
    save_json(output_path, dataset)

    # Print summary
    console.print("\n" + "=" * 70)
    console.print("                    BUILD SUMMARY")
    console.print("=" * 70 + "\n")
    console.print(f"Total processed:                {stats['total']}")
    total_skipped = sum([
        stats['skipped_unknown_client'],
        stats['skipped_unknown_process'],
        stats['skipped_unknown_step'],
        stats['skipped_no_role'],
        stats['skipped_low_confidence'],
    ])
    console.print(f"\n[yellow]Skipped instances:[/yellow]")
    console.print(f"  Unknown client:               {stats['skipped_unknown_client']}")
    console.print(f"  Unknown process:              {stats['skipped_unknown_process']}")
    console.print(f"  Unknown step:                 {stats['skipped_unknown_step']}")
    console.print(f"  No role/project:              {stats['skipped_no_role']}")
    console.print(f"  Low confidence:               {stats['skipped_low_confidence']}")
    console.print(f"  [bold]Total skipped:            {total_skipped}[/bold]")
    console.print(f"\n[green]Processed successfully:[/green]")
    console.print(f"  New instances created:        {stats['created']}")
    console.print(f"  Existing instances updated:   {stats['updated']}")
    console.print(f"  [bold]Total in dataset:         {len(existing_instances)}[/bold]")
    console.print(f"\n[green]Dataset saved to:[/green] {output_path}")

    if skipped_instances:
        console.print(f"\n[yellow]Sample of skipped instances ({len(skipped_instances)} total):[/yellow]")
        for skip in skipped_instances[:10]:
            console.print(f"  - {skip['instance_key']}: {skip['reason']} @ {skip['timestamp']}")
        if len(skipped_instances) > 10:
            console.print(f"  ... and {len(skipped_instances) - 10} more")

    console.print("\n" + "=" * 70 + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
