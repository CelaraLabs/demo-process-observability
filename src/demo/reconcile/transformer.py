"""
Transformer: Convert normalized instances to celara dataset format.
"""
from __future__ import annotations

import copy
from datetime import datetime
from typing import Any, Dict, List, Optional


def _get_process_by_id(process_def: Dict[str, Any], process_id: str) -> Optional[Dict[str, Any]]:
    """Find a process definition by ID."""
    for proc in process_def.get("process", []):
        if proc.get("id") == process_id:
            return proc
    return None


def _get_phase_by_id(process: Dict[str, Any], phase_id: str) -> Optional[Dict[str, Any]]:
    """Find a phase definition by ID within a process."""
    for phase in process.get("phases", []):
        if phase.get("id") == phase_id:
            return phase
    return None


def _get_phase_order(process_def: Dict[str, Any], process_id: str) -> List[str]:
    """Get ordered list of phase IDs for a process."""
    process = _get_process_by_id(process_def, process_id)
    if not process:
        return []
    return [phase.get("id") for phase in process.get("phases", []) if phase.get("id")]


def _build_steps_from_phase(
    phase: Dict[str, Any],
    current_step_id: Optional[str],
    current_status: str,
    timestamp: Optional[str],
) -> List[Dict[str, Any]]:
    """
    Build steps array from phase definition.

    Steps before current_step_id are marked as completed.
    Current step gets the provided status.
    Steps after are marked as pending.
    """
    steps = []
    found_current = False

    for i, step_def in enumerate(phase.get("steps", []), 1):
        step_id = step_def.get("id")
        is_current = step_id == current_step_id

        if is_current:
            found_current = True
            status = current_status
            started_at = timestamp
            completed_at = timestamp if current_status == "completed" else None
        elif found_current:
            # After current step
            status = "pending"
            started_at = None
            completed_at = None
        else:
            # Before current step - assume completed
            status = "completed"
            started_at = None  # Unknown
            completed_at = None  # Unknown

        steps.append({
            "step_number": i,
            "step_id": step_id,
            "step_name": step_def.get("name"),
            "step_description": step_def.get("name"),
            "status": status,
            "completion_status": status if status in ("completed", "pending") else status,
            "timing": {
                "started_at": started_at,
                "completed_at": completed_at,
                "deadline": None,
            },
            "owner": {},
        })

    # If current step wasn't found, mark first step as current
    if not found_current and steps:
        steps[0]["status"] = current_status
        steps[0]["timing"]["started_at"] = timestamp

    return steps


def create_new_instance(
    normalized: Dict[str, Any],
    process_def: Dict[str, Any],
    instance_id: str,
) -> Dict[str, Any]:
    """
    Create a new celara dataset entry from a normalized instance.

    Args:
        normalized: Instance with normalized_step field
        process_def: Process definition YAML data
        instance_id: ID to assign to the new instance

    Returns:
        New dataset entry in celara format
    """
    norm_step = normalized.get("normalized_step", {})
    state = normalized.get("state", {})

    process_id = norm_step.get("process_id", "unknown")
    phase_id = norm_step.get("phase_id", "unknown")
    step_id = norm_step.get("step_id")
    step_status = norm_step.get("step_status", "in_progress")
    timestamp = state.get("last_updated_at")

    # Get process and phase definitions
    process = _get_process_by_id(process_def, process_id)
    phase = _get_phase_by_id(process, phase_id) if process else None

    # Build steps array
    if phase:
        steps = _build_steps_from_phase(phase, step_id, step_status, timestamp)
    else:
        # No phase found - create minimal step
        steps = [{
            "step_number": 1,
            "step_id": step_id or "unknown",
            "step_name": norm_step.get("reasoning", "Unknown step"),
            "step_description": norm_step.get("reasoning", "Unknown step"),
            "status": step_status,
            "completion_status": step_status,
            "timing": {
                "started_at": timestamp,
                "completed_at": None,
                "deadline": None,
            },
            "owner": {},
        }]

    # Build instance name
    candidate_role = normalized.get("candidate_role") or "Unknown Role"
    candidate_client = normalized.get("candidate_client") or "Unknown Client"
    instance_name = f"{candidate_role} - {candidate_client}"

    # Build context based on process type
    if process_id == "recruiting":
        context = {
            "job_title": normalized.get("candidate_role"),
            "client_company": normalized.get("candidate_client"),
            "department": "Engineering",  # Default
            "seniority_level": None,
            "location": "Remote",
            "employment_type": "Full-time",
            "urgency": "medium",
        }
    elif process_id == "project-management":
        instance_name = f"{normalized.get("candidate_role") or "Unknown Project"} - {candidate_client}"

        context = {
            "project_name": normalized.get("candidate_role"),
            "client_company": normalized.get("candidate_client"),
            "department": "Engineering",
            "project_type": None,
        }
    else:
        context = {
            "client_company": normalized.get("candidate_client"),
            "description": normalized.get("candidate_role"),
        }

    return {
        "instance_id": instance_id,
        "instance_name": instance_name,
        "process_id": process_id,
        "phase_id": phase_id,
        "context": context,
        "steps": steps,
        "metadata": {
            "source_instance_key": normalized.get("instance_key"),
            "source_thread_ids": normalized.get("thread_ids"),
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
    """
    Update an existing celara entry with new state from normalized instance.

    Args:
        existing: Existing dataset entry
        normalized: Instance with normalized_step field
        process_def: Process definition YAML data

    Returns:
        Updated dataset entry
    """
    norm_step = normalized.get("normalized_step", {})
    state = normalized.get("state", {})
    updated = copy.deepcopy(existing)

    new_phase_id = norm_step.get("phase_id")
    new_step_id = norm_step.get("step_id")
    new_step_status = norm_step.get("step_status", "in_progress")
    timestamp = state.get("last_updated_at")

    # Check if phase has progressed
    process_id = updated.get("process_id")
    phase_order = _get_phase_order(process_def, process_id)

    current_phase_id = updated.get("phase_id")
    current_phase_idx = phase_order.index(current_phase_id) if current_phase_id in phase_order else -1
    new_phase_idx = phase_order.index(new_phase_id) if new_phase_id in phase_order else -1

    if new_phase_idx > current_phase_idx:
        # Phase has progressed - update phase and potentially add new steps
        updated["phase_id"] = new_phase_id

        # Mark all current steps as completed
        for step in updated.get("steps", []):
            if step.get("status") != "completed":
                step["status"] = "completed"
                step["completion_status"] = "completed"

        # Add steps from new phase
        process = _get_process_by_id(process_def, process_id)
        new_phase = _get_phase_by_id(process, new_phase_id) if process else None
        if new_phase:
            new_steps = _build_steps_from_phase(new_phase, new_step_id, new_step_status, timestamp)
            # Renumber steps
            start_num = len(updated.get("steps", [])) + 1
            for i, step in enumerate(new_steps):
                step["step_number"] = start_num + i
            updated["steps"] = updated.get("steps", []) + new_steps

    elif new_phase_idx == current_phase_idx or new_phase_idx == -1:
        # Same phase - update step status
        step_found = False
        for step in updated.get("steps", []):
            if step.get("step_id") == new_step_id:
                step["status"] = new_step_status
                step["completion_status"] = new_step_status
                if not step["timing"].get("started_at"):
                    step["timing"]["started_at"] = timestamp
                if new_step_status == "completed":
                    step["timing"]["completed_at"] = timestamp
                step_found = True
                break

        # If step not found in current steps, it might be a new step
        if not step_found and new_step_id:
            updated["steps"] = updated.get("steps", []) + [{
                "step_number": len(updated.get("steps", [])) + 1,
                "step_id": new_step_id,
                "step_name": new_step_id,
                "step_description": norm_step.get("reasoning"),
                "status": new_step_status,
                "completion_status": new_step_status,
                "timing": {
                    "started_at": timestamp,
                    "completed_at": timestamp if new_step_status == "completed" else None,
                    "deadline": None,
                },
                "owner": {},
            }]

    # Update metadata
    updated.setdefault("metadata", {})
    updated["metadata"]["last_reconciled"] = datetime.utcnow().isoformat() + "Z"
    updated["metadata"]["latest_source_key"] = normalized.get("instance_key")
    updated["metadata"]["latest_normalization"] = {
        "phase_id": new_phase_id,
        "step_id": new_step_id,
        "step_status": new_step_status,
        "confidence": norm_step.get("confidence"),
        "reasoning": norm_step.get("reasoning"),
    }

    # Store recent evidence
    evidence = normalized.get("evidence", [])[:3]
    if evidence:
        updated["metadata"]["latest_evidence"] = evidence

    return updated
