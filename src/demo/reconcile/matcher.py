"""
Matcher: Fuzzy matching logic for finding existing dataset instances.
"""
from __future__ import annotations

from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional

# Try to use rapidfuzz for better performance, fallback to difflib
try:
    from rapidfuzz import fuzz

    def _fuzzy_ratio(s1: str, s2: str) -> float:
        return fuzz.ratio(s1, s2) / 100.0

except ImportError:

    def _fuzzy_ratio(s1: str, s2: str) -> float:
        return SequenceMatcher(None, s1, s2).ratio()


def _normalize_string(s: Optional[str]) -> str:
    """Normalize a string for comparison."""
    if not s:
        return ""
    # Lowercase, strip, collapse whitespace
    return " ".join(s.strip().lower().split())


def _normalize_client_name(name: Optional[str]) -> str:
    """Normalize client company name for comparison."""
    if not name:
        return ""
    normalized = _normalize_string(name)
    # Remove common suffixes
    suffixes = ["inc", "inc.", "llc", "ltd", "ltd.", "corp", "corp.", "company", "co", "co."]
    words = normalized.split()
    if words and words[-1] in suffixes:
        words = words[:-1]
    return " ".join(words)


def find_matching_instance(
    normalized_instance: Dict[str, Any],
    dataset_instances: List[Dict[str, Any]],
    threshold: float = 0.8,
) -> Optional[Dict[str, Any]]:
    """
    Find an existing dataset entry that matches the normalized instance.

    Match criteria:
    1. process_id must match
    2. client_company fuzzy match >= threshold
    3. job_title/project_name fuzzy match >= threshold (if both available)

    Args:
        normalized_instance: Instance with normalized_step field
        dataset_instances: List of existing dataset entries
        threshold: Minimum similarity score (0-1) for matching

    Returns:
        Matching dataset instance or None
    """
    norm = normalized_instance
    norm_step = norm.get("normalized_step", {})

    # Get normalized values from the instance
    norm_process = norm_step.get("process_id")
    norm_client = _normalize_client_name(norm.get("candidate_client"))
    norm_role = _normalize_string(norm.get("candidate_role"))

    if not norm_client:
        # Can't match without client name
        return None

    best_match: Optional[Dict[str, Any]] = None
    best_score = 0.0

    for existing in dataset_instances:
        # Process must match
        existing_process = existing.get("process_id")
        if existing_process != norm_process:
            continue

        # Get existing client
        context = existing.get("context", {})
        existing_client = _normalize_client_name(context.get("client_company"))

        if not existing_client:
            continue

        # Client match
        client_score = _fuzzy_ratio(norm_client, existing_client)
        if client_score < threshold:
            continue

        # Role/project match (optional but improves precision)
        existing_role = _normalize_string(
            context.get("job_title") or context.get("project_name") or ""
        )

        role_score = 1.0  # Default to perfect if no role comparison possible
        if norm_role and existing_role:
            role_score = _fuzzy_ratio(norm_role, existing_role)
            if role_score < threshold:
                continue

        # Combined score (weighted average)
        combined_score = (client_score * 0.6) + (role_score * 0.4)

        if combined_score > best_score:
            best_score = combined_score
            best_match = existing

    return best_match


def find_all_matching_instances(
    normalized_instance: Dict[str, Any],
    dataset_instances: List[Dict[str, Any]],
    threshold: float = 0.8,
) -> List[Dict[str, Any]]:
    """
    Find all dataset entries that match the normalized instance.

    Useful for detecting potential duplicates.

    Args:
        normalized_instance: Instance with normalized_step field
        dataset_instances: List of existing dataset entries
        threshold: Minimum similarity score (0-1) for matching

    Returns:
        List of matching dataset instances (may be empty)
    """
    norm = normalized_instance
    norm_step = norm.get("normalized_step", {})

    norm_process = norm_step.get("process_id")
    norm_client = _normalize_client_name(norm.get("candidate_client"))
    norm_role = _normalize_string(norm.get("candidate_role"))

    if not norm_client:
        return []

    matches = []

    for existing in dataset_instances:
        existing_process = existing.get("process_id")
        if existing_process != norm_process:
            continue

        context = existing.get("context", {})
        existing_client = _normalize_client_name(context.get("client_company"))

        if not existing_client:
            continue

        client_score = _fuzzy_ratio(norm_client, existing_client)
        if client_score < threshold:
            continue

        existing_role = _normalize_string(
            context.get("job_title") or context.get("project_name") or ""
        )

        if norm_role and existing_role:
            role_score = _fuzzy_ratio(norm_role, existing_role)
            if role_score < threshold:
                continue

        matches.append(existing)

    return matches
