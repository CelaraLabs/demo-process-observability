"""
Pass3: Normalize free-form steps to structured step IDs using LLM.

This pass takes instances.json (output of Pass2/Stage3) and normalizes
the free-form state.step text to structured (process_id, phase_id, step_id)
tuples using the process_definition.yml as reference.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from rich.console import Console

from ..llm.client import LLMClientError, OpenAIClient
from ..llm.types import Pass3NormalizedStep
from ..utils.json_utils import write_json

console = Console()


@dataclass
class Pass3Result:
    total: int
    normalized: int
    skipped: int
    by_process: Dict[str, int]
    by_phase: Dict[str, int]


def _sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


def _safe_filename(text: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in text)


def _load_prompt(prompt_path: Path) -> str:
    return prompt_path.read_text(encoding="utf-8")


def _load_yaml(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _cache_path(cache_dir: Path, instance_key: str, prompt_hash: str, model: str) -> Path:
    key_hash = _sha1_text(instance_key)
    fname = f"{key_hash}__{prompt_hash}__{_safe_filename(model)}.json"
    return cache_dir / fname


def _try_load_cache(cache_path: Path) -> Optional[Dict[str, Any]]:
    if not cache_path.exists():
        return None
    try:
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        return data.get("parsed_result")
    except Exception:
        return None


def _write_cache(cache_path: Path, raw_output: str, parsed_result: Optional[Dict[str, Any]]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"raw_output": raw_output, "parsed_result": parsed_result}
    cache_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _normalize_company_name(company: str) -> str:
    """
    Normalize company names to handle case variations and abbreviations.
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


def _normalize_position_name(name: str) -> str:
    """
    Normalize position names to standard forms.
    - Backend / Back-end / BE -> Backend
    - Frontend / Front-end / FE -> Frontend
    - Fullstack / Full-stack / Full Stack / FS -> Fullstack
    """
    if not name:
        return name

    normalized = name.strip()

    # Map common variations to standard names
    position_mappings = {
        # Backend variations (including Engineer, Developer suffixes)
        r'\b(back[\s-]?end|BE)(\s+(engineer|developer|dev))?\b': 'Backend',
        # Frontend variations
        r'\b(front[\s-]?end|FE)(\s+(engineer|developer|dev|position))?\b': 'Frontend',
        # Fullstack variations
        r'\b(full[\s-]?stack|full\s+stack|FS)(\s+(engineer|developer|dev))?\b': 'Fullstack',
        # AI variations
        r'\b(AI\s+(engineer|developer)|AI\s+dev)\b': 'AI Engineer',
        # DevOps variations
        r'\b(dev[\s-]?ops)(\s+engineer)?\b': 'DevOps',
        # QA variations (including all suffixes)
        r'\b(QA|quality\s+assurance)(\s+(automation|engineer|role|new\s+hire|analyst))?\b': 'QA',
        # Developer without context
        r'\b^developer$\b': 'Developer',
    }

    for pattern, replacement in position_mappings.items():
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)

    return normalized


def _remove_duplicate_company_suffix(process_id: str, candidate_client: str) -> str:
    """
    Remove duplicate company name from process_id.
    Example: "Frontend - Altum - Altum" with candidate_client="Altum" -> "Frontend - Altum"
    """
    if not process_id or not candidate_client:
        return process_id

    # Split by common separators
    parts = re.split(r'\s*[-–—]\s*', process_id)

    # Remove duplicate occurrences of the company name (case-insensitive)
    seen_company = False
    cleaned_parts = []

    for part in parts:
        part_lower = part.strip().lower()
        company_lower = candidate_client.strip().lower()

        if part_lower == company_lower:
            if not seen_company:
                cleaned_parts.append(part.strip())
                seen_company = True
            # Skip duplicate company names
        else:
            cleaned_parts.append(part.strip())

    return ' - '.join(cleaned_parts)


def _normalize_process_id(process_id: str, candidate_client: str) -> str:
    """
    Normalize process_id by:
    1. Standardizing position names
    2. Removing duplicate company names
    """
    if not process_id:
        return process_id

    # First normalize position names
    normalized = _normalize_position_name(process_id)

    # Then remove duplicate company names
    normalized = _remove_duplicate_company_suffix(normalized, candidate_client)

    return normalized


def _build_input_payload(instance: Dict[str, Any]) -> Dict[str, Any]:
    """Build the input payload for the LLM prompt."""
    state = instance.get("state", {})
    return {
        "instance_key": instance.get("instance_key"),
        "candidate_client": instance.get("candidate_client"),
        "candidate_process": instance.get("candidate_process"),
        "candidate_role": instance.get("candidate_role"),
        "state": {
            "status": state.get("status"),
            "step": state.get("step"),
            "summary": state.get("summary"),
            "last_updated_at": state.get("last_updated_at"),
            "confidence": state.get("confidence"),
        },
    }


def run_pass3(
    instances_path: Path,
    process_def_path: Path,
    output_path: Path,
    config: Dict[str, Any],
) -> Pass3Result:
    """
    Run Pass3: normalize instance steps using LLM.

    Args:
        instances_path: Path to instances.json
        process_def_path: Path to process_definition.yml
        output_path: Path for output instances.normalized.json
        config: Configuration dict (pass3 section from config.yml)

    Returns:
        Pass3Result with statistics
    """
    # Load config
    llm_cfg = config.get("llm", {})
    pass3_cfg = config.get("pass3", {})

    # Resolve model (supports ${ENV} pattern)
    model_cfg = pass3_cfg.get("model") or llm_cfg.get("model")
    if isinstance(model_cfg, str) and model_cfg.startswith("${") and model_cfg.endswith("}"):
        env_name = model_cfg[2:-1]
        model = os.getenv(env_name, "")
    else:
        model = model_cfg or os.getenv("OPENAI_MODEL", "")

    api_key_env = llm_cfg.get("api_key_env", "OPENAI_API_KEY")
    temperature = float(pass3_cfg.get("temperature", llm_cfg.get("temperature", 0)))
    max_output_tokens = int(pass3_cfg.get("max_output_tokens", llm_cfg.get("max_output_tokens", 500)))
    timeout_s = int(pass3_cfg.get("timeout_s", llm_cfg.get("timeout_s", 60)))
    max_retries = int(pass3_cfg.get("max_retries", llm_cfg.get("max_retries", 3)))
    retry_backoff_s = float(llm_cfg.get("retry_backoff_s", 2.0))

    min_confidence = float(pass3_cfg.get("min_confidence", 0.5))
    cache_enabled = bool(pass3_cfg.get("cache", {}).get("enabled", True))
    cache_dir = Path(pass3_cfg.get("cache", {}).get("dir", "cache/pass3"))
    prompt_path = Path(pass3_cfg.get("prompt_path", "src/demo/llm/prompts/pass3_step_normalization.md"))

    # Load inputs
    instances_data = _load_json(instances_path)
    instances = instances_data.get("instances", [])
    process_def = _load_yaml(process_def_path)

    # Load prompt template
    prompt_template = _load_prompt(prompt_path)
    prompt_hash = _sha1_text(prompt_template)

    # Prepare process definition YAML string for prompt
    process_def_yaml = yaml.dump(process_def, default_flow_style=False, allow_unicode=True)

    # Initialize LLM client
    client = OpenAIClient(
        api_key_env=api_key_env,
        model=model,
        temperature=temperature,
        max_output_tokens=max_output_tokens,
        timeout_s=timeout_s,
        max_retries=max_retries,
        retry_backoff_s=retry_backoff_s,
    )

    console.print(
        f"[cyan]Pass3[/cyan]: starting ({len(instances)} instances), model={model}, "
        f"cache={'on' if cache_enabled else 'off'}"
    )

    normalized_instances: List[Dict[str, Any]] = []
    skipped = 0
    by_process: Dict[str, int] = {}
    by_phase: Dict[str, int] = {}

    progress_every = max(1, len(instances) // 20)

    for idx, instance in enumerate(instances):
        instance_key = instance.get("instance_key", f"instance_{idx}")
        state = instance.get("state", {})
        state_confidence = float(state.get("confidence", 0.0))

        # Skip low-confidence instances from Pass2
        if state_confidence < min_confidence:
            skipped += 1
            continue

        # Check cache
        cp = _cache_path(cache_dir, instance_key, prompt_hash, model)
        cached_result = None
        if cache_enabled:
            cached_result = _try_load_cache(cp)

        if cached_result is not None:
            # Use cached result (will be normalized below)
            normalized_step = cached_result
        else:
            # Build prompt
            input_payload = _build_input_payload(instance)
            input_json = json.dumps(input_payload, ensure_ascii=False, indent=2)

            prompt_text = prompt_template.replace(
                "{{PROCESS_DEFINITION_YAML}}", process_def_yaml
            ).replace(
                "{{INPUT_JSON}}", input_json
            )

            # Call LLM
            try:
                raw_output = client.chat(prompt_text)
                data = json.loads(raw_output)
                # Validate with schema
                validated = Pass3NormalizedStep.model_validate(data)
                normalized_step = validated.model_dump()

                if cache_enabled:
                    _write_cache(cp, raw_output, normalized_step)

            except LLMClientError as e:
                console.print(f"[yellow]Pass3[/yellow]: LLM error for {instance_key}: {e}")
                normalized_step = {
                    "process_id": "unknown",
                    "phase_id": "unknown",
                    "step_id": None,
                    "step_status": "in_progress",
                    "confidence": 0.0,
                    "reasoning": f"LLM error: {str(e)}",
                }
                if cache_enabled:
                    _write_cache(cp, "", normalized_step)

            except json.JSONDecodeError as e:
                console.print(f"[yellow]Pass3[/yellow]: JSON decode error for {instance_key}: {e}")
                normalized_step = {
                    "process_id": "unknown",
                    "phase_id": "unknown",
                    "step_id": None,
                    "step_status": "in_progress",
                    "confidence": 0.0,
                    "reasoning": f"JSON decode error: {str(e)}",
                }
                if cache_enabled:
                    _write_cache(cp, raw_output if 'raw_output' in dir() else "", normalized_step)

            except Exception as e:
                console.print(f"[yellow]Pass3[/yellow]: Validation error for {instance_key}: {e}")
                normalized_step = {
                    "process_id": "unknown",
                    "phase_id": "unknown",
                    "step_id": None,
                    "step_status": "in_progress",
                    "confidence": 0.0,
                    "reasoning": f"Validation error: {str(e)}",
                }
                if cache_enabled:
                    _write_cache(cp, "", normalized_step)

        # Apply post-processing normalization
        candidate_client = instance.get("candidate_client", "")
        candidate_role = instance.get("candidate_role", "")

        # Normalize company name in the instance data
        if candidate_client:
            normalized_client = _normalize_company_name(candidate_client)
            if normalized_client != candidate_client:
                instance["candidate_client"] = normalized_client
                candidate_client = normalized_client

        # Normalize role/position name in the instance data
        if candidate_role:
            normalized_role = _normalize_position_name(candidate_role)
            # Remove company name from role if present
            normalized_role = _remove_duplicate_company_suffix(normalized_role, candidate_client)
            if normalized_role != candidate_role:
                instance["candidate_role"] = normalized_role

        # Normalize process_id
        if normalized_step.get("process_id"):
            original_process_id = normalized_step["process_id"]
            normalized_process_id = _normalize_process_id(original_process_id, candidate_client)
            if normalized_process_id != original_process_id:
                normalized_step["process_id"] = normalized_process_id

        # Enrich instance with normalized step
        enriched = {**instance, "normalized_step": normalized_step}
        normalized_instances.append(enriched)

        # Track stats
        proc_id = normalized_step.get("process_id", "unknown")
        phase_id = normalized_step.get("phase_id", "unknown")
        by_process[proc_id] = by_process.get(proc_id, 0) + 1
        by_phase[phase_id] = by_phase.get(phase_id, 0) + 1

        # Progress
        processed = idx + 1
        if processed % progress_every == 0 or processed == len(instances):
            console.print(
                f"[cyan]Pass3[/cyan]: {processed}/{len(instances)} processed "
                f"(normalized={len(normalized_instances)}, skipped={skipped})"
            )

    # Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(output_path, {"instances": normalized_instances})

    console.print(
        f"[green]Pass3 complete[/green]: normalized={len(normalized_instances)} skipped={skipped}"
    )

    return Pass3Result(
        total=len(instances),
        normalized=len(normalized_instances),
        skipped=skipped,
        by_process=by_process,
        by_phase=by_phase,
    )
