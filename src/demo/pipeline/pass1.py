from __future__ import annotations

import hashlib
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from rich.console import Console

from ..llm.client import LLMClientError, OpenAIClient
from ..llm.types import Pass1Event
from ..schemas.messages import NormalizedMessage
from ..utils.json_utils import write_jsonl

console = Console()
_write_lock = threading.Lock()


@dataclass
class Pass1RunResult:
    total: int
    success: int
    errors: int
    by_event_type: Dict[str, int]


def _sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]


def _safe_filename(text: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in text)


def _load_prompt(prompt_path: Path) -> str:
    return prompt_path.read_text(encoding="utf-8")


def _build_input_payload(msg: NormalizedMessage) -> Dict:
    return {
        "message_id": msg.message_id,
        "timestamp": msg.timestamp,
        "source": msg.source,
        "sender": msg.sender,
        "recipients": msg.recipients,
        "subject": msg.subject,
        "text": msg.text,
        "thread_id": msg.thread_id,
    }


def _cache_paths(cache_dir: Path, message_id: str, prompt_hash: str, model: str) -> Path:
    fname = f"{_safe_filename(message_id)}__{prompt_hash}__{_safe_filename(model)}.json"
    return cache_dir / fname


def _try_load_cache(cache_path: Path) -> Tuple[str | None, Dict | None]:
    if not cache_path.exists():
        return None, None
    try:
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        return data.get("raw_output"), data.get("parsed_event")
    except Exception:
        return None, None


def _write_cache(cache_path: Path, raw_output: str, parsed_event: Dict | None) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"raw_output": raw_output, "parsed_event": parsed_event}
    cache_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def run_pass1(messages: List[NormalizedMessage], config: Dict, run_dir: Path) -> Pass1RunResult:
    # Config
    llm_cfg = config.get("llm", {})
    pass1_cfg = config.get("pass1", {})
    model_cfg = llm_cfg.get("model")
    if isinstance(model_cfg, str) and model_cfg.startswith("${") and model_cfg.endswith("}"):
        env_name = model_cfg[2:-1]
        model = os.getenv(env_name, "")
    else:
        model = model_cfg or os.getenv("OPENAI_MODEL", "")
    api_key_env = llm_cfg.get("api_key_env", "OPENAI_API_KEY")
    temperature = float(llm_cfg.get("temperature", 0.0))
    max_output_tokens = int(llm_cfg.get("max_output_tokens", 500))
    timeout_s = int(llm_cfg.get("timeout_s", 60))
    max_retries = int(llm_cfg.get("max_retries", 3))
    retry_backoff_s = float(llm_cfg.get("retry_backoff_s", 2.0))

    out_events_name = pass1_cfg.get("output_events", "events.pass1.jsonl")
    out_errors_name = pass1_cfg.get("output_errors", "events.pass1.errors.jsonl")
    prompt_path = Path(pass1_cfg.get("prompt_path", "src/demo/llm/prompts/pass1_event_extraction.md"))
    concurrency = int(pass1_cfg.get("concurrency", 8))
    cache_enabled = bool(pass1_cfg.get("cache", {}).get("enabled", True))
    cache_dir = Path(pass1_cfg.get("cache", {}).get("dir", "cache/pass1"))

    out_events_path = run_dir / out_events_name
    out_errors_path = run_dir / out_errors_name
    out_errors_path.parent.mkdir(parents=True, exist_ok=True)
    out_events_path.parent.mkdir(parents=True, exist_ok=True)

    prompt_template = _load_prompt(prompt_path)
    prompt_hash = _sha1_text(prompt_template)

    # Initialize client (will validate env key presence)
    client = OpenAIClient(
        api_key_env=api_key_env,
        model=model,
        temperature=temperature,
        max_output_tokens=max_output_tokens,
        timeout_s=timeout_s,
        max_retries=max_retries,
        retry_backoff_s=retry_backoff_s,
    )

    successes: List[Dict] = []
    errors: List[Dict] = []
    total = len(messages)
    console.print(
        f"[cyan]Pass1[/cyan]: starting ({total} messages), model={model}, "
        f"concurrency={concurrency}, cache={'on' if cache_enabled else 'off'}"
    )
    progress_every = max(1, total // 20)

    def process_one(msg: NormalizedMessage) -> Tuple[str, Dict | None, Dict | None]:
        # Returns (message_id, success_event_dict | None, error_record | None)
        payload = _build_input_payload(msg)
        payload_json = json.dumps(payload, ensure_ascii=False)
        prompt_text = prompt_template.replace("{{INPUT_JSON}}", payload_json)

        cache_path = _cache_paths(cache_dir, msg.message_id, prompt_hash, model)
        raw_output: str | None = None
        parsed_event: Dict | None = None

        if cache_enabled:
            cached_raw, cached_parsed = _try_load_cache(cache_path)
            if cached_raw is not None:
                raw_output = cached_raw
                parsed_event = cached_parsed

        if raw_output is None:
            try:
                raw_output = client.chat(prompt_text)
            except LLMClientError as e:
                err = {
                    "message_id": msg.message_id,
                    "error_type": "api_error",
                    "exception": str(e),
                    "raw_output": None,
                    "timestamp": msg.timestamp,
                    "source": msg.source,
                }
                if cache_enabled:
                    _write_cache(cache_path, raw_output="", parsed_event=None)
                return msg.message_id, None, err

        # Try to parse/validate; even if from cache re-validate to ensure schema evolves gracefully
        try:
            data = parsed_event if parsed_event is not None else json.loads(raw_output)
        except Exception as e:
            err = {
                "message_id": msg.message_id,
                "error_type": "json_decode",
                "exception": str(e),
                "raw_output": (raw_output[:2000] if raw_output else None),
                "timestamp": msg.timestamp,
                "source": msg.source,
            }
            if cache_enabled:
                _write_cache(cache_path, raw_output=raw_output or "", parsed_event=None)
            return msg.message_id, None, err

        try:
            evt = Pass1Event.model_validate(data)
        except Exception as e:
            err = {
                "message_id": msg.message_id,
                "error_type": "schema_validation",
                "exception": str(e),
                "raw_output": (raw_output[:2000] if isinstance(raw_output, str) else None),
                "timestamp": msg.timestamp,
                "source": msg.source,
            }
            if cache_enabled:
                _write_cache(cache_path, raw_output=raw_output if isinstance(raw_output, str) else "", parsed_event=None)
            return msg.message_id, None, err

        evt_dict = evt.model_dump()
        # Ensure thread_id present by propagating from normalized message if omitted
        if not evt_dict.get("thread_id") and msg.thread_id:
            evt_dict["thread_id"] = msg.thread_id
        if cache_enabled:
            _write_cache(cache_path, raw_output=raw_output if isinstance(raw_output, str) else json.dumps(raw_output), parsed_event=evt_dict)
        return msg.message_id, evt_dict, None

    processed = 0
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(process_one, m) for m in messages]
        for fut in as_completed(futures):
            mid, evt, err = fut.result()
            if evt is not None:
                successes.append(evt)
            elif err is not None:
                errors.append(err)
            processed += 1
            if processed % progress_every == 0 or processed == total:
                console.print(
                    f"[cyan]Pass1[/cyan]: {processed}/{total} processed "
                    f"(success={len(successes)}, errors={len(errors)})"
                )

    # Deterministic write order by message_id
    successes.sort(key=lambda d: d.get("message_id", ""))
    errors.sort(key=lambda d: d.get("message_id", ""))
    write_jsonl(out_events_path, successes)
    write_jsonl(out_errors_path, errors)

    by_event: Dict[str, int] = {}
    for evt in successes:
        et = evt.get("event_type") or "unknown"
        by_event[et] = by_event.get(et, 0) + 1

    console.print(
        f"[green]Pass1 complete[/green]: success={len(successes)} errors={len(errors)}"
    )

    return Pass1RunResult(
        total=len(messages),
        success=len(successes),
        errors=len(errors),
        by_event_type=by_event,
    )
