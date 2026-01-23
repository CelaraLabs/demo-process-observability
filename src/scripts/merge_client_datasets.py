from __future__ import annotations

import argparse
import glob
import json
import os
from collections import Counter, OrderedDict
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _short_hash(text: str) -> str:
    import hashlib

    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:10]


def _derive_client_labels(path: Path) -> Tuple[str, str]:
    """
    From filename like 01_raw_messages_public_relay.json → ("public_relay", "Public Relay")
    """
    name = path.name
    # strip extension
    base = name.rsplit(".", 1)[0]
    if base.startswith("01_raw_messages_"):
        base = base[len("01_raw_messages_") :]
    slug = base.lower()
    human = base.replace("_", " ").strip().title()
    return slug, human


def _load_messages_from_file(path: Path) -> List[Dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    data = json.loads(text)
    if isinstance(data, dict):
        msgs = data.get("messages")
        if isinstance(msgs, list):
            return msgs
        raise ValueError(f"File {path} is an object but missing 'messages' list")
    elif isinstance(data, list):
        return data
    else:
        raise ValueError(f"Unsupported JSON top-level in {path}: must be object or array")


def _stable_key(msg: Dict[str, Any]) -> str:
    msg_id = msg.get("id")
    if isinstance(msg_id, str) and msg_id:
        return msg_id
    source = str(msg.get("source") or "unknown")
    ts = str(msg.get("ts") or "unknown_ts")
    text = str(msg.get("text") or "")
    return f"{source}:{ts}:{_short_hash(text[:200])}"


def _merge(inputs: List[Path], dataset_id: str) -> Tuple[Dict[str, Any], Counter]:
    dedup: Dict[str, Dict[str, Any]] = OrderedDict()
    total_input_msgs = 0
    client_counts: Counter = Counter()

    for input_path in inputs:
        slug, human = _derive_client_labels(input_path)
        msgs = _load_messages_from_file(input_path)
        total_input_msgs += len(msgs)
        for m in msgs:
            key = _stable_key(m)
            if key not in dedup:
                obj = deepcopy(m)
                inj = obj.get("ingestion") or {}
                # ensure ingestion object and fields
                inj.setdefault("dataset_id", dataset_id)
                inj.setdefault("matched_clients", [])
                inj.setdefault("files_seen_in", [])
                obj["ingestion"] = inj
                dedup[key] = obj
            inj = dedup[key]["ingestion"]
            # Append provenance if not already present
            if human not in inj["matched_clients"]:
                inj["matched_clients"].append(human)
                client_counts[human] += 1
            fname = input_path.name
            if fname not in inj["files_seen_in"]:
                inj["files_seen_in"].append(fname)

    # Stable order + optional ts sort
    items = list(dedup.values())
    # Check if most items have ts; if present, sort lexicographically by ts
    if any("ts" in it for it in items):
        try:
            items.sort(key=lambda it: (it.get("ts") or "", it.get("id") or ""))
        except Exception:
            pass

    unique = len(items)
    out = {
        "meta": {
            "dataset_id": dataset_id,
            "created_at": _iso_now(),
            "input_files": [p.name for p in inputs],
            "counts": {
                "input_files": len(inputs),
                "raw_messages_total": total_input_msgs,
                "unique_messages": unique,
                "duplicates_removed": max(0, total_input_msgs - unique),
            },
        },
        "messages": items,
    }
    return out, client_counts


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Merge per-client raw datasets into one JSON for the demo pipeline")
    parser.add_argument("--inputs", nargs="*", help="Input JSON files (each with top-level messages[] or an array)")
    parser.add_argument("--inputs-glob", type=str, help='Glob for inputs, e.g. "data/test/01_raw_messages_*.json"')
    parser.add_argument("--output", type=str, required=True, help="Output merged JSON path")
    parser.add_argument("--dataset-id", type=str, required=True, help="Dataset id to record in meta and ingestion fields")
    args = parser.parse_args(argv)

    inputs: List[Path] = []
    if args.inputs:
        inputs.extend(Path(p) for p in args.inputs)
    if args.inputs_glob:
        inputs.extend(Path(p) for p in glob.glob(args.inputs_glob))
    # Deduplicate paths while preserving order
    seen = set()
    ordered_inputs: List[Path] = []
    for p in inputs:
        if p not in seen:
            ordered_inputs.append(p)
            seen.add(p)
    if not ordered_inputs:
        print("No inputs provided. Use --inputs or --inputs-glob.")
        return 2

    out_obj, client_counts = _merge(ordered_inputs, args.dataset_id)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out_obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    counts = out_obj["meta"]["counts"]
    print(
        f"Merged {len(ordered_inputs)} file(s): total={counts['raw_messages_total']} "
        f"unique={counts['unique_messages']} dupes_removed={counts['duplicates_removed']}"
    )
    if client_counts:
        top = ", ".join(f"{name}:{client_counts[name]}" for name, _ in client_counts.most_common(10))
        print(f"Top matched_clients: {top}")
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
