## Demo: Process Observability

An end-to-end demo that establishes a stable run contract and CLI, runs the full pipeline with LLMs, and ships a Streamlit dashboard:
- Stage 0: ingestion (Gmail + Slack, 12-week window)
- Stage 1: normalize messages
- Stage 2: pass1 LLM event extraction
- Stage 3: clustering + state inference
- Stage 4: dashboard for browsing, review, and evaluation

### What this provides

- `uv run demo ingest --config config/ingestion.yml --out data` creates a dataset from Gmail/Slack (Stage 0)
- `uv run demo run` loads the dataset, normalizes messages (Stage 1), runs pass1 LLM extraction (Stage 2), writes outputs, and updates `run_meta.json`
- `uv run demo eval` is a stub (placeholder) and prints helpful output
- Config and env conventions are in place
- `uv run streamlit run src/demo/dashboard/app.py -- --runs-dir runs --run-id latest` launches the dashboard (Stage 4)

### Requirements

- Python >= 3.11

### Setup

Install `uv` if needed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# ensure ~/.local/bin is on PATH for the current shell session
export PATH="$HOME/.local/bin:$PATH"
```

Install dependencies and the project (editable):

```bash
uv sync
```

Note: Run commands from the repository root.

### Ingestion (Stage 0)

Build a dataset from Gmail + Slack using a 12-week window.

Run:

```bash
uv sync
uv run demo ingest --config config/ingestion.yml --out data --auto-subdir
```

Outputs (in `--out`, default `data/`):
- `raw_messages.jsonl` (JSON Lines)
- `ingestion_manifest.json`
- `ingestion_stats.json`

Minimal `config/ingestion.yml` example:

```yaml
dataset:
  dataset_id: demo_12w_2owners_v1
  window:
    mode: relative   # relative|absolute
    weeks: 12
credentials:
  file: secrets/credentials.json
gmail:
  enabled: true
  owner_mailboxes: ["hr_owner@company.com", "pm_owner@company.com"]
  query: { extra: "" }
  page_size: 200
slack:
  enabled: true
  include_channels: null
  exclude_channels: ["random", "social"]
  include_archived: false
  page_size: 200
filters:
  min_text_len: 20
  drop_if_sender_contains: ["no-reply", "noreply"]
```

Credentials (`secrets/credentials.json`) must include a Google service account (domain-wide delegation) and `slack_bot_token`.

Tip: You can run the full pipeline with ingestion first:

```bash
uv run demo run --ingest-config config/ingestion.yml --ingest-out data
```

### Pipeline usage (Stages 1–3)

Create a run (loads dataset, normalizes messages, runs LLM extraction, writes outputs):

```bash
uv run demo run --input data/01_raw_messages.json
```

Or, after an ingestion run:

```bash
uv run demo run --input data/raw_messages.jsonl
```

This creates (per run):

- `runs/<run_id>/messages.normalized.jsonl`
- `runs/<run_id>/events.pass1.jsonl`
- `runs/<run_id>/events.pass1.errors.jsonl`
- `runs/<run_id>/instances.json`
- `runs/<run_id>/timeline.json`
- `runs/<run_id>/review_template.json`
- `runs/<run_id>/eval_report.json`
- `runs/<run_id>/run_meta.json` (with `stage: 3`, counts, and stats)

Rerun Stage 2 only (helpful when iterating on prompts/types) for an existing run:

```bash
uv run demo pass1 --run-id <run_id>
```

Rerun Stage 3 only (clustering + state inference):

```bash
uv run demo stage3 --run-id <run_id>
```

### Config

Default `config.yml`:

```yaml
project:
  name: demo-process-observability
io:
  runs_dir: runs
  input_path: data/01_raw_messages.json
  output:
    normalized_messages: messages.normalized.jsonl
normalize:
  sort_by_timestamp: true
  keep_raw: true
  allow_empty_text: true
llm:
  provider: openai
  model: ${OPENAI_MODEL}
  api_key_env: OPENAI_API_KEY
  temperature: 0
  max_output_tokens: 500
  timeout_s: 60
  max_retries: 3
  retry_backoff_s: 2
pass1:
  enabled: true
  output_events: events.pass1.jsonl
  output_errors: events.pass1.errors.jsonl
  prompt_path: src/demo/llm/prompts/pass1_event_extraction.md
  concurrency: 8
  min_confidence_to_keep: 0.0
  cache:
    enabled: true
    dir: cache/pass1
run:
  write_run_meta: true
eval:
  review_filename: review.json
  report_filename: eval_report.json
stage3:
  enabled: true
  input:
    events_pass1: events.pass1.jsonl
    normalized_messages: messages.normalized.jsonl
  output:
    instances: instances.json
    timeline: timeline.json
    review_template: review_template.json
    eval_report: eval_report.json
  clustering:
    method: thread_first
    min_event_confidence: 0.30
    allow_split_by_process: true
    max_threads_to_process: null
  pass2:
    model: ${OPENAI_MODEL}
    temperature: 0
    max_output_tokens: 900
    timeout_s: 90
    max_retries: 3
  evidence:
    max_items_per_instance: 7
    min_confidence: 0.30
  eval:
    review_filename: review.json
    labels:
      allowed: ["correct", "partial", "incorrect", "unsure"]
```

### Environment variables

`.env` is auto-loaded when you run the CLI. You can either export vars or create `.env`:

```bash
# Option A: .env file (preferred)
cp -n .env.example .env
# then edit .env and set:
# OPENAI_API_KEY=sk-...
# OPENAI_MODEL=gpt-5.1

# Option B: export in shell
export OPENAI_API_KEY=sk-...
export OPENAI_MODEL=gpt-5.1
```

### Dashboard (Stage 4)

A lightweight Streamlit dashboard to browse process instances, inspect evidence, review predictions, and view evaluation.

Run it:

```bash
uv sync
uv run streamlit run src/demo/dashboard/app.py -- --runs-dir runs --run-id latest
```

Usage:
- Select a run in the sidebar (defaults to the latest).
- Overview: metrics and a filterable instances table; pick an instance to view.
- Instance Detail: see current state and a rich evidence timeline; jump to review.
- Review: label instances (`correct | partial | incorrect | unsure`) and save `review.json`. You can copy `review_template.json` to `review.json` if it does not exist.
- Evaluation: shows `eval_report.json` if present, otherwise a lightweight live summary. Optionally recompute a local report from `review.json` to `eval_report.local.json`.

Expected run artifacts per run directory:
- `instances.json`, `timeline.json`, `run_meta.json` (required)
- `messages.normalized.jsonl`, `review.json`, `review_template.json`, `eval_report.json` (optional)

### Catalogs and canonicalization (Phase A)

Phase A adds deterministic catalogs and pure canonicalization helpers (no pipeline output changes yet).

Config files:
- `config/process_catalog.yml`: process keys, display names, steps, aliases, health thresholds
- `config/clients.yml`: canonical client names and aliases
- `config/roles.yml`: canonical roles and aliases (must include “Other” and “Unknown”)

Programmatic usage:

```python
from pathlib import Path
from demo.catalog.loaders import (
  load_process_catalog, load_clients_catalog, load_roles_catalog
)
from demo.catalog.canonicalize import (
  canonicalize_process, canonicalize_client, canonicalize_role, match_step
)

pc = load_process_catalog(Path("config/process_catalog.yml"))
cc = load_clients_catalog(Path("config/clients.yml"))
rc = load_roles_catalog(Path("config/roles.yml"))

# Process
assert canonicalize_process("Recruiting", pc) == "hiring"

# Client
assert canonicalize_client("Altum.ai", cc) == "Altum"

# Role
assert canonicalize_role("ML Engineer", rc) == "AI Engineer"

# Step (within a process)
assert match_step("phone screen", "hiring", pc) == "screening"
```

Notes:
- Functions are deterministic and return `None` on ambiguity rather than guessing.
- Phase A is isolated; pipeline/dashboard integration may come in later phases.

Run tests:

```bash
uv run pytest -q
```

### Merge per-client datasets (one-off tool)

If you have multiple per-client raw JSON files (with overlapping messages) and want a single merged dataset for this demo:

```bash
# Using explicit inputs
uv run python scripts/merge_client_datasets.py \
  --inputs data/test/01_raw_messages_altum.json data/test/01_raw_messages_public_relay.json \
  --output data/test/01_raw_messages_2025_merged.json \
  --dataset-id prod_repo_2025_client_loop

# Or via glob
uv run python scripts/merge_client_datasets.py \
  --inputs-glob "data/test/01_raw_messages_*.json" \
  --output data/test/01_raw_messages_2025_merged.json \
  --dataset-id prod_repo_2025_client_loop
```

What it does:
- Loads multiple inputs (top-level `messages` array or object with `messages`)
- Deduplicates by `id` (fallback key if `id` missing)
- Adds provenance on each message under `ingestion`:
  - `matched_clients` (derived from filenames)
  - `files_seen_in`
- Outputs `{ "meta": {...}, "messages": [...] }`

Feed the merged file directly into the pipeline:

```bash
uv run demo run --input data/test/01_raw_messages_2025_merged.json
```
