## Demo: Process Observability — Stage 0

A minimal demo repository that establishes a stable run contract and CLI scaffold. Stage 0 includes no data parsing and no LLM calls.

### What this provides

- `uv run demo run` loads the dataset, normalizes messages (Stage 1), runs pass1 LLM extraction (Stage 2), writes outputs, and updates `run_meta.json`
- `uv run demo eval` exists as a stub and prints helpful output
- Config and env conventions are in place

### Requirements

- Python >= 3.11

### Setup

You can use `uv`.

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

Quickstart sanity check:

```bash
# Run console script via uv
uv run demo --help

# Or module form
uv run -m demo.cli --help
```

Note: Run commands from the repository root.

### Stage 1–3 usage

Show CLI help:

```bash
uv run demo --help
```

Create a run (loads dataset, normalizes messages, runs LLM extraction, writes outputs):

```bash
uv run demo run --input data/01_raw_messages.json
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

Run the eval stub:

```bash
uv run demo eval --run-id <run_id>
```

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

### Definition of Done (Stage 1)

```bash
uv sync
uv run demo run --input data/01_raw_messages.json
```

Confirm:
- `runs/<run_id>/messages.normalized.jsonl` exists and has the same number of lines as raw messages
- `runs/<run_id>/run_meta.json` has `stage: 1`, counts, `stats.by_source`, and `stats.top_threads`

### Definition of Done (Stage 2)

From a clean repo with OpenAI env vars set:
```bash
uv sync
uv run demo run --input data/01_raw_messages.json
```
Confirm:
- `runs/<run_id>/events.pass1.jsonl` exists
- `runs/<run_id>/events.pass1.errors.jsonl` exists (may be empty)
- `runs/<run_id>/run_meta.json` has `stage: 2`, `counts.pass1_success`, `counts.pass1_errors`, and `stats.pass1_by_event_type`

### Definition of Done (Stage 3)

After Stage 2:
```bash
uv run demo run --input data/01_raw_messages.json
# or rerun only Stage 3 for an existing run:
uv run demo stage3 --run-id <run_id>
```
Confirm:
- `runs/<run_id>/instances.json`, `timeline.json`, `review_template.json`, `eval_report.json` exist
- `runs/<run_id>/run_meta.json` has `stage: 3`, `counts.instances`, `stats.instances_by_status`, and `stats.mean_instance_confidence`
