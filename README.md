## Demo: Process Observability — Stage 0

A minimal demo repository that establishes a stable run contract and CLI scaffold. Stage 0 includes no data parsing and no LLM calls.

### What this provides

- `python3 -m demo.cli run` creates `runs/<run_id>/` and writes `run_meta.json`
- `python3 -m demo.cli eval` exists as a stub and prints helpful output
- Config and env conventions are in place

### Requirements

- Python >= 3.11

### Setup

You can use either `uv` or `pip`.

Using pip:

```bash
python3 -m pip install -e .
```

Using uv:

```bash
uv sync
```

### Stage 0 usage

Show CLI help:

```bash
python3 -m demo.cli --help
```

Create a run (will create a new run id and write a meta file):

```bash
python3 -m demo.cli run --input data/01_raw_messages.json
```

This creates:

- `runs/<run_id>/run_meta.json`

Run the eval stub:

```bash
python3 -m demo.cli eval --run-id <run_id>
```

### Config

Default `config.yml`:

```yaml
project:
  name: demo-process-observability
io:
  runs_dir: runs
  input_path: data/01_raw_messages.json
run:
  write_run_meta: true
eval:
  review_filename: review.json
  report_filename: eval_report.json
```

### Roadmap

- Stage 1 will produce `messages.normalized.jsonl`
- Stage 3.5 will implement review/eval
