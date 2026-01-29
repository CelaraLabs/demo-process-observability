# Pipeline Overview

This document explains how the process observability pipeline works end‑to‑end, including data inputs, stage outputs, catalogs and canonicalization, reconciliation, and the Streamlit UI.

## 1) High‑level flow

The pipeline runs in stages and writes a run directory under `runs/<run_id>`:

1. **Stage 0 (Ingestion, optional)**: Collect raw messages from Gmail and Slack.
2. **Stage 1 (Normalization)**: Normalize raw messages into a consistent schema.
3. **Stage 2 (Pass 1 LLM extraction)**: Extract structured events from normalized messages.
4. **Stage 3 (Clustering + State inference)**: Cluster events into instances and infer current state.
5. **Phase B (Deterministic enrichment)**: Canonicalize fields, compute steps/health, and attach debug fields.
6. **Reconciliation**: Write a persistent workflow store and per‑run reports.
7. **Stage 4 (Dashboard)**: Streamlit UI for exploration and review.

Each run produces a snapshot of artifacts (e.g., `instances.json`, `timeline.json`, reports) and updates a persistent store (`data/workflow_store.json`).

---

## 2) Inputs and configuration

### 2.1 Global configuration

The main configuration is `config.yml`. It includes:

- `io` (runs directory, input/output paths)
- `pass1`, `stage3` (LLM settings and outputs)
- `catalog` (paths for the unified catalog)
- `reconciliation` (store paths, scope, match rules, report names)

Key catalog paths:

```yaml
catalog:
  workflow_definition_path: "config/workflow_definition.yaml"
  process_catalog_path: "config/process_catalog.yml"
  override_path: "config/workflow_aliases_override.yml"
```

### 2.2 Authoritative recruiting workflow

`config/workflow_definition.yaml` is **authoritative** for the recruiting workflow:

- The canonical process id is **always** `recruiting`
- Step IDs are the fine‑grained workflow definition step ids (e.g., `internal-kickoff`, `interview-scheduling`)

Non‑recruiting processes still come from `config/process_catalog.yml`.

---

## 3) Unified catalog (canonical process + steps)

The unified catalog is built at runtime and used by the pipeline for canonicalization and step matching.

### 3.1 Recruiting compilation

`src/demo/catalog/compiler.py`:

- Locates process `id: recruiting` in `workflow_definition.yaml`
- Flattens phase → step order into a single ordered list
- Seeds step aliases using:
  - step `id`
  - step `name`
  - step `short_name` (if present)
  - normalized variants (lowercase, hyphens/underscores → spaces)
- Seeds process aliases: `recruiting`, `recruitment`, `hiring` (alias only), `recruiting pipeline`, and the process name
- Applies optional overrides from `workflow_aliases_override.yml`

### 3.2 Merge with non‑recruiting processes

`src/demo/catalog/loader.py` merges:

- Compiled `recruiting` process
- All other processes from `config/process_catalog.yml`
- Skips any `hiring` or `recruiting` entries in the process catalog to avoid drift

### 3.3 Debug artifact

Each run writes:

```
runs/<run_id>/compiled_process_catalog.json
```

This file includes process ids, recruiting phases/steps, and alias counts for quick inspection.

---

## 4) Canonicalization rules

Canonicalization happens in Phase B (`stage3_postprocess.py`) using the unified catalog.

### 4.1 Fields canonicalized

Raw inputs are preserved and canonical versions are added:

- `candidate_process_raw` → `canonical_process`
- `candidate_client_raw` → `canonical_client`
- `candidate_role_raw` → `canonical_role`

The canonical process for hiring/recruiting signals is always **`recruiting`**.

### 4.2 Role alias matching

Role aliases are matched with **exact normalized equality**:

- lowercased
- trimmed
- whitespace collapsed

No fuzzy/substring matching is applied for roles.

### 4.3 Step matching

Steps are matched to canonical step IDs for the process:

1. **Exact match** on step id (normalized) → `match_type = "exact"`
2. **Exact match** on any step alias (normalized) → `match_type = "alias"`
3. **Fuzzy match** (substring, unique) → `match_type = "fuzzy"`
4. No match → `match_type = "none"`

The matched fields on each instance:

- `canonical_current_step_id`
- `canonical_current_step_match_type`
- `canonical_current_step_match_score`
- `canonical_current_step_matched_alias`

---

## 5) Stage details

### 5.1 Stage 0 — Ingestion (optional)

Entry: `demo ingest` or `demo run --ingest-config ...`

What it does:
- Connects to Gmail/Slack using `config/ingestion.yml`
- Pulls a time‑window of messages and writes a unified raw dataset
- Produces a manifest and stats summary for traceability

Outputs:
- `raw_messages.jsonl`
- `ingestion_manifest.json`
- `ingestion_stats.json`

### 5.2 Stage 1 — Normalization

Entry: `demo run`

What it does:
- Loads the raw dataset
- Normalizes each message into a consistent schema (timestamps, ids, thread ids, etc.)
- Sorts messages by timestamp (if enabled) and writes JSONL

Output:
- `runs/<run_id>/messages.normalized.jsonl`

### 5.3 Stage 2 — Pass 1 LLM extraction

Entry: `demo run` (if enabled)

What it does:
- Sends normalized messages to the Pass 1 prompt
- Extracts structured “events” (client/process/role signals and evidence)
- Writes successful events and errors separately

Outputs:
- `runs/<run_id>/events.pass1.jsonl`
- `runs/<run_id>/events.pass1.errors.jsonl`

### 5.4 Stage 3 — Clustering + state inference

Entry: `demo run` or `demo stage3 --run-id <id>`

What it does:
- Groups events by thread and optionally splits by process
- Runs Pass 2 to infer the current state and step for each instance
- Builds a per‑instance timeline of evidence

Outputs:
- `runs/<run_id>/instances.json`
- `runs/<run_id>/timeline.json`
- `runs/<run_id>/review_template.json`
- `runs/<run_id>/eval_report.json`

### 5.5 Phase B — Deterministic enrichment

Runs inside Stage 3:

What it does:
- Attaches raw vs canonical fields (`candidate_*_raw`, `canonical_*`)
- Computes step progress and health
- Derives canonical step match metadata for debugging
- Writes Phase B coverage into `run_meta.json`

Outputs (added to `instances.json` and `run_meta.json`):
- `steps_state`, `steps_total`, `steps_done`, `health`
- `canonical_current_step_id` and match metadata

---

## 6) Reconciliation

Reconciliation runs automatically after Stage 3 and writes a **persistent workflow store** plus per‑run reports.

### 6.1 Inputs

- `runs/<run_id>/instances.json`
- `runs/<run_id>/timeline.json` (optional for evidence fallback)
- `config/workflow_definition.yaml`

### 6.2 Outputs

Persistent:

- `data/workflow_store.json`

Per‑run:

- `runs/<run_id>/workflow_store.snapshot.json`
- `runs/<run_id>/coverage_report.json`
- `runs/<run_id>/reconciliation_report.json`
- `runs/<run_id>/mapping_drift_report.json`

### 6.3 Recruiting‑only scope

Reconciliation is scoped to `recruiting` by default:

- Non‑recruiting instances are excluded from the store
- Coverage is still computed globally and within recruiting

### 6.4 Matching and updates

Workflows are matched deterministically:

1. **Exact key** on `(canonical_client, canonical_role, canonical_process)`
2. **Fuzzy** match on display name if no exact match
3. **Create** new workflow if no match

On update:

- steps/phases are overwritten from the latest instance
- evidence ids are merged (set union)
- `observability.last_updated_at` updated if newer

### 6.5 How reconciliation works (step‑by‑step)

At a high level, reconciliation takes enriched instances and merges them into a persistent store:

1. **Load inputs and definitions**
   - Reads `instances.json` and optional `timeline.json`
   - Loads the workflow definition to build process → steps/phase maps
2. **Load and migrate the store**
   - Reads `data/workflow_store.json` if present
   - Migrates legacy `process_id: "hiring"` to `"recruiting"` in memory
3. **Per‑instance processing**
   - Computes evidence ids (prefer instance‑level ids, fallback to timeline ids)
   - Determines the current step using canonical step metadata and process definition
   - Infers ordered `steps` and `phases` based on current step (positional inference)
4. **Match or create workflow**
   - Exact match on canonical key if available
   - Fuzzy match on display name if needed
   - Otherwise create a new workflow with a deterministic `workflow_id`
5. **Update workflow fields**
   - Overwrite `process_id`, `phase_id`, `display_name`, `client`, `role`, `steps`, `phases`
   - Merge evidence ids and refresh observability metadata
6. **Write outputs**
   - Persist updated store to `data/workflow_store.json`
   - Write run snapshot + coverage/reconciliation/drift reports
   - Add reconciliation summary to `run_meta.json`

### 6.5 Coverage report fields

`coverage_report.json` includes:

- Global coverage: canonical process/client/role, step signal, evidence, health
- Recruiting funnel counts and recruiting‑scoped coverage
- Reconciliation match counts and steps/phase coverage

---

## 7) Workflow store

The workflow store is the UI‑facing, cross‑run state.

Each workflow includes:

- `workflow_id`
- `process_id` (always `recruiting` for recruiting workflows)
- `phase_id`
- `client`, `role`, `display_name`
- `steps` and `phases` arrays (optional)
- `observability` (health, confidence, evidence, reconciliation metadata)

Legacy `process_id: "hiring"` entries are migrated in memory to `recruiting` on load.

---

## 8) Streamlit dashboard

The dashboard supports:

- Portfolio view
- Process grid
- Instance detail
- Review & evaluation
- **Run Summary** (coverage + reconciliation + drift)
- **Workflow Store** viewer

Run Summary and Workflow Store use the reconciliation outputs and store to provide transparency for each run.

---

## 9) Where to change behavior

Common changes and where to apply them:

- **Add recruiting step aliases**: `config/workflow_aliases_override.yml`
- **Add non‑recruiting step aliases**: `config/process_catalog.yml`
- **Add role aliases**: `config/roles.yml`
- **Change scope**: `config.yml` → `reconciliation.scope`
- **Change store paths**: `config.yml` → `reconciliation.store`

---

## 10) Quick troubleshooting

- **Low canonical role coverage**: add aliases to `roles.yml` (exact normalized matches only).
- **Low canonical step coverage**: add step aliases via `workflow_aliases_override.yml`.
- **Workflow store empty**: check reconciliation scope and `canonical_process` values.
- **Inconsistent steps**: verify `workflow_definition.yaml` steps and `compiled_process_catalog.json`.
