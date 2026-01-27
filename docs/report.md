# Report: Observability Pipeline vs Workflow Store Pipeline

## Executive summary

We currently have two different product archetypes being mixed:

1. **Observability pipeline (current working baseline)**
   Outputs: *“best current understanding + evidence”* (status/health/steps grid inferred), scalable and debuggable.

2. **Workflow store pipeline (what the current dashboard schema implies)**
   Outputs: *“canonical process state machine with steps/phase/timings/deadlines”* persisted over time.

The main issue is that we’re trying to produce (2) from a system designed for (1). The result is glue stages that (a) add fragility, (b) reduce coverage, and (c) hide signal we already extracted.

The recommendation is to run **two tracks**:

* **Baseline track (observability):** keep it clean, add deterministic reconciliation + exporter to dashboard schema.
* **Experimental track (workflow store):** if we truly want workflow-store semantics, design it explicitly as an event-based + stateful pipeline from scratch, and measure it against the baseline.

---

## 1) Feasibility of matching the expected UI today

The “workflow store” output (`celara_sample_dataset.json`) expects fields that are not directly observable from raw Slack/Gmail without heavier extraction and persistence logic:

### What the workflow store expects (implied requirements)

* A stable taxonomy of `process_id / phase_id / step_id`
* A full ordered `steps[]` array with per-step:

  * `status` / `completion_status`
  * `timing.started_at / completed_at / deadline`
* Phase progression rules
* Potentially non-monotonic progress (“later step done while prior not done”)
* Human-correctable persistence (canonical store updated over time)

### What our observability pipeline produces reliably

* Stable instance candidates per thread/segment
* Current `state` (status/summary/last_updated/confidence)
* Evidence list (message_id + timestamps + snippets)
* Canonicalization via catalogs (client/process/role) and a step grid that is **inferred** or **unknown**
* Health based on recency / SLA rules

### The gap

The biggest gap is **per-step grounding and timing**:

* We can infer step ordering, but we usually *cannot* reliably produce:

  * exact “step X started at time T”
  * exact “step X completed at time T”
  * deadlines per step
* Without explicit step-completion events + persistence, the workflow store fields become either empty or fabricated.

---

## 2) Observability vs Workflow: the core difference

### Observability system

**Goal:** “what’s going on, and why do we think so?”
Primary outputs:

* A row per process-ish thing (instance)
* Current status + health
* Evidence drill-down
* Steps grid as *helpful approximation* (often inferred)

Key property: **truth is evidence-backed; unknown stays unknown**.

### Workflow system

**Goal:** “what is the canonical state of the workflow engine?”
Primary outputs:

* Canonical persistent instance store (like CRM/workflow tool)
* State machine progression (phase/step IDs)
* Step-by-step lifecycle, deadlines, escalations
* History of transitions

Key property: **truth is the state machine**; evidence is used to update it.

---

## 3) Why converting observability → workflow is hard

Trying to “convert” at the end (post-hoc) is difficult because a workflow store wants the pipeline to produce **events**, not just summaries.

### Observability-style output

* “Current step: interview scheduling / candidate response pending”
* “Last updated: Oct 13”
* Evidence list includes multiple messages with different event types

### Workflow-style store needs

* `step_started(interview-scheduling, t1, msg_id)`
* `step_blocked(interview-scheduling, t2, msg_id)`
* `step_completed(client-profile-feedback, t0, msg_id)`
* deadlines derived from SLA policy anchored to step entry time
* persistent instance updated incrementally across runs

If we don’t extract events with explicit evidence links, we can’t fill:

* per-step timings
* skipped steps
* monotonicity exceptions
* proper phase transitions

So the “conversion” ends up either:

* guessing (which makes it untrustworthy), or
* leaving fields blank (which makes dashboard blank).

---

## 4) Quality of `process_definition.yml` and `celara_sample_dataset.json` for development

### `process_definition.yml`

It’s intended as a workflow ontology, but in its current form it hurts downstream quality because:

* IDs and types are inconsistent / not strongly machine-validated (risk of drift)
* It mixes “retrieval hints” and “taxonomy truth”
* It doesn’t cover the full breadth of processes currently extracted
* As it grows, embedding the whole file into a prompt worsens quality/cost

Net effect: it’s hard for an LLM mapping step to achieve high coverage reliably, and downstream reconcile stages end up skipping “unknown”.

### `celara_sample_dataset.json`

As a UI mock it’s fine; as a canonical store schema it’s risky because:

* It mixes **canonical facts** with **derived analytics** (durations, overdue hours, etc.)
* It implies per-step timing certainty that we don’t have yet
* It implicitly enforces phase/step ontology as the “truth”, which isn’t aligned with the observability pipeline without additional extraction.

Net effect: if the UI assumes these fields exist, we get the “blank dashboard” failure mode.

---

## 5) Assessment of Pass 3

In our baseline, we already extract and preserve rich signal:

* evidence messages, timestamps, event-like types
* deterministic canonicalization (client/process/role)
* step grid derived from catalogs (with clear “unknown/inferred” semantics)

**Pass 3** (as implemented) attempts to map a free-text step label to a workflow ontology via an LLM. The issues:

1. **Redundant normalization layer**

   * We already canonicalized process/step concepts deterministically via catalogs.
   * Pass 3 introduces a second “truth” unless carefully bounded.

2. **Coverage collapse**

   * If Pass 3 yields `unknown` for many instances, and Pass 4 skips unknown, we lose rows.
   * This matches what we observed: “dashboard blank”.

3. **Signal compression**

   * Many implementations pass only `state.step`/`summary` into the mapping.
   * That discards rich evidence structure (multiple events, timestamps, event_types).
   * Even if run after our pipeline, Pass 3 may behave as a thin classifier, not a grounded reasoner.

4. **Scalability**

   * **Not scalable:** Pass 3 aggregates all messages into a single prompt, quickly hitting context limits as threads grow (months of Gmail/Slack), forcing truncation or omission of older but still relevant evidence.

   * **Quality degrades with size:** Long, noisy prompts dilute important signals, introduce recency bias, and reduce LLM reliability even before hard token limits are reached.

   * **Architectural mismatch:** A monolithic prompt contradicts the incremental nature of observability pipelines, prevents partial reprocessing, and causes non-linear cost growth as data volume increases.

**Conclusion:** Pass 3 should not be in the critical path unless it demonstrates measurable wins (coverage + correctness) over the deterministic baseline, and it must be grounded to evidence (message_ids) if we keep it.

---

## 6) Adding agents

Agents can be useful, but they are not a free win. They increase:

* complexity
* latency/cost
* evaluation difficulty
* operational brittleness

### When agents help

* When you already have a stable ontology and want **per-step evidence checks**, e.g.:

  * “Is step X completed? Cite message_id(s).”
* When you have a retrieval layer and you want targeted queries across a large corpus.

### When agents hurt

* When ontology coverage is incomplete
* When you don’t have a clean evaluation contract
* When the baseline pipeline can already produce a useful dashboard deterministically

**Recommended stance:** agents belong in the experimental track, gated by evaluation against baseline.

---

## 7. Tracking multiple candidates per role/client

* For hiring, a process instance is often **(client, role, candidate)**, not just (client, role); collapsing candidates loses visibility or creates noisy duplicates.

* With Gmail/Slack data, multi-candidate tracking is **feasible in many cases** using strong signals (email, full name, CVs), but ambiguous cases exist and must be handled conservatively.

* **Observability baseline (recommended now):**

  * Extract candidate identity when confidence is high.
  * Reconcile by `(client, role, candidate_id)` when available.
  * Treat “unknown candidate” as a first-class state (do not over-merge).
  * Let the UI group candidates under the same role/client.

* **Workflow store (later, higher effort):**

  * Requires explicit candidate-level events, persistent identity keys, and UI merge/split corrections.
  * Feasible, but needs stronger extraction and human-in-the-loop support.

Multi-candidate tracking can be supported now in an observability pipeline with conservative identity resolution, while full canonical handling belongs in a future workflow-store design.

---

## 8) How we should continue: two viable paths

### Path A: Double down on Observability (recommended baseline)

**Goal:** ship value sooner with robust, explainable outputs.

Work items:

1. **Deterministic reconciliation stage**

   * merge duplicates into stable instances (client/process/role/candidate keys + time windows)
   * union evidence, choose latest state deterministically
2. **Exporter to dashboard schema**

   * produce a “dataset-shaped” output that the UI reads
   * allow null/unknown fields (phase/timings) without dropping rows
3. **Evaluation contract**

   * define a minimal scoring schema (status, current_step, client, role/candidate, health)
   * use it to compare any future agentic/workflow proposals

What you get:

* non-blank dashboard
* fewer duplicate rows
* scalable pipeline
* clear “unknown vs inferred” semantics

### Path B: Build a Workflow Store pipeline from scratch (experimental)

**Goal:** fully support `process_id/phase_id/step_id`, timing, deadlines, step validation.

Required architecture changes:

* extract **events** grounded in evidence (`step_started/completed/blocked`)
* keep an append-only event log + deterministic projections into a store
* harden process definitions (IDs/types/validation)
* introduce human-in-the-loop corrections as first-class events

What you get:

* true workflow-system behavior
* per-step timing and SLA correctness (eventually)
* but much higher effort, and needs evaluation + UI correction loop

---

## 9) Decision gates and measurable criteria

To keep debates productive, we should evaluate proposals on metrics:

### Baseline metrics (must-haves)

* **Coverage:** % of instances that make it into the dashboard
* **Stability:** idempotent outputs across reruns
* **Explainability:** each status/step has evidence pointers
* **Cost/latency:** runtime and token usage

### Workflow/agentic metrics (for adopting extra complexity)

* improves step correctness on labeled set vs baseline
* improves merge correctness or reduces duplicates
* doesn’t reduce coverage
* demonstrates robust behavior under data scale (1 year, multiple clients)

If an approach makes the dashboard blank, it fails coverage and shouldn’t be the default.

---

## Conclusion

We’re not blocked because the goal is “impossible”, but because we’re mixing two valid goals that require different architectures. The clean way forward is:

1. ship a robust observability baseline (reconciliation + exporter)
2. treat workflow-store/agentic approaches as experiments measured against baseline
3. only promote workflow-store layers when they demonstrate measurable wins without collapsing coverage.
