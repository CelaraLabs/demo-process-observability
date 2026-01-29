from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st

from ..utils.io import safe_load_json


def _metric_value(value: Optional[float], pct: bool = True) -> str:
    if value is None:
        return "n/a"
    if pct:
        return f"{value * 100:.1f}%"
    return f"{value}"


def _get_nested(d: Dict[str, Any], keys: list[str]) -> Optional[Any]:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur


def _render_kpis(coverage: Dict[str, Any]) -> None:
    global_cov = coverage.get("global") or {}
    funnel = coverage.get("recruiting_funnel") or coverage.get("hiring_funnel") or {}
    recruiting = coverage.get("recruiting_reconciliation") or coverage.get("hiring_reconciliation") or {}

    cols = st.columns(4)
    cols[0].metric("Incoming total", str(global_cov.get("incoming_total", "n/a")))
    cols[1].metric("Recruiting total", str(funnel.get("incoming_recruiting_total", funnel.get("incoming_hiring_total", "n/a"))))
    cols[2].metric(
        "Workflows written",
        str(recruiting.get("recruiting_written_total", recruiting.get("hiring_written_total", "n/a"))),
    )
    cols[3].metric("Canonical role", _metric_value(global_cov.get("canonical_role_pct")))

    cols = st.columns(4)
    cols[0].metric("Canonical process", _metric_value(global_cov.get("canonical_process_pct")))
    cols[1].metric("Canonical client", _metric_value(global_cov.get("canonical_client_pct")))
    cols[2].metric("Current step", _metric_value(global_cov.get("current_step_pct")))
    cols[3].metric("Health known", _metric_value(global_cov.get("health_known_pct")))

    cols = st.columns(4)
    cols[0].metric("Evidence ids", _metric_value(global_cov.get("evidence_ids_pct")))

    # Recruiting-scoped coverage (denominator = incoming_recruiting_total)
    if funnel:
        cols = st.columns(4)
        cols[0].metric(
            "Recruiting: canonical process",
            _metric_value(funnel.get("canonical_process_pct")),
        )
        cols[1].metric(
            "Recruiting: canonical client",
            _metric_value(funnel.get("canonical_client_pct")),
        )
        cols[2].metric(
            "Recruiting: canonical role",
            _metric_value(funnel.get("canonical_role_pct")),
        )
        cols[3].metric(
            "Recruiting: current step",
            _metric_value(funnel.get("current_step_pct")),
        )

    match_counts = recruiting.get("match_counts") or {}
    if match_counts:
        st.write({"match_counts": match_counts})


def _render_drift(drift: Dict[str, Any]) -> None:
    st.subheader("Top drift items")
    raw_steps = drift.get("raw_steps_unmatched") or []
    raw_roles = drift.get("candidate_role_raw") or []
    raw_procs = drift.get("candidate_process_raw") or []
    step_failures = drift.get("canonical_step_match_failures") or []

    cols = st.columns(4)
    cols[0].caption("Raw steps (unmatched)")
    cols[1].caption("Raw roles (unmapped)")
    cols[2].caption("Raw processes (unmapped)")
    cols[3].caption("Step match failures")

    cols[0].dataframe(pd.DataFrame(raw_steps), hide_index=True, use_container_width=True)
    cols[1].dataframe(pd.DataFrame(raw_roles), hide_index=True, use_container_width=True)
    cols[2].dataframe(pd.DataFrame(raw_procs), hide_index=True, use_container_width=True)
    cols[3].dataframe(pd.DataFrame(step_failures), hide_index=True, use_container_width=True)


def render(run_id: str, run_dir: Path) -> None:
    st.title(f"Run Summary — {run_id}")

    coverage_path = run_dir / "coverage_report.json"
    coverage, err = safe_load_json(coverage_path)
    if err:
        st.warning(f"Missing coverage report. Expected at: {coverage_path}")
        return
    assert coverage is not None

    _render_kpis(coverage)

    drift_path = run_dir / "mapping_drift_report.json"
    drift, drift_err = safe_load_json(drift_path)
    if drift and not drift_err:
        _render_drift(drift)
    else:
        st.caption("Mapping drift report not found.")

    recon_path = run_dir / "reconciliation_report.json"
    recon, recon_err = safe_load_json(recon_path)
    if recon_err:
        recon = None

    with st.expander("Coverage report (raw JSON)"):
        st.json(coverage)

    with st.expander("Mapping drift (raw JSON)"):
        if drift:
            st.json(drift)
        else:
            st.caption("Not found.")

    with st.expander("Reconciliation summary (raw JSON)"):
        if recon:
            st.json(recon)
        else:
            st.caption("Not found.")
