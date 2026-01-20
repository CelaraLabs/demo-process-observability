from __future__ import annotations

from typing import Optional, Dict

from demo.pipeline.pass2 import canonicalize_process, _event_timestamp_str


def ev(evidence_ts: Optional[str], top_ts: Optional[str]) -> Dict:
    return {"timestamp": top_ts, "evidence": ({"timestamp": evidence_ts} if evidence_ts else {})}


def test_event_timestamp_priority():
    # 1) prefer evidence.timestamp
    assert _event_timestamp_str(ev("2025-01-02T00:00:00", "2025-01-01T00:00:00")) == "2025-01-02T00:00:00"
    # 2) fall back to event.timestamp
    assert _event_timestamp_str(ev(None, "2025-01-01T00:00:00")) == "2025-01-01T00:00:00"
    # 3) none -> None
    assert _event_timestamp_str(ev(None, None)) is None


def test_canonicalize_process_synonyms():
    assert canonicalize_process("Recruiting") == "hiring"
    assert canonicalize_process("hiring") == "hiring"
    assert canonicalize_process("AI searching") == "hiring"
    assert canonicalize_process("ai search") == "hiring"
    assert canonicalize_process("recruiting   pipeline") == "hiring"
    assert canonicalize_process("software delivery") == "delivery"
    assert canonicalize_process("ops") == "ops"
    assert canonicalize_process("Operations") == "ops"


def test_canonicalize_process_cleaning_and_titlecase():
    # cleaned original in Title Case for unknowns
    assert canonicalize_process("  biz   dev  ") == "Biz Dev"
    assert canonicalize_process("R&D") == "R&D".title()


def test_canonicalize_process_nulls():
    assert canonicalize_process(None) is None
    assert canonicalize_process("") is None
    assert canonicalize_process("   ") is None
