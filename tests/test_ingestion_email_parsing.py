from __future__ import annotations

from datetime import datetime, timezone

from demo.ingestion.utils_email import clean_gmail_text, parse_recipients, parse_sender
from demo.ingestion.normalize import normalize_gmail_message, normalize_slack_message


def test_parse_sender_and_recipients():
    name, addr = parse_sender('Alice Example <alice@example.com>')
    assert name == "Alice Example"
    assert addr == "alice@example.com"
    recips = parse_recipients("Bob <bob@ex.com>, carol@ex.com", "dave@ex.com")
    assert recips == ["bob@ex.com", "carol@ex.com", "dave@ex.com"]


def test_clean_gmail_text_removes_quotes_and_html():
    raw = "<div>Hello world</div>\nOn Tue, Someone wrote:\n> old stuff"
    cleaned = clean_gmail_text(raw)
    assert "Hello world" in cleaned
    assert "old stuff" not in cleaned
    assert "<div>" not in cleaned


def test_gmail_internaldate_preferred():
    gmail_obj = {
        "id": "1",
        "threadId": "t1",
        "internalDate": str(int(datetime(2026, 1, 9, 14, 27, 0, tzinfo=timezone.utc).timestamp() * 1000)),
        "payload": {"headers": [{"name": "From", "value": "Alice <alice@example.com>"}]},
        "snippet": "Hello",
    }
    rm = normalize_gmail_message(gmail_obj, "acc@ex.com", "ds", "2026-01-01", "2026-02-01", "q")
    assert rm is not None
    assert rm.ts.endswith("Z")
    assert rm.sender_email == "alice@example.com"


def test_gmail_date_header_fallback_timezone():
    gmail_obj = {
        "id": "1",
        "threadId": "t1",
        "payload": {
            "headers": [
                {"name": "From", "value": "Alice <alice@example.com>"},
                {"name": "Date", "value": "Fri, 09 Jan 2026 09:27:00 -0500"},
            ]
        },
        "snippet": "Hello",
    }
    rm = normalize_gmail_message(gmail_obj, "acc@ex.com", "ds", "2026-01-01", "2026-02-01", "q")
    assert rm is not None
    # -0500 should convert to 14:27:00Z
    assert rm.ts.startswith("2026-01-09T14:27:00")
