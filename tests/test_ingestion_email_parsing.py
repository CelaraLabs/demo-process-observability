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
    rm = normalize_gmail_message(
        gmail_obj,
        "acc@ex.com",
        "ds",
        "2026-01-01",
        "2026-02-01",
        "2026-01-01T00:00:00Z",
        "2026-02-01T00:00:00Z",
        "America/Argentina/Buenos_Aires",
        "2026-01-01T00:00:00-03:00",
        "2026-02-01T00:00:00-03:00",
        "q",
    )
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
    rm = normalize_gmail_message(
        gmail_obj,
        "acc@ex.com",
        "ds",
        "2026-01-01",
        "2026-02-01",
        "2026-01-01T00:00:00Z",
        "2026-02-01T00:00:00Z",
        "America/Argentina/Buenos_Aires",
        "2026-01-01T00:00:00-03:00",
        "2026-02-01T00:00:00-03:00",
        "q",
    )
    assert rm is not None
    # -0500 should convert to 14:27:00Z
    assert rm.ts.startswith("2026-01-09T14:27:00")


def test_clean_gmail_text_spanish_and_images():
    raw = (
        "Hola equipo,\nAdjunto el reporte.\n\n"
        "[image: image.png]\n"
        "El jue, 8 ene 2026 12:34 escribió:\n"
        "> viejo texto\n"
    )
    cleaned = clean_gmail_text(raw)
    assert "Hola equipo" in cleaned
    assert "[image:" not in cleaned
    assert "viejo texto" not in cleaned


def test_clean_gmail_text_on_wrote_two_line_header():
    raw = (
        "Top reply line.\n"
        "On Fri, Jan 9, 2026 at 9:27 AM Someone <someone@example.com>\n"
        "wrote:\n"
        "> quoted stuff\n"
    )
    cleaned = clean_gmail_text(raw)
    assert "Top reply line." in cleaned
    assert "quoted stuff" not in cleaned


def test_attachment_dedupe():
    gmail_obj = {
        "id": "1",
        "threadId": "t1",
        "internalDate": str(int(datetime(2026, 1, 9, 14, 27, 0, tzinfo=timezone.utc).timestamp() * 1000)),
        "payload": {
            "headers": [{"name": "From", "value": "Alice <alice@example.com>"}],
            "parts": [
                {"filename": "image.png", "mimeType": "image/png", "body": {"size": 123}},
                {"filename": "image.png", "mimeType": "image/png", "body": {"size": 123}},  # dup
                {"filename": "doc.pdf", "mimeType": "application/pdf", "body": {"size": 456}},
            ],
        },
        "snippet": "Hello",
    }
    rm = normalize_gmail_message(
        gmail_obj,
        "acc@ex.com",
        "ds",
        "2026-01-01",
        "2026-02-01",
        "2026-01-01T00:00:00Z",
        "2026-02-01T00:00:00Z",
        "America/Argentina/Buenos_Aires",
        "2026-01-01T00:00:00-03:00",
        "2026-02-01T00:00:00-03:00",
        "q",
    )
    assert rm is not None
    assert rm.has_attachments is True
    assert len(rm.attachments or []) == 2
