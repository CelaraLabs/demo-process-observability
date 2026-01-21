from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import base64

from .models import IngestionInfo, RawMessage, SlackMeta, build_slack_thread_id


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _gmail_decode_body(data: Optional[str]) -> str:
    if not data:
        return ""
    try:
        # Gmail uses URL-safe base64 without padding sometimes
        missing_padding = (-len(data)) % 4
        if missing_padding:
            data += "=" * missing_padding
        return base64.urlsafe_b64decode(data.encode("utf-8")).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _gmail_collect_text(payload: Dict[str, Any]) -> str:
    """
    Collect text content from Gmail message payload.
    Prefer text/plain parts; fallback to concatenated text of other parts if needed.
    """
    texts: List[str] = []

    def walk(part: Dict[str, Any]) -> None:
        mime = (part.get("mimeType") or "").lower()
        body = part.get("body") or {}
        data = body.get("data")
        if data and ("text/plain" in mime or mime.startswith("text/")):
            decoded = _gmail_decode_body(data)
            if decoded:
                texts.append(decoded)
        for child in (part.get("parts") or []):
            if isinstance(child, dict):
                walk(child)

    if payload:
        walk(payload)
        if not texts:
            # Fallback: try top-level body if present
            top = _gmail_decode_body((payload.get("body") or {}).get("data"))
            if top:
                texts.append(top)
    return "\n\n".join(t for t in texts if t)


def normalize_gmail_message(
    gmail_obj: Dict[str, Any],
    account_email: str,
    dataset_id: str,
    start_date: str,
    end_date: str,
    gmail_query: str,
) -> Optional[RawMessage]:
    """
    Minimal Gmail normalization. Expects fields like id, threadId, payload/headers, snippet.
    Returns None if timestamp cannot be parsed.
    """
    msg_id = gmail_obj.get("id")
    thread_id = gmail_obj.get("threadId")
    headers = {h.get("name"): h.get("value") for h in (gmail_obj.get("payload", {}).get("headers") or [])}
    date_header = headers.get("Date") or headers.get("date")
    ts: Optional[str] = None
    try:
        if date_header:
            # naive parsing; for robustness, prefer python-dateutil in real code
            ts_dt = datetime.strptime(date_header[:25], "%a, %d %b %Y %H:%M:%S")
            ts = ts_dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        ts = None
    if not ts:
        return None
    sender = headers.get("From")
    subject = headers.get("Subject")
    # Prefer full decoded body; fallback to snippet
    text = _gmail_collect_text(gmail_obj.get("payload") or {}) or (gmail_obj.get("snippet") or "")
    rules = [f"mailbox:{account_email}", "time_window"]
    ingestion = IngestionInfo(
        dataset_id=dataset_id,
        time_window={"start": start_date, "end": end_date},
        rules_matched=rules,
        source_ref={"gmail_query": gmail_query},
        ingested_at=_iso_now(),
    )
    rm = RawMessage(
        id=f"gmail_{msg_id}",
        source="gmail",
        ts=ts,
        thread_id=thread_id,
        sender=sender,
        sender_name=None,
        recipients=None,
        subject=subject,
        text=text,
        has_attachments=bool(gmail_obj.get("payload", {}).get("parts")),
        attachments=[],
        account_email=account_email,
        slack=None,
        ingestion=ingestion,
    )
    return rm


def normalize_slack_message(
    msg: Dict[str, Any],
    channel_id: str,
    channel_name: Optional[str],
    dataset_id: str,
    start_date: str,
    end_date: str,
) -> Optional[RawMessage]:
    ts = msg.get("ts")
    if not ts:
        return None
    # Slack ts is epoch string; convert to ISO seconds
    try:
        seconds = int(float(ts))
        ts_iso = datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None
    thread_ts = msg.get("thread_ts")
    text = msg.get("text") or ""
    user = msg.get("user")
    rules = ["channel_member", "time_window"]
    if channel_name:
        rules.append(f"channel:{channel_name}")
    thread_id = build_slack_thread_id(channel_id, thread_ts, ts)
    ingestion = IngestionInfo(
        dataset_id=dataset_id,
        time_window={"start": start_date, "end": end_date},
        rules_matched=rules,
        source_ref={"slack_channels_scanned": 1},
        ingested_at=_iso_now(),
    )
    slack_meta = SlackMeta(
        channel_id=channel_id,
        channel_name=channel_name,
        user_id=user,
        thread_ts=thread_ts,
    )
    rm = RawMessage(
        id=f"slack_{channel_id}_{ts}",
        source="slack",
        ts=ts_iso,
        thread_id=thread_id,
        sender=None,
        sender_name=None,
        recipients=None,
        subject=None,
        text=text,
        has_attachments=False,
        attachments=[],
        account_email=None,
        slack=slack_meta,
        ingestion=ingestion,
    )
    return rm


def apply_filters(
    items: List[RawMessage],
    min_text_len: int,
    drop_sender_contains: List[str],
) -> (List[RawMessage], Dict[str, int]):
    kept: List[RawMessage] = []
    drops: Dict[str, int] = {}
    for rm in items:
        if len((rm.text or "").strip()) < min_text_len:
            drops["short_text"] = drops.get("short_text", 0) + 1
            continue
        if rm.source == "gmail" and rm.sender:
            lowered = rm.sender.lower()
            if any(tok in lowered for tok in drop_sender_contains):
                drops["sender_blocklist"] = drops.get("sender_blocklist", 0) + 1
                continue
        kept.append(rm)
    return kept, drops


def dedup_and_sort(items: List[RawMessage]) -> List[RawMessage]:
    seen = set()
    unique: List[RawMessage] = []
    for rm in items:
        if rm.id in seen:
            continue
        seen.add(rm.id)
        unique.append(rm)
    unique.sort(key=lambda r: (r.ts, r.id))
    return unique
