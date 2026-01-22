from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field, field_validator


class IngestionInfo(BaseModel):
    dataset_id: str
    time_window: Dict[str, Any]  # contains UTC instants and semantics
    time_window_local: Optional[Dict[str, Any]] = None
    rules_matched: List[str]
    source_ref: Dict[str, Any]
    ingested_at: str


class SlackMeta(BaseModel):
    channel_id: Optional[str] = None
    channel_name: Optional[str] = None
    user_id: Optional[str] = None
    thread_ts: Optional[str] = None


class RawMessage(BaseModel):
    id: str
    source: str  # "gmail" | "slack"
    ts: str
    ts_local: Optional[str] = None
    thread_id: Optional[str] = None

    sender: Optional[str] = None
    sender_name: Optional[str] = None
    sender_email: Optional[str] = None
    recipients: Optional[List[str]] = None
    subject: Optional[str] = None
    text: str

    has_attachments: bool = False
    attachments: Optional[List[Dict[str, Any]]] = None

    account_email: Optional[str] = None

    slack: Optional[SlackMeta] = None

    ingestion: IngestionInfo

    @field_validator("source")
    @classmethod
    def _source_allowed(cls, v: str) -> str:
        if v not in {"gmail", "slack"}:
            raise ValueError("source must be 'gmail' or 'slack'")
        return v


def build_slack_thread_id(channel_id: str, thread_ts: Optional[str], ts: str) -> str:
    """
    Slack: use "<channel_id>:<thread_ts>" if thread_ts exists else "<channel_id>:<ts>"
    """
    base = thread_ts if thread_ts else ts
    return f"{channel_id}:{base}"


def build_gmail_query(start_date: str, end_date: str, extra: str | None = None) -> str:
    """
    Gmail search query with date window and optional extra terms.
    Dates must be YYYY/MM/DD for Gmail.
    """
    sd = start_date.replace("-", "/")
    ed = end_date.replace("-", "/")
    parts = [f"after:{sd}", f"before:{ed}"]
    if extra:
        parts.append(extra.strip())
    return " ".join(parts)
