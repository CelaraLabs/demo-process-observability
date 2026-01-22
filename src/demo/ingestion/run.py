from __future__ import annotations

import json
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

from rich.console import Console

from .config import IngestionConfig, compute_window, load_config
from .gmail_api import make_gmail_service
from .manifest import build_manifest, build_stats
from .models import RawMessage, build_gmail_query
from .normalize import apply_filters, dedup_and_sort, normalize_gmail_message, normalize_slack_message
from zoneinfo import ZoneInfo
from .slack_api import SlackClient
from .write import write_json_file, write_raw_messages

console = Console()


def _read_credentials(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as e:
        raise RuntimeError(f"Failed to read credentials at {path}: {e}") from e


def _derive_unique_out_dir(base_out: Path, dataset_id: str, start_date: str, end_date: str) -> Path:
    name = f"{dataset_id}_{start_date}_{end_date}"
    target = base_out / name
    if not target.exists():
        return target
    # Increment suffix until available
    i = 1
    while True:
        cand = base_out / f"{name}_{i}"
        if not cand.exists():
            return cand
        i += 1


def run_ingestion(config_path: Path, out_dir: Path, auto_subdir: bool = False) -> Path:
    """
    Execute ingestion per the Stage Ingestion spec.
    Returns path to data/raw_messages.jsonl (written).
    """
    cfg = load_config(config_path)
    creds = _read_credentials(cfg.credentials_file)

    start_dt, end_dt, start_date, end_date = compute_window(cfg)
    console.print(
        f"[bold]Ingestion window:[/bold] {start_date} → {end_date}  "
        f"(dataset_id={cfg.dataset_id})"
    )
    out_dir = Path(out_dir)
    if auto_subdir:
        out_dir = _derive_unique_out_dir(out_dir, cfg.dataset_id, start_date, end_date)
    out_dir.mkdir(parents=True, exist_ok=True)
    console.print(f"[bold]Output directory:[/bold] {out_dir}")

    dataset_id = cfg.dataset_id
    raw_items: List[RawMessage] = []
    rules_counter: Counter = Counter()
    read_counts: Dict[str, int] = {"gmail": 0, "slack": 0}
    dropped_by_reason: Dict[str, int] = {}
    per_channel_counts: Dict[str, int] = {}
    dataset_timezone = "America/Argentina/Buenos_Aires"

    # Prepare window representations
    dataset_timezone = "America/Argentina/Buenos_Aires"
    start_utc_iso = start_dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    end_utc_iso = end_dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    local_tz = ZoneInfo(dataset_timezone)
    start_local_iso = start_dt.astimezone(local_tz).isoformat()
    end_local_iso = end_dt.astimezone(local_tz).isoformat()

    # Gmail
    gmail_mailboxes: List[str] = []
    if cfg.gmail_enabled():
        gmail_conf = cfg.raw.get("gmail", {})
        gmail_mailboxes = [str(e) for e in (gmail_conf.get("owner_mailboxes") or [])]
        page_size = int(gmail_conf.get("page_size", 200))
        extra = str(gmail_conf.get("query", {}).get("extra") or "")
        max_per_mailbox = gmail_conf.get("max_per_mailbox")
        console.print(f"[bold]Gmail:[/bold] {len(gmail_mailboxes)} owner mailbox(es) to scan")
        for mailbox in gmail_mailboxes:
            try:
                service = make_gmail_service(cfg.credentials_file, mailbox)
            except Exception as e:
                console.print(f"[red]Gmail client init failed for {mailbox}:[/red] {e}")
                continue
            query = build_gmail_query(start_date, end_date, extra)
            console.print(f"[cyan]Gmail[/cyan] {mailbox}: query='{query}' page_size={page_size}")
            try:
                # List messages
                msg_ids: List[str] = []
                next_page: Optional[str] = None
                while True:
                    req = service.users().messages().list(userId="me", q=query, maxResults=page_size, pageToken=next_page)
                    resp = req.execute()
                    for item in resp.get("messages", []) or []:
                        msg_ids.append(item.get("id"))
                        if max_per_mailbox and len(msg_ids) >= int(max_per_mailbox):
                            break
                    if len(msg_ids) and len(msg_ids) % (page_size * 5) == 0:
                        console.print(f"[cyan]Gmail[/cyan] {mailbox}: listed {len(msg_ids)} ids so far...")
                    if max_per_mailbox and len(msg_ids) >= int(max_per_mailbox):
                        break
                    next_page = resp.get("nextPageToken")
                    if not next_page:
                        break
                console.print(f"[cyan]Gmail[/cyan] {mailbox}: total ids={len(msg_ids)}")
                read_counts["gmail"] += len(msg_ids)
                # Fetch each message detail
                fetched = 0
                total_to_fetch = len(msg_ids)
                if total_to_fetch > 0:
                    console.print(f"[cyan]Gmail[/cyan] {mailbox}: fetching {total_to_fetch} messages ...")
                # Log every ~5% or at least every 50, whichever is smaller threshold in count terms
                log_every = max(1, min(50, max(1, total_to_fetch // 20)))
                for mid in msg_ids:
                    try:
                        msg = service.users().messages().get(userId="me", id=mid, format="full").execute()
                        rm = normalize_gmail_message(
                            msg,
                            mailbox,
                            dataset_id,
                            start_date,
                            end_date,
                            start_utc_iso,
                            end_utc_iso,
                            dataset_timezone,
                            start_local_iso,
                            end_local_iso,
                            query,
                        )
                        if rm:
                            raw_items.append(rm)
                            for tag in rm.ingestion.rules_matched:
                                rules_counter[tag] += 1
                        fetched += 1
                        if fetched % log_every == 0 or fetched == total_to_fetch:
                            pct = int((fetched / max(1, total_to_fetch)) * 100)
                            console.print(f"[cyan]Gmail[/cyan] {mailbox}: fetched {fetched}/{total_to_fetch} ({pct}%)")
                    except Exception:
                        continue
                console.print(f"[cyan]Gmail[/cyan] {mailbox}: fetched {fetched}/{total_to_fetch} (done)")
            except Exception as e:
                console.print(f"[yellow]Gmail listing failed for {mailbox}:[/yellow] {e}")

    # Slack
    slack_strategy = {
        "include_channels": cfg.raw.get("slack", {}).get("include_channels"),
        "exclude_channels": cfg.raw.get("slack", {}).get("exclude_channels") or [],
        "include_archived": bool(cfg.raw.get("slack", {}).get("include_archived", False)),
    }
    if cfg.slack_enabled():
        slack_conf = cfg.raw.get("slack", {})
        token = creds.get("slack_bot_token")
        if not token:
            console.print("[yellow]No slack_bot_token in credentials; skipping Slack ingestion.[/yellow]")
        else:
            client = SlackClient(token=token)
            include_channels = slack_conf.get("include_channels")
            exclude_by_name = set(slack_conf.get("exclude_channels") or [])
            include_archived = bool(slack_conf.get("include_archived", False))
            page_size = int(slack_conf.get("page_size", 200))
            max_per_channel = slack_conf.get("max_per_channel")
            # Resolve channels to ingest
            channels: List[Dict[str, Any]] = []
            try:
                console.print("[bold]Slack:[/bold] discovering channels...")
                cursor = None
                while True:
                    resp = client.conversations_list(cursor=cursor, limit=page_size)
                    if not resp.get("ok"):
                        break
                    for ch in resp.get("channels", []) or []:
                        if not ch.get("is_member"):
                            continue
                        if ch.get("is_archived") and not include_archived:
                            continue
                        channels.append(ch)
                    cursor = (resp.get("response_metadata") or {}).get("next_cursor")
                    if not cursor:
                        break
            except Exception as e:
                console.print(f"[yellow]Slack conversations.list failed:[/yellow] {e}")
                channels = []
            # Filter by include/exclude if provided
            if include_channels:
                include_set = set(include_channels)
                channels = [c for c in channels if c.get("id") in include_set or c.get("name") in include_set]
            channels = [c for c in channels if c.get("name") not in exclude_by_name]
            console.print(f"[cyan]Slack[/cyan]: {len(channels)} channel(s) to scan")

            # Fetch messages for each channel
            oldest_epoch = int(start_dt.timestamp())
            latest_epoch = int(end_dt.timestamp())
            for ch in channels:
                cid = ch.get("id")
                cname = ch.get("name")
                per_channel_counts[cid] = 0
                try:
                    console.print(f"[cyan]Slack[/cyan] {cname or cid}: fetching history...")
                    cursor = None
                    pulled = 0
                    while True:
                        resp = client.conversations_history(
                            channel=cid,
                            oldest=str(oldest_epoch),
                            latest=str(latest_epoch),
                            cursor=cursor,
                            limit=page_size,
                        )
                        if not resp.get("ok"):
                            break
                        for msg in resp.get("messages", []) or []:
                            per_channel_counts[cid] += 1
                            pulled += 1
                            rm = normalize_slack_message(
                                msg,
                                cid,
                                cname,
                                dataset_id,
                                start_date,
                                end_date,
                                start_utc_iso,
                                end_utc_iso,
                                dataset_timezone,
                                start_local_iso,
                                end_local_iso,
                            )
                            if rm:
                                raw_items.append(rm)
                                for tag in rm.ingestion.rules_matched:
                                    rules_counter[tag] += 1
                            if max_per_channel and pulled >= int(max_per_channel):
                                break
                        if pulled and pulled % (page_size * 5) == 0:
                            console.print(f"[cyan]Slack[/cyan] {cname or cid}: pulled {pulled} so far...")
                        if max_per_channel and pulled >= int(max_per_channel):
                            break
                        cursor = (resp.get("response_metadata") or {}).get("next_cursor")
                        if not cursor:
                            break
                except Exception as e:
                    console.print(f"[yellow]Slack history failed for {cid} ({cname}):[/yellow] {e}")
                console.print(f"[cyan]Slack[/cyan] {cname or cid}: total pulled={pulled}")
            read_counts["slack"] = sum(per_channel_counts.values())

    # Enforce window post-filter (UTC, start inclusive, end exclusive)
    def _parse_ts(ts: str) -> Optional[datetime]:
        try:
            if ts.endswith("Z"):
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return datetime.fromisoformat(ts)
        except Exception:
            return None

    kept_time_window: List[RawMessage] = []
    out_of_window = 0
    min_ts_val: Optional[str] = None
    max_ts_val: Optional[str] = None
    for rm in raw_items:
        dt = _parse_ts(rm.ts)
        if not dt:
            continue
        if not (start_dt <= dt < end_dt):
            out_of_window += 1
            continue
        kept_time_window.append(rm)
        if min_ts_val is None or rm.ts < min_ts_val:
            min_ts_val = rm.ts
        if max_ts_val is None or rm.ts > max_ts_val:
            max_ts_val = rm.ts
    raw_items = kept_time_window

    # Filters
    filters_conf = cfg.raw.get("filters", {})
    min_text_len = int(filters_conf.get("min_text_len", 20))
    drop_sender_contains = [s.lower() for s in (filters_conf.get("drop_if_sender_contains") or [])]
    kept, drops = apply_filters(raw_items, min_text_len=min_text_len, drop_sender_contains=drop_sender_contains)
    for k, v in drops.items():
        dropped_by_reason[k] = dropped_by_reason.get(k, 0) + v
    console.print(
        f"[bold]Filtering:[/bold] raw={len(raw_items)} kept={len(kept)} "
        f"dropped={sum(drops.values())} reasons={drops or {}}"
    )

    # Dedup + sort
    final_items = dedup_and_sort(kept)
    console.print(f"[bold]Dedup + sort:[/bold] unique={len(final_items)}")

    # Write outputs
    raw_path = out_dir / "raw_messages.jsonl"
    console.print(f"[bold]Writing outputs[/bold] to {out_dir} ...")
    write_raw_messages(raw_path, final_items)
    manifest = build_manifest(
        dataset_id=dataset_id,
        start_date=start_date,
        end_date=end_date,
        gmail_mailboxes=gmail_mailboxes,
        slack_strategy=slack_strategy,
        items=final_items,
        rules_counter=rules_counter,
        cfg_snapshot=cfg.raw,
        timezone_name=dataset_timezone,
        start_utc_iso=start_utc_iso,
        end_utc_iso=end_utc_iso,
    )
    # Augment counts with window-level summary
    manifest["counts"].update(
        {
            "fetched_total": len(kept) + out_of_window,
            "kept_in_window": len(kept),
            "dropped_out_of_window": out_of_window,
        }
    )
    write_json_file(out_dir / "ingestion_manifest.json", manifest)
    stats = build_stats(
        read_counts=read_counts,
        kept_items=final_items,
        dropped_by_reason=dropped_by_reason,
        per_channel_counts=per_channel_counts,
        out_of_window_dropped=out_of_window,
        min_ts=min_ts_val,
        max_ts=max_ts_val,
    )
    write_json_file(out_dir / "ingestion_stats.json", stats)

    console.print(
        f"[green]Ingestion complete[/green]: kept={len(final_items)} read_gmail={read_counts['gmail']} read_slack={read_counts['slack']}"
    )
    return raw_path
