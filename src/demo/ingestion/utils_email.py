from __future__ import annotations

import re
from email.utils import getaddresses, parseaddr
from typing import List, Tuple


_RE_REPLY_MARKERS = re.compile(
    r"(\nOn .+ wrote:|\n-----Original Message-----|\nFrom: .+Sent: .+To:)",
    flags=re.IGNORECASE | re.DOTALL,
)
_RE_TAGS = re.compile(r"<[^>]+>")
_RE_MULTI_BLANKS = re.compile(r"\n{3,}")


def clean_gmail_text(text: str) -> str:
    """
    Heuristic cleanup: keep newest content only, remove HTML tags, normalize whitespace.
    """
    if not isinstance(text, str):
        text = str(text or "")
    s = text.replace("\r\n", "\n")
    # Keep content before first reply marker
    m = _RE_REPLY_MARKERS.search(s)
    if m:
        s = s[: m.start()]
    # Strip HTML tags if any present (basic)
    if "<div" in s or "<html" in s or "gmail_quote" in s or "<body" in s:
        s = _RE_TAGS.sub("", s)
    # Collapse excessive blank lines
    s = _RE_MULTI_BLANKS.sub("\n\n", s)
    return s.strip()


def parse_recipients(to_header: str | None, cc_header: str | None) -> List[str]:
    parts = []
    if to_header:
        parts.append(to_header)
    if cc_header:
        parts.append(cc_header)
    addrs = getaddresses(parts)
    result: List[str] = []
    seen = set()
    for name, addr in addrs:
        addr_l = (addr or "").strip().lower()
        if addr_l and addr_l not in seen:
            result.append(addr_l)
            seen.add(addr_l)
    return result


def parse_sender(sender_header: str | None) -> Tuple[str | None, str | None]:
    if not sender_header:
        return None, None
    name, addr = parseaddr(sender_header)
    name = name.strip() or None
    addr = (addr or "").strip() or None
    return name, addr
