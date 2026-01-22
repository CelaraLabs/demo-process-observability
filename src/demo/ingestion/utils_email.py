from __future__ import annotations

import re
from email.utils import getaddresses, parseaddr
from typing import List, Tuple


_RE_BOUNDARIES = [
    re.compile(r"^[>\s]*On .+ wrote:\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[>\s]*On .+\r?\n[>\s]*wrote:\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[>\s]*-----Original Message-----\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[>\s]*From:\s", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[>\s]*Sent:\s", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[>\s]*To:\s", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[>\s]*Subject:\s", re.IGNORECASE | re.MULTILINE),
    # Spanish
    re.compile(r"^[>\s]*El .+ escribi[oó]:\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[>\s]*El .+ a la?s .+ escribi[oó]:\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[>\s]*El .+\r?\n[>\s]*escribi[oó]:\s*$", re.IGNORECASE | re.MULTILINE),
    # Portuguese
    re.compile(r"^[>\s]*Em .+ escreveu:\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[>\s]*Em .+\r?\n[>\s]*escreveu:\s*$", re.IGNORECASE | re.MULTILINE),
    # French
    re.compile(r"^[>\s]*Le .+ a écrit\s*:\s*$", re.IGNORECASE | re.MULTILINE),
    re.compile(r"^[>\s]*Le .+\r?\n[>\s]*a écrit\s*:\s*$", re.IGNORECASE | re.MULTILINE),
]
_RE_TAGS = re.compile(r"<[^>]+>")
_RE_MULTI_BLANKS = re.compile(r"\n{3,}")
_RE_SIGNATURE_DELIM = re.compile(r"^--\s*$", re.MULTILINE)
_RE_SENT_FROM = re.compile(r"^\s*Sent from my iPhone\s*$", re.IGNORECASE | re.MULTILINE)
_RE_INLINE_IMAGE = re.compile(r"\[image:\s*[^\]]+\]", re.IGNORECASE)


def _find_wrapped_boundary(s: str) -> int | None:
    """
    Handle reply headers that wrap across multiple lines, e.g.:
      On Fri, Jan 9, 2026 at 9:27 AM Someone <someone@example.com>
      wrote:
    Or Spanish/French/Portuguese equivalents where the verb appears on a new line.
    Returns the earliest start index of the header line to truncate at, or None.
    """
    candidates: list[int] = []
    # Generic 'On ... wrote:' wrapped within next few lines
    first_on = re.compile(r"^[>\s]*On\s.*$", re.IGNORECASE | re.MULTILINE)
    # Accept 'wrote:' at end of any of the next few lines (even if preceded by email text)
    wrote = re.compile(r"wrote:\s*$", re.IGNORECASE | re.MULTILINE)
    for m in first_on.finditer(s):
        start = m.start()
        # Look ahead a small window (next 3 lines) for 'wrote:'
        end_scan = s.find("\n", start)
        if end_scan == -1:
            end_scan = len(s)
        # Extend to a few more lines to catch wrapped email/name
        lines_ahead = 0
        idx = end_scan
        while lines_ahead < 3 and idx < len(s):
            next_nl = s.find("\n", idx + 1)
            if next_nl == -1:
                next_nl = len(s)
            idx = next_nl
            lines_ahead += 1
        window = s[start:idx]
        if wrote.search(window):
            candidates.append(start)
    # Spanish: 'El ... escribió:' possibly wrapped
    first_es = re.compile(r"^[>\s]*El\s.*$", re.IGNORECASE | re.MULTILINE)
    escribio = re.compile(r"escribi[oó]:\s*$", re.IGNORECASE | re.MULTILINE)
    for m in first_es.finditer(s):
        start = m.start()
        idx = start
        for _ in range(3):
            next_nl = s.find("\n", idx + 1)
            if next_nl == -1:
                next_nl = len(s)
            idx = next_nl
        window = s[start:idx]
        if escribio.search(window):
            candidates.append(start)
    # Portuguese
    first_pt = re.compile(r"^[>\s]*Em\s.*$", re.IGNORECASE | re.MULTILINE)
    escreveu = re.compile(r"escreveu:\s*$", re.IGNORECASE | re.MULTILINE)
    for m in first_pt.finditer(s):
        start = m.start()
        idx = start
        for _ in range(3):
            next_nl = s.find("\n", idx + 1)
            if next_nl == -1:
                next_nl = len(s)
            idx = next_nl
        window = s[start:idx]
        if escreveu.search(window):
            candidates.append(start)
    # French
    first_fr = re.compile(r"^[>\s]*Le\s.*$", re.IGNORECASE | re.MULTILINE)
    a_ecrit = re.compile(r"a écrit\s*:\s*$", re.IGNORECASE | re.MULTILINE)
    for m in first_fr.finditer(s):
        start = m.start()
        idx = start
        for _ in range(3):
            next_nl = s.find("\n", idx + 1)
            if next_nl == -1:
                next_nl = len(s)
            idx = next_nl
        window = s[start:idx]
        if a_ecrit.search(window):
            candidates.append(start)
    if not candidates:
        return None
    return min(candidates)


def clean_gmail_text(text: str) -> str:
    """
    Heuristic cleanup: keep newest content only, remove HTML tags, normalize whitespace.
    """
    if not isinstance(text, str):
        text = str(text or "")
    s = text.replace("\r\n", "\n")
    # First handle wrapped multi-line reply headers
    wrapped_idx = _find_wrapped_boundary(s)
    if wrapped_idx is not None and wrapped_idx > 0:
        s = s[:wrapped_idx]
    # Trim at earliest reply boundary
    earliest = None
    for pat in _RE_BOUNDARIES:
        m = pat.search(s)
        if m:
            idx = m.start()
            if earliest is None or idx < earliest:
                earliest = idx
    if earliest is None and "gmail_quote" in s:
        earliest = s.lower().find("gmail_quote")
    if earliest is not None and earliest > 0:
        s = s[:earliest]
    # Strip HTML tags if any present (basic)
    if "<div" in s or "<html" in s or "gmail_quote" in s or "<body" in s:
        s = _RE_TAGS.sub("", s)
    # Remove inline image placeholders
    s = _RE_INLINE_IMAGE.sub("", s)
    # Remove trailing signature delimiter or simple mobile footer
    sig_match = _RE_SIGNATURE_DELIM.search(s)
    if sig_match:
        s = s[: sig_match.start()]
    s = _RE_SENT_FROM.sub("", s)
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
