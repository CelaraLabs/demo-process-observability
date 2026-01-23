from __future__ import annotations

import re
import string
from typing import Dict, List, Optional, Tuple

from .models import ClientsCatalog, ProcessCatalog, RolesCatalog


_SPACE_RE = re.compile(r"\s+")
_PUNCT_TABLE = str.maketrans({c: " " for c in ",.;:()[]{}<>/\\|"})


def norm_text(s: str) -> str:
    if s is None:
        return ""
    s = " ".join(str(s).strip().split())
    return s.lower()


def norm_tokenish(s: str) -> str:
    if s is None:
        return ""
    s = str(s).translate(_PUNCT_TABLE)
    s = _SPACE_RE.sub(" ", s)
    return s.strip().lower()


def _unique(values: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def canonicalize_process(process_raw: str | None, process_catalog: ProcessCatalog) -> str | None:
    if not process_raw:
        return None
    raw_norm = norm_text(process_raw)
    # 1) exact match on process key
    if raw_norm in (k.lower() for k in process_catalog.processes.keys()):
        for k in process_catalog.processes.keys():
            if raw_norm == k.lower():
                return k
    # 2) exact match on display_name
    for k, spec in process_catalog.processes.items():
        if raw_norm == norm_text(spec.display_name):
            return k
    # 3) exact match on any process_aliases
    for k, spec in process_catalog.processes.items():
        for alias in (spec.process_aliases or []):
            if raw_norm == norm_text(alias):
                return k
    # 4) substring match on aliases (unambiguous)
    candidates: List[str] = []
    for k, spec in process_catalog.processes.items():
        for alias in (spec.process_aliases or []):
            an = norm_text(alias)
            if an in raw_norm or raw_norm in an:
                candidates.append(k)
    candidates = _unique(candidates)
    if len(candidates) == 1:
        return candidates[0]
    return None


def canonicalize_client(client_raw: str | None, clients_catalog: ClientsCatalog) -> str | None:
    if client_raw is None:
        return None
    raw_norm = norm_text(client_raw)
    # 1) exact match on name
    for c in clients_catalog.clients:
        if raw_norm == norm_text(c.name):
            return c.name
    # 2) exact match on aliases
    for c in clients_catalog.clients:
        for alias in (c.aliases or []):
            if raw_norm == norm_text(alias):
                return c.name
    # 3) weak substring match if unique
    candidates: List[str] = []
    for c in clients_catalog.clients:
        for token in [c.name] + (c.aliases or []):
            tn = norm_text(token)
            if tn in raw_norm or raw_norm in tn:
                candidates.append(c.name)
                break
    candidates = _unique(candidates)
    if len(candidates) == 1:
        return candidates[0]
    # No match: return cleaned original (Title Case)
    return " ".join(w.capitalize() for w in raw_norm.split())


def canonicalize_role(role_raw: str | None, roles_catalog: RolesCatalog) -> str:
    if role_raw is None or not str(role_raw).strip():
        return "Unknown"
    raw_norm = norm_text(role_raw)
    # exact canonical
    for r in roles_catalog.canonical:
        if raw_norm == norm_text(r):
            return r
    # alias to canonical
    for canon, aliases in (roles_catalog.aliases or {}).items():
        for alias in aliases:
            if raw_norm == norm_text(alias):
                return canon
    # else Other
    return "Other"


def match_step(step_raw: str | None, canonical_process: str | None, process_catalog: ProcessCatalog) -> str | None:
    if not step_raw or not canonical_process:
        return None
    if canonical_process not in process_catalog.processes:
        return None
    spec = process_catalog.processes[canonical_process]
    step_n = norm_text(step_raw)
    # 1) exact match on steps
    for s in spec.steps:
        if step_n == norm_text(s):
            return s
    # 2) exact match on step_aliases
    for canon_step, aliases in (spec.step_aliases or {}).items():
        for alias in aliases:
            if step_n == norm_text(alias):
                return canon_step
    # 3) substring unique
    candidates: List[str] = []
    for s in spec.steps:
        sn = norm_text(s)
        # Only match when the full step name appears in the raw text (not the other way around)
        if sn in step_n:
            candidates.append(s)
    for canon_step, aliases in (spec.step_aliases or {}).items():
        for alias in aliases:
            an = norm_text(alias)
            # Only match when the full alias appears in the raw text (not the other way around)
            if an in step_n:
                candidates.append(canon_step)
    candidates = _unique(candidates)
    if len(candidates) == 1:
        return candidates[0]
    return None
