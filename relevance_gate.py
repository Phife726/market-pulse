"""Pure ZoomInfo relevance gate.

A targeted false-positive suppressor (NOT a second entity resolver). ZoomInfo
News candidates are already linked to a company by id; this gate drops a
candidate only when a curated `exclude_term` appears AND no identity term
(canonical name / identity terms / manual aliases) rescues it. Absence of
identity text alone never drops.

`evaluate` is pure (no I/O). `load_target_metadata` is the only I/O and swallows
read errors so a missing/bad companion file silently disables the gate rather
than crashing ingestion.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

GATE_REASON = "zoominfo_company_mismatch"


@dataclass(frozen=True)
class GateDecision:
    """Outcome of evaluating one candidate. `drop=True` means suppress it."""
    drop: bool
    reason: Optional[str] = None
    matched_exclude: Optional[str] = None
    matched_identity: Optional[str] = None


def _term_matches(text: str, term: str) -> bool:
    """True if `term` appears in `text` as a whole word/phrase, case-insensitive.

    Internal whitespace runs in `term` match one-or-more whitespace in `text`,
    so multi-word phrases survive irregular spacing. Word boundaries prevent
    partial matches (e.g. 'casino' does not match 'casinos')."""
    term = (term or "").strip()
    if not term:
        return False
    parts = [re.escape(p) for p in term.split()]
    pattern = r"\b" + r"\s+".join(parts) + r"\b"
    return re.search(pattern, text, re.IGNORECASE) is not None


def _identity_terms(record: dict) -> list[str]:
    """canonical_name + company_identity_terms + manual_aliases, non-empty,
    de-duplicated case-insensitively with first-occurrence (canonical-first) order."""
    terms: list[str] = []
    seen: set[str] = set()

    def _add(term: object) -> None:
        if isinstance(term, str) and term.strip():
            key = term.strip().lower()
            if key not in seen:
                seen.add(key)
                terms.append(term.strip())

    _add(record.get("canonical_name"))
    for key in ("company_identity_terms", "manual_aliases"):
        for term in (record.get(key) or []):
            _add(term)
    return terms


def evaluate(*, title: str, description: str, record: dict) -> GateDecision:
    """Decide whether a ZoomInfo candidate is an obvious company mismatch.

    Rule: identity rescue first (keep), then exclude hit (drop), else keep.
    """
    text = f"{title or ''} {description or ''}"

    for term in _identity_terms(record):
        if _term_matches(text, term):
            return GateDecision(drop=False, matched_identity=term)

    for term in (record.get("exclude_terms") or []):
        if isinstance(term, str) and _term_matches(text, term):
            return GateDecision(drop=True, reason=GATE_REASON, matched_exclude=term)

    return GateDecision(drop=False)


def load_target_metadata(path: str = "target_metadata.yaml") -> dict:
    """Return {target_key: record} from the companion file.

    Reads swallow exceptions and return {} — a missing or unparseable file
    silently disables the gate; ingestion never crashes on it."""
    try:
        with open(path, "r") as fh:
            data = yaml.safe_load(fh) or {}
    except (OSError, yaml.YAMLError) as exc:
        logger.warning(
            "relevance gate: could not load %s (%s) — gate disabled", path, exc
        )
        return {}
    if not isinstance(data, dict):
        # Syntactically valid YAML with a non-mapping root (e.g. a list or
        # scalar from a bad edit) — treat as malformed and disable the gate
        # rather than raising AttributeError on .get() and crashing ingestion.
        return {}
    targets = data.get("targets")
    return targets if isinstance(targets, dict) else {}
