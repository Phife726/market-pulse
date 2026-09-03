"""The Insight schema — one home for what an article row means.

An *Insight* is the structured object the LLM returns per article and the row
the pipeline stores in `daily_intelligence`. It travels as a plain dict
(synthesis → Supabase → fetch → render), because that is the shape the
repository and the HTML renderer both rely on. This module does not change that
wire format; it concentrates the *knowledge* about it that used to be split
between two engines:

- the valid-value taxonomies (sentiment tag, commercial segment, signal type,
  recommended action),
- the clamp/default/DISCARD rules that turn a raw LLM dict into a storable row
  (``normalize``), and
- the readers that pull a field back off a stored row with its default applied
  (``effective_impact``, ``commercial_segment``, ``signal_type``), and
- the two readers of a stored *value* rather than a row (``source_domain``,
  ``parse_timestamp``) — see the note at their definition.

Before this module, ``ingestion_engine.synthesize_insight`` owned the clamping
and ``delivery_engine`` re-derived the same defaults in three private readers.
Deletion test: drop this module and that logic reappears in both engines.

Scope is the per-article Insight only. The macro-summary schema
(``dominant_condition`` / ``executive_bullets``) is a different structured output
and stays with ``generate_macro_summary``.
"""

import logging
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# --- Taxonomies -------------------------------------------------------------
# Constant value sets the LLM output is validated against. The matching segment
# and signal *descriptions* that steer the model live in market_pulse_config.yaml
# and feed the synthesis prompt; these frozensets are the membership gate.

VALID_SENTIMENT_TAGS: frozenset[str] = frozenset({"Negative", "Neutral", "Positive"})

VALID_COMMERCIAL_SEGMENTS: frozenset[str] = frozenset({
    "Healthcare", "Fibers",
    "Transportation - Automotive", "Transportation - Non-Automotive",
    "Transportation - Aerospace",
    "Industrial", "Building & Construction", "Packaging", "Engineered Resins",
    "Enterprise / Cross-Segment",
})

VALID_SIGNAL_TYPES: frozenset[str] = frozenset({
    "Competitive", "Customer", "Regulatory", "Sustainability",
    "Supply Chain", "Technology", "Macro", "Other",
})

VALID_ACTIONS: frozenset[str] = frozenset({
    "No action", "Monitor", "Flag to procurement",
    "Share with sales", "Escalate to leadership",
})

# --- Defaults & sentinels ---------------------------------------------------
DEFAULT_SEGMENT = "Enterprise / Cross-Segment"
DEFAULT_SIGNAL = "Other"
DEFAULT_TAG = "Neutral"
DEFAULT_ACTION = "Monitor"
DEFAULT_SCORE = 5
DISCARD = "DISCARD"

REQUIRED_KEYS: frozenset[str] = frozenset({
    "headline", "americhem_impact", "sentiment_score", "source_url", "entities_mentioned",
})


def is_discard(raw: dict) -> bool:
    """True when the model flagged this article as a false-positive to drop."""
    return raw.get("americhem_impact") == DISCARD


def _clamp_score(value, default: int = DEFAULT_SCORE) -> int:
    try:
        return max(1, min(10, int(value)))
    except (TypeError, ValueError):
        return default


def normalize(raw: dict) -> Optional[dict]:
    """Validate, clamp and default a raw LLM insight into a storable row.

    Mutates and returns ``raw``. Returns None if a required key is missing — the
    caller maps that to its own sentinel. Does not handle the DISCARD case; check
    ``is_discard`` first.
    """
    missing = REQUIRED_KEYS - raw.keys()
    if missing:
        logger.error("Insight missing required keys %s", missing)
        return None

    raw["sentiment_score"] = _clamp_score(raw.get("sentiment_score"))

    if raw.get("sentiment_tag") not in VALID_SENTIMENT_TAGS:
        raw["sentiment_tag"] = DEFAULT_TAG

    raw["americhem_impact_score"] = _clamp_score(raw.get("americhem_impact_score"))
    raw.setdefault("impact_rationale", "")

    seg = raw.get("commercial_segment")
    seg = seg.strip() if isinstance(seg, str) else ""
    raw["commercial_segment"] = seg if seg in VALID_COMMERCIAL_SEGMENTS else DEFAULT_SEGMENT

    sig = raw.get("signal_type")
    sig = sig.strip() if isinstance(sig, str) else ""
    raw["signal_type"] = sig if sig in VALID_SIGNAL_TYPES else DEFAULT_SIGNAL

    # Drop the legacy field if the model still returns it.
    raw.pop("strategic_segment", None)

    if not isinstance(raw["entities_mentioned"], list):
        raw["entities_mentioned"] = []
    raw.setdefault("source_publication", "")
    raw.setdefault("sentiment_rationale", "")
    raw.setdefault("article_summary", "")

    if raw.get("recommended_action") not in VALID_ACTIONS:
        raw["recommended_action"] = DEFAULT_ACTION

    return raw


# --- Readers ----------------------------------------------------------------
# Pull a field off a stored row with its default applied. Tolerant of malformed
# values — a bad row should degrade the field, not crash the delivery run.

def effective_impact(row: dict) -> int:
    """Routing score: americhem_impact_score (materiality) preferred, sentiment_score fallback."""
    score = row.get("americhem_impact_score")
    if score is not None:
        try:
            return int(score)
        except (TypeError, ValueError):
            logger.warning(
                "Invalid americhem_impact_score %r for row %r; falling back to sentiment_score.",
                score, row.get("source_url") or row.get("headline") or row.get("url_hash"),
            )

    fallback = row.get("sentiment_score")
    if fallback is not None:
        try:
            return int(fallback)
        except (TypeError, ValueError):
            logger.warning(
                "Invalid sentiment_score %r for row %r; using default score %d.",
                fallback, row.get("source_url") or row.get("headline") or row.get("url_hash"),
                DEFAULT_SCORE,
            )

    return DEFAULT_SCORE


def commercial_segment(row: dict) -> str:
    """Return commercial_segment if set; else the cross-segment default."""
    seg = (row.get("commercial_segment") or "").strip()
    return seg or DEFAULT_SEGMENT


def signal_type(row: dict) -> str:
    """Return signal_type if set on the row; else 'Other'."""
    sig = (row.get("signal_type") or "").strip()
    return sig or DEFAULT_SIGNAL


# --- Value readers ----------------------------------------------------------
# Readers of one stored *value* rather than a row: the callers hold the value
# already, and one of them reads it from more than one column (the appendix
# ranks published_at *or* created_at through the same parse), so a row reader
# would not serve them. Both are total — any value in, "" / None out.
#
# They live here, the lowest pure module, because their readers span layers a
# report-side home could not reach: `daily_intelligence_repo` (a seam a pure
# module may not import) shares `parse_timestamp`, and `prompts` shares
# `source_domain`. `ingestion_engine`'s own host read deliberately does NOT
# share this one — UNSCRAPABLE_HOSTS matches the `www.` form too.

#: The seconds fraction of an ISO timestamp. The date part carries no '.',
#: so the first match is always the fraction.
_ISO_FRACTION = re.compile(r"\.(\d+)")


def source_domain(url: object) -> str:
    """Registrable host of `url` minus a leading 'www.'; '' when the URL is
    empty, not a string, or unparseable. urlparse().hostname strips any :port
    and lowercases."""
    if not isinstance(url, str):
        return ""
    try:
        host = urlparse(url).hostname or ""
    except (ValueError, TypeError):
        return ""
    return host[4:] if host.startswith("www.") else host


def parse_timestamp(value: object) -> Optional[datetime]:
    """`value` as a datetime when it is an ISO-8601 string, else None (so a
    caller can tell 'no usable date' from a real one). A non-None result
    guarantees `value` was a string.

    Normalizes the two shapes Postgres/PostgREST emit that `fromisoformat`
    rejects before Python 3.11, which the pipeline still runs on: a trailing
    'Z', and a seconds fraction of any width other than 3 or 6 digits
    (Postgres strips trailing zeros, so '10:44:12.500000' comes back as
    '10:44:12.5'). Surrounding whitespace is tolerated. Pure: parsing reads
    no clock, and the parsed value keeps whatever offset it was written with."""
    if not isinstance(value, str) or not value.strip():
        return None
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    match = _ISO_FRACTION.search(s)
    if match:
        s = s[:match.start(1)] + match.group(1)[:6].ljust(6, "0") + s[match.end(1):]
    try:
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None
