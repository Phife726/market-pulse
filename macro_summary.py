"""Pure schema/validation + assembly for the once-per-run **Macro summary**.

The run-level twin of ``insight.py``: ``insight.normalize`` turns a raw
per-article LLM dict into a storable Insight row; ``assemble_macro_content``
turns the raw macro LLM dict into the storable macro-summary content fields.
Both are pure — no I/O, clock, or env reads. ``ingestion_engine`` keeps the
effectful half (prompt build, the LLM call, and the ``daily_summaries`` upsert)
in ``generate_macro_summary``.

The vocabulary these validators enforce is owned by ``prompts.py`` (the module
that renders it into the prompt text) and ``insight.py`` — imported, never
re-defined, so the prompt's promises and the validator's checks stay one
definition.

The module owns both directions of the schema, the way ``suppression_ledger``
owns both of the suppression taxonomy: ``assemble_macro_content`` is the write
face (raw LLM dict -> storable content fields) and ``MacroSummary`` the read
face (a stored ``daily_summaries`` row -> the typed value delivery, report
assembly and the renderer all consume).
"""
from dataclasses import dataclass
from typing import Optional

import insight
from suppression_ledger import SuppressionAccounting
from prompts import (
    VALID_MACRO_CONDITIONS,
    VALID_MACRO_DIRECTIONS,
    EXEC_BULLET_LABELS,
    MAX_EXECUTIVE_BULLET_CITATIONS,
    MAX_MACRO_OUTLOOK_SIGNALS,
)


def _clean_citation_ids(raw, valid_source_ids: frozenset[int]) -> list[int]:
    """Keep only int ids present in valid_source_ids: dedupe (order preserved),
    cap at MAX_EXECUTIVE_BULLET_CITATIONS. bool is excluded (it subclasses int).
    Any non-list / garbage input yields []."""
    if not isinstance(raw, list):
        return []
    out: list[int] = []
    for v in raw:
        if isinstance(v, bool) or not isinstance(v, int):
            continue
        if v not in valid_source_ids or v in out:
            continue
        out.append(v)
        if len(out) >= MAX_EXECUTIVE_BULLET_CITATIONS:
            break
    return out


def validate_executive_bullets(raw, valid_source_ids: frozenset[int] = frozenset()) -> Optional[list[dict]]:
    """Return the cleaned bullets list if valid; None otherwise (delivery falls
    back to prose).

    Valid shape: exactly 3 objects, with labels matching EXEC_BULLET_LABELS in
    order, and non-empty string body fields. Each returned bullet carries a
    cleaned citation_source_ids list (only ids in valid_source_ids survive;
    invalid ids are never stored).
    """
    if not isinstance(raw, list) or len(raw) != 3:
        return None
    cleaned: list[dict] = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            return None
        label = item.get("label")
        body = item.get("body")
        if label != EXEC_BULLET_LABELS[i]:
            return None
        if not isinstance(body, str) or not body.strip():
            return None
        cleaned.append({
            "label": label,
            "body": body.strip(),
            "citation_source_ids": _clean_citation_ids(item.get("citation_source_ids"), valid_source_ids),
        })
    return cleaned


def validate_macro_outlook(raw, valid_source_ids: frozenset[int]) -> Optional[dict]:
    """Validate the structured macro_outlook. Returns a cleaned
    {current_condition, signals:[...]} dict, or None (delivery renders no
    section) when the shape is invalid or no material signal survives.

    A signal survives only when every field is well-formed AND it cites at
    least one valid source id — the deterministic materiality gate that makes
    'source-grounded, no fabricated implications' a structural guarantee. The
    enums (VALID_MACRO_DIRECTIONS, insight.VALID_COMMERCIAL_SEGMENTS) are the
    same definitions the prompt promises."""
    if not isinstance(raw, dict):
        return None
    current = raw.get("current_condition")
    if not isinstance(current, str) or not current.strip():
        return None
    signals_raw = raw.get("signals")
    if not isinstance(signals_raw, list):
        return None

    cleaned: list[dict] = []
    for sig in signals_raw:
        if not isinstance(sig, dict):
            continue
        indicator = sig.get("indicator")
        if not isinstance(indicator, str) or not indicator.strip():
            continue
        if sig.get("direction") not in VALID_MACRO_DIRECTIONS:
            continue
        implication = sig.get("americhem_implication")
        if not isinstance(implication, str) or not implication.strip():
            continue
        segments_raw = sig.get("affected_segments")
        if not isinstance(segments_raw, list):
            continue
        segments = [s for s in segments_raw if s in insight.VALID_COMMERCIAL_SEGMENTS]
        if not segments:
            continue
        citations = _clean_citation_ids(sig.get("citation_source_ids"), valid_source_ids)
        if not citations:  # materiality gate — an uncitable signal is dropped
            continue
        cleaned.append({
            "indicator": indicator.strip(),
            "direction": sig["direction"],
            "americhem_implication": implication.strip(),
            "affected_segments": segments,
            "citation_source_ids": citations,
        })
        if len(cleaned) >= MAX_MACRO_OUTLOOK_SIGNALS:
            break

    if not cleaned:
        return None
    return {"current_condition": current.strip(), "signals": cleaned}


def assemble_macro_content(parsed: dict, *, source_pack: list[dict], article_count: int) -> dict:
    """Turn the raw macro LLM dict into the storable macro-summary content
    fields — the pure, run-level twin of ``insight.normalize``.

    Returns the content half of the ``daily_summaries`` row (``dominant_condition``,
    ``executive_bullets``, ``macro_outlook``, ``executive_sources``,
    ``executive_summary``, ``macro_sentiment``). The caller merges it onto the
    accounting row and upserts; it owns no I/O, clock, or env reads.

    ``source_pack`` is the macro prompt's citation index; its ids are the only
    valid citation targets, and ``executive_sources`` is packed in pack order.
    ``article_count`` drives the dominant-condition fallback.
    """
    valid_source_ids = frozenset(s["id"] for s in source_pack)

    # dominant_condition: keep a valid enum value, else fall back by volume.
    cond_raw = parsed.get("dominant_condition")
    if cond_raw not in VALID_MACRO_CONDITIONS:
        cond = "Low Signal" if article_count < 3 else "Mixed / Watch"
    else:
        cond = cond_raw

    # executive_bullets (cleans per-bullet citation_source_ids against the pack).
    bullets = validate_executive_bullets(parsed.get("executive_bullets"), valid_source_ids)

    # Low Signal: force the third bullet body.
    if bullets is not None and cond == "Low Signal":
        bullets[2] = {
            "label": EXEC_BULLET_LABELS[2],
            "body": "No action required.",
            "citation_source_ids": [],
        }

    # Structured macro outlook (None -> delivery renders no section).
    macro_outlook = validate_macro_outlook(parsed.get("macro_outlook"), valid_source_ids)

    # executive_sources: pack entries cited by at least one surviving bullet OR
    # macro-outlook signal — the union, so every rendered citation id (in either
    # section) resolves against one shared numbering space.
    cited_ids: set[int] = set()
    for b in bullets or []:
        cited_ids.update(b["citation_source_ids"])
    for sig in (macro_outlook["signals"] if macro_outlook else []):
        cited_ids.update(sig["citation_source_ids"])
    executive_sources = [s for s in source_pack if s["id"] in cited_ids]

    # Legacy executive_summary string for backward compat.
    if bullets is not None:
        executive_summary = " ".join(f"{b['label']}: {b['body']}" for b in bullets)
    else:
        executive_summary = "Macro summary unavailable today."

    return {
        "dominant_condition": cond,
        "executive_bullets": bullets,
        "macro_outlook": macro_outlook,
        "executive_sources": executive_sources,
        "executive_summary": executive_summary,
        "macro_sentiment": cond,
    }


# ---------------------------------------------------------------------------
# MacroSummary — the read face of a stored daily_summaries row
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MacroSummary:
    """One stored ``daily_summaries`` row, read defensively and once.

    The read face of the schema ``assemble_macro_content`` writes. Delivery
    converts at the fetch — ``resolve_summary_row`` returns this, not a dict —
    so nothing downstream (report assembly, the report model, the renderer)
    holds raw jsonb, and there is no second place for a ``.get()`` on this
    shape to reappear. ``None`` still means *no row found*, which delivery logs
    on; a row that exists but carries no brief is ``has_content is False``.

    Every reader is defensive because legacy and half-written rows genuinely
    reach the renderer: pre-relevance-upgrade rows carry only
    ``executive_summary``/``macro_sentiment``, and a zero-yield run persists an
    **accounting-only row** (counts, no content). Contents were validated at
    ingestion; this is the read that assumes none of it survived.

    ``screened_count``/``surfaced_count`` stay ``Optional`` on purpose: these
    are the row's **recorded counts** (see CONTEXT.md), and the value types the
    read without picking a fallback — an absent count is shown as absent (the
    QA block's ``?``, the subtitle's omitted clause), never replaced with a
    different quantity.
    """
    legacy_text: str = ""
    # The row's two condition columns are kept apart: `condition` composes them
    # for display, while `has_content` must ask only about the structured one —
    # a row carrying nothing but the legacy macro_sentiment is not a brief.
    dominant_condition: str = ""
    legacy_condition: str = ""
    bullets: Optional[list[dict]] = None
    outlook: Optional[dict] = None
    sources: tuple[dict, ...] = ()
    screened_count: Optional[int] = None
    surfaced_count: Optional[int] = None
    # Frozen, so one shared default instance is safe.
    suppression: SuppressionAccounting = SuppressionAccounting()

    @classmethod
    def from_row(cls, row: Optional[dict]) -> "MacroSummary":
        row = row or {}
        return cls(
            legacy_text=row.get("executive_summary") or "",
            dominant_condition=row.get("dominant_condition") or "",
            legacy_condition=row.get("macro_sentiment") or "",
            bullets=_read_bullets(row),
            outlook=_read_outlook(row),
            sources=tuple(row.get("executive_sources") or ()),
            screened_count=row.get("screened_count"),
            surfaced_count=row.get("surfaced_count"),
            suppression=SuppressionAccounting.from_row(row),
        )

    @property
    def condition(self) -> str:
        """The one condition rule: the structured column, else the legacy one,
        else "". It was spelled twice in renderer.py, 130 lines apart."""
        return self.dominant_condition or self.legacy_condition or ""

    @property
    def has_content(self) -> bool:
        """True when the row carries a renderable brief. False for the
        accounting-only row a zero-yield run persists (issue #43) — the first
        ranking key of delivery's test-mode production fallback, which must
        never let an accounting-only row shadow a content-full one.

        Deliberately asks `dominant_condition`, NOT `condition`: the legacy
        `macro_sentiment` alone is a tone label, not a brief, and counting it
        would let a sentiment-only row outrank a real one in the QA fallback."""
        return bool(self.bullets or self.legacy_text or self.outlook
                    or self.dominant_condition)


def _read_bullets(row: dict) -> Optional[list[dict]]:
    """executive_bullets when it is a non-empty list of dict bullets, else None.
    A legacy row whose bullets are a list of strings would otherwise render
    blank "* :" rows instead of falling back to the prose."""
    bullets = row.get("executive_bullets")
    if isinstance(bullets, list) and bullets and all(isinstance(b, dict) for b in bullets):
        return bullets
    return None


def _read_outlook(row: dict) -> Optional[dict]:
    """A renderable macro_outlook: a dict with a non-empty current_condition and
    at least one signal. Anything else (missing, None, malformed, empty signals)
    is None, so the renderer shows no section. Signal *contents* were validated
    at ingestion by validate_macro_outlook; this is the defensive read of a
    stored row. Signals are sliced to MAX_MACRO_OUTLOOK_SIGNALS so rows stored
    before a cap reduction render at most the current cap.

    The report's display-segment remap is deliberately NOT applied here: that is
    display policy, not schema, and it stays in report.py."""
    outlook = row.get("macro_outlook")
    if not isinstance(outlook, dict):
        return None
    current = outlook.get("current_condition")
    signals = outlook.get("signals")
    if not isinstance(current, str) or not current.strip():
        return None
    if not isinstance(signals, list) or not signals:
        return None
    return {**outlook, "signals": signals[:MAX_MACRO_OUTLOOK_SIGNALS]}
