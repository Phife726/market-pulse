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
"""
from typing import Optional

import insight
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
