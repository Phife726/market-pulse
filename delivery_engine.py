import html
import logging
import os
import random
import time
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlparse

import requests
import yaml

from suppression_ledger import SuppressionLedger, label_for
from daily_intelligence_repo import _repo
from llm import _llm
from insight import (
    effective_impact as _effective_impact,
    commercial_segment as _commercial_segment_of,
    signal_type as _signal_type_of,
)
import scoring
from scoring import Scoring, tier as _alert_tier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Americhem brand constants
# ---------------------------------------------------------------------------

_BRAND_NAVY       = "#1B3A6B"
_BRAND_NAVY_DARK  = "#152E56"
_BRAND_GREEN      = "#7FB069"
_BRAND_AMBER      = "#D97706"
_LOGO_URL = (
    "https://www.americhem.com/wp-content/uploads/2025/07/logo-header.webp"
)

# ---------------------------------------------------------------------------
# Test-mode banner row (inserted into the email HTML when MARKET_PULSE_RUN_MODE=test)
# ---------------------------------------------------------------------------

_TEST_BANNER_ROW = (
    f'<tr><td style="background-color:{_BRAND_AMBER};padding:8px 32px;font-size:11px;'
    f'font-weight:700;letter-spacing:1.5px;color:#ffffff;'
    f'font-family:Arial,sans-serif;text-transform:uppercase;">'
    f'TEST RUN · Jason-only QA output — not for distribution'
    f'</td></tr>'
)

# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

_MP_CONFIG: Optional[dict] = None


def _load_mp_config() -> dict:
    """Load market_pulse_config.yaml once; return cached result on repeat calls."""
    global _MP_CONFIG
    if _MP_CONFIG is None:
        try:
            with open("market_pulse_config.yaml", "r") as fh:
                _MP_CONFIG = yaml.safe_load(fh) or {}
        except Exception as exc:
            logger.warning("Could not load market_pulse_config.yaml — using defaults: %s", exc)
            _MP_CONFIG = {}
    return _MP_CONFIG


# ---------------------------------------------------------------------------
# Task 10: Commercial segment grouping + new section renderer
# ---------------------------------------------------------------------------

def _group_by_commercial_segment(items: list[dict]) -> dict[str, list[dict]]:
    """Bucket items by commercial_segment; rows without one default to Enterprise / Cross-Segment."""
    from collections import defaultdict
    buckets: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        buckets[_commercial_segment_of(item)].append(item)
    return dict(buckets)


def _render_meta_strip(item: dict) -> str:
    """Return the inline meta strip HTML span: 'Impact: X/10 · Tag · Signal: Y · [CRITICAL]'."""
    score = item.get("americhem_impact_score")
    tag = item.get("sentiment_tag") or ""

    if score is not None and tag:
        score_html = (
            f'<span style="color:{_BRAND_NAVY};font-weight:600;">'
            f'Impact: {int(score)}/10</span>'
        )
        tag_color = _SENTIMENT_TAG_COLORS.get(tag, "#6B7280")
        tag_html = (
            f'<span style="color:#9CA3AF;">&nbsp;&#9679;&nbsp;</span>'
            f'<span style="color:{tag_color};font-weight:600;">{tag}</span>'
        )
        signal = (item.get("signal_type") or "").strip()
        signal_html = (
            f'<span style="color:#9CA3AF;">&nbsp;&#9679;&nbsp;Signal: {signal}</span>'
            if signal else ""
        )
    else:
        # Legacy row: use sentiment_score for the score display.
        legacy_score = item.get("sentiment_score") or 5
        score_word, score_color = _sentiment_word(int(legacy_score))
        score_html = (
            f'<span style="color:{score_color};font-weight:600;">{score_word}</span>'
            f'<span style="color:#9CA3AF;">&nbsp;&#9679;&nbsp;Score: {legacy_score}/10</span>'
        )
        tag_html = ""
        signal_html = ""

    # CRITICAL badge for legacy low-sentiment rows.
    critical_html = ""
    if scoring.is_legacy_critical(item):
        critical_html = (
            '<span style="color:#9CA3AF;">&nbsp;&#9679;&nbsp;</span>'
            '<span style="color:#DC2626;font-weight:700;">CRITICAL</span>'
        )

    return f'{score_html}{tag_html}{signal_html}{critical_html}'


def _render_card(item: dict) -> str:
    """Render one article card row: meta strip, linked headline, "So what".

    This is the card the email actually ships (per the 2026-05-21 commercial-brief
    redesign). The segment is the block header, so it is not repeated in the card;
    `recommended_action` and `impact_rationale` are deliberately not shown here —
    the action is consumed by the suppression policy, not the reader."""
    meta = _render_meta_strip(item)
    headline = item.get("headline", "")
    source_url = item.get("source_url", "#")
    americhem_impact = item.get("americhem_impact", "")
    so_what_html = (
        f'<p style="margin:4px 0 0 0;font-size:13px;color:#374151;'
        f"font-family:Georgia,'Times New Roman',serif;line-height:1.55;\">"
        f'<strong style="color:{_BRAND_NAVY};">So what:</strong> {americhem_impact}</p>'
        if americhem_impact else ""
    )
    return (
        f'<tr><td style="padding:6px 0 10px 0;">'
        f'<p style="margin:0 0 4px 0;font-size:11px;color:#6B7280;'
        f'font-family:Arial,sans-serif;">{meta}</p>'
        f'<a href="{source_url}" style="font-size:14px;font-weight:700;'
        f'color:{_BRAND_NAVY};font-family:Arial,sans-serif;'
        f'text-decoration:none;line-height:1.35;">{headline}</a>'
        f'{so_what_html}'
        f'</td></tr>'
    )


def _render_segment_watch_section(
    groups: dict[str, list[dict]],
    synthesis: dict[str, str],
) -> str:
    """Render the Commercial Segment Watch section.

    Each commercial segment becomes its own block. Within a segment, if a synthesis
    paragraph exists, it appears above the article cards.
    """
    if not groups:
        return ""

    ordered = sorted(
        groups.items(),
        key=lambda kv: -max(int(a.get("americhem_impact_score") or a.get("sentiment_score") or 0)
                            for a in kv[1]),
    )

    blocks_html = ""
    for segment_label, articles in ordered:
        para = synthesis.get(segment_label, "")
        para_html = (
            f'<p style="margin:0 0 10px 0;font-size:13px;color:#1a2a45;'
            f"font-family:Georgia,'Times New Roman',serif;line-height:1.65;\">"
            f'{para}</p>'
        ) if para else ""

        articles_sorted = sorted(
            articles,
            key=lambda x: -int(x.get("americhem_impact_score") or x.get("sentiment_score") or 0),
        )
        cards_html = "".join(_render_card(art) for art in articles_sorted)

        blocks_html += (
            f'<tr><td style="padding:18px 0 0 0;">'
            f'<p style="margin:0 0 8px 0;font-size:12px;font-weight:700;'
            f'letter-spacing:1px;text-transform:uppercase;color:{_BRAND_NAVY};'
            f'font-family:Arial,sans-serif;">{segment_label}</p>'
            f'{para_html}'
            f'<table width="100%" cellpadding="0" cellspacing="0" border="0">{cards_html}</table>'
            f'</td></tr>'
        )

    return f"""
      <tr>
        <td style="padding:24px 32px 4px 32px;">
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="padding-bottom:10px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="font-size:11px;font-weight:700;letter-spacing:1.5px;
                                text-transform:uppercase;color:{_BRAND_NAVY};
                                font-family:Arial,sans-serif;white-space:nowrap;
                                padding-right:12px;">
                      COMMERCIAL SEGMENT WATCH
                    </td>
                    <td style="border-bottom:1px solid {_BRAND_NAVY};width:100%;"></td>
                  </tr>
                </table>
              </td>
            </tr>
            {blocks_html}
          </table>
        </td>
      </tr>"""


# ---------------------------------------------------------------------------
# Delivery suppression guardrail (Task 9)
# ---------------------------------------------------------------------------

from rapidfuzz.fuzz import token_sort_ratio as _token_sort_ratio

def _matches_any_pattern(haystack: str, patterns: list[str]) -> bool:
    """Case-insensitive substring match: True if any pattern appears in haystack."""
    h = (haystack or "").lower()
    return any(p.lower() in h for p in patterns or [])


def _contains_any_term(text: str, terms: list[str]) -> bool:
    """True if any of `terms` appears in `text` (case-insensitive substring)."""
    t = (text or "").lower()
    return any(term.lower() in t for term in terms or [])


def _apply_delivery_suppression(
    rows: list[dict],
    config: dict,
) -> tuple[list[dict], SuppressionLedger]:
    """Run the deterministic seven-rule guardrail over fetched rows.

    Returns (kept_rows, ledger). First matching rule wins; each suppressed
    row is counted once and contributes at most one sample (deduped).
    """
    sup_cfg = config.get("delivery_suppression") or {}
    ledger = SuppressionLedger.for_delivery()
    kept: list[dict] = []
    kept_headlines: list[str] = []

    threshold = int(sup_cfg.get("headline_duplicate_threshold", 90))
    enterprise_min_impact = int(sup_cfg.get("enterprise_min_impact", 7))
    override_action = sup_cfg.get("job_posting_override_action", "Escalate to leadership")

    product_patterns   = sup_cfg.get("url_patterns_product_listing", [])
    job_patterns       = sup_cfg.get("url_patterns_job_posting", [])
    market_patterns    = sup_cfg.get("title_patterns_generic_market_report", [])
    color_terms        = sup_cfg.get("color_terms", [])
    plastics_terms     = sup_cfg.get("plastics_relevance_terms", [])

    for row in rows:
        url = row.get("source_url", "") or ""
        headline = row.get("headline", "") or ""
        americhem_impact = row.get("americhem_impact", "") or ""
        entities = row.get("entities_mentioned") or []
        entities_text = " ".join(str(e) for e in entities)
        action = row.get("recommended_action", "")

        # Rule 1: Enterprise / Cross-Segment with low impact
        if sup_cfg.get("enable_enterprise_low_impact", True):
            segment = _commercial_segment_of(row)
            score = int(row.get("americhem_impact_score") or 0)
            if segment == "Enterprise / Cross-Segment" and score < enterprise_min_impact:
                ledger = ledger.record(
                    "enterprise_cross_segment_low_impact",
                    url=url,
                    title=headline,
                )
                continue

        # Rule 2: Product listing URL
        if sup_cfg.get("enable_product_listing", True) and _matches_any_pattern(url, product_patterns):
            ledger = ledger.record("product_listing", url=url, title=headline)
            continue

        # Rule 3: Job posting URL (unless escalated)
        if sup_cfg.get("enable_job_posting", True) and _matches_any_pattern(url, job_patterns):
            if action != override_action:
                ledger = ledger.record("job_posting", url=url, title=headline)
                continue

        # Rule 4: Generic market report title with empty entities
        if sup_cfg.get("enable_generic_market_report", True):
            if _matches_any_pattern(headline, market_patterns) and not entities:
                ledger = ledger.record("generic_market_report", url=url, title=headline)
                continue

        # Rule 5: Unrelated color result
        if sup_cfg.get("enable_unrelated_color_result", True):
            if _contains_any_term(headline, color_terms):
                # Check headline and entities only — not americhem_impact, which may
                # contain negating language like "No plastics relevance." that would
                # cause a false substring match on "plastic".
                relevance_haystack = f"{headline} {entities_text}"
                if not _contains_any_term(relevance_haystack, plastics_terms):
                    ledger = ledger.record("unrelated_color_result", url=url, title=headline)
                    continue

        # Rule 6: Exact duplicate headline (case-insensitive)
        if sup_cfg.get("enable_duplicate_headline", True):
            if headline and any(h.lower() == headline.lower() for h in kept_headlines):
                ledger = ledger.record("duplicate_headline", url=url, title=headline)
                continue

        # Rule 7: Semantic duplicate headline
        # Lowercase before comparison — rapidfuzz 3.x no longer auto-lowercases,
        # so case differences in proper nouns would otherwise deflate the score.
        if sup_cfg.get("enable_semantic_duplicate_headline", True) and kept_headlines and headline:
            hl_lower = headline.lower()
            scores = [_token_sort_ratio(hl_lower, h.lower()) for h in kept_headlines]
            if scores and max(scores) >= threshold:
                ledger = ledger.record("semantic_duplicate_headline", url=url, title=headline)
                continue

        kept.append(row)
        if headline:
            kept_headlines.append(headline)

    return kept, ledger


def _config_int(cfg: dict, key: str, default: int) -> int:
    """Read an int from a config sub-dict, coercing strings and warning on bad values."""
    try:
        return int(cfg.get(key, default))
    except (TypeError, ValueError):
        logger.warning("Invalid config value for reporting.%s; using %d", key, default)
        return default


# ---------------------------------------------------------------------------
# Run-mode detection
# ---------------------------------------------------------------------------

def _run_mode() -> str:
    """Return 'test' when MARKET_PULSE_RUN_MODE=test (case-insensitive), else 'production'."""
    return "test" if os.environ.get("MARKET_PULSE_RUN_MODE", "").strip().lower() == "test" else "production"


def _is_test_mode() -> bool:
    """Return True when MARKET_PULSE_RUN_MODE=test (case-insensitive)."""
    return _run_mode() == "test"


# ---------------------------------------------------------------------------
# QA suppression-summary (test-mode only)
# ---------------------------------------------------------------------------



def _render_qa_debug_section(macro_summary: Optional[dict]) -> str:
    """Render the QA suppression summary block. Caller is responsible for gating
    on test mode; this function does not check MARKET_PULSE_RUN_MODE itself."""
    if not macro_summary:
        return ""

    screened = macro_summary.get("screened_count")
    surfaced = macro_summary.get("surfaced_count")
    breakdown = macro_summary.get("suppression_breakdown") or {}
    samples = macro_summary.get("suppression_samples") or []

    suppressed_total = sum(int(v) for v in breakdown.values())

    rows_html = ""
    # Stable display order: ingestion-side first, then delivery-side.
    display_order = [
        "duplicate_url", "semantic_duplicate", "llm_discard", "scrape_failed",
        "zoominfo_company_mismatch",
        "below_impact_threshold", "weak_relevance",
        "duplicate_headline", "semantic_duplicate_headline",
        "product_listing", "job_posting", "generic_market_report",
        "unrelated_color_result", "enterprise_cross_segment_low_impact",
    ]
    for code in display_order:
        if code in breakdown:
            label = label_for(code)
            rows_html += (
                f'<tr><td style="padding:2px 0;font-size:12px;color:#374151;'
                f'font-family:Arial,sans-serif;">'
                f'&nbsp;&nbsp;{label}'
                f'</td><td align="right" style="padding:2px 0;font-size:12px;'
                f'color:#374151;font-family:Arial,sans-serif;">{breakdown[code]}</td></tr>'
            )

    samples_html = ""
    for s in samples[-10:]:
        reason_code = s.get("reason", "")
        reason_label = label_for(reason_code)
        title = s.get("title", "")
        url = s.get("url", "")
        samples_html += (
            f'<tr><td style="padding:2px 0;font-size:11px;color:#6B7280;'
            f'font-family:monospace;">'
            f'[{reason_label}] "{title}" — {url}'
            f'</td></tr>'
        )

    return f"""
      <tr>
        <td style="padding:24px 32px 4px 32px;">
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="padding-bottom:10px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="font-size:11px;font-weight:700;letter-spacing:1.5px;
                                text-transform:uppercase;color:#9CA3AF;
                                font-family:Arial,sans-serif;white-space:nowrap;
                                padding-right:12px;">
                      QA &middot; Suppression Summary
                    </td>
                    <td style="border-bottom:1px solid #E5E7EB;width:100%;"></td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td>
                <p style="margin:0 0 8px 0;font-size:12px;color:#374151;
                           font-family:Arial,sans-serif;">
                  Screened: {screened if screened is not None else '?'} &nbsp;&middot;&nbsp;
                  Surfaced: {surfaced if surfaced is not None else '?'} &nbsp;&middot;&nbsp;
                  Suppressed: {suppressed_total}
                </p>
                <p style="margin:8px 0 4px 0;font-size:11px;color:#6B7280;
                           font-family:Arial,sans-serif;text-transform:uppercase;
                           letter-spacing:1px;">
                  By reason
                </p>
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  {rows_html}
                </table>
                <p style="margin:12px 0 4px 0;font-size:11px;color:#6B7280;
                           font-family:Arial,sans-serif;text-transform:uppercase;
                           letter-spacing:1px;">
                  Last 10 suppressed items
                </p>
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  {samples_html}
                </table>
              </td>
            </tr>
          </table>
        </td>
      </tr>"""


# ---------------------------------------------------------------------------
# Email delivery retry constants
# ---------------------------------------------------------------------------

_MAX_EMAIL_ATTEMPTS    = 5
_EMAIL_BASE_DELAY_S    = 2.0
_RESEND_API_URL        = "https://api.resend.com/emails"
_TRANSIENT_HTTP_CODES  = {429, 500, 502, 503, 504}

# ---------------------------------------------------------------------------
# 1. Data fetch — with Monday 72-hour lookback
# ---------------------------------------------------------------------------

def fetch_todays_intelligence() -> list[dict]:
    is_monday = datetime.now().weekday() == 0
    lookback_hours = 72 if is_monday else 24
    if is_monday:
        logger.info("Monday detected — extending lookback to 72 hours.")
    rows = _repo().fetch_recent(hours=lookback_hours)
    logger.info(
        "Fetched %d intelligence record(s) (lookback: %dh).",
        len(rows), lookback_hours,
    )
    return rows


def fetch_macro_summary() -> dict | None:
    from datetime import date
    min_run_date = (date.today() - timedelta(days=1)).isoformat()
    summary = _repo().fetch_latest_summary(
        run_mode=_run_mode(),
        min_date=min_run_date,
    )
    if summary is None:
        logger.warning("No macro summary found for run_date >= %s.", min_run_date)
    return summary


def _update_delivery_summary_counts(
    *,
    surfaced_count: int,
    ledger: SuppressionLedger,
) -> None:
    """Update today's daily_summaries row with delivery-side surfaced count
    and merged suppression accounting. Idempotent on same-day retry — the
    merge semantics live in SuppressionLedger.merge_with().

    Non-critical: a failed write is logged but does not raise. Keeps the
    email-sending path resilient to transient Supabase outages."""
    from datetime import date as _date
    today = _date.today().isoformat()
    run_mode = _run_mode()
    try:
        prior_row = _repo().require_delivery_state(run_date=today, run_mode=run_mode)
        prior = SuppressionLedger.from_row("delivery", prior_row)
        merged = ledger.merge_with(prior)
        _repo().update_delivery_counts(
            run_date=today,
            run_mode=run_mode,
            surfaced_count=surfaced_count,
            ledger_row=merged.to_row(),
        )
    except Exception as exc:
        logger.warning("Failed to update delivery counts on daily_summaries: %s", exc)


# ---------------------------------------------------------------------------
# 2. Thematic synthesis
# ---------------------------------------------------------------------------

# Cross-reference: an identical-body constant lives in ingestion_engine.py.
# Both prompts are gated in CI by tests that assert the same anchor substrings;
# if you reword this, reword the ingestion_engine.py copy in lockstep.
_ENGLISH_OUTPUT_RULE = (
    "All human-readable generated strings must be written in clear business English, "
    "regardless of the source article's language. Translate non-English source "
    "content into English. Preserve proper nouns — company names, product names, "
    "brand names, source publications, locations, URLs, and quoted legal or product "
    "identifiers — in their original form when translation would reduce precision. "
    "Enum/taxonomy fields must use the configured English labels exactly."
)


def synthesize_thematic_paragraphs(
    groups: dict[str, list[dict]],
) -> dict[str, str]:
    """Generate one synthesis paragraph per category group via OpenAI.

    Args:
        groups: Dict of {category: [articles]} — only groups with 2+ articles.

    Returns:
        Dict of {category: synthesis_paragraph}. Returns {} on any error so the
        caller can fall back to bullets-only rendering without blocking delivery.
    """
    if not groups:
        return {}

    lines: list[str] = []
    for category, articles in groups.items():
        lines.append(f"CATEGORY: {category}")
        for art in articles:
            impact_score = _effective_impact(art)
            tag = art.get("sentiment_tag") or ""
            entities = art.get("entities_mentioned") or []
            entity = entities[0] if entities else (art.get("commercial_segment") or art.get("category") or "Unknown")
            americhem_impact = art.get("americhem_impact", "")
            tag_suffix = f" | {tag}" if tag else ""
            lines.append(f"- [{entity} | impact:{impact_score}/10{tag_suffix}] {americhem_impact}")
        lines.append("")

    grouped_text = "\n".join(lines).strip()

    system_prompt = (
        f"OUTPUT LANGUAGE:\n{_ENGLISH_OUTPUT_RULE}\n\n"
        "You are a market intelligence analyst for Americhem, a specialty plastics compounder.\n\n"
        "For each CATEGORY block below, write exactly one synthesis paragraph (2–3 sentences).\n"
        "The paragraph must:\n"
        "- Identify the shared trend or structural driver across the listed signals\n"
        "- Explicitly state the implication for Americhem's supply chain, demand pipeline, or margin\n"
        "- Be written for a senior executive who will act on it — no hedging, no filler\n\n"
        "Return valid JSON with category names as keys and synthesis paragraphs as values.\n"
        "Use the exact category names provided. Do not invent categories.\n"
        "Only include categories that appear in the input."
    )

    result = _llm().complete_json(
        system=system_prompt,
        user=grouped_text,
        context="thematic synthesis",
    )
    if result is None:
        logger.error("Thematic synthesis failed — falling back to bullets-only.")
        return {}
    logger.info("Thematic synthesis complete — %d categories.", len(result))
    return result


# ---------------------------------------------------------------------------
# 3. HTML generation helpers
# ---------------------------------------------------------------------------

def _safe_http_url(url: Optional[str]) -> str:
    """Return url only when its scheme is http/https; otherwise ''. Guards
    against javascript:/data: and malformed values being placed into href."""
    if not isinstance(url, str) or not url:
        return ""
    try:
        scheme = urlparse(url).scheme.lower()
    except (ValueError, TypeError):
        return ""
    return url if scheme in ("http", "https") else ""


def _citation_display_map(bullets: list[dict], sources: Optional[list[dict]]) -> dict[int, int]:
    """Map raw cited source id -> sequential display number (1..N), ordered by
    first appearance across bullets. Only ids that have a matching source entry
    are numbered, so legacy rows (no executive_sources) yield an empty map."""
    src_ids = {s["id"] for s in (sources or []) if isinstance(s, dict) and "id" in s}
    order: list = []
    for b in bullets or []:
        if not isinstance(b, dict):
            continue
        for cid in b.get("citation_source_ids") or []:
            if cid in src_ids and cid not in order:
                order.append(cid)
    return {cid: n for n, cid in enumerate(order, start=1)}


def _render_citation_marker(cited_ids: Optional[list], src_by_id: dict, display_map: dict) -> str:
    """Grouped inline citation, e.g. [1, 2]. Each number links to its source URL
    (http/https only; otherwise plain text). Returns '' when nothing to show."""
    parts: list[str] = []
    for cid in cited_ids or []:
        if cid not in display_map:
            continue
        n = display_map[cid]
        url = _safe_http_url((src_by_id.get(cid) or {}).get("url"))
        if url:
            safe = html.escape(url, quote=True)
            parts.append(
                f'<a href="{safe}" title="{safe}" '
                f'style="color:{_BRAND_NAVY};text-decoration:none;">{n}</a>'
            )
        else:
            parts.append(str(n))
    if not parts:
        return ""
    inner = ", ".join(parts)
    return (
        f'&nbsp;<span style="font-size:10px;color:{_BRAND_NAVY};'
        f'vertical-align:super;">[{inner}]</span>'
    )


def _render_executive_bullets(bullets: list[dict], sources=None, display_map=None) -> str:
    """Render the 3-bullet executive summary body, each bullet followed by its
    grouped inline citation marker when it has resolvable cited sources.

    sources/display_map default to empty so legacy callers (and legacy rows with
    no citations) render exactly as before, with no markers.
    """
    sources = sources or []
    display_map = display_map or {}
    src_by_id = {s["id"]: s for s in sources if isinstance(s, dict) and "id" in s}
    items_html = ""
    for b in bullets:
        label = html.escape(b.get("label", "") if isinstance(b, dict) else "")
        body = html.escape(b.get("body", "") if isinstance(b, dict) else "")
        cited = b.get("citation_source_ids", []) if isinstance(b, dict) else []
        marker = _render_citation_marker(cited, src_by_id, display_map)
        items_html += (
            f'<tr><td style="padding:2px 0;font-size:13px;color:#1a2a45;'
            f"font-family:Georgia,'Times New Roman',serif;line-height:1.55;\">"
            f'&bull;&nbsp;<strong>{label}:</strong> {body}{marker}'
            f'</td></tr>'
        )
    return (
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0">'
        f'{items_html}</table>'
    )


def _render_sources_footer(sources: Optional[list[dict]], display_map: dict) -> str:
    """Render the 'Sources' footer: one row per cited source, ordered by display
    number, as '[n] headline — domain' linked to the source URL. Empty string
    when there are no cited sources."""
    if not display_map:
        return ""
    src_by_id = {s["id"]: s for s in (sources or []) if isinstance(s, dict) and "id" in s}
    rows = ""
    for cid, n in sorted(display_map.items(), key=lambda kv: kv[1]):
        src = src_by_id.get(cid) or {}
        headline = html.escape(src.get("headline") or "Headline unavailable")
        domain = html.escape(src.get("domain") or "source link")
        label = f"[{n}] {headline} &mdash; {domain}"
        url = _safe_http_url(src.get("url"))
        if url:
            safe = html.escape(url, quote=True)
            entry = (
                f'<a href="{safe}" style="color:{_BRAND_NAVY};text-decoration:none;">{label}</a>'
            )
        else:
            entry = label
        rows += (
            f'<tr><td style="padding:1px 0;font-size:11px;color:#5a6678;'
            f"font-family:Arial,sans-serif;line-height:1.5;\">{entry}</td></tr>"
        )
    return (
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0" '
        f'style="margin-top:10px;border-top:1px solid #d8deec;padding-top:6px;">'
        f'<tr><td style="padding:4px 0 2px 0;font-size:9px;font-weight:700;'
        f'letter-spacing:1px;color:#5a6678;font-family:Arial,sans-serif;'
        f'text-transform:uppercase;">Sources</td></tr>{rows}</table>'
    )


def _render_exec_summary(macro_summary: dict | None) -> str:
    """Render the Executive Summary row.

    Prefers structured executive_bullets; falls back to legacy executive_summary prose.
    Returns empty string when no summary data is present.
    """
    if not macro_summary:
        return ""

    bullets = macro_summary.get("executive_bullets")
    legacy_text = macro_summary.get("executive_summary") or ""
    condition = (
        macro_summary.get("dominant_condition")
        or macro_summary.get("macro_sentiment")
        or ""
    )

    if bullets:
        body_html = _render_executive_bullets(bullets)
    elif legacy_text:
        body_html = (
            f'<p style="margin:0;font-size:14px;color:#1a2a45;'
            f"font-family:Georgia,'Times New Roman',serif;line-height:1.65;\">"
            f'{legacy_text}</p>'
        )
    else:
        return ""

    badge_html = ""
    if condition:
        badge_html = (
            f'&nbsp;<span style="background-color:{_BRAND_NAVY};color:#ffffff;'
            f'padding:2px 10px;border-radius:20px;font-size:10px;font-weight:600;'
            f'letter-spacing:0.5px;">{condition}</span>'
        )

    return f"""
      <tr>
        <td style="padding:24px 32px 0 32px;">
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="background-color:#EEF2FF;border-left:3px solid {_BRAND_NAVY};
                          border-radius:0 6px 6px 0;padding:16px 20px;">
                <p style="margin:0 0 8px 0;font-size:10px;font-weight:700;
                           letter-spacing:1.5px;color:{_BRAND_NAVY};
                           font-family:Arial,sans-serif;text-transform:uppercase;">
                  Executive Summary{badge_html}
                </p>
                {body_html}
              </td>
            </tr>
          </table>
        </td>
      </tr>"""


def _sentiment_word(score: int) -> tuple[str, str]:
    if score <= 3:
        return ("Negative", "#DC2626")
    if score <= 4:
        return ("Cautionary", "#D97706")
    if score <= 6:
        return ("Neutral", "#6B7280")
    if score <= 8:
        return ("Positive", "#16A34A")
    return ("Opportunity", "#15803D")


_SENTIMENT_TAG_COLORS: dict[str, str] = {
    "Negative": "#DC2626",
    "Neutral":  "#6B7280",
    "Positive": "#16A34A",
}


# ---------------------------------------------------------------------------
# 3. Main email generator
# ---------------------------------------------------------------------------

def generate_html_email(
    data: list[dict],
    macro_summary: dict | None = None,
) -> str:
    config = _load_mp_config()
    reporting_cfg          = config.get("reporting", {})
    max_per_segment: int   = _config_int(reporting_cfg, "max_visible_articles_per_segment", 3)
    max_total_visible: int = _config_int(reporting_cfg, "max_total_visible_articles", 12)
    scorer = Scoring.from_config(config)

    # 1. Final guardrail suppression pass (delivery-side patterns + dedupe).
    kept, delivery_ledger = _apply_delivery_suppression(data, config)

    # 2. Visibility filter.
    visible_pool = [r for r in kept if scorer.is_visible(r)]
    below_threshold_count = len(kept) - len(visible_pool)

    # 3. Group by commercial segment.
    groups_full = _group_by_commercial_segment(visible_pool)

    # 4. Per-segment cap (highest-impact articles first within each group).
    groups = {
        seg: sorted(arts, key=lambda x: _effective_impact(x), reverse=True)[:max_per_segment]
        for seg, arts in groups_full.items()
    }

    # 5. Total visible cap across all groups (drop lowest-impact until count <= cap).
    total = sum(len(arts) for arts in groups.values())
    if total > max_total_visible:
        all_visible = sorted(
            [(seg, a) for seg, arts in groups.items() for a in arts],
            key=lambda kv: _effective_impact(kv[1]),
            reverse=True,
        )[:max_total_visible]
        selected_hashes = {a.get("url_hash") for _, a in all_visible}
        groups = {seg: [a for a in arts if a.get("url_hash") in selected_hashes]
                  for seg, arts in groups.items()}
        groups = {seg: arts for seg, arts in groups.items() if arts}

    # 6. Compute weak_relevance: rows in `kept` with effective impact 4-5 that
    # didn't make it into any final group (the old Peripheral pool, now hidden).
    final_hashes = {a.get("url_hash") for arts in groups.values() for a in arts}
    weak_relevance_count = sum(
        1 for r in kept
        if scorer.is_weak_relevance(r)
        and r.get("url_hash") not in final_hashes
    )

    # 7. surfaced_count is the FINAL visible-card count (post-cap, post-grouping).
    surfaced_count = sum(len(arts) for arts in groups.values())

    # 8. Write delivery-side counts + surfaced_count back to today's row.
    delivery_ledger = (delivery_ledger
                       .record_count("below_impact_threshold", below_threshold_count)
                       .record_count("weak_relevance", weak_relevance_count))
    _update_delivery_summary_counts(
        surfaced_count=surfaced_count,
        ledger=delivery_ledger,
    )

    # 9. Thematic synthesis paragraphs (existing helper, now keyed off the new groups).
    multi_article_groups = {seg: arts for seg, arts in groups.items() if len(arts) >= 2}
    synthesis = synthesize_thematic_paragraphs(multi_article_groups)

    # 10. Build the HTML body.
    sections_html = _render_segment_watch_section(groups, synthesis)

    # Executive summary block (rendered via the existing helper, which Task 12 will
    # upgrade to consume executive_bullets).
    exec_html = _render_exec_summary(macro_summary)

    # Header counts. Null-safe handling is finalized in Task 13.
    today_str = datetime.now().strftime("%A, %B %d, %Y")
    screened = (macro_summary or {}).get("screened_count")
    if screened is None:
        screened = len(data)
    dominant_condition = (macro_summary or {}).get("dominant_condition") or (
        macro_summary or {}
    ).get("macro_sentiment") or ""

    macro_badge_html = ""
    if dominant_condition:
        macro_badge_html = (
            f'<span style="background-color:rgba(127,176,105,0.2);'
            f'color:{_BRAND_GREEN};border:1px solid rgba(127,176,105,0.4);'
            f'padding:3px 12px;border-radius:20px;font-size:11px;font-weight:600;'
            f'font-family:Arial,sans-serif;letter-spacing:0.5px;">'
            f'{dominant_condition}</span>'
        )

    _test_mode = _is_test_mode()
    title_prefix = "[TEST] " if _test_mode else ""
    test_banner_row = _TEST_BANNER_ROW if _test_mode else ""

    qa_html = _render_qa_debug_section(macro_summary) if _test_mode else ""

    subtitle = (
        f"{today_str} &nbsp;&middot;&nbsp; "
        f"{surfaced_count} surfaced signals from {screened} screened items"
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="color-scheme" content="light">
  <title>Americhem Market-Pulse: Daily Intelligence</title>
</head>
<body style="margin:0;padding:0;background-color:#F3F4F6;
             font-family:Arial,sans-serif;-webkit-text-size-adjust:100%;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0"
         style="background-color:#F3F4F6;padding:24px 0;">
    <tr><td align="center">
      <table width="640" cellpadding="0" cellspacing="0" border="0"
             style="max-width:640px;background-color:#ffffff;
                    border:0.5px solid #E5E7EB;border-radius:8px;overflow:hidden;">
        <tr><td>
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="background-color:{_BRAND_NAVY};padding:20px 32px 0 32px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="width:1%;white-space:nowrap;padding-right:16px;">
                      <img src="{_LOGO_URL}" alt="Americhem" width="140"
                           style="display:block;height:auto;max-height:40px;background-color:#ffffff;padding:3px 8px;border-radius:3px;">
                    </td>
                    <td style="width:1%;white-space:nowrap;padding-right:16px;">
                      <div style="width:1px;height:32px;background-color:rgba(255,255,255,0.25);"></div>
                    </td>
                    <td>
                      <p style="margin:0;font-size:11px;font-weight:700;letter-spacing:1.5px;color:{_BRAND_GREEN};font-family:Arial,sans-serif;text-transform:uppercase;">Market Intelligence</p>
                      <p style="margin:2px 0 0 0;font-size:18px;font-weight:700;color:#ffffff;font-family:Arial,sans-serif;line-height:1.2;">{title_prefix}Market-Pulse: Daily Intelligence</p>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr><td style="background-color:{_BRAND_GREEN};height:3px;font-size:0;line-height:0;">&nbsp;</td></tr>
            {test_banner_row}
            <tr>
              <td style="background-color:{_BRAND_NAVY_DARK};padding:10px 32px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="font-size:12px;color:rgba(255,255,255,0.65);font-family:Arial,sans-serif;">{subtitle}</td>
                    <td align="right">{macro_badge_html}</td>
                  </tr>
                </table>
              </td>
            </tr>
          </table>
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            {exec_html}
            {sections_html}
            {qa_html}
            <tr><td style="height:24px;"></td></tr>
          </table>
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="border-top:0.5px solid #E5E7EB;background-color:#FAFAFA;padding:16px 32px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="font-size:11px;color:#9CA3AF;font-family:Arial,sans-serif;">
                      Generated by <strong style="color:{_BRAND_NAVY};">Americhem Market-Pulse</strong> &nbsp;&middot;&nbsp; Powered by OpenAI &amp; Supabase
                    </td>
                    <td align="right">
                      <img src="{_LOGO_URL}" alt="Americhem" width="80" style="display:block;height:auto;opacity:0.4;">
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
          </table>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""


# ---------------------------------------------------------------------------
# 4. No-news fallback
# ---------------------------------------------------------------------------

def _generate_no_news_email() -> str:
    today_str = datetime.now().strftime("%A, %B %d, %Y")
    _test_mode = _is_test_mode()
    title_prefix = "[TEST] " if _test_mode else ""
    test_banner_row = _TEST_BANNER_ROW if _test_mode else ""
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>Americhem Market-Pulse</title></head>
<body style="margin:0;padding:0;background-color:#F3F4F6;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#F3F4F6;padding:24px 0;">
    <tr><td align="center">
      <table width="640" cellpadding="0" cellspacing="0" border="0" style="max-width:640px;background-color:#ffffff;border:0.5px solid #E5E7EB;border-radius:8px;overflow:hidden;">
        <tr><td>
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr><td style="background-color:{_BRAND_NAVY};padding:20px 32px 18px;">
              <p style="margin:0;font-size:18px;font-weight:700;color:#ffffff;font-family:Arial,sans-serif;">{title_prefix}Market-Pulse: Daily Intelligence</p>
              <p style="margin:4px 0 0 0;font-size:12px;color:rgba(255,255,255,0.6);font-family:Arial,sans-serif;">{today_str}</p>
            </td></tr>
            <tr><td style="background-color:{_BRAND_GREEN};height:3px;font-size:0;line-height:0;">&nbsp;</td></tr>
            {test_banner_row}
          </table>
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr><td style="padding:32px;">
              <p style="margin:0;font-size:15px;color:#374151;font-family:Georgia,'Times New Roman',serif;line-height:1.65;">No significant market events were detected in today's monitoring window.</p>
            </td></tr>
          </table>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>"""


# ---------------------------------------------------------------------------
# 5. Email delivery
# ---------------------------------------------------------------------------

def send_email(html_content: str) -> None:
    """Send the HTML digest via the Resend HTTP API with exponential backoff retry.

    Uses ``SMTP_PASS`` as the Resend API key so no secret changes are required.

    Raises:
        requests.HTTPError: On a permanent (non-transient) HTTP error response.
        requests.ConnectionError: If the Resend API is unreachable.
        requests.Timeout: If the request times out.
    """
    api_key      = os.environ["SMTP_PASS"]
    sender_email = os.environ["SENDER_EMAIL"]
    recipients   = [
        e.strip()
        for e in os.environ["RECIPIENT_EMAILS"].split(",")
        if e.strip()
    ]

    subject = (
        f"Americhem Market-Pulse \u2014 "
        f"{datetime.now().strftime('%B %d, %Y')}"
    )
    if _is_test_mode():
        subject = f"[TEST] {subject}"

    payload = {
        "from":    sender_email,
        "to":      recipients,
        "subject": subject,
        "html":    html_content,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }

    for attempt in range(1, _MAX_EMAIL_ATTEMPTS + 1):
        try:
            resp = requests.post(
                _RESEND_API_URL,
                json=payload,
                headers=headers,
                timeout=30,
            )

            if resp.status_code in _TRANSIENT_HTTP_CODES and attempt < _MAX_EMAIL_ATTEMPTS:
                delay = _EMAIL_BASE_DELAY_S * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                logger.warning(
                    "Transient HTTP %s from Resend (attempt %d/%d) — retrying in %.1fs",
                    resp.status_code, attempt, _MAX_EMAIL_ATTEMPTS, delay,
                )
                time.sleep(delay)
                continue

            if not resp.ok:
                logger.error(
                    "Resend API returned HTTP %s — body: %s",
                    resp.status_code,
                    resp.text,
                )
                resp.raise_for_status()

            logger.info(
                "Email sent — subject: '%s' | recipients: %d",
                subject,
                len(recipients),
            )
            return

        except requests.HTTPError as exc:
            raise

        except requests.ConnectionError as exc:
            logger.error("Connection error reaching Resend API: %s", exc)
            raise

        except requests.Timeout:
            logger.error("Request to Resend API timed out")
            raise

        except Exception as exc:
            logger.error("Unexpected error sending email: %s", exc)
            raise


# ---------------------------------------------------------------------------
# 6. Entrypoint
# ---------------------------------------------------------------------------

def execute_pipeline() -> None:
    data          = fetch_todays_intelligence()
    macro_summary = fetch_macro_summary()

    if not data:
        logger.warning("No intelligence records for today — sending no-news notification.")
        html = _generate_no_news_email()
        send_email(html)
        return

    critical_count  = sum(1 for r in data if _alert_tier(r) == "CRITICAL")
    strategic_count = sum(1 for r in data if _alert_tier(r) == "STRATEGIC")
    routine_count   = sum(1 for r in data if _alert_tier(r) == "ROUTINE")
    logger.info(
        "Rendering email — critical: %d | strategic: %d | routine: %d",
        critical_count, strategic_count, routine_count,
    )

    html = generate_html_email(data, macro_summary)
    send_email(html)


if __name__ == "__main__":
    execute_pipeline()
