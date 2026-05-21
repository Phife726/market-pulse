import json
import logging
import os
import random
import time
from datetime import datetime, timedelta
from typing import Optional

import requests
import yaml
from openai import OpenAI
from supabase import create_client, Client

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


def _effective_impact(row: dict) -> int:
    """Return routing score: americhem_impact_score preferred, sentiment_score fallback."""
    score = row.get("americhem_impact_score")
    if score is not None:
        return int(score)
    return int(row.get("sentiment_score") or 5)


_LEGACY_STRATEGIC_SEGMENT_MAP: dict[str, str] = {
    "Healthcare":                    "Healthcare",
    "Fibers":                        "Fibers",
    "Packaging":                     "Packaging",
    "Industrial":                    "Industrial",
    "Raw Materials / Supply Chain":  "Enterprise / Cross-Segment",
    "Regulatory / Sustainability":   "Enterprise / Cross-Segment",
    "Competitive / Customer Signal": "Enterprise / Cross-Segment",
    "Broader Americhem":             "Enterprise / Cross-Segment",
}


def _commercial_segment_of(row: dict) -> str:
    """Return commercial_segment if set; else map legacy strategic_segment; else default."""
    seg = (row.get("commercial_segment") or "").strip()
    if seg:
        return seg
    legacy = (row.get("strategic_segment") or "").strip()
    return _LEGACY_STRATEGIC_SEGMENT_MAP.get(legacy, "Enterprise / Cross-Segment")


def _signal_type_of(row: dict) -> str:
    """Return signal_type if set on the row; else 'Other'."""
    sig = (row.get("signal_type") or "").strip()
    return sig if sig else "Other"


# ---------------------------------------------------------------------------
# Task 10: Commercial segment grouping + new section renderer
# ---------------------------------------------------------------------------

def _group_by_commercial_segment(items: list[dict]) -> dict[str, list[dict]]:
    """Bucket items by their resolved commercial segment (new field or legacy fallback)."""
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
    if score is None:
        legacy_sentiment = item.get("sentiment_score")
        if legacy_sentiment is not None and int(legacy_sentiment) <= 3:
            critical_html = (
                '<span style="color:#9CA3AF;">&nbsp;&#9679;&nbsp;</span>'
                '<span style="color:#DC2626;font-weight:700;">CRITICAL</span>'
            )

    return f'{score_html}{tag_html}{signal_html}{critical_html}'


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

        cards_html = ""
        articles_sorted = sorted(
            articles,
            key=lambda x: -int(x.get("americhem_impact_score") or x.get("sentiment_score") or 0),
        )
        for art in articles_sorted:
            meta = _render_meta_strip(art)
            headline = art.get("headline", "")
            source_url = art.get("source_url", "#")
            americhem_impact = art.get("americhem_impact", "")
            so_what_html = (
                f'<p style="margin:4px 0 0 0;font-size:13px;color:#374151;'
                f"font-family:Georgia,'Times New Roman',serif;line-height:1.55;\">"
                f'<strong style="color:{_BRAND_NAVY};">So what:</strong> {americhem_impact}</p>'
                if americhem_impact else ""
            )
            cards_html += (
                f'<tr><td style="padding:6px 0 10px 0;">'
                f'<p style="margin:0 0 4px 0;font-size:11px;color:#6B7280;'
                f'font-family:Arial,sans-serif;">{meta}</p>'
                f'<a href="{source_url}" style="font-size:14px;font-weight:700;'
                f'color:{_BRAND_NAVY};font-family:Arial,sans-serif;'
                f'text-decoration:none;line-height:1.35;">{headline}</a>'
                f'{so_what_html}'
                f'</td></tr>'
            )

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

_DELIVERY_SAMPLES_CAP = 10


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
) -> tuple[list[dict], dict, list[dict]]:
    """Run the deterministic seven-rule guardrail over fetched rows.

    Returns (kept_rows, counts_by_reason, samples_capped_at_10).
    First matching rule wins; a row is counted once.
    """
    sup_cfg = config.get("delivery_suppression") or {}
    counts: dict[str, int] = {}
    samples: list[dict] = []
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

    def _suppress(reason: str, row: dict) -> None:
        counts[reason] = counts.get(reason, 0) + 1
        samples.append({
            "reason": reason,
            "url": row.get("source_url", ""),
            "title": row.get("headline", ""),
        })
        if len(samples) > _DELIVERY_SAMPLES_CAP:
            del samples[0]

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
                _suppress("enterprise_cross_segment_low_impact", row)
                continue

        # Rule 2: Product listing URL
        if sup_cfg.get("enable_product_listing", True) and _matches_any_pattern(url, product_patterns):
            _suppress("product_listing", row)
            continue

        # Rule 3: Job posting URL (unless escalated)
        if sup_cfg.get("enable_job_posting", True) and _matches_any_pattern(url, job_patterns):
            if action != override_action:
                _suppress("job_posting", row)
                continue

        # Rule 4: Generic market report title with empty entities
        if sup_cfg.get("enable_generic_market_report", True):
            if _matches_any_pattern(headline, market_patterns) and not entities:
                _suppress("generic_market_report", row)
                continue

        # Rule 5: Unrelated color result
        if sup_cfg.get("enable_unrelated_color_result", True):
            if _contains_any_term(headline, color_terms):
                # Check headline and entities only — not americhem_impact, which may
                # contain negating language like "No plastics relevance." that would
                # cause a false substring match on "plastic".
                relevance_haystack = f"{headline} {entities_text}"
                if not _contains_any_term(relevance_haystack, plastics_terms):
                    _suppress("unrelated_color_result", row)
                    continue

        # Rule 6: Exact duplicate headline (case-insensitive)
        if sup_cfg.get("enable_duplicate_headline", True):
            if headline and any(h.lower() == headline.lower() for h in kept_headlines):
                _suppress("duplicate_headline", row)
                continue

        # Rule 7: Semantic duplicate headline
        # Lowercase before comparison — rapidfuzz 3.x no longer auto-lowercases,
        # so case differences in proper nouns would otherwise deflate the score.
        if sup_cfg.get("enable_semantic_duplicate_headline", True) and kept_headlines and headline:
            hl_lower = headline.lower()
            scores = [_token_sort_ratio(hl_lower, h.lower()) for h in kept_headlines]
            if scores and max(scores) >= threshold:
                _suppress("semantic_duplicate_headline", row)
                continue

        kept.append(row)
        if headline:
            kept_headlines.append(headline)

    return kept, counts, samples


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

_QA_REASON_LABELS: dict[str, str] = {
    "duplicate_url":                       "duplicate URL",
    "semantic_duplicate":                  "semantic duplicate",
    "llm_discard":                         "LLM discard",
    "scrape_failed":                       "scrape failed",
    "below_impact_threshold":              "below impact threshold",
    "weak_relevance":                      "weak relevance (4-5, ungrouped)",
    "duplicate_headline":                  "duplicate headline",
    "semantic_duplicate_headline":         "semantic duplicate headline",
    "product_listing":                     "product listing",
    "job_posting":                         "job posting",
    "generic_market_report":               "generic market report",
    "unrelated_color_result":              "unrelated color result",
    "enterprise_cross_segment_low_impact": "Enterprise / Cross-Segment, low impact",
}


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
        "below_impact_threshold", "weak_relevance",
        "duplicate_headline", "semantic_duplicate_headline",
        "product_listing", "job_posting", "generic_market_report",
        "unrelated_color_result", "enterprise_cross_segment_low_impact",
    ]
    for code in display_order:
        if code in breakdown:
            label = _QA_REASON_LABELS.get(code, code)
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
        reason_label = _QA_REASON_LABELS.get(reason_code, reason_code)
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
# OpenAI client
# ---------------------------------------------------------------------------

OPENAI_MODEL = "gpt-5.4-nano"


def _get_openai() -> OpenAI:
    """Return an authenticated OpenAI client using env credentials."""
    return OpenAI(api_key=os.environ["OPENAI_API_KEY"])


# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------

def _get_supabase() -> Client:
    """Return an authenticated Supabase client using env credentials."""
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)


# ---------------------------------------------------------------------------
# 1. Data fetch — with Monday 72-hour lookback
# ---------------------------------------------------------------------------

def fetch_todays_intelligence() -> list[dict]:
    try:
        supabase = _get_supabase()
        is_monday = datetime.now().weekday() == 0
        lookback_hours = 72 if is_monday else 24
        if is_monday:
            logger.info("Monday detected — extending lookback to 72 hours.")

        cutoff = (datetime.utcnow() - timedelta(hours=lookback_hours)).isoformat()

        result = (
            supabase.table("daily_intelligence")
            .select("*")
            .gte("created_at", cutoff)
            .order("americhem_impact_score", desc=True)
            .execute()
        )

        records: list[dict] = []
        for row in result.data or []:
            impact = _effective_impact(row)
            if impact <= 3:
                row["alert_tier"] = "CRITICAL"
            elif impact >= 8:
                row["alert_tier"] = "STRATEGIC"
            else:
                row["alert_tier"] = "ROUTINE"
            records.append(row)

        logger.info(
            "Fetched %d intelligence record(s) (lookback: %dh).",
            len(records),
            lookback_hours,
        )
        return records

    except Exception as exc:
        logger.error("Failed to fetch intelligence from Supabase: %s", exc)
        return []


def fetch_macro_summary() -> dict | None:
    try:
        from datetime import date

        min_run_date = (date.today() - timedelta(days=1)).isoformat()
        supabase = _get_supabase()
        result = (
            supabase.table("daily_summaries")
            .select(
                "run_date, run_mode, executive_summary, macro_sentiment, "
                "dominant_condition, executive_bullets, screened_count, "
                "surfaced_count, suppression_breakdown, suppression_samples"
            )
            .eq("run_mode", _run_mode())
            .gte("run_date", min_run_date)
            .order("run_date", desc=True)
            .limit(1)
            .execute()
        )
        if result.data:
            return result.data[0]

        logger.warning("No macro summary found for run_date >= %s.", min_run_date)
        return None
    except Exception as exc:
        logger.error("Failed to fetch macro summary: %s", exc)
        return None


def _update_delivery_summary_counts(
    *,
    surfaced_count: int,
    delivery_counts: dict,
    delivery_samples: list,
) -> None:
    """Update today's daily_summaries row (filtered by run_mode) with delivery-side stats.

    Non-critical: failures are logged but do not raise.
    """
    try:
        from datetime import date as _date
        supabase = _get_supabase()

        # Fetch the existing breakdown/samples so we can merge ours in.
        existing = (
            supabase.table("daily_summaries")
            .select("suppression_breakdown, suppression_samples")
            .eq("run_date", _date.today().isoformat())
            .eq("run_mode", _run_mode())
            .limit(1)
            .execute()
        )
        rows = existing.data or []
        prior_breakdown = (rows[0].get("suppression_breakdown") if rows else None) or {}
        prior_samples   = (rows[0].get("suppression_samples")   if rows else None) or []

        merged_breakdown = dict(prior_breakdown)
        for k, v in delivery_counts.items():
            merged_breakdown[k] = merged_breakdown.get(k, 0) + int(v)

        merged_samples = list(prior_samples) + list(delivery_samples)
        if len(merged_samples) > _DELIVERY_SAMPLES_CAP:
            merged_samples = merged_samples[-_DELIVERY_SAMPLES_CAP:]

        supabase.table("daily_summaries").update({
            "surfaced_count": surfaced_count,
            "suppression_breakdown": merged_breakdown,
            "suppression_samples": merged_samples,
        }).eq("run_date", _date.today().isoformat()).eq("run_mode", _run_mode()).execute()
    except Exception as exc:
        logger.warning("Failed to update delivery counts on daily_summaries: %s", exc)


# ---------------------------------------------------------------------------
# 2. Thematic routing helpers
# ---------------------------------------------------------------------------

def _group_for_thematic(items: list[dict]) -> dict[str, list[dict]]:
    """Group qualifying articles by strategic segment (or category fallback) for thematic synthesis.

    Args:
        items: Articles pre-filtered to scores 4–10. Score 1–3 articles are
            silently skipped as a safety guard.

    Returns:
        Dict of {segment: [articles]} containing only groups with 2+ articles.
        Articles with missing segment and category are grouped under 'Uncategorized'.
    """
    from collections import defaultdict
    buckets: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        if _effective_impact(item) <= 3:
            continue
        segment = item.get("strategic_segment") or item.get("category") or "Uncategorized"
        buckets[segment].append(item)
    return {seg: arts for seg, arts in buckets.items() if len(arts) >= 2}


def _collect_thin_entries(
    items: list[dict],
    groups: dict[str, list[dict]],
) -> list[dict]:
    """Return ungrouped high-impact (7–10) articles for thin thematic rendering.

    Args:
        items: All non-critical articles at or above the visible threshold.
        groups: The 2+ article groups from _group_for_thematic().

    Returns:
        Single-article items with effective impact 7–10 not captured in any group,
        ordered by impact score ascending.
    """
    grouped_hashes = {
        art.get("url_hash") for arts in groups.values() for art in arts
    }
    thin = [
        item for item in items
        if item.get("url_hash") not in grouped_hashes
        and _effective_impact(item) >= 7
    ]
    return sorted(thin, key=lambda x: _effective_impact(x))


def _collect_peripheral(
    items: list[dict],
    groups: dict[str, list[dict]],
) -> list[dict]:
    """Return ungrouped moderate-impact (4–6) articles for the Peripheral Signals section.

    Args:
        items: Articles not in the critical zone.
        groups: The 2+ article groups from _group_for_thematic().

    Returns:
        Single-article items with effective impact 4–6 not captured in any group,
        ordered by impact score ascending.
    """
    grouped_hashes = {
        art.get("url_hash") for arts in groups.values() for art in arts
    }
    peripheral = [
        item for item in items
        if item.get("url_hash") not in grouped_hashes
        and _effective_impact(item) <= 6
    ]
    return sorted(peripheral, key=lambda x: _effective_impact(x))


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
            entity = entities[0] if entities else (art.get("strategic_segment") or art.get("category") or "Unknown")
            americhem_impact = art.get("americhem_impact", "")
            tag_suffix = f" | {tag}" if tag else ""
            lines.append(f"- [{entity} | impact:{impact_score}/10{tag_suffix}] {americhem_impact}")
        lines.append("")

    grouped_text = "\n".join(lines).strip()

    system_prompt = (
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

    try:
        client = _get_openai()
        completion = client.chat.completions.create(
            model=OPENAI_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": grouped_text},
            ],
        )
        result: dict[str, str] = json.loads(completion.choices[0].message.content)
        logger.info("Thematic synthesis complete — %d categories.", len(result))
        return result
    except Exception as exc:
        logger.error(
            "Thematic synthesis failed — falling back to bullets-only: %s", exc
        )
        return {}


def _render_peripheral_section(items: list[dict]) -> str:
    """Render the Peripheral Signals compact bullet list.

    Args:
        items: Score 4–6 articles not captured in any synthesis group.

    Returns:
        HTML string for the Peripheral Signals section, or empty string if
        items is empty.
    """
    if not items:
        return ""

    bullets_html = ""
    for item in items:
        entities = item.get("entities_mentioned") or []
        entity = entities[0] if entities else (item.get("strategic_segment") or item.get("category") or "Unknown")
        impact_score = _effective_impact(item)
        headline = item.get("headline", "")
        source_url = item.get("source_url", "#")
        bullets_html += (
            f'<tr><td style="padding:2px 0;">'
            f'<span style="font-size:12px;font-family:Arial,sans-serif;color:#6B7280;">'
            f'&bull;&nbsp;<strong style="color:#374151;">[{entity}: {impact_score}/10]</strong>'
            f'&nbsp;<a href="{source_url}" style="color:#374151;text-decoration:none;">'
            f'{headline}</a>'
            f'</span></td></tr>'
        )

    return f"""
      <tr>
        <td style="padding:24px 32px 4px 32px;">
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="padding-bottom:8px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="font-size:11px;font-weight:700;letter-spacing:1.5px;
                                text-transform:uppercase;color:#9CA3AF;
                                font-family:Arial,sans-serif;white-space:nowrap;
                                padding-right:12px;">
                      PERIPHERAL SIGNALS
                    </td>
                    <td style="border-bottom:1px solid #E5E7EB;width:100%;"></td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td>
                <p style="margin:0 0 8px 0;font-size:11px;color:#9CA3AF;
                           font-family:Arial,sans-serif;font-style:italic;">
                  Monitoring only &mdash; lower probability of direct impact
                </p>
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  {bullets_html}
                </table>
              </td>
            </tr>
          </table>
        </td>
      </tr>"""


def _render_thematic_section(
    groups: dict[str, list[dict]],
    thin_entries: list[dict],
    synthesis: dict[str, str],
) -> str:
    """Render the Thematic Intelligence section.

    Args:
        groups: 2+ article groups from _group_for_thematic(), keyed by category.
        thin_entries: Single-article items scoring 7–10 from _collect_thin_entries().
        synthesis: LLM paragraphs from synthesize_thematic_paragraphs(). May be
            empty dict — sections render bullets-only in that case.

    Returns:
        HTML string for the Thematic Intelligence section, or empty string if
        both groups and thin_entries are empty.
    """
    if not groups and not thin_entries:
        return ""

    ordered_groups = sorted(
        groups.items(),
        key=lambda kv: min(_effective_impact(a) for a in kv[1]),
    )

    def _bullet(item: dict) -> str:
        entities = item.get("entities_mentioned") or []
        entity = entities[0] if entities else (item.get("strategic_segment") or item.get("category") or "Unknown")
        impact_score = _effective_impact(item)
        headline = item.get("headline", "")
        source_url = item.get("source_url", "#")
        return (
            f'<tr><td style="padding:2px 0;">'
            f'<span style="font-size:12px;font-family:Arial,sans-serif;">'
            f'&bull;&nbsp;'
            f'<a href="{source_url}" style="color:{_BRAND_NAVY};text-decoration:none;'
            f'font-weight:600;">[{entity}: {impact_score}/10]</a>'
            f'&nbsp;<span style="color:#374151;">{headline}</span>'
            f'</span></td></tr>'
        )

    def _category_block(category: str, articles: list[dict], para: str) -> str:
        para_html = (
            f'<p style="margin:0 0 10px 0;font-size:13px;color:#1a2a45;'
            f"font-family:Georgia,'Times New Roman',serif;line-height:1.65;\">"
            f'{para}</p>'
        ) if para else ""
        sorted_articles = sorted(
            articles, key=lambda x: _effective_impact(x)
        )
        bullets = "".join(_bullet(a) for a in sorted_articles)
        return (
            f'<tr><td style="padding:0 0 18px 0;">'
            f'<p style="margin:0 0 6px 0;font-size:11px;font-weight:700;'
            f'letter-spacing:1px;text-transform:uppercase;color:{_BRAND_NAVY};'
            f'font-family:Arial,sans-serif;">{category}</p>'
            f'{para_html}'
            f'<table width="100%" cellpadding="0" cellspacing="0" border="0">'
            f'{bullets}</table>'
            f'</td></tr>'
        )

    categories_html = "".join(
        _category_block(cat, arts, synthesis.get(cat, ""))
        for cat, arts in ordered_groups
    )

    thin_sorted = sorted(thin_entries, key=lambda x: _effective_impact(x))
    for item in thin_sorted:
        segment = item.get("strategic_segment") or item.get("category") or "Uncategorized"
        categories_html += _category_block(segment, [item], "")

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
                      THEMATIC INTELLIGENCE
                    </td>
                    <td style="border-bottom:1px solid {_BRAND_NAVY};width:100%;"></td>
                  </tr>
                </table>
              </td>
            </tr>
            {categories_html}
          </table>
        </td>
      </tr>"""


# ---------------------------------------------------------------------------
# 3. HTML generation helpers
# ---------------------------------------------------------------------------

def _render_executive_bullets(bullets: list[dict]) -> str:
    """Render the 3-bullet executive summary body.

    Each bullet uses bold label + body text. Labels come from the data, which
    guarantees they match the configured executive_bullet_labels enforced by
    ingestion's _validate_executive_bullets().
    """
    items_html = ""
    for b in bullets:
        label = b.get("label", "")
        body = b.get("body", "")
        items_html += (
            f'<tr><td style="padding:2px 0;font-size:13px;color:#1a2a45;'
            f"font-family:Georgia,'Times New Roman',serif;line-height:1.55;\">"
            f'&bull;&nbsp;<strong>{label}:</strong> {body}'
            f'</td></tr>'
        )
    return (
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0">'
        f'{items_html}</table>'
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


def _render_card(item: dict, accent: str, bg: str, text: str) -> str:
    headline            = item.get("headline", "No headline")
    source_url          = item.get("source_url", "#")
    americhem_impact    = item.get("americhem_impact", "")
    source_publication  = item.get("source_publication", "")
    recommended_action  = item.get("recommended_action", "")

    # Segment label: new field preferred, category fallback
    segment_label = (item.get("strategic_segment") or item.get("category") or "").upper()

    # Score display: new fields preferred, old sentiment_score fallback
    impact_score_raw = item.get("americhem_impact_score")
    sentiment_tag    = item.get("sentiment_tag")
    if impact_score_raw is not None and sentiment_tag:
        tag_color   = _SENTIMENT_TAG_COLORS.get(sentiment_tag, "#6B7280")
        score_html  = (
            f'<span style="color:{_BRAND_NAVY};font-weight:600;">'
            f'Impact: {impact_score_raw}/10</span>'
            f'<span style="color:#9CA3AF;">&nbsp;&#9679;&nbsp;</span>'
            f'<span style="color:{tag_color};font-weight:600;">{sentiment_tag}</span>'
        )
        rationale_text = item.get("impact_rationale", "")
    else:
        score = item.get("sentiment_score", "")
        sentiment_word, sentiment_color = _sentiment_word(int(score) if score else 5)
        score_html = (
            f'<span style="color:{sentiment_color};font-weight:600;">'
            f'{sentiment_word}</span>'
            f'<span style="color:#9CA3AF;">&nbsp;&#9679;&nbsp;Score: {score}/10</span>'
        )
        rationale_text = item.get("sentiment_rationale", "")

    source_pub_html = (
        f'<span style="font-size:11px;color:#9CA3AF;'
        f'font-family:Arial,sans-serif;">via {source_publication}</span>'
        if source_publication else ""
    )

    rationale_html = (
        f'<p style="margin:0 0 10px 0;font-size:12px;color:#6B7280;'
        f'font-family:Arial,sans-serif;font-style:italic;line-height:1.4;">'
        f'{rationale_text}</p>'
        if rationale_text else ""
    )

    action_html = (
        f'<p style="margin:0 0 10px 0;padding:6px 10px;background-color:#F9FAFB;'
        f'border-left:3px solid {accent};font-size:12px;font-weight:600;'
        f'font-family:Arial,sans-serif;color:{accent};">'
        f'&#9654; ACTION: {recommended_action}</p>'
        if recommended_action and recommended_action not in {"No action", "Monitor"} else ""
    )

    return f"""
            <tr>
              <td style="padding:0 0 10px 0;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0"
                       style="border:0.5px solid #E5E7EB;border-radius:6px;
                              overflow:hidden;background-color:#ffffff;">
                  <tr>
                    <td style="background-color:{accent};height:3px;
                                font-size:0;line-height:0;">&nbsp;</td>
                  </tr>
                  <tr>
                    <td style="padding:14px 16px;">
                      <a href="{source_url}"
                         style="font-size:14px;font-weight:700;color:{accent};
                                font-family:Arial,sans-serif;text-decoration:none;
                                line-height:1.4;display:block;margin-bottom:8px;">
                        {headline}
                      </a>
                      <p style="margin:0 0 8px 0;font-size:13px;color:#374151;
                                 font-family:Georgia,'Times New Roman',serif;
                                 line-height:1.6;">
                        {americhem_impact}
                      </p>
                      {rationale_html}
                      {action_html}
                      <table width="100%" cellpadding="0" cellspacing="0" border="0">
                        <tr>
                          <td>
                            <span style="display:inline-block;font-size:10px;
                                          font-weight:700;letter-spacing:0.8px;
                                          text-transform:uppercase;padding:2px 6px;
                                          border-radius:3px;background-color:{bg};
                                          color:{text};border:1px solid {accent};
                                          font-family:Arial,sans-serif;">
                              {segment_label}
                            </span>
                            <span style="margin-left:6px;">{source_pub_html}</span>
                          </td>
                          <td align="right"
                              style="font-size:11px;font-family:Arial,sans-serif;">
                            {score_html}
                          </td>
                        </tr>
                      </table>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>"""


def _render_section(
    tier: str,
    label: str,
    accent: str,
    bg: str,
    text: str,
    items: list[dict],
) -> str:
    if not items:
        return ""

    cards_html = "".join(
        _render_card(item, accent, bg, text) for item in items
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
                                text-transform:uppercase;color:{text};
                                font-family:Arial,sans-serif;white-space:nowrap;
                                padding-right:12px;">
                      {label.upper()}
                    </td>
                    <td style="border-bottom:1px solid {accent};width:100%;"></td>
                  </tr>
                </table>
              </td>
            </tr>
            {cards_html}
          </table>
        </td>
      </tr>"""


# ---------------------------------------------------------------------------
# 3. Main email generator
# ---------------------------------------------------------------------------

def generate_html_email(
    data: list[dict],
    macro_summary: dict | None = None,
) -> str:
    config = _load_mp_config()
    reporting_cfg          = config.get("reporting", {})
    visible_threshold: int = _config_int(reporting_cfg, "visible_impact_threshold", 6)
    max_per_segment: int   = _config_int(reporting_cfg, "max_visible_articles_per_segment", 3)
    max_total_visible: int = _config_int(reporting_cfg, "max_total_visible_articles", 12)

    # 1. Final guardrail suppression pass (delivery-side patterns + dedupe).
    kept, delivery_sup_counts, delivery_sup_samples = _apply_delivery_suppression(data, config)

    # 2. Visibility filter.
    visible_pool = [r for r in kept if _effective_impact(r) >= visible_threshold]
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
        if 4 <= _effective_impact(r) <= 5
        and r.get("url_hash") not in final_hashes
    )

    # 7. surfaced_count is the FINAL visible-card count (post-cap, post-grouping).
    surfaced_count = sum(len(arts) for arts in groups.values())

    # 8. Write delivery-side counts + surfaced_count back to today's row.
    _update_delivery_summary_counts(
        surfaced_count=surfaced_count,
        delivery_counts={
            **delivery_sup_counts,
            "below_impact_threshold": below_threshold_count,
            "weak_relevance": weak_relevance_count,
        },
        delivery_samples=delivery_sup_samples,
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

    critical_count  = sum(1 for r in data if r.get("alert_tier") == "CRITICAL")
    strategic_count = sum(1 for r in data if r.get("alert_tier") == "STRATEGIC")
    routine_count   = sum(1 for r in data if r.get("alert_tier") == "ROUTINE")
    logger.info(
        "Rendering email — critical: %d | strategic: %d | routine: %d",
        critical_count, strategic_count, routine_count,
    )

    html = generate_html_email(data, macro_summary)
    send_email(html)


if __name__ == "__main__":
    execute_pipeline()
