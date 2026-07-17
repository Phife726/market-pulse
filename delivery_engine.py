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
import prompts
import scoring
from scoring import tier as _alert_tier
# Report assembly lives in report.py (the pure decision pipeline); tests
# exercise its internals via `report` directly.
from report import ReportModel, assemble_report

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
# Shared section header
# ---------------------------------------------------------------------------

def _section_header_row(title: str, *, title_color: str, rule_color: str) -> str:
    """A section-header table row: the uppercase title spans the full content
    width with a hairline underline beneath it.

    Full-width by design — the old layout put the title in a `white-space:nowrap`
    cell beside a `width:100%` rule cell, so on a reflowing/narrow mail client
    the title column was squeezed and long titles ("Additional Articles to
    Explore", "Macroeconomic Outlook") wrapped to 3+ lines. Spanning the full
    width, the title fits on one line normally and wraps to at most two on very
    narrow screens, with the rule always beneath it."""
    return (
        f'<tr>'
        f'<td style="padding-bottom:8px;border-bottom:1px solid {rule_color};'
        f'font-size:11px;font-weight:700;letter-spacing:1.5px;'
        f'text-transform:uppercase;color:{title_color};'
        f'font-family:Arial,sans-serif;">{title}</td>'
        f'</tr>'
    )


# ---------------------------------------------------------------------------
# Task 10: Commercial Segment Watch renderers
# ---------------------------------------------------------------------------

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
            {_section_header_row("COMMERCIAL SEGMENT WATCH", title_color=_BRAND_NAVY, rule_color=_BRAND_NAVY)}
            {blocks_html}
          </table>
        </td>
      </tr>"""


# ---------------------------------------------------------------------------
# Additional Articles to Explore — compact optional-discovery appendix
# ---------------------------------------------------------------------------

def _appendix_domain(url: str) -> str:
    """Registrable host minus a leading 'www.'; '' when unparseable/empty."""
    try:
        host = urlparse(url or "").hostname or ""
    except (ValueError, TypeError):
        return ""
    return host[4:] if host.startswith("www.") else host


def _appendix_source_label(item: dict) -> str:
    """Publisher name when known, else the source domain."""
    pub = (item.get("source_publication") or "").strip()
    return pub or _appendix_domain(item.get("source_url") or "")


def _appendix_pub_date(item: dict) -> str:
    """Human date from published_at ONLY (never the scrape timestamp). Empty
    string when published_at is absent or unparseable."""
    val = item.get("published_at")
    if not isinstance(val, str) or not val.strip():
        return ""
    s = val.strip()
    try:
        dt = datetime.fromisoformat(s[:-1] + "+00:00" if s.endswith("Z") else s)
    except (ValueError, TypeError):
        return ""
    return dt.strftime("%b %d, %Y")


def _render_additional_articles_section(items: list[dict]) -> str:
    """Render the compact 'Additional Articles to Explore' appendix.

    One row per item: linked headline plus a meta line (segment · Impact X/10 ·
    source · date). Deliberately omits the 'So what' narrative — this is
    optional reading, visually distinct from surfaced intelligence. All
    untrusted values are HTML-escaped and hrefs pass through _safe_http_url."""
    if not items:
        return ""

    rows_html = ""
    for item in items:
        headline = html.escape(item.get("headline", "") or "")
        segment = html.escape((item.get("commercial_segment") or "").strip())
        score = item.get("americhem_impact_score")
        try:
            score_txt = f"Impact: {int(score)}/10" if score is not None else ""
        except (TypeError, ValueError):
            score_txt = ""
        source = html.escape(_appendix_source_label(item))
        date = html.escape(_appendix_pub_date(item))

        meta_parts = [p for p in (segment, score_txt, source, date) if p]
        meta = ' <span style="color:#9CA3AF;">&middot;</span> '.join(meta_parts)

        url = _safe_http_url(item.get("source_url"))
        if url:
            safe = html.escape(url, quote=True)
            headline_html = (
                f'<a href="{safe}" style="font-size:13px;font-weight:600;'
                f'color:{_BRAND_NAVY};font-family:Arial,sans-serif;'
                f'text-decoration:none;line-height:1.35;">{headline}</a>'
            )
        else:
            headline_html = (
                f'<span style="font-size:13px;font-weight:600;color:{_BRAND_NAVY};'
                f'font-family:Arial,sans-serif;line-height:1.35;">{headline}</span>'
            )

        rows_html += (
            f'<tr><td style="padding:5px 0 7px 0;">'
            f'{headline_html}'
            f'<p style="margin:2px 0 0 0;font-size:11px;color:#6B7280;'
            f'font-family:Arial,sans-serif;">{meta}</p>'
            f'</td></tr>'
        )

    return f"""
      <tr>
        <td style="padding:24px 32px 4px 32px;">
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            {_section_header_row("Additional Articles to Explore", title_color="#5a6678", rule_color="#E5E7EB")}
            <tr>
              <td>
                <table width="100%" cellpadding="0" cellspacing="0" border="0">{rows_html}</table>
              </td>
            </tr>
          </table>
        </td>
      </tr>"""


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
    on test mode; this function does not check MARKET_PULSE_RUN_MODE itself.

    Deliberate staleness: the fields come from the macro-summary row fetched at
    the START of the run — the pre-write-back state — so on the day's first run
    this block shows ingestion-only counts and a stale/None surfaced count.
    Showing this run's post-merge accounting would require re-fetching the row
    after prepare_report's write-back; the email subtitle's surfaced count
    (model.surfaced_count) is the authoritative same-run number."""
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
        "unscrapable_domain", "zoominfo_company_mismatch",
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
            {_section_header_row("QA &middot; Suppression Summary", title_color="#9CA3AF", rule_color="#E5E7EB")}
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


def _summary_has_content(row: Optional[dict]) -> bool:
    """True when the row carries renderable macro-summary content. Zero-yield
    ingestion runs persist accounting-only rows (screened/suppression counts
    with no summary fields, issue #43) — those return False."""
    if not row:
        return False
    return bool(
        row.get("executive_bullets")
        or row.get("executive_summary")
        or row.get("macro_outlook")
        or row.get("dominant_condition")
    )


def _prefer_production_summary(test_row: Optional[dict], production_row: Optional[dict]) -> bool:
    """Test-mode fallback comparison: content-fullness first, then strict
    run_date recency; ties keep the test row (the date-rollover grace). An
    accounting-only row therefore never shadows a content-full one in either
    direction — before issue #43 such rows did not exist at all."""
    if production_row is None:
        return False
    if test_row is None:
        return True
    prod_content = _summary_has_content(production_row)
    test_content = _summary_has_content(test_row)
    if prod_content != test_content:
        return prod_content
    return (
        str(production_row.get("run_date") or "")
        > str(test_row.get("run_date") or "")
    )


def fetch_macro_summary() -> dict | None:
    from datetime import date
    min_run_date = (date.today() - timedelta(days=1)).isoformat()
    summary = _repo().fetch_latest_summary(
        run_mode=_run_mode(),
        min_date=min_run_date,
    )
    if _is_test_mode():
        # Delivery-only test runs (run_ingestion=false) have no same-day
        # test-mode macro row — ingestion is what writes it — and a leftover
        # test row from yesterday's QA run would be stale against today's
        # articles. Use the production row READ-ONLY whenever it out-ranks
        # the test candidate per _prefer_production_summary (absent candidate,
        # more content, or strictly newer at equal content); recency ties keep
        # the test row, which preserves the date-rollover grace the
        # >= yesterday window exists for. Production accounting is never
        # touched: the delivery write-back keys on run_mode='test', which
        # matches no row and is a silent no-op UPDATE. Production mode never
        # falls back — it must not read test rows.
        production_row = _repo().fetch_latest_summary(
            run_mode="production",
            min_date=min_run_date,
        )
        if _prefer_production_summary(summary, production_row):
            logger.info(
                "Using the production macro-summary row (run_date %s) for the "
                "QA re-render — test candidate absent, stale, or content-empty.",
                production_row.get("run_date"),
            )
            summary = production_row
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

    result = _llm().complete_json(**prompts.thematic_prompt(groups).kwargs())
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


def _citation_display_map(bullets: list[dict], sources: Optional[list[dict]],
                          signals: Optional[list[dict]] = None) -> dict[int, int]:
    """Map raw cited source id -> sequential display number (1..N), ordered by
    first appearance across bullets then macro-outlook signals. Only ids that
    have a matching source entry
    are numbered, so legacy rows (no executive_sources) yield an empty map."""
    src_ids = {s["id"] for s in (sources or []) if isinstance(s, dict) and "id" in s}
    order: list = []

    def _collect(items):
        for it in items or []:
            if not isinstance(it, dict):
                continue
            for cid in it.get("citation_source_ids") or []:
                if cid in src_ids and cid not in order:
                    order.append(cid)

    # Bullets first, then macro-outlook signals — one shared numbering space
    # across the executive summary, the macro section, and the Sources footer.
    _collect(bullets)
    _collect(signals)
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


def _structured_exec_bullets(macro_summary: dict | None):
    """Return executive_bullets when it's a non-empty list of dict bullets; else
    None (legacy/empty/malformed rows). Shared gate so the inline citation
    markers and the bottom-of-email Sources section agree on when to render and
    derive the same display-number map.

    A legacy/malformed row whose executive_bullets is a list of strings would
    otherwise render blank "• :" rows and skip the prose fallback.
    """
    bullets = (macro_summary or {}).get("executive_bullets")
    if isinstance(bullets, list) and bullets and all(isinstance(b, dict) for b in bullets):
        return bullets
    return None


def _macro_outlook_signals(macro_summary: dict | None) -> list:
    """The macro-outlook signals list from the summary row, or [] when absent or
    malformed. Sliced to prompts.MAX_MACRO_OUTLOOK_SIGNALS so a row stored
    before the cap reduction numbers its footer/citations from the same signals
    the sliced outlook body renders (no orphan Sources entries on QA re-renders).
    Feeds the shared citation numbering (bullets then signals)."""
    outlook = (macro_summary or {}).get("macro_outlook")
    if isinstance(outlook, dict) and isinstance(outlook.get("signals"), list):
        return outlook["signals"][:prompts.MAX_MACRO_OUTLOOK_SIGNALS]
    return []


# Direction is factual, not valenced: "Rising" is adverse for cost-side
# indicators (inflation, energy, freight) but favorable for demand-side ones,
# and the signal carries no good/bad field. So direction is styled neutrally —
# the Americhem good/bad reading lives in the implication text, never the color.
_MACRO_DIRECTION_COLOR = "#475569"


def _render_macro_outlook_section(macro_outlook: dict | None, macro_summary: dict | None) -> str:
    """Render the Macroeconomic Outlook section: a one-line current condition
    plus one compact row per material macro signal (indicator, direction,
    Americhem implication, affected segments, inline citation). Returns '' when
    there is no outlook or no signal. All untrusted text is escaped; citation
    markers share the email's single numbering space (bullets then signals)."""
    if not macro_outlook:
        return ""
    signals = macro_outlook.get("signals") or []
    if not signals:
        return ""

    current = html.escape(macro_outlook.get("current_condition") or "")
    sources = (macro_summary or {}).get("executive_sources") or []
    bullets = _structured_exec_bullets(macro_summary)
    display_map = _citation_display_map(bullets, sources, signals)
    src_by_id = {s["id"]: s for s in sources if isinstance(s, dict) and "id" in s}

    rows_html = ""
    for sig in signals:
        if not isinstance(sig, dict):
            continue
        indicator = html.escape(sig.get("indicator") or "")
        direction = html.escape(sig.get("direction") or "")
        implication = html.escape(sig.get("americhem_implication") or "")
        segments = ", ".join(html.escape(str(s)) for s in (sig.get("affected_segments") or []))
        marker = _render_citation_marker(sig.get("citation_source_ids"), src_by_id, display_map)
        dir_color = _MACRO_DIRECTION_COLOR
        seg_html = (
            f'<span style="color:#9CA3AF;">&nbsp;&#9679;&nbsp;</span>{segments}'
            if segments else ""
        )
        rows_html += (
            f'<tr><td style="padding:6px 0 8px 0;border-bottom:1px solid #F1F3F5;">'
            f'<p style="margin:0 0 2px 0;font-size:12px;color:#6B7280;'
            f'font-family:Arial,sans-serif;">'
            f'<strong style="color:{_BRAND_NAVY};font-size:13px;">{indicator}</strong>'
            f'<span style="color:#9CA3AF;">&nbsp;&#9679;&nbsp;</span>'
            f'<span style="color:{dir_color};font-weight:600;">{direction}</span>'
            f'{seg_html}</p>'
            f'<p style="margin:0;font-size:13px;color:#374151;'
            f"font-family:Georgia,'Times New Roman',serif;line-height:1.5;\">"
            f'{implication}{marker}</p>'
            f'</td></tr>'
        )

    return f"""
      <tr>
        <td style="padding:24px 32px 4px 32px;">
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            {_section_header_row("MACROECONOMIC OUTLOOK", title_color=_BRAND_NAVY, rule_color=_BRAND_NAVY)}
            <tr>
              <td>
                <p style="margin:0 0 8px 0;font-size:13px;color:#1a2a45;
                           font-family:Georgia,'Times New Roman',serif;line-height:1.6;">
                  {current}
                </p>
                <table width="100%" cellpadding="0" cellspacing="0" border="0">{rows_html}</table>
              </td>
            </tr>
          </table>
        </td>
      </tr>"""


def _render_exec_summary(macro_summary: dict | None) -> str:
    """Render the Executive Summary row.

    Prefers structured executive_bullets; falls back to legacy executive_summary prose.
    Returns empty string when no summary data is present. The cited-source list
    itself is rendered separately at the bottom of the email by
    _render_sources_section, not inside this block.
    """
    if not macro_summary:
        return ""

    sources = macro_summary.get("executive_sources") or []
    legacy_text = macro_summary.get("executive_summary") or ""
    condition = (
        macro_summary.get("dominant_condition")
        or macro_summary.get("macro_sentiment")
        or ""
    )

    bullets = _structured_exec_bullets(macro_summary)
    if bullets is not None:
        # Include signals so bullet numbering shares one space with the macro
        # section (bullets enumerate first, so their numbers are unchanged).
        display_map = _citation_display_map(bullets, sources, _macro_outlook_signals(macro_summary))
        body_html = _render_executive_bullets(bullets, sources, display_map)
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


def _render_sources_section(macro_summary: dict | None) -> str:
    """Render the cited-source list as a full-width row at the very bottom of the
    email. Reuses the same display-number map as the inline citation markers in
    the executive summary AND the macro outlook, so the numbering is identical.
    Returns '' when nothing is cited (legacy rows, or no bullet/signal cited
    anything)."""
    if not macro_summary:
        return ""
    bullets = _structured_exec_bullets(macro_summary)
    signals = _macro_outlook_signals(macro_summary)
    sources = macro_summary.get("executive_sources") or []
    display_map = _citation_display_map(bullets, sources, signals)
    footer_html = _render_sources_footer(sources, display_map)
    if not footer_html:
        return ""
    return f"""
      <tr>
        <td style="padding:0 32px 8px 32px;">
          {footer_html}
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
# 3. Report preparation (the run's single effectful step) + pure renderer
# ---------------------------------------------------------------------------

def prepare_report(
    rows: list[dict],
    macro_summary: dict | None,
    *,
    config: dict | None = None,
) -> ReportModel:
    """Assemble the report model and perform the run's two side effects —
    the daily_summaries write-back (repo seam, same-day-retry merge) and
    thematic synthesis (LLM seam) — exactly once, in that order.

    Both effects are skipped for the no_news variant: that path never wrote
    back, and there is nothing to synthesize. config=None loads
    market_pulse_config.yaml; tests pass a dict. The returned model is ready
    for render_report."""
    cfg = config if config is not None else _load_mp_config()
    model = assemble_report(rows, macro_summary, cfg)
    if model.variant == "daily":
        _update_delivery_summary_counts(
            surfaced_count=model.surfaced_count,
            ledger=model.ledger,
        )
        synthesis = synthesize_thematic_paragraphs(model.synthesis_candidates())
        model = model.with_synthesis(synthesis)
    return model


def render_report(
    model: ReportModel,
    *,
    today_str: str,
    test_mode: bool = False,
) -> str:
    """Render the report model to the final email HTML.

    Pure: same (model, today_str, test_mode) -> same bytes. The clock and the
    MARKET_PULSE_RUN_MODE resolution belong to the caller. A model whose
    synthesis is empty renders bullets-only — that IS the fallback, so tests
    may render an unprepared model directly. test_mode=True adds the [TEST]
    title prefix, the amber banner row, and the QA suppression summary."""
    title_prefix = "[TEST] " if test_mode else ""
    test_banner_row = _TEST_BANNER_ROW if test_mode else ""

    if model.variant == "no_news":
        return _render_no_news_email(
            today_str=today_str,
            title_prefix=title_prefix,
            test_banner_row=test_banner_row,
        )

    macro_summary = model.macro_summary

    sections_html = _render_segment_watch_section(model.groups, model.synthesis)
    additional_html = _render_additional_articles_section(list(model.additional_articles))
    exec_html = _render_exec_summary(macro_summary)
    macro_outlook_html = _render_macro_outlook_section(model.macro_outlook, macro_summary)

    # Cited-source list, rendered at the very bottom of the email (below the
    # segment-watch content) rather than under the executive summary block.
    sources_html = _render_sources_section(macro_summary)

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

    qa_html = _render_qa_debug_section(macro_summary) if test_mode else ""

    subtitle = (
        f"{today_str} &nbsp;&middot;&nbsp; "
        f"{model.surfaced_count} surfaced signals from {model.screened_count} screened items"
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
            {macro_outlook_html}
            {sections_html}
            {additional_html}
            {sources_html}
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
# 4. No-news fallback (render_report dispatches here on the no_news variant)
# ---------------------------------------------------------------------------

def _render_no_news_email(
    *,
    today_str: str,
    title_prefix: str,
    test_banner_row: str,
) -> str:
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
    else:
        critical_count  = sum(1 for r in data if _alert_tier(r) == "CRITICAL")
        strategic_count = sum(1 for r in data if _alert_tier(r) == "STRATEGIC")
        routine_count   = sum(1 for r in data if _alert_tier(r) == "ROUTINE")
        logger.info(
            "Rendering email — critical: %d | strategic: %d | routine: %d",
            critical_count, strategic_count, routine_count,
        )

    model = prepare_report(data, macro_summary)
    html = render_report(
        model,
        today_str=datetime.now().strftime("%A, %B %d, %Y"),
        test_mode=_is_test_mode(),
    )
    send_email(html)


if __name__ == "__main__":
    execute_pipeline()
