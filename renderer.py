"""renderer.py — the pure email renderer.

The function from a report model to the email's HTML (see **Renderer** in
CONTEXT.md). `render_report(model, *, today_str, test_mode)` is the one
entry point; `delivery_engine.execute_pipeline` calls it between
`prepare_report` and `send_email`. The only other public name is
`TEST_MARKER`, the `[TEST] ` spelling `send_email` imports for the subject so
the subject and the title carry one marker. Everything below is the layout — the
section renderers in the order the email shows them, the card, the citation
markers and Sources footer, the test-mode markings, the no-news variant —
plus the two rules every section follows: every interpolated data value is
HTML-escaped, trusted or not (`tests/test_renderer.py` poisons every field
and renders the whole email), and every href passes `_safe_http_url` — inside
`_link`, the one place a URL becomes an anchor — (an
unsafe URL renders its text unlinked).

Pure and deterministic: same (model, today_str, test_mode) -> same bytes. No
clock, config, seam or logger — the header date and the test-mode flag are
the caller's to derive from the run instant. Imports only the pure modules
it presents (`report`, `scoring`, `suppression_ledger`) — the allow-list row in
`tests/test_purity.py`.
"""

import html
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

from suppression_ledger import ALL_CODES, SAMPLES_CAP, SuppressionAccounting, label_for
import scoring
from report import (
    CitationSet,
    ReportModel,
    structured_exec_bullets as _structured_exec_bullets,
)

__all__ = ["render_report", "TEST_MARKER"]

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
# Test-mode markings (MARKET_PULSE_RUN_MODE=test): the title/subject marker
# and the banner row. The subject is composed by delivery_engine.send_email,
# which imports the marker from here so the two spellings cannot drift.
# ---------------------------------------------------------------------------

TEST_MARKER = "[TEST] "

_TEST_BANNER_ROW = (
    f'<tr><td style="background-color:{_BRAND_AMBER};padding:8px 32px;font-size:11px;'
    f'font-weight:700;letter-spacing:1.5px;color:#ffffff;'
    f'font-family:Arial,sans-serif;text-transform:uppercase;">'
    f'TEST RUN · Jason-only QA output — not for distribution'
    f'</td></tr>'
)

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


def _section(title: str, rows: str, *, title_color: str, rule_color: str) -> str:
    """The shell around a headed section: the outer row / cell / table with
    the section header row above `rows` (the section table's own `<tr>`s).
    The executive summary (its own padding, no header) and the Sources row
    deliberately do not use it."""
    return f"""
      <tr>
        <td style="padding:24px 32px 4px 32px;">
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            {_section_header_row(title, title_color=title_color, rule_color=rule_color)}
            {rows}
          </table>
        </td>
      </tr>"""


def _single_cell(inner: str) -> str:
    """One row holding one cell — the `rows` of a section whose content is a
    single block (the outlook, the appendix, the QA block) rather than a
    list of rows (Commercial Segment Watch)."""
    return f"""<tr>
              <td>
{inner}
              </td>
            </tr>"""


# ---------------------------------------------------------------------------
# Commercial Segment Watch renderers
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
            f'<span style="color:{tag_color};font-weight:600;">{html.escape(tag)}</span>'
        )
        signal = (item.get("signal_type") or "").strip()
        signal_html = (
            f'<span style="color:#9CA3AF;">&nbsp;&#9679;&nbsp;Signal: {html.escape(signal)}</span>'
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
    the action is consumed by the suppression policy, not the reader.
    Untrusted values are HTML-escaped and the href passes through
    _safe_http_url inside _link (an unsafe URL renders the headline unlinked)."""
    meta = _render_meta_strip(item)
    headline = html.escape(item.get("headline", "") or "")
    americhem_impact = html.escape(item.get("americhem_impact", "") or "")
    tag = item.get("sentiment_tag") or ""
    glyph = _SENTIMENT_TAG_GLYPHS.get(tag)
    glyph_html = (
        f'<span style="color:{_SENTIMENT_TAG_COLORS.get(tag, "#6B7280")};'
        f'font-family:Arial,sans-serif;">{glyph}</span> '
        if glyph else ""
    )
    so_what_html = (
        f'<p style="margin:4px 0 0 0;font-size:13px;color:#374151;'
        f"font-family:Georgia,'Times New Roman',serif;line-height:1.55;\">"
        f'{glyph_html}<strong style="color:{_BRAND_NAVY};">So what:</strong> {americhem_impact}</p>'
        if americhem_impact else ""
    )
    headline_style = (
        f'font-size:14px;font-weight:700;color:{_BRAND_NAVY};'
        f'font-family:Arial,sans-serif;text-decoration:none;line-height:1.35;'
    )
    headline_html = _link(item.get("source_url"), headline,
                          style=headline_style, unlinked_style=headline_style)
    return (
        f'<tr><td style="padding:6px 0 10px 0;">'
        f'<p style="margin:0 0 4px 0;font-size:11px;color:#6B7280;'
        f'font-family:Arial,sans-serif;">{meta}</p>'
        f'{headline_html}'
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
            f'{html.escape(para)}</p>'
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
            f'font-family:Arial,sans-serif;">{html.escape(segment_label)}</p>'
            f'{para_html}'
            f'<table width="100%" cellpadding="0" cellspacing="0" border="0">{cards_html}</table>'
            f'</td></tr>'
        )

    return _section("COMMERCIAL SEGMENT WATCH", blocks_html,
                    title_color=_BRAND_NAVY, rule_color=_BRAND_NAVY)


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
    untrusted values are HTML-escaped and hrefs pass through _link's guard."""
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

        headline_html = _link(item.get("source_url"), headline,
                              style=_APPENDIX_LINK_STYLE, unlinked_style=_APPENDIX_UNLINKED_STYLE)

        rows_html += (
            f'<tr><td style="padding:5px 0 7px 0;">'
            f'{headline_html}'
            f'<p style="margin:2px 0 0 0;font-size:11px;color:#6B7280;'
            f'font-family:Arial,sans-serif;">{meta}</p>'
            f'</td></tr>'
        )

    listing = f'                <table width="100%" cellpadding="0" cellspacing="0" border="0">{rows_html}</table>'
    return _section("Additional Articles to Explore", _single_cell(listing),
                    title_color="#5a6678", rule_color="#E5E7EB")


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
    # The row's suppression columns are read through the ledger module's own
    # reader — one tolerant parser of the persisted shape, shared with the
    # same-day-retry merge, rather than a second and weaker one here. It also
    # owns the FIFO display slice, so no cap policy is re-implemented below.
    accounting = SuppressionAccounting.from_row(macro_summary)
    breakdown = accounting.breakdown
    samples = accounting.recent()

    # Escaping rule for this block: every value sourced from the row is escaped
    # (the counts too, though the reader now types them as int — the rule holds
    # by construction, not by re-deriving the type at each site); derived
    # numbers and taxonomy labels are not row data and are interpolated bare.
    suppressed_total = sum(breakdown.values())

    rows_html = ""
    # Stable display order — ingestion-side first, then delivery-side — derived
    # from the ledger taxonomy so a new reason code gets its labeled row without
    # a hand-copied list to forget (the old copy here had already drifted:
    # synthesis_failed was folded into the total but never listed).
    for code in ALL_CODES:
        if code in breakdown:
            label = label_for(code)
            rows_html += (
                f'<tr><td style="padding:2px 0;font-size:12px;color:#374151;'
                f'font-family:Arial,sans-serif;">'
                f'&nbsp;&nbsp;{label}'
                f'</td><td align="right" style="padding:2px 0;font-size:12px;'
                f'color:#374151;font-family:Arial,sans-serif;">'
                f'{html.escape(str(breakdown[code]))}</td></tr>'
            )

    samples_html = ""
    for s in samples:
        reason_label = label_for(s.reason)
        samples_html += (
            f'<tr><td style="padding:2px 0;font-size:11px;color:#6B7280;'
            f'font-family:monospace;">'
            f'[{html.escape(reason_label)}] "{html.escape(s.title)}" — {html.escape(s.url)}'
            f'</td></tr>'
        )

    summary = f"""                <p style="margin:0 0 8px 0;font-size:12px;color:#374151;
                           font-family:Arial,sans-serif;">
                  Screened: {html.escape(str(screened)) if screened is not None else '?'} &nbsp;&middot;&nbsp;
                  Surfaced: {html.escape(str(surfaced)) if surfaced is not None else '?'} &nbsp;&middot;&nbsp;
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
                  Last {SAMPLES_CAP} suppressed items
                </p>
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  {samples_html}
                </table>"""
    return _section("QA &middot; Suppression Summary", _single_cell(summary),
                    title_color="#9CA3AF", rule_color="#E5E7EB")


# ---------------------------------------------------------------------------
# HTML generation helpers
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


#: The plain in-text link style (citation markers, the Sources footer), and
#: the appendix headline's linked / unlinked styles.
_LINK_STYLE = f"color:{_BRAND_NAVY};text-decoration:none;"
_APPENDIX_LINK_STYLE = (f"font-size:13px;font-weight:600;color:{_BRAND_NAVY};"
                        f"font-family:Arial,sans-serif;text-decoration:none;line-height:1.35;")
_APPENDIX_UNLINKED_STYLE = (f"font-size:13px;font-weight:600;color:{_BRAND_NAVY};"
                            f"font-family:Arial,sans-serif;line-height:1.35;")


def _link(raw_url: Optional[str], inner: str, *, style: str,
          unlinked_style: Optional[str] = None, titled: bool = False) -> str:
    """Every link in the email: `inner` (already escaped) as an anchor when
    `raw_url` passes `_safe_http_url`, otherwise unlinked — a `<span>` in
    `unlinked_style` when given, else bare text. The href guard lives here,
    so no site can place an unguarded URL. `titled` repeats the href as a
    title attribute (the citation markers, whose visible text is a number)."""
    url = _safe_http_url(raw_url)
    if not url:
        return inner if unlinked_style is None else f'<span style="{unlinked_style}">{inner}</span>'
    safe = html.escape(url, quote=True)
    title = f' title="{safe}"' if titled else ""
    return f'<a href="{safe}"{title} style="{style}">{inner}</a>'


def _render_citation_marker(cited_ids: Optional[list], citations: CitationSet) -> str:
    """Grouped inline citation, e.g. [1, 2]. Each number links to its source URL
    (http/https only; otherwise plain text). Returns '' when nothing to show."""
    parts: list[str] = []
    for cid in cited_ids or []:
        n = citations.display_number(cid)
        if n is None:
            continue
        parts.append(_link(citations.source(cid).get("url"), str(n), style=_LINK_STYLE, titled=True))
    if not parts:
        return ""
    inner = ", ".join(parts)
    return (
        f'&nbsp;<span style="font-size:10px;color:{_BRAND_NAVY};'
        f'vertical-align:super;">[{inner}]</span>'
    )


def _render_executive_bullets(bullets: list[dict], sources: Optional[list[dict]] = None,
                              display_map: Optional[dict] = None,
                              citations: CitationSet | None = None) -> str:
    """Render the 3-bullet executive summary body, each bullet followed by its
    grouped inline citation marker when it has resolvable cited sources.

    Pass the report's `citations`; the (sources, display_map) pair is the legacy
    spelling of the same value. Both default to empty so legacy callers (and
    legacy rows with no citations) render exactly as before, with no markers.
    """
    if citations is None:
        citations = CitationSet(tuple(sources or []), display_map or {})
    items_html = ""
    for b in bullets:
        label = html.escape(b.get("label", "") if isinstance(b, dict) else "")
        body = html.escape(b.get("body", "") if isinstance(b, dict) else "")
        cited = b.get("citation_source_ids", []) if isinstance(b, dict) else []
        marker = _render_citation_marker(cited, citations)
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


def _render_sources_footer(sources: Optional[list[dict]] = None, display_map: Optional[dict] = None,
                           citations: CitationSet | None = None) -> str:
    """Render the 'Sources' footer: one row per cited source, ordered by display
    number, as '[n] headline — domain' linked to the source URL. Empty string
    when there are no cited sources.

    Pass the report's `citations`; the (sources, display_map) pair is the legacy
    spelling of the same value."""
    if citations is None:
        citations = CitationSet(tuple(sources or []), display_map or {})
    if not citations:
        return ""
    rows = ""
    for n, src in citations.ordered():
        headline = html.escape(src.get("headline") or "Headline unavailable")
        domain = html.escape(src.get("domain") or "source link")
        label = f"[{n}] {headline} &mdash; {domain}"
        entry = _link(src.get("url"), label, style=_LINK_STYLE)
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


# Direction is factual, not valenced: "Rising" is adverse for cost-side
# indicators (inflation, energy, freight) but favorable for demand-side ones,
# and the signal carries no good/bad field. So direction is styled neutrally —
# the Americhem good/bad reading lives in the implication text, never the color.
_MACRO_DIRECTION_COLOR = "#475569"


def _render_macro_outlook_section(macro_outlook: dict | None, citations: CitationSet) -> str:
    """Render the Macroeconomic Outlook section: a one-line current condition
    plus one compact row per material macro signal (indicator, direction,
    Americhem implication, affected segments, inline citation). Returns '' when
    there is no outlook or no signal. All untrusted text is escaped; citation
    numbers come from the report's citation set — the email's one numbering
    space, shared with the executive bullets and the Sources footer."""
    if not macro_outlook:
        return ""
    signals = macro_outlook.get("signals") or []
    if not signals:
        return ""

    current = html.escape(macro_outlook.get("current_condition") or "")

    rows_html = ""
    for sig in signals:
        if not isinstance(sig, dict):
            continue
        indicator = html.escape(sig.get("indicator") or "")
        direction = html.escape(sig.get("direction") or "")
        implication = html.escape(sig.get("americhem_implication") or "")
        segments = ", ".join(html.escape(str(s)) for s in (sig.get("affected_segments") or []))
        marker = _render_citation_marker(sig.get("citation_source_ids"), citations)
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

    outlook = f"""                <p style="margin:0 0 8px 0;font-size:13px;color:#1a2a45;
                           font-family:Georgia,'Times New Roman',serif;line-height:1.6;">
                  {current}
                </p>
                <table width="100%" cellpadding="0" cellspacing="0" border="0">{rows_html}</table>"""
    return _section("MACROECONOMIC OUTLOOK", _single_cell(outlook), title_color=_BRAND_NAVY, rule_color=_BRAND_NAVY)


def _render_exec_summary(macro_summary: dict | None,
                         citations: CitationSet | None = None) -> str:
    """Render the Executive Summary row.

    Prefers structured executive_bullets; falls back to legacy executive_summary prose.
    Returns empty string when no summary data is present. The cited-source list
    itself is rendered separately at the bottom of the email by
    _render_sources_section, not inside this block.
    """
    if not macro_summary:
        return ""

    if citations is None:
        citations = CitationSet.from_summary(macro_summary)
    legacy_text = macro_summary.get("executive_summary") or ""
    condition = (
        macro_summary.get("dominant_condition")
        or macro_summary.get("macro_sentiment")
        or ""
    )

    bullets = _structured_exec_bullets(macro_summary)
    if bullets is not None:
        # Bullets enumerate first in the shared numbering space, so their
        # numbers are the same whether or not the macro section renders.
        body_html = _render_executive_bullets(bullets, citations=citations)
    elif legacy_text:
        body_html = (
            f'<p style="margin:0;font-size:14px;color:#1a2a45;'
            f"font-family:Georgia,'Times New Roman',serif;line-height:1.65;\">"
            f'{html.escape(legacy_text)}</p>'
        )
    else:
        return ""

    badge_html = ""
    if condition:
        badge_html = (
            f'&nbsp;<span style="background-color:{_BRAND_NAVY};color:#ffffff;'
            f'padding:2px 10px;border-radius:20px;font-size:10px;font-weight:600;'
            f'letter-spacing:0.5px;">{html.escape(condition)}</span>'
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


def _render_sources_section(macro_summary: dict | None,
                            citations: CitationSet | None = None) -> str:
    """Render the cited-source list as a full-width row at the very bottom of the
    email. Numbers come from the same citation set as the inline markers in the
    executive summary AND the macro outlook, so the numbering is identical.
    Returns '' when nothing is cited (legacy rows, or no bullet/signal cited
    anything)."""
    if not macro_summary:
        return ""
    if citations is None:
        citations = CitationSet.from_summary(macro_summary)
    footer_html = _render_sources_footer(citations=citations)
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

_SENTIMENT_TAG_GLYPHS: dict[str, str] = {
    "Negative": "&#9660;",  # ▼
    "Neutral":  "&#9679;",  # ●
    "Positive": "&#9650;",  # ▲
}


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
    title_prefix = TEST_MARKER if test_mode else ""
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
    # One citation set, built during assembly, read by all three citation-bearing
    # sections — the numbering agrees by construction, not by convention.
    citations = model.citations
    exec_html = _render_exec_summary(macro_summary, citations)
    macro_outlook_html = _render_macro_outlook_section(model.macro_outlook, citations)

    # Cited-source list, rendered at the very bottom of the email (below the
    # segment-watch content) rather than under the executive summary block.
    sources_html = _render_sources_section(macro_summary, citations)

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
            f'{html.escape(dominant_condition)}</span>'
        )

    qa_html = _render_qa_debug_section(macro_summary) if test_mode else ""

    subtitle = (
        f"{html.escape(today_str)} &nbsp;&middot;&nbsp; "
        f"{model.surfaced_count} surfaced signals from {html.escape(str(model.screened_count))} screened items"
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
# No-news fallback (render_report dispatches here on the no_news variant)
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
              <p style="margin:4px 0 0 0;font-size:12px;color:rgba(255,255,255,0.6);font-family:Arial,sans-serif;">{html.escape(today_str)}</p>
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
