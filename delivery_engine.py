import json
import logging
import os
import random
import time
from datetime import datetime, timedelta

import requests
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
_LOGO_URL = (
    "https://www.americhem.com/wp-content/uploads/2025/07/logo-header.webp"
)

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
            .order("sentiment_score", desc=False)
            .execute()
        )

        records: list[dict] = []
        for row in result.data or []:
            score = row.get("sentiment_score", 5)
            if score <= 3:
                row["alert_tier"] = "CRITICAL"
            elif score >= 8:
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
            .select("run_date, executive_summary, macro_sentiment")
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


# ---------------------------------------------------------------------------
# 2. Thematic routing helpers
# ---------------------------------------------------------------------------

def _group_for_thematic(items: list[dict]) -> dict[str, list[dict]]:
    """Group qualifying articles (score 4–10) by category for thematic synthesis.

    Args:
        items: Articles pre-filtered to scores 4–10. Score 1–3 articles are
            silently skipped as a safety guard.

    Returns:
        Dict of {category: [articles]} containing only groups with 2+ articles.
        Articles with a missing or null category are grouped under 'Uncategorized'.
    """
    from collections import defaultdict
    buckets: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        score = item.get("sentiment_score") or 0
        if score <= 3:
            continue
        category = item.get("category") or "Uncategorized"
        buckets[category].append(item)
    return {cat: arts for cat, arts in buckets.items() if len(arts) >= 2}


def _collect_thin_entries(
    items: list[dict],
    groups: dict[str, list[dict]],
) -> list[dict]:
    """Return ungrouped score 7–10 articles for thin thematic rendering.

    Args:
        items: All non-critical articles (scores 4–10).
        groups: The 2+ article groups from _group_for_thematic().

    Returns:
        Single-article items scoring 7–10 not captured in any group,
        ordered by sentiment_score ascending.
    """
    grouped_hashes = {
        art.get("url_hash") for arts in groups.values() for art in arts
    }
    thin = [
        item for item in items
        if item.get("url_hash") not in grouped_hashes
        and (item.get("sentiment_score") or 0) >= 7
    ]
    return sorted(thin, key=lambda x: x.get("sentiment_score") or 0)


def _collect_peripheral(
    items: list[dict],
    groups: dict[str, list[dict]],
) -> list[dict]:
    """Return ungrouped score 4–6 articles for the Peripheral Signals section.

    Args:
        items: All non-critical articles (scores 4–10).
        groups: The 2+ article groups from _group_for_thematic().

    Returns:
        Single-article items scoring 4–6 not captured in any group,
        ordered by sentiment_score ascending.
    """
    grouped_hashes = {
        art.get("url_hash") for arts in groups.values() for art in arts
    }
    peripheral = [
        item for item in items
        if item.get("url_hash") not in grouped_hashes
        and (item.get("sentiment_score") or 0) <= 6
    ]
    return sorted(peripheral, key=lambda x: x.get("sentiment_score") or 0)


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
            score = art.get("sentiment_score", 5)
            entities = art.get("entities_mentioned") or []
            entity = entities[0] if entities else (art.get("category") or "Unknown")
            impact = art.get("americhem_impact", "")
            lines.append(f"- [{entity} | {score}/10] {impact}")
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
        entity = entities[0] if entities else (item.get("category") or "Unknown")
        score = item.get("sentiment_score", "")
        headline = item.get("headline", "")
        source_url = item.get("source_url", "#")
        bullets_html += (
            f'<tr><td style="padding:2px 0;">'
            f'<span style="font-size:12px;font-family:Arial,sans-serif;color:#6B7280;">'
            f'&bull;&nbsp;<strong style="color:#374151;">[{entity}: {score}/10]</strong>'
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
        key=lambda kv: min((a.get("sentiment_score") or 5) for a in kv[1]),
    )

    def _bullet(item: dict) -> str:
        entities = item.get("entities_mentioned") or []
        entity = entities[0] if entities else (item.get("category") or "Unknown")
        score = item.get("sentiment_score", "")
        headline = item.get("headline", "")
        source_url = item.get("source_url", "#")
        return (
            f'<tr><td style="padding:2px 0;">'
            f'<span style="font-size:12px;font-family:Arial,sans-serif;">'
            f'&bull;&nbsp;'
            f'<a href="{source_url}" style="color:{_BRAND_NAVY};text-decoration:none;'
            f'font-weight:600;">[{entity}: {score}/10]</a>'
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
            articles, key=lambda x: x.get("sentiment_score") or 0
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

    thin_sorted = sorted(thin_entries, key=lambda x: x.get("sentiment_score") or 0)
    for item in thin_sorted:
        category = item.get("category") or "Uncategorized"
        categories_html += _category_block(category, [item], "")

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

def _render_exec_summary(macro_summary: dict | None) -> str:
    if not macro_summary:
        return ""

    sentiment    = macro_summary.get("macro_sentiment", "")
    summary_text = macro_summary.get("executive_summary", "")

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
                  Executive Summary &nbsp;
                  <span style="background-color:{_BRAND_NAVY};color:#ffffff;
                                padding:2px 10px;border-radius:20px;
                                font-size:10px;font-weight:600;
                                letter-spacing:0.5px;">{sentiment}</span>
                </p>
                <p style="margin:0;font-size:14px;color:#1a2a45;
                           font-family:Georgia,'Times New Roman',serif;
                           line-height:1.65;">{summary_text}</p>
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


def _render_card(item: dict, accent: str, bg: str, text: str) -> str:
    headline            = item.get("headline", "No headline")
    source_url          = item.get("source_url", "#")
    americhem_impact    = item.get("americhem_impact", "")
    category            = item.get("category", "").upper()
    score               = item.get("sentiment_score", "")
    source_publication  = item.get("source_publication", "")
    sentiment_rationale = item.get("sentiment_rationale", "")

    recommended_action  = item.get("recommended_action", "")
    sentiment_word, sentiment_color = _sentiment_word(int(score) if score else 5)

    source_pub_html = (
        f'<span style="font-size:11px;color:#9CA3AF;'
        f'font-family:Arial,sans-serif;">via {source_publication}</span>'
        if source_publication else ""
    )

    rationale_html = (
        f'<p style="margin:0 0 10px 0;font-size:12px;color:#6B7280;'
        f'font-family:Arial,sans-serif;font-style:italic;line-height:1.4;">'
        f'Score rationale: {sentiment_rationale}</p>'
        if sentiment_rationale else ""
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
                              {category}
                            </span>
                            <span style="margin-left:6px;">{source_pub_html}</span>
                          </td>
                          <td align="right"
                              style="font-size:11px;font-family:Arial,sans-serif;">
                            <span style="color:{sentiment_color};font-weight:600;">
                              {sentiment_word}
                            </span>
                            <span style="color:#9CA3AF;">
                              &nbsp;&#9679;&nbsp;Score: {score}/10
                            </span>
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
    # Zone 1: Critical (score 1–3) — always full cards
    critical = [r for r in data if (r.get("sentiment_score") or 0) <= 3]

    # Zones 2 & 3: Thematic + Peripheral (scores 4–10)
    non_critical = [r for r in data if (r.get("sentiment_score") or 0) >= 4]
    groups       = _group_for_thematic(non_critical)
    thin_entries = _collect_thin_entries(non_critical, groups)
    peripheral   = _collect_peripheral(non_critical, groups)
    synthesis    = synthesize_thematic_paragraphs(groups)

    sections_html = (
        _render_section(
            "CRITICAL", "Critical Disruptions",
            "#EF4444", "#FEF2F2", "#B91C1C", critical,
        )
        + _render_thematic_section(groups, thin_entries, synthesis)
        + _render_peripheral_section(peripheral)
    )

    exec_html = _render_exec_summary(macro_summary)

    today_str = datetime.now().strftime("%A, %B %d, %Y")
    total     = len(data)
    item_word = "item" if total == 1 else "items"

    macro_badge_html = ""
    if macro_summary:
        sentiment = macro_summary.get("macro_sentiment", "")
        macro_badge_html = (
            f'<span style="background-color:rgba(127,176,105,0.2);'
            f'color:{_BRAND_GREEN};border:1px solid rgba(127,176,105,0.4);'
            f'padding:3px 12px;border-radius:20px;font-size:11px;font-weight:600;'
            f'font-family:Arial,sans-serif;letter-spacing:0.5px;">'
            f'{sentiment}</span>'
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
                      <p style="margin:2px 0 0 0;font-size:18px;font-weight:700;color:#ffffff;font-family:Arial,sans-serif;line-height:1.2;">Market-Pulse: Daily Intelligence</p>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr><td style="background-color:{_BRAND_GREEN};height:3px;font-size:0;line-height:0;">&nbsp;</td></tr>
            <tr>
              <td style="background-color:{_BRAND_NAVY_DARK};padding:10px 32px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="font-size:12px;color:rgba(255,255,255,0.65);font-family:Arial,sans-serif;">{today_str} &nbsp;&middot;&nbsp; {total} {item_word} today</td>
                    <td align="right">{macro_badge_html}</td>
                  </tr>
                </table>
              </td>
            </tr>
          </table>
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            {exec_html}
            {sections_html}
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
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>Americhem Market-Pulse</title></head>
<body style="margin:0;padding:0;background-color:#F3F4F6;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#F3F4F6;padding:24px 0;">
    <tr><td align="center">
      <table width="640" cellpadding="0" cellspacing="0" border="0" style="max-width:640px;background-color:#ffffff;border:0.5px solid #E5E7EB;border-radius:8px;overflow:hidden;">
        <tr><td>
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr><td style="background-color:{_BRAND_NAVY};padding:20px 32px 18px;">
              <p style="margin:0;font-size:18px;font-weight:700;color:#ffffff;font-family:Arial,sans-serif;">Market-Pulse: Daily Intelligence</p>
              <p style="margin:4px 0 0 0;font-size:12px;color:rgba(255,255,255,0.6);font-family:Arial,sans-serif;">{today_str}</p>
            </td></tr>
            <tr><td style="background-color:{_BRAND_GREEN};height:3px;font-size:0;line-height:0;">&nbsp;</td></tr>
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
