import logging
import os
import smtplib
import socket
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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
    """Fetch intelligence records, extending lookback to 72 h on Mondays.

    On Monday mornings the standard 24-hour window misses Friday news.
    alert_tier is computed client-side to mirror the DB view logic.

    Returns:
        List of row dicts ordered by sentiment_score ascending (critical first).
    """
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
    """Fetch the most recent executive summary from daily_summaries.

    Returns:
        Dict with 'executive_summary' and 'macro_sentiment' keys, or None.
    """
    try:
        from datetime import date

        # Query a short window and select the newest row to avoid timing races
        # between ingestion upsert and delivery read in the same workflow run.
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
# 2. HTML generation helpers
# ---------------------------------------------------------------------------

def _render_exec_summary(macro_summary: dict | None) -> str:
    """Return the HTML block for the executive summary section.

    Args:
        macro_summary: Dict with 'executive_summary' and 'macro_sentiment',
                       or None if unavailable.

    Returns:
        HTML string — empty string if no summary available.
    """
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
    """Map a sentiment score to a (word, hex_color) pair for display.

    Args:
        score: Integer sentiment score in the range 1–10.

    Returns:
        Tuple of (sentiment_word, hex_color_string).
    """
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
    """Return the HTML for a single article card.

    Args:
        item:   Row dict from daily_intelligence.
        accent: Hex color for accent bar and headline link.
        bg:     Hex color for category badge background.
        text:   Hex color for category badge text.

    Returns:
        HTML string for one card block.
    """
    headline            = item.get("headline", "No headline")
    source_url          = item.get("source_url", "#")
    americhem_impact    = item.get("americhem_impact", "")
    category            = item.get("category", "").upper()
    score               = item.get("sentiment_score", "")
    source_publication  = item.get("source_publication", "")
    sentiment_rationale = item.get("sentiment_rationale", "")
    article_summary     = item.get("article_summary", "")

    recommended_action  = item.get("recommended_action", "")
    sentiment_word, sentiment_color = _sentiment_word(int(score) if score else 5)

    source_pub_html = (
        f'<span style="font-size:11px;color:#9CA3AF;'
        f'font-family:Arial,sans-serif;">via {source_publication}</span>'
        if source_publication else ""
    )

    summary_html = (
        f'<p style="margin:0 0 8px 0;font-size:12px;color:#6B7280;'
        f'font-family:Arial,sans-serif;line-height:1.5;">'
        f'{article_summary}</p>'
        if article_summary else ""
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
        if recommended_action and recommended_action != "No action" else ""
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
                      {summary_html}
                      <p style="margin:0 0 8px 0;font-size:13px;color:#374151;
                                 font-family:Georgia,'Times New Roman',serif;
                                 line-height:1.6;">
                        <strong style="color:#111827;">Americhem impact:</strong>
                        &nbsp;{americhem_impact}
                      </p>
                      {rationale_html}
                      {action_html}
                      <table width="100%" cellpadding="0" cellspacing="0" border="0">
                        <tr>
                          <td>
                            <span style="display:inline-block;font-size:10px;
                                          font-weight:700;letter-spacing:0.8px;
                                          text-transform:uppercase;padding:2px 8px;
                                          border-radius:3px;background-color:{bg};
                                          color:{text};border:1px solid {accent};
                                          font-family:Arial,sans-serif;">
                              {category}
                            </span>
                            &nbsp;{source_pub_html}
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
    """Return HTML for one alert-tier section (header + cards).

    Returns empty string if items list is empty (no empty sections rendered).
    """
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
    """Build the full Americhem-branded BLUF HTML email.

    Table-based layout with inline CSS for Outlook compatibility.
    Logo loads from Americhem CDN; text fallback handles blocked images.

    Args:
        data:          List of intelligence rows (alert_tier must be set).
        macro_summary: Optional dict with executive_summary and macro_sentiment.

    Returns:
        Complete HTML email string ready for SMTP transmission.
    """
    critical  = [r for r in data if r.get("alert_tier") == "CRITICAL"]
    strategic = [r for r in data if r.get("alert_tier") == "STRATEGIC"]
    routine   = [r for r in data if r.get("alert_tier") == "ROUTINE"]

    sections_html = (
        _render_section("CRITICAL",  "Critical Disruptions",    "#EF4444", "#FEF2F2", "#B91C1C", critical)
        + _render_section("STRATEGIC", "Strategic Opportunities", "#22C55E", "#F0FDF4", "#15803D", strategic)
        + _render_section("ROUTINE",   "Routine Monitoring",      "#A3A3A3", "#F5F5F5", "#525252", routine)
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
    <tr>
      <td align="center">
        <table width="640" cellpadding="0" cellspacing="0" border="0"
               style="max-width:640px;background-color:#ffffff;
                      border:0.5px solid #E5E7EB;border-radius:8px;
                      overflow:hidden;">
          <tr>
            <td>

              <!-- HEADER -->
              <table width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td style="background-color:{_BRAND_NAVY};
                              padding:20px 32px 0 32px;">
                    <table width="100%" cellpadding="0" cellspacing="0" border="0">
                      <tr>
                        <td style="width:1%;white-space:nowrap;padding-right:16px;">
                          <img src="{_LOGO_URL}"
                               alt="Americhem"
                               width="140"
                               style="display:block;height:auto;max-height:40px;background-color:#ffffff;padding:3px 8px;border-radius:3px;">
                        </td>
                        <td style="width:1%;white-space:nowrap;padding-right:16px;">
                          <div style="width:1px;height:32px;
                                      background-color:rgba(255,255,255,0.25);">
                          </div>
                        </td>
                        <td>
                          <p style="margin:0;font-size:11px;font-weight:700;
                                     letter-spacing:1.5px;color:{_BRAND_GREEN};
                                     font-family:Arial,sans-serif;
                                     text-transform:uppercase;">
                            Market Intelligence
                          </p>
                          <p style="margin:2px 0 0 0;font-size:18px;font-weight:700;
                                     color:#ffffff;font-family:Arial,sans-serif;
                                     line-height:1.2;">
                            Market-Pulse: Daily Intelligence
                          </p>
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
                <tr>
                  <td style="background-color:{_BRAND_GREEN};height:3px;
                              font-size:0;line-height:0;">&nbsp;</td>
                </tr>
                <tr>
                  <td style="background-color:{_BRAND_NAVY_DARK};
                              padding:10px 32px;">
                    <table width="100%" cellpadding="0" cellspacing="0" border="0">
                      <tr>
                        <td style="font-size:12px;color:rgba(255,255,255,0.65);
                                    font-family:Arial,sans-serif;">
                          {today_str} &nbsp;&middot;&nbsp;
                          {total} {item_word} today
                        </td>
                        <td align="right">{macro_badge_html}</td>
                      </tr>
                    </table>
                  </td>
                </tr>
              </table>

              <!-- BODY -->
              <table width="100%" cellpadding="0" cellspacing="0" border="0">
                {exec_html}
                {sections_html}
                <tr><td style="height:24px;"></td></tr>
              </table>

              <!-- FOOTER -->
              <table width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td style="border-top:0.5px solid #E5E7EB;
                              background-color:#FAFAFA;padding:16px 32px;">
                    <table width="100%" cellpadding="0" cellspacing="0" border="0">
                      <tr>
                        <td style="font-size:11px;color:#9CA3AF;
                                    font-family:Arial,sans-serif;">
                          Generated by
                          <strong style="color:{_BRAND_NAVY};">
                            Americhem Market-Pulse
                          </strong>
                          &nbsp;&middot;&nbsp;
                          Powered by OpenAI &amp; Supabase
                        </td>
                        <td align="right">
                          <img src="{_LOGO_URL}"
                               alt="Americhem"
                               width="80"
                               style="display:block;height:auto;opacity:0.4;">
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
              </table>

            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>

</body>
</html>"""


# ---------------------------------------------------------------------------
# 4. No-news fallback email
# ---------------------------------------------------------------------------

def _generate_no_news_email() -> str:
    """Return a minimal branded HTML email for days with no processed articles."""
    today_str = datetime.now().strftime("%A, %B %d, %Y")

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>Americhem Market-Pulse</title></head>
<body style="margin:0;padding:0;background-color:#F3F4F6;
             font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0"
         style="background-color:#F3F4F6;padding:24px 0;">
    <tr>
      <td align="center">
        <table width="640" cellpadding="0" cellspacing="0" border="0"
               style="max-width:640px;background-color:#ffffff;
                      border:0.5px solid #E5E7EB;border-radius:8px;
                      overflow:hidden;">
          <tr>
            <td>
              <table width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td style="background-color:{_BRAND_NAVY};
                              padding:20px 32px 18px;">
                    <p style="margin:0;font-size:18px;font-weight:700;
                               color:#ffffff;font-family:Arial,sans-serif;">
                      Market-Pulse: Daily Intelligence
                    </p>
                    <p style="margin:4px 0 0 0;font-size:12px;
                               color:rgba(255,255,255,0.6);
                               font-family:Arial,sans-serif;">{today_str}</p>
                  </td>
                </tr>
                <tr>
                  <td style="background-color:{_BRAND_GREEN};height:3px;
                              font-size:0;line-height:0;">&nbsp;</td>
                </tr>
              </table>
              <table width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td style="padding:32px;">
                    <p style="margin:0;font-size:15px;color:#374151;
                               font-family:Georgia,'Times New Roman',serif;
                               line-height:1.65;">
                      No significant market events were detected in today's
                      monitoring window. All target entities were checked &mdash;
                      no articles met the relevance threshold.
                    </p>
                    <p style="margin:16px 0 0 0;font-size:13px;color:#9CA3AF;
                               font-family:Arial,sans-serif;">
                      The pipeline ran successfully. Normal delivery resumes
                      when qualifying intelligence is detected.
                    </p>
                  </td>
                </tr>
              </table>
              <table width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td style="border-top:0.5px solid #E5E7EB;
                              background-color:#FAFAFA;padding:14px 32px;">
                    <p style="margin:0;font-size:11px;color:#9CA3AF;
                               font-family:Arial,sans-serif;">
                      Generated by
                      <strong style="color:{_BRAND_NAVY};">
                        Americhem Market-Pulse
                      </strong>
                      &nbsp;&middot;&nbsp; Powered by OpenAI &amp; Supabase
                    </p>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


# ---------------------------------------------------------------------------
# 5. Email delivery
# ---------------------------------------------------------------------------

def send_email(html_content: str) -> None:
    """Send the HTML digest via SMTP with STARTTLS.

    Reads all connection parameters from environment variables.
    RECIPIENT_EMAILS is a comma-separated string split at send time.

    Args:
        html_content: Complete HTML email string.

    Raises:
        smtplib.SMTPAuthenticationError: On bad SMTP credentials.
        smtplib.SMTPException:           On SMTP-level failures.
        socket.timeout:                  If SMTP server is unreachable.
    """
    smtp_server  = os.environ["SMTP_SERVER"]
    smtp_port    = int(os.environ["SMTP_PORT"])
    smtp_user    = os.environ["SMTP_USER"]
    smtp_pass    = os.environ["SMTP_PASS"]
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

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = sender_email
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html_content, "html"))

    try:
        if smtp_port == 465:
            _ctx = smtplib.ssl.create_default_context()
            _conn = smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=30, context=_ctx)
        else:
            _conn = smtplib.SMTP(smtp_server, smtp_port, timeout=30)

        with _conn as server:
            if smtp_port != 465:
                server.ehlo()
                server.starttls()
                server.ehlo()
            server.login(smtp_user, smtp_pass)
            server.sendmail(sender_email, recipients, msg.as_string())

        logger.info(
            "Email sent — subject: '%s' | recipients: %d",
            subject,
            len(recipients),
        )

    except smtplib.SMTPAuthenticationError as exc:
        logger.error(
            "SMTP authentication failed (check SMTP_USER / SMTP_PASS): %s", exc
        )
        raise
    except smtplib.SMTPException as exc:
        logger.error("SMTP error while sending email: %s", exc)
        raise
    except socket.timeout:
        logger.error(
            "SMTP connection timed out to %s:%s", smtp_server, smtp_port
        )
        raise
    except Exception as exc:
        logger.error("Unexpected error sending email: %s", exc)
        raise


# ---------------------------------------------------------------------------
# 6. Entrypoint
# ---------------------------------------------------------------------------

def execute_pipeline() -> None:
    """Orchestrate the delivery: fetch data → generate HTML → send email.

    Sends a no-news notification when no records are found so stakeholders
    know the pipeline ran successfully even on quiet market days.
    """
    data          = fetch_todays_intelligence()
    macro_summary = fetch_macro_summary()

    if not data:
        logger.warning(
            "No intelligence records for today — sending no-news notification."
        )
        html = _generate_no_news_email()
        send_email(html)
        return

    critical_count  = sum(1 for r in data if r.get("alert_tier") == "CRITICAL")
    strategic_count = sum(1 for r in data if r.get("alert_tier") == "STRATEGIC")
    routine_count   = sum(1 for r in data if r.get("alert_tier") == "ROUTINE")
    logger.info(
        "Rendering email — critical: %d | strategic: %d | routine: %d",
        critical_count,
        strategic_count,
        routine_count,
    )

    html = generate_html_email(data, macro_summary)
    send_email(html)


if __name__ == "__main__":
    execute_pipeline()
