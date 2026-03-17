import logging
import os
import smtplib
import ssl
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
# Client factory
# ---------------------------------------------------------------------------

def _get_supabase() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)


# ---------------------------------------------------------------------------
# 1. Data fetch
# ---------------------------------------------------------------------------

def fetch_todays_intelligence() -> list[dict]:
    """Fetch intelligence records, extending lookback to 72 h on Mondays."""
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

        # Compute alert_tier client-side (mirrors the DB view logic)
        records = []
        for row in result.data or []:
            score = row.get("sentiment_score", 5)
            if score <= 3:
                row["alert_tier"] = "CRITICAL"
            elif score >= 8:
                row["alert_tier"] = "STRATEGIC"
            else:
                row["alert_tier"] = "ROUTINE"
            records.append(row)

        logger.info("Fetched %d intelligence record(s) (lookback: %dh).", len(records), lookback_hours)
        return records
    except Exception as exc:
        logger.error("Failed to fetch intelligence from Supabase: %s", exc)
        return []


def fetch_macro_summary() -> dict | None:
    """Fetch today's macro summary from daily_summaries table.

    Returns the row dict on success, or None if not found or on error.
    """
    try:
        today = datetime.utcnow().date().isoformat()
        supabase = _get_supabase()
        result = (
            supabase.table("daily_summaries")
            .select("executive_summary, macro_sentiment")
            .eq("run_date", today)
            .limit(1)
            .execute()
        )
        if result.data:
            return result.data[0]
        logger.warning("No macro summary found for today (%s).", today)
        return None
    except Exception as exc:
        logger.error("Failed to fetch macro summary: %s", exc)
        return None


# ---------------------------------------------------------------------------
# 2. HTML generation
# ---------------------------------------------------------------------------

# Tier configuration: (alert_tier value, emoji + label, hex colour, light bg)
_TIERS = [
    ("CRITICAL",  "🛑 CRITICAL DISRUPTIONS",     "#D32F2F", "#FFEBEE"),
    ("STRATEGIC", "📈 STRATEGIC OPPORTUNITIES",  "#388E3C", "#E8F5E9"),
    ("ROUTINE",   "📊 ROUTINE MONITORING",        "#555555", "#F5F5F5"),
]


def _render_section(label: str, colour: str, bg: str, items: list[dict]) -> str:
    """Return the HTML string for one alert-tier section."""
    rows = []

    # Section header
    rows.append(
        f'<tr><td style="padding:24px 0 8px 0;">'
        f'<h2 style="margin:0;font-size:16px;font-weight:700;'
        f'color:{colour};font-family:Arial,sans-serif;'
        f'border-bottom:2px solid {colour};padding-bottom:8px;">'
        f'{label}</h2>'
        f'</td></tr>'
    )

    if not items:
        rows.append(
            f'<tr><td style="padding:8px 0 16px 0;">'
            f'<p style="margin:0;font-size:13px;color:#9E9E9E;'
            f'font-family:Arial,sans-serif;font-style:italic;">'
            f'No items in this category today.</p>'
            f'</td></tr>'
        )
        return "\n".join(rows)

    for item in items:
        headline = item.get("headline", "")
        source_url = item.get("source_url", "#")
        americhem_impact = item.get("americhem_impact", "")
        category = item.get("category", "").upper()
        score = item.get("sentiment_score", "")
        source_publication = item.get("source_publication", "")
        sentiment_rationale = item.get("sentiment_rationale", "")

        rationale_html = (
            f'<p style="margin:0 0 8px 0;font-size:12px;color:#757575;'
            f'font-family:Arial,sans-serif;font-style:italic;line-height:1.4;">'
            f'Score rationale: {sentiment_rationale}</p>'
            if sentiment_rationale else ""
        )

        publication_html = (
            f'<span style="display:inline-block;font-size:11px;color:#9E9E9E;'
            f'font-family:Arial,sans-serif;margin-left:8px;">via {source_publication}</span>'
            if source_publication else ""
        )

        rows.append(
            f'<tr><td style="padding:8px 0 16px 0;">'

            # Card container
            f'<table width="100%" cellpadding="0" cellspacing="0" border="0">'
            f'<tr><td style="background-color:{bg};border-left:4px solid {colour};'
            f'padding:14px 16px;border-radius:0 4px 4px 0;">'

            # Headline as link
            f'<a href="{source_url}" style="font-size:15px;font-weight:700;'
            f'color:{colour};font-family:Arial,sans-serif;text-decoration:none;'
            f'line-height:1.4;">{headline}</a>'

            # Impact paragraph
            f'<p style="margin:8px 0 10px 0;font-size:13px;color:#333333;'
            f'font-family:Arial,sans-serif;line-height:1.5;">{americhem_impact}</p>'

            # Sentiment rationale (italic, small)
            + rationale_html +

            # Category badge + score badge + publication
            f'<span style="display:inline-block;font-size:11px;font-weight:700;'
            f'color:{colour};background-color:#FFFFFF;border:1px solid {colour};'
            f'border-radius:3px;padding:2px 7px;font-family:Arial,sans-serif;'
            f'letter-spacing:0.5px;margin-right:6px;">{category}</span>'

            f'<span style="display:inline-block;font-size:11px;color:#757575;'
            f'font-family:Arial,sans-serif;">Score: {score}/10</span>'

            + publication_html +

            f'</td></tr>'
            f'</table>'
            f'</td></tr>'
        )

    return "\n".join(rows)


def generate_html_email(data: list[dict], macro_summary: dict | None = None) -> str:
    """Build an Outlook-safe, inline-CSS HTML email from today's intelligence records."""
    critical  = [r for r in data if r.get("alert_tier") == "CRITICAL"]
    strategic = [r for r in data if r.get("alert_tier") == "STRATEGIC"]
    routine   = [r for r in data if r.get("alert_tier") == "ROUTINE"]

    tier_map = {"CRITICAL": critical, "STRATEGIC": strategic, "ROUTINE": routine}

    section_html = "\n".join(
        _render_section(label, colour, bg, tier_map[tier])
        for tier, label, colour, bg in _TIERS
    )

    today_str = datetime.now().strftime("%B %d, %Y")
    total = len(data)

    exec_block = ""
    if macro_summary:
        sentiment = macro_summary.get("macro_sentiment", "")
        summary_text = macro_summary.get("executive_summary", "")
        exec_block = f"""
                    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:16px;">
                      <tr>
                        <td style="background-color:#E8EAF6;border-left:4px solid #3949AB;
                                    padding:16px 20px;border-radius:0 4px 4px 0;">
                          <p style="margin:0 0 6px 0;font-size:11px;font-weight:700;letter-spacing:1px;
                                     color:#3949AB;font-family:Arial,sans-serif;text-transform:uppercase;">
                            Executive Summary &nbsp;&middot;&nbsp;
                            <span style="background-color:#3949AB;color:#FFFFFF;padding:2px 8px;
                                          border-radius:3px;font-size:10px;">{sentiment}</span>
                          </p>
                          <p style="margin:0;font-size:13px;color:#1A237E;font-family:Arial,sans-serif;
                                     line-height:1.6;">{summary_text}</p>
                        </td>
                      </tr>
                    </table>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>AmI Market-Pulse: Daily Intelligence</title>
</head>
<body style="margin:0;padding:0;background-color:#EEEEEE;font-family:Arial,sans-serif;">

  <!-- Outer wrapper -->
  <table width="100%" cellpadding="0" cellspacing="0" border="0"
         style="background-color:#EEEEEE;padding:24px 0;">
    <tr>
      <td align="center">

        <!-- Inner content table (600 px wide) -->
        <table width="600" cellpadding="0" cellspacing="0" border="0"
               style="background-color:#FFFFFF;border-radius:6px;
                      box-shadow:0 2px 6px rgba(0,0,0,0.08);">
          <tr>
            <td style="padding:0;">

              <!-- ── HEADER ── -->
              <table width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td style="background-color:#1A237E;padding:24px 32px;
                              border-radius:6px 6px 0 0;">
                    <h1 style="margin:0;font-size:20px;font-weight:700;
                                color:#FFFFFF;font-family:Arial,sans-serif;
                                letter-spacing:0.5px;">
                      AmI Market-Pulse: Daily Intelligence
                    </h1>
                    <p style="margin:6px 0 0 0;font-size:13px;color:#C5CAE9;
                               font-family:Arial,sans-serif;">
                      {today_str} &nbsp;·&nbsp; {total} item{'s' if total != 1 else ''} today
                    </p>
                  </td>
                </tr>
              </table>

              <!-- ── BODY ── -->
              <table width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td style="padding:8px 32px 24px 32px;">
                    <table width="100%" cellpadding="0" cellspacing="0" border="0">
                      {exec_block}
                      {section_html}
                    </table>
                  </td>
                </tr>
              </table>

              <!-- ── FOOTER ── -->
              <table width="100%" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td style="background-color:#F5F5F5;padding:16px 32px;
                              border-top:1px solid #E0E0E0;
                              border-radius:0 0 6px 6px;">
                    <p style="margin:0;font-size:11px;color:#9E9E9E;
                               font-family:Arial,sans-serif;text-align:center;">
                      Generated by the AmI (Americhem Intelligence) Pipeline.
                      Powered by OpenAI &amp; Supabase.
                    </p>
                  </td>
                </tr>
              </table>

            </td>
          </tr>
        </table>
        <!-- /inner -->

      </td>
    </tr>
  </table>
  <!-- /outer -->

</body>
</html>"""

    return html


def _generate_no_news_email() -> str:
    """Return a minimal HTML email for days with no processed articles."""
    today_str = datetime.now().strftime("%B %d, %Y")
    return f"""<!DOCTYPE html>
<html lang="en">
<body style="font-family:Arial,sans-serif;padding:24px;color:#333;">
  <h2 style="color:#1A237E;">AmI Market-Pulse: Daily Intelligence</h2>
  <p style="color:#666;font-size:13px;">{today_str}</p>
  <p>No significant market events were detected in today's monitoring window.
  All target entities were checked — no articles met the relevance threshold.</p>
  <hr style="border:0;border-top:1px solid #eee;margin:24px 0;">
  <p style="font-size:11px;color:#999;text-align:center;">
    Generated by the AmI (Americhem Intelligence) Pipeline.
  </p>
</body>
</html>"""


# ---------------------------------------------------------------------------
# 3. Email delivery
# ---------------------------------------------------------------------------

def send_email(html_content: str) -> None:
    """Send the HTML digest via SMTP (STARTTLS on port 587)."""
    smtp_server   = os.environ["SMTP_SERVER"]
    smtp_port     = int(os.environ["SMTP_PORT"])
    smtp_user     = os.environ["SMTP_USER"]
    smtp_pass     = os.environ["SMTP_PASS"]
    sender_email  = os.environ["SENDER_EMAIL"]
    recipients    = [e.strip() for e in os.environ["RECIPIENT_EMAILS"].split(",") if e.strip()]

    subject = f"AmI Market-Pulse \u2014 {datetime.now().strftime('%B %d, %Y')}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = sender_email
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html_content, "html"))

    try:
        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_user, smtp_pass)
            server.sendmail(sender_email, recipients, msg.as_string())

        logger.info(
            "Email sent — subject: '%s' | recipients: %d", subject, len(recipients)
        )

    except smtplib.SMTPAuthenticationError as exc:
        logger.error("SMTP authentication failed (check SMTP_USER / SMTP_PASS): %s", exc)
        raise
    except smtplib.SMTPException as exc:
        logger.error("SMTP error while sending email: %s", exc)
        raise
    except socket.timeout:
        logger.error("SMTP connection timed out to %s:%s", smtp_server, smtp_port)
        raise
    except Exception as exc:
        logger.error("Unexpected error sending email: %s", exc)
        raise


# ---------------------------------------------------------------------------
# 4. Entrypoint
# ---------------------------------------------------------------------------

def execute_pipeline() -> None:
    data = fetch_todays_intelligence()
    macro_summary = fetch_macro_summary()

    if not data:
        logger.warning("No intelligence records found — sending no-news notification.")
        send_email(_generate_no_news_email())
        return

    html = generate_html_email(data, macro_summary)
    send_email(html)


if __name__ == "__main__":
    execute_pipeline()
