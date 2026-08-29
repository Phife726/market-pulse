import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional


from suppression_ledger import SuppressionLedger
from daily_intelligence_repo import _repo
from llm import _llm
from mailer import EmailMessage, _mailer
from run_instant import RunInstant
import prompts
import config
from scoring import tier as _alert_tier
# Report assembly lives in report.py (the pure decision pipeline) and
# rendering in renderer.py (the pure email renderer); tests exercise their
# internals via those modules directly.
from report import ReportModel, assemble_report
from renderer import render_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. Data fetch — the delivery window, anchored to the last production email
# ---------------------------------------------------------------------------

#: Wall-clock lookback used ONLY when no prior production delivery is recorded
#: (fresh database, or migration 007 not yet applied). 72 h on Mondays so a
#: fallback run still covers the weekend.
FALLBACK_LOOKBACK_HOURS = 24
FALLBACK_LOOKBACK_HOURS_MONDAY = 72

#: The delivery-window anchor and the QA fallback summary are always the
#: PRODUCTION row, whatever mode this run is in (see CONTEXT.md).
ANCHOR_RUN_MODE = "production"


@dataclass(frozen=True)
class DeliveryWindow:
    """The rows this email carries: everything created strictly after
    `cutoff`. `anchored` says whether the cutoff is the last recorded
    production delivery (exact "since the last email" semantics) or the
    wall-clock fallback."""
    cutoff: datetime
    anchored: bool


def delivery_window(now: datetime, last_delivered_at: Optional[datetime]) -> DeliveryWindow:
    """Pure: choose the window for a delivery running at `now` (UTC).

    A recorded delivery wins outright — it is exact, so neither a late
    scheduled start (issue #64: 13:53 UTC instead of ~10:25 pushed a 24 h
    window past every row of the previous day) nor the Monday rule needs to
    widen it. Without one, fall back to the legacy wall-clock lookback."""
    if last_delivered_at is not None:
        return DeliveryWindow(cutoff=last_delivered_at, anchored=True)
    hours = FALLBACK_LOOKBACK_HOURS_MONDAY if now.weekday() == 0 else FALLBACK_LOOKBACK_HOURS
    return DeliveryWindow(cutoff=now - timedelta(hours=hours), anchored=False)


def fetch_todays_intelligence(run: RunInstant) -> list[dict]:
    """Rows created since the last production email (see delivery_window).

    The anchor is always the PRODUCTION delivery on an earlier run_date,
    whatever mode this run is in: a QA re-render must see the rows production
    saw, and a same-day retry re-sends the whole day's window rather than only
    what arrived since the morning email. `run` is the run instant — the one
    clock reading this run makes (execute_pipeline stamps `run.now` after a
    successful send); it is naive UTC to match created_at. Propagates a failed read —
    a no-news email on a database outage would be wrong, and its stamp would
    hide the rows the outage concealed."""
    anchor = _repo().fetch_last_delivery(run_mode=ANCHOR_RUN_MODE, before_date=run.run_date)
    window = delivery_window(run.now, anchor)
    rows = _repo().fetch_since(window.cutoff)
    if window.anchored:
        logger.info(
            "Fetched %d intelligence record(s) created after the last production "
            "delivery at %s.", len(rows), window.cutoff.isoformat(),
        )
    else:
        logger.warning(
            "No prior production delivery recorded — fetched %d intelligence "
            "record(s) with the wall-clock fallback window (cutoff %s).",
            len(rows), window.cutoff.isoformat(),
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


def fetch_macro_summary(run: RunInstant) -> dict | None:
    """The macro-summary row for this run: the latest row in the run's mode
    dated on or after the run instant's yesterday (the date-rollover grace)."""
    summary = _repo().fetch_latest_summary(
        run_mode=run.run_mode,
        min_date=run.min_summary_date,
    )
    if run.test_mode:
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
            run_mode=ANCHOR_RUN_MODE,
            min_date=run.min_summary_date,
        )
        if _prefer_production_summary(summary, production_row):
            logger.info(
                "Using the production macro-summary row (run_date %s) for the "
                "QA re-render — test candidate absent, stale, or content-empty.",
                production_row.get("run_date"),
            )
            summary = production_row
    if summary is None:
        logger.warning("No macro summary found for run_date >= %s.", run.min_summary_date)
    return summary


def _update_delivery_summary_counts(
    *,
    run: RunInstant,
    surfaced_count: int,
    ledger: SuppressionLedger,
) -> None:
    """Update the run's daily_summaries row (keyed on the run instant's
    run_date + run_mode) with the delivery-side surfaced count
    and merged suppression accounting. Idempotent on same-day retry — the
    merge semantics live in SuppressionLedger.merge_with().

    Non-critical: a failed write is logged but does not raise. Keeps the
    email-sending path resilient to transient Supabase outages."""
    try:
        prior_row = _repo().require_delivery_state(run_date=run.run_date, run_mode=run.run_mode)
        prior = SuppressionLedger.from_row("delivery", prior_row)
        merged = ledger.merge_with(prior)
        _repo().update_delivery_counts(
            run_date=run.run_date,
            run_mode=run.run_mode,
            surfaced_count=surfaced_count,
            ledger_row=merged.to_row(),
        )
    except Exception as exc:
        logger.warning("Failed to update delivery counts on daily_summaries: %s", exc)


def _record_delivery(run: RunInstant) -> None:
    """Stamp delivered_at on the run's daily_summaries row — the anchor the
    next run's delivery window starts from. The stamp is `run.now`, the
    instant this run's fetch ran (its window's "now"), not the send time: the anchor
    may only move past rows this email actually carried. Called only after
    the send succeeded, so a failed send leaves the previous anchor in place
    and the next successful email reaches back over the rows that never went
    out; a failed fetch never gets here at all (fetch_since is a strict read
    that raises).

    Non-critical: the email has already gone out, so a failure here is a
    warning, not a red job (which would invite a manual re-run and a
    duplicate email). In test mode this keys on run_mode='test' and is a
    silent no-op on production accounting."""
    try:
        _repo().record_delivery(
            run_date=run.run_date,
            run_mode=run.run_mode,
            delivered_at=run.now,
        )
    except Exception as exc:
        logger.warning("Failed to stamp delivered_at on daily_summaries: %s", exc)


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
    # The seam returns the parsed JSON unvalidated; this caller's validation is
    # the shape the renderer relies on — one prose string per category. A list
    # of sentences or a nested object becomes bullets-only for that category.
    paragraphs = {k: v for k, v in result.items() if isinstance(k, str) and isinstance(v, str)}
    if len(paragraphs) != len(result):
        logger.warning("Thematic synthesis returned %d non-text entries — dropped.",
                       len(result) - len(paragraphs))
    logger.info("Thematic synthesis complete — %d categories.", len(paragraphs))
    return paragraphs


# ---------------------------------------------------------------------------
# 3. Report preparation (the run's single effectful step)
# ---------------------------------------------------------------------------

def prepare_report(
    rows: list[dict],
    macro_summary: dict | None,
    *,
    run: RunInstant,
    report_config: dict | None = None,
) -> ReportModel:
    """Assemble the report model and perform the run's two side effects —
    the daily_summaries write-back (repo seam, same-day-retry merge) and
    thematic synthesis (LLM seam) — exactly once, in that order.

    Both effects are skipped for the no_news variant: that path never wrote
    back, and there is nothing to synthesize. `run` keys the write-back on
    the run instant's run_date + run_mode. report_config=None loads
    market_pulse_config.yaml; tests pass a dict. The returned model is ready
    for render_report."""
    cfg = report_config if report_config is not None else config.mp_config()
    model = assemble_report(rows, macro_summary, cfg)
    if model.variant == "daily":
        _update_delivery_summary_counts(
            run=run,
            surfaced_count=model.surfaced_count,
            ledger=model.ledger,
        )
        synthesis = synthesize_thematic_paragraphs(model.synthesis_candidates())
        model = model.with_synthesis(synthesis)
    return model


# ---------------------------------------------------------------------------
# 4. Email delivery
# ---------------------------------------------------------------------------

def send_email(html_content: str, *, run: RunInstant) -> None:
    """Compose this run's digest email and hand it to the mailer seam.

    Addressing is the consumer's: `SENDER_EMAIL` is the sender and
    `RECIPIENT_EMAILS` (comma-separated) is the only source of the recipient
    list; the subject's date and `[TEST]` marker come from the run instant.
    Transport and retry are the seam's; its failures propagate, so
    `execute_pipeline` never stamps `delivered_at` for an unsent email.
    """
    recipients = tuple(
        e.strip() for e in os.environ["RECIPIENT_EMAILS"].split(",") if e.strip()
    )
    if not recipients:
        raise ValueError("RECIPIENT_EMAILS is set but contains no addresses — nothing to send to")
    subject = f"Americhem Market-Pulse \u2014 {run.subject_date}"
    if run.test_mode:
        subject = f"[TEST] {subject}"

    _mailer().send(EmailMessage(
        sender=os.environ["SENDER_EMAIL"],
        recipients=recipients,
        subject=subject,
        html=html_content,
    ))


# ---------------------------------------------------------------------------
# 5. Entrypoint
# ---------------------------------------------------------------------------

def execute_pipeline(run: RunInstant) -> None:
    # `run` is the run instant main() read once (see CONTEXT.md): its `now` is
    # the window's "now" and the delivered_at stamp (see _record_delivery);
    # its run_mode keys every daily_summaries read and write.
    data          = fetch_todays_intelligence(run)
    macro_summary = fetch_macro_summary(run)

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

    model = prepare_report(data, macro_summary, run=run)
    html = render_report(model, today_str=run.header_date, test_mode=run.test_mode)
    send_email(html, run=run)
    _record_delivery(run)


def main() -> None:
    """Cron entrypoint: fail fast on missing secrets, read the run instant
    once, then run the pipeline."""
    config.validate_environment("delivery")
    execute_pipeline(RunInstant.current())


if __name__ == "__main__":
    main()
