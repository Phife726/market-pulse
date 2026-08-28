"""Delivery window anchoring (issue #64).

The delivery step used to fetch "rows created in the last 24 h" by its own
wall clock. A late scheduled start (2026-08-27 began at 13:53 UTC instead of
~10:25) pushed that window past all of the previous day's rows and delivered
nothing from it. The window is now anchored to the last recorded production
delivery: rows created strictly after it are delivered. The wall-clock
lookback (24 h; 72 h on Mondays) survives only as the fallback when no prior
delivery is recorded.

Every clock-sensitive path takes the **run instant** (CONTEXT.md) — the one
clock reading a run makes, plus its run mode — so no test here reads the
process clock or MARKET_PULSE_RUN_MODE. Pure window arithmetic is tested
through ``delivery_window``; the fetch and record paths run against
``InMemoryIntelligenceRepo`` with no patching beyond the repo seam; the
end-to-end runs go through the shared ``run_delivery_pipeline`` harness.
"""
from dataclasses import replace
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

import delivery_engine
from daily_intelligence_repo import InMemoryIntelligenceRepo
from delivery_engine import delivery_window, fetch_todays_intelligence
from run_instant import RunInstant
from tests.conftest import RUN_INSTANT, TEST_RUN_INSTANT

# Thursday 2026-08-27 14:01 UTC — the late start that motivated the fix.
T = RUN_INSTANT.now
RUN_DATE = RUN_INSTANT.run_date
MONDAY = datetime(2026, 8, 31, 10, 30, 0)
MONDAY_RUN = replace(RUN_INSTANT, now=MONDAY)

_H = timedelta(hours=1)


def _row(url_hash: str, created_at: datetime) -> dict:
    return {
        "url_hash": url_hash,
        "headline": f"Headline {url_hash}",
        "americhem_impact_score": 8,
        "created_at": created_at.isoformat(),
    }


def _summary(fake: InMemoryIntelligenceRepo, run_date: str, run_mode: str = "production") -> None:
    fake.upsert_summary({
        "run_date": run_date, "run_mode": run_mode,
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": {}, "suppression_samples": [],
    })


# ---------------------------------------------------------------------------
# delivery_window — pure
# ---------------------------------------------------------------------------

def test_window_anchors_to_last_delivery():
    anchor = T - 27 * _H
    window = delivery_window(T, anchor)
    assert window.cutoff == anchor
    assert window.anchored is True


def test_window_falls_back_to_24h_without_anchor():
    window = delivery_window(T, None)
    assert window.cutoff == T - 24 * _H
    assert window.anchored is False


def test_window_falls_back_to_72h_on_monday_without_anchor():
    window = delivery_window(MONDAY, None)
    assert window.cutoff == MONDAY - 72 * _H
    assert window.anchored is False


def test_window_anchor_supersedes_monday_rule():
    """Friday's delivery already covers the weekend — the anchor is exact, so
    Monday does not widen it further (that would re-deliver Friday's rows)."""
    friday_delivery = MONDAY - 71 * _H
    window = delivery_window(MONDAY, friday_delivery)
    assert window.cutoff == friday_delivery
    assert window.anchored is True


# ---------------------------------------------------------------------------
# fetch_todays_intelligence — the issue-#64 gate and its neighbours
# ---------------------------------------------------------------------------

def test_late_start_still_delivers_previous_days_rows(monkeypatch):
    """The gate from issue #64: rows created at T-25h (yesterday's run) and
    T-1h, delivery at T, yesterday's email recorded at T-27h — both sets are
    fetched. Under the old 24 h window the T-25h row was silently dropped."""
    fake = InMemoryIntelligenceRepo(now=lambda: T)
    fake.upsert_insight(_row("yesterday", T - 25 * _H))
    fake.upsert_insight(_row("today", T - 1 * _H))
    _summary(fake, "2026-08-26")
    fake.record_delivery(run_date="2026-08-26", run_mode="production",
                         delivered_at=T - 27 * _H)
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    rows = fetch_todays_intelligence(RUN_INSTANT)

    assert {r["url_hash"] for r in rows} == {"yesterday", "today"}


def test_normal_day_does_not_redeliver_rows_before_last_email(monkeypatch):
    """Rows stored before yesterday's email went out were in yesterday's
    email; the strict cutoff keeps them out of today's."""
    anchor = T - 27 * _H
    fake = InMemoryIntelligenceRepo(now=lambda: T)
    fake.upsert_insight(_row("already-sent", anchor - timedelta(minutes=1)))
    fake.upsert_insight(_row("at-anchor", anchor))
    fake.upsert_insight(_row("after-anchor", anchor + timedelta(minutes=1)))
    _summary(fake, "2026-08-26")
    fake.record_delivery(run_date="2026-08-26", run_mode="production", delivered_at=anchor)
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    rows = fetch_todays_intelligence(RUN_INSTANT)

    assert {r["url_hash"] for r in rows} == {"after-anchor"}


def test_without_recorded_delivery_falls_back_to_wall_clock(monkeypatch):
    """Pre-migration / first-run behaviour is the legacy 24 h window."""
    fake = InMemoryIntelligenceRepo(now=lambda: T)
    fake.upsert_insight(_row("yesterday", T - 25 * _H))
    fake.upsert_insight(_row("today", T - 1 * _H))
    _summary(fake, "2026-08-26")   # a summary row with no delivered_at
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    rows = fetch_todays_intelligence(RUN_INSTANT)

    assert {r["url_hash"] for r in rows} == {"today"}


def test_fallback_uses_72h_on_monday(monkeypatch):
    fake = InMemoryIntelligenceRepo(now=lambda: MONDAY)
    fake.upsert_insight(_row("friday", MONDAY - 70 * _H))
    fake.upsert_insight(_row("last-week", MONDAY - 74 * _H))
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    rows = fetch_todays_intelligence(MONDAY_RUN)

    assert {r["url_hash"] for r in rows} == {"friday"}


def test_todays_delivery_does_not_anchor_a_same_day_retry(monkeypatch):
    """A same-day re-run re-sends the whole day's window (plus anything new)
    rather than only what arrived since this morning's email — the anchor is
    the last delivery on an EARLIER run_date."""
    fake = InMemoryIntelligenceRepo(now=lambda: T)
    fake.upsert_insight(_row("yesterday-late", T - 25 * _H))
    fake.upsert_insight(_row("this-morning", T - 3 * _H))
    fake.upsert_insight(_row("just-now", T - timedelta(minutes=10)))
    _summary(fake, "2026-08-26")
    fake.record_delivery(run_date="2026-08-26", run_mode="production",
                         delivered_at=T - 27 * _H)
    _summary(fake, RUN_DATE)
    fake.record_delivery(run_date=RUN_DATE, run_mode="production",
                         delivered_at=T - timedelta(minutes=30))
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    rows = fetch_todays_intelligence(RUN_INSTANT)

    assert {r["url_hash"] for r in rows} == {"yesterday-late", "this-morning", "just-now"}


def test_test_mode_reads_the_production_anchor(monkeypatch):
    """A QA re-render must see the rows production saw. Test-mode deliveries
    never anchor anything — a late QA run yesterday would otherwise shrink
    today's window."""
    fake = InMemoryIntelligenceRepo(now=lambda: T)
    fake.upsert_insight(_row("yesterday", T - 25 * _H))
    fake.upsert_insight(_row("today", T - 1 * _H))
    _summary(fake, "2026-08-26", "production")
    fake.record_delivery(run_date="2026-08-26", run_mode="production",
                         delivered_at=T - 27 * _H)
    _summary(fake, "2026-08-26", "test")
    fake.record_delivery(run_date="2026-08-26", run_mode="test",
                         delivered_at=T - 2 * _H)   # late QA run yesterday
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    rows = fetch_todays_intelligence(TEST_RUN_INSTANT)

    assert {r["url_hash"] for r in rows} == {"yesterday", "today"}


def test_fetch_passes_the_window_cutoff_to_the_repo(monkeypatch):
    fake = MagicMock(spec=InMemoryIntelligenceRepo)
    fake.fetch_last_delivery.return_value = None
    fake.fetch_since.return_value = []
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    fetch_todays_intelligence(RUN_INSTANT)

    fake.fetch_last_delivery.assert_called_once_with(
        run_mode="production", before_date=RUN_DATE,
    )
    fake.fetch_since.assert_called_once_with(T - 24 * _H)


# ---------------------------------------------------------------------------
# Recording the delivery — execute_pipeline wiring
# ---------------------------------------------------------------------------

def _seed(run_mode: str = "production") -> InMemoryIntelligenceRepo:
    fake = InMemoryIntelligenceRepo(now=lambda: T)
    fake.upsert_insight({
        **_row("wire0", T - 1 * _H),
        "sentiment_tag": "Neutral", "signal_type": "Customer",
        "commercial_segment": "Healthcare", "americhem_impact": "Effect.",
        "source_url": "https://x/wire0", "entities_mentioned": ["Acme"],
    })
    _summary(fake, RUN_DATE, run_mode)
    return fake


def test_write_back_and_delivery_stamp_land_on_the_same_row(run_delivery_pipeline):
    """The reason the run instant exists: the write-back (surfaced_count +
    merged suppression) and the delivered_at stamp both key on the run's
    (run_date, run_mode) — one value, one row — and the stamp is the run's
    `now`, which is what tomorrow's anchor read returns. Before, the write-back
    keyed on the process's local date.today() while the stamp keyed on the UTC
    fetch instant, and only the UTC runner kept them on the same row."""
    fake = _seed()

    result = run_delivery_pipeline(fake)

    assert len(result.sent) == 1   # exactly one email per run
    row = fake.get_delivery_state(run_date=RUN_DATE, run_mode="production")
    assert row["surfaced_count"] == 1
    assert row["delivered_at"], "delivered_at must be stamped after a successful send"
    assert fake.fetch_last_delivery(run_mode="production", before_date="2999-01-01") == T
    # ...and the same instant composed the subject that crossed the mailer seam.
    assert result.sent[-1].subject == f"Americhem Market-Pulse \u2014 {RUN_INSTANT.subject_date}"


def test_execute_pipeline_records_delivery_for_no_news_variant(run_delivery_pipeline):
    """A no-news email is still an email: the next run anchors on it."""
    fake = InMemoryIntelligenceRepo(now=lambda: T)     # no rows at all
    _summary(fake, RUN_DATE)                           # accounting-only-ish row exists

    run_delivery_pipeline(fake)

    assert fake.get_delivery_state(run_date=RUN_DATE, run_mode="production")["delivered_at"]


def test_execute_pipeline_does_not_record_delivery_when_send_fails(run_delivery_pipeline, fake_mailer):
    fake = _seed()
    fake_mailer.fail_with = RuntimeError("resend down")

    with pytest.raises(RuntimeError, match="resend down"):
        run_delivery_pipeline(fake)

    row = fake.get_delivery_state(run_date=RUN_DATE, run_mode="production")
    assert "delivered_at" not in row


def test_test_mode_record_is_a_noop_on_the_production_row(run_delivery_pipeline):
    """The write-back contract for QA runs: keyed on run_mode='test', which
    matches no production row."""
    fake = _seed("production")   # production row only

    run_delivery_pipeline(fake, run=TEST_RUN_INSTANT)

    assert "delivered_at" not in fake.get_delivery_state(run_date=RUN_DATE, run_mode="production")
    assert fake.get_delivery_state(run_date=RUN_DATE, run_mode="test") is None


def test_record_delivery_failure_is_logged_not_raised(monkeypatch, caplog):
    """The email has already gone out — a red job here would invite a manual
    re-run and a duplicate email. Warn and move on."""
    failing = MagicMock(spec=InMemoryIntelligenceRepo)
    failing.record_delivery.side_effect = RuntimeError("column does not exist")
    monkeypatch.setattr("delivery_engine._repo", lambda: failing)

    with caplog.at_level("WARNING"):
        delivery_engine._record_delivery(RUN_INSTANT)

    assert any("delivered_at" in r.getMessage() for r in caplog.records)


def test_main_reads_the_run_instant_once_and_hands_it_down(monkeypatch):
    """main() is the one place the clock and MARKET_PULSE_RUN_MODE are read:
    it builds the run instant and passes it to execute_pipeline."""
    monkeypatch.setattr(delivery_engine.config, "validate_environment", lambda engine: None)
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    seen: list = []
    monkeypatch.setattr(delivery_engine, "execute_pipeline", seen.append)

    delivery_engine.main()

    assert len(seen) == 1
    assert isinstance(seen[0], RunInstant) and seen[0].test_mode


# ---------------------------------------------------------------------------
# Codex review on PR #67: the anchor must only ever move past rows that
# actually went out.
# ---------------------------------------------------------------------------

def test_fetch_failure_sends_nothing_and_leaves_the_anchor_alone(run_delivery_pipeline, fake_mailer, monkeypatch):
    """A Supabase outage must not become a no-news email whose stamp then
    hides every row that existed at the time. fetch_since is a STRICT read:
    the error propagates, no email is sent, no stamp is written — the red job
    is the alarm, and the next successful run reaches back over the gap."""
    fake = _seed()

    def outage(cutoff):
        raise RuntimeError("supabase unreachable")
    monkeypatch.setattr(fake, "fetch_since", outage)

    with pytest.raises(RuntimeError, match="supabase unreachable"):
        run_delivery_pipeline(fake)

    assert fake_mailer.sent == []
    assert "delivered_at" not in fake.get_delivery_state(run_date=RUN_DATE, run_mode="production")


def test_stamp_is_the_fetch_instant_so_rows_arriving_mid_run_are_not_lost(run_delivery_pipeline, monkeypatch):
    """A row written by a concurrent ingestion between the fetch and the send
    is absent from this email. Stamping the SEND time would put it before the
    anchor and drop it forever; stamping the fetch instant leaves it for the
    next run. The run instant is read once and threaded, so the stamp IS the
    fetch instant by construction — there is no second clock reading to get
    wrong."""
    fake = _seed()

    real_fetch = fake.fetch_since
    def fetch_then_concurrent_write(cutoff):
        rows = real_fetch(cutoff)
        # Simulates the QA/manual ingestion landing a row after the fetch,
        # before Resend returns.
        fake.upsert_insight(_row("mid-run", T + timedelta(seconds=30)))
        return rows
    monkeypatch.setattr(fake, "fetch_since", fetch_then_concurrent_write)

    run_delivery_pipeline(fake)

    stamped = fake.fetch_last_delivery(run_mode="production", before_date="2999-01-01")
    assert stamped == T
    monkeypatch.setattr(fake, "fetch_since", real_fetch)
    tomorrow = replace(RUN_INSTANT, now=T + timedelta(days=1))
    assert "mid-run" in {r["url_hash"] for r in fetch_todays_intelligence(tomorrow)}
