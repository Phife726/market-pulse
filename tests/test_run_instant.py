"""The run instant — the one clock reading a pipeline run makes, plus the run
mode, as a frozen value (see CONTEXT.md).

Pure: every derived date is a function of `now` and nothing else. Only
`naive_utcnow()` touches the clock, and only `RunInstant.current()` composes
it with `config.run_mode()`. The last test here pins the invariant behind
the value: the engine modules make no ambient clock or run-mode read.
"""
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

import pytest

import run_instant
from run_instant import RunInstant, SummaryKey, naive_utcnow
from tests.conftest import REPO_ROOT

# Thursday 2026-08-27 14:01 UTC — the late start from issue #64.
T = datetime(2026, 8, 27, 14, 1, 0)


def test_run_date_is_the_iso_date_of_now():
    assert RunInstant(now=T, run_mode="production").run_date == "2026-08-27"


def test_min_summary_date_is_the_day_before():
    """The macro-summary lookback floor (`run_date >= yesterday`) — the
    date-rollover grace the QA fallback relies on."""
    assert RunInstant(now=T, run_mode="production").min_summary_date == "2026-08-26"


def test_min_summary_date_crosses_a_month_boundary():
    first = datetime(2026, 9, 1, 0, 30, 0)
    assert RunInstant(now=first, run_mode="production").min_summary_date == "2026-08-31"


def test_header_date_is_the_long_weekday_form():
    assert RunInstant(now=T, run_mode="production").header_date == "Thursday, August 27, 2026"


def test_subject_date_is_the_month_day_year_form():
    assert RunInstant(now=T, run_mode="production").subject_date == "August 27, 2026"


def test_test_mode_follows_run_mode():
    assert RunInstant(now=T, run_mode="test").test_mode is True
    assert RunInstant(now=T, run_mode="production").test_mode is False


def test_summary_key_is_the_run_date_and_run_mode_pair():
    """The `daily_summaries` row this run belongs to by its own clock — the
    key ingestion writes, and delivery's fallback when it finds no row."""
    key = RunInstant(now=T, run_mode="test").summary_key
    assert (key.run_date, key.run_mode) == ("2026-08-27", "test")


def test_summary_key_is_frozen():
    key = SummaryKey(run_date="2026-08-27", run_mode="production")
    with pytest.raises(FrozenInstanceError):
        key.run_date = "2026-08-28"


def test_run_instant_is_frozen():
    run = RunInstant(now=T, run_mode="production")
    with pytest.raises(FrozenInstanceError):
        run.now = T


class _FixedDatetime:
    """Stands in for run_instant.datetime: records the zone asked for and
    answers one fixed aware instant, so the test pins 'UTC, made naive'
    without a real clock read (which a UTC host could not tell from a
    naive local now())."""
    FIXED = datetime(2026, 8, 27, 14, 1, 0, tzinfo=timezone.utc)

    def __init__(self) -> None:
        self.asked: list = []

    def now(self, tz=None) -> datetime:
        self.asked.append(tz)
        return self.FIXED


def test_naive_utcnow_asks_for_utc_and_drops_the_zone(monkeypatch):
    fixed = _FixedDatetime()
    monkeypatch.setattr(run_instant, "datetime", fixed)

    now = naive_utcnow()

    assert fixed.asked == [timezone.utc]
    assert now == T and now.tzinfo is None


def test_current_reads_a_naive_utc_clock_and_the_configured_run_mode(monkeypatch):
    """The single effectful line: naive UTC (the convention every stored
    timestamp follows) and config.run_mode() — never the local clock."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    fixed = _FixedDatetime()
    monkeypatch.setattr(run_instant, "datetime", fixed)

    run = RunInstant.current()

    assert fixed.asked == [timezone.utc]
    assert run.now == T and run.now.tzinfo is None
    assert run.run_mode == "test"


def test_current_defaults_to_production_without_the_env_var(monkeypatch):
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    assert RunInstant.current().run_mode == "production"


# ---------------------------------------------------------------------------
# The invariant: engines take the run instant; they never read the clock or
# MARKET_PULSE_RUN_MODE themselves (CLAUDE.md, Key Invariants).
# ---------------------------------------------------------------------------

ENGINE_MODULES = ("ingestion_engine.py", "delivery_engine.py")
AMBIENT_READS = ("date.today(", "datetime.now(", "datetime.today(", "utcnow(", "config.run_mode(")


@pytest.mark.parametrize("module", ENGINE_MODULES)
def test_engine_modules_make_no_ambient_clock_or_run_mode_reads(module):
    """A stray read would otherwise be caught only on days when the fixture
    date differs from the wall-clock date — this makes the rule structural."""
    src = (REPO_ROOT / module).read_text(encoding="utf-8")
    for needle in AMBIENT_READS:
        assert needle not in src, f"{module} reads {needle} — take the run instant instead"
