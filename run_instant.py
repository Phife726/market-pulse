"""The run instant — the one clock reading a pipeline run makes, plus the run
mode, as a frozen value (see the **run instant** entry in CONTEXT.md).

`now` + `run_mode` together name the `daily_summaries` row this run belongs to
(`run_date` + `run_mode` is that table's key); every other date a run derives
from "today" is a property here, so the key is spelled once per run instead of
once per function. `now` is naive UTC — the convention every stored timestamp
follows — so the email's date is the UTC run date by construction.

Everything here is pure except `current()`. Each engine's `main()` reads one
and hands it to `execute_pipeline(run)`; tests build one from a literal
datetime.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import config


def naive_utcnow() -> datetime:
    """Naive UTC 'now' — the convention every stored timestamp follows.

    The engines never call this (they take the run instant); it exists so
    the two lookback-window clocks that stay separate on purpose — the
    repo's `recent_headlines` and ZoomInfo's `publishing_date_start` — and
    `RunInstant.current()` spell the convention once."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass(frozen=True)
class RunInstant:
    now: datetime
    run_mode: str

    @classmethod
    def current(cls) -> "RunInstant":
        """Read the clock (naive UTC) and the configured run mode — the only
        effectful line in this module."""
        return cls(now=naive_utcnow(), run_mode=config.run_mode())

    @property
    def run_date(self) -> str:
        """The `daily_summaries.run_date` key for this run (ISO date)."""
        return self.now.date().isoformat()

    @property
    def min_summary_date(self) -> str:
        """The macro-summary lookback floor: yesterday, so a summary written
        just before midnight is still found just after it (the date-rollover
        grace)."""
        return (self.now.date() - timedelta(days=1)).isoformat()

    @property
    def header_date(self) -> str:
        """The email header's date, e.g. 'Thursday, August 27, 2026'."""
        return self.now.strftime("%A, %B %d, %Y")

    @property
    def subject_date(self) -> str:
        """The email subject's date, e.g. 'August 27, 2026'."""
        return self.now.strftime("%B %d, %Y")

    @property
    def test_mode(self) -> bool:
        return self.run_mode == "test"
