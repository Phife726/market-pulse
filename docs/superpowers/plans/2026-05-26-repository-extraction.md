# Repository Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract every Supabase call in the market-pulse pipeline into a single `IntelligenceRepo` module with one live adapter (Supabase) and one in-memory fake, delivered in three staged PRs that each leave the cron green and deployable.

**Architecture:** PR 1 introduces the module as dead code with full fake-fidelity tests. PR 2 migrates the four ingestion call sites and rewrites affected tests. PR 3 migrates the three delivery call sites, moves `alert_tier` decoration out of fetch, and rewrites the integration tests that currently mock six-deep Supabase builder chains.

**Tech Stack:** Python 3, `supabase>=2.4.0`, `pytest`, `unittest.mock`, `typing.Protocol`. No new dependencies.

---

## Background

Read these before starting:

- [CLAUDE.md](../../../CLAUDE.md) — project overview and key invariants. Pay attention to: URL normalization MUST precede hashing; `SUPABASE_KEY` must be the Service Role key; `MAX_DAILY_SCRAPES = 150`; pipeline deadline 600s; Monday delivery uses 72h lookback; `MARKET_PULSE_RUN_MODE=test` routes mail to QA pool.
- [schema.sql](../../../schema.sql) — table definitions for `daily_intelligence` (unique on `url_hash`) and `daily_summaries` (unique on `run_date, run_mode`).
- [suppression_ledger.py](../../../suppression_ledger.py) — immutable accumulator used by `_update_delivery_summary_counts` for same-day-retry merge.

## Design (locked in)

```python
# daily_intelligence_repo.py
class IntelligenceRepo(Protocol):
    # daily_intelligence (articles)
    def exists_by_hash(self, url_hash: str) -> bool                                # swallow → False
    def upsert_insight(self, payload: dict) -> None                                # raise
    def recent_headlines(self, hours: int) -> set[str]                             # swallow → set()
    def fetch_recent(self, hours: int) -> list[dict]                               # swallow → []
    # daily_summaries (one row per (run_date, run_mode))
    def upsert_summary(self, row: dict) -> None                                    # raise
    def fetch_latest_summary(self, run_mode: str, min_date: str) -> dict | None    # swallow → None
    def get_delivery_state(self, run_date: str, run_mode: str) -> dict | None      # swallow → None
    def update_delivery_counts(self, *, run_date: str, run_mode: str,
                               surfaced_count: int, ledger_row: dict) -> None      # raise
```

**Policy:** Repo writes raise. Repo reads swallow exceptions and return an empty sentinel. Callers decide whether to wrap a raising write in try/except (used for `upsert_insight` per-article and `update_delivery_counts` to preserve "email still sends if metadata write fails").

**Dependency wiring:** Module-level lazy singleton `_repo()` in `daily_intelligence_repo.py`. Callers do `from daily_intelligence_repo import _repo`. Tests monkeypatch at the **consumer** module (`ingestion_engine._repo` / `delivery_engine._repo`) — the same convention this repo already uses for `_get_openai`. Patching `daily_intelligence_repo._repo` would NOT work because consumers bind the name at module-load time.

**No type changes for rows:** dicts in, dicts out. Typed records is a separate, later refactor.

**`alert_tier` decoration leaves the repo:** moves into `fetch_todays_intelligence` (PR 3, after the `fetch_recent` call). Was already a caller-side concern.

**Monday-72h lookback stays at the caller:** repo exposes `fetch_recent(hours)`. The Monday detection lives in `delivery_engine.fetch_todays_intelligence`.

**Caller owns the same-day-retry merge:** repo exposes `get_delivery_state` + `update_delivery_counts` only. The `SuppressionLedger.from_row(...).merge_with(prior)` step stays in `_update_delivery_summary_counts`. The repo does NOT import `SuppressionLedger`.

## File structure

**New files:**
- `daily_intelligence_repo.py` — protocol, `SupabaseIntelligenceRepo`, `InMemoryIntelligenceRepo`, `_repo()` singleton, `_reset_repo()` test helper.
- `tests/test_intelligence_repo.py` — fidelity tests for the in-memory fake; wiring tests for the Supabase adapter.

**Modified files:**
- `ingestion_engine.py` — PR 2: remove `_get_supabase`, route four call sites through `_repo()`.
- `delivery_engine.py` — PR 3: remove `_get_supabase`, route three call sites through `_repo()`, move `alert_tier` decoration out of fetch.
- `tests/test_pipeline.py` — PR 2 and PR 3: replace six-deep `MagicMock` chains with `InMemoryIntelligenceRepo` injected via `monkeypatch`.
- `CLAUDE.md` — appended note about the repository layer (PR 1 and again in PR 3).

**Unchanged:** `suppression_ledger.py`, `schema.sql`, `migrations/`, `targets.yaml`, `market_pulse_config.yaml`, all GitHub Actions workflows.

---

# PR 1 — Introduce the module (no callers migrated)

**Acceptance criteria:** The new module compiles and is imported by no production code. The full existing test suite still passes. New tests cover every Repo method on both adapters. Runtime behavior of the pipeline is unchanged.

**Branch:** `feat/intelligence-repo-module`

---

### Task 1.1: Create the module skeleton with the Protocol

**Files:**
- Create: `daily_intelligence_repo.py`

- [ ] **Step 1: Create the skeleton file**

Create [daily_intelligence_repo.py](../../../daily_intelligence_repo.py) with the Protocol, sentinel imports, and stubbed adapter classes. Methods raise `NotImplementedError` initially — they'll be filled in by subsequent tasks.

```python
"""Single seam for every Supabase query the market-pulse pipeline makes.

Two adapters: SupabaseIntelligenceRepo (production) and
InMemoryIntelligenceRepo (tests, faithful fake).

Error policy:
- Writes raise on failure. Callers decide whether to wrap.
- Reads swallow exceptions and return an empty sentinel
  (False / set() / [] / None) so a transient Supabase blip
  degrades the run instead of crashing it.

The repo does NOT import SuppressionLedger. Same-day-retry merge
semantics live in the caller (delivery_engine._update_delivery_summary_counts).
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Callable, Optional, Protocol

from supabase import create_client, Client

logger = logging.getLogger(__name__)


class IntelligenceRepo(Protocol):
    # daily_intelligence (articles)
    def exists_by_hash(self, url_hash: str) -> bool: ...
    def upsert_insight(self, payload: dict) -> None: ...
    def recent_headlines(self, hours: int) -> set[str]: ...
    def fetch_recent(self, hours: int) -> list[dict]: ...
    # daily_summaries (one row per (run_date, run_mode))
    def upsert_summary(self, row: dict) -> None: ...
    def fetch_latest_summary(self, run_mode: str, min_date: str) -> Optional[dict]: ...
    def get_delivery_state(self, run_date: str, run_mode: str) -> Optional[dict]: ...
    def update_delivery_counts(
        self,
        *,
        run_date: str,
        run_mode: str,
        surfaced_count: int,
        ledger_row: dict,
    ) -> None: ...


class SupabaseIntelligenceRepo:
    """Live adapter. Lazily constructs one Supabase client per instance."""

    def __init__(self) -> None:
        self._client: Optional[Client] = None

    def _supabase(self) -> Client:
        if self._client is None:
            url = os.environ["SUPABASE_URL"]
            key = os.environ["SUPABASE_KEY"]
            self._client = create_client(url, key)
        return self._client

    def exists_by_hash(self, url_hash: str) -> bool:
        raise NotImplementedError

    def upsert_insight(self, payload: dict) -> None:
        raise NotImplementedError

    def recent_headlines(self, hours: int) -> set[str]:
        raise NotImplementedError

    def fetch_recent(self, hours: int) -> list[dict]:
        raise NotImplementedError

    def upsert_summary(self, row: dict) -> None:
        raise NotImplementedError

    def fetch_latest_summary(self, run_mode: str, min_date: str) -> Optional[dict]:
        raise NotImplementedError

    def get_delivery_state(self, run_date: str, run_mode: str) -> Optional[dict]:
        raise NotImplementedError

    def update_delivery_counts(
        self,
        *,
        run_date: str,
        run_mode: str,
        surfaced_count: int,
        ledger_row: dict,
    ) -> None:
        raise NotImplementedError


class InMemoryIntelligenceRepo:
    """Faithful in-memory fake. Honors the same invariants the schema enforces:
    - url_hash is unique on daily_intelligence (upsert semantics).
    - (run_date, run_mode) is unique on daily_summaries.
    - created_at is set automatically on insight upsert if missing.
    """

    def __init__(self, *, now: Optional[Callable[[], datetime]] = None) -> None:
        self._now: Callable[[], datetime] = now or datetime.utcnow
        self._articles: dict[str, dict] = {}                 # url_hash -> row
        self._summaries: dict[tuple[str, str], dict] = {}    # (run_date, run_mode) -> row

    def exists_by_hash(self, url_hash: str) -> bool:
        raise NotImplementedError

    def upsert_insight(self, payload: dict) -> None:
        raise NotImplementedError

    def recent_headlines(self, hours: int) -> set[str]:
        raise NotImplementedError

    def fetch_recent(self, hours: int) -> list[dict]:
        raise NotImplementedError

    def upsert_summary(self, row: dict) -> None:
        raise NotImplementedError

    def fetch_latest_summary(self, run_mode: str, min_date: str) -> Optional[dict]:
        raise NotImplementedError

    def get_delivery_state(self, run_date: str, run_mode: str) -> Optional[dict]:
        raise NotImplementedError

    def update_delivery_counts(
        self,
        *,
        run_date: str,
        run_mode: str,
        surfaced_count: int,
        ledger_row: dict,
    ) -> None:
        raise NotImplementedError


# Module-level lazy singleton. Tests monkeypatch this function.
_INSTANCE: Optional[IntelligenceRepo] = None


def _repo() -> IntelligenceRepo:
    """Return the process-wide IntelligenceRepo. Constructs the Supabase
    adapter on first call. Tests override via monkeypatch."""
    global _INSTANCE
    if _INSTANCE is None:
        _INSTANCE = SupabaseIntelligenceRepo()
    return _INSTANCE


def _reset_repo() -> None:
    """Drop the cached singleton. Test-only — used in fixtures to guarantee
    a clean slate between tests that exercise the live adapter."""
    global _INSTANCE
    _INSTANCE = None
```

- [ ] **Step 2: Confirm the module imports cleanly**

Run: `python -c "import daily_intelligence_repo; print(daily_intelligence_repo.IntelligenceRepo)"`
Expected: prints `<class 'daily_intelligence_repo.IntelligenceRepo'>` with no traceback.

- [ ] **Step 3: Run the full existing test suite**

Run: `pytest tests/ -q`
Expected: PASS with the same test count and outcomes as before this change. No tests reference the new module yet.

- [ ] **Step 4: Commit**

```bash
git add daily_intelligence_repo.py
git commit -m "feat(repo): add IntelligenceRepo protocol and adapter skeletons

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.2: `InMemoryIntelligenceRepo.exists_by_hash` + `upsert_insight`

These two are paired because the test for `exists_by_hash` requires `upsert_insight` to populate state.

**Files:**
- Modify: `daily_intelligence_repo.py`
- Create: `tests/test_intelligence_repo.py`

- [ ] **Step 1: Write the failing tests**

Create [tests/test_intelligence_repo.py](../../../tests/test_intelligence_repo.py):

```python
"""Tests for the IntelligenceRepo adapters.

Two layers:
- InMemoryIntelligenceRepo fidelity tests (most of the file). The fake is
  what we depend on in higher-level tests, so its invariants are pinned.
- SupabaseIntelligenceRepo wiring tests. We mock the supabase client and
  assert each method targets the right table, on_conflict, and filters.
"""
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from daily_intelligence_repo import (
    InMemoryIntelligenceRepo,
    SupabaseIntelligenceRepo,
    _repo,
    _reset_repo,
)


# ---------------------------------------------------------------------------
# InMemoryIntelligenceRepo — articles (daily_intelligence)
# ---------------------------------------------------------------------------

def test_exists_by_hash_false_when_empty():
    repo = InMemoryIntelligenceRepo()
    assert repo.exists_by_hash("nonexistent") is False


def test_exists_by_hash_true_after_upsert():
    repo = InMemoryIntelligenceRepo()
    repo.upsert_insight({"url_hash": "abc123", "headline": "Test"})
    assert repo.exists_by_hash("abc123") is True


def test_upsert_insight_enforces_url_hash_uniqueness():
    """Second upsert with the same url_hash overwrites the first (matches
    Supabase on_conflict=url_hash semantics)."""
    repo = InMemoryIntelligenceRepo()
    repo.upsert_insight({"url_hash": "abc123", "headline": "First"})
    repo.upsert_insight({"url_hash": "abc123", "headline": "Second"})
    rows = repo.fetch_recent(hours=24)
    assert len(rows) == 1
    assert rows[0]["headline"] == "Second"


def test_upsert_insight_sets_created_at_when_missing():
    """If payload omits created_at, the fake injects 'now' so fetch_recent
    can filter on it deterministically."""
    fixed_now = datetime(2026, 5, 26, 12, 0, 0)
    repo = InMemoryIntelligenceRepo(now=lambda: fixed_now)
    repo.upsert_insight({"url_hash": "abc123", "headline": "Test"})
    rows = repo.fetch_recent(hours=24)
    assert rows[0]["created_at"] == fixed_now.isoformat()


def test_upsert_insight_preserves_explicit_created_at():
    """If payload includes created_at, the fake keeps it verbatim."""
    repo = InMemoryIntelligenceRepo()
    explicit = "2026-05-20T10:30:00"
    repo.upsert_insight({"url_hash": "abc123", "headline": "Test", "created_at": explicit})
    rows = repo.fetch_recent(hours=24 * 365)  # very wide window
    assert rows[0]["created_at"] == explicit
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_intelligence_repo.py -v`
Expected: FAIL with `NotImplementedError` for each test.

- [ ] **Step 3: Implement the methods**

Modify [daily_intelligence_repo.py](../../../daily_intelligence_repo.py), replacing the `NotImplementedError` bodies in `InMemoryIntelligenceRepo`:

```python
def exists_by_hash(self, url_hash: str) -> bool:
    return url_hash in self._articles

def upsert_insight(self, payload: dict) -> None:
    url_hash = payload.get("url_hash")
    if not url_hash:
        raise ValueError("payload missing url_hash")
    row = dict(payload)
    row.setdefault("created_at", self._now().isoformat())
    self._articles[url_hash] = row

def fetch_recent(self, hours: int) -> list[dict]:
    cutoff = self._now() - timedelta(hours=hours)
    rows = []
    for row in self._articles.values():
        created = row.get("created_at")
        if isinstance(created, str):
            try:
                ts = datetime.fromisoformat(created)
            except ValueError:
                continue
        elif isinstance(created, datetime):
            ts = created
        else:
            continue
        if ts >= cutoff:
            rows.append(dict(row))
    return rows
```

(`fetch_recent` is needed now because the uniqueness test reads through it. Its own dedicated test comes in Task 1.3.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_intelligence_repo.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Confirm the existing suite still passes**

Run: `pytest tests/ -q`
Expected: PASS (existing test count + 5 new tests).

- [ ] **Step 6: Commit**

```bash
git add daily_intelligence_repo.py tests/test_intelligence_repo.py
git commit -m "feat(repo): implement InMemoryRepo exists_by_hash + upsert_insight

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.3: `InMemoryIntelligenceRepo.fetch_recent` + `recent_headlines`

**Files:**
- Modify: `daily_intelligence_repo.py`
- Modify: `tests/test_intelligence_repo.py`

- [ ] **Step 1: Write the failing tests**

Append to [tests/test_intelligence_repo.py](../../../tests/test_intelligence_repo.py):

```python
def test_fetch_recent_filters_by_created_at():
    """Rows older than `hours` must not be returned."""
    fixed_now = datetime(2026, 5, 26, 12, 0, 0)
    repo = InMemoryIntelligenceRepo(now=lambda: fixed_now)

    # 50 hours ago — outside a 24h window, inside a 72h window
    repo.upsert_insight({
        "url_hash": "old",
        "headline": "Old article",
        "created_at": (fixed_now - timedelta(hours=50)).isoformat(),
    })
    # 5 hours ago — inside both windows
    repo.upsert_insight({
        "url_hash": "fresh",
        "headline": "Fresh article",
        "created_at": (fixed_now - timedelta(hours=5)).isoformat(),
    })

    rows_24 = repo.fetch_recent(hours=24)
    assert {r["url_hash"] for r in rows_24} == {"fresh"}

    rows_72 = repo.fetch_recent(hours=72)
    assert {r["url_hash"] for r in rows_72} == {"old", "fresh"}


def test_fetch_recent_returns_independent_copies():
    """Mutating a returned row must not affect repo state."""
    repo = InMemoryIntelligenceRepo()
    repo.upsert_insight({"url_hash": "abc", "headline": "Original"})
    rows = repo.fetch_recent(hours=24)
    rows[0]["headline"] = "Mutated"
    again = repo.fetch_recent(hours=24)
    assert again[0]["headline"] == "Original"


def test_recent_headlines_returns_set_of_headlines():
    fixed_now = datetime(2026, 5, 26, 12, 0, 0)
    repo = InMemoryIntelligenceRepo(now=lambda: fixed_now)
    repo.upsert_insight({"url_hash": "a", "headline": "Alpha"})
    repo.upsert_insight({"url_hash": "b", "headline": "Beta"})
    assert repo.recent_headlines(hours=24) == {"Alpha", "Beta"}


def test_recent_headlines_honors_time_window():
    fixed_now = datetime(2026, 5, 26, 12, 0, 0)
    repo = InMemoryIntelligenceRepo(now=lambda: fixed_now)
    repo.upsert_insight({
        "url_hash": "old",
        "headline": "Old",
        "created_at": (fixed_now - timedelta(hours=100)).isoformat(),
    })
    repo.upsert_insight({"url_hash": "fresh", "headline": "Fresh"})
    assert repo.recent_headlines(hours=24) == {"Fresh"}


def test_recent_headlines_empty_when_no_rows():
    repo = InMemoryIntelligenceRepo()
    assert repo.recent_headlines(hours=72) == set()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_intelligence_repo.py -v`
Expected: the new tests FAIL because `recent_headlines` still raises `NotImplementedError`. Tests for `fetch_recent` should PASS already (implemented in Task 1.2).

- [ ] **Step 3: Implement `recent_headlines`**

In [daily_intelligence_repo.py](../../../daily_intelligence_repo.py), replace the `recent_headlines` body in `InMemoryIntelligenceRepo`:

```python
def recent_headlines(self, hours: int) -> set[str]:
    return {row.get("headline", "") for row in self.fetch_recent(hours)
            if row.get("headline")}
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/test_intelligence_repo.py -v`
Expected: PASS (all 10 tests so far).

- [ ] **Step 5: Commit**

```bash
git add daily_intelligence_repo.py tests/test_intelligence_repo.py
git commit -m "feat(repo): implement InMemoryRepo fetch_recent + recent_headlines

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.4: `InMemoryIntelligenceRepo` summary operations

Implements `upsert_summary`, `fetch_latest_summary`, `get_delivery_state`, `update_delivery_counts`.

**Files:**
- Modify: `daily_intelligence_repo.py`
- Modify: `tests/test_intelligence_repo.py`

- [ ] **Step 1: Write the failing tests**

Append to [tests/test_intelligence_repo.py](../../../tests/test_intelligence_repo.py):

```python
# ---------------------------------------------------------------------------
# InMemoryIntelligenceRepo — summaries (daily_summaries)
# ---------------------------------------------------------------------------

def test_upsert_summary_compound_key():
    """(run_date, run_mode) is the compound primary key. Same key overwrites;
    different key creates a separate row."""
    repo = InMemoryIntelligenceRepo()
    repo.upsert_summary({
        "run_date": "2026-05-26",
        "run_mode": "production",
        "executive_summary": "First",
    })
    repo.upsert_summary({
        "run_date": "2026-05-26",
        "run_mode": "production",
        "executive_summary": "Second",
    })
    repo.upsert_summary({
        "run_date": "2026-05-26",
        "run_mode": "test",
        "executive_summary": "Test mode",
    })
    prod = repo.get_delivery_state(run_date="2026-05-26", run_mode="production")
    test = repo.get_delivery_state(run_date="2026-05-26", run_mode="test")
    assert prod["executive_summary"] == "Second"
    assert test["executive_summary"] == "Test mode"


def test_upsert_summary_round_trips_all_columns():
    """Every column the engines write must round-trip unchanged."""
    repo = InMemoryIntelligenceRepo()
    row = {
        "run_date": "2026-05-26",
        "run_mode": "production",
        "executive_summary": "summary text",
        "macro_sentiment": "Mixed / Watch",
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "..."},
            {"label": "Supply chain watch", "body": "..."},
            {"label": "Commercial action", "body": "..."},
        ],
        "screened_count": 42,
        "surfaced_count": 8,
        "suppression_breakdown": {"duplicate_url": 3, "scrape_failed": 2},
        "suppression_samples": [{"reason": "duplicate_url", "url": "x", "title": "y"}],
    }
    repo.upsert_summary(row)
    got = repo.get_delivery_state(run_date="2026-05-26", run_mode="production")
    for key, value in row.items():
        assert got[key] == value, f"mismatch on {key}"


def test_get_delivery_state_missing_returns_none():
    repo = InMemoryIntelligenceRepo()
    assert repo.get_delivery_state(run_date="2026-05-26", run_mode="production") is None


def test_fetch_latest_summary_filters_by_min_date_and_run_mode():
    repo = InMemoryIntelligenceRepo()
    repo.upsert_summary({"run_date": "2026-05-24", "run_mode": "production",
                         "executive_summary": "stale", "macro_sentiment": "x"})
    repo.upsert_summary({"run_date": "2026-05-26", "run_mode": "production",
                         "executive_summary": "fresh", "macro_sentiment": "x"})
    repo.upsert_summary({"run_date": "2026-05-26", "run_mode": "test",
                         "executive_summary": "test mode", "macro_sentiment": "x"})

    got = repo.fetch_latest_summary(run_mode="production", min_date="2026-05-25")
    assert got["executive_summary"] == "fresh"

    got_old_cutoff = repo.fetch_latest_summary(run_mode="production", min_date="2026-05-20")
    assert got_old_cutoff["executive_summary"] == "fresh"  # picks the latest, not the oldest

    none = repo.fetch_latest_summary(run_mode="production", min_date="2026-05-27")
    assert none is None


def test_fetch_latest_summary_returns_none_when_empty():
    repo = InMemoryIntelligenceRepo()
    assert repo.fetch_latest_summary(run_mode="production", min_date="2026-05-26") is None


def test_update_delivery_counts_partial_update():
    """update_delivery_counts patches surfaced_count and the ledger fields
    without disturbing the rest of the row."""
    repo = InMemoryIntelligenceRepo()
    repo.upsert_summary({
        "run_date": "2026-05-26", "run_mode": "production",
        "executive_summary": "stays put", "macro_sentiment": "stays put",
        "screened_count": 100,
        "suppression_breakdown": {"old": 1},
        "suppression_samples": [{"reason": "old", "url": "u", "title": "t"}],
    })
    repo.update_delivery_counts(
        run_date="2026-05-26",
        run_mode="production",
        surfaced_count=7,
        ledger_row={
            "suppression_breakdown": {"new": 2},
            "suppression_samples": [{"reason": "new", "url": "u2", "title": "t2"}],
        },
    )
    got = repo.get_delivery_state(run_date="2026-05-26", run_mode="production")
    assert got["executive_summary"] == "stays put"        # untouched
    assert got["macro_sentiment"] == "stays put"          # untouched
    assert got["screened_count"] == 100                   # untouched
    assert got["surfaced_count"] == 7                     # patched
    assert got["suppression_breakdown"] == {"new": 2}     # patched
    assert got["suppression_samples"] == [{"reason": "new", "url": "u2", "title": "t2"}]


def test_update_delivery_counts_silent_when_row_missing():
    """Matches Supabase UPDATE-WHERE-no-match semantics: silent no-op."""
    repo = InMemoryIntelligenceRepo()
    # No row inserted yet.
    repo.update_delivery_counts(
        run_date="2026-05-26",
        run_mode="production",
        surfaced_count=7,
        ledger_row={"suppression_breakdown": {}, "suppression_samples": []},
    )
    assert repo.get_delivery_state(run_date="2026-05-26", run_mode="production") is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_intelligence_repo.py -v`
Expected: the new tests FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement the methods**

In [daily_intelligence_repo.py](../../../daily_intelligence_repo.py), replace the four summary-related `NotImplementedError` bodies in `InMemoryIntelligenceRepo`:

```python
def upsert_summary(self, row: dict) -> None:
    run_date = row.get("run_date")
    run_mode = row.get("run_mode", "production")
    if not run_date:
        raise ValueError("summary row missing run_date")
    self._summaries[(run_date, run_mode)] = dict(row)

def fetch_latest_summary(self, run_mode: str, min_date: str) -> Optional[dict]:
    candidates = [
        row for (rd, rm), row in self._summaries.items()
        if rm == run_mode and rd >= min_date
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda r: r["run_date"], reverse=True)
    return dict(candidates[0])

def get_delivery_state(self, run_date: str, run_mode: str) -> Optional[dict]:
    row = self._summaries.get((run_date, run_mode))
    return dict(row) if row is not None else None

def update_delivery_counts(
    self,
    *,
    run_date: str,
    run_mode: str,
    surfaced_count: int,
    ledger_row: dict,
) -> None:
    key = (run_date, run_mode)
    if key not in self._summaries:
        # Matches Supabase: UPDATE on no matching row is a silent no-op.
        return
    existing = self._summaries[key]
    existing["surfaced_count"] = surfaced_count
    for k, v in ledger_row.items():
        existing[k] = v
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/test_intelligence_repo.py -v`
Expected: PASS (17 tests total).

- [ ] **Step 5: Commit**

```bash
git add daily_intelligence_repo.py tests/test_intelligence_repo.py
git commit -m "feat(repo): implement InMemoryRepo summary operations

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.5: `SupabaseIntelligenceRepo` — article methods

Wires the four article operations through the live Supabase client. Tests assert each method targets the right table and uses the right query operators.

**Files:**
- Modify: `daily_intelligence_repo.py`
- Modify: `tests/test_intelligence_repo.py`

- [ ] **Step 1: Write the failing tests**

Append to [tests/test_intelligence_repo.py](../../../tests/test_intelligence_repo.py):

```python
# ---------------------------------------------------------------------------
# SupabaseIntelligenceRepo — wiring tests
#
# We mock the supabase Client and assert each method touches the right
# table + filters + on_conflict. These replace the six-deep MagicMock
# chains scattered through test_pipeline.py.
# ---------------------------------------------------------------------------

@pytest.fixture
def supabase_repo(monkeypatch):
    """Yield a SupabaseIntelligenceRepo whose underlying client is a MagicMock."""
    monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "test_key")
    repo = SupabaseIntelligenceRepo()
    mock_client = MagicMock()
    # Pre-populate the client so _supabase() returns the mock without create_client.
    repo._client = mock_client
    return repo, mock_client


def test_supabase_exists_by_hash_queries_daily_intelligence(supabase_repo):
    repo, mock_client = supabase_repo
    mock_client.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = [
        {"url_hash": "abc123"}
    ]
    assert repo.exists_by_hash("abc123") is True
    mock_client.table.assert_called_with("daily_intelligence")
    mock_client.table.return_value.select.assert_called_with("url_hash")
    mock_client.table.return_value.select.return_value.eq.assert_called_with("url_hash", "abc123")


def test_supabase_exists_by_hash_returns_false_when_empty(supabase_repo):
    repo, mock_client = supabase_repo
    mock_client.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = []
    assert repo.exists_by_hash("absent") is False


def test_supabase_exists_by_hash_swallows_errors(supabase_repo):
    """Reads must NOT raise — they swallow and return the empty sentinel."""
    repo, mock_client = supabase_repo
    mock_client.table.side_effect = Exception("network down")
    assert repo.exists_by_hash("abc123") is False


def test_supabase_upsert_insight_targets_daily_intelligence_with_on_conflict(supabase_repo):
    repo, mock_client = supabase_repo
    repo.upsert_insight({"url_hash": "abc123", "headline": "Test"})
    mock_client.table.assert_called_with("daily_intelligence")
    mock_client.table.return_value.upsert.assert_called_with(
        {"url_hash": "abc123", "headline": "Test"},
        on_conflict="url_hash",
    )


def test_supabase_upsert_insight_raises_on_error(supabase_repo):
    """Writes must raise — silent write failure is worse than crashing the cron."""
    repo, mock_client = supabase_repo
    mock_client.table.side_effect = Exception("write conflict")
    with pytest.raises(Exception, match="write conflict"):
        repo.upsert_insight({"url_hash": "abc", "headline": "x"})


def test_supabase_recent_headlines_filters_by_created_at(supabase_repo):
    repo, mock_client = supabase_repo
    mock_client.table.return_value.select.return_value.gte.return_value.execute.return_value.data = [
        {"headline": "Alpha"}, {"headline": "Beta"},
    ]
    assert repo.recent_headlines(hours=72) == {"Alpha", "Beta"}
    mock_client.table.assert_called_with("daily_intelligence")
    mock_client.table.return_value.select.assert_called_with("headline")
    # The .gte filter is "created_at" >= some-ISO-string; we don't pin the timestamp.
    args, _ = mock_client.table.return_value.select.return_value.gte.call_args
    assert args[0] == "created_at"


def test_supabase_recent_headlines_swallows_errors(supabase_repo):
    repo, mock_client = supabase_repo
    mock_client.table.side_effect = Exception("read failed")
    assert repo.recent_headlines(hours=72) == set()


def test_supabase_fetch_recent_orders_by_impact(supabase_repo):
    repo, mock_client = supabase_repo
    mock_client.table.return_value.select.return_value.gte.return_value.order.return_value.execute.return_value.data = [
        {"url_hash": "a", "headline": "x"},
    ]
    rows = repo.fetch_recent(hours=24)
    assert rows == [{"url_hash": "a", "headline": "x"}]
    mock_client.table.assert_called_with("daily_intelligence")
    mock_client.table.return_value.select.assert_called_with("*")
    mock_client.table.return_value.select.return_value.gte.return_value.order.assert_called_with(
        "americhem_impact_score", desc=True,
    )


def test_supabase_fetch_recent_swallows_errors(supabase_repo):
    repo, mock_client = supabase_repo
    mock_client.table.side_effect = Exception("read failed")
    assert repo.fetch_recent(hours=24) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_intelligence_repo.py -v -k Supabase`
Expected: FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement the methods**

In [daily_intelligence_repo.py](../../../daily_intelligence_repo.py), replace the four article-related `NotImplementedError` bodies in `SupabaseIntelligenceRepo`:

```python
def exists_by_hash(self, url_hash: str) -> bool:
    try:
        result = (
            self._supabase().table("daily_intelligence")
            .select("url_hash")
            .eq("url_hash", url_hash)
            .limit(1)
            .execute()
        )
        return len(result.data) > 0
    except Exception as exc:
        logger.error("Supabase exists_by_hash failed for %s: %s", url_hash, exc)
        return False

def upsert_insight(self, payload: dict) -> None:
    self._supabase().table("daily_intelligence").upsert(
        payload, on_conflict="url_hash"
    ).execute()

def recent_headlines(self, hours: int) -> set[str]:
    try:
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        result = (
            self._supabase().table("daily_intelligence")
            .select("headline")
            .gte("created_at", cutoff)
            .execute()
        )
        return {str(row["headline"]) for row in (result.data or [])
                if row.get("headline")}
    except Exception as exc:
        logger.error("Supabase recent_headlines failed: %s", exc)
        return set()

def fetch_recent(self, hours: int) -> list[dict]:
    try:
        cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
        result = (
            self._supabase().table("daily_intelligence")
            .select("*")
            .gte("created_at", cutoff)
            .order("americhem_impact_score", desc=True)
            .execute()
        )
        return list(result.data or [])
    except Exception as exc:
        logger.error("Supabase fetch_recent failed: %s", exc)
        return []
```

- [ ] **Step 4: Run the new tests**

Run: `pytest tests/test_intelligence_repo.py -v -k Supabase`
Expected: PASS (9 wiring tests).

- [ ] **Step 5: Run the whole suite**

Run: `pytest tests/ -q`
Expected: PASS. The pre-existing tests are untouched; only new tests have been added.

- [ ] **Step 6: Commit**

```bash
git add daily_intelligence_repo.py tests/test_intelligence_repo.py
git commit -m "feat(repo): wire SupabaseRepo article methods to client

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.6: `SupabaseIntelligenceRepo` — summary methods

**Files:**
- Modify: `daily_intelligence_repo.py`
- Modify: `tests/test_intelligence_repo.py`

- [ ] **Step 1: Write the failing tests**

Append to [tests/test_intelligence_repo.py](../../../tests/test_intelligence_repo.py):

```python
def test_supabase_upsert_summary_uses_compound_on_conflict(supabase_repo):
    repo, mock_client = supabase_repo
    row = {
        "run_date": "2026-05-26", "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "y",
    }
    repo.upsert_summary(row)
    mock_client.table.assert_called_with("daily_summaries")
    mock_client.table.return_value.upsert.assert_called_with(
        row, on_conflict="run_date,run_mode",
    )


def test_supabase_upsert_summary_raises_on_error(supabase_repo):
    repo, mock_client = supabase_repo
    mock_client.table.side_effect = Exception("write failed")
    with pytest.raises(Exception, match="write failed"):
        repo.upsert_summary({"run_date": "2026-05-26"})


def test_supabase_fetch_latest_summary_filters_and_orders(supabase_repo):
    repo, mock_client = supabase_repo
    chain = (
        mock_client.table.return_value
        .select.return_value
        .eq.return_value
        .gte.return_value
        .order.return_value
        .limit.return_value
        .execute.return_value
    )
    chain.data = [{"run_date": "2026-05-26", "executive_summary": "x"}]
    got = repo.fetch_latest_summary(run_mode="production", min_date="2026-05-25")
    assert got == {"run_date": "2026-05-26", "executive_summary": "x"}
    mock_client.table.assert_called_with("daily_summaries")
    mock_client.table.return_value.select.return_value.eq.assert_called_with(
        "run_mode", "production",
    )
    mock_client.table.return_value.select.return_value.eq.return_value.gte.assert_called_with(
        "run_date", "2026-05-25",
    )


def test_supabase_fetch_latest_summary_returns_none_when_empty(supabase_repo):
    repo, mock_client = supabase_repo
    chain = (
        mock_client.table.return_value
        .select.return_value
        .eq.return_value
        .gte.return_value
        .order.return_value
        .limit.return_value
        .execute.return_value
    )
    chain.data = []
    assert repo.fetch_latest_summary(run_mode="production", min_date="2026-05-25") is None


def test_supabase_get_delivery_state_filters_by_compound_key(supabase_repo):
    repo, mock_client = supabase_repo
    chain = (
        mock_client.table.return_value
        .select.return_value
        .eq.return_value
        .eq.return_value
        .limit.return_value
        .execute.return_value
    )
    chain.data = [{"suppression_breakdown": {"x": 1}}]
    got = repo.get_delivery_state(run_date="2026-05-26", run_mode="production")
    assert got == {"suppression_breakdown": {"x": 1}}
    mock_client.table.assert_called_with("daily_summaries")


def test_supabase_get_delivery_state_returns_none_when_empty(supabase_repo):
    repo, mock_client = supabase_repo
    chain = (
        mock_client.table.return_value
        .select.return_value
        .eq.return_value
        .eq.return_value
        .limit.return_value
        .execute.return_value
    )
    chain.data = []
    assert repo.get_delivery_state(run_date="2026-05-26", run_mode="production") is None


def test_supabase_update_delivery_counts_targets_compound_key(supabase_repo):
    repo, mock_client = supabase_repo
    repo.update_delivery_counts(
        run_date="2026-05-26",
        run_mode="production",
        surfaced_count=7,
        ledger_row={"suppression_breakdown": {"a": 1}, "suppression_samples": []},
    )
    mock_client.table.assert_called_with("daily_summaries")
    mock_client.table.return_value.update.assert_called_with({
        "surfaced_count": 7,
        "suppression_breakdown": {"a": 1},
        "suppression_samples": [],
    })


def test_supabase_update_delivery_counts_raises_on_error(supabase_repo):
    repo, mock_client = supabase_repo
    mock_client.table.side_effect = Exception("update failed")
    with pytest.raises(Exception, match="update failed"):
        repo.update_delivery_counts(
            run_date="2026-05-26", run_mode="production",
            surfaced_count=0, ledger_row={},
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_intelligence_repo.py -v -k summary`
Expected: FAIL with `NotImplementedError`.

- [ ] **Step 3: Implement the methods**

In [daily_intelligence_repo.py](../../../daily_intelligence_repo.py), replace the four summary-related `NotImplementedError` bodies in `SupabaseIntelligenceRepo`:

```python
def upsert_summary(self, row: dict) -> None:
    self._supabase().table("daily_summaries").upsert(
        row, on_conflict="run_date,run_mode"
    ).execute()

def fetch_latest_summary(self, run_mode: str, min_date: str) -> Optional[dict]:
    try:
        result = (
            self._supabase().table("daily_summaries")
            .select(
                "run_date, run_mode, executive_summary, macro_sentiment, "
                "dominant_condition, executive_bullets, screened_count, "
                "surfaced_count, suppression_breakdown, suppression_samples"
            )
            .eq("run_mode", run_mode)
            .gte("run_date", min_date)
            .order("run_date", desc=True)
            .limit(1)
            .execute()
        )
        if result.data:
            return result.data[0]
        return None
    except Exception as exc:
        logger.error("Supabase fetch_latest_summary failed: %s", exc)
        return None

def get_delivery_state(self, run_date: str, run_mode: str) -> Optional[dict]:
    try:
        result = (
            self._supabase().table("daily_summaries")
            .select("suppression_breakdown, suppression_samples")
            .eq("run_date", run_date)
            .eq("run_mode", run_mode)
            .limit(1)
            .execute()
        )
        rows = result.data or []
        return rows[0] if rows else None
    except Exception as exc:
        logger.error("Supabase get_delivery_state failed: %s", exc)
        return None

def update_delivery_counts(
    self,
    *,
    run_date: str,
    run_mode: str,
    surfaced_count: int,
    ledger_row: dict,
) -> None:
    payload = {"surfaced_count": surfaced_count, **ledger_row}
    (
        self._supabase().table("daily_summaries")
        .update(payload)
        .eq("run_date", run_date)
        .eq("run_mode", run_mode)
        .execute()
    )
```

- [ ] **Step 4: Run the new tests**

Run: `pytest tests/test_intelligence_repo.py -v`
Expected: PASS (full repo test file now ~25 tests).

- [ ] **Step 5: Run the whole suite**

Run: `pytest tests/ -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add daily_intelligence_repo.py tests/test_intelligence_repo.py
git commit -m "feat(repo): wire SupabaseRepo summary methods to client

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.7: Pin the singleton + reset behavior

**Files:**
- Modify: `tests/test_intelligence_repo.py`

- [ ] **Step 1: Write the tests**

Append to [tests/test_intelligence_repo.py](../../../tests/test_intelligence_repo.py):

```python
# ---------------------------------------------------------------------------
# Singleton lifecycle
# ---------------------------------------------------------------------------

def test_repo_singleton_returns_same_instance(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "test_key")
    _reset_repo()
    a = _repo()
    b = _repo()
    assert a is b


def test_reset_repo_rebuilds_singleton(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://test.supabase.co")
    monkeypatch.setenv("SUPABASE_KEY", "test_key")
    _reset_repo()
    first = _repo()
    _reset_repo()
    second = _repo()
    assert first is not second


def test_repo_is_monkeypatchable_at_consumer(monkeypatch):
    """Consumer modules do `from daily_intelligence_repo import _repo`,
    binding the name at import time. Tests patch at the CONSUMER module
    (e.g. `ingestion_engine._repo`) — patching `daily_intelligence_repo._repo`
    after the consumer has imported would have no effect.

    This test simulates a consumer: it has its own bound `_repo`,
    and we patch that binding."""
    import types
    consumer = types.ModuleType("fake_consumer")
    from daily_intelligence_repo import _repo as repo_accessor
    consumer._repo = repo_accessor

    fake = InMemoryIntelligenceRepo()
    monkeypatch.setattr(consumer, "_repo", lambda: fake)
    assert consumer._repo() is fake
```

- [ ] **Step 2: Run tests**

Run: `pytest tests/test_intelligence_repo.py -v -k repo`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_intelligence_repo.py
git commit -m "test(repo): pin singleton + monkeypatch behavior

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.8: Document the new layer in CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Append a section to CLAUDE.md**

Edit [CLAUDE.md](../../../CLAUDE.md). After the "Architecture" section and before "Tests", insert this paragraph:

```markdown
**`daily_intelligence_repo.py`** — Single seam for every Supabase query the pipeline makes. One Protocol (`IntelligenceRepo`), two adapters (`SupabaseIntelligenceRepo` for prod, `InMemoryIntelligenceRepo` for tests). Reads swallow exceptions and return an empty sentinel; writes raise so silent write failures crash the cron loudly. Callers do `from daily_intelligence_repo import _repo` and call `_repo()`; tests inject the fake at the consumer module — e.g. `monkeypatch.setattr("delivery_engine._repo", lambda: fake)`. The repo does not know about `SuppressionLedger` — the same-day-retry merge for delivery counts lives in `delivery_engine._update_delivery_summary_counts`.
```

Note: as of PR 1 the engines do not yet route through this layer. PR 2 and PR 3 migrate them.

- [ ] **Step 2: Verify the doc reads cleanly**

Run: `grep -A 1 "daily_intelligence_repo" /workspaces/market-pulse/CLAUDE.md`
Expected: shows the new paragraph.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: describe the new IntelligenceRepo layer

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 1.9: Open PR 1

- [ ] **Step 1: Push the branch**

```bash
git push -u origin feat/intelligence-repo-module
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create --title "feat(repo): introduce IntelligenceRepo module (PR 1/3)" --body "$(cat <<'EOF'
## Summary

- Adds \`daily_intelligence_repo.py\` with a single \`IntelligenceRepo\` Protocol and two adapters: \`SupabaseIntelligenceRepo\` (live) and \`InMemoryIntelligenceRepo\` (faithful fake).
- Adds \`tests/test_intelligence_repo.py\` — fidelity tests for the fake (uniqueness, time filtering, compound key, round-trip) and wiring tests for the live adapter.
- No callers migrated. This PR is dead code from the engines' perspective. PR 2 migrates ingestion; PR 3 migrates delivery.

## Test plan

- [ ] \`pytest tests/test_intelligence_repo.py -v\` passes (≈25 new tests)
- [ ] \`pytest tests/\` passes with no behavior change to existing tests
- [ ] Pipeline still runs end-to-end manually (\`python ingestion_engine.py\` + \`python delivery_engine.py\`) — repo module is unused at this stage so no behavior change is possible

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL printed to stdout.

---

# PR 2 — Migrate ingestion (four call sites)

**Acceptance criteria:** Every Supabase call in `ingestion_engine.py` routes through `_repo()`. `_get_supabase` is deleted from the file. All existing tests in `test_pipeline.py` still pass (some rewritten to use `InMemoryIntelligenceRepo`). One intentional behavior change: a Supabase write failure in `upsert_insight` or `upsert_summary` now raises; the cron task fails loudly instead of silently storing nothing.

**Branch:** `feat/intelligence-repo-ingestion` (off `main` after PR 1 merges)

**Files:**
- Modify: `ingestion_engine.py`
- Modify: `tests/test_pipeline.py`
- Modify: `CLAUDE.md`

---

### Task 2.1: Migrate `url_already_processed`

- [ ] **Step 1: Write a regression test that uses the fake**

Append to [tests/test_pipeline.py](../../../tests/test_pipeline.py):

```python
# ---------------------------------------------------------------------------
# Repository wiring — ingestion paths route through _repo()
# ---------------------------------------------------------------------------

from daily_intelligence_repo import InMemoryIntelligenceRepo


def test_url_already_processed_routes_through_repo(monkeypatch):
    """url_already_processed returns True iff the InMemory fake reports a hit."""
    from ingestion_engine import url_already_processed
    fake = InMemoryIntelligenceRepo()
    fake.upsert_insight({"url_hash": "abc123", "headline": "Test"})
    monkeypatch.setattr("ingestion_engine._repo", lambda: fake)
    assert url_already_processed("abc123") is True
    assert url_already_processed("never_seen") is False
```

- [ ] **Step 2: Run it to verify it fails**

Run: `pytest tests/test_pipeline.py::test_url_already_processed_routes_through_repo -v`
Expected: FAIL — current code still talks directly to Supabase via `_get_supabase`, not the patched `_repo`.

- [ ] **Step 3: Migrate the function**

In [ingestion_engine.py](../../../ingestion_engine.py), at the top of the file, add the import alongside the existing `from suppression_ledger import SuppressionLedger`:

```python
from daily_intelligence_repo import _repo
```

Replace [ingestion_engine.py:314-327](../../../ingestion_engine.py#L314-L327) with:

```python
def url_already_processed(url_hash: str) -> bool:
    return _repo().exists_by_hash(url_hash)
```

- [ ] **Step 4: Run the test**

Run: `pytest tests/test_pipeline.py::test_url_already_processed_routes_through_repo -v`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `pytest tests/ -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add ingestion_engine.py tests/test_pipeline.py
git commit -m "refactor(ingestion): route url_already_processed through IntelligenceRepo

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2.2: Migrate `_hydrate_seen_headlines`

- [ ] **Step 1: Write the test**

Append to [tests/test_pipeline.py](../../../tests/test_pipeline.py):

```python
def test_hydrate_seen_headlines_routes_through_repo(monkeypatch):
    """_hydrate_seen_headlines returns the fake's recent headlines."""
    from ingestion_engine import _hydrate_seen_headlines
    fake = InMemoryIntelligenceRepo()
    fake.upsert_insight({"url_hash": "a", "headline": "Alpha"})
    fake.upsert_insight({"url_hash": "b", "headline": "Beta"})
    monkeypatch.setattr("ingestion_engine._repo", lambda: fake)
    assert _hydrate_seen_headlines() == {"Alpha", "Beta"}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `pytest tests/test_pipeline.py::test_hydrate_seen_headlines_routes_through_repo -v`
Expected: FAIL — current code still queries Supabase directly.

- [ ] **Step 3: Migrate the function**

Replace [ingestion_engine.py:617-632](../../../ingestion_engine.py#L617-L632) with:

```python
def _hydrate_seen_headlines() -> set[str]:
    headlines = _repo().recent_headlines(hours=72)
    logger.info("Hydrated seen_headlines buffer with %d entries.", len(headlines))
    return headlines
```

- [ ] **Step 4: Run the tests**

Run: `pytest tests/test_pipeline.py -v -k headlines`
Expected: PASS.

Run: `pytest tests/ -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ingestion_engine.py tests/test_pipeline.py
git commit -m "refactor(ingestion): route _hydrate_seen_headlines through IntelligenceRepo

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2.3: Migrate `store_insight` (and inline the error counter at the call site)

`store_insight` is currently a try/except wrapper that returns `bool`. Per the new policy writes raise — but `execute_pipeline` needs to keep going after a single article fails, so we move the try/except to the caller. This deletes a layer.

- [ ] **Step 1: Write the tests**

Append to [tests/test_pipeline.py](../../../tests/test_pipeline.py):

```python
def test_store_insight_routes_through_repo(monkeypatch):
    """store_insight upserts via the repo and returns the fake's stored row."""
    from ingestion_engine import store_insight
    fake = InMemoryIntelligenceRepo()
    monkeypatch.setattr("ingestion_engine._repo", lambda: fake)
    store_insight({"url_hash": "abc", "headline": "Stored"})
    rows = fake.fetch_recent(hours=24)
    assert rows[0]["headline"] == "Stored"


def test_store_insight_raises_on_repo_write_failure(monkeypatch):
    """The repo's write methods raise; store_insight propagates."""
    from ingestion_engine import store_insight
    failing = MagicMock()
    failing.upsert_insight.side_effect = RuntimeError("write blew up")
    monkeypatch.setattr("ingestion_engine._repo", lambda: failing)
    with pytest.raises(RuntimeError, match="write blew up"):
        store_insight({"url_hash": "abc", "headline": "x"})
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k store_insight`
Expected: FAIL.

- [ ] **Step 3: Migrate `store_insight`**

Replace [ingestion_engine.py:635-642](../../../ingestion_engine.py#L635-L642) with:

```python
def store_insight(payload: dict) -> None:
    """Persist an article insight. Raises on Supabase failure — callers in
    execute_pipeline catch and bump stats['errors'] so the batch continues."""
    _repo().upsert_insight(payload)
```

- [ ] **Step 4: Wrap the call site inside execute_pipeline**

In [ingestion_engine.py](../../../ingestion_engine.py), find the existing block (around line 925-936) that reads:

```python
if store_insight(payload):
    logger.info(
        "Stored [impact=%d, sentiment=%s] %s",
        insight.get("americhem_impact_score", 5),
        insight.get("sentiment_tag", "Neutral"),
        insight["headline"],
    )
    stats["insights_stored"] += 1
    stored_articles_buffer.append(payload)
    seen_headlines.add(insight["headline"])
else:
    stats["errors"] += 1
```

Replace it with:

```python
try:
    store_insight(payload)
except Exception as exc:
    logger.error("Failed to store insight for %s: %s", normalized, exc)
    stats["errors"] += 1
else:
    logger.info(
        "Stored [impact=%d, sentiment=%s] %s",
        insight.get("americhem_impact_score", 5),
        insight.get("sentiment_tag", "Neutral"),
        insight["headline"],
    )
    stats["insights_stored"] += 1
    stored_articles_buffer.append(payload)
    seen_headlines.add(insight["headline"])
```

- [ ] **Step 5: Run the new tests + the full suite**

Run: `pytest tests/test_pipeline.py -v -k store_insight`
Expected: PASS.

Run: `pytest tests/ -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add ingestion_engine.py tests/test_pipeline.py
git commit -m "refactor(ingestion): route store_insight through IntelligenceRepo; raise on write failure

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2.4: Migrate the macro-summary upsert in `generate_macro_summary`

The function currently wraps the upsert in try/except and returns False on failure. After this change, the upsert raises and `generate_macro_summary` propagates — the only callers are inside `execute_pipeline`, which already terminates at that point.

- [ ] **Step 1: Write the tests**

Append to [tests/test_pipeline.py](../../../tests/test_pipeline.py):

```python
def test_generate_macro_summary_routes_through_repo(monkeypatch):
    """The summary upsert hits repo.upsert_summary, not Supabase directly."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    fake = InMemoryIntelligenceRepo()
    monkeypatch.setattr("ingestion_engine._repo", lambda: fake)

    # Mock OpenAI to return a valid macro summary JSON.
    mock_message = MagicMock()
    mock_message.content = json.dumps({
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Some pressure body text."},
            {"label": "Supply chain watch", "body": "Some supply watch text."},
            {"label": "Commercial action", "body": "Some commercial text."},
        ],
    })
    mock_choice = MagicMock(); mock_choice.message = mock_message
    mock_completion = MagicMock(); mock_completion.choices = [mock_choice]
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_completion

    with patch("ingestion_engine._get_openai", return_value=mock_client):
        result = generate_macro_summary([
            {"category": "competitors", "headline": "x",
             "sentiment_score": 5, "americhem_impact": "y"}
        ])

    assert result is True
    from datetime import date
    stored = fake.get_delivery_state(run_date=date.today().isoformat(), run_mode="production")
    assert stored is not None
    assert stored["dominant_condition"] == "Mixed / Watch"


def test_generate_macro_summary_propagates_repo_write_failure(monkeypatch):
    """If repo.upsert_summary raises, the function raises."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    failing = MagicMock()
    failing.upsert_summary.side_effect = RuntimeError("DB down")
    monkeypatch.setattr("ingestion_engine._repo", lambda: failing)

    mock_message = MagicMock()
    mock_message.content = json.dumps({
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "x"},
            {"label": "Supply chain watch", "body": "y"},
            {"label": "Commercial action", "body": "z"},
        ],
    })
    mock_choice = MagicMock(); mock_choice.message = mock_message
    mock_completion = MagicMock(); mock_completion.choices = [mock_choice]
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_completion

    with patch("ingestion_engine._get_openai", return_value=mock_client):
        with pytest.raises(RuntimeError, match="DB down"):
            generate_macro_summary([
                {"category": "competitors", "headline": "x",
                 "sentiment_score": 5, "americhem_impact": "y"}
            ])
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k generate_macro_summary_routes`
Expected: FAIL.

- [ ] **Step 3: Migrate the upsert block**

In [ingestion_engine.py](../../../ingestion_engine.py), find the existing block at lines 764-785 that reads:

```python
try:
    from datetime import date
    supabase = _get_supabase()
    supabase.table("daily_summaries").upsert(
        {
            "run_date": date.today().isoformat(),
            "run_mode": _run_mode(),
            "dominant_condition": cond,
            "executive_bullets": bullets,
            "executive_summary": executive_summary,
            "macro_sentiment": cond,
            "screened_count": screened_count,
            "suppression_breakdown": suppression_breakdown or {},
            "suppression_samples": suppression_samples or [],
        },
        on_conflict="run_date,run_mode",
    ).execute()
    logger.info("Macro summary upserted — condition: %s", cond)
    return True
except Exception as exc:
    logger.error("Failed to upsert macro summary to Supabase: %s", exc)
    return False
```

Replace it with:

```python
from datetime import date
_repo().upsert_summary({
    "run_date": date.today().isoformat(),
    "run_mode": _run_mode(),
    "dominant_condition": cond,
    "executive_bullets": bullets,
    "executive_summary": executive_summary,
    "macro_sentiment": cond,
    "screened_count": screened_count,
    "suppression_breakdown": suppression_breakdown or {},
    "suppression_samples": suppression_samples or [],
})
logger.info("Macro summary upserted — condition: %s", cond)
return True
```

- [ ] **Step 4: Run the new tests**

Run: `pytest tests/test_pipeline.py -v -k generate_macro_summary_routes`
Expected: PASS.

- [ ] **Step 5: Update the existing macro-summary test that relied on the old `_get_supabase` mock**

Find [tests/test_pipeline.py](../../../tests/test_pipeline.py) `test_generate_macro_summary_uses_gpt_5_4_nano` (currently around line 339). It patches `ingestion_engine._get_supabase` — that import path will be deleted in Task 2.5. Update it to use the fake:

Replace the test body's setup section:
```python
    mock_supabase = MagicMock()
    mock_supabase.table.return_value.upsert.return_value.execute.return_value = MagicMock()

    with patch("ingestion_engine._get_openai", return_value=mock_client), patch(
        "ingestion_engine._get_supabase", return_value=mock_supabase
    ):
```

with:
```python
    fake_repo = InMemoryIntelligenceRepo()

    with patch("ingestion_engine._get_openai", return_value=mock_client), \
         patch("ingestion_engine._repo", lambda: fake_repo):
```

- [ ] **Step 6: Run the suite**

Run: `pytest tests/ -q`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add ingestion_engine.py tests/test_pipeline.py
git commit -m "refactor(ingestion): route macro-summary upsert through IntelligenceRepo

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2.5: Delete `_get_supabase` from `ingestion_engine.py`

The four ingestion call sites no longer use it.

- [ ] **Step 1: Verify no remaining usage**

Run: `grep -n "_get_supabase" /workspaces/market-pulse/ingestion_engine.py`
Expected: shows only the definition (around lines 201-204) and no other references.

- [ ] **Step 2: Delete the function and its import**

In [ingestion_engine.py](../../../ingestion_engine.py):

- Delete lines 201-204 entirely:

```python
def _get_supabase() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)
```

- Update the import at line 17 from:

```python
from supabase import create_client, Client
```

to:

```python
# (Supabase client construction lives in daily_intelligence_repo.py.)
```

(Remove the import entirely — `Client` and `create_client` are no longer referenced in this file.)

- [ ] **Step 3: Run the suite**

Run: `pytest tests/ -q`
Expected: PASS.

- [ ] **Step 4: Run a syntax sanity check**

Run: `python -c "import ingestion_engine; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 5: Commit**

```bash
git add ingestion_engine.py
git commit -m "chore(ingestion): drop _get_supabase and supabase imports

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2.6: Open PR 2

- [ ] **Step 1: Push the branch**

```bash
git push -u origin feat/intelligence-repo-ingestion
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create --title "refactor(ingestion): route Supabase calls through IntelligenceRepo (PR 2/3)" --body "$(cat <<'EOF'
## Summary

- Routes all four \`ingestion_engine.py\` Supabase call sites through \`_repo()\`: \`url_already_processed\`, \`_hydrate_seen_headlines\`, \`store_insight\`, and the macro-summary upsert in \`generate_macro_summary\`.
- Deletes \`_get_supabase\` and the \`supabase\` SDK imports from \`ingestion_engine.py\`.
- Inlines the article-write error counter at the call site in \`execute_pipeline\` (try/except moved from inside \`store_insight\`).
- Updates the existing macro-summary test to use \`InMemoryIntelligenceRepo\` instead of mocking \`_get_supabase\`.

## Behavior changes

- **Intentional:** A Supabase write failure now raises. Today these failures are silent — \`store_insight\` returns False or \`generate_macro_summary\` returns False with only a log line. Silent write failures are worse than crashing the cron because they hide real outages. The per-article failure path still gracefully continues the batch (try/except at the call site in \`execute_pipeline\`).

## Test plan

- [ ] \`pytest tests/ -q\` passes
- [ ] No test references \`ingestion_engine._get_supabase\` (deleted)
- [ ] Manual ingestion run: \`python ingestion_engine.py\` — observe the same end-of-run summary log line and the same row count in \`daily_intelligence\`
- [ ] (Optional) Inject a Supabase write failure manually and confirm the cron now exits non-zero rather than completing silently

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL printed to stdout.

---

# PR 3 — Migrate delivery (three call sites)

**Acceptance criteria:** Every Supabase call in `delivery_engine.py` routes through `_repo()`. `_get_supabase` is deleted. `alert_tier` decoration moves out of `fetch_todays_intelligence` and into the caller. The same-day-retry merge for `_update_delivery_summary_counts` stays at the caller. The five generate_html_email integration tests are rewritten to use `InMemoryIntelligenceRepo`. The pipeline still sends an email — the metadata write inside `_update_delivery_summary_counts` is wrapped in try/except so a failed write does not block the email.

**Branch:** `feat/intelligence-repo-delivery` (off `main` after PR 2 merges)

**Files:**
- Modify: `delivery_engine.py`
- Modify: `tests/test_pipeline.py`
- Modify: `CLAUDE.md`

---

### Task 3.1: Migrate `fetch_todays_intelligence` (and move `alert_tier` decoration to `execute_pipeline`)

The current `fetch_todays_intelligence` does THREE things: query Supabase, apply the Monday-72h rule, and decorate rows with `alert_tier`. After this task, the query goes through the repo; the Monday rule stays in the function; the `alert_tier` decoration moves into `execute_pipeline` where the consumer (logging) lives.

- [ ] **Step 1: Write the test**

Append to [tests/test_pipeline.py](../../../tests/test_pipeline.py):

```python
def test_fetch_todays_intelligence_routes_through_repo(monkeypatch):
    """fetch_todays_intelligence returns repo.fetch_recent rows verbatim
    (alert_tier decoration is no longer this function's job)."""
    from delivery_engine import fetch_todays_intelligence
    fake = InMemoryIntelligenceRepo()
    fake.upsert_insight({
        "url_hash": "a", "headline": "Alpha",
        "americhem_impact_score": 8, "sentiment_score": 7,
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    rows = fetch_todays_intelligence()
    assert len(rows) == 1
    assert rows[0]["headline"] == "Alpha"
    assert "alert_tier" not in rows[0]   # decoration moved to caller


def test_fetch_todays_intelligence_uses_72h_on_monday(monkeypatch):
    """Monday detection still drives the lookback parameter."""
    import delivery_engine
    fake = MagicMock(spec=InMemoryIntelligenceRepo)
    fake.fetch_recent.return_value = []
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    # Force "today" to be a Monday for this test.
    fixed_monday = datetime(2026, 5, 25, 9, 0, 0)  # Monday
    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_monday
    monkeypatch.setattr(delivery_engine, "datetime", _FixedDateTime)

    delivery_engine.fetch_todays_intelligence()
    fake.fetch_recent.assert_called_once_with(hours=72)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k fetch_todays_intelligence_routes`
Expected: FAIL — current code still uses raw Supabase.

- [ ] **Step 3: Migrate the function**

In [delivery_engine.py](../../../delivery_engine.py), at the top of the file (alongside the existing `from suppression_ledger import SuppressionLedger, label_for`), add:

```python
from daily_intelligence_repo import _repo
```

Replace [delivery_engine.py:506-544](../../../delivery_engine.py#L506-L544) with:

```python
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
```

- [ ] **Step 4: Move `alert_tier` decoration to `execute_pipeline`**

The old logging in `execute_pipeline` (delivery side) reads:

```python
critical_count  = sum(1 for r in data if r.get("alert_tier") == "CRITICAL")
strategic_count = sum(1 for r in data if r.get("alert_tier") == "STRATEGIC")
routine_count   = sum(1 for r in data if r.get("alert_tier") == "ROUTINE")
logger.info(
    "Rendering email — critical: %d | strategic: %d | routine: %d",
    critical_count, strategic_count, routine_count,
)
```

Replace it with code that computes `alert_tier` inline from `_effective_impact`:

```python
def _alert_tier(row: dict) -> str:
    impact = _effective_impact(row)
    if impact <= 3:
        return "CRITICAL"
    if impact >= 8:
        return "STRATEGIC"
    return "ROUTINE"

critical_count  = sum(1 for r in data if _alert_tier(r) == "CRITICAL")
strategic_count = sum(1 for r in data if _alert_tier(r) == "STRATEGIC")
routine_count   = sum(1 for r in data if _alert_tier(r) == "ROUTINE")
logger.info(
    "Rendering email — critical: %d | strategic: %d | routine: %d",
    critical_count, strategic_count, routine_count,
)
```

Define `_alert_tier` once at module scope just below `_effective_impact` (around line 72).

- [ ] **Step 5: Run the tests**

Run: `pytest tests/test_pipeline.py -v -k fetch_todays_intelligence`
Expected: PASS.

Run: `pytest tests/ -q`
Expected: PASS — none of the existing tests check for `alert_tier` on returned rows (the field is only consumed by the logging block we just moved).

- [ ] **Step 6: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "refactor(delivery): route fetch_todays_intelligence through IntelligenceRepo; move alert_tier to caller

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3.2: Migrate `fetch_macro_summary`

- [ ] **Step 1: Write the test**

Append to [tests/test_pipeline.py](../../../tests/test_pipeline.py):

```python
def test_fetch_macro_summary_routes_through_repo(monkeypatch):
    """fetch_macro_summary returns repo.fetch_latest_summary verbatim."""
    from delivery_engine import fetch_macro_summary
    fake = InMemoryIntelligenceRepo()
    from datetime import date
    today = date.today().isoformat()
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "today's summary", "macro_sentiment": "x",
        "dominant_condition": "Mixed / Watch",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    got = fetch_macro_summary()
    assert got is not None
    assert got["executive_summary"] == "today's summary"


def test_fetch_macro_summary_returns_none_when_missing(monkeypatch):
    from delivery_engine import fetch_macro_summary
    fake = InMemoryIntelligenceRepo()
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    assert fetch_macro_summary() is None
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k fetch_macro_summary`
Expected: FAIL.

- [ ] **Step 3: Migrate the function**

Replace [delivery_engine.py:547-573](../../../delivery_engine.py#L547-L573) with:

```python
def fetch_macro_summary() -> dict | None:
    from datetime import date
    min_run_date = (date.today() - timedelta(days=1)).isoformat()
    summary = _repo().fetch_latest_summary(
        run_mode=_run_mode(),
        min_date=min_run_date,
    )
    if summary is None:
        logger.warning("No macro summary found for run_date >= %s.", min_run_date)
    return summary
```

- [ ] **Step 4: Run the tests**

Run: `pytest tests/test_pipeline.py -v -k fetch_macro_summary`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "refactor(delivery): route fetch_macro_summary through IntelligenceRepo

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3.3: Migrate `_update_delivery_summary_counts`

The caller-side merge contract: read prior row → `SuppressionLedger.from_row("delivery", prior)` → `ledger.merge_with(prior)` → write merged. Wrap the write in try/except/log so a failed metadata write does not block the email send (preserves current operational behavior — see the existing "Non-critical" comment in the source).

- [ ] **Step 1: Write the tests**

Append to [tests/test_pipeline.py](../../../tests/test_pipeline.py):

```python
def test_update_delivery_summary_counts_merges_with_prior(monkeypatch):
    """The same-day-retry merge: prior delivery counts are preserved through
    ingestion-owned codes; new delivery-owned codes overwrite."""
    from delivery_engine import _update_delivery_summary_counts
    from suppression_ledger import SuppressionLedger
    from datetime import date

    fake = InMemoryIntelligenceRepo()
    today = date.today().isoformat()
    # Seed a prior row mimicking ingestion having already written.
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": {"duplicate_url": 5, "below_impact_threshold": 9},
        "suppression_samples": [{"reason": "duplicate_url", "url": "u", "title": "t"}],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)

    new_ledger = (
        SuppressionLedger.for_delivery()
        .record_count("below_impact_threshold", 3)
        .record_count("product_listing", 1)
    )
    _update_delivery_summary_counts(surfaced_count=4, ledger=new_ledger)

    got = fake.get_delivery_state(run_date=today, run_mode="production")
    assert got["surfaced_count"] == 4
    # Ingestion-owned code preserved from prior.
    assert got["suppression_breakdown"]["duplicate_url"] == 5
    # Delivery-owned code overwritten by this run.
    assert got["suppression_breakdown"]["below_impact_threshold"] == 3
    assert got["suppression_breakdown"]["product_listing"] == 1


def test_update_delivery_summary_counts_swallows_write_failure(monkeypatch, caplog):
    """A failed metadata write must not block the email — preserves the
    existing 'Non-critical' operational decision."""
    from delivery_engine import _update_delivery_summary_counts
    from suppression_ledger import SuppressionLedger

    failing = MagicMock()
    failing.get_delivery_state.return_value = None
    failing.update_delivery_counts.side_effect = RuntimeError("DB down")
    monkeypatch.setattr("delivery_engine._repo", lambda: failing)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)

    # Should not raise.
    _update_delivery_summary_counts(
        surfaced_count=0,
        ledger=SuppressionLedger.for_delivery(),
    )
    assert "Failed to update delivery counts" in caplog.text
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k update_delivery_summary_counts`
Expected: FAIL.

- [ ] **Step 3: Migrate the function**

Replace [delivery_engine.py:576-607](../../../delivery_engine.py#L576-L607) with:

```python
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
        prior_row = _repo().get_delivery_state(run_date=today, run_mode=run_mode)
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
```

- [ ] **Step 4: Run the new tests**

Run: `pytest tests/test_pipeline.py -v -k update_delivery_summary_counts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "refactor(delivery): route _update_delivery_summary_counts through IntelligenceRepo

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3.4: Rewrite the `generate_html_email` integration tests to use the fake

The five existing integration tests in `test_pipeline.py` use six-deep `MagicMock` chains like `mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value` — those references break once `_get_supabase` leaves `delivery_engine.py`. Each test gets rewritten to inject the in-memory fake.

The affected tests (search for `patch("delivery_engine._get_supabase"` to find them):
- `test_generate_html_email_legacy_critical_appears_with_badge`
- `test_generate_html_email_routes_two_plus_to_segment_watch`
- `test_generate_html_email_single_low_relevance_hidden_in_production`

(Other `generate_html_email_*` tests in the file don't patch Supabase at all — they only patch `_load_mp_config` and `_get_openai` — and need no changes.)

- [ ] **Step 1: Update `test_generate_html_email_legacy_critical_appears_with_badge`**

Find the test (around line 864). Replace:

```python
mock_supa = MagicMock()
mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()
mock_supa.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(data=[])

with patch("delivery_engine._get_openai", return_value=MagicMock()), \
     patch("delivery_engine._get_supabase", return_value=mock_supa), \
     patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
    html = generate_html_email(data)
```

with:

```python
fake_repo = InMemoryIntelligenceRepo()
with patch("delivery_engine._get_openai", return_value=MagicMock()), \
     patch("delivery_engine._repo", lambda: fake_repo), \
     patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
    html = generate_html_email(data)
```

- [ ] **Step 2: Update `test_generate_html_email_routes_two_plus_to_segment_watch`**

Around line 894. Apply the same substitution: replace the `mock_supa = MagicMock()...` block and the `patch("delivery_engine._get_supabase", return_value=mock_supa)` line with `fake_repo = InMemoryIntelligenceRepo()` and `patch("delivery_engine._repo", lambda: fake_repo)`.

- [ ] **Step 3: Update `test_generate_html_email_single_low_relevance_hidden_in_production`**

Around line 925. Same substitution.

- [ ] **Step 4: Run the integration tests**

Run: `pytest tests/test_pipeline.py -v -k generate_html_email`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `pytest tests/ -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add tests/test_pipeline.py
git commit -m "test(delivery): replace Supabase mock chains with InMemoryIntelligenceRepo

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3.5: Delete `_get_supabase` from `delivery_engine.py`

- [ ] **Step 1: Verify no remaining usage**

Run: `grep -n "_get_supabase" /workspaces/market-pulse/delivery_engine.py`
Expected: only the definition (around line 495-499) is found.

- [ ] **Step 2: Delete the function and the supabase imports**

In [delivery_engine.py](../../../delivery_engine.py):

- Delete the function (around lines 495-499):

```python
def _get_supabase() -> Client:
    """Return an authenticated Supabase client using env credentials."""
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)
```

- Update the import at line 12 from:

```python
from supabase import create_client, Client
```

to:

```python
# (Supabase client construction lives in daily_intelligence_repo.py.)
```

- [ ] **Step 3: Run the suite**

Run: `pytest tests/ -q`
Expected: PASS.

- [ ] **Step 4: Sanity import**

Run: `python -c "import delivery_engine; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 5: Commit**

```bash
git add delivery_engine.py
git commit -m "chore(delivery): drop _get_supabase and supabase imports

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3.6: Final CLAUDE.md polish

The PR 1 paragraph said "as of PR 1 the engines do not yet route through this layer." That's now false. Update the doc to reflect the final state.

- [ ] **Step 1: Edit CLAUDE.md**

In [CLAUDE.md](../../../CLAUDE.md), find the `**daily_intelligence_repo.py**` paragraph and remove any "PR 2/PR 3" language. The final wording should read:

```markdown
**`daily_intelligence_repo.py`** — Single seam for every Supabase query the pipeline makes. One Protocol (`IntelligenceRepo`), two adapters (`SupabaseIntelligenceRepo` for prod, `InMemoryIntelligenceRepo` for tests). Reads swallow exceptions and return an empty sentinel; writes raise so silent write failures crash the cron loudly. Callers do `from daily_intelligence_repo import _repo` and call `_repo()`; tests inject the fake at the consumer module — e.g. `monkeypatch.setattr("delivery_engine._repo", lambda: fake)`. The repo does not know about `SuppressionLedger` — the same-day-retry merge for delivery counts lives in `delivery_engine._update_delivery_summary_counts`.
```

Also update the "Architecture" section's introductory line. Currently it reads:

```markdown
The pipeline is two sequential scripts sharing a Supabase database, plus one pure module that owns the suppression accounting:
```

Change to:

```markdown
The pipeline is two sequential scripts sharing a Supabase database, plus two pure modules — one owning suppression accounting and one owning every database call:
```

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: finalize CLAUDE.md description of IntelligenceRepo layer

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3.7: Open PR 3

- [ ] **Step 1: Push the branch**

```bash
git push -u origin feat/intelligence-repo-delivery
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create --title "refactor(delivery): route Supabase calls through IntelligenceRepo (PR 3/3)" --body "$(cat <<'EOF'
## Summary

- Routes all three \`delivery_engine.py\` Supabase call sites through \`_repo()\`: \`fetch_todays_intelligence\`, \`fetch_macro_summary\`, and \`_update_delivery_summary_counts\`.
- Moves \`alert_tier\` decoration out of \`fetch_todays_intelligence\` into the only consumer (the logging block in \`execute_pipeline\`).
- Rewrites three \`generate_html_email_*\` integration tests to use \`InMemoryIntelligenceRepo\` instead of six-deep Supabase mock chains.
- Deletes \`_get_supabase\` and the \`supabase\` SDK imports from \`delivery_engine.py\`.
- Updates \`CLAUDE.md\` to reflect the final architecture.

## Behavior changes

- **Preserved:** \`_update_delivery_summary_counts\` still swallows write failures (the email goes out even if the metadata write fails). The repo's underlying write contract is "raise"; this caller wraps the call in try/except per the existing operational decision documented in the source comment.
- **No other behavior changes** — the renderer, the suppression rules, and the email-send path are untouched.

## Test plan

- [ ] \`pytest tests/ -q\` passes
- [ ] No test references \`delivery_engine._get_supabase\` (deleted)
- [ ] Manual delivery run: \`python delivery_engine.py\` against today's articles — email arrives with same content and the QA suppression block shows the same numbers
- [ ] (Optional) Same-day-retry verification: run delivery twice and confirm the second \`update_delivery_counts\` call merges with the prior row, not overwrites it

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL printed to stdout.

---

# Risk register

| Risk | Trigger | Mitigation |
|------|---------|------------|
| In-memory fake drifts from Supabase semantics | A test passes against the fake but the real pipeline fails | Wiring tests on `SupabaseIntelligenceRepo` (Tasks 1.5, 1.6) pin the exact table names, on_conflict strings, and filter chains. If they change, those tests fail loudly. |
| `upsert_insight` raising changes cron behavior | A transient Supabase write failure now propagates to the GitHub Action and shows red | This is intentional — see PR 2 acceptance criteria. The per-article try/except at the call site in `execute_pipeline` keeps the batch alive; only an exhausted retry loop or a complete outage will surface. If this turns out to be too noisy in practice, wrap at execute_pipeline scope. |
| `update_delivery_counts` raises and blocks the email | A bug in PR 3 forgets the try/except wrapper | Task 3.3 has an explicit test (`test_update_delivery_summary_counts_swallows_write_failure`) that pins the wrapper. |
| Singleton state leaks between tests | One test patches `_repo`, the next assumes the live adapter | `monkeypatch` automatically undoes patches at test teardown. The `_reset_repo()` helper exists for the rare test that wants to exercise the live singleton fresh. |
| Move of `alert_tier` decoration changes downstream behavior | Something else reads `row["alert_tier"]` we haven't found | Grep before merging Task 3.1: `grep -rn 'alert_tier' /workspaces/market-pulse/` should show only the logging block and the SQL view definition in `schema.sql` (the view is unused by the engines at runtime per CLAUDE.md). |

# Self-review checklist

- [ ] Every spec requirement (locked-in design from the grilling loop) maps to a task. ✓
- [ ] No placeholders. ✓
- [ ] Method signatures used in PR 2 / PR 3 match the ones defined in PR 1. ✓
- [ ] Exact file paths and line ranges given. ✓
- [ ] Each task is independently committable and leaves the suite green. ✓
- [ ] Each PR is independently shippable and leaves the cron pipeline functional. ✓
