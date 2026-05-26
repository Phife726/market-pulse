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
