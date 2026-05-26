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
