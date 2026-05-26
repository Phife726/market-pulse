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
