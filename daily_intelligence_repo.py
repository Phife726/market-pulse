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
