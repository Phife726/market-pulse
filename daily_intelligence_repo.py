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
        return url_hash in self._articles

    def upsert_insight(self, payload: dict) -> None:
        url_hash = payload.get("url_hash")
        if not url_hash:
            raise ValueError("payload missing url_hash")
        row = dict(payload)
        row.setdefault("created_at", self._now().isoformat())
        self._articles[url_hash] = row

    def recent_headlines(self, hours: int) -> set[str]:
        return {row.get("headline", "") for row in self.fetch_recent(hours)
                if row.get("headline")}

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
