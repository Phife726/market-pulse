"""Discovery-provider seam — how the ingestion engine consumes article sources.

One Protocol (``DiscoveryProvider``) with two production adapters
(``SerperProvider`` always on; ``ZoomInfoProvider`` feature-flagged) and an
in-memory fake (``FakeDiscoveryProvider``). Matches the house pattern of the
``llm`` / ``daily_intelligence_repo`` seams: callers do ``from discovery import
_discovery_providers`` and tests inject the fake at the consumer module, e.g.
``monkeypatch.setattr("ingestion_engine._discovery_providers", lambda: [fake])``.

A provider exposes three things:

- ``name`` — the provider label stamped on every candidate and used as the
  provider_yield bookkeeping key (generic over N providers).
- ``eligible(target)`` — whether discovery should run for this target this run
  (reads its own feature flags via ``config`` at use time — config owns the flag
  *values*, the adapter *consumes* them).
- ``discover(target) -> list[dict]`` — provider-neutral candidate dicts.
- ``gate(candidate, target) -> Optional[GateDecision]`` — an optional
  post-discovery false-positive decision the *consumer* applies (so suppression
  accounting stays in the ledger); providers with no gate return ``None``.

Candidates stay plain dicts (like ``insight``), so the stored-row builder and
the gauntlet keep relying on the shape.
"""
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, Protocol

import requests

import config
import relevance_gate
import zoominfo_client

logger = logging.getLogger(__name__)


class DiscoveryProvider(Protocol):
    """A source of article candidates for one target."""

    name: str

    def eligible(self, target: dict) -> bool: ...

    def discover(self, target: dict) -> list[dict]: ...

    def gate(self, candidate: dict, target: dict) -> "Optional[relevance_gate.GateDecision]": ...


# ---------------------------------------------------------------------------
# Serper
# ---------------------------------------------------------------------------

def discover_urls(query: str, lookback_hours: int, results_per_entity: int) -> list[tuple[str, str]]:
    api_key = os.environ["SERPER_API_KEY"]
    endpoint = "https://google.serper.dev/news"
    payload = {"q": query, "num": results_per_entity, "tbs": f"qdr:h{lookback_hours}"}
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
    try:
        response = requests.post(endpoint, json=payload, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logger.error("Serper.dev request timed out for query '%s'", query[:80])
        return []
    except requests.exceptions.HTTPError as exc:
        logger.error("Serper.dev HTTP error for query '%s': %s", query[:80], exc.response.status_code)
        return []
    except requests.exceptions.RequestException as exc:
        logger.error("Serper.dev request failed for query '%s': %s", query[:80], exc)
        return []
    data = response.json()
    # Serper's news endpoint returns pages of 10 and ignores small `num`
    # values, so results_per_entity must be enforced client-side.
    raw_results = [
        (item["link"], item.get("title", ""))
        for item in data.get("news", [])
        if "link" in item
    ]
    results = raw_results[:results_per_entity]
    logger.info(
        "Discovered %d URL(s) (kept %d) for query '%s'",
        len(raw_results), len(results), query[:80],
    )
    return results


def _serper_candidate(raw_url: str, title: str) -> dict:
    """Wrap a Serper (url, title) pair in the provider-neutral candidate shape."""
    return {
        "url": raw_url,
        "title": title,
        "provider": "serper",
        "source_publication": "",
        "published_at": "",
        "description": "",
        "categories": [],
        "zoominfo_company_id": None,
        "raw": {},
    }


class SerperProvider:
    """Always-on Serper.dev news discovery. Never gates (Serper candidates are
    not company-linked, so the ZoomInfo false-positive gate does not apply)."""

    name = "serper"

    def eligible(self, target: dict) -> bool:
        return True

    def discover(self, target: dict) -> list[dict]:
        raw_results = discover_urls(
            target["query"], target["lookback_hours"], target["results_per_entity"]
        )
        return [_serper_candidate(url, title) for url, title in raw_results]

    def gate(self, candidate: dict, target: dict) -> "Optional[relevance_gate.GateDecision]":
        return None


# ---------------------------------------------------------------------------
# ZoomInfo
# ---------------------------------------------------------------------------

ZOOMINFO_NEWS_LOOKBACK_DAYS_DEFAULT = 2
ZOOMINFO_NEWS_PER_COMPANY_DEFAULT = 5


class ZoomInfoProvider:
    """Feature-flagged ZoomInfo company-news discovery. Owns the relevance
    gate: it loads ``target_metadata.yaml`` itself (once per run, gated by
    ``relevance_gate_enabled``) and returns a drop/keep decision the consumer
    applies. Delegates all HTTP to the ``zoominfo_client`` extraction library."""

    name = "zoominfo"

    def __init__(self) -> None:
        self._metadata: Optional[dict] = None

    def eligible(self, target: dict) -> bool:
        """True when ZoomInfo discovery should run for this target: the feature
        flag is on, a company id is mapped, and zoominfo_news is not disabled.
        Concept-mode targets carry no company id, so they are ineligible."""
        return (
            config.zoominfo_news_enabled()
            and bool(target.get("zoominfo_company_id"))
            and bool(target.get("zoominfo_news", True))
        )

    def discover(self, target: dict) -> list[dict]:
        company_id = target["zoominfo_company_id"]
        lookback_days = config.env_int(
            "ZOOMINFO_NEWS_LOOKBACK_DAYS", ZOOMINFO_NEWS_LOOKBACK_DAYS_DEFAULT
        )
        per_company = config.env_int(
            "ZOOMINFO_NEWS_PER_COMPANY", ZOOMINFO_NEWS_PER_COMPANY_DEFAULT
        )
        start_date = (datetime.utcnow() - timedelta(days=lookback_days)).date().isoformat()
        return zoominfo_client.discover_company_news(
            zoominfo_company_id=company_id,
            publishing_date_start=start_date,
            page_size=per_company,
        )

    def _target_metadata(self) -> dict:
        """Lazily load the relevance-gate companion file once per run. Returns
        ``{}`` (gate disabled) when the flag is off or the file is missing /
        malformed — ``load_target_metadata`` swallows read errors to ``{}``."""
        if self._metadata is None:
            self._metadata = (
                relevance_gate.load_target_metadata("target_metadata.yaml")
                if config.relevance_gate_enabled() else {}
            )
        return self._metadata

    def gate(self, candidate: dict, target: dict) -> "Optional[relevance_gate.GateDecision]":
        """Evaluate the ZoomInfo false-positive gate, or ``None`` when it does
        not apply (gate disabled / empty metadata, no record for the target, or
        a non-active record). Never raises."""
        metadata = self._target_metadata()
        if not metadata:
            return None
        record = metadata.get(target["name"])
        if not record or record.get("metadata_record_status") != "active":
            return None
        return relevance_gate.evaluate(
            title=candidate.get("title", ""),
            description=candidate.get("description", ""),
            record=record,
        )


# ---------------------------------------------------------------------------
# Test fake + provider registry
# ---------------------------------------------------------------------------

class FakeDiscoveryProvider:
    """In-memory discovery adapter for tests. Returns scripted candidates and
    records every ``discover`` call; ``eligible`` / ``gate`` are configurable so
    a test can exercise the consumer's provider loop without any HTTP."""

    def __init__(
        self,
        name: str,
        candidates: Optional[list[dict]] = None,
        *,
        eligible: bool = True,
        discover_error: Optional[Exception] = None,
    ) -> None:
        self.name = name
        self._candidates = candidates or []
        self._eligible = eligible
        self._discover_error = discover_error
        self.discover_calls: list[dict] = []

    def eligible(self, target: dict) -> bool:
        return self._eligible

    def discover(self, target: dict) -> list[dict]:
        self.discover_calls.append(target)
        if self._discover_error is not None:
            raise self._discover_error
        return list(self._candidates)

    def gate(self, candidate: dict, target: dict) -> "Optional[relevance_gate.GateDecision]":
        return None


_providers_singleton: Optional[list[DiscoveryProvider]] = None


def _discovery_providers() -> list[DiscoveryProvider]:
    """Return the process-wide discovery providers, in fan-in order (Serper
    first, then ZoomInfo). Tests inject fakes at the consumer module."""
    global _providers_singleton
    if _providers_singleton is None:
        _providers_singleton = [SerperProvider(), ZoomInfoProvider()]
    return _providers_singleton


def _reset_discovery_providers() -> None:
    """Drop the cached providers — used by tests for isolation."""
    global _providers_singleton
    _providers_singleton = None
