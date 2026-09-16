"""The link-reputation seam — the one outbound URL-safety check (see the
**Link-reputation seam** entry in CONTEXT.md).

One Protocol (`LinkReputation`), two adapters: `SafeBrowsingLinkReputation`
for production (the Google Safe Browsing Lookup API v4, `threatMatches:find`)
and `FakeLinkReputation` for tests. The interface is one question —
`unsafe(urls) -> frozenset[str]`, the subset of `urls` the service flags —
so the callers (the ingestion gauntlet, before the scrape; delivery, before
the report is assembled) never see the request, the response, the key or
the failure modes.

Written after the 2026-09-16 Proofpoint quarantine: the security block
(`blocked_domains.py`) stops a domain IT has already reported; this catches
a compromised page *before* IT does. It is the proactive half of the pair.

**Feature-flagged by the key.** `SAFE_BROWSING_API_KEY` absent or blank means
the check is off — every call answers "nothing flagged" with one INFO line —
so nothing changes until the secret is added to both workflows.

**Tolerant by design.** The check exists to make the digest *safer*; it must
never make the cron *red*. A missing key, an HTTP error, a quota rejection,
a timeout, an unparseable body — each degrades to "no check" for that call
with a WARNING, and the first failure switches the adapter off for the rest
of the run (`_disabled`), so a dead API costs one timeout, not one per
target inside the 30-minute pipeline deadline. Nothing here raises.

**Batched and cached.** URLs are sent in chunks of `MAX_URLS_PER_REQUEST`
(the API's limit is 500 per request) and every verdict is cached on the
adapter for the run, so a URL both engines ask about — a candidate at
ingestion, the same row at delivery — is looked up once per process.

The key is only ever a query parameter on the request; it is never logged
(a 4xx body is logged truncated, and the key is scrubbed from it).
Callers do `from link_reputation import _link_reputation` and call
`_link_reputation().unsafe(urls)`; tests inject the fake at the consumer —
e.g. `monkeypatch.setattr("ingestion_engine._link_reputation", lambda: fake)`.
"""
import logging
import os
from typing import Iterable, Optional, Protocol

import requests

logger = logging.getLogger(__name__)

SAFE_BROWSING_URL = "https://safebrowsing.googleapis.com/v4/threatMatches:find"
#: The Lookup API accepts up to 500 threat entries per request.
MAX_URLS_PER_REQUEST = 500
REQUEST_TIMEOUT_S = 10
THREAT_TYPES: tuple[str, ...] = (
    "MALWARE",
    "SOCIAL_ENGINEERING",
    "UNWANTED_SOFTWARE",
    "POTENTIALLY_HARMFUL_APPLICATION",
)
_CLIENT = {"clientId": "americhem-market-pulse", "clientVersion": "1.0"}
_BODY_LOG_LIMIT = 200


class LinkReputation(Protocol):
    def unsafe(self, urls: Iterable[Optional[str]]) -> frozenset[str]:
        """The subset of `urls` the service flags as unsafe. Empty on any
        failure — the check degrades, it never raises."""
        ...


class SafeBrowsingLinkReputation:
    """Production adapter — the Google Safe Browsing Lookup API v4 (policy:
    module docstring). One instance per process (`_link_reputation()`); the
    verdict cache and the run-level circuit breaker live on it."""

    def __init__(self) -> None:
        self._verdicts: dict[str, bool] = {}
        self._disabled = False
        self._reported_off = False

    def _key(self) -> str:
        return (os.environ.get("SAFE_BROWSING_API_KEY") or "").strip()

    def unsafe(self, urls: Iterable[Optional[str]]) -> frozenset[str]:
        wanted = list(dict.fromkeys(u for u in urls if isinstance(u, str) and u))
        if not wanted:
            return frozenset()
        key = self._key()
        if not key:
            if not self._reported_off:
                logger.info("Link-reputation check off — SAFE_BROWSING_API_KEY is not set")
                self._reported_off = True
            return frozenset()
        pending = [u for u in wanted if u not in self._verdicts]
        if pending and not self._disabled:
            for start in range(0, len(pending), MAX_URLS_PER_REQUEST):
                chunk = pending[start:start + MAX_URLS_PER_REQUEST]
                flagged = self._lookup(chunk, key)
                if flagged is None:
                    self._disabled = True
                    break
                for u in chunk:
                    self._verdicts[u] = u in flagged
        return frozenset(u for u in wanted if self._verdicts.get(u, False))

    def _lookup(self, chunk: list[str], key: str) -> Optional[set[str]]:
        """One request. The flagged subset of `chunk`, or None on any failure
        (logged as a WARNING, key scrubbed) — the caller switches off."""
        body = {
            "client": dict(_CLIENT),
            "threatInfo": {
                "threatTypes": list(THREAT_TYPES),
                "platformTypes": ["ANY_PLATFORM"],
                "threatEntryTypes": ["URL"],
                "threatEntries": [{"url": u} for u in chunk],
            },
        }
        try:
            resp = requests.post(
                SAFE_BROWSING_URL, params={"key": key}, json=body, timeout=REQUEST_TIMEOUT_S,
            )
        except requests.RequestException as exc:
            logger.warning(
                "Safe Browsing lookup failed (%s) — link-reputation check off for this run",
                type(exc).__name__,
            )
            return None
        if not resp.ok:
            snippet = (resp.text or "")[:_BODY_LOG_LIMIT].replace(key, "<key>")
            logger.warning(
                "Safe Browsing lookup returned HTTP %s — link-reputation check off for this "
                "run — body: %s", resp.status_code, snippet,
            )
            return None
        try:
            data = resp.json()
            matches = data.get("matches") or []
            if not isinstance(matches, list):
                raise ValueError("matches is not a list")
            flagged = {m["threat"]["url"] for m in matches}
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            logger.warning(
                "Safe Browsing response unreadable (%s) — link-reputation check off for this run",
                exc,
            )
            return None
        asked = set(chunk)
        for url in sorted(flagged & asked):
            logger.warning("Safe Browsing flagged URL: %s", url)
        return flagged & asked


class FakeLinkReputation:
    """Test adapter — a scripted set of unsafe URLs; records every call's
    URL list in `calls` so a consumer test can assert on what crossed the
    seam (batching, which URLs were asked about)."""

    def __init__(self, unsafe_urls: Iterable[str] = ()) -> None:
        self.unsafe_urls: set[str] = set(unsafe_urls)
        self.calls: list[list[str]] = []

    def unsafe(self, urls: Iterable[Optional[str]]) -> frozenset[str]:
        asked = [u for u in urls if isinstance(u, str) and u]
        self.calls.append(asked)
        return frozenset(u for u in asked if u in self.unsafe_urls)


_link_reputation_singleton: Optional[LinkReputation] = None


def _link_reputation() -> LinkReputation:
    """Return the process-wide adapter (Safe Browsing in prod; tests inject a fake)."""
    global _link_reputation_singleton
    if _link_reputation_singleton is None:
        _link_reputation_singleton = SafeBrowsingLinkReputation()
    return _link_reputation_singleton


def _reset_link_reputation() -> None:
    """Drop the cached adapter — used by tests for isolation."""
    global _link_reputation_singleton
    _link_reputation_singleton = None
