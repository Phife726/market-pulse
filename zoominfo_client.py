"""ZoomInfo company-news enrichment client.

Supplemental article-discovery provider for the market-pulse ingestion
pipeline. Company/news enrichment ONLY — this module never calls any
ZoomInfo Contact API and never returns contact data.

Auth (in priority order):
  1. OAuth Client Credentials (preferred): ``ZOOMINFO_CLIENT_ID`` +
     ``ZOOMINFO_CLIENT_SECRET`` exchange for a short-lived access token at
     ``ZOOMINFO_TOKEN_URL`` (HTTP Basic auth, ``grant_type=client_credentials``).
     The access token is cached in-process until shortly before it expires.
  2. Static bearer token (local/dev fallback): ``ZOOMINFO_BEARER_TOKEN``.
  3. None configured -> warn and return ``[]``.

Neither the client secret nor any access/bearer token is ever logged. Every
failure mode (missing creds, auth error, rate limit, server error, transport
error, malformed body) is swallowed: the call logs and returns ``[]`` so a
ZoomInfo outage degrades discovery instead of crashing the cron.

The News Enrichment and OAuth token endpoint URLs default to ZoomInfo's
published paths but can be overridden with ``ZOOMINFO_NEWS_ENDPOINT`` and
``ZOOMINFO_TOKEN_URL`` so operators can correct them without a code change.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Override-able so the endpoints can be corrected without a code change.
# Published ZoomInfo GTM Enrich News + OAuth token endpoints.
_DEFAULT_NEWS_ENDPOINT = "https://api.zoominfo.com/gtm/data/v1/news/enrich"
_DEFAULT_TOKEN_URL = "https://api.zoominfo.com/gtm/oauth/v1/token"
_REQUEST_TIMEOUT = 15  # seconds
_TOKEN_SAFETY_MARGIN = 60  # seconds shaved off expires_in before re-auth

# In-process OAuth access-token cache: {"access_token": str, "expires_at": float}
# expires_at is a time.monotonic() deadline. Reset via _reset_token_cache().
_TOKEN_CACHE: dict = {}

# News categories/scopes requested from ZoomInfo.
NEWS_SCOPES: list[str] = [
    "FINANCIAL_RESULTS",
    "FUNDING",
    "GENERAL_NEWS",
    "GENERAL_PRESS_RELEASE",
    "MERGER_OR_ACQUISITION",
    "PERSON",
    "PRODUCT",
]

# Candidate keys ZoomInfo may use for each logical field. First non-empty wins.
_URL_KEYS = ("url", "newsUrl", "link", "articleUrl", "sourceUrl", "newsLink")
_TITLE_KEYS = ("title", "headline", "newsTitle", "name")
_PUBLISHER_KEYS = (
    "source", "publisher", "sourcePublication", "publicationName",
    "sourceName", "publication",
)
_PUBLISHED_AT_KEYS = (
    "publishedDate", "publicationDate", "publishedAt", "date",
    "newsDate", "publishDate",
)
_DESCRIPTION_KEYS = ("description", "summary", "snippet", "content", "abstract")
_CATEGORY_KEYS = ("categories", "newsTypes", "types", "tags", "topics", "scopes")

# Candidate keys the article list may live under in the response envelope.
_NEWS_LIST_KEYS = ("news", "results", "result", "data", "articles", "items")


def _endpoint() -> str:
    return os.environ.get("ZOOMINFO_NEWS_ENDPOINT", "").strip() or _DEFAULT_NEWS_ENDPOINT


def _token_url() -> str:
    return os.environ.get("ZOOMINFO_TOKEN_URL", "").strip() or _DEFAULT_TOKEN_URL


def _reset_token_cache() -> None:
    """Drop the cached OAuth access token. Test-only/dev helper."""
    _TOKEN_CACHE.clear()


def _request_oauth_token(client_id: str, client_secret: str) -> Optional[str]:
    """Exchange client credentials for an access token, caching it in-process.

    Returns the access token, or None on any failure (logged). Neither the
    client secret nor the access token is ever logged.
    """
    now = time.monotonic()
    cached = _TOKEN_CACHE.get("access_token")
    if cached and now < _TOKEN_CACHE.get("expires_at", 0.0):
        return cached

    try:
        response = requests.post(
            _token_url(),
            auth=(client_id, client_secret),
            data={"grant_type": "client_credentials"},
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=_REQUEST_TIMEOUT,
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        status = getattr(exc.response, "status_code", None)
        if status in (401, 403):
            logger.error(
                "ZoomInfo OAuth auth error (%s) — check ZOOMINFO_CLIENT_ID/"
                "ZOOMINFO_CLIENT_SECRET and scopes (api:data:company, api:data:news)",
                status,
            )
        elif status == 429:
            logger.warning("ZoomInfo OAuth rate limited (429) — skipping")
        elif status is not None and 500 <= status < 600:
            logger.warning("ZoomInfo OAuth server error (%s) — skipping", status)
        else:
            logger.error("ZoomInfo OAuth HTTP error (%s)", status)
        return None
    except requests.exceptions.RequestException as exc:
        logger.error("ZoomInfo OAuth token request failed: %s", exc)
        return None

    try:
        data = response.json()
    except ValueError as exc:
        logger.error("ZoomInfo OAuth returned a non-JSON token response: %s", exc)
        return None

    token = data.get("access_token")
    if not isinstance(token, str) or not token:
        logger.error("ZoomInfo OAuth token response missing access_token")
        return None

    expires_in = data.get("expires_in")
    if isinstance(expires_in, (int, float)) and expires_in > 0:
        _TOKEN_CACHE["access_token"] = token
        _TOKEN_CACHE["expires_at"] = now + max(0.0, float(expires_in) - _TOKEN_SAFETY_MARGIN)
    else:
        # No usable expiry — don't cache; re-auth on the next call.
        _TOKEN_CACHE.clear()
    return token


def _resolve_access_token() -> Optional[str]:
    """Resolve a bearer value for the news request.

    Prefers OAuth client credentials; falls back to a static bearer token for
    local/dev. Returns None (with a warning) when nothing is configured.
    """
    client_id = os.environ.get("ZOOMINFO_CLIENT_ID")
    client_secret = os.environ.get("ZOOMINFO_CLIENT_SECRET")
    if client_id and client_secret:
        return _request_oauth_token(client_id, client_secret)

    bearer = os.environ.get("ZOOMINFO_BEARER_TOKEN")
    if bearer:
        return bearer

    logger.warning(
        "No ZoomInfo credentials configured — set ZOOMINFO_CLIENT_ID/"
        "ZOOMINFO_CLIENT_SECRET (preferred) or ZOOMINFO_BEARER_TOKEN; "
        "skipping ZoomInfo news"
    )
    return None


def _first_str(item: dict, keys: tuple[str, ...]) -> str:
    """Return the first non-empty string value among *keys*."""
    for key in keys:
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _extract_url(item: dict) -> str:
    return _first_str(item, _URL_KEYS)


def _extract_title(item: dict) -> str:
    return _first_str(item, _TITLE_KEYS)


def _extract_publisher(item: dict) -> str:
    return _first_str(item, _PUBLISHER_KEYS)


def _extract_published_at(item: dict) -> str:
    return _first_str(item, _PUBLISHED_AT_KEYS)


def _extract_description(item: dict) -> str:
    return _first_str(item, _DESCRIPTION_KEYS)


def _extract_categories(item: dict) -> list[str]:
    """Return a list of category labels, normalising scalars/strings into a list."""
    for key in _CATEGORY_KEYS:
        value = item.get(key)
        if isinstance(value, list):
            return [str(v) for v in value if v not in (None, "")]
        if isinstance(value, str) and value.strip():
            return [value.strip()]
    return []


def _extract_news_items(payload: object) -> list[dict]:
    """Locate the list of news article dicts inside a ZoomInfo response.

    ZoomInfo may return the list at the top level, nested under ``data``, or
    under one of several envelope keys. Be defensive about all of them.
    """
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    # Direct list under a known key.
    for key in _NEWS_LIST_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    # One level of nesting (e.g. {"data": {"news": [...]}}).
    for key in ("data", "result"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            for sub_key in _NEWS_LIST_KEYS:
                value = nested.get(sub_key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
    return []


def _build_request(*, zoominfo_company_id: int, page_size: int) -> tuple[dict, dict]:
    """Assemble the News Enrichment request for a single company.

    Returns ``(params, body)``. Pagination travels as JSON:API query params;
    the enrichment criteria live under ``data.attributes``. No date attribute
    is sent — the live API rejects ``publishingDateStart`` ("Invalid field
    requested"), so recency is enforced best-effort client-side after parsing.
    """
    params = {
        "page[number]": 1,
        "page[size]": page_size,
    }
    body = {
        "data": {
            # Documented JSON:API resource type; the live API requires "NewsEnrich".
            "type": "NewsEnrich",
            "attributes": {
                # Documented Enrich News identifier (companyId | companyName |
                # companyWebsite). The undocumented zoominfoCompanyId returns 400.
                "companyId": zoominfo_company_id,
                "categories": NEWS_SCOPES,
            },
        },
    }
    return params, body


def _parse_date(value: object) -> Optional[date]:
    """Best-effort parse of a date or ISO datetime string into a date.

    Returns None for empty/unparseable values so callers can treat 'no usable
    date' as 'do not filter'."""
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    for candidate in (raw, raw.replace("Z", "+00:00")):
        try:
            return datetime.fromisoformat(candidate).date()
        except ValueError:
            pass
    try:
        return datetime.strptime(raw[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _filter_by_published_date(candidates: list[dict], cutoff: Optional[date]) -> list[dict]:
    """Best-effort recency filter applied client-side.

    Drops candidates whose published_at parses to a date strictly before
    *cutoff*. Candidates with a missing or unparseable date are always kept —
    the filter must never discard records just because they lack a date."""
    if cutoff is None:
        return candidates
    kept: list[dict] = []
    for candidate in candidates:
        published = _parse_date(candidate.get("published_at"))
        if published is None or published >= cutoff:
            kept.append(candidate)
    return kept


def _response_snippet(response: object, limit: int = 500) -> str:
    """Return a single-line, length-capped snippet of a response body for
    diagnostics. Reads only the response body — never request headers/body —
    so credentials are never surfaced."""
    try:
        text = getattr(response, "text", "") or ""
    except Exception:
        return ""
    return " ".join(str(text).split())[:limit]


def discover_company_news(
    *,
    zoominfo_company_id: int,
    publishing_date_start: str,
    page_size: int,
) -> list[dict]:
    """Fetch recent company news for one ZoomInfo company id.

    Returns a list of normalised candidate dicts (possibly empty). Never
    raises: every failure is logged and yields ``[]``.
    """
    token = _resolve_access_token()
    if not token:
        return []

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    params, body = _build_request(
        zoominfo_company_id=zoominfo_company_id,
        page_size=page_size,
    )

    try:
        response = requests.post(
            _endpoint(), params=params, json=body, headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        status = getattr(exc.response, "status_code", None)
        if status in (401, 403):
            logger.error(
                "ZoomInfo auth error (%s) for company %s — check ZoomInfo credentials and scopes",
                status, zoominfo_company_id,
            )
        elif status == 400:
            logger.error(
                "ZoomInfo bad request (400) for company %s — response: %s",
                zoominfo_company_id, _response_snippet(exc.response),
            )
        elif status == 429:
            logger.warning(
                "ZoomInfo rate limited (429) for company %s — skipping", zoominfo_company_id
            )
        elif status is not None and 500 <= status < 600:
            logger.warning(
                "ZoomInfo server error (%s) for company %s — skipping",
                status, zoominfo_company_id,
            )
        else:
            logger.error(
                "ZoomInfo HTTP error (%s) for company %s", status, zoominfo_company_id
            )
        return []
    except requests.exceptions.RequestException as exc:
        logger.error("ZoomInfo request failed for company %s: %s", zoominfo_company_id, exc)
        return []

    try:
        data = response.json()
    except ValueError as exc:
        logger.error(
            "ZoomInfo returned non-JSON body for company %s: %s", zoominfo_company_id, exc
        )
        return []

    candidates: list[dict] = []
    for item in _extract_news_items(data):
        url = _extract_url(item)
        if not url:
            continue
        candidates.append({
            "url": url,
            "title": _extract_title(item),
            "provider": "zoominfo",
            "source_publication": _extract_publisher(item),
            "published_at": _extract_published_at(item),
            "description": _extract_description(item),
            "categories": _extract_categories(item),
            "zoominfo_company_id": zoominfo_company_id,
            "raw": item,
        })

    parsed_count = len(candidates)
    # Best-effort recency filter: the API no longer takes a date param, so apply
    # the lookback cutoff client-side. Undated/unparseable records are kept.
    candidates = _filter_by_published_date(candidates, _parse_date(publishing_date_start))
    dropped = parsed_count - len(candidates)
    if dropped:
        logger.info(
            "ZoomInfo client-side date filter dropped %d of %d item(s) "
            "older than %s for company %s",
            dropped, parsed_count, publishing_date_start, zoominfo_company_id,
        )

    logger.info(
        "ZoomInfo discovered %d news item(s) for company %s",
        len(candidates), zoominfo_company_id,
    )
    return candidates
