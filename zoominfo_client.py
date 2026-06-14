"""ZoomInfo company-news enrichment client.

Supplemental article-discovery provider for the market-pulse ingestion
pipeline. Company/news enrichment ONLY — this module never calls any
ZoomInfo Contact API and never returns contact data.

Auth is a static bearer token read from ``ZOOMINFO_BEARER_TOKEN``. The token
is never logged. Every failure mode (missing token, auth error, rate limit,
server error, transport error, malformed body) is swallowed: the function
logs and returns ``[]`` so a ZoomInfo outage degrades discovery instead of
crashing the cron.

The News Enrichment endpoint URL defaults to ZoomInfo's published path but
can be overridden with ``ZOOMINFO_NEWS_ENDPOINT`` so operators can correct it
without a code change.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Override-able so the endpoint can be corrected without a code change.
_DEFAULT_NEWS_ENDPOINT = "https://api.zoominfo.com/enrich/news"
_REQUEST_TIMEOUT = 15  # seconds

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


def _build_request_body(
    *, zoominfo_company_id: int, publishing_date_start: str, page_size: int
) -> dict:
    """Assemble the News Enrichment request body for a single company."""
    return {
        "companyId": zoominfo_company_id,
        "rpp": page_size,
        "page": 1,
        "publishedStartDate": publishing_date_start,
        "newsTypes": NEWS_SCOPES,
    }


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
    token = os.environ.get("ZOOMINFO_BEARER_TOKEN")
    if not token:
        logger.warning(
            "ZOOMINFO_BEARER_TOKEN not set — skipping ZoomInfo news for company %s",
            zoominfo_company_id,
        )
        return []

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    body = _build_request_body(
        zoominfo_company_id=zoominfo_company_id,
        publishing_date_start=publishing_date_start,
        page_size=page_size,
    )

    try:
        response = requests.post(
            _endpoint(), json=body, headers=headers, timeout=_REQUEST_TIMEOUT
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        status = getattr(exc.response, "status_code", None)
        if status in (401, 403):
            logger.error(
                "ZoomInfo auth error (%s) for company %s — check ZOOMINFO_BEARER_TOKEN",
                status, zoominfo_company_id,
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

    logger.info(
        "ZoomInfo discovered %d news item(s) for company %s",
        len(candidates), zoominfo_company_id,
    )
    return candidates
