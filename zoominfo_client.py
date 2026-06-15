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
# Verified against ZoomInfo GTM API docs 2026-06-14 (POST, "companies" plural).
# Override-able so a doc correction never needs a code change.
_DEFAULT_ENRICH_ENDPOINT = "https://api.zoominfo.com/gtm/data/v1/companies/enrich"
_DEFAULT_SEARCH_ENDPOINT = "https://api.zoominfo.com/gtm/data/v1/companies/search"
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
_URL_KEYS = ("url", "articleUrl", "link", "webUrl", "newsUrl", "sourceUrl", "newsLink")
_TITLE_KEYS = ("title", "headline", "newsTitle", "name")
_PUBLISHER_KEYS = (
    "source", "sourceName", "publisher", "publication",
    "sourcePublication", "publicationName",
)
_PUBLISHED_AT_KEYS = (
    "publishedDate", "publishingDate", "publicationDate", "publishedAt",
    "date", "newsDate", "publishDate",
)
_DESCRIPTION_KEYS = ("description", "summary", "snippet", "body", "content", "abstract")
_CATEGORY_KEYS = ("categories", "category", "newsTypes", "types", "tags", "topics", "scopes")

# Candidate keys the article list may live under in the response envelope.
_NEWS_LIST_KEYS = ("news", "results", "result", "data", "articles", "items")


def _endpoint() -> str:
    return os.environ.get("ZOOMINFO_NEWS_ENDPOINT", "").strip() or _DEFAULT_NEWS_ENDPOINT


def _token_url() -> str:
    return os.environ.get("ZOOMINFO_TOKEN_URL", "").strip() or _DEFAULT_TOKEN_URL


def _enrich_endpoint() -> str:
    return os.environ.get("ZOOMINFO_ENRICH_ENDPOINT", "").strip() or _DEFAULT_ENRICH_ENDPOINT


def _search_endpoint() -> str:
    return os.environ.get("ZOOMINFO_SEARCH_ENDPOINT", "").strip() or _DEFAULT_SEARCH_ENDPOINT


# Verified live 2026-06-14 (company_id 357374413 / Avient): the exact Company
# Enrich `outputFields` tokens that returned populated attributes. The endpoint
# requires this list — without it the call 400s. Keep in sync with the response
# field names target_enricher.extract_firmographics maps.
_ENRICH_OUTPUT_FIELDS = [
    "name", "revenue", "employeeCount", "primaryIndustry",
    "industries", "country", "state",
]

# Candidate keys ZoomInfo may use for a company id / name in search + enrich bodies.
_COMPANY_ID_KEYS = ("id", "companyId", "zoominfoCompanyId", "company_id")
_COMPANY_NAME_KEYS = ("name", "companyName", "canonicalName")  # reserved for enrich_company body builder / firmographics extraction
# Candidate keys the company list may live under in a search response envelope.
_COMPANY_LIST_KEYS = ("data", "results", "result", "companies", "items")


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


def _classify_http_error(exc: requests.exceptions.HTTPError, context: str) -> str:
    """Map an HTTPError to the 'error' sentinel, logging per status. Returns
    the literal "error" so callers can `return {"status": _classify...}`."""
    status = getattr(exc.response, "status_code", None)
    if status in (401, 403):
        logger.error(
            "ZoomInfo auth/scope error (%s) for %s — entitlement is per-endpoint; "
            "this proves nothing about other endpoints", status, context,
        )
    elif status == 400:
        # Sanitized request-schema diagnostic: surface ZoomInfo's own error
        # pointer (e.g. "Invalid field requested") so the next entitled live run
        # can confirm the Company Search/Enrich body schema. Reads only the
        # response body, capped — never the request body, headers, or token.
        logger.error(
            "ZoomInfo bad request (400) for %s — response: %s",
            context, _response_snippet(exc.response),
        )
    elif status == 429:
        logger.warning("ZoomInfo rate limited (429) for %s — skipping", context)
    elif status is not None and 500 <= status < 600:
        logger.warning("ZoomInfo server error (%s) for %s — skipping", status, context)
    else:
        logger.error("ZoomInfo HTTP error (%s) for %s", status, context)
    return "error"


def _company_search_body(*, domain, name, hq_country, hq_state) -> dict:
    """Assemble a Company Search request body from whichever hints are given.

    NOTE (verification obligation): the JSON:API `data` wrapper + POST method are
    doc-verified, but the attribute names below (companyWebsite/companyName/
    country/state) and the resource `type` are NOT yet confirmed against the
    ZoomInfo API reference. Confirm and adjust before relying on live results.
    """
    criteria: dict = {}
    if domain:
        criteria["companyWebsite"] = domain
    if name:
        criteria["companyName"] = name
    if hq_country:
        criteria["country"] = hq_country
    if hq_state:
        criteria["state"] = hq_state
    return {"data": {"type": "CompanySearch", "attributes": criteria}}


def _extract_company_list(payload: object) -> list:
    """Locate the list of company dicts inside a search response envelope."""
    if isinstance(payload, list):
        return [c for c in payload if isinstance(c, dict)]
    if not isinstance(payload, dict):
        return []
    for key in _COMPANY_LIST_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            return [c for c in value if isinstance(c, dict)]
    return []


def _first_company_id(company: dict) -> Optional[int]:
    """Return the first integer-coercible company id among known keys."""
    attrs = company.get("attributes")
    sources = (company, attrs) if isinstance(attrs, dict) else (company,)
    for source in sources:
        for key in _COMPANY_ID_KEYS:
            value = source.get(key)
            if isinstance(value, bool):
                continue
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.strip().isdigit():
                return int(value.strip())
    return None


def resolve_company(*, domain=None, name=None, hq_country=None, hq_state=None) -> dict:
    """Resolve a ZoomInfo company id from a single set of hints.

    Returns {"status": "ok", "company_id": int} on a match, {"status": "empty"}
    when the API returns no candidate, or {"status": "error"} on any auth/scope/
    transport/malformed failure. Never raises.
    """
    token = _resolve_access_token()
    if not token:
        return {"status": "error"}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = _company_search_body(
        domain=domain, name=name, hq_country=hq_country, hq_state=hq_state
    )
    context = f"resolve(domain={domain!r}, name={name!r})"
    try:
        response = requests.post(
            _search_endpoint(), json=body, headers=headers, timeout=_REQUEST_TIMEOUT
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        return {"status": _classify_http_error(exc, context)}
    except requests.exceptions.RequestException as exc:
        logger.error("ZoomInfo search request failed for %s: %s", context, exc)
        return {"status": "error"}
    try:
        data = response.json()
    except ValueError as exc:
        logger.error("ZoomInfo search non-JSON body for %s: %s", context, exc)
        return {"status": "error"}
    # Keys-only structural diagnostics (no values) so the next entitled live run
    # can confirm where the company id/firmographics live in the response.
    logger.info(
        "ZoomInfo search raw response summary for %s: %s",
        context, _summarize_response_shape(data),
    )
    companies = _extract_company_list(data)
    if companies:
        logger.info(
            "ZoomInfo search first company shape for %s: %s",
            context, _summarize_first_item_shape(companies),
        )
    for company in companies:
        company_id = _first_company_id(company)
        if company_id is not None:
            return {"status": "ok", "company_id": company_id}
    return {"status": "empty"}


def enrich_company(company_id: int) -> dict:
    """Fetch firmographics for one ZoomInfo company id.

    Returns {"status": "ok", "company": <raw dict>} on success, {"status":
    "empty"} when no company is returned, or {"status": "error"} on any failure.
    The raw company dict is handed back unmapped — field mapping is the pure
    target_enricher module's job. Never raises.
    """
    token = _resolve_access_token()
    if not token:
        return {"status": "error"}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # VERIFIED LIVE 2026-06-14 (company_id 357374413 / Avient) via a now-removed
    # request-schema probe run in the smoke workflow. The GTM Company Enrich endpoint:
    #   - identifies records by a `matchCompanyInput` LIST (singular `companyId`
    #     and plural `companyIds` under data.attributes both return 400), and
    #   - REQUIRES an `outputFields` list (omitting it returns 400 "Missing
    #     required field 'outputfields'"); the camelCase `outputFields` key with
    #     the tokens below produced the 200.
    # target_enricher still gates `verified` on a populated canonical_name, so a
    # sparse response is recorded as `missing`, never a misleading `verified`.
    body = {"data": {"type": "CompanyEnrich", "attributes": {
        "matchCompanyInput": [{"companyId": company_id}],
        "outputFields": _ENRICH_OUTPUT_FIELDS,
    }}}
    context = f"enrich(company_id={company_id})"
    try:
        response = requests.post(
            _enrich_endpoint(), json=body, headers=headers, timeout=_REQUEST_TIMEOUT
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        return {"status": _classify_http_error(exc, context)}
    except requests.exceptions.RequestException as exc:
        logger.error("ZoomInfo enrich request failed for %s: %s", context, exc)
        return {"status": "error"}
    try:
        data = response.json()
    except ValueError as exc:
        logger.error("ZoomInfo enrich non-JSON body for %s: %s", context, exc)
        return {"status": "error"}
    # Keys-only structural diagnostics (no values) so the next entitled live run
    # can confirm which firmographic field tokens come back (and whether an
    # `outputFields` request body is needed to populate them at all).
    logger.info(
        "ZoomInfo enrich raw response summary for %s: %s",
        context, _summarize_response_shape(data),
    )
    companies = _extract_company_list(data)
    if not companies:
        return {"status": "empty"}
    logger.info(
        "ZoomInfo enrich first company shape for %s: %s",
        context, _summarize_first_item_shape(companies),
    )
    company = companies[0]
    attrs = company.get("attributes")
    return {"status": "ok", "company": attrs if isinstance(attrs, dict) else company}


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


def _article_payload(item: dict) -> dict:
    """Return the field bag for one news record.

    Live Enrich News records are JSON:API resource objects whose fields live
    under ``attributes``; older/flat shapes carry fields on the item itself.
    Falls back to ``{}`` for anything non-dict so a malformed item is skipped,
    not fatal.
    """
    if not isinstance(item, dict):
        return {}
    attrs = item.get("attributes")
    if isinstance(attrs, dict):
        return attrs
    return item


def _summarize_first_item_shape(items: list) -> Optional[str]:
    """Return a keys-only shape summary of the first data record (no values).

    Surfaces ``item_keys`` and ``attribute_keys`` so the smoke can confirm
    where the article fields live. Never emits titles, URLs, article bodies,
    sources, tokens, headers, or the request body.
    """
    if not items:
        return None
    first = items[0]
    if not isinstance(first, dict):
        return "item_keys=[] attribute_keys=[]"
    item_keys = sorted(first.keys())
    attrs = first.get("attributes")
    attribute_keys = sorted(attrs.keys()) if isinstance(attrs, dict) else []
    return f"item_keys={item_keys} attribute_keys={attribute_keys}"


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


def _summarize_response_shape(data: object) -> str:
    """Return a structural summary of a parsed ZoomInfo 200 response.

    Surfaces only shapes, types, counts, and top-level key *names* — never
    values, article bodies, URLs, tokens, headers, or the request body. Lets
    the smoke distinguish 'ZoomInfo returned empty data' from 'the parser is
    not looking in the right response path'.
    """
    if isinstance(data, list):
        return (
            f"status=200 top_level_keys=[] data_type=list "
            f"data_count={len(data)} included_count=n/a"
        )
    if not isinstance(data, dict):
        return (
            f"status=200 top_level_keys=[] data_type={type(data).__name__} "
            f"data_count=n/a included_count=n/a"
        )
    top_level_keys = sorted(data.keys())
    payload_data = data.get("data")
    data_count = len(payload_data) if isinstance(payload_data, list) else "n/a"
    included = data.get("included")
    included_count = len(included) if isinstance(included, list) else "n/a"
    return (
        f"status=200 top_level_keys={top_level_keys} "
        f"data_type={type(payload_data).__name__} "
        f"data_count={data_count} included_count={included_count}"
    )


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

    # Diagnostic: structural summary only (no values) so the smoke can tell an
    # empty result from a parser-path mismatch.
    logger.info(
        "ZoomInfo raw response summary for company %s: %s",
        zoominfo_company_id, _summarize_response_shape(data),
    )

    items = _extract_news_items(data)
    if items:
        logger.info(
            "ZoomInfo first data item shape for company %s: %s",
            zoominfo_company_id, _summarize_first_item_shape(items),
        )

    candidates: list[dict] = []
    for item in items:
        payload = _article_payload(item)
        url = _extract_url(payload)
        title = _extract_title(payload)
        if not url or not title:
            continue
        candidates.append({
            "url": url,
            "title": title,
            "provider": "zoominfo",
            "source_publication": _extract_publisher(payload),
            "published_at": _extract_published_at(payload),
            "description": _extract_description(payload),
            "categories": _extract_categories(payload),
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
