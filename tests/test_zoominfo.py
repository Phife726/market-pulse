"""
Tests for the ZoomInfo company-news enrichment discovery provider.

No live API calls — requests.post is always mocked and no real
ZOOMINFO_BEARER_TOKEN is required.
"""
import textwrap
from unittest.mock import MagicMock, patch

import pytest
import requests

import ingestion_engine
import zoominfo_client
from ingestion_engine import (
    compute_url_hash,
    discover_candidates,
    discover_serper_candidates,
    discover_zoominfo_candidates,
    load_targets,
    normalize_url,
    _env_int,
    _zoominfo_news_enabled,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _entity_yaml(extra_entity_lines: str = "") -> str:
    return textwrap.dedent(
        f"""\
        customers:
          search_mode: entity
          include_all: []
          exclude_any: []
          entities:
            - name: Magna International
              active: true
        {extra_entity_lines}
        discovery:
          results_per_entity: 2
          lookback_hours: 24
          min_article_length: 500
        """
    )


def _zi_target(company_id=12345678, zoominfo_news=True) -> dict:
    return {
        "name": "Magna International",
        "category": "customers",
        "query": '"Magna International"',
        "results_per_entity": 2,
        "lookback_hours": 24,
        "min_article_length": 500,
        "zoominfo_company_id": company_id,
        "zoominfo_news": zoominfo_news,
    }


def _post_mock(json_payload: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_payload
    resp.raise_for_status = MagicMock()
    return resp


@pytest.fixture(autouse=True)
def _reset_zoominfo_token_cache():
    """Keep the in-process OAuth token cache from leaking across tests."""
    zoominfo_client._reset_token_cache()
    yield
    zoominfo_client._reset_token_cache()


def _token_resp(access_token: str = "oauth-access-token", expires_in=3600) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {
        "access_token": access_token,
        "expires_in": expires_in,
        "token_type": "Bearer",
        "scope": "api:data:company api:data:news",
    }
    resp.raise_for_status = MagicMock()
    return resp


def _error_resp(status: int) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=resp)
    return resp


def _routed_post(token_resp: MagicMock, news_resp: MagicMock):
    """Build a requests.post side_effect that routes token vs news calls and
    records how each was invoked."""
    record = {"token_calls": 0, "news_calls": 0, "token_kwargs": None, "news_kwargs": None}

    def _post(url, *args, **kwargs):
        if "oauth" in url or url.rstrip("/").endswith("token"):
            record["token_calls"] += 1
            record["token_kwargs"] = kwargs
            return token_resp
        record["news_calls"] += 1
        record["news_kwargs"] = kwargs
        return news_resp

    return _post, record


def _set_client_creds(monkeypatch, client_id="cid-123", secret="secret-xyz") -> None:
    monkeypatch.setenv("ZOOMINFO_CLIENT_ID", client_id)
    monkeypatch.setenv("ZOOMINFO_CLIENT_SECRET", secret)


# ===========================================================================
# load_targets() — ZoomInfo field extension
# ===========================================================================

def test_load_targets_old_yaml_has_no_zoominfo_id(tmp_path):
    """Entity targets without ZoomInfo fields default to id=None, news=True."""
    cfg = tmp_path / "targets.yaml"
    cfg.write_text(_entity_yaml())
    target = load_targets(str(cfg))[0]
    assert target["zoominfo_company_id"] is None
    assert target["zoominfo_news"] is True


def test_load_targets_includes_zoominfo_company_id(tmp_path):
    """A mapped zoominfo_company_id is carried onto the target dict and news defaults True."""
    cfg = tmp_path / "targets.yaml"
    cfg.write_text(
        textwrap.dedent(
            """\
            customers:
              search_mode: entity
              include_all: []
              exclude_any: []
              entities:
                - name: Magna International
                  active: true
                  zoominfo_company_id: 12345678
            discovery:
              results_per_entity: 2
              lookback_hours: 24
              min_article_length: 500
            """
        )
    )
    target = load_targets(str(cfg))[0]
    assert target["zoominfo_company_id"] == 12345678
    assert target["zoominfo_news"] is True


def test_load_targets_respects_zoominfo_news_false(tmp_path):
    """zoominfo_news: false must be preserved even when an id exists."""
    cfg = tmp_path / "targets.yaml"
    cfg.write_text(
        textwrap.dedent(
            """\
            customers:
              search_mode: entity
              include_all: []
              exclude_any: []
              entities:
                - name: Magna International
                  active: true
                  zoominfo_company_id: 12345678
                  zoominfo_news: false
            discovery:
              results_per_entity: 2
              lookback_hours: 24
              min_article_length: 500
            """
        )
    )
    target = load_targets(str(cfg))[0]
    assert target["zoominfo_company_id"] == 12345678
    assert target["zoominfo_news"] is False


# ===========================================================================
# Feature-flag helpers
# ===========================================================================

@pytest.mark.parametrize("value", ["true", "True", "1", "yes", "on", " ON "])
def test_zoominfo_news_enabled_truthy(monkeypatch, value):
    monkeypatch.setenv("ZOOMINFO_NEWS_ENABLED", value)
    assert _zoominfo_news_enabled() is True


@pytest.mark.parametrize("value", ["false", "0", "no", "off", "", "maybe"])
def test_zoominfo_news_enabled_falsy(monkeypatch, value):
    monkeypatch.setenv("ZOOMINFO_NEWS_ENABLED", value)
    assert _zoominfo_news_enabled() is False


def test_zoominfo_news_enabled_default_off(monkeypatch):
    monkeypatch.delenv("ZOOMINFO_NEWS_ENABLED", raising=False)
    assert _zoominfo_news_enabled() is False


def test_env_int_uses_default_when_unset(monkeypatch):
    monkeypatch.delenv("ZOOMINFO_NEWS_PER_COMPANY", raising=False)
    assert _env_int("ZOOMINFO_NEWS_PER_COMPANY", 5) == 5


def test_env_int_parses_valid(monkeypatch):
    monkeypatch.setenv("ZOOMINFO_NEWS_PER_COMPANY", "7")
    assert _env_int("ZOOMINFO_NEWS_PER_COMPANY", 5) == 7


def test_env_int_invalid_falls_back_to_default(monkeypatch, caplog):
    monkeypatch.setenv("ZOOMINFO_NEWS_LOOKBACK_DAYS", "not-an-int")
    with caplog.at_level("WARNING"):
        assert _env_int("ZOOMINFO_NEWS_LOOKBACK_DAYS", 2) == 2


# ===========================================================================
# zoominfo_client.discover_company_news
# ===========================================================================

def test_discover_company_news_missing_token_returns_empty(monkeypatch):
    """No credentials at all -> log warning, return [], never raise."""
    monkeypatch.delenv("ZOOMINFO_CLIENT_ID", raising=False)
    monkeypatch.delenv("ZOOMINFO_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("ZOOMINFO_BEARER_TOKEN", raising=False)
    with patch("zoominfo_client.requests.post") as mock_post:
        result = zoominfo_client.discover_company_news(
            zoominfo_company_id=12345678,
            publishing_date_start="2026-06-12",
            page_size=5,
        )
    assert result == []
    mock_post.assert_not_called()


def test_discover_company_news_success_returns_candidates(monkeypatch):
    """Parses a representative payload with mixed key shapes into candidate dicts."""
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-token")
    payload = {
        "data": {
            "companyId": 12345678,
            "news": [
                {
                    "title": "Magna Q2 results beat estimates",
                    "url": "https://news.example.com/magna-q2",
                    "source": "Reuters",
                    "publishedDate": "2026-06-13",
                    "description": "Magna reported strong Q2 earnings.",
                    "newsTypes": ["FINANCIAL_RESULTS"],
                },
                {
                    # Alternate key shapes — exercises defensive extraction.
                    "headline": "Magna acquires Beta Corp",
                    "newsUrl": "https://news.example.com/magna-beta",
                    "publisher": "Bloomberg",
                    "publicationDate": "2026-06-12",
                    "summary": "An M&A transaction.",
                    "categories": ["MERGER_OR_ACQUISITION"],
                },
                {
                    # No URL — must be dropped.
                    "title": "Headline with no link",
                },
            ],
        }
    }
    with patch("zoominfo_client.requests.post", return_value=_post_mock(payload)):
        result = zoominfo_client.discover_company_news(
            zoominfo_company_id=12345678,
            publishing_date_start="2026-06-12",
            page_size=5,
        )

    assert len(result) == 2
    first, second = result
    assert first["url"] == "https://news.example.com/magna-q2"
    assert first["title"] == "Magna Q2 results beat estimates"
    assert first["provider"] == "zoominfo"
    assert first["source_publication"] == "Reuters"
    assert first["published_at"] == "2026-06-13"
    assert first["description"] == "Magna reported strong Q2 earnings."
    assert first["categories"] == ["FINANCIAL_RESULTS"]
    assert first["zoominfo_company_id"] == 12345678
    assert first["raw"]  # original item preserved

    assert second["url"] == "https://news.example.com/magna-beta"
    assert second["title"] == "Magna acquires Beta Corp"
    assert second["source_publication"] == "Bloomberg"
    assert second["categories"] == ["MERGER_OR_ACQUISITION"]


def test_discover_company_news_does_not_log_token(monkeypatch, caplog):
    """The bearer token must never appear in logs."""
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "super-secret-token")
    with patch("zoominfo_client.requests.post", return_value=_post_mock({"data": {"news": []}})), \
         caplog.at_level("DEBUG"):
        zoominfo_client.discover_company_news(
            zoominfo_company_id=12345678,
            publishing_date_start="2026-06-12",
            page_size=5,
        )
    assert "super-secret-token" not in caplog.text


# ---------------------------------------------------------------------------
# Published endpoint + request shape (Codex P2 finding)
# ---------------------------------------------------------------------------

def test_default_endpoint_is_published_path(monkeypatch):
    monkeypatch.delenv("ZOOMINFO_NEWS_ENDPOINT", raising=False)
    assert zoominfo_client._endpoint() == "https://api.zoominfo.com/gtm/data/v1/news/enrich"


def test_endpoint_override_preserved(monkeypatch):
    monkeypatch.setenv("ZOOMINFO_NEWS_ENDPOINT", "https://example.test/custom/news")
    assert zoominfo_client._endpoint() == "https://example.test/custom/news"


def test_request_uses_published_shape(monkeypatch):
    """Pagination travels as JSON:API query params; criteria live under
    data.attributes with ZoomInfo Enrich News field names."""
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-token")
    monkeypatch.delenv("ZOOMINFO_NEWS_ENDPOINT", raising=False)
    mock = _post_mock({"data": {"news": []}})
    with patch("zoominfo_client.requests.post", return_value=mock) as post:
        zoominfo_client.discover_company_news(
            zoominfo_company_id=12345678,
            publishing_date_start="2026-06-12",
            page_size=5,
        )

    _, kwargs = post.call_args
    # Endpoint is the published GTM path.
    assert kwargs.get("url", (post.call_args.args or [None])[0]) == \
        "https://api.zoominfo.com/gtm/data/v1/news/enrich"

    # Pagination is in the query params, not the body.
    assert kwargs["params"] == {"page[number]": 1, "page[size]": 5}

    body = kwargs["json"]
    assert body["data"]["type"] == "NewsEnrich"
    assert "newsEnrichRequest" not in str(body)
    attrs = body["data"]["attributes"]
    assert attrs["companyId"] == 12345678
    assert attrs["categories"] == zoominfo_client.NEWS_SCOPES

    # Legacy / flat / undocumented / unsupported field names must be gone.
    # publishingDateStart is rejected by the live API ("Invalid field requested").
    for bad in (
        "zoominfoCompanyId", "rpp", "page", "pageSize",
        "publishedStartDate", "publishingDateStart", "publishingDateEnd", "newsTypes",
    ):
        assert bad not in attrs
        assert bad not in body
        assert bad not in kwargs["json"]["data"]


def test_request_passes_page_size_through(monkeypatch):
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-token")
    mock = _post_mock({"data": {"news": []}})
    with patch("zoominfo_client.requests.post", return_value=mock) as post:
        zoominfo_client.discover_company_news(
            zoominfo_company_id=999,
            publishing_date_start="2026-06-01",
            page_size=10,
        )
    _, kwargs = post.call_args
    assert kwargs["params"]["page[size]"] == 10
    assert kwargs["json"]["data"]["attributes"]["companyId"] == 999


def test_request_body_uses_documented_company_id(monkeypatch):
    """Enrich News expects companyId (a documented identifier), not the
    undocumented zoominfoCompanyId that triggered HTTP 400."""
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-token")
    mock = _post_mock({"data": {"news": []}})
    with patch("zoominfo_client.requests.post", return_value=mock) as post:
        zoominfo_client.discover_company_news(
            zoominfo_company_id=357374413, publishing_date_start="2026-06-12", page_size=5
        )
    attrs = post.call_args.kwargs["json"]["data"]["attributes"]
    assert attrs["companyId"] == 357374413
    assert "zoominfoCompanyId" not in attrs


def test_request_body_omits_publishing_date_start(monkeypatch):
    """The live API rejects publishingDateStart — it must not be sent."""
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-token")
    mock = _post_mock({"data": {"news": []}})
    with patch("zoominfo_client.requests.post", return_value=mock) as post:
        zoominfo_client.discover_company_news(
            zoominfo_company_id=1, publishing_date_start="2026-06-12", page_size=5
        )
    kwargs = post.call_args.kwargs
    attrs = kwargs["json"]["data"]["attributes"]
    assert "publishingDateStart" not in attrs
    assert "publishedStartDate" not in attrs
    assert attrs["companyId"] == 1
    assert attrs["categories"] == zoominfo_client.NEWS_SCOPES
    # Pagination still travels as query params.
    assert kwargs["params"] == {"page[number]": 1, "page[size]": 5}


def test_client_side_date_filter_drops_old_keeps_undated(monkeypatch):
    """Best-effort client-side filter: drop parseable dates before the cutoff,
    but never drop records that have no parseable published date."""
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-token")
    payload = {"data": {"news": [
        {"title": "recent", "url": "https://n/recent", "publishedDate": "2026-06-13"},
        {"title": "old", "url": "https://n/old", "publishedDate": "2026-05-01"},
        {"title": "undated", "url": "https://n/undated"},
    ]}}
    with patch("zoominfo_client.requests.post", return_value=_post_mock(payload)):
        result = zoominfo_client.discover_company_news(
            zoominfo_company_id=1, publishing_date_start="2026-06-12", page_size=5
        )
    urls = {c["url"] for c in result}
    assert "https://n/recent" in urls
    assert "https://n/undated" in urls   # undated kept (best-effort)
    assert "https://n/old" not in urls    # parseable + before cutoff -> dropped


def test_client_side_date_filter_keeps_all_when_cutoff_unparseable(monkeypatch):
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-token")
    payload = {"data": {"news": [
        {"title": "old", "url": "https://n/old", "publishedDate": "2020-01-01"},
    ]}}
    with patch("zoominfo_client.requests.post", return_value=_post_mock(payload)):
        result = zoominfo_client.discover_company_news(
            zoominfo_company_id=1, publishing_date_start="not-a-date", page_size=5
        )
    assert len(result) == 1  # no usable cutoff -> keep everything


def _bad_request_resp(text: str) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 400
    resp.text = text
    resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=resp)
    return resp


def test_bad_request_400_logs_sanitized_snippet_and_returns_empty(monkeypatch, caplog):
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-token")
    resp = _bad_request_resp('{"error":"invalid field foo"}')
    with patch("zoominfo_client.requests.post", return_value=resp), caplog.at_level("ERROR"):
        result = zoominfo_client.discover_company_news(
            zoominfo_company_id=357374413, publishing_date_start="2026-06-12", page_size=5
        )
    assert result == []
    assert "400" in caplog.text
    assert "invalid field foo" in caplog.text


def test_bad_request_400_snippet_capped_at_500(monkeypatch, caplog):
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-token")
    resp = _bad_request_resp("x" * 1000)
    with patch("zoominfo_client.requests.post", return_value=resp), caplog.at_level("ERROR"):
        zoominfo_client.discover_company_news(
            zoominfo_company_id=1, publishing_date_start="2026-06-12", page_size=5
        )
    assert "x" * 500 in caplog.text
    assert "x" * 501 not in caplog.text


def test_bad_request_400_does_not_log_token(monkeypatch, caplog):
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "secret-bearer-xyz")
    resp = _bad_request_resp("bad request body")
    with patch("zoominfo_client.requests.post", return_value=resp), caplog.at_level("DEBUG"):
        zoominfo_client.discover_company_news(
            zoominfo_company_id=1, publishing_date_start="2026-06-12", page_size=5
        )
    assert "secret-bearer-xyz" not in caplog.text


@pytest.mark.parametrize("status", [401, 403])
def test_discover_company_news_auth_error_returns_empty(monkeypatch, status):
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-token")
    resp = MagicMock()
    resp.status_code = status
    err = requests.exceptions.HTTPError(response=resp)
    resp.raise_for_status.side_effect = err
    with patch("zoominfo_client.requests.post", return_value=resp):
        result = zoominfo_client.discover_company_news(
            zoominfo_company_id=12345678,
            publishing_date_start="2026-06-12",
            page_size=5,
        )
    assert result == []


@pytest.mark.parametrize("status", [429, 500, 503])
def test_discover_company_news_retryable_error_returns_empty(monkeypatch, status):
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-token")
    resp = MagicMock()
    resp.status_code = status
    err = requests.exceptions.HTTPError(response=resp)
    resp.raise_for_status.side_effect = err
    with patch("zoominfo_client.requests.post", return_value=resp):
        result = zoominfo_client.discover_company_news(
            zoominfo_company_id=12345678,
            publishing_date_start="2026-06-12",
            page_size=5,
        )
    assert result == []


def test_discover_company_news_request_exception_returns_empty(monkeypatch):
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-token")
    with patch("zoominfo_client.requests.post",
               side_effect=requests.exceptions.Timeout("slow")):
        result = zoominfo_client.discover_company_news(
            zoominfo_company_id=12345678,
            publishing_date_start="2026-06-12",
            page_size=5,
        )
    assert result == []


# ===========================================================================
# OAuth Client Credentials auth (preferred path)
# ===========================================================================

def test_token_url_default_and_override(monkeypatch):
    monkeypatch.delenv("ZOOMINFO_TOKEN_URL", raising=False)
    assert zoominfo_client._token_url() == "https://api.zoominfo.com/gtm/oauth/v1/token"
    monkeypatch.setenv("ZOOMINFO_TOKEN_URL", "https://example.test/oauth/token")
    assert zoominfo_client._token_url() == "https://example.test/oauth/token"


def test_client_credentials_preferred_over_bearer(monkeypatch):
    """When both are configured, OAuth wins and the bearer token is ignored."""
    _set_client_creds(monkeypatch)
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "static-bearer")
    post, rec = _routed_post(_token_resp(access_token="oauth-token"), _post_mock({"data": {"news": []}}))
    with patch("zoominfo_client.requests.post", side_effect=post):
        zoominfo_client.discover_company_news(
            zoominfo_company_id=1, publishing_date_start="2026-06-12", page_size=5
        )
    assert rec["token_calls"] == 1
    assert rec["news_kwargs"]["headers"]["Authorization"] == "Bearer oauth-token"


def test_token_request_uses_http_basic_and_form_grant(monkeypatch):
    _set_client_creds(monkeypatch, "cid-123", "secret-xyz")
    post, rec = _routed_post(_token_resp(), _post_mock({"data": {"news": []}}))
    with patch("zoominfo_client.requests.post", side_effect=post):
        zoominfo_client.discover_company_news(
            zoominfo_company_id=1, publishing_date_start="2026-06-12", page_size=5
        )
    tk = rec["token_kwargs"]
    assert tk["auth"] == ("cid-123", "secret-xyz")
    assert tk["data"] == {"grant_type": "client_credentials"}
    assert tk["headers"]["Content-Type"] == "application/x-www-form-urlencoded"
    assert tk["headers"]["Accept"] == "application/json"


def test_access_token_used_for_news_request(monkeypatch):
    _set_client_creds(monkeypatch)
    payload = {"data": {"news": [{"title": "t", "url": "https://news.example.com/x"}]}}
    post, rec = _routed_post(_token_resp(access_token="abc123"), _post_mock(payload))
    with patch("zoominfo_client.requests.post", side_effect=post):
        result = zoominfo_client.discover_company_news(
            zoominfo_company_id=1, publishing_date_start="2026-06-12", page_size=5
        )
    assert rec["news_kwargs"]["headers"]["Authorization"] == "Bearer abc123"
    assert len(result) == 1


def test_token_cached_across_calls(monkeypatch):
    """One token call should cover multiple company lookups in a process."""
    _set_client_creds(monkeypatch)
    post, rec = _routed_post(_token_resp(expires_in=3600), _post_mock({"data": {"news": []}}))
    with patch("zoominfo_client.requests.post", side_effect=post):
        zoominfo_client.discover_company_news(
            zoominfo_company_id=1, publishing_date_start="2026-06-12", page_size=5
        )
        zoominfo_client.discover_company_news(
            zoominfo_company_id=2, publishing_date_start="2026-06-12", page_size=5
        )
    assert rec["token_calls"] == 1
    assert rec["news_calls"] == 2


@pytest.mark.parametrize("status", [401, 403])
def test_token_auth_failure_degrades(monkeypatch, status):
    _set_client_creds(monkeypatch)
    post, rec = _routed_post(_error_resp(status), _post_mock({"data": {"news": []}}))
    with patch("zoominfo_client.requests.post", side_effect=post):
        result = zoominfo_client.discover_company_news(
            zoominfo_company_id=1, publishing_date_start="2026-06-12", page_size=5
        )
    assert result == []
    assert rec["news_calls"] == 0  # never reached the news endpoint


@pytest.mark.parametrize("status", [429, 500, 503])
def test_token_retryable_failure_degrades(monkeypatch, status):
    _set_client_creds(monkeypatch)
    post, rec = _routed_post(_error_resp(status), _post_mock({"data": {"news": []}}))
    with patch("zoominfo_client.requests.post", side_effect=post):
        result = zoominfo_client.discover_company_news(
            zoominfo_company_id=1, publishing_date_start="2026-06-12", page_size=5
        )
    assert result == []
    assert rec["news_calls"] == 0


def test_token_transport_error_degrades(monkeypatch):
    _set_client_creds(monkeypatch)
    with patch("zoominfo_client.requests.post",
               side_effect=requests.exceptions.Timeout("slow")):
        result = zoominfo_client.discover_company_news(
            zoominfo_company_id=1, publishing_date_start="2026-06-12", page_size=5
        )
    assert result == []


def test_malformed_token_response_degrades(monkeypatch):
    _set_client_creds(monkeypatch)
    bad = MagicMock()
    bad.status_code = 200
    bad.raise_for_status = MagicMock()
    bad.json.return_value = {"no_access_token": "here"}
    post, rec = _routed_post(bad, _post_mock({"data": {"news": []}}))
    with patch("zoominfo_client.requests.post", side_effect=post):
        result = zoominfo_client.discover_company_news(
            zoominfo_company_id=1, publishing_date_start="2026-06-12", page_size=5
        )
    assert result == []
    assert rec["news_calls"] == 0


def test_client_secret_and_access_token_not_logged(monkeypatch, caplog):
    _set_client_creds(monkeypatch, "cid-123", "super-secret-value")
    post, _ = _routed_post(_token_resp(access_token="tok-do-not-log"), _post_mock({"data": {"news": []}}))
    with patch("zoominfo_client.requests.post", side_effect=post), caplog.at_level("DEBUG"):
        zoominfo_client.discover_company_news(
            zoominfo_company_id=1, publishing_date_start="2026-06-12", page_size=5
        )
    assert "super-secret-value" not in caplog.text
    assert "tok-do-not-log" not in caplog.text


def test_bearer_fallback_when_no_client_creds(monkeypatch):
    monkeypatch.delenv("ZOOMINFO_CLIENT_ID", raising=False)
    monkeypatch.delenv("ZOOMINFO_CLIENT_SECRET", raising=False)
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "static-bearer")
    payload = {"data": {"news": [{"title": "t", "url": "https://news.example.com/x"}]}}
    post, rec = _routed_post(_token_resp(), _post_mock(payload))
    with patch("zoominfo_client.requests.post", side_effect=post):
        result = zoominfo_client.discover_company_news(
            zoominfo_company_id=1, publishing_date_start="2026-06-12", page_size=5
        )
    assert rec["token_calls"] == 0  # no OAuth round-trip
    assert rec["news_kwargs"]["headers"]["Authorization"] == "Bearer static-bearer"
    assert len(result) == 1


def test_no_credentials_at_all_returns_empty(monkeypatch):
    monkeypatch.delenv("ZOOMINFO_CLIENT_ID", raising=False)
    monkeypatch.delenv("ZOOMINFO_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("ZOOMINFO_BEARER_TOKEN", raising=False)
    with patch("zoominfo_client.requests.post") as post:
        result = zoominfo_client.discover_company_news(
            zoominfo_company_id=1, publishing_date_start="2026-06-12", page_size=5
        )
    assert result == []
    post.assert_not_called()


# ===========================================================================
# discover_serper_candidates
# ===========================================================================

def test_discover_serper_candidates_shape(monkeypatch):
    monkeypatch.setattr(
        ingestion_engine, "discover_urls",
        lambda q, lb, n: [("https://news.com/a", "Serper Title")],
    )
    result = discover_serper_candidates(_zi_target())
    assert len(result) == 1
    c = result[0]
    assert c["url"] == "https://news.com/a"
    assert c["title"] == "Serper Title"
    assert c["provider"] == "serper"
    assert c["zoominfo_company_id"] is None
    assert c["categories"] == []


# ===========================================================================
# discover_zoominfo_candidates
# ===========================================================================

def test_zoominfo_candidates_empty_when_disabled(monkeypatch):
    """Flag off -> never call ZoomInfo, return []."""
    monkeypatch.setenv("ZOOMINFO_NEWS_ENABLED", "false")
    with patch("zoominfo_client.discover_company_news") as mock_call:
        result = discover_zoominfo_candidates(_zi_target())
    assert result == []
    mock_call.assert_not_called()


def test_zoominfo_candidates_empty_without_company_id(monkeypatch):
    monkeypatch.setenv("ZOOMINFO_NEWS_ENABLED", "true")
    target = _zi_target(company_id=None)
    with patch("zoominfo_client.discover_company_news") as mock_call:
        result = discover_zoominfo_candidates(target)
    assert result == []
    mock_call.assert_not_called()


def test_zoominfo_candidates_skipped_when_news_false(monkeypatch):
    monkeypatch.setenv("ZOOMINFO_NEWS_ENABLED", "true")
    target = _zi_target(zoominfo_news=False)
    with patch("zoominfo_client.discover_company_news") as mock_call:
        result = discover_zoominfo_candidates(target)
    assert result == []
    mock_call.assert_not_called()


def test_zoominfo_candidates_missing_token_no_crash(monkeypatch):
    """Enabled + mapped id but no token -> [] from the client, pipeline survives."""
    monkeypatch.setenv("ZOOMINFO_NEWS_ENABLED", "true")
    monkeypatch.delenv("ZOOMINFO_BEARER_TOKEN", raising=False)
    result = discover_zoominfo_candidates(_zi_target())
    assert result == []


def test_zoominfo_candidates_enabled_returns_candidates(monkeypatch):
    monkeypatch.setenv("ZOOMINFO_NEWS_ENABLED", "true")
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-token")
    fake_candidates = [{
        "url": "https://news.example.com/x", "title": "ZI headline",
        "provider": "zoominfo", "source_publication": "Reuters",
        "published_at": "2026-06-13", "description": "d",
        "categories": ["PRODUCT"], "zoominfo_company_id": 12345678, "raw": {},
    }]
    with patch("zoominfo_client.discover_company_news", return_value=fake_candidates) as mock_call:
        result = discover_zoominfo_candidates(_zi_target())
    assert result == fake_candidates
    assert mock_call.call_count == 1
    _, kwargs = mock_call.call_args
    assert kwargs["zoominfo_company_id"] == 12345678
    assert kwargs["page_size"] == 5  # default ZOOMINFO_NEWS_PER_COMPANY


# ===========================================================================
# discover_candidates — merge + failure isolation
# ===========================================================================

def test_discover_candidates_merges_both_providers(monkeypatch):
    serper = [{"url": "https://news.com/s", "provider": "serper"}]
    zoominfo = [{"url": "https://news.com/z", "provider": "zoominfo"}]
    monkeypatch.setattr(ingestion_engine, "discover_serper_candidates", lambda t: serper)
    monkeypatch.setattr(ingestion_engine, "discover_zoominfo_candidates", lambda t: zoominfo)
    result = discover_candidates(_zi_target())
    providers = {c["provider"] for c in result}
    assert providers == {"serper", "zoominfo"}
    assert len(result) == 2


def test_discover_candidates_serper_failure_does_not_suppress_zoominfo(monkeypatch):
    def boom(_target):
        raise RuntimeError("serper down")
    zoominfo = [{"url": "https://news.com/z", "provider": "zoominfo"}]
    monkeypatch.setattr(ingestion_engine, "discover_serper_candidates", boom)
    monkeypatch.setattr(ingestion_engine, "discover_zoominfo_candidates", lambda t: zoominfo)
    result = discover_candidates(_zi_target())
    assert result == zoominfo


def test_discover_candidates_zoominfo_failure_does_not_suppress_serper(monkeypatch):
    def boom(_target):
        raise RuntimeError("zoominfo down")
    serper = [{"url": "https://news.com/s", "provider": "serper"}]
    monkeypatch.setattr(ingestion_engine, "discover_serper_candidates", lambda t: serper)
    monkeypatch.setattr(ingestion_engine, "discover_zoominfo_candidates", boom)
    result = discover_candidates(_zi_target())
    assert result == serper


def test_cross_provider_url_dedupe_hash_matches():
    """A Serper (tracking-polluted) and ZoomInfo (clean) URL for the same article
    must hash identically so the existing dedupe gate catches the duplicate."""
    serper_url = "https://news.com/article?utm_source=serp&utm_medium=news"
    zoominfo_url = "https://news.com/article"
    assert (
        compute_url_hash(normalize_url(serper_url))
        == compute_url_hash(normalize_url(zoominfo_url))
    )


# ===========================================================================
# Discovery-metadata payload helper + gating
# ===========================================================================

def test_store_discovery_metadata_default_off(monkeypatch):
    monkeypatch.delenv("STORE_DISCOVERY_METADATA", raising=False)
    assert ingestion_engine._store_discovery_metadata() is False


def test_store_discovery_metadata_truthy(monkeypatch):
    monkeypatch.setenv("STORE_DISCOVERY_METADATA", "true")
    assert ingestion_engine._store_discovery_metadata() is True


def test_discovery_metadata_shape_for_zoominfo():
    candidate = {
        "url": "https://news.example.com/x", "title": "t", "provider": "zoominfo",
        "source_publication": "Reuters", "published_at": "2026-06-13",
        "description": "d", "categories": ["PRODUCT"],
        "zoominfo_company_id": 12345678, "raw": {},
    }
    meta = ingestion_engine._discovery_metadata(candidate)
    assert meta["discovery_source"] == "zoominfo"
    assert meta["external_company_id"] == "12345678"
    assert meta["published_at"] == "2026-06-13"
    assert meta["source_metadata"] == {
        "provider": "zoominfo",
        "source_publication": "Reuters",
        "description": "d",
        "categories": ["PRODUCT"],
    }


def test_discovery_metadata_serper_has_empty_company_id():
    candidate = ingestion_engine._serper_candidate("https://news.com/a", "Title")
    meta = ingestion_engine._discovery_metadata(candidate)
    assert meta["discovery_source"] == "serper"
    assert meta["external_company_id"] == ""
    assert meta["published_at"] is None


# ===========================================================================
# execute_pipeline integration — candidates flow through, metadata gating
# ===========================================================================

def _stub_pipeline_internals(monkeypatch, tmp_path, candidate, captured):
    """Wire up execute_pipeline so exactly one candidate reaches store_insight."""
    cfg = tmp_path / "targets.yaml"
    cfg.write_text(_entity_yaml())
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr(ingestion_engine, "discover_candidates", lambda target: [candidate])
    monkeypatch.setattr(ingestion_engine, "_hydrate_seen_headlines", lambda: set())
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h: False)
    monkeypatch.setattr(ingestion_engine, "scrape_article", lambda url, mn: "Article body text.")
    monkeypatch.setattr(ingestion_engine.time, "sleep", lambda s: None)
    monkeypatch.setattr(ingestion_engine, "generate_macro_summary", lambda *a, **k: True)
    monkeypatch.setattr(
        ingestion_engine, "synthesize_insight",
        lambda *a, **k: {
            "headline": "Stored headline",
            "americhem_impact": "Impact.",
            "sentiment_score": 5,
            "source_url": candidate["url"],
            "entities_mentioned": ["Magna"],
            "americhem_impact_score": 7,
            "sentiment_tag": "Neutral",
        },
    )
    monkeypatch.setattr(ingestion_engine, "store_insight", lambda payload: captured.append(payload))


def _zi_candidate() -> dict:
    return {
        "url": "https://news.example.com/magna-news", "title": "Magna headline",
        "provider": "zoominfo", "source_publication": "Reuters",
        "published_at": "2026-06-13", "description": "desc",
        "categories": ["FINANCIAL_RESULTS"], "zoominfo_company_id": 12345678, "raw": {},
    }


def test_execute_pipeline_stores_candidate_with_metadata_when_enabled(monkeypatch, tmp_path):
    monkeypatch.setenv("STORE_DISCOVERY_METADATA", "true")
    captured: list[dict] = []
    _stub_pipeline_internals(monkeypatch, tmp_path, _zi_candidate(), captured)

    ingestion_engine.execute_pipeline()

    assert len(captured) == 1
    payload = captured[0]
    assert payload["discovery_source"] == "zoominfo"
    assert payload["external_company_id"] == "12345678"
    assert payload["published_at"] == "2026-06-13"
    assert payload["source_metadata"]["categories"] == ["FINANCIAL_RESULTS"]


def test_execute_pipeline_omits_metadata_when_disabled(monkeypatch, tmp_path):
    monkeypatch.delenv("STORE_DISCOVERY_METADATA", raising=False)
    captured: list[dict] = []
    _stub_pipeline_internals(monkeypatch, tmp_path, _zi_candidate(), captured)

    ingestion_engine.execute_pipeline()

    assert len(captured) == 1
    payload = captured[0]
    # Backwards-compatible: no new columns until the migration is applied.
    assert "discovery_source" not in payload
    assert "external_company_id" not in payload
    assert "source_metadata" not in payload
    # Core fields still present.
    assert payload["headline"] == "Stored headline"
