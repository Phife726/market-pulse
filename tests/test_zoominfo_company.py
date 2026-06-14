"""Tests for the ZoomInfo company resolve/enrich functions used by the
target-metadata enrichment utility. No live API calls — requests.post is
always mocked."""
from unittest.mock import MagicMock, patch

import pytest
import requests

import zoominfo_client


def _ok(json_payload: dict) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = json_payload
    resp.raise_for_status = MagicMock()
    return resp


def _err(status: int) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=resp)
    return resp


@pytest.fixture(autouse=True)
def _bearer(monkeypatch):
    # Static bearer path keeps these tests off the OAuth exchange.
    zoominfo_client._reset_token_cache()
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-bearer")
    monkeypatch.delenv("ZOOMINFO_CLIENT_ID", raising=False)
    monkeypatch.delenv("ZOOMINFO_CLIENT_SECRET", raising=False)
    yield
    zoominfo_client._reset_token_cache()


def test_resolve_company_returns_id_on_match():
    payload = {"data": [{"id": 357374413, "name": "Avient Corporation"}]}
    with patch("zoominfo_client.requests.post", return_value=_ok(payload)):
        result = zoominfo_client.resolve_company(domain="avient.com")
    assert result == {"status": "ok", "company_id": 357374413}


def test_resolve_company_empty_when_no_candidates():
    with patch("zoominfo_client.requests.post", return_value=_ok({"data": []})):
        result = zoominfo_client.resolve_company(name="No Such Co")
    assert result == {"status": "empty"}


def test_resolve_company_error_on_403():
    with patch("zoominfo_client.requests.post", return_value=_err(403)):
        result = zoominfo_client.resolve_company(name="Avient")
    assert result == {"status": "error"}


def test_resolve_company_error_on_transport_failure():
    with patch(
        "zoominfo_client.requests.post",
        side_effect=requests.exceptions.ConnectionError("boom"),
    ):
        result = zoominfo_client.resolve_company(name="Avient")
    assert result == {"status": "error"}


def test_resolve_company_error_on_malformed_json():
    resp = _ok({})
    resp.json.side_effect = ValueError("no json")
    with patch("zoominfo_client.requests.post", return_value=resp):
        result = zoominfo_client.resolve_company(name="Avient")
    assert result == {"status": "error"}


def test_enrich_company_returns_raw_company_on_success():
    payload = {"data": [{"attributes": {"name": "Avient Corporation",
                                        "revenueRange": "$1B - $5B"}}]}
    with patch("zoominfo_client.requests.post", return_value=_ok(payload)):
        result = zoominfo_client.enrich_company(357374413)
    assert result["status"] == "ok"
    assert result["company"]["name"] == "Avient Corporation"
    assert result["company"]["revenueRange"] == "$1B - $5B"


def test_enrich_company_empty_when_no_company():
    with patch("zoominfo_client.requests.post", return_value=_ok({"data": []})):
        result = zoominfo_client.enrich_company(999)
    assert result == {"status": "empty"}


def test_enrich_company_error_on_403():
    with patch("zoominfo_client.requests.post", return_value=_err(403)):
        result = zoominfo_client.enrich_company(357374413)
    assert result == {"status": "error"}


def test_enrich_company_error_on_transport_failure():
    with patch(
        "zoominfo_client.requests.post",
        side_effect=requests.exceptions.ConnectionError("boom"),
    ):
        result = zoominfo_client.enrich_company(357374413)
    assert result == {"status": "error"}


def test_enrich_company_error_on_malformed_json():
    resp = _ok({})
    resp.json.side_effect = ValueError("no json")
    with patch("zoominfo_client.requests.post", return_value=resp):
        result = zoominfo_client.enrich_company(357374413)
    assert result == {"status": "error"}
