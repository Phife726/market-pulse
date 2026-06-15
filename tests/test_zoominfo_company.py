"""Tests for the ZoomInfo company resolve/enrich functions used by the
target-metadata enrichment utility. No live API calls — requests.post is
always mocked."""
import logging
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


def _err(status: int, body: str = "") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.text = body
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


# ── Verified request shape (live 2026-06-14, company_id 357374413 / Avient) ────
# Probe result: matchCompanyInput[] + outputFields produced a 200; the singular
# companyId and plural companyIds shapes returned 400. These tests lock the
# verified body so a regression can't silently revert to the rejected shape.

def test_enrich_company_sends_matchcompanyinput_identifier():
    payload = {"data": [{"attributes": {"name": "Avient Corporation"}}]}
    with patch("zoominfo_client.requests.post", return_value=_ok(payload)) as m:
        zoominfo_client.enrich_company(357374413)
    attrs = m.call_args.kwargs["json"]["data"]["attributes"]
    assert m.call_args.kwargs["json"]["data"]["type"] == "CompanyEnrich"
    assert attrs["matchCompanyInput"] == [{"companyId": 357374413}]


def test_enrich_company_no_longer_sends_rejected_shapes():
    payload = {"data": [{"attributes": {"name": "Avient Corporation"}}]}
    with patch("zoominfo_client.requests.post", return_value=_ok(payload)) as m:
        zoominfo_client.enrich_company(357374413)
    attrs = m.call_args.kwargs["json"]["data"]["attributes"]
    # The two live-rejected (400) identifier shapes must be gone.
    assert "companyId" not in attrs
    assert "companyIds" not in attrs


def test_enrich_company_requests_verified_output_fields():
    payload = {"data": [{"attributes": {"name": "Avient Corporation"}}]}
    with patch("zoominfo_client.requests.post", return_value=_ok(payload)) as m:
        zoominfo_client.enrich_company(357374413)
    attrs = m.call_args.kwargs["json"]["data"]["attributes"]
    assert attrs["outputFields"] == [
        "name", "revenue", "employeeCount", "primaryIndustry",
        "industries", "country", "state",
    ]


def test_enrich_company_parses_first_item_attributes_from_data_list():
    # The verified 200 shape: top-level data is a list of JSON:API resource
    # objects; firmographics live under the first item's attributes.
    payload = {"data": [{
        "type": "CompanyEnrich", "id": "357374413", "meta": {},
        "attributes": {"name": "Avient Corporation", "revenue": 3600000,
                       "industries": ["Plastics & Rubber Manufacturing"]},
    }]}
    with patch("zoominfo_client.requests.post", return_value=_ok(payload)):
        result = zoominfo_client.enrich_company(357374413)
    assert result["status"] == "ok"
    assert result["company"]["name"] == "Avient Corporation"
    assert result["company"]["revenue"] == 3600000


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


# ── Diagnostics: sanitized 400 snippet + keys-only structural logging ──────────
# These prove the schema-confirmation machinery is safe (no secrets/values
# leaked) and degrades to "error" — they do NOT assert any outputFields tokens,
# which stay unverified until an entitled live run confirms them.

def test_enrich_company_400_is_error_and_logs_sanitized_snippet(caplog):
    body = '{"error":"Invalid field requested: outputFields is required"}'
    with caplog.at_level(logging.ERROR, logger="zoominfo_client"):
        with patch("zoominfo_client.requests.post", return_value=_err(400, body)):
            result = zoominfo_client.enrich_company(357374413)
    assert result == {"status": "error"}  # 400 degrades, never raises
    text = caplog.text
    assert "400" in text
    assert "outputFields is required" in text  # response body pointer surfaced


def test_resolve_company_400_is_error_and_logs_sanitized_snippet(caplog):
    body = '{"error":"Invalid search criteria"}'
    with caplog.at_level(logging.ERROR, logger="zoominfo_client"):
        with patch("zoominfo_client.requests.post", return_value=_err(400, body)):
            result = zoominfo_client.resolve_company(name="Avient")
    assert result == {"status": "error"}
    assert "400" in caplog.text
    assert "Invalid search criteria" in caplog.text


def test_enrich_company_400_snippet_capped_at_500_chars(caplog):
    body = "x" * 5000
    with caplog.at_level(logging.ERROR, logger="zoominfo_client"):
        with patch("zoominfo_client.requests.post", return_value=_err(400, body)):
            zoominfo_client.enrich_company(357374413)
    # The raw 5000-char body must never appear in full; the snippet is capped.
    assert ("x" * 5000) not in caplog.text
    assert ("x" * 500) in caplog.text


def test_enrich_company_structural_log_is_keys_only_no_values(caplog):
    payload = {"data": [{"attributes": {"name": "SENTINEL_VALUE",
                                        "revenueRange": "$1B - $5B"}}]}
    with caplog.at_level(logging.INFO, logger="zoominfo_client"):
        with patch("zoominfo_client.requests.post", return_value=_ok(payload)):
            zoominfo_client.enrich_company(357374413)
    text = caplog.text
    # Field NAMES are surfaced so the next entitled run can confirm the schema...
    assert "name" in text and "revenueRange" in text
    # ...but no firmographic VALUES (or tokens/secrets) are ever logged.
    assert "SENTINEL_VALUE" not in text
    assert "$1B - $5B" not in text


def test_resolve_company_structural_log_is_keys_only_no_values(caplog):
    payload = {"data": [{"id": 357374413, "name": "SENTINEL_VALUE"}]}
    with caplog.at_level(logging.INFO, logger="zoominfo_client"):
        with patch("zoominfo_client.requests.post", return_value=_ok(payload)):
            zoominfo_client.resolve_company(name="Avient")
    assert "SENTINEL_VALUE" not in caplog.text


def test_diagnostics_never_log_the_bearer_token(caplog):
    # The autouse fixture sets ZOOMINFO_BEARER_TOKEN="test-bearer". No code path
    # (200 structural log or 400 snippet) may ever surface it.
    with caplog.at_level(logging.DEBUG, logger="zoominfo_client"):
        with patch("zoominfo_client.requests.post",
                   return_value=_err(400, '{"error":"bad"}')):
            zoominfo_client.enrich_company(357374413)
        with patch("zoominfo_client.requests.post",
                   return_value=_ok({"data": [{"attributes": {"name": "X"}}]})):
            zoominfo_client.enrich_company(357374413)
    assert "test-bearer" not in caplog.text
