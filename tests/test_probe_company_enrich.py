"""Tests for scripts/probe_company_enrich.py — the live Company Enrich request
-schema probe. No live API calls: requests.post is always mocked. The probe is a
read-only diagnostic; it must never write files, never raise, and never log the
bearer token or firmographic values."""
import logging
from unittest.mock import MagicMock, patch

import pytest

import probe_company_enrich as probe
import zoominfo_client


@pytest.fixture(autouse=True)
def _bearer(monkeypatch):
    zoominfo_client._reset_token_cache()
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-bearer")
    monkeypatch.delenv("ZOOMINFO_CLIENT_ID", raising=False)
    monkeypatch.delenv("ZOOMINFO_CLIENT_SECRET", raising=False)
    yield
    zoominfo_client._reset_token_cache()


def _resp(status: int, json_payload=None, text: str = "") -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.text = text
    if json_payload is None:
        r.json.side_effect = ValueError("no json")
    else:
        r.json.return_value = json_payload
    return r


# The exact 400 body the live Avient run returned.
_AVIENT_400 = {
    "detail": "There is invalid field(s) in the request",
    "errors": [{
        "code": "PFAPI0005", "detail": "Invalid field requested",
        "id": "25723600-2ccf-42bb-9f09-fb81e346c218",
        "source": {"pointer": "/data/attributes/companyId"}, "status": "400",
    }],
    "title": "Invalid request body",
}


def _400(pointer: str, code: str = "PFAPI0005") -> MagicMock:
    """A 400 whose error names a specific JSON:API pointer."""
    return _resp(400, {"errors": [{"code": code, "source": {"pointer": pointer}}]})


def _200_populated() -> MagicMock:
    return _resp(200, {"data": [{"attributes": {
        "name": "Avient Corporation", "primaryIndustry": "Plastics",
        "industries": ["Plastics"]}}]})


def _200_sparse() -> MagicMock:
    return _resp(200, {"data": [{"attributes": {}}]})


def test_build_candidates_covers_doc_plausible_input_shapes():
    cands = probe.build_candidates(357374413)
    labels = [label for label, _ in cands]
    # Baseline (known-bad) must be included so the probe reproduces the pointer.
    assert any("companyId" == label.split()[0] for label in labels)
    bodies = {label: body for label, body in cands}
    # Every candidate is a JSON:API CompanyEnrich body.
    for body in bodies.values():
        assert body["data"]["type"] == "CompanyEnrich"
    attrs = [body["data"]["attributes"] for body in bodies.values()]
    # The array-shaped inputs the docs imply ("Company IDs", "Multiple Inputs").
    assert any(a.get("companyIds") == [357374413] for a in attrs)
    assert any(a.get("matchCompanyInput") == [{"companyId": 357374413}] for a in attrs)
    # At least one candidate also exercises outputFields.
    assert any("outputFields" in a for a in attrs)


def test_probe_one_400_captures_pointer_and_does_not_raise(caplog):
    body = {"data": {"type": "CompanyEnrich", "attributes": {"companyId": 1}}}
    with caplog.at_level(logging.INFO, logger="probe_company_enrich"):
        with patch("probe_company_enrich.requests.post",
                   return_value=_resp(400, _AVIENT_400)):
            result = probe._probe_one("companyId", body, {"Authorization": "Bearer x"}, "http://e")
    assert result["status"] == 400
    assert "/data/attributes/companyId" in result["pointers"]
    assert "PFAPI0005" in result.get("codes", [])
    assert "/data/attributes/companyId" in caplog.text


def test_probe_one_200_populated_logs_keys_only_no_values(caplog):
    payload = {"data": [{"attributes": {
        "name": "SENTINEL_NAME", "primaryIndustry": "SENTINEL_INDUSTRY",
        "industries": ["SENTINEL_LIST"],
    }}]}
    body = {"data": {"type": "CompanyEnrich", "attributes": {"companyIds": [1]}}}
    with caplog.at_level(logging.INFO, logger="probe_company_enrich"):
        with patch("probe_company_enrich.requests.post", return_value=_resp(200, payload)):
            result = probe._probe_one("companyIds[]", body, {"Authorization": "Bearer x"}, "http://e")
    assert result["status"] == 200
    assert result["populated"] == {
        "canonical_name": True, "primary_industry": True, "industries": True}
    # Field NAMES surface for schema confirmation; VALUES never do.
    assert "name" in caplog.text and "primaryIndustry" in caplog.text
    assert "SENTINEL_NAME" not in caplog.text
    assert "SENTINEL_INDUSTRY" not in caplog.text
    assert "SENTINEL_LIST" not in caplog.text


def test_probe_one_200_sparse_marks_all_unpopulated():
    payload = {"data": [{"attributes": {}}]}
    body = {"data": {"type": "CompanyEnrich", "attributes": {"companyIds": [1]}}}
    with patch("probe_company_enrich.requests.post", return_value=_resp(200, payload)):
        result = probe._probe_one("companyIds[]", body, {"Authorization": "Bearer x"}, "http://e")
    assert result["status"] == 200
    assert result["populated"] == {
        "canonical_name": False, "primary_industry": False, "industries": False}


def test_probe_one_transport_error_does_not_raise():
    import requests as _rq
    body = {"data": {"type": "CompanyEnrich", "attributes": {"companyIds": [1]}}}
    with patch("probe_company_enrich.requests.post",
               side_effect=_rq.exceptions.ConnectionError("boom")):
        result = probe._probe_one("companyIds[]", body, {"Authorization": "Bearer x"}, "http://e")
    assert result["status"] is None
    assert result.get("error")  # recorded, not raised


def test_run_probe_returns_empty_without_token(monkeypatch):
    monkeypatch.setattr(zoominfo_client, "_resolve_access_token", lambda: None)
    assert probe.run_probe(357374413) == []


def test_run_probe_stops_after_first_populated_200(caplog):
    # baseline 400 (rejected) -> companyIds[] 200 populated -> STOP. No 3rd call,
    # so only one credit-charging request is sent.
    posts = [_400("/data/attributes/companyId"), _200_populated()]
    with patch("probe_company_enrich.requests.post", side_effect=posts) as m:
        results = probe.run_probe(357374413)
    assert m.call_count == 2
    assert len(results) == 2
    assert results[-1]["status"] == 200
    assert any(results[-1]["populated"].values())


def test_run_probe_sparse_200_triggers_exactly_one_outputfields_probe():
    # baseline 400 -> companyIds[] 200 sparse -> ONE outputFields probe (200).
    posts = [_400("/data/attributes/companyId"), _200_sparse(), _200_populated()]
    with patch("probe_company_enrich.requests.post", side_effect=posts) as m:
        results = probe.run_probe(357374413)
    assert m.call_count == 3
    assert results[-1]["label"] == "accepted input + outputFields"
    assert results[-1]["status"] == 200


def test_run_probe_400_outputfields_required_triggers_outputfields_probe():
    # baseline 400 names companyId (rejected); companyIds[] 400 names outputFields
    # (input accepted, outputFields required) -> ONE outputFields probe.
    posts = [_400("/data/attributes/companyId"),
             _400("/data/attributes/outputFields"), _200_populated()]
    with patch("probe_company_enrich.requests.post", side_effect=posts) as m:
        results = probe.run_probe(357374413)
    assert m.call_count == 3
    assert results[-1]["label"] == "accepted input + outputFields"


def test_run_probe_all_inputs_rejected_sends_no_outputfields_probe():
    # Every input shape 400s naming its own key -> no input accepted, no credit
    # spent (all 400), and no outputFields probe.
    posts = [_400("/data/attributes/companyId"),
             _400("/data/attributes/companyIds"),
             _400("/data/attributes/matchCompanyInput")]
    with patch("probe_company_enrich.requests.post", side_effect=posts) as m:
        results = probe.run_probe(357374413)
    assert m.call_count == 3
    assert all(r["status"] == 400 for r in results)
    assert not any(r.get("status") == 200 for r in results)


def test_run_probe_all_flag_sends_full_matrix():
    with patch("probe_company_enrich.requests.post",
               return_value=_400("/data/attributes/companyId")) as m:
        results = probe.run_probe(357374413, probe_all=True)
    assert m.call_count == len(probe.build_candidates(357374413))
    assert len(results) == len(probe.build_candidates(357374413))


def test_probe_never_logs_bearer_token(caplog):
    with caplog.at_level(logging.DEBUG, logger="probe_company_enrich"):
        with patch("probe_company_enrich.requests.post",
                   side_effect=[_400("/data/attributes/companyId"), _200_populated()]):
            probe.run_probe(357374413)
    assert "test-bearer" not in caplog.text
