"""Tests for the link-reputation seam (link_reputation.py).

The transport contract — the Google Safe Browsing Lookup API v4 endpoint,
the key from SAFE_BROWSING_API_KEY, the request body, the 500-URL batching,
the in-run cache, and the tolerance policy (every failure degrades to "no
check" with a warning; nothing ever raises) — lives here, at the adapter.
Consumer tests inject ``FakeLinkReputation`` and assert on what crossed the
seam (see test_ingestion_engine.py / test_delivery_engine.py).
"""
import logging

import pytest
import requests

import link_reputation
from tests.conftest import stub_http_response
from link_reputation import (
    MAX_URLS_PER_REQUEST,
    SAFE_BROWSING_URL,
    THREAT_TYPES,
    FakeLinkReputation,
    SafeBrowsingLinkReputation,
    _link_reputation,
    _reset_link_reputation,
)

KEY = "AIza-test-key-0123"
SAFE = "https://www.plasticsnews.com/story"
BAD = "https://chargedevs.com/newswire/x/"


def _match(url: str, threat: str = "MALWARE") -> dict:
    return {"threatType": threat, "platformType": "ANY_PLATFORM",
            "threatEntryType": "URL", "threat": {"url": url}}


def _post_returning(monkeypatch, *responses) -> list:
    """Stub link_reputation.requests.post to serve `responses` in order
    (repeating the last); returns the (url, kwargs) list it was called with.
    A response that is an Exception instance is raised instead."""
    calls: list = []
    queue = list(responses)

    def fake_post(url, **kwargs):
        calls.append((url, kwargs))
        resp = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(resp, Exception):
            raise resp
        return resp

    monkeypatch.setattr(link_reputation.requests, "post", fake_post)
    return calls


@pytest.fixture
def client(monkeypatch) -> SafeBrowsingLinkReputation:
    monkeypatch.setenv("SAFE_BROWSING_API_KEY", KEY)
    return SafeBrowsingLinkReputation()


# ---------------------------------------------------------------------------
# Request shape
# ---------------------------------------------------------------------------

def test_posts_the_lookup_request_with_the_key_as_a_query_parameter(client, monkeypatch):
    calls = _post_returning(monkeypatch, stub_http_response(200, json={}))

    assert client.unsafe([SAFE, BAD]) == frozenset()

    (url, kwargs), = calls
    assert url == SAFE_BROWSING_URL
    assert kwargs["params"] == {"key": KEY}
    body = kwargs["json"]
    assert body["client"]["clientId"]
    info = body["threatInfo"]
    assert set(info["threatTypes"]) == set(THREAT_TYPES)
    assert info["platformTypes"] == ["ANY_PLATFORM"]
    assert info["threatEntryTypes"] == ["URL"]
    assert info["threatEntries"] == [{"url": SAFE}, {"url": BAD}]
    assert kwargs["timeout"]


def test_returns_exactly_the_urls_the_api_matched(client, monkeypatch):
    _post_returning(monkeypatch, stub_http_response(200, json={"matches": [_match(BAD)]}))
    assert client.unsafe([SAFE, BAD]) == frozenset({BAD})


def test_a_match_for_a_url_not_asked_about_is_ignored(client, monkeypatch):
    _post_returning(monkeypatch, stub_http_response(
        200, json={"matches": [_match("https://elsewhere.example/")]}))
    assert client.unsafe([SAFE]) == frozenset()


def test_empty_input_makes_no_request(client, monkeypatch):
    calls = _post_returning(monkeypatch, stub_http_response(200, json={}))
    assert client.unsafe([]) == frozenset()
    assert client.unsafe(["", None]) == frozenset()
    assert calls == []


# ---------------------------------------------------------------------------
# Batching and the in-run cache
# ---------------------------------------------------------------------------

def test_batches_urls_in_chunks_of_the_api_maximum(client, monkeypatch):
    urls = [f"https://example.com/{i}" for i in range(MAX_URLS_PER_REQUEST + 1)]
    calls = _post_returning(monkeypatch, stub_http_response(200, json={"matches": [_match(urls[-1])]}))

    assert client.unsafe(urls) == frozenset({urls[-1]})

    sizes = [len(kwargs["json"]["threatInfo"]["threatEntries"]) for _, kwargs in calls]
    assert sizes == [MAX_URLS_PER_REQUEST, 1]


def test_a_url_already_checked_this_run_is_not_sent_again(client, monkeypatch):
    calls = _post_returning(monkeypatch, stub_http_response(200, json={"matches": [_match(BAD)]}))

    assert client.unsafe([SAFE, BAD]) == frozenset({BAD})
    assert client.unsafe([BAD, "https://new.example/"]) == frozenset({BAD})

    sent = [[e["url"] for e in kwargs["json"]["threatInfo"]["threatEntries"]] for _, kwargs in calls]
    assert sent == [[SAFE, BAD], ["https://new.example/"]]


def test_duplicate_urls_in_one_call_are_sent_once(client, monkeypatch):
    calls = _post_returning(monkeypatch, stub_http_response(200, json={}))
    client.unsafe([SAFE, SAFE])
    assert calls[0][1]["json"]["threatInfo"]["threatEntries"] == [{"url": SAFE}]


# ---------------------------------------------------------------------------
# Tolerance: every failure is "no check", logged, never raised
# ---------------------------------------------------------------------------

def test_no_key_means_no_check_and_no_request(monkeypatch, caplog):
    monkeypatch.delenv("SAFE_BROWSING_API_KEY", raising=False)
    calls = _post_returning(monkeypatch, stub_http_response(200, json={"matches": [_match(BAD)]}))
    with caplog.at_level(logging.INFO):
        assert SafeBrowsingLinkReputation().unsafe([BAD]) == frozenset()
    assert calls == []
    assert "SAFE_BROWSING_API_KEY" in caplog.text


def test_blank_key_means_no_check(monkeypatch):
    monkeypatch.setenv("SAFE_BROWSING_API_KEY", "   ")
    calls = _post_returning(monkeypatch, stub_http_response(200, json={}))
    assert SafeBrowsingLinkReputation().unsafe([BAD]) == frozenset()
    assert calls == []


def _not_json_response():
    resp = stub_http_response(200, text="<html>not json</html>")
    resp.json.side_effect = ValueError("no json")
    return resp


@pytest.mark.parametrize("failure", [
    stub_http_response(403, text="quota"),
    stub_http_response(429),
    stub_http_response(500),
    requests.ConnectionError("down"),
    requests.Timeout("slow"),
    _not_json_response(),
    stub_http_response(200, json={"matches": "garbage"}),
    stub_http_response(200, json={"matches": [{"threat": "no url dict"}]}),
], ids=["403", "429", "500", "connection", "timeout", "not-json", "matches-not-a-list", "match-shape"])
def test_any_failure_degrades_to_no_check_with_a_warning(client, monkeypatch, caplog, failure):
    _post_returning(monkeypatch, failure)
    with caplog.at_level(logging.WARNING):
        assert client.unsafe([BAD]) == frozenset()
    assert "Safe Browsing" in caplog.text
    assert any(r.levelno >= logging.WARNING for r in caplog.records)


def test_after_a_failure_the_check_is_off_for_the_rest_of_the_run(client, monkeypatch, caplog):
    """A dead API must not cost one timeout per target for the whole run
    (the pipeline deadline is 30 min): the first failure disables the
    check for this adapter instance, and later calls make no request."""
    calls = _post_returning(monkeypatch, requests.Timeout("slow"))
    with caplog.at_level(logging.WARNING):
        client.unsafe([BAD])
        client.unsafe([SAFE])
        client.unsafe([BAD])
    assert len(calls) == 1
    assert sum("Safe Browsing" in r.getMessage() for r in caplog.records
               if r.levelno >= logging.WARNING) == 1


def test_the_api_key_is_never_logged(client, monkeypatch, caplog):
    _post_returning(monkeypatch, stub_http_response(403, text=f"denied key={KEY}"))
    with caplog.at_level(logging.DEBUG):
        client.unsafe([BAD])
    assert KEY not in caplog.text


# ---------------------------------------------------------------------------
# The fake and the singleton
# ---------------------------------------------------------------------------

def test_fake_returns_the_scripted_unsafe_subset_and_records_calls():
    fake = FakeLinkReputation(unsafe_urls={BAD})
    assert fake.unsafe([SAFE, BAD]) == frozenset({BAD})
    assert fake.unsafe([SAFE]) == frozenset()
    assert fake.calls == [[SAFE, BAD], [SAFE]]


def test_singleton_is_the_production_adapter_and_resets():
    _reset_link_reputation()
    first = _link_reputation()
    assert isinstance(first, SafeBrowsingLinkReputation)
    assert _link_reputation() is first
    _reset_link_reputation()
    assert _link_reputation() is not first
    _reset_link_reputation()
