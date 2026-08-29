"""Tests for the mailer seam (mailer.py).

The transport contract — Resend endpoint, bearer auth from SMTP_PASS, payload
shape, and the retry policy (transient HTTP codes retry with backoff; any
other failure propagates at once) — lives here, at the adapter, instead of
being re-asserted inside every caller. Consumer tests inject ``FakeMailer``
and assert on the ``EmailMessage`` that crossed the seam (see test_delivery_engine.py).
"""
from dataclasses import FrozenInstanceError

import pytest
import requests

import mailer
from tests.conftest import stub_http_response
from mailer import (
    BASE_DELAY_S,
    MAX_ATTEMPTS,
    RESEND_API_URL,
    TRANSIENT_HTTP_CODES,
    EmailMessage,
    FakeMailer,
    ResendMailer,
    _mailer,
    _reset_mailer,
)

MESSAGE = EmailMessage(
    sender="noreply@test.com",
    recipients=("a@test.com", "b@test.com"),
    subject="Americhem Market-Pulse — August 27, 2026",
    html="<html>digest</html>",
)


def _post_returning(monkeypatch, *responses) -> list:
    """Stub mailer.requests.post to serve `responses` in order (repeating the
    last one); returns the list of (url, kwargs) it was called with."""
    calls: list = []
    queue = list(responses)

    def fake_post(url, **kwargs):
        calls.append((url, kwargs))
        return queue.pop(0) if len(queue) > 1 else queue[0]

    monkeypatch.setattr(mailer.requests, "post", fake_post)
    return calls


@pytest.fixture
def resend(monkeypatch) -> ResendMailer:
    """A production adapter with its API key set and its backoff sleep silenced."""
    monkeypatch.setenv("SMTP_PASS", "re_test_key")
    return ResendMailer(sleep=lambda s: None)


# ---------------------------------------------------------------------------
# EmailMessage — the value that crosses the seam
# ---------------------------------------------------------------------------

def test_email_message_is_frozen():
    with pytest.raises(FrozenInstanceError):
        MESSAGE.subject = "x"


# ---------------------------------------------------------------------------
# ResendMailer — request shape
# ---------------------------------------------------------------------------

def test_resend_mailer_posts_the_message_to_resend_with_bearer_auth(resend, monkeypatch):
    calls = _post_returning(monkeypatch, stub_http_response(200))

    resend.send(MESSAGE)

    assert len(calls) == 1
    url, kwargs = calls[0]
    assert url == RESEND_API_URL
    assert kwargs["headers"]["Authorization"] == "Bearer re_test_key"
    assert kwargs["headers"]["Content-Type"] == "application/json"
    assert kwargs["json"] == {
        "from": "noreply@test.com",
        "to": ["a@test.com", "b@test.com"],
        "subject": "Americhem Market-Pulse — August 27, 2026",
        "html": "<html>digest</html>",
    }
    assert kwargs["timeout"] == 30


# ---------------------------------------------------------------------------
# ResendMailer — retry policy
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status", sorted(TRANSIENT_HTTP_CODES))
def test_resend_mailer_retries_a_transient_status_with_backoff(monkeypatch, status):
    monkeypatch.setenv("SMTP_PASS", "re_test_key")
    calls = _post_returning(monkeypatch, stub_http_response(status), stub_http_response(200))
    slept: list = []

    ResendMailer(sleep=slept.append).send(MESSAGE)

    assert len(calls) == 2
    assert len(slept) == 1 and BASE_DELAY_S <= slept[0] <= BASE_DELAY_S + 0.5   # + jitter


def test_resend_mailer_gives_up_after_max_attempts(monkeypatch):
    monkeypatch.setenv("SMTP_PASS", "re_test_key")
    calls = _post_returning(monkeypatch, stub_http_response(503))
    slept: list = []

    with pytest.raises(requests.HTTPError):
        ResendMailer(sleep=slept.append).send(MESSAGE)

    assert len(calls) == MAX_ATTEMPTS
    assert len(slept) == MAX_ATTEMPTS - 1   # no backoff after the final attempt


def test_resend_mailer_raises_at_once_on_a_permanent_http_error(resend, monkeypatch):
    calls = _post_returning(monkeypatch, stub_http_response(403))

    with pytest.raises(requests.HTTPError):
        resend.send(MESSAGE)

    assert len(calls) == 1   # auth failures are not retried


def test_resend_mailer_does_not_retry_connection_errors(resend, monkeypatch):
    """The pinned contract: only transient HTTP statuses retry. A connection
    error or timeout is never retried — it propagates on whichever attempt it
    occurs (the old docstring promised otherwise; the behaviour never did)."""
    attempts: list = []

    def fake_post(url, **kw):
        attempts.append(url)
        raise requests.ConnectionError("resend unreachable")

    monkeypatch.setattr(mailer.requests, "post", fake_post)

    with pytest.raises(requests.ConnectionError):
        resend.send(MESSAGE)

    assert len(attempts) == 1


def test_resend_mailer_logs_and_propagates_any_other_request_error(resend, monkeypatch, caplog):
    """Everything that is not a transient status raises at once — and leaves a
    breadcrumb naming the email step, so a red job log is greppable."""
    def fake_post(url, **kw):
        raise requests.exceptions.ChunkedEncodingError("truncated body")

    monkeypatch.setattr(mailer.requests, "post", fake_post)

    with caplog.at_level("ERROR"), pytest.raises(requests.exceptions.ChunkedEncodingError):
        resend.send(MESSAGE)

    assert any("sending email" in r.getMessage() for r in caplog.records)


def test_resend_mailer_reads_the_api_key_at_send_time(monkeypatch):
    """Like the other seams, the adapter reads its own secret when used, not
    when constructed — a missing key surfaces as KeyError from send()."""
    monkeypatch.delenv("SMTP_PASS", raising=False)
    adapter = ResendMailer(sleep=lambda s: None)
    _post_returning(monkeypatch, stub_http_response(200))

    with pytest.raises(KeyError):
        adapter.send(MESSAGE)


# ---------------------------------------------------------------------------
# FakeMailer — the in-memory adapter
# ---------------------------------------------------------------------------

def test_fake_mailer_records_every_message_in_order():
    fake = FakeMailer()
    other = EmailMessage(sender="s", recipients=("r",), subject="second", html="")

    fake.send(MESSAGE)
    fake.send(other)

    assert fake.sent == [MESSAGE, other]


def test_fake_mailer_can_fail_like_the_transport():
    fake = FakeMailer(fail_with=RuntimeError("resend down"))

    with pytest.raises(RuntimeError, match="resend down"):
        fake.send(MESSAGE)

    assert fake.sent == []


# ---------------------------------------------------------------------------
# _mailer() — the process-wide adapter
# ---------------------------------------------------------------------------

def test_mailer_singleton_is_resend_in_production_and_resettable():
    _reset_mailer()
    try:
        first = _mailer()
        assert isinstance(first, ResendMailer)
        assert _mailer() is first
        _reset_mailer()
        assert _mailer() is not first
    finally:
        _reset_mailer()
