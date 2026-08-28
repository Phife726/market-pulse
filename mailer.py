"""The mailer seam — the one outbound email transport (see the **Mailer seam**
entry in CONTEXT.md).

One Protocol (`Mailer`), two adapters: `ResendMailer` for production and
`FakeMailer` for tests. The value that crosses the seam is `EmailMessage` —
the composed digest, transport-agnostic. The adapter owns everything about
*sending*: the Resend endpoint, the API key (`SMTP_PASS` — the legacy secret
name, kept so no rotation is needed), the payload shape and the retry policy.
It knows nothing about addressing or subject lines: the consumer
(`delivery_engine.send_email`) composes the message.

Retry policy: a transient HTTP status (`TRANSIENT_HTTP_CODES`) is retried up
to `MAX_ATTEMPTS` times with exponential backoff; any other outcome — a
permanent HTTP error, a connection error, a timeout — propagates at once so
`execute_pipeline` never stamps `delivered_at` for an email that did not go
out. Callers do `from mailer import _mailer` and call `_mailer().send(...)`;
tests inject the fake at the consumer module — e.g.
`monkeypatch.setattr("delivery_engine._mailer", lambda: FakeMailer())`.
"""
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Callable, Optional, Protocol

import requests

logger = logging.getLogger(__name__)

RESEND_API_URL = "https://api.resend.com/emails"
MAX_ATTEMPTS = 5
BASE_DELAY_S = 2.0
TRANSIENT_HTTP_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})


@dataclass(frozen=True)
class EmailMessage:
    """A composed email — what crosses the mailer seam."""
    sender: str
    recipients: tuple[str, ...]
    subject: str
    html: str


class Mailer(Protocol):
    def send(self, message: EmailMessage) -> None: ...


class ResendMailer:
    """Production adapter — the Resend HTTP API (policy: module docstring)."""

    def __init__(self, *, sleep: Callable[[float], None] = time.sleep) -> None:
        self._sleep = sleep

    def send(self, message: EmailMessage) -> None:
        """Raises:
            requests.HTTPError: a permanent HTTP error, or transient retries exhausted.
            requests.RequestException (ConnectionError, Timeout, ...): never
                retried — propagates on whichever attempt it occurs.
        """
        headers = {
            "Authorization": f"Bearer {os.environ['SMTP_PASS']}",
            "Content-Type": "application/json",
        }
        payload = {
            "from": message.sender,
            "to": list(message.recipients),
            "subject": message.subject,
            "html": message.html,
        }

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                resp = requests.post(RESEND_API_URL, json=payload, headers=headers, timeout=30)
            except requests.ConnectionError as exc:
                logger.error("Connection error reaching Resend API: %s", exc)
                raise
            except requests.Timeout:
                logger.error("Request to Resend API timed out")
                raise
            except requests.RequestException as exc:
                logger.error("Unexpected error sending email: %s", exc)
                raise

            if resp.status_code in TRANSIENT_HTTP_CODES and attempt < MAX_ATTEMPTS:
                delay = BASE_DELAY_S * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                logger.warning(
                    "Transient HTTP %s from Resend (attempt %d/%d) — retrying in %.1fs",
                    resp.status_code, attempt, MAX_ATTEMPTS, delay,
                )
                self._sleep(delay)
                continue

            if not resp.ok:
                logger.error("Resend API returned HTTP %s — body: %s", resp.status_code, resp.text)
                resp.raise_for_status()

            logger.info(
                "Email sent — subject: '%s' | recipients: %d",
                message.subject, len(message.recipients),
            )
            return


class FakeMailer:
    """Test adapter — records every message that crosses the seam. Set
    `fail_with` (constructor or attribute) to raise instead, standing in for
    a transport failure."""

    def __init__(self, *, fail_with: Optional[Exception] = None) -> None:
        self.sent: list[EmailMessage] = []
        self.fail_with = fail_with

    def send(self, message: EmailMessage) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        self.sent.append(message)


_mailer_singleton: Optional[Mailer] = None


def _mailer() -> Mailer:
    """Return the process-wide mailer (Resend in prod; tests inject a fake)."""
    global _mailer_singleton
    if _mailer_singleton is None:
        _mailer_singleton = ResendMailer()
    return _mailer_singleton


def _reset_mailer() -> None:
    """Drop the cached adapter — used by tests for isolation."""
    global _mailer_singleton
    _mailer_singleton = None
