# Plan: SMTP Retry with Exponential Backoff

**Goal:** Make `send_email()` resilient to transient Resend 421 errors by retrying up to 5 times with exponential backoff + jitter before propagating failure.

**Spec:** `docs/superpowers/specs/2026-03-27-smtp-retry-design.md`

**Architecture overview:** Single-function change inside `delivery_engine.py`. The existing `try/except` block is replaced with a `for` loop. No new dependencies, no new public symbols, no changes to callers or CI.

**Tech stack:** Python 3.10, stdlib `smtplib` / `time` / `random`, pytest

---

## File Map

| File | Change |
|------|--------|
| `delivery_engine.py` | Add `import random`, `import time`; add 3 module-level constants; replace `try/except` in `send_email()` with retry loop; update docstring |
| `tests/test_pipeline.py` | Add 2 new tests: retry-then-succeed on 421, immediate raise on auth failure |

---

## Tasks

---

### Task 1 — Write failing tests for retry behaviour

**Why first:** Confirms the existing code does NOT retry (tests will fail until Task 2 is done).

**File:** `tests/test_pipeline.py`

Append the following two tests at the bottom of the file:

```python
# ---------------------------------------------------------------------------
# 10. send_email() SMTP retry behaviour
# ---------------------------------------------------------------------------

import smtplib as _smtplib
import time as _time
from unittest.mock import MagicMock

from delivery_engine import send_email as _send_email


def _smtp_env(monkeypatch) -> None:
    """Inject minimal SMTP env vars required by send_email()."""
    monkeypatch.setenv("SMTP_SERVER", "smtp.resend.com")
    monkeypatch.setenv("SMTP_PORT", "465")
    monkeypatch.setenv("SMTP_USER", "resend")
    monkeypatch.setenv("SMTP_PASS", "re_test_key")
    monkeypatch.setenv("SENDER_EMAIL", "noreply@test.com")
    monkeypatch.setenv("RECIPIENT_EMAILS", "user@test.com")


def test_send_email_retries_on_421_then_succeeds(monkeypatch):
    """send_email() must retry after a transient 421 and succeed on the second attempt."""
    _smtp_env(monkeypatch)
    monkeypatch.setattr(_time, "sleep", lambda s: None)  # no actual sleeping

    attempt = {"count": 0}

    def fake_smtp_ssl(*args, **kwargs):
        attempt["count"] += 1
        if attempt["count"] == 1:
            raise _smtplib.SMTPConnectError(421, b"Too many connected clients")
        mock_server = MagicMock()
        mock_server.__enter__ = lambda s: s
        mock_server.__exit__ = MagicMock(return_value=False)
        return mock_server

    monkeypatch.setattr(_smtplib, "SMTP_SSL", fake_smtp_ssl)

    _send_email("<html>test</html>")  # must not raise
    assert attempt["count"] == 2


def test_send_email_raises_immediately_on_auth_failure(monkeypatch):
    """send_email() must not retry on SMTPAuthenticationError — raise on first attempt."""
    _smtp_env(monkeypatch)
    monkeypatch.setattr(_time, "sleep", lambda s: None)

    attempt = {"count": 0}

    def fake_smtp_ssl(*args, **kwargs):
        attempt["count"] += 1
        raise _smtplib.SMTPAuthenticationError(535, b"Bad credentials")

    monkeypatch.setattr(_smtplib, "SMTP_SSL", fake_smtp_ssl)

    with pytest.raises(_smtplib.SMTPAuthenticationError):
        _send_email("<html>test</html>")

    assert attempt["count"] == 1  # must NOT have retried
```

**Run test — confirm failure:**
```bash
pytest tests/test_pipeline.py::test_send_email_retries_on_421_then_succeeds tests/test_pipeline.py::test_send_email_raises_immediately_on_auth_failure -v
```

Expected output: both tests **FAIL** (no retry logic exists yet).

---

### Task 2 — Add `import random` and `import time` to delivery_engine.py

**File:** `delivery_engine.py`, lines 1–7 (the stdlib import block)

Change:
```python
import logging
import os
import smtplib
import socket
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
```

To:
```python
import logging
import os
import random
import smtplib
import socket
import time
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
```

No test step needed — import errors would surface in Task 3's test run.

---

### Task 3 — Add retry constants after brand constants block

**File:** `delivery_engine.py`, after line 26 (after the `_LOGO_URL` assignment, before the blank line leading to `# Client factory`)

Insert after the `_LOGO_URL` block:

```python
# ---------------------------------------------------------------------------
# SMTP retry constants
# ---------------------------------------------------------------------------

_MAX_SMTP_ATTEMPTS   = 5
_SMTP_BASE_DELAY_S   = 2.0
_TRANSIENT_SMTP_CODES = {421, 450, 451, 452}
```

No test step needed — verified implicitly in Task 4.

---

### Task 4 — Replace the try/except block in send_email() with a retry loop

**File:** `delivery_engine.py`, lines 639–675

Replace the entire `try/except` block (from `try:` through the final `raise`) with:

```python
    for attempt in range(1, _MAX_SMTP_ATTEMPTS + 1):
        try:
            if smtp_port == 465:
                _ctx = smtplib.ssl.create_default_context()
                _conn = smtplib.SMTP_SSL(smtp_server, smtp_port, timeout=30, context=_ctx)
            else:
                _conn = smtplib.SMTP(smtp_server, smtp_port, timeout=30)

            with _conn as server:
                if smtp_port != 465:
                    server.ehlo()
                    server.starttls()
                    server.ehlo()
                server.login(smtp_user, smtp_pass)
                server.sendmail(sender_email, recipients, msg.as_string())

            logger.info(
                "Email sent — subject: '%s' | recipients: %d",
                subject,
                len(recipients),
            )
            return

        except smtplib.SMTPAuthenticationError as exc:
            # Not transient — bad credentials will not improve with retries.
            logger.error(
                "SMTP authentication failed (check SMTP_USER / SMTP_PASS): %s", exc
            )
            raise

        except (smtplib.SMTPConnectError, smtplib.SMTPResponseException) as exc:
            # SMTPAuthenticationError is a subclass of SMTPResponseException,
            # so it must be caught first (above) to avoid being treated as transient.
            if exc.args[0] in _TRANSIENT_SMTP_CODES and attempt < _MAX_SMTP_ATTEMPTS:
                delay = _SMTP_BASE_DELAY_S * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                logger.warning(
                    "Transient SMTP error %s (attempt %d/%d) — retrying in %.1fs",
                    exc.args[0],
                    attempt,
                    _MAX_SMTP_ATTEMPTS,
                    delay,
                )
                time.sleep(delay)
                continue
            logger.error("SMTP error while sending email: %s", exc)
            raise

        except socket.timeout:
            logger.error(
                "SMTP connection timed out to %s:%s", smtp_server, smtp_port
            )
            raise

        except Exception as exc:
            logger.error("Unexpected error sending email: %s", exc)
            raise
```

**Run tests — confirm both new tests pass and no regressions:**
```bash
pytest tests/ -v
```

Expected output: **all tests pass**, including the two new SMTP retry tests.

---

### Task 5 — Update send_email() docstring

**File:** `delivery_engine.py`, lines 603–616 (`send_email` docstring `Raises:` section)

Change:
```python
    Raises:
        smtplib.SMTPAuthenticationError: On bad SMTP credentials.
        smtplib.SMTPException:           On SMTP-level failures.
        socket.timeout:                  If SMTP server is unreachable.
```

To:
```python
    Raises:
        smtplib.SMTPAuthenticationError: On bad SMTP credentials (not retried).
        smtplib.SMTPException:           On SMTP-level failures after all retries.
        smtplib.SMTPConnectError:        After all retry attempts on transient 421/45x errors.
        socket.timeout:                  If SMTP server is unreachable.
```

**Run full test suite one final time:**
```bash
pytest tests/ -v
```

Expected: all tests pass, zero failures.

---

### Task 6 — Commit

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "$(cat <<'EOF'
feat(delivery): retry send_email() on transient SMTP 421/45x with exponential backoff

Resend returned 421 "Too many connected clients" on 2026-03-26, failing the
delivery run. send_email() now retries up to 5 times (2s→4s→8s→16s + jitter)
before propagating. Non-transient errors (auth failures, socket timeouts) still
raise immediately.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Verification Checklist

- [ ] `pytest tests/ -v` — all tests green
- [ ] `delivery_engine.py` imports include `random` and `time`
- [ ] Three `_MAX_SMTP_ATTEMPTS` / `_SMTP_BASE_DELAY_S` / `_TRANSIENT_SMTP_CODES` constants visible at module level
- [ ] `send_email()` body contains `for attempt in range(1, _MAX_SMTP_ATTEMPTS + 1):`
- [ ] `SMTPAuthenticationError` is caught before `SMTPResponseException` in the loop
- [ ] No changes to `execute_pipeline()`, CI workflow, or `requirements.txt`
