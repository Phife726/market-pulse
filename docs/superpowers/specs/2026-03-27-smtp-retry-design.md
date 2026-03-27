# SMTP Retry Design — Transient 421/45x Backoff

**Date:** 2026-03-27
**Status:** Approved
**Scope:** `delivery_engine.py` — `send_email()` only

---

## Problem

The GitHub Actions delivery run on 2026-03-26 failed with:

```
smtplib.SMTPConnectError: (421, b'Resend SMTP Relay Too many connected clients, try again in a moment')
```

`send_email()` makes a single connection attempt and propagates any exception immediately. A transient 421 ("try again in a moment") causes the entire workflow to fail and no email is delivered.

---

## Goal

Make `send_email()` resilient to transient SMTP errors by retrying with exponential backoff + jitter before giving up.

---

## Approach

**Option B — inline retry loop inside `send_email()`, hardcoded constants, no helper function.**

Chosen because:
- Zero new dependencies
- Minimum surface area (no new public symbols, no env var config)
- Consistent with the project's YAGNI / minimum-complexity ethos (CLAUDE.md)

---

## Design

### New imports (stdlib only)

```python
import random
import time
```

Added to the existing import block in `delivery_engine.py`.

### Retry constants (module-level)

```python
_MAX_SMTP_ATTEMPTS = 5
_SMTP_BASE_DELAY_S = 2.0
_TRANSIENT_SMTP_CODES = {421, 450, 451, 452}
```

Backoff schedule (exponential + jitter up to 0.5s):

| Attempt | Sleep before next |
|---------|------------------|
| 1 → 2   | ~2s              |
| 2 → 3   | ~4s              |
| 3 → 4   | ~8s              |
| 4 → 5   | ~16s             |

Worst-case total wait: 30s base + up to 2s jitter (4 intervals × 0.5s max) = ~32s maximum. Well within GitHub Actions' 6-minute job timeout.

### Loop structure

The existing `try/except` block inside `send_email()` is replaced with a
`for attempt in range(1, _MAX_SMTP_ATTEMPTS + 1):` loop:

```
for attempt in 1..5:
    try:
        connect → login → sendmail → log success → return
    except SMTPAuthenticationError:
        raise immediately  # not transient; must be caught BEFORE SMTPResponseException
                           # because SMTPAuthenticationError is a subclass of SMTPResponseException
    except (SMTPConnectError, SMTPResponseException) where exc.args[0] in _TRANSIENT_SMTP_CODES:
        # extract code via exc.args[0] (integer)
        if attempt < max: log warning, sleep(backoff + random.uniform(0, 0.5)), continue
        else: raise
    except socket.timeout | SMTPException | Exception:
        raise immediately  # non-transient, existing behavior preserved
```

### Docstring update

Add to `Raises:` section:

```
smtplib.SMTPConnectError: After all retry attempts on transient 421/45x errors.
```

---

## Out of Scope

- No changes to `execute_pipeline()`
- No changes to tests (`tests/test_pipeline.py` — SMTP is mocked, no retry path exercised)
- No changes to CI workflow (`.github/workflows/market_pulse.yml`)
- No changes to `requirements.txt`

---

## Success Criteria

- On a 421 response, the delivery run logs a warning and retries rather than failing immediately
- On a non-transient error (bad credentials, real socket timeout), behavior is identical to today
- On success after retry, the log line reads identically to a first-attempt success
- All existing tests continue to pass (`pytest tests/`)
