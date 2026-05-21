# Jason-Only Test Pipeline — Design Spec

**Date:** 2026-05-21
**Author:** Jason (jsp3822@gmail.com), via Claude Code brainstorming
**Status:** Approved — ready for implementation plan

---

## Purpose

Create a secondary, manually-triggered GitHub Actions workflow that runs the full Market-Pulse pipeline (ingestion + delivery) but sends the resulting email only to Jason. This gives a private QA channel for iterating on prompts, filtering, and email layout without disturbing the stakeholder-facing daily report.

## Goals

1. Production daily report (`market_pulse.yml`) continues to run Monday–Friday at 10:00 UTC against the stakeholder recipient list. No behavioural change.
2. A new workflow can be dispatched manually from the GitHub Actions tab to run the same pipeline against a Jason-only recipient list.
3. Every test-mode email is visually marked as a test in both subject and body, so that accidental forwarding cannot be mistaken for a production report.
4. Test-mode delivery is opt-out per run via a `send_email` workflow input.
5. No schema changes. Same Supabase tables for both modes; dedupe behaviour shared.

## Non-Goals

- Separate Supabase project or test tables.
- A `run_mode` column on `daily_intelligence` to isolate test articles from production dedupe.
- Scheduling the test workflow on a cron.
- Test-mode-specific ingestion behaviour (smaller scrape cap, separate config branch, etc.). The env var `MARKET_PULSE_RUN_MODE=test` is propagated to ingestion to leave room for this later, but ingestion code does not yet read it.
- Modifications to the production workflow file.

---

## Architecture

### Components touched

| Component | Change |
|---|---|
| `.github/workflows/market_pulse.yml` | None |
| `.github/workflows/market_pulse_test.yml` | **New file** |
| `delivery_engine.py` | Add `_is_test_mode()` helper; thread test-mode awareness through subject builder, main HTML header, and no-news HTML |
| `tests/test_pipeline.py` | Add 5 new tests covering subject + header + no-news markings, both modes |
| GitHub repo secrets | Operator adds `TEST_RECIPIENT_EMAILS=jphifer@americhem.com` (out-of-band; not a code change) |

### Data flow

```
workflow_dispatch ── checkout ── pytest ── ingestion_engine.py ── delivery_engine.py ── Resend API ── jphifer@americhem.com
                                                ↑                          ↑
                                  MARKET_PULSE_RUN_MODE=test    RECIPIENT_EMAILS=TEST_RECIPIENT_EMAILS
                                  RECIPIENT_EMAILS=TEST_…        + [TEST] subject prefix
                                                                 + TEST RUN banner in HTML
```

Test mode is signalled to the application code by exactly one environment variable: `MARKET_PULSE_RUN_MODE`. The workflow sets it to `test`. Any other value (or unset) means production.

---

## Detailed Design

### 1. New workflow file — `.github/workflows/market_pulse_test.yml`

```yaml
name: Market Pulse Test Pipeline

on:
  workflow_dispatch:
    inputs:
      send_email:
        description: "Send test email after generation?"
        required: true
        default: "true"
        type: choice
        options:
          - "true"
          - "false"

env:
  FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true

jobs:
  run-market-pulse-test:
    runs-on: ubuntu-latest
    timeout-minutes: 15

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python 3.10
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      - name: Install requirements.txt
        run: pip install -r requirements.txt

      - name: Run pytest tests/test_pipeline.py
        env:
          FIRECRAWL_API_KEY: ${{ secrets.FIRECRAWL_API_KEY }}
          OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
          SERPER_API_KEY: ${{ secrets.SERPER_API_KEY }}
          SMTP_PASS: ${{ secrets.SMTP_PASS }}
          SENDER_EMAIL: ${{ secrets.SENDER_EMAIL }}
          RECIPIENT_EMAILS: ${{ secrets.TEST_RECIPIENT_EMAILS }}
        run: pytest tests/test_pipeline.py

      - name: Run ingestion_engine.py
        env:
          FIRECRAWL_API_KEY: ${{ secrets.FIRECRAWL_API_KEY }}
          OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
          SERPER_API_KEY: ${{ secrets.SERPER_API_KEY }}
          SMTP_PASS: ${{ secrets.SMTP_PASS }}
          SENDER_EMAIL: ${{ secrets.SENDER_EMAIL }}
          RECIPIENT_EMAILS: ${{ secrets.TEST_RECIPIENT_EMAILS }}
          MARKET_PULSE_RUN_MODE: test
        run: python ingestion_engine.py

      - name: Run delivery_engine.py
        if: ${{ github.event.inputs.send_email == 'true' }}
        env:
          FIRECRAWL_API_KEY: ${{ secrets.FIRECRAWL_API_KEY }}
          OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
          SERPER_API_KEY: ${{ secrets.SERPER_API_KEY }}
          SMTP_PASS: ${{ secrets.SMTP_PASS }}
          SENDER_EMAIL: ${{ secrets.SENDER_EMAIL }}
          RECIPIENT_EMAILS: ${{ secrets.TEST_RECIPIENT_EMAILS }}
          MARKET_PULSE_RUN_MODE: test
        run: python delivery_engine.py
```

**Notable differences from production workflow:**

- Trigger is `workflow_dispatch` only (no `schedule`).
- One input: `send_email` (boolean choice, default `"true"`).
- Delivery step has an `if:` guard that skips execution when `send_email == "false"`. Ingestion always runs.
- `RECIPIENT_EMAILS` env var is populated from `secrets.TEST_RECIPIENT_EMAILS` (a new secret the operator adds).
- `MARKET_PULSE_RUN_MODE: test` is added to both ingestion and delivery steps.
- Drops the legacy `SMTP_SERVER` / `SMTP_PORT` / `SMTP_USER` env vars — delivery uses the Resend HTTP API (only `SMTP_PASS` is read, as the Resend API key under a legacy name). Production keeps them for now; the test workflow starts clean.

### 2. `delivery_engine.py` — test-mode awareness

Add a single helper near the top of the file (after the brand constants, before the client factories):

```python
def _is_test_mode() -> bool:
    """Return True when MARKET_PULSE_RUN_MODE env var is set to 'test' (case-insensitive)."""
    return os.environ.get("MARKET_PULSE_RUN_MODE", "").strip().lower() == "test"
```

Then thread that boolean through three call sites. **The function is called fresh at each site rather than cached** so test code can use `monkeypatch.setenv` between calls without import-time stickiness.

#### 2a. Subject prefix — `send_email()` at delivery_engine.py:896-899

```python
subject = (
    f"Americhem Market-Pulse — "
    f"{datetime.now().strftime('%B %d, %Y')}"
)
if _is_test_mode():
    subject = f"[TEST] {subject}"
```

This applies to both the main email path and the no-news fallback path, since both call `send_email()` with whatever HTML body they produced.

#### 2b. Main email header — `generate_html_email()` at delivery_engine.py:795

The existing header has a navy bar with the title `Market-Pulse: Daily Intelligence`. In test mode:

1. Prepend `[TEST] ` to the title text in the `<p>` element, so accidental forwarding shows the marker in narrow preview panes.
2. Insert a new orange sub-banner row immediately below the existing green divider strip (between the navy header and the navy-dark date strip). The banner reads:

   ```
   TEST RUN · Jason-only QA output — not for distribution
   ```

   Styling: orange background `#D97706`, white text, same horizontal padding as the date row, 8px vertical padding, uppercase letter-spacing 1.5px, font size 11px, weight 700.

The banner row is only emitted in test mode. In production mode, the rendered HTML is byte-identical to today's output.

#### 2c. No-news email — `_generate_no_news_email()` at delivery_engine.py:846-871

Apply the same two changes to the no-news template:

1. Title text gets the `[TEST] ` prefix.
2. The TEST RUN banner row is inserted below the green divider, same styling.

The subject is already covered by the change to `send_email()`.

### 3. Tests — `tests/test_pipeline.py`

Add a new section at the end of the file with these 5 tests:

```python
# ===========================================================================
# MARKET_PULSE_RUN_MODE — test-mode markings
# ===========================================================================

def test_generate_html_email_test_mode_prefixes_header(monkeypatch):
    """In test mode, the header title carries [TEST] and the TEST RUN banner is present."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    data = [_make_new_article("h", 8, headline="Some Headline")]
    with patch("delivery_engine._get_openai", return_value=MagicMock()):
        html = generate_html_email(data)
    assert "[TEST]" in html
    assert "TEST RUN" in html
    assert "Jason-only QA output" in html


def test_generate_html_email_production_mode_unchanged(monkeypatch):
    """In production mode (env unset), the header has no [TEST] or banner."""
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    data = [_make_new_article("h", 8, headline="Some Headline")]
    with patch("delivery_engine._get_openai", return_value=MagicMock()):
        html = generate_html_email(data)
    assert "[TEST]" not in html
    assert "TEST RUN" not in html


def test_send_email_test_mode_prefixes_subject(monkeypatch):
    """Captured Resend payload subject starts with '[TEST] ' in test mode."""
    _email_env(monkeypatch)
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    monkeypatch.setattr(_time, "sleep", lambda s: None)
    captured = {}
    def fake_post(*args, **kwargs):
        captured["payload"] = kwargs["json"]
        resp = MagicMock(); resp.status_code = 200; resp.ok = True
        resp.raise_for_status = MagicMock()
        return resp
    monkeypatch.setattr(_requests, "post", fake_post)
    _send_email("<html>x</html>")
    assert captured["payload"]["subject"].startswith("[TEST] ")


def test_send_email_production_mode_subject_unchanged(monkeypatch):
    """Subject has no [TEST] prefix when env var is unset."""
    _email_env(monkeypatch)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    monkeypatch.setattr(_time, "sleep", lambda s: None)
    captured = {}
    def fake_post(*args, **kwargs):
        captured["payload"] = kwargs.get("json")
        resp = MagicMock(); resp.status_code = 200; resp.ok = True
        resp.raise_for_status = MagicMock()
        return resp
    monkeypatch.setattr(_requests, "post", fake_post)
    _send_email("<html>x</html>")
    assert "[TEST]" not in captured["payload"]["subject"]


def test_no_news_email_test_mode_marks_header(monkeypatch):
    """The no-news fallback HTML carries [TEST] and the TEST RUN banner in test mode."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    from delivery_engine import _generate_no_news_email
    html = _generate_no_news_email()
    assert "[TEST]" in html
    assert "TEST RUN" in html
```

The existing test helpers `_make_new_article`, `_email_env`, `_send_email`, `_time`, and `_requests` are reused from earlier sections of `test_pipeline.py`.

---

## Out-of-band Operator Step

After this work merges, Jason adds a new GitHub repo secret:

- **Name:** `TEST_RECIPIENT_EMAILS`
- **Value:** `jphifer@americhem.com`

This is not a code change. It must happen before the test workflow can be dispatched.

---

## Risks & Mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Test run ingests articles that production then skips as duplicates within 72h | Medium | Operational: dispatch test runs **after** the day's production send (10:00 UTC). If this becomes painful, add a `run_mode` column to `daily_intelligence` later — out of scope here. |
| Operator forgets to set `TEST_RECIPIENT_EMAILS` and the test workflow fails noisily | Low | The workflow fails at the delivery step with a clear "RECIPIENT_EMAILS not set" KeyError. No silent fallthrough to the production list, because the secret is referenced by a different name. |
| Test mode env var leaks into a production run somehow (e.g., manually re-dispatched production workflow with edited env) | Very low | The production workflow file does not set `MARKET_PULSE_RUN_MODE`. A leak would require either repo-level env var configuration or workflow file edits — both visible in PR review. |
| `[TEST]` prefix breaks email client previews or thread grouping | Low | Bracket prefixes are standard practice and well-supported. The banner provides redundancy if subject is truncated. |

## Verification Checklist

After implementation:

- [ ] `pytest tests/` passes locally.
- [ ] Production workflow file diff is empty (no changes).
- [ ] Test workflow appears in the GitHub Actions sidebar after merge.
- [ ] Dispatching the test workflow with `send_email=true` results in a single email to `jphifer@americhem.com` with `[TEST]` in the subject and a visible TEST RUN banner.
- [ ] Dispatching with `send_email=false` runs ingestion only; no email is sent.
- [ ] The next scheduled production run produces an email with no `[TEST]` marker and the existing recipient list.
