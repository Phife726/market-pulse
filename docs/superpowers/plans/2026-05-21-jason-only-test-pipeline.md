# Jason-Only Test Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a manually-triggered GitHub Actions workflow that runs the full Market-Pulse pipeline against a Jason-only recipient list, with `[TEST]` markings on subject and body so test emails cannot be confused with production reports.

**Architecture:** A new workflow file (`.github/workflows/market_pulse_test.yml`) sets `MARKET_PULSE_RUN_MODE=test` and points `RECIPIENT_EMAILS` at a new `TEST_RECIPIENT_EMAILS` secret. `delivery_engine.py` gains a tiny `_is_test_mode()` helper that three call sites consult to prefix the subject and inject a visible TEST RUN banner in the HTML header (both the main and no-news templates). Production workflow stays untouched.

**Tech Stack:** Python 3.10, pytest, GitHub Actions, Resend HTTP API.

**Spec:** `docs/superpowers/specs/2026-05-21-jason-only-test-pipeline-design.md`

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `.github/workflows/market_pulse.yml` | **Unchanged** | Production daily report — must not be touched |
| `.github/workflows/market_pulse_test.yml` | **Create** | Jason-only manual test pipeline |
| `delivery_engine.py` | **Modify** | Add `_is_test_mode()`; thread through `send_email`, `generate_html_email`, `_generate_no_news_email` |
| `tests/test_pipeline.py` | **Modify** | Append a new test section with 6 tests covering subject + header + no-news markings + recipient isolation |
| `docs/superpowers/plans/2026-05-21-jason-only-test-pipeline.md` | This file | The plan itself |

No new modules. No schema changes. No production workflow edits.

---

## Task 1: Add subject prefix and recipient-isolation guarantees in `send_email()`

This task adds the `_is_test_mode()` helper and modifies `send_email()` to prefix the subject with `[TEST] ` when test mode is active. Includes a guard test that proves the recipient list is sourced exclusively from the `RECIPIENT_EMAILS` env var.

**Files:**
- Modify: `delivery_engine.py:69` (add helper near config loaders), `delivery_engine.py:896-899` (subject builder in `send_email`)
- Modify: `tests/test_pipeline.py` (append new section at end of file)

- [ ] **Step 1.1: Write the failing subject-prefix test**

Append at the end of `tests/test_pipeline.py`:

```python
# ===========================================================================
# MARKET_PULSE_RUN_MODE — test-mode markings
# ===========================================================================

def test_send_email_test_mode_prefixes_subject(monkeypatch):
    """In test mode, the Resend payload subject must start with '[TEST] '."""
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
```

- [ ] **Step 1.2: Run the test and verify it fails**

Run: `pytest tests/test_pipeline.py::test_send_email_test_mode_prefixes_subject -v`

Expected: FAIL — assertion `captured["payload"]["subject"].startswith("[TEST] ")` returns False because `send_email()` does not yet check `MARKET_PULSE_RUN_MODE`.

- [ ] **Step 1.3: Add `_is_test_mode()` helper to `delivery_engine.py`**

In `delivery_engine.py`, immediately after the `_config_int` function (around line 65, before the "Email delivery retry constants" comment block at line 68), insert:

```python
# ---------------------------------------------------------------------------
# Run-mode detection
# ---------------------------------------------------------------------------

def _is_test_mode() -> bool:
    """Return True when MARKET_PULSE_RUN_MODE env var is set to 'test' (case-insensitive)."""
    return os.environ.get("MARKET_PULSE_RUN_MODE", "").strip().lower() == "test"
```

- [ ] **Step 1.4: Apply `[TEST]` subject prefix in `send_email()`**

In `delivery_engine.py`, find the subject builder at line 896-899:

```python
    subject = (
        f"Americhem Market-Pulse — "
        f"{datetime.now().strftime('%B %d, %Y')}"
    )
```

Replace it with:

```python
    subject = (
        f"Americhem Market-Pulse — "
        f"{datetime.now().strftime('%B %d, %Y')}"
    )
    if _is_test_mode():
        subject = f"[TEST] {subject}"
```

- [ ] **Step 1.5: Run the test and verify it passes**

Run: `pytest tests/test_pipeline.py::test_send_email_test_mode_prefixes_subject -v`

Expected: PASS.

- [ ] **Step 1.6: Add the production-mode subject-unchanged test**

Append immediately after the previous test:

```python
def test_send_email_production_mode_subject_unchanged(monkeypatch):
    """When MARKET_PULSE_RUN_MODE is unset, the subject must have no [TEST] prefix."""
    _email_env(monkeypatch)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    monkeypatch.setattr(_time, "sleep", lambda s: None)

    captured = {}
    def fake_post(*args, **kwargs):
        captured["payload"] = kwargs["json"]
        resp = MagicMock(); resp.status_code = 200; resp.ok = True
        resp.raise_for_status = MagicMock()
        return resp

    monkeypatch.setattr(_requests, "post", fake_post)
    _send_email("<html>x</html>")
    assert "[TEST]" not in captured["payload"]["subject"]
```

- [ ] **Step 1.7: Run the new test and verify it passes**

Run: `pytest tests/test_pipeline.py::test_send_email_production_mode_subject_unchanged -v`

Expected: PASS — production mode has no `[TEST]` prefix.

- [ ] **Step 1.8: Add the recipient-isolation safety test**

Append immediately after the previous test:

```python
def test_send_email_recipient_list_is_only_recipient_emails_env(monkeypatch):
    """Recipient invariant: send_email() builds the Resend 'to' list strictly from the
    RECIPIENT_EMAILS env var and never falls back to any hardcoded address. This is
    the safety guarantee that lets the workflow swap recipient pools by env var alone.
    """
    monkeypatch.setenv("SMTP_PASS", "re_test_key")
    monkeypatch.setenv("SENDER_EMAIL", "noreply@test.com")
    monkeypatch.setenv("RECIPIENT_EMAILS", "jphifer@americhem.com")
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
    assert captured["payload"]["to"] == ["jphifer@americhem.com"]
```

- [ ] **Step 1.9: Run the recipient-isolation test and verify it passes**

Run: `pytest tests/test_pipeline.py::test_send_email_recipient_list_is_only_recipient_emails_env -v`

Expected: PASS — no implementation change needed, this is a guard test that proves the existing behaviour.

- [ ] **Step 1.10: Run the full suite to catch regressions**

Run: `pytest tests/`

Expected: All tests pass.

- [ ] **Step 1.11: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git -c commit.gpgsign=false commit -m "feat(delivery): add MARKET_PULSE_RUN_MODE test-mode subject prefix

Introduce _is_test_mode() helper and use it in send_email() to prefix
the Resend subject with [TEST] when MARKET_PULSE_RUN_MODE=test. Adds
three tests: subject prefix in test mode, no prefix in production mode,
and the recipient-isolation guarantee that send_email() reads the to
list strictly from RECIPIENT_EMAILS."
```

---

## Task 2: Add `[TEST]` marker and TEST RUN banner to `generate_html_email()`

This task modifies the main HTML email template so that, in test mode, the title is prefixed with `[TEST]` and an orange `TEST RUN · Jason-only QA output` banner appears below the existing green divider strip.

**Files:**
- Modify: `delivery_engine.py:795` (header title text) and `delivery_engine.py:801-811` (insert banner row after green divider)
- Modify: `tests/test_pipeline.py` (append two more tests)

- [ ] **Step 2.1: Write the failing header-marker test**

Append at the end of `tests/test_pipeline.py` after Task 1's tests:

```python
def test_generate_html_email_test_mode_prefixes_header(monkeypatch):
    """In test mode, generate_html_email() must include [TEST] in the title and
    a visible TEST RUN banner in the rendered HTML."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    data = [_make_new_article("h", 8, headline="Some Headline")]
    with patch("delivery_engine._get_openai", return_value=MagicMock()):
        html = generate_html_email(data)
    assert "[TEST]" in html
    assert "TEST RUN" in html
    assert "Jason-only QA output" in html
```

- [ ] **Step 2.2: Run the test and verify it fails**

Run: `pytest tests/test_pipeline.py::test_generate_html_email_test_mode_prefixes_header -v`

Expected: FAIL — `[TEST]` and `TEST RUN` are not present in the rendered HTML.

- [ ] **Step 2.3: Add the `[TEST]` title prefix in the main header**

In `delivery_engine.py`, find line 795:

```python
                      <p style="margin:2px 0 0 0;font-size:18px;font-weight:700;color:#ffffff;font-family:Arial,sans-serif;line-height:1.2;">Market-Pulse: Daily Intelligence</p>
```

This line is inside the `f"""..."""` block that begins at line 764 (`return f"""<!DOCTYPE html>...`), so it has access to local variables. Before the `return f"""<!DOCTYPE html>` statement (line 764), add a new local:

```python
    title_prefix = "[TEST] " if _is_test_mode() else ""
```

Then change the title line at 795 to:

```python
                      <p style="margin:2px 0 0 0;font-size:18px;font-weight:700;color:#ffffff;font-family:Arial,sans-serif;line-height:1.2;">{title_prefix}Market-Pulse: Daily Intelligence</p>
```

- [ ] **Step 2.4: Insert the TEST RUN banner row after the green divider**

Still in `generate_html_email()`, before the `return f"""<!DOCTYPE html>` statement, add a second local right after `title_prefix`:

```python
    test_banner_row = (
        '<tr><td style="background-color:#D97706;padding:8px 32px;font-size:11px;'
        'font-weight:700;letter-spacing:1.5px;color:#ffffff;'
        'font-family:Arial,sans-serif;text-transform:uppercase;">'
        'TEST RUN · Jason-only QA output — not for distribution'
        '</td></tr>'
        if _is_test_mode() else ""
    )
```

Then find line 801 in the template literal:

```python
            <tr><td style="background-color:{_BRAND_GREEN};height:3px;font-size:0;line-height:0;">&nbsp;</td></tr>
```

Change it to add the banner row immediately after:

```python
            <tr><td style="background-color:{_BRAND_GREEN};height:3px;font-size:0;line-height:0;">&nbsp;</td></tr>
            {test_banner_row}
```

- [ ] **Step 2.5: Run the test and verify it passes**

Run: `pytest tests/test_pipeline.py::test_generate_html_email_test_mode_prefixes_header -v`

Expected: PASS.

- [ ] **Step 2.6: Add the production-mode unchanged test**

Append immediately after:

```python
def test_generate_html_email_production_mode_unchanged(monkeypatch):
    """When MARKET_PULSE_RUN_MODE is unset, the rendered HTML must contain
    no [TEST] markers or TEST RUN banner."""
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    data = [_make_new_article("h", 8, headline="Some Headline")]
    with patch("delivery_engine._get_openai", return_value=MagicMock()):
        html = generate_html_email(data)
    assert "[TEST]" not in html
    assert "TEST RUN" not in html
```

- [ ] **Step 2.7: Run the test and verify it passes**

Run: `pytest tests/test_pipeline.py::test_generate_html_email_production_mode_unchanged -v`

Expected: PASS — production-mode output is byte-clean of test markers.

- [ ] **Step 2.8: Run the full suite to catch regressions**

Run: `pytest tests/`

Expected: All tests pass.

- [ ] **Step 2.9: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git -c commit.gpgsign=false commit -m "feat(delivery): mark test-mode emails with [TEST] title and TEST RUN banner

generate_html_email() now prefixes the header title with [TEST] and
inserts a visible orange banner row when MARKET_PULSE_RUN_MODE=test.
Production-mode output is byte-identical to before."
```

---

## Task 3: Add `[TEST]` marker and banner to `_generate_no_news_email()`

The no-news fallback path is invoked when ingestion finds nothing for the day. It must carry the same markings as the main email so a test-mode no-news run cannot be mistaken for a real one.

**Files:**
- Modify: `delivery_engine.py:846-871` (no-news template)
- Modify: `tests/test_pipeline.py` (append one more test)

- [ ] **Step 3.1: Write the failing no-news marker test**

Append at the end of `tests/test_pipeline.py`:

```python
def test_no_news_email_test_mode_marks_header(monkeypatch):
    """The no-news fallback HTML must carry [TEST] and the TEST RUN banner in test mode."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    from delivery_engine import _generate_no_news_email
    html = _generate_no_news_email()
    assert "[TEST]" in html
    assert "TEST RUN" in html
```

- [ ] **Step 3.2: Run the test and verify it fails**

Run: `pytest tests/test_pipeline.py::test_no_news_email_test_mode_marks_header -v`

Expected: FAIL — the no-news template does not consult `_is_test_mode()`.

- [ ] **Step 3.3: Apply markings to `_generate_no_news_email()`**

In `delivery_engine.py`, find the `_generate_no_news_email` function starting at line 846. Replace the whole function body with:

```python
def _generate_no_news_email() -> str:
    today_str = datetime.now().strftime("%A, %B %d, %Y")
    title_prefix = "[TEST] " if _is_test_mode() else ""
    test_banner_row = (
        '<tr><td style="background-color:#D97706;padding:8px 32px;font-size:11px;'
        'font-weight:700;letter-spacing:1.5px;color:#ffffff;'
        'font-family:Arial,sans-serif;text-transform:uppercase;">'
        'TEST RUN · Jason-only QA output — not for distribution'
        '</td></tr>'
        if _is_test_mode() else ""
    )
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>Americhem Market-Pulse</title></head>
<body style="margin:0;padding:0;background-color:#F3F4F6;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#F3F4F6;padding:24px 0;">
    <tr><td align="center">
      <table width="640" cellpadding="0" cellspacing="0" border="0" style="max-width:640px;background-color:#ffffff;border:0.5px solid #E5E7EB;border-radius:8px;overflow:hidden;">
        <tr><td>
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr><td style="background-color:{_BRAND_NAVY};padding:20px 32px 18px;">
              <p style="margin:0;font-size:18px;font-weight:700;color:#ffffff;font-family:Arial,sans-serif;">{title_prefix}Market-Pulse: Daily Intelligence</p>
              <p style="margin:4px 0 0 0;font-size:12px;color:rgba(255,255,255,0.6);font-family:Arial,sans-serif;">{today_str}</p>
            </td></tr>
            <tr><td style="background-color:{_BRAND_GREEN};height:3px;font-size:0;line-height:0;">&nbsp;</td></tr>
            {test_banner_row}
          </table>
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr><td style="padding:32px;">
              <p style="margin:0;font-size:15px;color:#374151;font-family:Georgia,'Times New Roman',serif;line-height:1.65;">No significant market events were detected in today's monitoring window.</p>
            </td></tr>
          </table>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>"""
```

- [ ] **Step 3.4: Run the test and verify it passes**

Run: `pytest tests/test_pipeline.py::test_no_news_email_test_mode_marks_header -v`

Expected: PASS.

- [ ] **Step 3.5: Run the full suite to catch regressions**

Run: `pytest tests/`

Expected: All tests pass.

- [ ] **Step 3.6: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git -c commit.gpgsign=false commit -m "feat(delivery): mark no-news fallback email with [TEST] and TEST RUN banner

_generate_no_news_email() now applies the same MARKET_PULSE_RUN_MODE
markings as the main email, so a test-mode no-news run cannot be
mistaken for a production no-news report."
```

---

## Task 4: Create the test workflow YAML

This task creates `.github/workflows/market_pulse_test.yml`. There is no pytest test for the workflow itself; correctness is verified via the verification checklist after merge.

**Files:**
- Create: `.github/workflows/market_pulse_test.yml`

- [ ] **Step 4.1: Create the workflow file**

Write the following exact content to `.github/workflows/market_pulse_test.yml`:

```yaml
name: Market Pulse Test Pipeline

run-name: >-
  Test Pipeline - ingest=${{ github.event.inputs.run_ingestion }} - email=${{ github.event.inputs.send_email }} - ${{ github.actor }}

on:
  workflow_dispatch:
    inputs:
      run_ingestion:
        description: "Run ingestion before delivery? (true writes new rows to Supabase — only set true when you intentionally want to test ingestion)"
        required: true
        default: "true"
        type: choice
        options:
          - "true"
          - "false"
      send_email:
        description: "Send test email after generation?"
        required: true
        default: "true"
        type: choice
        options:
          - "true"
          - "false"

permissions:
  contents: read

concurrency:
  group: market-pulse-test-pipeline
  cancel-in-progress: false

env:
  FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true

jobs:
  run-market-pulse-test:
    runs-on: ubuntu-latest
    timeout-minutes: 15

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Validate inputs and required secrets
        env:
          TEST_RECIPIENT_EMAILS: ${{ secrets.TEST_RECIPIENT_EMAILS }}
          RUN_INGESTION: ${{ github.event.inputs.run_ingestion }}
          SEND_EMAIL: ${{ github.event.inputs.send_email }}
        run: |
          if [ -z "$TEST_RECIPIENT_EMAILS" ]; then
            echo "::error::TEST_RECIPIENT_EMAILS secret is not set. Add it under Settings -> Secrets and variables -> Actions."
            exit 1
          fi
          if [ "$RUN_INGESTION" = "false" ] && [ "$SEND_EMAIL" = "false" ]; then
            echo "::error::run_ingestion=false and send_email=false is a no-op. Set at least one to true."
            exit 1
          fi

      - name: Setup Python 3.10
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      - name: Install requirements.txt
        run: pip install -r requirements.txt

      - name: Run pytest tests/test_pipeline.py
        env:
          FIRECRAWL_API_KEY: test_firecrawl_key
          OPENAI_API_KEY: test_openai_key
          SUPABASE_URL: https://example.supabase.co
          SUPABASE_KEY: test_supabase_key
          SERPER_API_KEY: test_serper_key
          SMTP_PASS: test_resend_key
          SENDER_EMAIL: test@example.com
          RECIPIENT_EMAILS: jphifer@americhem.com
        run: pytest tests/test_pipeline.py

      - name: Run ingestion_engine.py
        if: ${{ github.event.inputs.run_ingestion == 'true' }}
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

- [ ] **Step 4.2: Verify the YAML parses cleanly**

Run: `python -c "import yaml; yaml.safe_load(open('.github/workflows/market_pulse_test.yml'))"`

Expected: no output (clean parse). Any error means a YAML syntax mistake.

- [ ] **Step 4.3: Verify the production workflow file is unchanged**

Run: `git diff .github/workflows/market_pulse.yml`

Expected: empty diff. The production workflow must not have been modified.

- [ ] **Step 4.4: Commit**

```bash
git add .github/workflows/market_pulse_test.yml
git -c commit.gpgsign=false commit -m "feat(actions): add Jason-only test pipeline workflow

New workflow_dispatch workflow runs the full pipeline against
TEST_RECIPIENT_EMAILS with MARKET_PULSE_RUN_MODE=test. Inputs gate
ingestion and delivery independently; a preflight step validates the
recipient secret and rejects no-op input combinations. Concurrency
group serialises overlapping dispatches. Production workflow is
unchanged."
```

---

## Task 5: Full-pipeline regression check and final verification

Confirm the full test suite still passes and run the spec's verification checklist against the changes that can be validated pre-merge.

**Files:** none modified.

- [ ] **Step 5.1: Run the full pytest suite one last time**

Run: `pytest tests/ -v`

Expected: all tests pass. Confirm the 6 new tests from this plan appear in the output with PASS status:

- `test_send_email_test_mode_prefixes_subject`
- `test_send_email_production_mode_subject_unchanged`
- `test_send_email_recipient_list_is_only_recipient_emails_env`
- `test_generate_html_email_test_mode_prefixes_header`
- `test_generate_html_email_production_mode_unchanged`
- `test_no_news_email_test_mode_marks_header`

- [ ] **Step 5.2: Verify the production workflow diff is empty**

Run: `git diff main -- .github/workflows/market_pulse.yml`

Expected: empty diff.

- [ ] **Step 5.3: Inspect the test workflow for the recipient-isolation invariant**

Run: `grep -n "RECIPIENT_EMAILS:" .github/workflows/market_pulse_test.yml`

Expected output (showing only `TEST_RECIPIENT_EMAILS` or the dummy `jphifer@americhem.com` value, never `${{ secrets.RECIPIENT_EMAILS }}`):

```
<lineno>:          RECIPIENT_EMAILS: jphifer@americhem.com
<lineno>:          RECIPIENT_EMAILS: ${{ secrets.TEST_RECIPIENT_EMAILS }}
<lineno>:          RECIPIENT_EMAILS: ${{ secrets.TEST_RECIPIENT_EMAILS }}
```

Confirm: no occurrence of `secrets.RECIPIENT_EMAILS` (the production secret) anywhere in this file.

- [ ] **Step 5.4: Print a final summary**

Run: `git log --oneline main..HEAD`

Expected: four feature commits from Tasks 1–4 above (plus any spec/plan commits from earlier).

- [ ] **Step 5.5: Print the operator handoff reminder**

Print to console (this is informational for the user — no command needed):

```
Operator action required after merge:
  1. Add a GitHub repo secret named TEST_RECIPIENT_EMAILS with value jphifer@americhem.com
     (Settings → Secrets and variables → Actions → New repository secret)
  2. Dispatch "Market Pulse Test Pipeline" from the Actions tab and confirm:
     - Email arrives at jphifer@americhem.com with [TEST] in subject
     - TEST RUN banner is visible below the header
     - No email is sent to the production recipient list
```

---

## Notes for the engineer

- **TDD discipline:** every test in this plan is written and run before its corresponding implementation. If a test passes before the implementation is added, stop and figure out why — it usually means the test doesn't actually exercise the new behaviour.
- **Test helper aliases:** the tests use `_email_env`, `_send_email`, `_time`, and `_requests` aliases that already exist in `tests/test_pipeline.py` (imported around line 416–420). If you find those aliases have been refactored, follow the current convention rather than reintroducing the old names.
- **`generate_html_email()` template literal:** the function returns a huge f-string starting at line 764. Both new locals (`title_prefix`, `test_banner_row`) must be defined **before** the `return f"""..."""` statement, not embedded inside it. The f-string then interpolates them like any other local.
- **Banner colour:** `#D97706` is amber-600 from Tailwind, chosen to read as "test/warning" without being alarming. It matches the existing `_sentiment_word()` "Cautionary" colour, which is intentional — the visual language stays internally consistent.
- **Production safety check after every task:** `git diff main -- .github/workflows/market_pulse.yml` should always return an empty diff. Run it whenever you feel uncertain.
- **Why no `_is_test_mode()` unit test:** the helper is exercised end-to-end by every other test in the plan via environment variables. A standalone parametrized unit test would be redundant — the integration tests catch any change in its behaviour.
