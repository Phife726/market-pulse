# Plan: Email Intelligence Upgrade — Scoring, Actionability, Sentiment & Logo
Date: 2026-03-17
Spec: `docs/superpowers/specs/2026-03-17-email-intelligence-upgrade-design.md`

## Goal
Fix flat 5/10 scoring, add per-card sentiment labels and recommended actions, and make the Americhem logo render white on the dark header.

## Architecture
Four surgical changes across three files. No new modules. DB migration must run before code deployment.

## Tech Stack
- Python 3.10, OpenAI gpt-4o-mini, Supabase (PostgreSQL), smtplib, pytest

## File Map

| File | Change |
|---|---|
| `schema.sql` | Add `recommended_action TEXT` column definition |
| `ingestion_engine.py` | Rewrite Rules 2/3/4 in `_SYSTEM_PROMPT`; add `recommended_action` validation + upsert in `synthesize_insight()` and `execute_pipeline()` |
| `delivery_engine.py` | Add `_sentiment_word()` helper; update `_render_card()` with sentiment badge + action line; fix logo filter |
| `tests/test_pipeline.py` | Add `test_recommended_action_default` test |

---

## Pre-flight: Run Supabase Migration

**Before any code changes**, run this SQL in the Supabase dashboard SQL editor:

```sql
ALTER TABLE daily_intelligence
  ADD COLUMN IF NOT EXISTS recommended_action TEXT;
```

Confirm the column appears in the `daily_intelligence` table. The `todays_intelligence` view requires no changes (it selects `*`).

Also update `schema.sql` in the repo to keep it in sync (done in Task 1).

---

## Task 1 — Update `schema.sql` to include `recommended_action`

**File:** `schema.sql`

`sentiment_rationale text` already exists at line 16. Insert `recommended_action text,` between line 16 and line 17 (`raw_content text`):

```sql
-- before (lines 16–17):
    sentiment_rationale text,
    raw_content text

-- after:
    sentiment_rationale text,
    recommended_action text,
    raw_content text
```

Commit:
```bash
git add schema.sql
git commit -m "feat(schema): add recommended_action column to daily_intelligence"
```

---

## Task 2 — Write failing test for `recommended_action` default

**File:** `tests/test_pipeline.py`

Add this test after the existing `test_discard_signal_detected` test:

```python
# ---------------------------------------------------------------------------
# 7. recommended_action soft default
# ---------------------------------------------------------------------------

def _make_openai_mock_no_action(sentiment_score: int) -> MagicMock:
    """Return a mock OpenAI client whose response omits recommended_action."""
    content = json.dumps(
        {
            "headline": "Test Headline",
            "americhem_impact": "Test impact on Americhem.",
            "sentiment_score": sentiment_score,
            "source_url": "https://news.com/article",
            "entities_mentioned": ["Avient"],
        }
    )
    mock_message = MagicMock()
    mock_message.content = content
    mock_choice = MagicMock()
    mock_choice.message = mock_message
    mock_completion = MagicMock()
    mock_completion.choices = [mock_choice]
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_completion
    return mock_client


def _make_openai_mock_invalid_action(sentiment_score: int) -> MagicMock:
    """Return a mock OpenAI client whose response has an invalid recommended_action."""
    content = json.dumps(
        {
            "headline": "Test Headline",
            "americhem_impact": "Test impact on Americhem.",
            "sentiment_score": sentiment_score,
            "recommended_action": "Do something weird",
            "source_url": "https://news.com/article",
            "entities_mentioned": ["Avient"],
        }
    )
    mock_message = MagicMock()
    mock_message.content = content
    mock_choice = MagicMock()
    mock_choice.message = mock_message
    mock_completion = MagicMock()
    mock_completion.choices = [mock_choice]
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_completion
    return mock_client


@pytest.mark.parametrize("mock_fn", [_make_openai_mock_no_action, _make_openai_mock_invalid_action])
def test_recommended_action_default(mock_fn):
    """Missing or invalid recommended_action must soft-default to 'Monitor', not discard the article."""
    with patch("ingestion_engine._get_openai", return_value=mock_fn(5)):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )

    assert result is not None, "synthesize_insight must not return None for missing recommended_action"
    assert result["recommended_action"] == "Monitor"
```

Run test — confirm it **fails** (function doesn't set `recommended_action` yet):
```bash
pytest tests/test_pipeline.py::test_recommended_action_default -v
# Expected: FAILED
```

---

## Task 3 — Implement `recommended_action` validation in `synthesize_insight()`

**File:** `ingestion_engine.py`

**Step 1:** Add `_VALID_ACTIONS` as a module-level constant, directly after the `_SYSTEM_PROMPT` string (after the `}"""` closing line, before the `def synthesize_insight` line):

```python
_VALID_ACTIONS: frozenset[str] = frozenset({
    "No action", "Monitor", "Flag to procurement",
    "Share with sales", "Escalate to leadership",
})
```

**Step 2:** Inside `synthesize_insight()`, after line 335 (`insight.setdefault("sentiment_rationale", "")`), add:

```python
# Validate recommended_action — soft default to "Monitor" if missing or invalid
if insight.get("recommended_action") not in _VALID_ACTIONS:
    insight["recommended_action"] = "Monitor"
```

Run test — confirm it **passes**:
```bash
pytest tests/test_pipeline.py::test_recommended_action_default -v
# Expected: PASSED
```

Run full suite — confirm no regressions:
```bash
pytest tests/test_pipeline.py -v
# Expected: all PASSED
```

Commit:
```bash
git add ingestion_engine.py tests/test_pipeline.py
git commit -m "feat(ingestion): validate recommended_action with soft Monitor default"
```

---

## Task 4 — Rewrite LLM prompt Rules 2, 3, and 4

**File:** `ingestion_engine.py`, `_SYSTEM_PROMPT` string (lines 229–251)

Replace the entire `RULE 2`, `RULE 3`, and `RULE 4` blocks with:

```python
RULE 2 — THREAT MATRIX CALIBRATION:
Anchor sentiment_score strictly to supply chain and commercial physics.
Use the full 1–10 scale. Do NOT default to 5 unless the article is genuinely neutral.

- Score 1–2: Immediate physical supply chain threat (plant fire, port strike, supplier bankruptcy, force majeure)
- Score 3:   Significant disruption risk — major price spike, force majeure warning, capacity cut >10%
- Score 4:   Negative trend with indirect Americhem exposure (demand softness, margin pressure signals)
- Score 5:   Genuinely neutral — no discernible positive or negative lean for Americhem
- Score 6:   Mild positive — market growth or innovation in Americhem's end markets
- Score 7:   Moderate positive — competitor weakness, OEM expansion, favorable regulation
- Score 8–9: Clear commercial opportunity — large feedstock price drops, competitor capacity loss
- Score 10:  Transformational opportunity — major OEM win potential or supply disruption benefiting Americhem

Alert tier mapping (read-only context — do NOT include in output):
  CRITICAL  = score 1–3  |  ROUTINE = score 4–7  |  STRATEGIC = score 8–10

RULE 3 — RIGOROUS IMPACT STATEMENT:
Always write a specific So-What for Americhem even for routine items.
Identify which business unit or cost line could be affected and in what direction.
If truly no commercial connection exists, write: "Indirect exposure only — monitor for [specific reason]."
Do NOT write "No direct impact. Monitoring required." — this phrase is banned.
Do NOT write phrases like "may increase demand" or "could affect" without citing specific data.

RULE 4 — DOMAIN RELEVANCE FIREWALL:
Americhem is a plastics and specialty chemicals manufacturer. Only DISCARD if the article has
absolutely zero connection to plastics, polymers, chemicals, materials, manufacturing,
composites, packaging, or supply chain dynamics.
Examples of noise to DISCARD: sports results, political news, celebrity stories, unrelated
financial instruments (stock tips, crypto), or general HR policy.
When relevance is uncertain, do NOT discard. Set sentiment_score to 5 and apply Rule 3.
```

Also update the JSON schema in the prompt to add `recommended_action` (replace the closing `}"""` block):

```python
{
  "headline": "<concise factual summary, max 12 words>",
  "source_publication": "<name of the publisher, e.g. Reuters, Chemical Week, Plastics News>",
  "americhem_impact": "<BLUF So What for Americhem. Apply Rule 3. Never generic.>",
  "sentiment_score": <integer 1-10 per Rule 2>,
  "sentiment_rationale": "<max 10 words explaining exactly why this score was assigned>",
  "recommended_action": "<one of: No action | Monitor | Flag to procurement | Share with sales | Escalate to leadership>",
  "source_url": "<MUST EXACTLY MATCH the URL provided in the user prompt>",
  "entities_mentioned": ["<companies, chemicals, or regions mentioned>"]
}"""
```

Run full test suite — confirm no regressions (prompt changes don't affect unit tests since OpenAI is mocked):
```bash
pytest tests/test_pipeline.py -v
# Expected: all PASSED
```

Commit:
```bash
git add ingestion_engine.py
git commit -m "feat(ingestion): recalibrate scoring prompt and add recommended_action to LLM schema"
```

---

## Task 5 — Add `recommended_action` to upsert payload in `execute_pipeline()`

**File:** `ingestion_engine.py`, payload dict (lines 509–520)

Add one line to the payload dict:

```python
payload = {
    "headline": insight["headline"],
    "americhem_impact": insight["americhem_impact"],
    "sentiment_score": insight["sentiment_score"],
    "source_url": insight["source_url"],
    "url_hash": url_hash,
    "entities_mentioned": insight["entities_mentioned"],
    "category": category,
    "trigger_entity": entity_name,
    "source_publication": insight.get("source_publication", ""),
    "sentiment_rationale": insight.get("sentiment_rationale", ""),
    "recommended_action": insight.get("recommended_action", "Monitor"),  # ← add this line
}
```

Run full test suite:
```bash
pytest tests/test_pipeline.py -v
# Expected: all PASSED
```

Commit:
```bash
git add ingestion_engine.py
git commit -m "feat(ingestion): include recommended_action in Supabase upsert payload"
```

---

## Task 6 — Add `_sentiment_word()` helper to `delivery_engine.py`

**File:** `delivery_engine.py`, add before `_render_card()` (before line 170)

```python
def _sentiment_word(score: int) -> tuple[str, str]:
    """Map a sentiment score to a (word, hex_color) pair for display.

    Scores 1–3 map to red/negative labels, consistent with the CRITICAL alert tier.
    Scores 4 maps to amber (cautionary negative trend).
    Scores 5–6 are neutral. 7–8 positive. 9–10 opportunity.

    Args:
        score: Integer sentiment score in the range 1–10.

    Returns:
        Tuple of (sentiment_word, hex_color_string).
    """
    if score <= 3:
        return ("Negative", "#DC2626")
    if score <= 4:
        return ("Cautionary", "#D97706")
    if score <= 6:
        return ("Neutral", "#6B7280")
    if score <= 8:
        return ("Positive", "#16A34A")
    return ("Opportunity", "#15803D")
```

No test needed — pure function with no external dependencies, covered implicitly by card rendering.

Commit:
```bash
git add delivery_engine.py
git commit -m "feat(delivery): add _sentiment_word() helper for score-to-label mapping"
```

---

## Task 7 — Update `_render_card()` with sentiment badge and action line

**File:** `delivery_engine.py`, `_render_card()` function (lines 170–252)

**Step 1:** Add two new local variables after line 188 (`sentiment_rationale = ...`):

```python
recommended_action  = item.get("recommended_action", "")
sentiment_word, sentiment_color = _sentiment_word(int(score) if score else 5)
```

**Step 2:** Add `action_html` block after `rationale_html` (after line 201):

```python
action_html = (
    f'<p style="margin:0 0 10px 0;padding:6px 10px;background-color:#F9FAFB;'
    f'border-left:3px solid {accent};font-size:12px;font-weight:600;'
    f'font-family:Arial,sans-serif;color:{accent};">'
    f'&#9654; ACTION: {recommended_action}</p>'
    if recommended_action and recommended_action != "No action" else ""
)
```

**Step 3:** Insert `{action_html}` in the return f-string, between `{rationale_html}` and the bottom `<table>`:

```html
                      {rationale_html}
                      {action_html}
                      <table width="100%" ...>
```

**Step 4:** Update the bottom-row score cell to include the sentiment word. Replace:

```html
                          <td align="right"
                              style="font-size:11px;color:#9CA3AF;
                                     font-family:Arial,sans-serif;">
                            Score: {score}/10
                          </td>
```

With:

```html
                          <td align="right"
                              style="font-size:11px;font-family:Arial,sans-serif;">
                            <span style="color:{sentiment_color};font-weight:600;">
                              {sentiment_word}
                            </span>
                            <span style="color:#9CA3AF;">
                              &nbsp;&#9679;&nbsp;Score: {score}/10
                            </span>
                          </td>
```

Run full test suite:
```bash
pytest tests/test_pipeline.py -v
# Expected: all PASSED
```

Commit:
```bash
git add delivery_engine.py
git commit -m "feat(delivery): add sentiment word badge and recommended action line to email cards"
```

---

## Task 8 — Fix Americhem logo CSS filter

**File:** `delivery_engine.py`, line 376–379

The logo `<img>` tag currently has a white pill applied directly on the `style` attribute (`background-color:#ffffff;padding:3px 8px;border-radius:3px`). Replace the entire `<img>` tag (lines 376–379) with:

```html
<img src="{_LOGO_URL}"
     alt="Americhem"
     width="140"
     style="display:block;height:auto;max-height:40px;filter:brightness(0) invert(1);">
```

This removes the white pill and replaces it with the CSS filter. The surrounding `<td>` at line 375 is unchanged.

Run full test suite:
```bash
pytest tests/test_pipeline.py -v
# Expected: all PASSED
```

Commit:
```bash
git add delivery_engine.py
git commit -m "fix(email): apply brightness(0) invert(1) filter to header logo for white rendering"
```

---

## Task 9 — Push and trigger manual workflow run

```bash
git push origin main
```

Then in GitHub Actions: **Actions → Market Pulse Pipeline → Run workflow**.

Verify in the Actions log:
- `Run ingestion_engine.py` completes without Supabase column errors
- `Run delivery_engine.py` completes with `[INFO] Email sent`

Check the received email:
- [ ] Scores vary across the 1–10 range (not all 5)
- [ ] Each card shows a colored sentiment word (Neutral/Cautionary/Positive etc.)
- [ ] Cards with non-"No action" recommended actions show `▶ ACTION:` line
- [ ] Impact statements are specific (no bare "No direct impact. Monitoring required.")
- [ ] Americhem header logo renders white
