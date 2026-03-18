# Plan: Logo Fix & Article Summary
Date: 2026-03-17
Spec: `docs/superpowers/specs/2026-03-17-logo-and-article-summary-design.md`

## Goal
Revert the Americhem header logo to the proven white-pill style, and add a 2–3 sentence LLM-generated article summary to every email card.

## Architecture
Six surgical changes across three files. Supabase migration must run before code deployment.

## Tech Stack
Python 3.10, OpenAI gpt-4o-mini, Supabase (PostgreSQL), smtplib, pytest

## File Map

| File | Change |
|---|---|
| `schema.sql` | Add `article_summary text,` to table; add `article_summary` and `recommended_action` to view SELECT list |
| `ingestion_engine.py` | Add `article_summary` to LLM JSON schema; soft default in `synthesize_insight()`; add to upsert payload |
| `delivery_engine.py` | Revert header logo to white pill; add `summary_html` block in `_render_card()` |
| `tests/test_pipeline.py` | Add `test_article_summary_default` |

---

## Pre-flight: Run Supabase Migration

**Before any code changes**, run both SQL statements in the Supabase dashboard SQL editor:

```sql
-- 1. Add the column
ALTER TABLE daily_intelligence ADD COLUMN IF NOT EXISTS article_summary TEXT;

-- 2. Rebuild the view to expose article_summary and the previously-missing recommended_action
CREATE OR REPLACE VIEW todays_intelligence AS
SELECT
    id,
    created_at,
    headline,
    article_summary,
    americhem_impact,
    sentiment_score,
    source_url,
    url_hash,
    entities_mentioned,
    category,
    trigger_entity,
    source_publication,
    sentiment_rationale,
    recommended_action,
    CASE
        WHEN sentiment_score BETWEEN 1 AND 3 THEN 'CRITICAL'
        WHEN sentiment_score BETWEEN 8 AND 10 THEN 'STRATEGIC'
        ELSE 'ROUTINE'
    END AS alert_tier
FROM daily_intelligence
WHERE created_at >= NOW() - INTERVAL '24 hours'
ORDER BY sentiment_score ASC;
```

Confirm both statements succeed before proceeding.

---

## Task 1 — Update `schema.sql`

**File:** `schema.sql`

**Step 1:** Add `article_summary text,` between `recommended_action text,` and `raw_content text` in the table definition (currently line 17–18):

```sql
-- before:
    recommended_action text,
    raw_content text

-- after:
    recommended_action text,
    article_summary text,
    raw_content text
```

**Step 2:** Update the `todays_intelligence` view to match the migration SQL exactly. Replace the current view definition (lines 46–67) with:

```sql
create or replace view todays_intelligence as
select
    id,
    created_at,
    headline,
    article_summary,
    americhem_impact,
    sentiment_score,
    source_url,
    url_hash,
    entities_mentioned,
    category,
    trigger_entity,
    source_publication,
    sentiment_rationale,
    recommended_action,
    case
        when sentiment_score between 1 and 3 then 'CRITICAL'
        when sentiment_score between 8 and 10 then 'STRATEGIC'
        else 'ROUTINE'
    end as alert_tier
from daily_intelligence
where created_at >= now() - interval '24 hours'
order by sentiment_score asc;
```

No test needed for schema.sql — verify by inspection.

Commit:
```bash
git add schema.sql
git commit -m "feat(schema): add article_summary column and fix view to include recommended_action"
```

---

## Task 2 — Write failing test for `article_summary` default

**File:** `tests/test_pipeline.py`

Add after the existing `test_recommended_action_default` parametrized test:

```python
# ---------------------------------------------------------------------------
# 8. article_summary soft default
# ---------------------------------------------------------------------------

def test_article_summary_default():
    """Missing article_summary must soft-default to empty string, not discard the article."""
    with patch("ingestion_engine._get_openai", return_value=_make_openai_mock(5)):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None, "synthesize_insight must not return None for missing article_summary"
    assert result["article_summary"] == ""
```

Note: `_make_openai_mock` already omits `article_summary` — no new mock helper needed.

Run test — confirm it **fails** (RED):
```bash
pytest tests/test_pipeline.py::test_article_summary_default -v
# Expected: FAILED (KeyError: 'article_summary')
```

**Do not commit yet** — the test + implementation are committed together in Task 3.

---

## Task 3 — Implement `article_summary` in `synthesize_insight()`

**File:** `ingestion_engine.py`

**Step 1:** Add `article_summary` to the LLM JSON schema in `_SYSTEM_PROMPT`. Insert between `source_publication` and `americhem_impact` (currently lines 264–266):

```python
# before:
  "source_publication": "<name of the publisher, e.g. Reuters, Chemical Week, Plastics News>",
  "americhem_impact": "<BLUF So What for Americhem. Apply Rule 3. Never generic.>",

# after:
  "source_publication": "<name of the publisher, e.g. Reuters, Chemical Week, Plastics News>",
  "article_summary": "<2–3 sentences, max 50 words. What happened, who is involved, key numbers. Factual only — no Americhem framing.>",
  "americhem_impact": "<BLUF So What for Americhem. Apply Rule 3. Never generic.>",
```

**Step 2:** In `synthesize_insight()`, add soft default after `insight.setdefault("sentiment_rationale", "")` (currently line 348):

```python
    insight.setdefault("source_publication", "")
    insight.setdefault("sentiment_rationale", "")
    insight.setdefault("article_summary", "")   # ← add this line
```

Run test — confirm it **passes** (GREEN):
```bash
pytest tests/test_pipeline.py::test_article_summary_default -v
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
git commit -m "feat(ingestion): add article_summary to LLM schema with soft empty-string default"
```

---

## Task 4 — Add `article_summary` to upsert payload

**File:** `ingestion_engine.py`, `execute_pipeline()` payload dict (~line 537)

Add one line after `recommended_action`:

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
                "recommended_action": insight.get("recommended_action", "Monitor"),
                "article_summary": insight.get("article_summary", ""),  # ← add this line
            }
```

Run full suite:
```bash
pytest tests/test_pipeline.py -v
# Expected: all PASSED
```

Commit:
```bash
git add ingestion_engine.py
git commit -m "feat(ingestion): include article_summary in Supabase upsert payload"
```

---

## Task 5 — Fix header logo in `delivery_engine.py`

**File:** `delivery_engine.py`, header `<img>` tag (~line 413)

Replace `filter:brightness(0) invert(1)` with the white pill style. The **footer** `<img>` (~line 484) is left unchanged.

```python
# before:
                          <img src="{_LOGO_URL}"
                               alt="Americhem"
                               width="140"
                               style="display:block;height:auto;max-height:40px;filter:brightness(0) invert(1);">

# after:
                          <img src="{_LOGO_URL}"
                               alt="Americhem"
                               width="140"
                               style="display:block;height:auto;max-height:40px;background-color:#ffffff;padding:3px 8px;border-radius:3px;">
```

Run full suite:
```bash
pytest tests/test_pipeline.py -v
# Expected: all PASSED
```

Commit:
```bash
git add delivery_engine.py
git commit -m "fix(email): revert header logo to white pill for cross-client visibility"
```

---

## Task 6 — Add `summary_html` to `_render_card()`

**File:** `delivery_engine.py`, `_render_card()` function

**Step 1 — Write failing test first.** Add to `tests/test_pipeline.py` after `test_article_summary_default`:

```python
# ---------------------------------------------------------------------------
# 9. _render_card() article_summary rendering
# ---------------------------------------------------------------------------

from delivery_engine import _render_card

def test_render_card_shows_summary():
    """_render_card must include article_summary text when the field is populated."""
    item = {
        "headline": "Test Headline",
        "source_url": "https://news.com/article",
        "americhem_impact": "Some impact.",
        "category": "competitors",
        "sentiment_score": 5,
        "source_publication": "Reuters",
        "sentiment_rationale": "Neutral article.",
        "recommended_action": "Monitor",
        "article_summary": "BASF announced a new plant in Germany. The facility will produce 50kt of polymer annually. Production starts Q1 2027.",
    }
    html = _render_card(item, accent="#1B3A6B", bg="#E8EDF5", text="#1B3A6B")
    assert "BASF announced a new plant in Germany" in html


def test_render_card_omits_summary_when_empty():
    """_render_card must not emit an empty <p> tag when article_summary is absent."""
    item = {
        "headline": "Test Headline",
        "source_url": "https://news.com/article",
        "americhem_impact": "Some impact.",
        "category": "competitors",
        "sentiment_score": 5,
        "source_publication": "Reuters",
        "sentiment_rationale": "Neutral article.",
        "recommended_action": "Monitor",
        "article_summary": "",
    }
    html = _render_card(item, accent="#1B3A6B", bg="#E8EDF5", text="#1B3A6B")
    # No empty paragraph should be present
    assert '<p style="margin:0 0 8px 0;font-size:12px;color:#6B7280' not in html
```

Run tests — confirm they **fail** (RED):
```bash
pytest tests/test_pipeline.py::test_render_card_shows_summary tests/test_pipeline.py::test_render_card_omits_summary_when_empty -v
# Expected: FAILED (article_summary key missing from _render_card)
```

**Step 2 — Implement.** After `sentiment_rationale = item.get("sentiment_rationale", "")` (~line 208), add:

```python
    article_summary     = item.get("article_summary", "")
```

**Step 3:** After the `rationale_html` block (~line 224) and before `action_html`, add:

```python
    summary_html = (
        f'<p style="margin:0 0 8px 0;font-size:12px;color:#6B7280;'
        f'font-family:Arial,sans-serif;line-height:1.5;">'
        f'{article_summary}</p>'
        if article_summary else ""
    )
```

**Step 4:** In the return f-string, insert `{summary_html}` between the headline `</a>` closing tag and the impact `<p>` tag (~line 251–252):

```html
                      </a>
                      {summary_html}
                      <p style="margin:0 0 8px 0;font-size:13px;color:#374151;
```

Run the two new tests — confirm they **pass** (GREEN):
```bash
pytest tests/test_pipeline.py::test_render_card_shows_summary tests/test_pipeline.py::test_render_card_omits_summary_when_empty -v
# Expected: PASSED
```

Run full suite — confirm no regressions:
```bash
pytest tests/test_pipeline.py -v
# Expected: all PASSED
```

Commit:
```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat(delivery): add article_summary block to email card between headline and impact"
```

---

## Task 7 — Push and trigger manual workflow run

```bash
git push origin main
```

Then in GitHub Actions: **Actions → Market Pulse Pipeline → Run workflow**.

Verify in the Actions log:
- `Run ingestion_engine.py` completes without Supabase column errors
- `Run delivery_engine.py` completes with `[INFO] Email sent`

Check the received email:
- [ ] Header logo is visible (white pill on navy header)
- [ ] New cards show a 2–3 sentence summary below the headline
- [ ] Old cards (no `article_summary` in DB) render cleanly with no empty space
