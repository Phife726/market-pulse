# Design: Logo Fix & Article Summary
Date: 2026-03-17
Approach: Option A (white pill logo) + Option A (LLM-generated summary stored in DB)

## Goal
1. Make the Americhem header logo reliably visible across all email clients by reverting to a white pill background.
2. Add a concise 2–3 sentence LLM-generated article summary to each email card so readers get the "what happened" context before the "so what for Americhem" impact line.

## Architecture
Two independent, targeted changes across three files. DB migration must run before code deployment.

### Alert tier / card structure — UNCHANGED
Tier boundaries (CRITICAL/ROUTINE/STRATEGIC) and card layout are unchanged. The summary is inserted between the headline and the impact line on all cards.

---

## Section 1: Logo Fix (`delivery_engine.py`)

### Problem
`filter:brightness(0) invert(1)` requires the image to load successfully. The Americhem CDN (`americhem.com/wp-content/...`) blocks hotlinking from email clients, so the image never loads and the filter has nothing to act on.

### Fix
Revert the **header** `<img>` style to the white pill approach confirmed working in commit `4c3f6cc`.
The **footer** `<img>` tag (which uses `opacity:0.4`) is intentionally left unchanged.

```html
<img src="{_LOGO_URL}"
     alt="Americhem"
     width="140"
     style="display:block;height:auto;max-height:40px;
            background-color:#ffffff;padding:3px 8px;border-radius:3px;">
```

**Files changed:** `delivery_engine.py` — header `<img>` tag only (~line 413). Footer `<img>` (~line 484) is out of scope.
**DB migration:** none
**Tests:** none (pure style change)

---

## Section 2: Article Summary

### Pre-flight: Supabase Migration
Run in the Supabase SQL editor **before deploying code**. Both statements are required:

```sql
-- 1. Add the column
ALTER TABLE daily_intelligence ADD COLUMN IF NOT EXISTS article_summary TEXT;

-- 2. Rebuild the view to expose the new column
-- (the view uses an explicit column list, not SELECT *)
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

Existing rows get `NULL` for `article_summary`; cards with no summary render without the summary block.

---

### `schema.sql` update
Add `article_summary text,` between `recommended_action text,` and `raw_content text` in the table definition.

Update the view's SELECT list to match the migration SQL above exactly:
- Add `article_summary,` immediately after `headline,` (before `americhem_impact,`)
- Add `recommended_action,` between `sentiment_rationale,` and the `CASE` expression

Note: `recommended_action` is already in the **table** definition in `schema.sql` but was never added to the view — this is a pre-existing drift that the migration SQL above already corrects. The view column order intentionally differs from the table column order.

---

### LLM Schema (`ingestion_engine.py`)
Add `article_summary` to `_SYSTEM_PROMPT` JSON schema, between `source_publication` and `americhem_impact`:

```json
"article_summary": "<2–3 sentences, max 50 words. What happened, who is involved, key numbers. Factual only — no Americhem framing.>",
```

### `synthesize_insight()` update
After `insight.setdefault("sentiment_rationale", "")`, add:
```python
insight.setdefault("article_summary", "")
```
- Soft default — missing field returns `""`, article is never discarded
- Do NOT add to `required_keys`

### `execute_pipeline()` upsert payload
Add after the `recommended_action` line (~line 538):
```python
"article_summary": insight.get("article_summary", ""),
```

---

### Email Card (`delivery_engine.py`)

In `_render_card()`, after reading `sentiment_rationale` (~line 208), add:
```python
article_summary = item.get("article_summary", "")
```

Add `summary_html` block immediately after:
```python
summary_html = (
    f'<p style="margin:0 0 8px 0;font-size:12px;color:#6B7280;'
    f'font-family:Arial,sans-serif;line-height:1.5;">'
    f'{article_summary}</p>'
    if article_summary else ""
)
```

In the return f-string, insert `{summary_html}` after the closing `</a>` of the headline link and before the `<p style=...Americhem impact...>` impact paragraph:
```html
      {headline link closing </a>}
      {summary_html}
      <p style="margin:0 0 8px 0;font-size:13px;...">
        <strong ...>Americhem impact:</strong> ...
      </p>
```

---

### Tests (`tests/test_pipeline.py`)
Add one test after `test_recommended_action_default`:

```python
def test_article_summary_default():
    """Missing article_summary must soft-default to empty string, not discard the article."""
    with patch("ingestion_engine._get_openai", return_value=_make_openai_mock(5)):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["article_summary"] == ""
```

Note: `_make_openai_mock` already omits `article_summary` and all other optional fields, so no new mock helper is needed.

---

## File Map

| File | Change |
|---|---|
| `schema.sql` | Add `article_summary text,` to table; add `article_summary,` to view SELECT list |
| `ingestion_engine.py` | Add `article_summary` to LLM JSON schema; soft default in `synthesize_insight()`; add to upsert payload |
| `delivery_engine.py` | Revert header logo to white pill; add `summary_html` block in `_render_card()` |
| `tests/test_pipeline.py` | Add `test_article_summary_default` |

---

## Success Criteria
1. Americhem header logo is visible (white pill on navy header) in Gmail and Outlook Web
2. Every new card shows a 2–3 sentence factual summary below the headline
3. Cards with `NULL` or empty `article_summary` (old rows) render without the summary block — no empty space
4. `test_article_summary_default` passes in CI
5. No existing tests regress
