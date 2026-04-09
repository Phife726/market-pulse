# Plan: Restore Executive Summary + Remove Article Summary Display

**Goal:** Restore the `generate_macro_summary()` function in `ingestion_engine.py` (stubbed out in commit 46ed706) and remove the `article_summary` display block from email cards in `delivery_engine.py`.

**Architecture overview:**
- `ingestion_engine.py` → runs first; scrapes articles, synthesizes insights, then calls `generate_macro_summary()` to write one row to `daily_summaries` per run
- `delivery_engine.py` → runs second; reads `daily_summaries` via `fetch_macro_summary()`, renders the exec summary block at the top of the email, then renders per-article cards
- The `daily_summaries` table and all delivery-side rendering code are intact — the sole regression is the stubbed ingestion function

**Tech stack:** Python 3.12, OpenAI `gpt-4o-mini`, Supabase (Postgres), pytest

---

## File Map

| File | Change |
|------|--------|
| `ingestion_engine.py` | Restore full `generate_macro_summary()` implementation |
| `delivery_engine.py` | Remove `summary_html` variable and its `{summary_html}` placeholder in `_render_card()` |
| `tests/test_pipeline.py` | Add test for `generate_macro_summary()` (mocked OpenAI + Supabase); add test confirming `article_summary` absent from rendered card HTML |

---

## Tasks

### Task 1 — Write failing test for `generate_macro_summary()`

**File:** `tests/test_pipeline.py`

Add two tests — one for the success path (OpenAI returns valid JSON, Supabase upsert called), one for the empty-articles guard (returns `False` without calling OpenAI).

```python
from unittest.mock import patch, MagicMock

def test_generate_macro_summary_empty_articles():
    """Should return False immediately when no articles are provided."""
    from ingestion_engine import generate_macro_summary
    result = generate_macro_summary([])
    assert result is False


def test_generate_macro_summary_success():
    """Should call OpenAI, parse response, upsert to daily_summaries, return True."""
    from ingestion_engine import generate_macro_summary

    articles = [
        {
            "headline": "Polymer prices surge",
            "category": "markets",
            "sentiment_score": 2,
            "americhem_impact": "Cost pressure on compounding margins.",
        }
    ]

    mock_completion = MagicMock()
    mock_completion.choices[0].message.content = json.dumps({
        "executive_summary": "Polymer prices are surging.",
        "macro_sentiment": "Bearish",
    })

    mock_openai = MagicMock()
    mock_openai.chat.completions.create.return_value = mock_completion

    mock_supabase = MagicMock()
    mock_supabase.table.return_value.upsert.return_value.execute.return_value = MagicMock()

    with patch("ingestion_engine._get_openai", return_value=mock_openai), \
         patch("ingestion_engine._get_supabase", return_value=mock_supabase):
        result = generate_macro_summary(articles)

    assert result is True
    mock_supabase.table.assert_called_with("daily_summaries")
    call_kwargs = mock_supabase.table.return_value.upsert.call_args[0][0]
    assert call_kwargs["executive_summary"] == "Polymer prices are surging."
    assert call_kwargs["macro_sentiment"] == "Bearish"
    assert "run_date" in call_kwargs
```

**Run test — confirm it fails:**
```bash
pytest tests/test_pipeline.py::test_generate_macro_summary_empty_articles tests/test_pipeline.py::test_generate_macro_summary_success -v
```
Expected: both fail (current stub just returns `True`, not `False` for empty, and never calls Supabase).

---

### Task 2 — Restore `generate_macro_summary()` in `ingestion_engine.py`

**File:** `ingestion_engine.py` — replace lines 355–356 (the stub).

```python
def generate_macro_summary(articles: list[dict]) -> bool:
    if not articles:
        logger.warning("No articles to summarize — skipping macro summary generation.")
        return False

    client = _get_openai()

    article_digest = "\n".join(
        f"- [{a.get('category', '').upper()}] {a.get('headline', '')} "
        f"(Score {a.get('sentiment_score', '')}/10): {a.get('americhem_impact', '')}"
        for a in articles
    )

    user_prompt = (
        f"Today's market intelligence digest for Americhem ({len(articles)} articles):\n\n"
        f"{article_digest}\n\n"
        f"Generate a JSON object with exactly two keys:\n"
        f"- executive_summary: A 3-sentence macro summary of today's most important market movements "
        f"and their implications for Americhem's supply chain and commercial position.\n"
        f"- macro_sentiment: One word or short phrase describing overall market tone "
        f"(e.g. Stable, Bearish, Volatile, Cautiously Optimistic, Bullish).\n"
        f"Output ONLY the JSON object."
    )

    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You are a senior market intelligence analyst. Output only valid JSON."},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
        )
        parsed = json.loads(completion.choices[0].message.content)
        executive_summary = parsed["executive_summary"]
        macro_sentiment = parsed["macro_sentiment"]
    except Exception as exc:
        logger.error("Failed to generate macro summary from OpenAI: %s", exc)
        return False

    try:
        from datetime import date
        supabase = _get_supabase()
        supabase.table("daily_summaries").upsert(
            {
                "run_date": date.today().isoformat(),
                "executive_summary": executive_summary,
                "macro_sentiment": macro_sentiment,
            },
            on_conflict="run_date",
        ).execute()
        logger.info("Macro summary upserted — sentiment: %s", macro_sentiment)
        return True
    except Exception as exc:
        logger.error("Failed to upsert macro summary to Supabase: %s", exc)
        return False
```

**Run tests — confirm they pass:**
```bash
pytest tests/test_pipeline.py::test_generate_macro_summary_empty_articles tests/test_pipeline.py::test_generate_macro_summary_success -v
```
Expected: both pass.

**Run full suite to confirm no regressions:**
```bash
pytest tests/ -v
```
Expected: all tests pass.

**Commit:**
```bash
git add ingestion_engine.py tests/test_pipeline.py
git commit -m "fix: restore generate_macro_summary() — was stubbed out in 46ed706"
```

---

### Task 3 — Write failing test confirming `article_summary` is absent from rendered card

**File:** `tests/test_pipeline.py`

```python
def test_render_card_excludes_article_summary():
    """article_summary must not appear in rendered card HTML."""
    from delivery_engine import _render_card
    item = {
        "headline": "Test headline",
        "source_url": "https://example.com",
        "americhem_impact": "Some impact.",
        "category": "markets",
        "sentiment_score": 5,
        "article_summary": "This is the article summary text.",
    }
    html = _render_card(item, "#000000", "#ffffff", "#000000")
    assert "This is the article summary text." not in html
```

**Run test — confirm it fails:**
```bash
pytest tests/test_pipeline.py::test_render_card_excludes_article_summary -v
```
Expected: fails (current code renders `summary_html` which includes the article summary text).

---

### Task 4 — Remove `article_summary` display from `_render_card()` in `delivery_engine.py`

**File:** `delivery_engine.py`

**Step A** — Delete the `summary_html` variable block (lines 187–192):

Remove:
```python
    summary_html = (
        f'<p style="margin:0 0 8px 0;font-size:12px;color:#6B7280;'
        f'font-family:Arial,sans-serif;line-height:1.5;">'
        f'{article_summary}</p>'
        if article_summary else ""
    )
```

Also remove the `article_summary` local variable assignment on line 176:
```python
    article_summary     = item.get("article_summary", "")
```

**Step B** — Remove the `{summary_html}` placeholder from the HTML template in `_render_card()` (the line between the headline `<a>` tag and the `americhem_impact` paragraph).

Remove this line from the f-string:
```
                      {summary_html}
```

**Run test — confirm it passes:**
```bash
pytest tests/test_pipeline.py::test_render_card_excludes_article_summary -v
```
Expected: passes.

**Run full suite:**
```bash
pytest tests/ -v
```
Expected: all tests pass.

**Commit:**
```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat: remove article_summary display from email cards"
```

---

## Verification Checklist

- [ ] `pytest tests/ -v` — all tests green
- [ ] `generate_macro_summary()` in `ingestion_engine.py` is no longer a stub
- [ ] `daily_summaries` upsert call uses `on_conflict="run_date"`
- [ ] No `{summary_html}` or `article_summary` variable remains in `_render_card()`
- [ ] `article_summary` column still exists in DB schema (no schema migration needed)
