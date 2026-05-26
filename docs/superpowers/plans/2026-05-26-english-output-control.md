# English Output Control Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Force every LLM-generated human-readable string in the Market-Pulse email to be written in English, regardless of source-article language.

**Architecture:** Phase 1 prompt-contract control. Introduce a single `_ENGLISH_OUTPUT_RULE` string constant in each of `ingestion_engine.py` and `delivery_engine.py`. Wire it into the three LLM call sites: `synthesize_insight()` (as RULE 0 of the existing numbered system prompt), `generate_macro_summary()` (prepended to the system prompt), and `synthesize_thematic_paragraphs()` (prepended to the system prompt). Four prompt-contract unit tests assert both anchor substrings (`"business English"` and `"regardless of the source article"`) survive in every assembled prompt.

**Tech Stack:** Python 3, OpenAI SDK (`openai`), `pytest`, `unittest.mock`.

**Spec:** `docs/superpowers/specs/2026-05-26-english-output-control-design.md`

---

## File Structure

**Modify:**

- `ingestion_engine.py` — add `_ENGLISH_OUTPUT_RULE` constant; insert `{english_output_rule}` placeholder into `_SYSTEM_PROMPT_BASE` (as RULE 0 framing) and extend `_build_system_prompt()` to substitute it; prepend rule to `generate_macro_summary()` system prompt.
- `delivery_engine.py` — add a local `_ENGLISH_OUTPUT_RULE` constant (duplicate body; cross-reference in comment); prepend it to `synthesize_thematic_paragraphs()` system prompt.
- `tests/test_pipeline.py` — append four prompt-contract tests (a new "Section 7: English-output rule" block at the end of the file).

**No new files.** No DB migration. No config changes. No backfill of existing rows.

---

### Task 1: Define `_ENGLISH_OUTPUT_RULE` and wire it into `_SYSTEM_PROMPT_BASE`

**Files:**

- Modify: `ingestion_engine.py` (add constant near top of module; edit `_SYSTEM_PROMPT_BASE` lines 389–470; edit `_build_system_prompt()` around line 473)
- Test: `tests/test_pipeline.py` (append new test at end of file)

- [ ] **Step 1: Write the failing test**

Open `tests/test_pipeline.py` and append at the very end of the file:

```python
# ---------------------------------------------------------------------------
# 7. English-output rule — prompt-contract tests
# ---------------------------------------------------------------------------

_ENGLISH_ANCHORS = ("business English", "regardless of the source article")


def _assert_english_anchors_present(prompt_text: str) -> None:
    for anchor in _ENGLISH_ANCHORS:
        assert anchor in prompt_text, (
            f"Expected English-output anchor {anchor!r} in prompt, but it was missing.\n"
            f"Prompt:\n{prompt_text}"
        )


def test_ingestion_system_prompt_contains_english_rule():
    """RULE 0 — OUTPUT LANGUAGE must be present in the assembled ingestion prompt."""
    from ingestion_engine import _build_system_prompt, _load_mp_config

    prompt = _build_system_prompt(_load_mp_config())
    _assert_english_anchors_present(prompt)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_pipeline.py::test_ingestion_system_prompt_contains_english_rule -v`

Expected: FAIL with `AssertionError: Expected English-output anchor 'business English' in prompt, but it was missing.`

- [ ] **Step 3: Add the `_ENGLISH_OUTPUT_RULE` constant**

In `ingestion_engine.py`, add this constant immediately above the `_SYSTEM_PROMPT_BASE` definition (currently at line 389). Insert before the line `_SYSTEM_PROMPT_BASE = """You are an expert market intelligence analyst...`:

```python
# Cross-reference: an identical-body constant lives in delivery_engine.py.
# Both prompts are gated in CI by tests that assert the same anchor substrings;
# if you reword this, reword the delivery_engine.py copy in lockstep.
_ENGLISH_OUTPUT_RULE = (
    "All human-readable generated strings must be written in clear business English, "
    "regardless of the source article's language. Translate non-English source "
    "content into English. Preserve proper nouns — company names, product names, "
    "brand names, source publications, locations, URLs, and quoted legal or product "
    "identifiers — in their original form when translation would reduce precision. "
    "Enum/taxonomy fields must use the configured English labels exactly."
)
```

- [ ] **Step 4: Insert a `{english_output_rule}` placeholder as RULE 0 in `_SYSTEM_PROMPT_BASE`**

In `ingestion_engine.py`, locate `_SYSTEM_PROMPT_BASE` (begins with `"""You are an expert market intelligence analyst...`). Find this block:

```
Your job is to analyze news articles and extract structured intelligence. You MUST enforce all
seven rules below before generating any output.

RULE 1 — ENTITY DISAMBIGUATION:
```

Change `all\nseven rules` to `all\neight rules` and insert a new `RULE 0` block between the preamble and `RULE 1`. The resulting region must read:

```
Your job is to analyze news articles and extract structured intelligence. You MUST enforce all
eight rules below before generating any output.

RULE 0 — OUTPUT LANGUAGE:
{english_output_rule}

RULE 1 — ENTITY DISAMBIGUATION:
```

- [ ] **Step 5: Extend `_build_system_prompt()` to substitute the placeholder**

In `ingestion_engine.py`, replace the body of `_build_system_prompt()` (currently lines 473–477). Find:

```python
def _build_system_prompt(config: dict) -> str:
    """Assemble the full system prompt, injecting commercial segment and signal type taxonomies."""
    rule4 = _build_commercial_segment_rule(config)
    rule5 = _build_signal_type_rule(config)
    return _SYSTEM_PROMPT_BASE.replace("{rule4}", rule4).replace("{rule5}", rule5)
```

Replace with:

```python
def _build_system_prompt(config: dict) -> str:
    """Assemble the full system prompt, injecting commercial segment and signal type taxonomies."""
    rule4 = _build_commercial_segment_rule(config)
    rule5 = _build_signal_type_rule(config)
    return (
        _SYSTEM_PROMPT_BASE
        .replace("{english_output_rule}", _ENGLISH_OUTPUT_RULE)
        .replace("{rule4}", rule4)
        .replace("{rule5}", rule5)
    )
```

- [ ] **Step 6: Run test to verify it passes**

Run: `pytest tests/test_pipeline.py::test_ingestion_system_prompt_contains_english_rule -v`

Expected: PASS.

- [ ] **Step 7: Run the full suite to confirm no regressions**

Run: `pytest tests/ -q`

Expected: all existing tests pass alongside the new one.

- [ ] **Step 8: Commit**

```bash
git add ingestion_engine.py tests/test_pipeline.py
git commit -m "feat(ingestion): add English-output RULE 0 to synthesize_insight prompt"
```

---

### Task 2: Prepend the rule to `generate_macro_summary()`

**Files:**

- Modify: `ingestion_engine.py` (edit `generate_macro_summary()` system prompt around lines 681–698)
- Test: `tests/test_pipeline.py` (append second prompt-contract test)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_pipeline.py`, after the test added in Task 1:

```python
def test_macro_summary_system_prompt_contains_english_rule():
    """The macro-summary system prompt must include the English-output directive."""
    from ingestion_engine import generate_macro_summary

    mock_message = MagicMock()
    mock_message.content = json.dumps(
        {
            "dominant_condition": "Mixed / Watch",
            "executive_bullets": [
                {"label": "Market pressure", "body": "Stub bullet body."},
                {"label": "Supply chain watch", "body": "Stub bullet body."},
                {"label": "Commercial action", "body": "Stub bullet body."},
            ],
        }
    )
    mock_choice = MagicMock()
    mock_choice.message = mock_message
    mock_completion = MagicMock()
    mock_completion.choices = [mock_choice]
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_completion

    mock_supabase = MagicMock()
    mock_supabase.table.return_value.upsert.return_value.execute.return_value = MagicMock()

    with patch("ingestion_engine._get_openai", return_value=mock_client), patch(
        "ingestion_engine._get_supabase", return_value=mock_supabase
    ):
        generate_macro_summary(
            [
                {
                    "category": "competitors",
                    "headline": "Stub headline",
                    "sentiment_score": 5,
                    "americhem_impact": "Stub impact.",
                }
            ]
        )

    _, kwargs = mock_client.chat.completions.create.call_args
    system_message = kwargs["messages"][0]["content"]
    _assert_english_anchors_present(system_message)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_pipeline.py::test_macro_summary_system_prompt_contains_english_rule -v`

Expected: FAIL with anchor-substring `AssertionError`.

- [ ] **Step 3: Prepend `_ENGLISH_OUTPUT_RULE` to the macro-summary system prompt**

In `ingestion_engine.py`, locate the `system_prompt = (` assignment inside `generate_macro_summary()` (currently line 681). Find:

```python
    system_prompt = (
        "You are a senior Americhem commercial intelligence analyst writing the morning brief\n"
        "for GMMs and Sales leaders. Output ONLY a JSON object with two keys.\n\n"
        "1. dominant_condition — pick exactly one value from this list that best describes\n"
        "   today's overall commercial weather across the digest:\n"
        f"     {macro_conditions_text}\n\n"
        "2. executive_bullets — exactly three objects, in this order, with these exact labels:\n"
        f'     {{"label": "{label_a}",    "body": "<one sentence, <=30 words>"}}\n'
        f'     {{"label": "{label_b}", "body": "<one sentence, <=30 words>"}}\n'
        f'     {{"label": "{label_c}",  "body": "<one sentence, <=30 words>"}}\n\n'
        '   Each body must reference specific named entities or segments from the digest.\n'
        '   Do NOT hedge ("may", "could", "potentially") without a specific data point.\n'
        '   Do NOT write generic statements ("monitor closely", "remain vigilant").\n\n'
        '   Low-signal special case:\n'
        '   If dominant_condition is "Low Signal", the Commercial action body MUST be the\n'
        '   literal string "No action required." The other two bullets MUST describe the\n'
        '   absence of meaningful signal.'
    )
```

Replace with:

```python
    system_prompt = (
        f"OUTPUT LANGUAGE:\n{_ENGLISH_OUTPUT_RULE}\n\n"
        "You are a senior Americhem commercial intelligence analyst writing the morning brief\n"
        "for GMMs and Sales leaders. Output ONLY a JSON object with two keys.\n\n"
        "1. dominant_condition — pick exactly one value from this list that best describes\n"
        "   today's overall commercial weather across the digest:\n"
        f"     {macro_conditions_text}\n\n"
        "2. executive_bullets — exactly three objects, in this order, with these exact labels:\n"
        f'     {{"label": "{label_a}",    "body": "<one sentence, <=30 words>"}}\n'
        f'     {{"label": "{label_b}", "body": "<one sentence, <=30 words>"}}\n'
        f'     {{"label": "{label_c}",  "body": "<one sentence, <=30 words>"}}\n\n'
        '   Each body must reference specific named entities or segments from the digest.\n'
        '   Do NOT hedge ("may", "could", "potentially") without a specific data point.\n'
        '   Do NOT write generic statements ("monitor closely", "remain vigilant").\n\n'
        '   Low-signal special case:\n'
        '   If dominant_condition is "Low Signal", the Commercial action body MUST be the\n'
        '   literal string "No action required." The other two bullets MUST describe the\n'
        '   absence of meaningful signal.'
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_pipeline.py::test_macro_summary_system_prompt_contains_english_rule -v`

Expected: PASS.

- [ ] **Step 5: Run the full suite to confirm no regressions**

Run: `pytest tests/ -q`

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add ingestion_engine.py tests/test_pipeline.py
git commit -m "feat(ingestion): add English-output rule to generate_macro_summary prompt"
```

---

### Task 3: Add `_ENGLISH_OUTPUT_RULE` in `delivery_engine.py` and wire it into `synthesize_thematic_paragraphs()`

**Files:**

- Modify: `delivery_engine.py` (add constant near top of module; edit `synthesize_thematic_paragraphs()` around lines 644–654)
- Test: `tests/test_pipeline.py` (append third prompt-contract test)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_pipeline.py`, after the test added in Task 2:

```python
def test_thematic_synthesis_system_prompt_contains_english_rule():
    """The thematic-synthesis system prompt must include the English-output directive."""
    mock_message = MagicMock()
    mock_message.content = json.dumps({"Healthcare": "Stub paragraph."})
    mock_choice = MagicMock()
    mock_choice.message = mock_message
    mock_completion = MagicMock()
    mock_completion.choices = [mock_choice]
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_completion

    stub_article = {
        "headline": "Stub headline",
        "sentiment_tag": "Neutral",
        "americhem_impact": "Stub impact.",
        "entities_mentioned": ["Stub Co."],
        "americhem_impact_score": 7,
    }

    with patch("delivery_engine._get_openai", return_value=mock_client):
        synthesize_thematic_paragraphs({"Healthcare": [stub_article, stub_article]})

    _, kwargs = mock_client.chat.completions.create.call_args
    system_message = kwargs["messages"][0]["content"]
    _assert_english_anchors_present(system_message)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_pipeline.py::test_thematic_synthesis_system_prompt_contains_english_rule -v`

Expected: FAIL with anchor-substring `AssertionError`.

- [ ] **Step 3: Add the `_ENGLISH_OUTPUT_RULE` constant in `delivery_engine.py`**

Open `delivery_engine.py`. Find a sensible module-level location — directly above the `synthesize_thematic_paragraphs` function definition (currently at line 614) is appropriate. Insert immediately before `def synthesize_thematic_paragraphs(`:

```python
# Cross-reference: an identical-body constant lives in ingestion_engine.py.
# Both prompts are gated in CI by tests that assert the same anchor substrings;
# if you reword this, reword the ingestion_engine.py copy in lockstep.
_ENGLISH_OUTPUT_RULE = (
    "All human-readable generated strings must be written in clear business English, "
    "regardless of the source article's language. Translate non-English source "
    "content into English. Preserve proper nouns — company names, product names, "
    "brand names, source publications, locations, URLs, and quoted legal or product "
    "identifiers — in their original form when translation would reduce precision. "
    "Enum/taxonomy fields must use the configured English labels exactly."
)


```

(Note the trailing blank line — keep one-line spacing before the `def`.)

- [ ] **Step 4: Prepend the rule to the synthesis system prompt**

In `delivery_engine.py`, locate the `system_prompt = (` assignment inside `synthesize_thematic_paragraphs()` (currently line 644). Find:

```python
    system_prompt = (
        "You are a market intelligence analyst for Americhem, a specialty plastics compounder.\n\n"
        "For each CATEGORY block below, write exactly one synthesis paragraph (2–3 sentences).\n"
        "The paragraph must:\n"
        "- Identify the shared trend or structural driver across the listed signals\n"
        "- Explicitly state the implication for Americhem's supply chain, demand pipeline, or margin\n"
        "- Be written for a senior executive who will act on it — no hedging, no filler\n\n"
        "Return valid JSON with category names as keys and synthesis paragraphs as values.\n"
        "Use the exact category names provided. Do not invent categories.\n"
        "Only include categories that appear in the input."
    )
```

Replace with:

```python
    system_prompt = (
        f"OUTPUT LANGUAGE:\n{_ENGLISH_OUTPUT_RULE}\n\n"
        "You are a market intelligence analyst for Americhem, a specialty plastics compounder.\n\n"
        "For each CATEGORY block below, write exactly one synthesis paragraph (2–3 sentences).\n"
        "The paragraph must:\n"
        "- Identify the shared trend or structural driver across the listed signals\n"
        "- Explicitly state the implication for Americhem's supply chain, demand pipeline, or margin\n"
        "- Be written for a senior executive who will act on it — no hedging, no filler\n\n"
        "Return valid JSON with category names as keys and synthesis paragraphs as values.\n"
        "Use the exact category names provided. Do not invent categories.\n"
        "Only include categories that appear in the input."
    )
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_pipeline.py::test_thematic_synthesis_system_prompt_contains_english_rule -v`

Expected: PASS.

- [ ] **Step 6: Run the full suite to confirm no regressions**

Run: `pytest tests/ -q`

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat(delivery): add English-output rule to synthesize_thematic_paragraphs prompt"
```

---

### Task 4: Regression test — non-English article body reaches `synthesize_insight()` with English directive intact

This is the fourth test specified in the spec. It is distinct from Task 1's prompt-builder unit test: it goes through the full `synthesize_insight()` call path with a real Chinese article body, and confirms (a) the directive is present in the system message actually sent to OpenAI, and (b) the source-language article body is forwarded verbatim in the user message (no premature client-side translation, since the spec explicitly excludes a separate translation pass).

**Files:**

- Test: `tests/test_pipeline.py` (append fourth prompt-contract test)
- No production code change.

- [ ] **Step 1: Write the failing test**

(It will pass on first run because Task 1 already wired the rule into `_SYSTEM_PROMPT_BASE`. We write it anyway to lock the regression behavior.)

Append to `tests/test_pipeline.py`, after the test added in Task 3:

```python
def test_synthesize_insight_non_english_body_keeps_english_directive():
    """Regression: a Chinese article body must reach synthesize_insight with the
    English-output directive intact in the system prompt, and the source-language
    body must be forwarded verbatim in the user prompt (no client-side translation)."""
    chinese_body = "中文测试文本 — Teknor Apex 推出含 70% PCR 的 Crealen R PP 汽车内饰再生材料。"

    mock_client = _make_openai_mock(5)
    with patch("ingestion_engine._get_openai", return_value=mock_client):
        result = synthesize_insight(
            article_text=chinese_body,
            source_url="https://example.cn/article",
            trigger_entity="Teknor Apex",
            category="competitors",
        )

    assert result is not None
    _, kwargs = mock_client.chat.completions.create.call_args
    system_message = kwargs["messages"][0]["content"]
    user_message = kwargs["messages"][1]["content"]
    _assert_english_anchors_present(system_message)
    assert chinese_body in user_message, (
        "Source-language article body should be forwarded verbatim to the LLM; "
        "no client-side translation should occur."
    )
```

- [ ] **Step 2: Run test to verify it passes**

Run: `pytest tests/test_pipeline.py::test_synthesize_insight_non_english_body_keeps_english_directive -v`

Expected: PASS (Task 1 already wired the rule into `_SYSTEM_PROMPT_BASE`, and `synthesize_insight()` forwards `article_text` verbatim in its user prompt — see `ingestion_engine.py:512–515`).

If this test fails, do **not** weaken the assertion. Investigate why Task 1's change isn't visible — most likely a stale `_SYSTEM_PROMPT_BASE` definition.

- [ ] **Step 3: Run the full suite — final verification**

Run: `pytest tests/ -q`

Expected: all tests pass. No skips, no warnings introduced by these changes.

- [ ] **Step 4: Commit**

```bash
git add tests/test_pipeline.py
git commit -m "test(ingestion): regression test for non-English body + English directive"
```

---

## Manual verification (post-merge)

Per the spec, delivery rerender does **not** validate this change because delivery reads stored `headline` / `americhem_impact` fields — it never re-synthesizes them.

After merging:

1. Wait for the next production run that naturally surfaces a non-English source article (Chinese trade press, Japanese chemical news, etc.), or
2. Trigger `market_pulse_test.yml` with `run_ingestion=true` on a day when the discovery query is expected to return at least one non-English article, and inspect the resulting `daily_intelligence` row(s) for English `headline` / `americhem_impact`.

If a non-English string appears in either field after this change is live, that is the trigger to escalate to a Phase 2 deterministic validator (out of scope for this PR).

---

## Out of scope (reminder)

- Backfilling existing non-English rows in `daily_intelligence`. They age out within the 72-hour Monday lookback.
- Separate translation API call before synthesis.
- Language detection of source articles.
- Unicode/script rejection of generated fields.
- Discarding non-English articles at ingestion.
- Displaying original-language headlines alongside English translations.
