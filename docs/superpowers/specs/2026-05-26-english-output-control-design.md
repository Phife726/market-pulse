# English Output Control — Design

**Date:** 2026-05-26
**Status:** Approved (pending user review of this written spec)

## Problem

The 2026-05-26 production run rendered an email in which the Teknor Apex card's headline and "So what" were emitted in Mandarin (e.g., "Teknor Apex推出高PCR含量汽车内饰再生材料Crealen R PP"). Stakeholders cannot act on text they cannot read.

Root cause: `synthesize_insight()` in `ingestion_engine.py` feeds the scraped article body to the LLM with no instruction about output language. When the source article is non-English, the model preserves the source language for fields it generates (`headline`, `americhem_impact`, `article_summary`, etc.). The same lack of constraint exists in `generate_macro_summary()` and `synthesize_thematic_paragraphs()` — today's executive summary is English by luck, not by policy.

## Objective

Ensure every human-readable field rendered in the Market-Pulse email is generated in English, regardless of source article language.

## Approach

**Phase 1 control** — prompt contract at every LLM generation surface. No new API calls, no second pass, no language detection, no Unicode rejection.

The model is fully capable of synthesizing a 12-word English headline from a Chinese trade-press article as part of the same call that performs analysis. The change is to *instruct* it to do so.

If non-English output recurs after this change is deployed, the follow-up is a Phase 2 deterministic validator (flag generated fields with disallowed scripts before storage/delivery). Phase 2 is explicitly out of scope for this PR.

## The English-output rule

Defined once per file as a module-level constant `_ENGLISH_OUTPUT_RULE`. The constant holds the rule **body only**, without a section heading — the call site supplies its own framing (e.g., `RULE 0 — ` prefix for the numbered-rules prompt).

Body wording (identical in both `ingestion_engine.py` and `delivery_engine.py`):

```text
All human-readable generated strings must be written in clear business English,
regardless of the source article's language. Translate non-English source
content into English. Preserve proper nouns — company names, product names,
brand names, source publications, locations, URLs, and quoted legal or product
identifiers — in their original form when translation would reduce precision.
Enum/taxonomy fields must use the configured English labels exactly.
```

The substrings `"business English"` and `"regardless of the source article"` are the two anchor tokens the prompt-contract tests assert against. Neither appears elsewhere in any prompt, and the pair guards both register and source-language independence — a future rewording must preserve both phrases.

## Change surface

### 1. `ingestion_engine.py` — `synthesize_insight()`

Add `_ENGLISH_OUTPUT_RULE` as a module-level constant. Insert it as **`RULE 0 — OUTPUT LANGUAGE`** at the top of `_SYSTEM_PROMPT_BASE`, before RULE 1. Renumbering of subsequent rules is *not* needed — the existing rules can keep their numbers; RULE 0 reads naturally as a precondition.

Fields covered (all LLM-generated strings on the article record):

- `headline`
- `source_publication` (free-form — when model-derived, must use the publication's official or common name; do not translate a publisher's proper noun, even if the publication is non-English. This aligns with the proper-noun carve-out in the rule.)
- `article_summary`
- `americhem_impact`
- `impact_rationale`
- `sentiment_rationale`
- `recommended_action` (constrained enum — already validated against `_VALID_ACTIONS`; the rule's "use configured English labels exactly" clause reinforces this)
- Enum fields kept English by existing validators: `sentiment_tag`, `commercial_segment`, `signal_type`
- `entities_mentioned` — names preserved as-is (proper-noun carve-out)
- `source_url` — unaffected

### 2. `ingestion_engine.py` — `generate_macro_summary()`

Prepend `_ENGLISH_OUTPUT_RULE` to the existing `system_prompt` string. Covers:

- `executive_bullets[*].body` (three free-form sentences)
- `dominant_condition` — constrained enum from `_VALID_MACRO_CONDITIONS`, kept English by existing validator
- Legacy `executive_summary` and `macro_sentiment` columns written from the same parsed output

Inputs to this call are already-translated headlines and `americhem_impact` strings from #1, so the macro summary should be English regardless. The rule is belt-and-suspenders.

### 3. `delivery_engine.py` — `synthesize_thematic_paragraphs()`

Define a local `_ENGLISH_OUTPUT_RULE` constant (duplicate of the ingestion-side text — avoids introducing a shared prompt-helper module for a single string in a two-file pipeline). Prepend it to the existing `system_prompt`. Covers the category-paragraph values.

Inputs are stored `headline` + `americhem_impact` fields, already English under #1. The rule prevents drift if upstream changes.

## Out of scope

- Backfilling existing non-English rows in `daily_intelligence`. The two affected rows from 2026-05-26 will age out naturally within the 72-hour Monday lookback window.
- A separate translation API call before synthesis.
- Language detection of source articles.
- Unicode/script rejection of generated fields. Proper nouns may legitimately contain non-Latin characters (e.g., a Korean supplier name in the entities list); a blunt detector would create false positives.
- Discarding non-English articles at ingestion.
- Displaying the original-language headline alongside the English translation.

## Testing

Four tests added to `tests/test_pipeline.py`:

All four tests assert that **both** anchor substrings `"business English"` and `"regardless of the source article"` are present in the relevant prompt. Two anchors — one for register, one for source-language independence — guard against a future edit that keeps the phrasing but silently removes the real instruction.

1. **Ingestion prompt contains rule.** `_build_system_prompt(_load_mp_config())` returns a string containing both anchor substrings.
2. **Macro-summary prompt contains rule.** Mock the OpenAI client, invoke `generate_macro_summary()` with one stub article, inspect the `messages[0].content` payload passed to `chat.completions.create()`, assert both anchors are present.
3. **Thematic-synthesis prompt contains rule.** Mock the OpenAI client, invoke `synthesize_thematic_paragraphs({"Healthcare": [stub, stub]})`, inspect the system message, assert both anchors are present.
4. **Regression: non-English article body reaches synthesize_insight with English directive intact.** Mock the OpenAI client, call `synthesize_insight(article_text="中文测试文本 …", source_url=..., trigger_entity=..., category=...)`, inspect the system message passed to `chat.completions.create()`, assert both anchors are present and that the source-language body is forwarded unchanged in the user message.

These are prompt-contract tests, not LLM-behavior tests. We are verifying the instruction was sent. Verifying the LLM obeyed is a live-API test and outside the unit-test scope.

## Manual verification

Delivery rerender does **not** validate this change. Delivery reads stored `headline` / `americhem_impact` fields from `daily_intelligence` — it does not re-synthesize them. Running `market_pulse_test.yml` with `run_ingestion=false` against today's rows would render the same Chinese text.

Acceptable manual-verification paths:

1. Run `market_pulse_test.yml` with `run_ingestion=true` on a day when at least one non-English article is naturally discovered, and inspect the resulting `daily_intelligence` row for English `headline` / `americhem_impact`.
2. Wait for the next naturally occurring non-English article after deployment and inspect that day's email.

Option 1 is preferable if speed matters; option 2 is acceptable as passive verification given the test suite already gates the prompt contract.

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| LLM ignores the directive on a specific article. | Phase 2 validator (out of scope) is the staged response if this recurs. |
| Translation degrades technical precision (e.g., a Chinese polymer trade name). | Proper-noun carve-out in the rule wording. The model already preserves brand names like "Crealen R PP" in today's broken output, so the capability is there. |
| Prompt growth (~80 tokens × three call sites). | Negligible at current volume (<150 articles/day). |
| Duplicated rule constant across two files drifts over time. | Cross-reference in a comment above each constant; the test suite asserts both anchor substrings (`"business English"` and `"regardless of the source article"`) in all three built prompts, so a drift on either side fails CI. |
