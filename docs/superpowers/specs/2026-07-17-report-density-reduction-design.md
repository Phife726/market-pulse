# Report Density Reduction — Design

**Date:** 2026-07-17
**Status:** Approved (design); pending implementation plan

## Problem

Since the pipeline deadline was raised to 30 minutes (PR #44) the daily now covers
the full ~120-target list, and the uncapped report (PR #39) has grown too long:

1. Segments can show an unbounded number of visible cards.
2. The Macroeconomic Outlook renders up to 6 signals.
3. The per-segment synthesis paragraphs (2–3 sentences each) take too much
   real estate atop each segment block.

## Goals

- Cap visible cards at **5 per segment**; capped-out articles must not vanish —
  they flow into the "Additional Articles to Explore" appendix.
- Cap Macroeconomic Outlook at **3 signals**, effective immediately even for
  already-stored `daily_summaries` rows (QA re-renders with
  `run_ingestion=false` included).
- Segment synthesis becomes **one sentence** per segment.

## Non-goals

- No total visible-card cap (`max_total_visible_articles` stays `null`).
- No new config knobs for the macro signal cap or summary length (approach B
  was considered and rejected as premature plumbing — `macro_prompt` would need
  a config parameter for a number changed once).
- No schema/migration changes; no changes to suppression reason codes.

## Design

### 1. Per-segment card cap (config-only)

`market_pulse_config.yaml`: set `reporting.max_visible_articles_per_segment: 5`.
The existing cap machinery in `report.assemble_report` (step 4) keeps each
segment's top 5 by `insight.effective_impact`, materiality-descending. Update
the adjacent config comment to note that capped-out articles now flow into the
appendix rather than being dropped.

### 2. Cap overflow flows into the appendix (`report.py`)

Widen `_is_usable_additional_article` from the weak-relevance band
(`supporting ≤ impact < visible`, i.e. 4–5) to **`impact ≥ supporting`**
(≥ 4). The caller already restricts the pool to suppression survivors not
shown as visible cards, so the appendix band becomes: *every kept row scoring
≥ 4 that is not a visible card*.

- Ordering is unchanged (impact-desc → recency-desc → normalized headline →
  url_hash): capped-out score-6+ rows automatically rank ahead of 4–5 rows.
- `reporting.max_additional_articles` stays 10; overflow competes in the same
  pool under the same cap.
- Ledger semantics unchanged: `weak_relevance` still counts only 4–5 rows
  shown **nowhere**; `below_impact_threshold` still counts every
  suppression-surviving below-visible row; `surfaced_count` stays
  visible-cards-only. Capped-out rows shown in the appendix are displayed,
  not suppressed — no new reason code.

### 3. Macroeconomic Outlook capped at 3 signals

`prompts.py`: `MAX_MACRO_OUTLOOK_SIGNALS = 6` → `3`. This single constant
already drives both the prompt's promised signal count and the truncation in
`ingestion_engine._validate_macro_outlook`, so promise and enforcement move
together.

Defense in depth: `report._extract_macro_outlook` slices stored signals to
`MAX_MACRO_OUTLOOK_SIGNALS` (`signals[:3]`), so `daily_summaries` rows written
before this change (up to 6 signals) render at most 3 immediately — including
test-workflow re-renders against existing rows.

`MACRO_OUTLOOK_SOURCE_PACK_QUOTA` stays 10: the model keeps a full macro
source pack and selects the 3 most material signals from it.

### 4. One-sentence segment summaries

`prompts.thematic_prompt` system text: replace "exactly one synthesis
paragraph (2–3 sentences)" with **"exactly one sentence (maximum ~30 words)"**
that fuses the shared trend/driver and the Americhem implication. The
"written for a senior executive — no hedging, no filler" instruction stays.

Prompt-only change: `synthesize_thematic_paragraphs` validation (free-form
string per category) and the renderer are untouched, so an occasionally long
sentence degrades gracefully instead of being rejected.

## Error handling

Nothing new. All changes sit in pure modules (`report.py`, `prompts.py`) with
existing failure contracts; no I/O, schema, env, or migration changes. A
malformed stored `macro_outlook` still resolves to `None` before the slice.

## Testing

- `tests/test_prompts.py`: macro-prompt contract asserts the new signal cap
  (3) in the prompt text; thematic-prompt contract asserts the one-sentence
  instruction.
- Report tests (`assemble_report`, dict literals, zero patches):
  - Per-segment cap of 5 keeps the top 5 by impact; overflow rows appear in
    `additional_articles` ranked ahead of weak-relevance rows.
  - **Intentional expectation flip:** `test_report_capped_articles_do_not_reappear`
    (tests/test_pipeline.py) currently asserts capped-out articles appear
    nowhere in the email. Under this design they DO reappear — in the
    appendix. Rewrite that test to assert they appear in `additional_articles`
    (and still not as visible cards).
  - `weak_relevance` and `below_impact_threshold` counts unchanged in the
    overflow scenario; `surfaced_count` reflects capped cards only.
  - `_extract_macro_outlook` truncates a stored 6-signal outlook to 3.
- New ingestion test: `_validate_macro_outlook` truncates at
  `MAX_MACRO_OUTLOOK_SIGNALS` (no existing test pins the count).

## Documentation

Update `CLAUDE.md` (report step 4/5 description, `MAX_MACRO_OUTLOOK_SIGNALS`
mentions, config-file section) and `CONTEXT.md` (its "Additional Articles to
Explore" entry defines the appendix band as weak-relevance-only) to reflect
the widened appendix eligibility and the new caps.
