# So-What Honesty, Appendix Breadth, and Direction Cue — Design

**Date:** 2026-08-03
**Status:** Approved (pending spec review)
**Driver:** Recipient feedback (tech-hat reader, non-target audience; commercial team — the target audience — is more positive). Complaints addressed: the "So what" line spins direction (positive framing on Americhem-negative news, upside claimed in markets Americhem doesn't serve) and the report is slower to scan than the old News Edge feed. The reader explicitly likes the Additional Articles appendix.

## Scope

Three changes, all reversible without migration or feature flag:

1. Prompt honesty rules (`prompts.py`, text-only)
2. Appendix widening (`market_pulse_config.yaml`, production config only)
3. Sentiment-direction glyph on the So-What line (`delivery_engine.py`)

Out of scope (explicitly deferred by Jason): So-What word cap / brevity rules, a curated out-of-scope-markets list, per-audience report variants, a headlines-only appendix tier below the supporting threshold.

## Root cause

RULE 6 of the insight system prompt demands a specific commercial So-What "even for routine items" and bans hedged phrasing without offering an honest alternative, so the model fabricates impact — most visibly as invented upside (a competitor launch framed as "increases our sales"; an opportunity in recycled resin, a market Americhem does not serve). Nothing in the prompt requires the So-What's direction to agree with `sentiment_tag`, and nothing constrains upside claims to markets Americhem actually serves.

## 1. Prompt changes (`prompts.py`)

Text-only edits to `_SYSTEM_PROMPT_BASE`. No schema change; assembly stays `str.replace()` and the literal-JSON-brace guard test is unaffected.

**RULE 2 — one added line (fix the tag at the source):**

> A competitor's product launch, contract win, or expansion is Negative or Neutral for Americhem by default (competitive threat). Only tag Positive if the article demonstrates growth in a market Americhem serves.

**RULE 6 — rewrite (keep the "never generic" spirit; repair three failure modes):**

- *Directional consistency:* the So-What's direction must agree with `sentiment_tag`. Never describe upside under a Negative tag or vice versa.
- *Upside routes through the taxonomy:* claim demand or sales upside only when the mechanism runs through a RULE 4 commercial segment (the taxonomy already injected into the prompt). If the market is adjacent to but outside those segments, say so explicitly: "Adjacent market — no direct Americhem participation indicated."
- *Calibrated honesty replaces the blanket ban:* the lazy phrase "No direct impact. Monitoring required." stays banned, but the template "Limited direct exposure — [specific reason]" becomes explicitly permitted when true, giving the model an honest exit instead of forcing invented impact.

## 2. Config changes (`market_pulse_config.yaml`)

- `reporting.supporting_impact_threshold: 4 → 3` — widens the Additional Articles band (rows ≥ 3 and below the visible threshold, plus cap overflow).
- `reporting.max_additional_articles: 10 → 20` — more headline-only rows.

Code defaults in `scoring.py` / `report.py` are untouched; this is production YAML only. Rollback is one line each.

## 3. Direction cue on the So-What line (`delivery_engine.py`)

`_render_card` prepends a colored glyph to the "So what:" label, keyed by `sentiment_tag` via the existing `_SENTIMENT_TAG_COLORS` map:

| Tag | Glyph | Color |
|---|---|---|
| Negative | ▼ | `#DC2626` |
| Neutral | ● | `#6B7280` |
| Positive | ▲ | `#16A34A` |

Plain unicode with inline styles (email-safe). Rows with no `sentiment_tag` render the label unchanged. The meta strip's existing colored tag stays as-is; the glyph puts direction where the reader's eye actually is — on the prose — so a mis-spun sentence is visibly contradicted by the marker.

Alternative considered and rejected: a colored left card border (heavier visual change; less consistent rendering across email clients).

## Tests & QA

- `tests/test_prompts.py`: pin the presence of the new RULE 2 competitor line, the directional-consistency rule, and the "Limited direct exposure" template; the existing brace-guard test confirms assembly still works.
- Card render tests: glyph present with correct color per tag; absent-tag rows unchanged; legacy rows unaffected.
- Pre-production QA: `python scripts/show_prompts.py` to diff the assembled prompt at zero API spend, then a test-workflow run with `run_ingestion=true` (a re-render-only run would not exercise the new prompt) delivering to the Jason-only QA pool.

## Rollback

Config: revert the two YAML lines. Prompt and glyph: git revert. No migration, no schema change, no feature flag.

## Success criteria

- No So-What claims sales/demand upside on a Negative-tagged row (spot-check QA runs).
- Competitor-launch articles read as competitive threat or neutral, not Americhem upside.
- Appendix shows up to 20 rows spanning the ≥3 band on typical news days.
- Commercial-team recipients report no regression in report usefulness.
