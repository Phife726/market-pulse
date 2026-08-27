# Low-Exposure Rows Reach the Appendix — Design

**Date:** 2026-08-27
**Status:** Implemented (issue #65)
**Driver:** `/code-review` of PR #62 found that RULE 6's honest low-exposure templates (`Adjacent market — …`, `Limited direct exposure — …`) are bound to `americhem_impact_score` 3–4 on the promise that 3 still reaches the Additional Articles appendix — but an adjacent-market row is by definition outside every RULE 4 segment, so the model files it under `Enterprise / Cross-Segment` (also `insight.DEFAULT_SEGMENT`), and delivery suppression rule 1 drops every Enterprise row below `enterprise_min_impact` (7) **before** appendix selection runs. The band the prompt promises is discarded for exactly the rows it was written for.

## The defect

Two layers make a promise and a third silently voids it:

1. `prompts.py` RULE 6 — "never below 3 (the article still passed Rule 7)" — a literal that only means "reaches the appendix" while `reporting.supporting_impact_threshold` is 3.
2. `report._select_additional_articles` — admits suppression survivors at or above the supporting threshold.
3. `report._apply_delivery_suppression` rule 1 — drops `Enterprise / Cross-Segment` rows scoring below 7, first, unconditionally. `tests/test_pipeline.py::test_appendix_never_includes_enterprise_cross_segment_low_impact` pinned the drop as deliberate — it predates the templates.

The issue's data gate (share of template rows that are Enterprise, from production `daily_intelligence`) could not be run from this session — no Supabase credentials are available locally and the job logs print stored rows' impact but not their segment or So-What. The fix does not depend on the answer: the contract is broken for every Enterprise template row whatever their share, and the fix chosen below cannot touch a visible card, so its downside is bounded by the appendix cap (20). The share question stays open as the appendix-lever question from #62; the fix makes it observable (template rows now appear in the appendix on quiet days).

## Options considered

1. **Prompt fix** — tell the model an adjacent market still has an end-market and to classify it under RULE 4. Rejected: it asks the model to file "no Americhem participation" rows under a served segment (a mislabel that would pollute per-segment counts and the thematic synthesis), it is probabilistic, and it does nothing for `Limited direct exposure` rows that are genuinely Enterprise (multi-segment corporate notes).
2. **Lower `enterprise_min_impact`** toward the supporting threshold (config-only). Rejected: it reopens the Enterprise noise rule 1 exists to cut for *every* Enterprise row, and at 6 it also changes which rows become visible cards — the shared commercial-team surface the audience note says to leave alone.
3. **Exempt template rows from rule 1** — a row whose `americhem_impact` opens with a RULE 6 template skips rule 1, *only while it is below the visible threshold*. Deterministic, restores exactly the promised band, cannot alter a visible card.

**Chosen: (3).** The template prefixes are defined once and shared by the prompt and the suppression check, the same one-definition pattern `macro_summary.py` uses for the macro vocabulary.

## Design

### `prompts.py` — one definition of the templates and the band

- `LOW_EXPOSURE_TEMPLATE_PREFIXES = ("Adjacent market", "Limited direct exposure")` — module-level, imported by `report.py`. RULE 6's template sentences are assembled from it.
- The score band RULE 6 binds the templates to is **derived from `Scoring.from_config(config)`**, not written as a literal: low = `supporting`, high = `min(supporting + 1, visible - 1)` (never below low). Production config (3 / 6) yields "3 or 4" — byte-identical intent to today's wording; code defaults (4 / 6) yield "4 or 5". RULE 7's "set `americhem_impact_score` to 4 when relevance is uncertain" is the same band's upper edge — one derivation, so a `supporting_impact_threshold` rollback moves both rules together instead of stranding template rows below the appendix floor. Assembly stays `str.replace()` on named markers.

### `report.py` — the exemption

`_is_low_exposure_template(row) -> bool`: `americhem_impact` stripped of leading whitespace/quotes, case-insensitive `startswith` on any prefix. Rule 1 becomes: Enterprise **and** below `enterprise_min_impact` **and not** (template **and** below the visible threshold). The `Scoring` the report already builds is passed into `_apply_delivery_suppression`, so the visible edge is the configured one. Config switch `delivery_suppression.enable_low_exposure_template_exemption` (default `true` in code; set explicitly in YAML) is the config-only rollback.

Everything downstream is unchanged: an exempted row flows through the visibility filter (it is below it by construction), into the appendix pool, ranked impact-desc with the rest and subject to the cap — score-3 template rows sit at the tail and are the first pushed out on a heavy day, which is the intended priority.

### `delivery_engine._render_card` — escaping parity

`headline` and `americhem_impact` are HTML-escaped; `source_url` passes through `_safe_http_url` and renders as an unlinked span when unsafe (the same shape the appendix renderer uses). Pre-existing gap noted in #62 and #65; no visual change for well-formed rows.

### Tests

- Rule 1 exemption: template row below visible → kept; template row at/above visible → still dropped by rule 1 (no new visible card); non-template Enterprise low row → still dropped; prefix match is case-insensitive and tolerant of leading quote; config switch off restores the drop. `test_appendix_never_includes_enterprise_cross_segment_low_impact` is rewritten to pin the narrower rule (non-template rows still never reach the appendix) plus its mirror (template rows do).
- Prompt band derivation: `{}` → "4 or 5" / "never below 4" / RULE 7 "to 5"; production-style config → "3 or 4" / RULE 7 "to 4"; the RULE 6 sentences embed every `LOW_EXPOSURE_TEMPLATE_PREFIXES` entry (drift guard).
- Real-config pins: `reporting.supporting_impact_threshold == 3`, `max_additional_articles == 20`, `max_visible_articles_per_segment == 5`, the new switch is `true`, and the assembled production prompt says "3 or 4".
- `_render_card`: a `<script>` in headline / So-What is escaped; a `javascript:` URL renders no `href`.

### Docs

`CLAUDE.md` (rule 1 description, appendix step 5, RULE 6 note), `CONTEXT.md` (appendix entry), `market_pulse_config.yaml` (switch + comment).
