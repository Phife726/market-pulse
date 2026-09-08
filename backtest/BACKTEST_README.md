# Market-Pulse scorer backtest set

`market_pulse_scorer_backtest.csv` — 150 rows drawn from `daily_intelligence` (Jul 17 – Sep 8, 2026), stratified by period × signal_type.

## Columns
| Column | Meaning |
|---|---|
| `backtest_id` | Stable row ID (BT001–BT150) |
| `daily_intelligence_id` | Original Supabase row id |
| `period` | `P2_jul17_aug4` (over-generous rubric) · `P3_aug5_aug27` (first tightening) · `P4_aug28_sep8` (second tightening) |
| `headline`, `article_summary`, `trigger_entity`, `signal_type`, `source_publication` | Scorer inputs |
| `original_score`, `original_rationale`, `original_action` | What the production scorer produced at the time |
| `expected_tier` | `surface` = should score ≥5 · `suppress` = should score ≤4 · `unlabeled` = no prior, use for distribution shape only |
| `label_reason` | Why the label was assigned |
| `revised_score`, `revised_rationale`, `pass` | **Fill these in** with the revised rubric |

## Label counts
- 37 `surface` — includes 13 near-misses currently stuck at 5 or below, plus 24 true positives from P2 that should still surface
- 39 `suppress` — 34 of which the P2 rubric scored ≥5 (the noise the old rubric let through)
- 74 `unlabeled` — proportional sample for measuring distribution shape

## Pass criteria for a revised rubric
1. **Recall:** ≥ 90% of `surface` rows score ≥5 (≥ 34 of 37)
2. **Precision:** ≥ 90% of `suppress` rows score ≤4 (≥ 35 of 39)
3. **Distribution:** no single score value exceeds 50% of scored rows. The production
   proxy this used to carry — "10–15% score ≥5 across all 150 rows" — was re-specified on
   2026-09-08: the set is stratified (76 of 150 rows are labeled), so that share cannot hold
   alongside ≥90% recall, and the target was always a proxy for production `surfaced_count`.
   What is actually watched: **median production `surfaced_count` over the first 5 crons
   after a rubric change lands in 8–15** (the `replay` job on a PR estimates it read-only).
   The runner still prints the ≥5 share of the 74 `unlabeled` rows for information.
4. **No regression on threshold items:** `surface` rows with `original_score >= 6` should still score ≥6

Grade with `pass = (expected_tier=='surface' and revised_score>=5) or (expected_tier=='suppress' and revised_score<=4)`; leave blank for `unlabeled`.

Note: `article_summary` is what the scorer sees in production. If the revised rubric needs full scraped text, re-fetch via `source_url` from the original table — this file intentionally omits it to stay small.

## Running it

`python scripts/backtest_scorer.py csv` re-scores every row through the live
insight prompt (`prompts.insight_prompt` → the `llm` seam → `insight.normalize`,
exactly the production path minus the scrape), fills the three blank columns,
writes `market_pulse_scorer_backtest_v2.csv` next to this file, and prints the
four criteria with every failure named. It needs `OPENAI_API_KEY`; the
`Scorer Backtest` workflow (`.github/workflows/scorer_backtest.yml`) runs it
where that secret lives and uploads the filled CSV as an artifact.
