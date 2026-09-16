---
status: accepted
date: 2026-09-16
---

# One `daily_intelligence` row per URL across run modes; test rows are disposable

`daily_intelligence` carried no run mode, so a QA ingestion (`MARKET_PULSE_RUN_MODE=test`) stored rows in the same table the production delivery window reads, and the next morning's stakeholder email carried scores from whatever prompt the QA branch was on (issue #100). We decided to tag every row with its **run mode** (see `CONTEXT.md`) and keep `url_hash` unique: one URL has one row, whichever mode wrote it. Visibility is one-directional — production reads only production rows; a test run reads production rows and its own — and a test row is a disposable QA artifact: a production run that reaches a URL a test row holds does not see it as a duplicate, re-scrapes and re-scores it, and the existing upsert replaces the row as production, with `created_at` reset to that run's instant so the row lands in the delivery window of the run that produced its content.

## Considered options

- **Parallel copies** — a `(url_hash, run_mode)` unique index, both rows kept, test dedup seeing only test rows, so a QA ingestion could re-score the day's news under a branch prompt and the two scores could be diffed per URL in SQL. Rejected: it needs a second write path (the test upsert must not clobber the production row), doubles the scrape spend for a QA run (~100 Firecrawl and OpenAI calls), and the use case it serves — comparing a prompt against production on the same articles — already has a read-only home in the backtest replay (`scripts/backtest_scorer.py`), which writes nothing.
- **Test mode stores nothing** — rejected: a QA email could not be re-rendered from stored rows, and a branch prompt's scores could not be inspected after the fact.

## Consequences

- A QA ingestion dispatched after the 10:00 UTC cron stores only what production missed; its email is mostly a re-render of production rows. That is intended: prompt A/B belongs in the backtest tooling, not in the shared table.
- The migration (008) is required, not flag-gated, like 004–006: production reads filter on the new column, and a filter on a column that does not exist is a failed strict read — a red job with no email, never a leak.
- The one-directional rule now covers both tables with one spelling; `daily_summaries` had it since migration 002.
