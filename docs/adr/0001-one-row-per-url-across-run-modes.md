---
status: accepted
date: 2026-09-16
---

# One `daily_intelligence` row per URL across run modes; test rows are disposable

`daily_intelligence` carried no run mode, so a QA ingestion (`MARKET_PULSE_RUN_MODE=test`) stored rows in the same table the production delivery window reads, and the next morning's stakeholder email carried scores from whatever prompt the QA branch was on (issue #100). We decided to tag every row with its **run mode** (see `CONTEXT.md`) and keep `url_hash` unique: one URL has one row, whichever mode wrote it. Visibility is one-directional — production reads only production rows; a test run reads production rows and its own — and a test row is a disposable QA artifact: a production run that reaches a URL a test row holds does not see it as a duplicate, re-scrapes and re-scores it, and its write replaces the row as production, with `created_at` reset to that run's instant so the row lands in the delivery window of the run that produced its content. Production precedence is **atomic at the database, not a property of the duplicate check**: a production write is an upsert that updates on `url_hash` conflict; a test write is an insert that does nothing on conflict (`ON CONFLICT DO NOTHING`). Whichever order two overlapping runs write in, the production row stands — the mode-filtered duplicate check before the write is an economy (it saves the scrape), never the guard.

## Considered options

- **Parallel copies** — a `(url_hash, run_mode)` unique index, both rows kept, test dedup seeing only test rows, so a QA ingestion could re-score the day's news under a branch prompt and the two scores could be diffed per URL in SQL. Rejected: it needs a second write path (the test upsert must not clobber the production row), doubles the scrape spend for a QA run (~100 Firecrawl and OpenAI calls), and the use case it serves — comparing a prompt against production on the same articles — already has a read-only home in the backtest replay (`scripts/backtest_scorer.py`), which writes nothing.
- **Test mode stores nothing** — rejected: a QA email could not be re-rendered from stored rows, and a branch prompt's scores could not be inspected after the fact.

## Consequences

- A QA ingestion dispatched after the 10:00 UTC cron stores only what production missed; its email is mostly a re-render of production rows. That is intended: prompt A/B belongs in the backtest tooling, not in the shared table.
- The migration (008) is required, not flag-gated, like 004–006: production reads filter on the new column, and a filter on a column that does not exist is a failed strict read — a red job with no email, never a leak.
- A QA dispatch can overlap the scheduled run (the workflows share no concurrency group, and none is required). Both mode-filtered checks may then see a URL as new and both may scrape it; the write policy settles it: if production writes last it replaces the test row, if test writes last its insert is ignored and the production row is untouched. In the second case the test run has spent a scrape and a synthesis call on a row that did not land; it records that as a duplicate, not a store.
- The one-directional rule now covers both tables with one spelling; `daily_summaries` had it since migration 002.
