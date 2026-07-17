-- Migration 006: Allow accounting-only daily_summaries rows (issue #43).
-- Apply via Supabase SQL editor or psql. Safe to run multiple times
-- (DROP NOT NULL is a no-op on an already-nullable column).
--
-- IMPORTANT ROLLOUT ORDER:
--   Apply this migration BEFORE deploying the issue-#43 code. A run that
--   stores zero articles (or whose macro-summary LLM call fails) now upserts
--   an accounting-only row — run_date/run_mode plus screened_count and the
--   suppression breakdown/samples — with NO summary content columns. With
--   executive_summary / macro_sentiment still NOT NULL, that insert would
--   violate the constraints and crash the ingestion cron on exactly the runs
--   this fix exists to record.
--
-- Contract:
--   Content columns are OMITTED from the accounting-only payload (never
--   written as null), so a same-day retry upsert cannot wipe an earlier full
--   summary. Delivery treats a row without content as summary-less: no
--   Executive Summary / Macroeconomic Outlook sections render, and in the
--   test-mode fallback a content-empty row never shadows a content-full one.

alter table daily_summaries alter column executive_summary drop not null;
alter table daily_summaries alter column macro_sentiment drop not null;
