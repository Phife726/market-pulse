-- Migration 004: Add executive_sources to daily_summaries.
-- Backs reader-facing source citations on the executive-summary bullets.
-- Apply via Supabase SQL editor or psql. Safe to run multiple times.
--
-- IMPORTANT ROLLOUT ORDER:
--   Apply this migration BEFORE deploying the citation code. The column is
--   REQUIRED (not flag-gated): generate_macro_summary writes executive_sources
--   on every run, and delivery's fetch_latest_summary SELECTs it. Deploying the
--   code first would crash ingestion upserts and blank out delivery's summary.
--
-- Column:
--   executive_sources — JSON array of cited source objects:
--     [{ "id": int, "headline": text, "url": text,
--        "domain": text, "segment": text, "score": int }]
--   Holds only sources cited by at least one executive bullet. Empty array when
--   the summary has no valid citations.

alter table daily_summaries
  add column if not exists executive_sources jsonb;
