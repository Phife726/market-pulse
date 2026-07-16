-- Migration 005: Add macro_outlook to daily_summaries.
-- Backs the Macroeconomic Outlook section (current_condition + material,
-- source-cited macro signals). Apply via Supabase SQL editor or psql. Safe to
-- run multiple times.
--
-- IMPORTANT ROLLOUT ORDER:
--   Apply this migration BEFORE deploying the macro-outlook code. The column is
--   REQUIRED (not flag-gated): generate_macro_summary writes macro_outlook on
--   every run (as null when no material signal exists), and delivery's
--   fetch_latest_summary SELECTs it. Deploying the code first would crash
--   ingestion upserts and blank out delivery's summary — same contract as
--   migration 004.
--
-- Column:
--   macro_outlook — JSON object, or null when no material macro signal exists:
--     {
--       "current_condition": text,
--       "signals": [
--         { "indicator": text,
--           "direction": "Rising" | "Stable" | "Declining",
--           "americhem_implication": text,
--           "affected_segments": [text, ...],   -- canonical commercial segments
--           "citation_source_ids": [int, ...]   -- always >= 1 (materiality gate)
--         }, ...
--       ]
--     }

alter table daily_summaries
  add column if not exists macro_outlook jsonb;
