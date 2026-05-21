-- Migration 002: Split commercial segment from signal type; add structured macro
-- summary fields; add run_mode isolation; capture suppression counts and samples.
-- Apply via Supabase SQL editor or psql. Safe to run multiple times.

alter table daily_intelligence
  add column if not exists commercial_segment text,
  add column if not exists signal_type text;

create index if not exists idx_daily_intelligence_commercial_segment
  on daily_intelligence (commercial_segment);

create index if not exists idx_daily_intelligence_signal_type
  on daily_intelligence (signal_type);

alter table daily_summaries
  add column if not exists run_mode text not null default 'production',
  add column if not exists dominant_condition text,
  add column if not exists executive_bullets jsonb,
  add column if not exists screened_count integer,
  add column if not exists surfaced_count integer,
  add column if not exists suppression_breakdown jsonb,
  add column if not exists suppression_samples jsonb;

-- Replace single-key uniqueness on run_date with a composite unique index on
-- (run_date, run_mode) so test runs never overwrite production summary rows.
-- Existing uniqueness verified as idx_daily_summaries_run_date_unique (index, not constraint).
drop index if exists idx_daily_summaries_run_date_unique;

create unique index if not exists idx_daily_summaries_run_date_mode_unique
  on daily_summaries (run_date, run_mode);
