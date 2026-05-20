-- Migration 001: Add relevance fields for Market-Pulse scoring upgrade.
-- Apply via Supabase SQL editor or psql before deploying the updated engine code.
-- Safe to run multiple times (all statements are IF NOT EXISTS / idempotent).

alter table daily_intelligence
  add column if not exists sentiment_tag text
    check (sentiment_tag in ('Negative', 'Neutral', 'Positive')),
  add column if not exists americhem_impact_score smallint
    check (americhem_impact_score between 1 and 10),
  add column if not exists impact_rationale text,
  add column if not exists strategic_segment text,
  add column if not exists include_in_report boolean default true;

create index if not exists idx_daily_intelligence_impact_score
    on daily_intelligence (americhem_impact_score);

create index if not exists idx_daily_intelligence_strategic_segment
    on daily_intelligence (strategic_segment);
