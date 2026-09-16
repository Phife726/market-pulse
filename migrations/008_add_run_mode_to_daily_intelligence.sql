-- Migration 008: Tag every daily_intelligence row with the run mode that
-- wrote it (issue #100, ADR 0001). Apply via Supabase SQL editor or psql.
-- Safe to run multiple times.
--
-- WHY:
--   Only daily_summaries was keyed by run mode. A QA ingestion dispatched
--   with MARKET_PULSE_RUN_MODE=test stored its rows in the same table the
--   production delivery window reads, so the next morning's stakeholder
--   email carried scores from whatever prompt the QA branch was on, and the
--   production cron could not re-score those URLs (url_hash dedup is shared).
--
-- CONTRACT (CONTEXT.md, Run mode):
--   Every row belongs to exactly one mode. Visibility is one-directional:
--   production reads only production rows; a test run reads production rows
--   and its own. url_hash stays unique — one URL has one row whichever mode
--   wrote it — and a test row is disposable: a production write updates on
--   conflict (replacing the test row, refreshing created_at); a test write is
--   ON CONFLICT DO NOTHING, so the production row stands whichever order two
--   overlapping runs write in.
--
-- ROLLOUT ORDER (required, not flag-gated — like 004–006):
--   Apply BEFORE deploying the issue-#100 code. Production's reads filter on
--   this column; a filter on a column that does not exist is a failed strict
--   read — a red delivery job with no email, never a leak. Existing rows
--   take the default: every row stored before this migration went out in a
--   production email.

alter table daily_intelligence
  add column if not exists run_mode text not null default 'production';

-- The production reads always filter on run_mode and a created_at bound.
create index if not exists idx_daily_intelligence_run_mode_created_at
    on daily_intelligence (run_mode, created_at);

-- Expose the column on the ad-hoc view (no filter: the view is for humans).
-- DROP + CREATE, not CREATE OR REPLACE: the live view's column order predates
-- schema.sql's, and CREATE OR REPLACE VIEW refuses any change to existing
-- column names or positions (42P16). Nothing depends on the view — it exists
-- for ad-hoc queries only — so recreating it is safe and idempotent, and the
-- live definition matches schema.sql afterwards.
drop view if exists todays_intelligence;
create view todays_intelligence as
select
    id,
    created_at,
    headline,
    article_summary,
    americhem_impact,
    sentiment_score,
    sentiment_tag,
    americhem_impact_score,
    impact_rationale,
    strategic_segment,
    include_in_report,
    source_url,
    url_hash,
    entities_mentioned,
    category,
    trigger_entity,
    source_publication,
    sentiment_rationale,
    recommended_action,
    case
        when sentiment_score between 1 and 3 then 'CRITICAL'
        when sentiment_score between 8 and 10 then 'STRATEGIC'
        else 'ROUTINE'
    end as alert_tier,
    -- appended last: CREATE OR REPLACE VIEW may only add columns at the end
    run_mode
from daily_intelligence
where created_at >= now() - interval '24 hours'
order by coalesce(americhem_impact_score, sentiment_score) desc;
