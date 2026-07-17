-- Enable UUID generation in Postgres/Supabase.
create extension if not exists pgcrypto;

create table if not exists daily_intelligence (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    headline text not null,
    americhem_impact text not null,
    sentiment_score smallint not null check (sentiment_score between 1 and 10),
    source_url text not null,
    url_hash text not null,
    entities_mentioned jsonb not null default '[]'::jsonb,
    category text not null,
    trigger_entity text not null,
    source_publication text,
    sentiment_rationale text,
    recommended_action text,
    article_summary text,
    raw_content text,
    -- Relevance upgrade fields (migration 001)
    sentiment_tag text check (sentiment_tag in ('Negative', 'Neutral', 'Positive')),
    americhem_impact_score smallint check (americhem_impact_score between 1 and 10),
    impact_rationale text,
    strategic_segment text,
    include_in_report boolean default true,
    -- Commercial intelligence brief fields (migration 002)
    commercial_segment text,
    signal_type text
);

-- Unique index to prevent duplicate entries for normalized article URLs.
create unique index if not exists idx_daily_intelligence_url_hash_unique
    on daily_intelligence (url_hash);

create index if not exists idx_daily_intelligence_created_at
    on daily_intelligence (created_at);

create index if not exists idx_daily_intelligence_category
    on daily_intelligence (category);

create index if not exists idx_daily_intelligence_sentiment_score
    on daily_intelligence (sentiment_score);

create index if not exists idx_daily_intelligence_impact_score
    on daily_intelligence (americhem_impact_score);

create index if not exists idx_daily_intelligence_strategic_segment
    on daily_intelligence (strategic_segment);

-- Stores one executive summary row per pipeline run date.
-- Content columns are nullable (migration 006): a run that stores zero
-- articles persists an accounting-only row — screened/suppression columns
-- with no summary content.
create table if not exists daily_summaries (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    run_date date not null,
    executive_summary text,
    macro_sentiment text,
    -- Commercial intelligence brief fields (migration 002)
    run_mode text not null default 'production',
    dominant_condition text,
    executive_bullets jsonb,
    -- Executive-summary source citations (migration 004)
    executive_sources jsonb,
    -- Macroeconomic Outlook: current_condition + material macro signals (migration 005)
    macro_outlook jsonb,
    screened_count integer,
    surfaced_count integer,
    suppression_breakdown jsonb,
    suppression_samples jsonb
);

create unique index if not exists idx_daily_summaries_run_date_mode_unique
    on daily_summaries (run_date, run_mode);

create index if not exists idx_daily_intelligence_commercial_segment
    on daily_intelligence (commercial_segment);

create index if not exists idx_daily_intelligence_signal_type
    on daily_intelligence (signal_type);

create or replace view todays_intelligence as
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
    end as alert_tier
from daily_intelligence
where created_at >= now() - interval '24 hours'
order by coalesce(americhem_impact_score, sentiment_score) desc;
