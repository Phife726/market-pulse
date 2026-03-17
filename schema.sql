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
    raw_content text
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

-- Stores one executive summary row per pipeline run date.
create table if not exists daily_summaries (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),
    run_date date not null,
    executive_summary text not null,
    macro_sentiment text not null
);

create unique index if not exists idx_daily_summaries_run_date_unique
    on daily_summaries (run_date);

create or replace view todays_intelligence as
select
    id,
    created_at,
    headline,
    americhem_impact,
    sentiment_score,
    source_url,
    url_hash,
    entities_mentioned,
    category,
    trigger_entity,
    source_publication,
    sentiment_rationale,
    case
        when sentiment_score between 1 and 3 then 'CRITICAL'
        when sentiment_score between 8 and 10 then 'STRATEGIC'
        else 'ROUTINE'
    end as alert_tier
from daily_intelligence
where created_at >= now() - interval '24 hours'
order by sentiment_score asc;
