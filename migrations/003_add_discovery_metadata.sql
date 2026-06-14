-- Migration 003: Add discovery-provenance columns for multi-provider article
-- discovery (Serper + ZoomInfo news enrichment).
-- Apply via Supabase SQL editor or psql. Safe to run multiple times.
--
-- IMPORTANT ROLLOUT ORDER:
--   1. Merge the ZoomInfo code (all feature flags default OFF — production
--      upserts are unaffected because the ingestion engine does NOT write these
--      columns until STORE_DISCOVERY_METADATA is truthy).
--   2. Apply this migration in Supabase.
--   3. Set the STORE_DISCOVERY_METADATA repository variable to true so the
--      ingestion engine begins persisting discovery provenance.
--
-- Columns:
--   discovery_source    — provider that surfaced the article ('serper' | 'zoominfo')
--   external_company_id — ZoomInfo company id as text ('' for Serper rows)
--   published_at        — article publish timestamp reported by the provider
--   source_metadata     — provider/publication/description/categories blob

alter table daily_intelligence
  add column if not exists discovery_source text,
  add column if not exists external_company_id text,
  add column if not exists published_at timestamptz,
  add column if not exists source_metadata jsonb;

create index if not exists idx_daily_intelligence_discovery_source
  on daily_intelligence (discovery_source);

create index if not exists idx_daily_intelligence_external_company_id
  on daily_intelligence (external_company_id);
