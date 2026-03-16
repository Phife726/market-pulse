# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose
Automated daily market intelligence pipeline (for Americhem) that replaces a Moody's News Edge subscription. Scrapes open-web news, applies LLM synthesis, and delivers a BLUF-formatted HTML email to stakeholders.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Copy and populate environment variables before running locally
cp .env.example .env

# Run the full pipeline manually (ingestion then delivery)
python ingestion_engine.py
python delivery_engine.py

# Run all tests
pytest tests/

# Run a single test
pytest tests/test_pipeline.py::test_name
```

The GitHub Actions workflow (`.github/workflows/market_pulse.yml`) runs ingestion then delivery Monday–Friday at 10:00 UTC. It can also be triggered manually via the Actions tab.

## Architecture

The pipeline is two sequential scripts sharing a Supabase database:

**`ingestion_engine.py`** — Scrape → Synthesize → Store
1. Loads `targets.yaml` to get active entities (competitors, customers, suppliers, markets)
2. Queries Serper.dev for recent article URLs per entity (`results_per_entity`, `lookback_hours` from `targets.yaml`)
3. Strips URL query parameters, computes SHA-256 hash → skips if already in DB
4. Extracts article markdown via Firecrawl; skips if below `min_article_length`
5. Calls OpenAI `gpt-4o-mini` with article text; receives structured JSON: `headline`, `americhem_impact`, `sentiment_score` (1–10), `source_url`, `entities_mentioned`, `category`
6. Upserts row into `daily_intelligence` table (unique constraint on `url_hash`)
7. Enforces `MAX_DAILY_SCRAPES = 20` hard cap to protect free-tier API quotas

**`delivery_engine.py`** — Fetch → Format → Send
1. Queries the `todays_intelligence` view (last 24 hours, ordered by `sentiment_score` ascending)
2. Groups items by `alert_tier`: CRITICAL (1–3), ROUTINE (4–7), STRATEGIC (8–10)
3. Renders HTML email (BLUF format) and sends via SMTP (Resend)

**Database** (`schema.sql`) — Single table `daily_intelligence` with a `todays_intelligence` view. The view adds an `alert_tier` column derived from `sentiment_score`. The unique index on `url_hash` is the deduplication gate.

**`targets.yaml`** — The only file non-technical editors need to touch. Add/remove entities here; no Python changes required.

## Key Invariants
- URL normalization (strip query params) MUST happen before hashing — this is the sole deduplication mechanism.
- `source_url` is injected into the LLM prompt so the model returns the canonical URL deterministically.
- `SUPABASE_KEY` must be the **Service Role** key (not anon) to bypass Row Level Security.
- `MAX_DAILY_SCRAPES = 20` must never be raised without confirming free-tier quota headroom across Serper, Firecrawl, and OpenAI.
