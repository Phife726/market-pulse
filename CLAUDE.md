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

1. Loads `targets.yaml` to get active targets — two modes: **entity** (one Serper query per company name) and **concept** (one combined OR query per group, gated by `active: true` at group level)
2. Queries Serper.dev for recent article URLs; runs semantic deduplication (`rapidfuzz token_sort_ratio >= 88`) against headlines seen in the last 72 h before scraping
3. Strips URL query parameters, computes SHA-256 hash → skips if already in DB
4. Extracts article markdown via Firecrawl; falls back to a direct-HTTP scraper on HTTP 402 (quota exhaustion); skips if below `min_article_length`
5. Calls OpenAI `gpt-5.4-nano` with article text; receives structured JSON: `headline`, `americhem_impact`, `sentiment_score` (1–10), `source_url`, `entities_mentioned`, `recommended_action`, etc.
6. Upserts row into `daily_intelligence` table (unique constraint on `url_hash`)
7. After all articles are stored, calls `generate_macro_summary()` — a second OpenAI call that writes `executive_summary` and `macro_sentiment` to the `daily_summaries` table (keyed on `run_date`)
8. Enforces `MAX_DAILY_SCRAPES = 150` hard cap and `PIPELINE_DEADLINE_SECONDS = 600` wall-clock deadline (keeps runtime inside the GitHub Actions 15-min limit)

**`delivery_engine.py`** — Fetch → Format → Send

1. Queries `daily_intelligence` directly (last 24 h; 72 h on Mondays) via `fetch_todays_intelligence()` — does **not** use the `todays_intelligence` view at runtime
2. Calls `fetch_macro_summary()` to retrieve the executive summary written by ingestion
3. Renders a three-zone HTML email and sends via the **Resend HTTP API** (`POST https://api.resend.com/emails`) with exponential-backoff retry (5 attempts; retries on 429, 500, 502, 503, 504). `SMTP_PASS` env var holds the Resend API key (legacy name).

**Email layout — three zones:**

- **Critical Disruptions** (score 1–3): full article cards
- **Thematic Intelligence** (score 4–10): categories with 2+ articles get an LLM-generated synthesis paragraph (`synthesize_thematic_paragraphs()`); single high-score articles (7–10) render as bullets without synthesis
- **Peripheral Signals** (score 4–6, ungrouped singletons): compact bullet list

**Database** (`schema.sql`) — Two tables: `daily_intelligence` (articles) and `daily_summaries` (one macro summary row per run date). The `todays_intelligence` view adds an `alert_tier` column for ad-hoc queries; the unique index on `url_hash` is the deduplication gate.

**`targets.yaml`** — The only file non-technical editors need to touch. Add/remove entities here; no Python changes required. Top-level keys:

- `discovery.results_per_entity` / `lookback_hours` / `min_article_length` — discovery tuning
- **Entity-mode groups** (`search_mode: entity`): list entities under `entities:` as `{name, active}`; set `active: false` to pause without deleting
- **Concept-mode groups** (`search_mode: concept`): set `active: true/false` at the group level; define `include_any` (OR'd terms) and optional `include_all` / `exclude_any`
- `exclude_any` entries matching Moody's platform identifiers (e.g. `"source set 238658"`, `"PR wires"`) are silently dropped by `build_query()` — only real search terms become `-"term"` operators

## Tests

`tests/test_pipeline.py` covers: URL normalization (query params/fragments stripped), SHA-256 hash collision (UTM-polluted vs. clean URL must hash identically), sentiment score clamping to [1, 10], and `load_targets()` filtering inactive entities. All external API clients (OpenAI, Supabase, Serper, Firecrawl) are mocked — no live calls in the test suite.

## Key Invariants

- URL normalization (strip query params) MUST happen before hashing — this is the sole deduplication mechanism.
- `source_url` is injected into the LLM prompt so the model returns the canonical URL deterministically.
- `SUPABASE_KEY` must be the **Service Role** key (not anon) to bypass Row Level Security.
- `MAX_DAILY_SCRAPES = 150` — all three APIs (Serper, Firecrawl, OpenAI) are on paid-tier subscriptions. Adjust only if subscription tiers change.
- `PIPELINE_DEADLINE_SECONDS = 600` — ingestion stops early if the wall clock exceeds 10 min, then flushes stats and calls `generate_macro_summary()` before returning.
- Monday delivery uses a 72-hour lookback (vs. 24 h on other days) to capture weekend news — this logic lives in `fetch_todays_intelligence()`.
- `SMTP_PASS` env var holds the **Resend API key** (legacy name retained to avoid secret rotation). Email is sent via `POST https://api.resend.com/emails`, not SMTP.

## Python Conventions

- Type hints on all function signatures; `Optional[T]` for nullable returns.
- Structured logging with `%s` placeholders — never f-strings in `logger.*()` calls.
- Specific exception handling — never bare `except:` or broad `except Exception` without logging `exc`.
