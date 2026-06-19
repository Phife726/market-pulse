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

Two GitHub Actions workflows exist:

- `.github/workflows/market_pulse.yml` — production schedule, runs ingestion then delivery Monday–Friday at 10:00 UTC; also dispatchable manually.
- `.github/workflows/market_pulse_test.yml` — manually-dispatched sandbox run. Sets `MARKET_PULSE_RUN_MODE=test`, routes mail to the `TEST_RECIPIENT_EMAILS` secret (Jason-only QA pool), and exposes `run_ingestion` / `send_email` input flags so you can re-render against existing rows without re-billing APIs.

`delivery_engine_old.py` is a legacy snapshot — do not edit it; the active delivery code is `delivery_engine.py`.

## Architecture

The pipeline is two sequential scripts sharing a Supabase database, plus three seam modules — one owning suppression accounting, one owning every database call, and one owning every structured LLM call — plus `insight.py` (the per-article schema: taxonomies, normalization, field readers) and `scoring.py` (the relevance thresholds the report applies):

**`suppression_ledger.py`** — Pure in-process module owning the suppression reason taxonomy (5 ingestion-owned + 9 delivery-owned codes), `SAMPLES_CAP = 10`, and the same-day-retry merge semantics. Used by both engines; performs zero I/O.

**`daily_intelligence_repo.py`** — Single seam for every Supabase query the pipeline makes. One Protocol (`IntelligenceRepo`), two adapters (`SupabaseIntelligenceRepo` for prod, `InMemoryIntelligenceRepo` for tests). Reads swallow exceptions and return an empty sentinel; writes raise so silent write failures crash the cron loudly. Callers do `from daily_intelligence_repo import _repo` and call `_repo()`; tests inject the fake at the consumer module — e.g. `monkeypatch.setattr("delivery_engine._repo", lambda: fake)`. The repo does not know about `SuppressionLedger` — the same-day-retry merge for delivery counts lives in `delivery_engine._update_delivery_summary_counts`.

**`llm.py`** — Single seam for every structured (JSON) OpenAI call the pipeline makes. One Protocol (`LLM`), two adapters (`OpenAILLM` for prod, `FakeLLM` for tests). The interface is `complete_json(*, system, user, temperature=None, context="") -> Optional[dict]`: it owns the OpenAI client, `OPENAI_MODEL`, the `response_format=json_object` request, and the response-envelope handling (`choices[0].message.content` extraction + `json.loads`). It does **not** own response *validation* — each caller validates the parsed dict itself, because the rules are domain-specific (relevance fields, executive bullets, free-form paragraphs). Failure contract: `complete_json` never raises; on a transport error, empty content, or unparseable JSON it logs and returns `None`, and each caller maps `None` to its own sentinel (`synthesize_insight` → `None`, `generate_macro_summary` → `False`, `synthesize_thematic_paragraphs` → `{}`). Callers do `from llm import _llm` and call `_llm()`; tests inject the fake at the consumer module — e.g. `monkeypatch.setattr("ingestion_engine._llm", lambda: FakeLLM(returns=...))`. The SDK-shape contract (model id, json format) is asserted once in `tests/test_llm.py`, not re-asserted in every caller test.

**`insight.py`** — Pure module owning the per-article **Insight** schema: the value taxonomies (`VALID_SENTIMENT_TAGS`, `VALID_COMMERCIAL_SEGMENTS`, `VALID_SIGNAL_TYPES`, `VALID_ACTIONS`), `normalize(raw) -> Optional[dict]` (the clamp/default/validate rules that turn a raw LLM dict into a storable row; returns `None` if a required key is missing), `is_discard(raw)`, and the field readers `effective_impact` / `commercial_segment` / `signal_type`. The row stays a plain dict (the shape the repo and renderer rely on) — this module concentrates the *knowledge* about it, not the format. `ingestion_engine.synthesize_insight` calls `normalize`; `delivery_engine` re-exports the readers (`from insight import effective_impact as _effective_impact, …`) so its internal call sites and existing tests are unchanged. The macro-summary schema (`dominant_condition` / `executive_bullets`) is a separate structured output and stays in `generate_macro_summary`. Schema rules are tested directly in `tests/test_insight.py`.

**`scoring.py`** — Pure module owning the relevance thresholds the report applies to an Insight's materiality. `Scoring.from_config(config)` resolves the configurable bands (`reporting.visible_impact_threshold` default 6, `supporting_impact_threshold` default 4) and exposes `is_visible(row)` / `is_weak_relevance(row)`; module-level `tier(row)` (CRITICAL ≤3 / STRATEGIC ≥8 / ROUTINE) and `is_legacy_critical(row)` use fixed edges. Builds on `insight.effective_impact` (the *what*); scoring is the *what it means for the report*. `delivery_engine` constructs a `Scoring` in `generate_html_email`, re-exports `tier` as `_alert_tier`, and calls `is_legacy_critical` for the meta-strip badge. **Out of scope** (left where they are): the suppression policy's `enterprise_min_impact` (a suppression parameter) and `_sentiment_word`'s legacy directional-sentiment mapping (tone, not materiality). Tested directly in `tests/test_scoring.py`.

**`ingestion_engine.py`** — Scrape → Synthesize → Store

1. Loads `targets.yaml` to get active targets — two modes: **entity** (one Serper query per company name) and **concept** (one combined OR query per group, gated by `active: true` at group level)
2. Queries Serper.dev for recent article URLs; runs semantic deduplication (`rapidfuzz token_sort_ratio >= 88`) against headlines seen in the last 72 h before scraping
3. Strips URL query parameters, computes SHA-256 hash → skips if already in DB
4. Extracts article markdown via Firecrawl; falls back to a direct-HTTP scraper on HTTP 402 (quota exhaustion); skips if below `min_article_length`
5. Calls OpenAI `gpt-5.4-nano` with article text; receives structured JSON including `headline`, `americhem_impact` (BLUF "so what"), `sentiment_score` (1–10, legacy directional), the relevance-upgrade fields `sentiment_tag` (Negative/Neutral/Positive), `americhem_impact_score` (1–10, **materiality** — independent of tone), `impact_rationale`, `commercial_segment` and `signal_type` (validated against the labels in `market_pulse_config.yaml`), and `recommended_action`. The model may return `{"americhem_impact": "DISCARD"}` to drop false-positive entity matches.
6. Upserts row into `daily_intelligence` table (unique constraint on `url_hash`)
7. After all articles are stored, calls `generate_macro_summary()` — a second OpenAI call that writes `executive_summary` and `macro_sentiment` to the `daily_summaries` table (keyed on `run_date`)
8. Enforces `MAX_DAILY_SCRAPES = 150` hard cap and `PIPELINE_DEADLINE_SECONDS = 600` wall-clock deadline (keeps runtime inside the GitHub Actions 15-min limit)

**`delivery_engine.py`** — Fetch → Format → Send

1. Queries `daily_intelligence` directly (last 24 h; 72 h on Mondays) via `fetch_todays_intelligence()` — does **not** use the `todays_intelligence` view at runtime
2. Calls `fetch_macro_summary()` to retrieve the executive summary written by ingestion
3. Renders a single-zone HTML email and sends via the **Resend HTTP API** (`POST https://api.resend.com/emails`) with exponential-backoff retry (5 attempts; retries on 429, 500, 502, 503, 504). `SMTP_PASS` env var holds the Resend API key (legacy name).

**Email layout — Commercial Segment Watch.** Rendering is one zone, produced by `_render_segment_watch_section()`. The pipeline inside `generate_html_email()`:

1. Runs `_apply_delivery_suppression()` — a deterministic seven-rule guardrail (product-listing URLs, job postings, generic market reports, unrelated-color results, exact and semantic headline duplicates, Enterprise / Cross-Segment low-impact). First match wins; counts and last-10 samples are recorded as the delivery-side suppression breakdown.
2. Filters to rows the `Scoring` object (from `scoring.py`) marks visible — `scorer.is_visible(r)`, i.e. materiality `>= visible_impact_threshold` (default 6). Materiality is `insight.effective_impact()` (`americhem_impact_score` if present, else `sentiment_score`).
3. Groups visible rows by `commercial_segment`, defaulting missing values to Enterprise / Cross-Segment.
4. Applies `max_visible_articles_per_segment` then `max_total_visible_articles`. Capped-out rows are dropped — there is no fallback section.
5. For groups with 2+ articles, calls `synthesize_thematic_paragraphs()` for an LLM-generated synthesis paragraph above the cards.
6. Writes `surfaced_count` and the merged ingestion+delivery `suppression_breakdown` back to today's `daily_summaries` row via `_update_delivery_summary_counts()` (idempotent on same-day retry).

Each card is rendered by `_render_card(item)` (the single, shipped card renderer) and displays the `impact_score`, `sentiment_tag`, `signal_type`, BLUF "So what", and a `CRITICAL` badge when a legacy row is `scoring.is_legacy_critical()`. The card shows **no** ACTION line — `recommended_action` is a suppression-policy override (job-posting escalation), not reader-facing copy; the segment is the block header, not an in-card badge. The old Critical Disruptions / Thematic Intelligence / Peripheral Signals zones have been removed (along with the pre-redesign `_render_card`/`_render_section` pair); integration tests still assert those section headers do not appear.

When `MARKET_PULSE_RUN_MODE=test`, both the subject line and the rendered HTML are marked: `[TEST]` prefix and an amber "TEST RUN · Jason-only QA output" banner row.

**Database** (`schema.sql`) — Two tables: `daily_intelligence` (articles) and `daily_summaries` (one macro summary row per run date). The `todays_intelligence` view adds an `alert_tier` column for ad-hoc queries; the unique index on `url_hash` is the deduplication gate.

`migrations/` holds incremental SQL applied via the Supabase SQL editor. `001_add_relevance_fields.sql` adds the relevance-upgrade columns (`sentiment_tag`, `americhem_impact_score`, `impact_rationale`, `strategic_segment`, `include_in_report`); existing DBs must run this before the current code stores rows. A fresh DB can initialize from `schema.sql` alone — it already contains those columns.

**`targets.yaml`** — The first of two non-technical control files. Add/remove entities here; no Python changes required. Top-level keys:

- `discovery.results_per_entity` / `lookback_hours` / `min_article_length` — discovery tuning
- **Entity-mode groups** (`search_mode: entity`): list entities under `entities:` as `{name, active}`; set `active: false` to pause without deleting
- **Concept-mode groups** (`search_mode: concept`): set `active: true/false` at the group level; define `include_any` (OR'd terms) and optional `include_all` / `exclude_any`
- `exclude_any` entries matching Moody's platform identifiers (e.g. `"source set 238658"`, `"PR wires"`) are silently dropped by `build_query()` — only real search terms become `-"term"` operators

**`market_pulse_config.yaml`** — The second control file. Tunes the report and the LLM's segment taxonomy without code changes:

- `reporting.visible_impact_threshold` (default 6) — minimum `americhem_impact_score` for an article to appear as a visible card; raise if the report feels noisy.
- `reporting.supporting_impact_threshold` (default 4) — rows above this but below the visible threshold can still feed thematic context.
- `reporting.max_visible_articles_per_segment` (default 3) and `reporting.max_total_visible_articles` (default 12) — prevent any one segment from dominating and cap total card count.
- `strategic_segments.<key>.label` / `description` — passed verbatim into RULE 4 of the synthesis system prompt via `_build_segment_rule()`. Editing labels/descriptions changes how the LLM classifies articles. Add new segments at the bottom; do **not** reorder keys.

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
- `RECIPIENT_EMAILS` is the **only** source for the Resend `to:` list — there are no hardcoded fallbacks. The production workflow injects the production recipient pool; the test workflow injects `TEST_RECIPIENT_EMAILS` instead. Swapping pools is therefore a workflow-level secret change.
- Report filtering uses `americhem_impact_score` (materiality), **not** `sentiment_tag` (tone). A Negative-low-impact article is excluded; a Negative-high-impact supply disruption appears prominently. Don't conflate the two.
- `MARKET_PULSE_RUN_MODE=test` is the single switch that marks both the subject line (`[TEST]`) and the HTML body (amber banner row). The test workflow sets it; production never does.

## Python Conventions

- Type hints on all function signatures; `Optional[T]` for nullable returns.
- Structured logging with `%s` placeholders — never f-strings in `logger.*()` calls.
- Specific exception handling — never bare `except:` or broad `except Exception` without logging `exc`.
