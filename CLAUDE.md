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
- `.github/workflows/market_pulse_test.yml` — manually-dispatched sandbox run. Sets `MARKET_PULSE_RUN_MODE=test`, routes mail to the `TEST_RECIPIENT_EMAILS` secret (Jason-only QA pool), and exposes `run_ingestion` / `send_email` input flags so you can re-render against existing rows without re-billing APIs. With `run_ingestion=false` no same-day test-mode macro-summary row exists (ingestion writes it), so `fetch_macro_summary` uses the **production** row read-only whenever it is strictly newer than the test candidate (recency ties keep the test row — the date-rollover grace) — the QA email keeps the executive summary and citations, and the test write-back stays a no-op on production accounting. The fallback is one-directional: production never reads test rows.

`delivery_engine_old.py` is a legacy snapshot — do not edit it; the active delivery code is `delivery_engine.py`.

## Architecture

`CONTEXT.md` is the companion glossary — the shared vocabulary for seams and domain terms (Insight, materiality, relevance thresholds, macro summary, relevance gate). Read it alongside this section when the naming matters.

The pipeline is two sequential scripts sharing a Supabase database, plus three seam modules — one owning suppression accounting, one owning every database call, and one owning every structured LLM call — plus `insight.py` (the per-article schema: taxonomies, normalization, field readers), `scoring.py` (the relevance thresholds the report applies), `report.py` (the report model and the pure report-assembly decision pipeline), and `prompts.py` (every LLM prompt the pipeline assembles):

**`suppression_ledger.py`** — Pure in-process module owning the suppression reason taxonomy (6 ingestion-owned + 9 delivery-owned codes), `SAMPLES_CAP = 10`, and the same-day-retry merge semantics. Used by both engines; performs zero I/O.

**`daily_intelligence_repo.py`** — Single seam for every Supabase query the pipeline makes. One Protocol (`IntelligenceRepo`), two adapters (`SupabaseIntelligenceRepo` for prod, `InMemoryIntelligenceRepo` for tests). Reads swallow exceptions and return an empty sentinel; writes raise so silent write failures crash the cron loudly. Callers do `from daily_intelligence_repo import _repo` and call `_repo()`; tests inject the fake at the consumer module — e.g. `monkeypatch.setattr("delivery_engine._repo", lambda: fake)`. The repo does not know about `SuppressionLedger` — the same-day-retry merge for delivery counts lives in `delivery_engine._update_delivery_summary_counts`.

**`llm.py`** — Single seam for every structured (JSON) OpenAI call the pipeline makes. One Protocol (`LLM`), two adapters (`OpenAILLM` for prod, `FakeLLM` for tests). The interface is `complete_json(*, system, user, temperature=None, context="") -> Optional[dict]`: it owns the OpenAI client, `OPENAI_MODEL`, the `response_format=json_object` request, and the response-envelope handling (`choices[0].message.content` extraction + `json.loads`). It does **not** own response *validation* — each caller validates the parsed dict itself, because the rules are domain-specific (relevance fields, executive bullets, free-form paragraphs). Failure contract: `complete_json` never raises; on a transport error, empty content, or unparseable JSON it logs and returns `None`, and each caller maps `None` to its own sentinel (`synthesize_insight` → `None`, `generate_macro_summary` → `False`, `synthesize_thematic_paragraphs` → `{}`). Callers do `from llm import _llm` and call `_llm()`; tests inject the fake at the consumer module — e.g. `monkeypatch.setattr("ingestion_engine._llm", lambda: FakeLLM(returns=...))`. The SDK-shape contract (model id, json format) is asserted once in `tests/test_llm.py`, not re-asserted in every caller test.

**`insight.py`** — Pure module owning the per-article **Insight** schema: the value taxonomies (`VALID_SENTIMENT_TAGS`, `VALID_COMMERCIAL_SEGMENTS`, `VALID_SIGNAL_TYPES`, `VALID_ACTIONS`), `normalize(raw) -> Optional[dict]` (the clamp/default/validate rules that turn a raw LLM dict into a storable row; returns `None` if a required key is missing), `is_discard(raw)`, and the field readers `effective_impact` / `commercial_segment` / `signal_type`. The row stays a plain dict (the shape the repo and renderer rely on) — this module concentrates the *knowledge* about it, not the format. `ingestion_engine.synthesize_insight` calls `normalize`; the field readers are consumed by `report.py` and `prompts.py`, which import them from `insight` directly (tests do the same — `delivery_engine` no longer re-exports them). The macro-summary schema (`dominant_condition` / `executive_bullets`) is a separate structured output and stays in `generate_macro_summary`. Schema rules are tested directly in `tests/test_insight.py`.

**`scoring.py`** — Pure module owning the relevance thresholds the report applies to an Insight's materiality. `Scoring.from_config(config)` resolves the configurable bands (`reporting.visible_impact_threshold` default 6, `supporting_impact_threshold` default 4) and exposes `is_visible(row)` / `is_weak_relevance(row)`; module-level `tier(row)` (CRITICAL ≤3 / STRATEGIC ≥8 / ROUTINE) and `is_legacy_critical(row)` use fixed edges. Builds on `insight.effective_impact` (the *what*); scoring is the *what it means for the report*. `report.assemble_report` constructs a `Scoring`; `delivery_engine` re-exports `tier` as `_alert_tier` and calls `is_legacy_critical` for the meta-strip badge. **Out of scope** (left where they are): the suppression policy's `enterprise_min_impact` (a suppression parameter) and `_sentiment_word`'s legacy directional-sentiment mapping (tone, not materiality). Tested directly in `tests/test_scoring.py`.

**`prompts.py`** — Pure module owning every LLM prompt the pipeline assembles (see the **prompt spec** entry in `CONTEXT.md`). Three builders — `insight_prompt(config, *, article_text, source_url, trigger_entity, category)`, `macro_prompt(articles)`, `thematic_prompt(groups)` — each return a frozen `PromptSpec` (`system`, `user`, `temperature`, `context`); callers splat it into the LLM seam with `_llm().complete_json(**spec.kwargs())`. Text assembly only: callers keep validation, `llm.py` keeps transport. Owns the single `ENGLISH_OUTPUT_RULE` (the old ingestion/delivery duplicate pair and its lockstep comment are gone) and the macro vocabulary (`VALID_MACRO_CONDITIONS`, `VALID_MACRO_DIRECTIONS`, `EXEC_BULLET_LABELS`, `MAX_EXECUTIVE_BULLET_CITATIONS`, `MAX_MACRO_OUTLOOK_SIGNALS`) that `generate_macro_summary`'s validators import — prompt promises and validator checks are one definition. `macro_prompt` also promises the `macro_outlook` contract (the third output key) with `direction` from `VALID_MACRO_DIRECTIONS` and `affected_segments` from the canonical `insight.VALID_COMMERCIAL_SEGMENTS` labels, and reserves `MACRO_OUTLOOK_SOURCE_PACK_QUOTA` pack slots for `signal_type == "Macro"` rows. `macro_prompt` ranks and caps the articles itself and returns `MacroPrompt` with `source_pack` attached: digest `[n]` markers and pack ids come from one enumeration, so the citation contract holds by construction. RULE 4/5 of the insight prompt are assembled from `market_pulse_config.yaml` (`commercial_segments` / `signal_types`) with fallback lists; assembly is `str.replace()`, never `.format()` — the base prompt's literal JSON braces are load-bearing (pinned by a test). `python scripts/show_prompts.py` dumps the assembled prompts (with `system_fingerprint`) for offline rewording/diffing at zero API spend. Prompt-contract tests live in `tests/test_prompts.py`.

### Discovery & enrichment subsystem (ZoomInfo)

A second, feature-flagged article-discovery path that supplements Serper. All flags default **off** — production behaviour is unchanged until each is explicitly enabled, and the ingestion engine does not write the discovery-provenance columns until `STORE_DISCOVERY_METADATA` is truthy.

**`zoominfo_client.py`** — ZoomInfo company-news discovery provider. Company/news enrichment **only** — never calls a Contact API and never returns contact data. Auth priority: OAuth client-credentials (`ZOOMINFO_CLIENT_ID` + `ZOOMINFO_CLIENT_SECRET`, token cached in-process) → static `ZOOMINFO_BEARER_TOKEN` → none (warn, return `[]`). Endpoints are override-able via env vars so a published-path change never needs code. Every failure mode is swallowed to `[]` — a ZoomInfo outage degrades discovery instead of crashing the cron; no secret or token is ever logged. Gated per-entity by `_zoominfo_target_eligible` (flag on + a mapped `zoominfo_company_id` + `zoominfo_news` not disabled); concept-mode targets carry no company id and short-circuit.

**`relevance_gate.py`** — Pure ZoomInfo false-positive suppressor (NOT a second entity resolver). ZoomInfo candidates are already company-linked; `evaluate` drops one only when a curated `exclude_term` appears AND no identity term (canonical name / identity terms / manual aliases) rescues it — identity rescue is checked first, absence of identity text alone never drops. `load_target_metadata` is the only I/O and swallows read/parse errors to `{}`, so a missing or malformed companion file silently disables the gate. Gated by `ZOOMINFO_RELEVANCE_GATE_ENABLED`; suppression reason is `zoominfo_company_mismatch`.

**`target_enricher.py`** + **`scripts/enrich_targets.py`** — An offline, operator-run utility (not part of the daily cron) that populates `target_metadata.yaml`. `target_enricher.py` is a pure, mock-free transform (ZoomInfo raw response + prior metadata → proposed metadata: status, confidence, conservative de-suffixed identity/industry terms; no acronyms); the clock and all I/O live in the CLI. Run with `python scripts/enrich_targets.py`. `target_metadata.yaml` is **machine-managed** — the CLI header marks which fields are hand-editable; do not hand-edit the generated ones.

**`ingestion_engine.py`** — Scrape → Synthesize → Store

1. Loads `targets.yaml` to get active targets — two modes: **entity** (one Serper query per company name) and **concept** (one combined OR query per group, gated by `active: true` at group level)
2. Gathers candidate URLs from up to **two discovery providers**: Serper.dev (always on) and, per-entity, ZoomInfo company-news enrichment (feature-flagged — see the discovery/enrichment subsystem below). Runs semantic deduplication (`rapidfuzz token_sort_ratio >= 88`) against headlines seen in the last 72 h before scraping. Serper results are truncated client-side to `results_per_entity` (the API returns pages of 10 regardless of `num`), and candidates on known-unscrapable domains (login-walled platforms, retail product pages — `UNSCRAPABLE_DOMAINS` / `UNSCRAPABLE_HOSTS`) are suppressed before any scrape with reason `unscrapable_domain`
3. Strips URL query parameters, computes SHA-256 hash → skips if already in DB
4. Extracts article markdown via Firecrawl; falls back to a direct-HTTP scraper on HTTP 402 (quota exhaustion); skips if below `min_article_length`. Each Firecrawl request runs under a 20-second wall-clock ceiling (`FIRECRAWL_WALL_CLOCK_TIMEOUT`) so a hung scrape can't eat the pipeline deadline
5. Calls OpenAI `gpt-5.4-nano` with article text; receives structured JSON including `headline`, `americhem_impact` (BLUF "so what"), `sentiment_score` (1–10, legacy directional), the relevance-upgrade fields `sentiment_tag` (Negative/Neutral/Positive), `americhem_impact_score` (1–10, **materiality** — independent of tone), `impact_rationale`, `commercial_segment` and `signal_type` (validated against the labels in `market_pulse_config.yaml`), and `recommended_action`. The model may return `{"americhem_impact": "DISCARD"}` to drop false-positive entity matches.
6. Upserts row into `daily_intelligence` table (unique constraint on `url_hash`)
7. After all articles are stored, calls `generate_macro_summary()` — a second OpenAI call that writes `executive_summary`, `macro_sentiment`, the structured `dominant_condition` / `executive_bullets`, `macro_outlook`, and `executive_sources` to the `daily_summaries` table (keyed on `run_date`). Each executive bullet may carry `citation_source_ids`. `macro_outlook` is the **Macroeconomic Outlook**: `{current_condition, signals:[{indicator, direction, americhem_implication, affected_segments, citation_source_ids}]}`, validated by `_validate_macro_outlook` — each signal must have a valid `direction` (`VALID_MACRO_DIRECTIONS`), canonical `affected_segments` (`insight.VALID_COMMERCIAL_SEGMENTS`), and **at least one valid citation** (the deterministic materiality gate: an uncitable signal is dropped; no surviving signal → `macro_outlook` is `null`). `executive_sources` is the packed list of sources cited by at least one surviving bullet **or** macro signal — the union (`[{id, headline, url, domain, segment, score}]`). Delivery renders inline citation markers (bullets *and* signals share one numbering space) plus a "Sources" list at the bottom of the email. `macro_prompt` reserves up to `MACRO_OUTLOOK_SOURCE_PACK_QUOTA` source-pack slots for `signal_type == "Macro"` rows so the outlook always has citable sources.
8. Enforces `MAX_DAILY_SCRAPES = 150` hard cap and `PIPELINE_DEADLINE_SECONDS = 1800` wall-clock deadline (keeps runtime inside the GitHub Actions 40-min job timeout)

**`delivery_engine.py`** — Fetch → Format → Send

1. Queries `daily_intelligence` directly (last 24 h; 72 h on Mondays) via `fetch_todays_intelligence()` — does **not** use the `todays_intelligence` view at runtime
2. Calls `fetch_macro_summary()` to retrieve the executive summary written by ingestion
3. Renders a single-zone HTML email and sends via the **Resend HTTP API** (`POST https://api.resend.com/emails`) with exponential-backoff retry (5 attempts; retries on 429, 500, 502, 503, 504). `SMTP_PASS` env var holds the Resend API key (legacy name).

**`report.py`** — Pure module owning the **report model** (`ReportModel`, a frozen plain-data value — the data seam between report assembly and rendering) and `assemble_report(rows, macro_summary, config)`, the decision pipeline:

1. Runs `_apply_delivery_suppression()` — a deterministic seven-rule guardrail (product-listing URLs, job postings, generic market reports, unrelated-color results, exact and semantic headline duplicates, Enterprise / Cross-Segment low-impact). First match wins; counts and last-10 samples are recorded as the delivery-side suppression breakdown.
2. Filters to rows the `Scoring` object (from `scoring.py`) marks visible — `scorer.is_visible(r)`, i.e. materiality `>= visible_impact_threshold` (default 6). Materiality is `insight.effective_impact()` (`americhem_impact_score` if present, else `sentiment_score`).
3. Groups visible rows by `commercial_segment`, defaulting missing values to Enterprise / Cross-Segment.
4. Applies `max_visible_articles_per_segment` then `max_total_visible_articles` **only when they are configured** — both default to `null` (uncapped), so by default every visible (materiality ≥ 6) article that survives suppression is shown. An integer re-imposes the cap (a config-only rollback); capped-out rows are dropped, there is no fallback section.
5. Selects the **optional-discovery appendix** (`ReportModel.additional_articles`): suppression-surviving rows in the weak-relevance band (`supporting ≤ effective_impact < visible`, i.e. score 4–5 today) that are not visible cards, ranked impact-desc → recency-desc (`published_at`, else `created_at`) → normalized headline → `url_hash`, capped at `reporting.max_additional_articles` (default 10). It does **not** affect `surfaced_count` (visible cards only). Enterprise / Cross-Segment low-impact rows are dropped by suppression rule 1 before this step, so they never appear in the appendix.
6. Folds the derived `below_impact_threshold` and `weak_relevance` counts into the ledger, so the model's ledger is the complete delivery-side accounting and the write-back consumes it verbatim. `weak_relevance` counts only weak-relevance rows shown **nowhere** (neither a visible card nor the appendix — e.g. pushed out by the appendix cap); `below_impact_threshold` is deliberately broader (every suppression-surviving below-visible row, including ones the appendix now displays).

`assemble_report` is pure and deterministic (no I/O, clock, or env reads; `config=None` means built-in defaults, never a file read); `rows == []` yields the `no_news` model variant. Tests exercise the whole pipeline with dict literals and zero patches, importing the suppression/grouping internals from `report` directly; `delivery_engine` imports only `ReportModel` / `assemble_report`.

**Email layout — Commercial Segment Watch.** Rendering is one zone, produced by `_render_segment_watch_section()`. `execute_pipeline` makes **two calls** — `prepare_report(rows, macro_summary)` then `render_report(model, ...)` — composing three stages (see the report-model entry in `CONTEXT.md`):

1. `assemble_report()` (from `report.py`, above) produces the `ReportModel`. In production this runs **inside** `prepare_report`, which takes raw rows — there is no model-in/model-out effectful call; tests and QA scripts wanting a pure model call `assemble_report` directly.
2. The effects half of `prepare_report()` — the run's **single effectful step**, exactly once per delivery run, skipped entirely for the `no_news` variant: writes `surfaced_count` and the merged ingestion+delivery `suppression_breakdown` back to today's `daily_summaries` row via `_update_delivery_summary_counts()` (idempotent on same-day retry), then fills `model.synthesis` via `synthesize_thematic_paragraphs()` (only `model.synthesis_candidates()` — the final capped groups with 2+ articles; `{}` on LLM failure).
3. `render_report(model, *, today_str, test_mode)` — **pure**: the clock and the `MARKET_PULSE_RUN_MODE` resolution belong to the caller (`execute_pipeline`, whose wiring is pinned by the delivery execute-pipeline tests). Dispatches on `model.variant` — the no-news email is a model variant, not a separate code path. A model whose `synthesis` is empty renders bullets-only; that *is* the fallback, so tests render unprepared models directly.

Between the executive summary and Commercial Segment Watch, `render_report` emits the **Macroeconomic Outlook** via `_render_macro_outlook_section(model.macro_outlook, macro_summary)` — a one-line current condition plus one compact escaped row per material signal (indicator, colored direction, Americhem implication, affected segments, inline citation). Omitted entirely when `model.macro_outlook` is `None` or has no signals. Its citation markers share the email's single numbering space with the executive bullets (`_citation_display_map` enumerates bullets then signals), and the bottom "Sources" list covers both.

Below Commercial Segment Watch (and above the Sources footer) `render_report` emits the **Additional Articles to Explore** appendix via `_render_additional_articles_section(model.additional_articles)` — a compact, escaped, `_safe_http_url`-guarded list (linked headline + segment / impact / source / date meta, **no** "So what" narrative; the date renders only from `published_at`, never the scrape timestamp). The section is omitted entirely when `additional_articles` is empty.

Each card is rendered by `_render_card(item)` (the single, shipped card renderer) and displays the `impact_score`, `sentiment_tag`, `signal_type`, BLUF "So what", and a `CRITICAL` badge when a legacy row is `scoring.is_legacy_critical()`. The card shows **no** ACTION line — `recommended_action` is a suppression-policy override (job-posting escalation), not reader-facing copy; the segment is the block header, not an in-card badge. The old Critical Disruptions / Thematic Intelligence / Peripheral Signals zones have been removed (along with the pre-redesign `_render_card`/`_render_section` pair); integration tests still assert those section headers do not appear.

When `MARKET_PULSE_RUN_MODE=test`, both the subject line and the rendered HTML are marked: `[TEST]` prefix and an amber "TEST RUN · Jason-only QA output" banner row.

**Database** (`schema.sql`) — Two tables: `daily_intelligence` (articles) and `daily_summaries` (one macro summary row per run date). The `todays_intelligence` view adds an `alert_tier` column for ad-hoc queries; the unique index on `url_hash` is the deduplication gate.

`migrations/` holds incremental SQL applied via the Supabase SQL editor; each file is idempotent (safe to re-run). A fresh DB can initialize from `schema.sql` alone — it already contains the current columns; existing DBs must apply the migrations in order before running the matching code:

- `001_add_relevance_fields.sql` — relevance-upgrade columns (`sentiment_tag`, `americhem_impact_score`, `impact_rationale`, `strategic_segment`, `include_in_report`).
- `002_split_segment_and_structured_summary.sql` — splits `commercial_segment` from `signal_type`; adds the structured macro-summary fields (`dominant_condition`, `executive_bullets`), `run_mode` isolation, and suppression counts/samples.
- `003_add_discovery_metadata.sql` — multi-provider discovery provenance (`discovery_source`, `external_company_id`, `published_at`, `source_metadata`). **Rollout order matters**: merge the ZoomInfo code (flags off) → apply this migration → set `STORE_DISCOVERY_METADATA=true`.
- `004_add_executive_sources.sql` — `executive_sources` jsonb for citations. **Required, not flag-gated** — apply this migration *before* deploying the citation code, or ingestion upserts crash and delivery's summary blanks out.
- `005_add_macro_outlook.sql` — `macro_outlook` jsonb for the Macroeconomic Outlook section. **Required, not flag-gated** (same contract as 004) — apply *before* deploying the macro-outlook code, or `generate_macro_summary` upserts crash and delivery's summary blanks out. Stored `null` when no material signal exists.

**`targets.yaml`** — The first of two non-technical control files. Add/remove entities here; no Python changes required. Top-level keys:

- `discovery.results_per_entity` / `lookback_hours` / `min_article_length` — discovery tuning
- **Entity-mode groups** (`search_mode: entity`): list entities under `entities:` as `{name, active}`; set `active: false` to pause without deleting
- **Concept-mode groups** (`search_mode: concept`): set `active: true/false` at the group level; define `include_any` (OR'd terms) and optional `include_all` / `exclude_any`
- **Macro groups** (`macro_manufacturing`, `macro_construction`, `macro_automotive`, `macro_consumer_demand`, `macro_inflation_rates`, `macro_energy_freight`, `macro_business_investment`): dedicated concept groups feeding the Macroeconomic Outlook. They **must stay last** in file order — `load_targets()` processes groups top-to-bottom, so a deadline-limited run sacrifices macro coverage before entity coverage (graceful degradation). The old generic `economic` group was absorbed here; do not reintroduce it.
- `exclude_any` entries matching Moody's platform identifiers (e.g. `"source set 238658"`, `"PR wires"`) are silently dropped by `build_query()` — only real search terms become `-"term"` operators

**`market_pulse_config.yaml`** — The second control file. Tunes the report and the LLM's segment taxonomy without code changes:

- `reporting.visible_impact_threshold` (default 6) — minimum `americhem_impact_score` for an article to appear as a visible card; raise if the report feels noisy.
- `reporting.supporting_impact_threshold` (default 4) — rows above this but below the visible threshold feed thematic context **and** populate the Additional Articles appendix (the eligibility band is `Scoring.is_weak_relevance`).
- `reporting.max_visible_articles_per_segment` and `reporting.max_total_visible_articles` (both default `null` = uncapped) — set either to an integer to cap per-segment / total visible cards if the report gets noisy. Config-only, no code change.
- `reporting.max_additional_articles` (default 10) — cap on the Additional Articles to Explore appendix.
- `strategic_segments.<key>.label` / `description` — passed verbatim into RULE 4 of the synthesis system prompt via `_build_segment_rule()`. Editing labels/descriptions changes how the LLM classifies articles. Add new segments at the bottom; do **not** reorder keys.

## Tests

`tests/test_pipeline.py` covers: URL normalization (query params/fragments stripped), SHA-256 hash collision (UTM-polluted vs. clean URL must hash identically), sentiment score clamping to [1, 10], and `load_targets()` filtering inactive entities. The pure seam/schema modules are tested directly and in isolation — `test_insight.py`, `test_scoring.py`, `test_suppression_ledger.py`, `test_intelligence_repo.py`, `test_llm.py` (the SDK-shape contract, asserted once), `test_prompts.py` (the prompt contracts: English rule, RULE 4/5 config injection, macro digest↔source-pack id parity, the JSON-brace guard — no fakes, no patching). The ZoomInfo/enrichment subsystem has its own coverage — `test_zoominfo.py`, `test_zoominfo_company.py`, `test_relevance_gate.py`, `test_target_enricher.py`, `test_enrich_targets_cli.py`. All external API clients (OpenAI, Supabase, Serper, Firecrawl, ZoomInfo) are mocked or use the in-memory adapter — no live calls in the test suite.

## Key Invariants

- URL normalization (strip query params) MUST happen before hashing — this is the sole deduplication mechanism.
- `source_url` is injected into the LLM prompt so the model returns the canonical URL deterministically.
- `SUPABASE_KEY` must be the **Service Role** key (not anon) to bypass Row Level Security.
- `MAX_DAILY_SCRAPES = 150` — all three APIs (Serper, Firecrawl, OpenAI) are on paid-tier subscriptions. Adjust only if subscription tiers change.
- `PIPELINE_DEADLINE_SECONDS = 1800` — ingestion stops early if the wall clock exceeds 30 min, then flushes stats and calls `generate_macro_summary()` before returning. Raised from 600 on 2026-07-17 after the budget-efficiency fixes (PR #42): the remaining per-target cost is genuine scrape+LLM work, and full coverage of ~120 targets needs ~25–35 min. `MAX_DAILY_SCRAPES = 150` remains the API-cost guard.
- Monday delivery uses a 72-hour lookback (vs. 24 h on other days) to capture weekend news — this logic lives in `fetch_todays_intelligence()`.
- `SMTP_PASS` env var holds the **Resend API key** (legacy name retained to avoid secret rotation). Email is sent via `POST https://api.resend.com/emails`, not SMTP.
- `RECIPIENT_EMAILS` is the **only** source for the Resend `to:` list — there are no hardcoded fallbacks. The production workflow injects the production recipient pool; the test workflow injects `TEST_RECIPIENT_EMAILS` instead. Swapping pools is therefore a workflow-level secret change.
- Report filtering uses `americhem_impact_score` (materiality), **not** `sentiment_tag` (tone). A Negative-low-impact article is excluded; a Negative-high-impact supply disruption appears prominently. Don't conflate the two.
- `MARKET_PULSE_RUN_MODE=test` is the single switch that marks both the subject line (`[TEST]`) and the HTML body (amber banner row). The test workflow sets it; production never does.
- ZoomInfo discovery is gated by three independent flags, all defaulting **off**: `ZOOMINFO_NEWS_ENABLED` (turns the provider on), `ZOOMINFO_RELEVANCE_GATE_ENABLED` (turns the false-positive gate on), `STORE_DISCOVERY_METADATA` (persists provenance columns — leave off until migration 003 is applied). Enabling them out of order degrades gracefully; it does not crash the cron.

## Python Conventions

- Type hints on all function signatures; `Optional[T]` for nullable returns.
- Structured logging with `%s` placeholders — never f-strings in `logger.*()` calls.
- Specific exception handling — never bare `except:` or broad `except Exception` without logging `exc`.
