# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

Automated daily market intelligence pipeline (for Americhem) that replaces a Moody's News Edge subscription. Scrapes open-web news, applies LLM synthesis, and delivers a BLUF-formatted HTML email to stakeholders.

Three documents carry the detail this file only maps: `CONTEXT.md` (the glossary — read it when naming matters), `docs/architecture.md` (the full per-module reference, including the dated history behind each rule) and `docs/adr/`. Read `docs/prompt-engineering.md` before rewording any LLM prompt rule.

## Commands

```bash
pip install -r requirements.txt
cp .env.example .env                 # populate before running locally

python ingestion_engine.py           # full pipeline: ingestion, then
python delivery_engine.py            # delivery

pytest tests/                        # all tests (CI runs Python 3.10)
pytest tests/test_report.py::test_name

python scripts/show_prompts.py       # dump assembled prompts + fingerprint, zero API spend
python scripts/backtest_scorer.py csv  # scorer acceptance test (needs OPENAI_API_KEY; CI: "Scorer Backtest")
```

No linter or formatter is configured.

Workflows: `market_pulse.yml` (production, Mon–Fri 10:00 UTC, ingestion then delivery; `-f run_ingestion=false` re-sends without re-scraping) and `market_pulse_test.yml` (manual QA run: `MARKET_PULSE_RUN_MODE=test`, mail to `TEST_RECIPIENT_EMAILS`, `run_ingestion` / `send_email` inputs). Test-mode rows are tagged `run_mode='test'` and never reach a production email; a delivery-only test run reads the production summary row read-only.

## Runbooks

- `docs/runbooks/no-email-but-green-run.md` — the on-call path for "the run was green, Resend says Delivered, nobody received the digest" (the 2026-09-16 Proofpoint quarantine): log lines, Resend checks, what to ask Americhem IT for, the delivery-only re-send, and how to block a domain. `tests/test_runbooks.py` pins every command and key it quotes (and that this file links it).

## Architecture

Two sequential scripts share a Supabase database:

- **`ingestion_engine.py`** — load targets → discover candidates (Serper, optionally ZoomInfo; hand-supplied seed URLs) → per-candidate **gauntlet** (`process_candidate` → `Stored | Suppressed(reason) | Error`: security block, link reputation, market-report/unscrapable gates, URL-hash and fuzzy-headline dedup, Firecrawl scrape, LLM synthesis, upsert) under the **run budget** → `_finalize_run`: one macro-summary LLM call written to `daily_summaries` (an accounting-only row when nothing can be summarized), then the synthesis-outage guard.
- **`delivery_engine.py`** — `resolve_summary_row(run)` (the summary row and its key) → fetch the **delivery window** → `prepare_report` (pure `report.assemble_report` + the one effectful write-back + thematic synthesis) → `renderer.render_report` → `send_email` → stamp `delivered_at`.

Modules around them, by role:

| Role | Modules |
| --- | --- |
| **Protocol seams** — one Protocol, a prod adapter and a fake; callers do `from x import _x` and call `_x()` | `daily_intelligence_repo` (every Supabase call), `llm` (every structured OpenAI call), `mailer` (Resend), `link_reputation` (Google Safe Browsing; off without `SAFE_BROWSING_API_KEY`), `discovery` (Serper / ZoomInfo providers) |
| **Run values** — frozen, built once in `main()` and threaded | `run_instant` (the only clock + run-mode read; `RunInstant`, `SummaryKey`, `visible_modes`), `run_budget` (scrape cap, deadline, tail reserve → `Proceed` / `SkipEntity` / `Stop`) |
| **Schemas** | `insight` (per-article row: taxonomies, `normalize`, field readers), `macro_summary` (run-level twin: `assemble_macro_content` write face, `MacroSummary` read face), `suppression_ledger` (reason taxonomy; `SuppressionLedger` writer, `SuppressionAccounting` reader) |
| **Pure policy** | `scoring` (relevance thresholds), `report` (`ReportModel`, `CitationSet`, the suppression rules, segment grouping/caps, Watch List, appendix), `renderer` (model → HTML; no logger), `prompts` (every prompt as a `PromptSpec`), `market_reports`, `blocked_domains`, `relevance_gate` |
| **Config** | `config` (every env/YAML read; `validate_environment` fails fast at startup), `targets` (sole parser of `targets.yaml`; `TargetsError` on any shape error) |
| **Offline tools** | `scripts/` (`enrich_targets`, `review_resolution`, `sync_zoominfo_ids`, `probe_serper`, `embed_logo`, `show_prompts`, `backtest_scorer`), `target_enricher.py` |

Control files: `targets.yaml` (what to search; **group order is the degradation policy** — Tier 0 `seed_urls` (hand-picked articles, never searched), Tier 1 priority-segment concepts, Tier 2 entities, Tier 3 other concepts, Tier 4 `macro_*` groups last) and `market_pulse_config.yaml` (report thresholds/caps, segment taxonomy, `security.blocked_domains`). Database: `schema.sql` plus idempotent `migrations/NNN_*.sql`, applied by hand in the Supabase SQL editor.

## Conventions that span files

- **Pure modules** (the table in `tests/test_purity.py` is the source of truth) do no I/O and read no clock, env or config file: they take a plain config dict (`prepare_report(..., report_config=...)`, `Scoring.from_config(cfg)`).
- **Repo failure contract:** reads swallow errors and return an empty value; writes raise. Two reads are strict on purpose (`require_delivery_state`, `fetch_since`) because a write depends on them. `llm.complete_json` never raises — it returns `None` and each caller validates the dict itself.
- **Prompt assembly uses `str.replace()`, never `.format()`** — the prompts contain literal JSON braces. Prompt vocabulary lives in `prompts.py`, and the validators in `macro_summary.py` import it rather than redefining it.
- **Renderer:** every interpolated value is HTML-escaped (`str()` first), and every href goes through `_link` / `_safe_http_url`. `test_every_interpolated_data_value_is_escaped` enforces both across the whole email.

## Tests

- A test lives with the module whose **output** it asserts on (a test that builds a `ReportModel` only to render it is a `test_renderer.py` test). Pins against the real control files live in `tests/test_targets.py`. Each test file's docstring lists what it covers.
- Inject fakes **at the consumer**: `monkeypatch.setattr("delivery_engine._repo", lambda: fake)`, `patch("config.mp_config", ...)`. Autouse fixtures install a `FakeMailer` and a `FakeLinkReputation`, so no test can reach Resend or Safe Browsing by omission; take `fake_mailer` to assert on what was sent.
- Pass `tests/conftest.py`'s `RUN_INSTANT` / `TEST_RUN_INSTANT` instead of patching `datetime` or setting `MARKET_PULSE_RUN_MODE`. Pass budget overrides through the harness's `limits=` knob, never by overwriting module constants.
- End-to-end runs go through `run_ingestion_pipeline` (`targets=[...]` or `targets_yaml="..."`; the latter uses the real `load_targets`) and `run_delivery_pipeline`. `tests/test_pipeline_harness.py` fails when a pipeline gains a call its harness neither stubs nor lists in `deliberately_real`, and when a harness stubs a call that no longer exists. Use the shared builders (`stub_row`, `stub_insight`, `stub_target`, `stub_summary_row`, …) and add local `functools.partial`s over them rather than new builders.
- No live API calls anywhere in the suite.

## Key Invariants

- URL normalization (strip query params) MUST happen before hashing — the unique `url_hash` is the only deduplication mechanism, and it is unique across run modes (ADR 0001).
- `source_url` is injected into the LLM prompt so the model returns the canonical URL deterministically.
- `SUPABASE_KEY` must be the **Service Role** key (not anon) to bypass Row Level Security.
- `MAX_DAILY_SCRAPES = 180` and `PIPELINE_DEADLINE_SECONDS = 1800` (`run_budget.py`) are the API-cost and CI-timeout guards; change them only if subscription tiers or the 40-min job timeout change. The tail reserve skips entity targets so concept/macro groups always run.
- A **synthesis outage exits non-zero** (`SynthesisOutageError` → `sys.exit(1)`) after persisting the accounting row. The delivery step must stay conditional on ingestion's success — **never add `if: always()`** in either workflow; the red job and the missing email are the alarm.
- No `date.today()` / `datetime.now()` / `config.run_mode()` read inside an engine module: `main()` calls `RunInstant.current()` once and threads it (pinned in `tests/test_run_instant.py`). Lookback windows use `run_instant.naive_utcnow()`. Delivery's writes take the **summary key it read**, never one derived from its own clock (the two steps can disagree about `run_date` near midnight UTC).
- The delivery window is anchored to the last **production** `delivered_at` on an earlier `run_date`; the 24 h / 72 h-Monday lookback is only the fallback. `delivered_at` is stamped with the fetch instant **after** a successful send, and a failed stamp is a warning, not a red job.
- **A test-mode row never reaches a production email:** each run reads `visible_modes(run_mode)`; a production write replaces a test row, and a test write is `ON CONFLICT DO NOTHING`.
- A **security-blocked domain** (`security.blocked_domains`) never reaches an email — ingestion gate, delivery rule 9 and citations all read `blocked_domains.py`; a mis-shaped list fails at t=0. Blocking a domain is a config edit, and un-blocking re-admits its stored rows.
- The link-reputation check can only remove things and can never turn a run red.
- Report filtering uses `americhem_impact_score` (materiality), **not** `sentiment_tag` (tone). Don't conflate them.
- **Migrations go first:** apply each migration before deploying the code that reads its columns (004–006 and 008 crash or red-fail otherwise). ZoomInfo's three flags (`ZOOMINFO_NEWS_ENABLED`, `ZOOMINFO_RELEVANCE_GATE_ENABLED`, `STORE_DISCOVERY_METADATA`) all default off; leave the last off until migration 003 is applied.
- Mail: `SMTP_PASS` holds the **Resend API key** (legacy name). `RECIPIENT_EMAILS` is the only `to:` source, with no hardcoded fallback. `MARKET_PULSE_RUN_MODE=test` marks both the subject and the body via `renderer.TEST_MARKER`.
- Control-file edits that are not config-only:
  - A new commercial segment must also be added to `insight.VALID_COMMERCIAL_SEGMENTS` and `prompts._FALLBACK_COMMERCIAL_SEGMENT_LIST` (append at the bottom, never reorder).
  - A new `macro_*` group in `targets.yaml` must also be listed in `reporting.appendix_exclude_categories`.
  - Retire a target group with `active: false`, not by emptying its terms; the whole file, inactive entries included, is validated.
- `target_metadata.yaml` is machine-managed: edit only the fields its header marks hand-editable.

## Python Conventions

- Type hints on all function signatures; `Optional[T]` for nullable returns.
- Structured logging with `%s` placeholders — never f-strings in `logger.*()` calls.
- Specific exception handling — never bare `except:` or broad `except Exception` without logging `exc`.
