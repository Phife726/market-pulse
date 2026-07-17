# Context — domain & architecture vocabulary

Shared names for the things this codebase is made of. Architecture terms (module,
interface, seam, adapter) follow the conventions in the architecture-review
language; domain terms are specific to the Americhem market-intelligence pipeline.

## Seams

A **seam** is where an interface lives — a place behaviour can be swapped without
editing in place. This pipeline has three, each a pure module with a Protocol and
two adapters (one production, one in-memory for tests):

- **Repo seam** (`daily_intelligence_repo.py`, `IntelligenceRepo`) — every Supabase
  query. Adapters: `SupabaseIntelligenceRepo`, `InMemoryIntelligenceRepo`. Reads
  swallow and return a sentinel; writes raise.
- **LLM seam** (`llm.py`, `LLM`) — every structured (JSON) OpenAI call. Adapters:
  `OpenAILLM`, `FakeLLM`. Interface: `complete_json(*, system, user,
  temperature=None, context="") -> Optional[dict]`. Owns the client, `OPENAI_MODEL`,
  the `json_object` response format, and envelope handling (content extraction +
  `json.loads`). Never raises — returns `None` on any failure; the caller maps
  `None` to its own sentinel and does its own domain validation. Does **not** own
  response validation.
- **Suppression ledger** (`suppression_ledger.py`, `SuppressionLedger`) — the
  suppression reason taxonomy, `SAMPLES_CAP`, and same-day-retry merge semantics.
  Pure value type; both engines record into it.

Tests inject the in-memory adapter at the consumer module, e.g.
`monkeypatch.setattr("ingestion_engine._llm", lambda: FakeLLM(returns=...))`.

A fourth seam is data-shaped rather than Protocol-shaped: the **report model**
(see Domain terms) — a plain frozen value between report assembly and rendering.
It has no adapters; behaviour on either side of it is swapped by composing the
pure functions differently, not by injection.

## Domain terms

- **Insight** — the structured JSON the LLM returns per article: `headline`,
  `americhem_impact` (BLUF "so what"), `sentiment_score`, `sentiment_tag`,
  `americhem_impact_score`, `commercial_segment`, `signal_type`,
  `recommended_action`. May be the `DISCARD` sentinel to drop a false-positive
  entity match. Travels as a plain dict; its schema — taxonomies, the
  `normalize` clamp/default rules, and the `effective_impact` /
  `commercial_segment` / `signal_type` readers — lives in `insight.py`.
- **Materiality** (`americhem_impact_score`, 1–10) — how much an article matters to
  Americhem, independent of tone. The report filters on materiality, **not** on
  `sentiment_tag` (tone). `insight.effective_impact` reads it (with the legacy
  `sentiment_score` fallback).
- **Relevance thresholds** — what a materiality score means for the report:
  **visible** (≥ `visible_impact_threshold`, default 6), **weak-relevance**
  (supporting context, `supporting_impact_threshold ≤ score < visible`), and the
  **alert tiers** (CRITICAL ≤3 / STRATEGIC ≥8 / ROUTINE). All live in `scoring.py`
  (`Scoring.from_config`, `tier`, `is_legacy_critical`).
- **Macro summary** — the once-per-run brief (`dominant_condition` +
  `executive_bullets` + `macro_outlook`) written to `daily_summaries`.
- **Accounting-only summary row** — the `daily_summaries` row a run persists
  when it cannot generate a macro summary (zero stored articles, or an
  unusable LLM response): `run_date`/`run_mode` plus `screened_count` and the
  suppression breakdown/samples, with every content column **omitted** from
  the upsert payload (Supabase updates only provided columns, so a same-day
  retry never wipes an earlier full summary). Delivery renders it summary-less
  (no Executive Summary / Macroeconomic Outlook), and in the test-mode
  fallback `_summary_has_content` ranks content-fullness before recency so an
  accounting-only row never shadows a content-full one.
- **Macroeconomic Outlook** (`macro_outlook`) — the structured macro read:
  `{current_condition, signals:[{indicator, direction, americhem_implication,
  affected_segments, citation_source_ids}]}`. Validated at ingestion by
  `_validate_macro_outlook`: every signal needs a valid `direction`
  (`prompts.VALID_MACRO_DIRECTIONS`), canonical `affected_segments`
  (`insight.VALID_COMMERCIAL_SEGMENTS`), and **at least one valid citation**
  (the materiality gate — an uncitable signal is dropped; no surviving signal
  → `null`). Carried on `ReportModel.macro_outlook`, rendered between the
  executive summary and Commercial Segment Watch. Its citations share one
  numbering space with the executive bullets, and `executive_sources` is the
  **union** of bullet- and signal-cited sources.
- **Commercial Segment Watch** — the primary rendered email zone, grouped by
  `commercial_segment`.
- **Additional Articles to Explore** — the optional-discovery appendix
  (`ReportModel.additional_articles`): suppression-surviving rows scoring at
  or above the supporting threshold (≥ 4) that are not visible cards — the
  weak-relevance band plus cap overflow — ranked deterministically and
  capped at `reporting.max_additional_articles` (default 10). Rendered
  compactly below Commercial Segment Watch, without the "So what" narrative.
  Never affects `surfaced_count`. Rows shown here are excluded from the
  `weak_relevance` count (but still counted in the broader
  `below_impact_threshold`).
- **Report model** (`report.py`, `ReportModel`) — the assembled daily report as
  plain frozen data: `variant` (`daily` / `no_news`), the final segment groups
  (capped only when configured; caps default to `null` = uncapped),
  `additional_articles` (the optional-discovery appendix — see below),
  `macro_outlook` (the renderable Macroeconomic Outlook, or `None`),
  `surfaced_count` / `screened_count`, the delivery-side suppression
  ledger (including the derived `below_impact_threshold` and `weak_relevance`
  counts), the raw macro-summary row, and the thematic synthesis paragraphs.
  Produced by `assemble_report` (pure decision pipeline: delivery suppression →
  visibility filter → segment grouping → optional per-segment cap → optional
  total cap → appendix selection → weak-relevance accounting). Consumed by the pure renderer
  (`delivery_engine.render_report`) and the `daily_summaries` write-back.
  `delivery_engine.prepare_report(rows, macro_summary)` runs assembly itself
  (there is no model-in/model-out effectful call), then performs the run's two
  side effects — write-back + thematic synthesis — exactly once, after
  assembly and before rendering; both are skipped for `no_news`. Rendering a
  model whose synthesis is empty **is** the bullets-only fallback.
- **Relevance gate** — the ZoomInfo false-positive suppression rule
  (`relevance_gate.py`), applied to ZoomInfo candidates during ingestion.
- **Prompt spec** (`prompts.py`, `PromptSpec` / `MacroPrompt`) — a fully
  assembled structured-LLM call as plain frozen data (`system`, `user`,
  `temperature`, `context`; `spec.kwargs()` splats into the LLM seam).
  `prompts.py` is the pure module owning every prompt the pipeline assembles —
  text assembly only: callers keep validation, the LLM seam keeps transport.
  It owns the single `ENGLISH_OUTPUT_RULE` and the macro vocabulary
  (`VALID_MACRO_CONDITIONS`, `EXEC_BULLET_LABELS`, the citation cap), which
  the macro validators import — the prompt's promises and the validator's
  checks are one definition. `MacroPrompt.source_pack` is the digest's
  citation index: digest `[n]` markers and pack ids come from one enumeration,
  so the citation contract holds by construction. `system_fingerprint`
  identifies the prompt wording in logs; `scripts/show_prompts.py` dumps the
  assembled prompts for offline rewording/diffing.
