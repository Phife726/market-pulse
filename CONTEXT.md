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
  `executive_bullets`) written to `daily_summaries`.
- **Commercial Segment Watch** — the single rendered email zone, grouped by
  `commercial_segment`.
- **Relevance gate** — the ZoomInfo false-positive suppression rule
  (`relevance_gate.py`), applied to ZoomInfo candidates during ingestion.
