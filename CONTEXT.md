# Context — domain & architecture vocabulary

Shared names for the things this codebase is made of. Architecture terms (module,
interface, seam, adapter) follow the conventions in the architecture-review
language; domain terms are specific to the Americhem market-intelligence pipeline.

## Seams

A **seam** is where an interface lives — a place behaviour can be swapped without
editing in place. This pipeline has five, each a module with a Protocol and
production + in-memory adapters (tests inject the fake at the consumer):

- **Repo seam** (`daily_intelligence_repo.py`, `IntelligenceRepo`) — every Supabase
  query. Adapters: `SupabaseIntelligenceRepo`, `InMemoryIntelligenceRepo`. Reads
  swallow and return a sentinel; writes raise; the two reads a write depends on
  (`require_delivery_state`, `fetch_since`) are strict and raise.
- **LLM seam** (`llm.py`, `LLM`) — every structured (JSON) OpenAI call. Adapters:
  `OpenAILLM`, `FakeLLM`. Interface: `complete_json(*, system, user,
  temperature=None, context="") -> Optional[dict]`. Owns the client, `OPENAI_MODEL`,
  the `json_object` response format, and envelope handling (content extraction +
  `json.loads`). Never raises — returns `None` on any failure; the caller maps
  `None` to its own sentinel and does its own domain validation. Does **not** own
  response validation.
- **Mailer seam** (`mailer.py`, `Mailer`) — the one outbound email transport.
  Interface: `send(message: EmailMessage) -> None`, where **`EmailMessage`** is
  a frozen value (`sender`, `recipients`, `subject`, `html`) — the composed
  digest, transport-agnostic. Adapters: `ResendMailer` (owns the Resend API
  key `SMTP_PASS` — legacy name —, the endpoint, the retry policy: transient
  HTTP codes retry with exponential backoff, everything else propagates at
  once) and `FakeMailer` (records every message; `fail_with` raises). The
  consumer, `delivery_engine.send_email(html, *, run)`, keeps the
  *addressing* — it reads `SENDER_EMAIL` / `RECIPIENT_EMAILS` (the only source
  of the `to:` list) and composes the subject from the **run instant** — then
  calls `_mailer().send(message)`; `execute_pipeline` never sees the seam.
  Retry policy and request shape are asserted once, at the adapter
  (`tests/test_mailer.py`); consumer tests inject `FakeMailer` and assert on
  the message that crossed the seam.
- **Discovery seam** (`discovery.py`, `DiscoveryProvider`) — how the ingestion
  engine consumes article-discovery providers. Interface: `name`,
  `eligible(target) -> bool`, `discover(target) -> list[dict]` (provider-neutral
  candidate dicts), `gate(candidate, target) -> Optional[GateDecision]` (an
  optional false-positive post-filter the *consumer* applies, so suppression
  accounting stays in the ledger). Adapters: `SerperProvider` (always eligible,
  never gates), `ZoomInfoProvider` (feature-flagged via `config`, owns the
  ZoomInfo relevance gate — loads `target_metadata.yaml` itself), and the fake
  `FakeDiscoveryProvider`. The consumer (`ingestion_engine.discover_candidates` +
  `execute_pipeline`) fans in `_discovery_providers()` in registry order (Serper
  before ZoomInfo), so provider_yield seeding, gate dispatch, and eligibility are
  all provider-list-driven — no `"serper"` / `"zoominfo"` literal leaks into the
  loop. Candidates stay plain dicts (like Insight).
- **Suppression ledger** (`suppression_ledger.py`, `SuppressionLedger`) — the
  suppression reason taxonomy, `SAMPLES_CAP`, and same-day-retry merge semantics.
  Pure value type; both engines record into it. Side-tagged: a ledger only ever
  accumulates codes its own side owns, so it is the taxonomy's *write* face.
  **Suppression accounting** (`SuppressionAccounting`) — the same taxonomy read
  from the other end: what a stored summary row *records*, as against what a run
  *accumulates*. A stored row is merged and two-sided — delivery's write-back
  keeps ingestion's codes — so it carries no side at all. It is the one reader of
  that shape, tolerant of the ways a stored row goes ragged, and what the
  test-mode QA block renders.
  *Avoid*: suppression view (the **renderer** entry reserves "view"), suppression
  report, suppression summary (that names the QA block that renders it, not the
  value).

Tests inject the in-memory adapter at the consumer module, e.g.
`monkeypatch.setattr("ingestion_engine._llm", lambda: FakeLLM(returns=...))`.

One further seam is data-shaped rather than Protocol-shaped: the **report model**
(see Domain terms) — a plain frozen value between report assembly and rendering.
It has no adapters; behaviour on either side of it is swapped by composing the
pure functions differently, not by injection.

## Config

`config.py` concentrates every runtime configuration read the two engines make:
`mp_config()` (the cached `market_pulse_config.yaml` load), `run_mode()`,
`env_int()`, the ZoomInfo feature flags (`zoominfo_news_enabled`,
`relevance_gate_enabled`, `store_discovery_metadata`), and
`validate_environment(engine)` — a fail-fast startup check (driven by
`REQUIRED_SECRETS`, raising `MissingEnvironmentError`) that each engine's
`main()` runs before any API spend, so a misconfigured cron crashes at t=0
instead of part-way through. It is **not** a Protocol seam: it has no adapters,
because the Protocol seams (`llm`, `daily_intelligence_repo`, `mailer`,
`zoominfo_client`, and `discovery`'s `ZoomInfoProvider`) keep reading their own secrets / feature
flags at use time — config only *validates their presence* and owns the flag
*values*, it does not own the seams' values. The pure
report/scoring/prompt modules never import it: they receive a plain config dict
as a parameter (e.g. `prepare_report(..., report_config=...)`), so their
zero-I/O purity is untouched.

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
  `executive_bullets` + `macro_outlook`) written to `daily_summaries`. Its
  schema — the validators and the pure `assemble_macro_content` transform that
  turns the raw macro LLM dict into the storable content fields — lives in
  `macro_summary.py` (the run-level twin of `insight.py`); the LLM call and
  upsert stay in `ingestion_engine.generate_macro_summary`. That module owns the
  schema in **both** directions, as `suppression_ledger.py` does for suppression:
  `assemble_macro_content` is the write face, and `MacroSummary` the read face —
  one stored row, read defensively and once, at the delivery fetch. Nothing
  downstream (report assembly, the **report model**, the **renderer**) holds raw
  stored jsonb, so a reader of this shape has one place to live. A `MacroSummary`
  that exists but carries no brief is the **accounting-only summary row** below;
  *no row at all* stays a separate, logged condition.
  *Avoid*: macro summary row (the row is the storage; the macro summary is what
  it records), summary model (the **report model** is the model).
- **Accounting-only summary row** — the `daily_summaries` row a run persists
  when it cannot generate a macro summary (zero stored articles, or an
  unusable LLM response): `run_date`/`run_mode` plus `screened_count` and the
  suppression breakdown/samples, with every content column **omitted** from
  the upsert payload (Supabase updates only provided columns, so a same-day
  retry never wipes an earlier full summary). Delivery renders it summary-less
  (no Executive Summary / Macroeconomic Outlook), and in the test-mode
  fallback `MacroSummary.has_content` ranks content-fullness before recency so an
  accounting-only row never shadows a content-full one.
- **Recorded counts** — the accounting the fetched `daily_summaries` row
  already carries at delivery-fetch time, as opposed to the counts the
  delivery run computes. The two counts sit differently against that line:
  `screened_count` is recorded by ingestion, so the fetched reading is the
  authoritative one; `surfaced_count` is recorded by delivery's own
  write-back, so the fetched reading is the *previous* state — absent on a
  day's first run, the earlier run's number on a same-day retry. Display
  policy follows the split: reader-facing sections show the run's computed
  `surfaced_count` and the recorded `screened_count`, while the test-mode QA
  block shows both counts as recorded — deliberately pre-write-back. A
  recorded count that is absent is shown as absent (`?`, or its clause
  omitted), never replaced with a different quantity.
  *Avoid*: stored counts (every count ends up stored; “recorded” names the
  fetched, pre-write-back reading).
- **Delivery window** (`delivery_engine.delivery_window`, `DeliveryWindow`) —
  the set of `daily_intelligence` rows one email carries: everything created
  **strictly after** the last recorded production delivery
  (`daily_summaries.delivered_at` on the most recent *earlier* `run_date`). The
  anchor is production-only in every run mode (a QA re-render sees what
  production saw) and strictly-earlier-day (a same-day retry re-sends the whole
  day's window). `delivered_at` is stamped by `_record_delivery` only **after**
  the send succeeds — with the run's *fetch instant*, not the send time, so a
  row a concurrent ingestion writes mid-run stays inside the next window — and
  a failed send widens the next window over the rows that never went out. The
  fetch itself (`fetch_since`) is a strict read: a database outage is a red job
  with no email, never a no-news email whose stamp would hide the rows. Without any recorded delivery — fresh DB, or migration 007 not
  applied — the window falls back to the legacy wall-clock lookback
  (`FALLBACK_LOOKBACK_HOURS` 24; 72 on Mondays). Named after issue #64: the
  old "rows created in the last 24 h" window was relative to the delivery
  step's own clock, so a late scheduled start (13:53 UTC on 2026-08-27) closed
  it after every row of the previous day and silently dropped them.
- **Run instant** (`run_instant.py`, `RunInstant`) — the one clock reading a
  pipeline run makes, plus the run mode, as a frozen value: `now` (naive UTC —
  the convention every stored timestamp follows) and `run_mode`. Together they
  are the `daily_summaries` row this run belongs to (`run_date` + `run_mode`
  is that table's key) and everything else a run derives from "today" —
  `min_summary_date` (the macro-summary lookback floor), `header_date` /
  `subject_date` (the email's display dates), `test_mode`. `RunInstant.current()`
  is the single effectful line (clock + `config.run_mode()`); each engine's
  `main()` reads one and hands it to `execute_pipeline(run)`, which threads it,
  so the row key is spelled once per run instead of once per function. The
  email's date is the **UTC** run date by construction. Ingestion hands it to
  `_finalize_run` (the gauntlet never reads it); delivery to every fetch,
  write-back, send and stamp. The rule: effectful functions take the value,
  pure functions take the one field they need — `delivery_window(now, …)` and
  `render_report(today_str, test_mode)` never see it, the caller derives the
  field. The instant is per process: ingestion and delivery each read their
  own, so a workflow that straddles 00:00 UTC keys the two on different
  days (never the 10:00 UTC cron; see the Key Invariants entry in
  `CLAUDE.md`). Named to end the five independent clock reads (two time
  zones) one delivery run used to make, which spelled the summary-row key
  four different ways.
  *Avoid*: run clock, run context (that is ingestion's mutable gauntlet state).
- **Synthesis outage** (`ingestion_engine.is_synthesis_outage`) — a run that
  stored nothing because **every** LLM synthesis call failed, as opposed to a
  quiet news day where the LLM answered and there was simply nothing material.
  The two are indistinguishable downstream — both end in zero rows and a
  no-news email — because the LLM seam swallows failures to `None` by design.
  The predicate separates them from the accounting alone: zero stored, at least
  `SYNTHESIS_OUTAGE_MIN_ATTEMPTS` (3) `synthesis_failed` records, and zero
  `llm_discard` (a discard is a *successful* call, so it proves the LLM was
  up). `_finalize_run` raises `SynthesisOutageError` after writing the
  **accounting-only summary row**, and `main()` exits 1 — failing the workflow
  step and thereby skipping delivery, so no misleading email goes out.
  Named for the 2026-08-03 run: expired API credits, 98/98 calls rejected, a
  green job and a "no significant market events" email.
- **Macroeconomic Outlook** (`macro_outlook`) — the structured macro read:
  `{current_condition, signals:[{indicator, direction, americhem_implication,
  affected_segments, citation_source_ids}]}`. Validated by
  `macro_summary.validate_macro_outlook`: every signal needs a valid `direction`
  (`prompts.VALID_MACRO_DIRECTIONS`), canonical `affected_segments`
  (`insight.VALID_COMMERCIAL_SEGMENTS`), and **at least one valid citation**
  (the materiality gate — an uncitable signal is dropped; no surviving signal
  → `null`). Carried on `ReportModel.macro_outlook`, rendered between the
  executive summary and Commercial Segment Watch. Its citations share one
  numbering space with the executive bullets (see **Citation set**), and
  `executive_sources` is the **union** of bullet- and signal-cited sources.
- **Citation set** (`report.py`, `CitationSet`) — the email's single citation
  numbering space as plain frozen data: which cited sources are numbered, what
  display number each one carries, and the order the Sources footer lists them
  in. Numbers are assigned by first appearance, **executive bullets first, then
  Macroeconomic Outlook signals**, and only for ids that resolve to an
  `executive_sources` entry — so a legacy row with no cited sources yields an
  empty set, which renders no inline markers and no footer. It numbers the
  signals the report model says will *render*, so the footer can never list a
  source no inline marker references. Built once during report assembly and
  carried on `ReportModel.citations`; every renderer that shows a citation —
  the executive summary, the Macroeconomic Outlook, the Sources footer — reads
  that one value rather than re-deriving its own, which is what makes the three
  agree by construction instead of by convention. The delivery-side twin of
  `macro_summary.py`'s ingestion-side citation cleaning (which decides *whether*
  a citation is valid and gates uncitable macro signals); the two are a matching
  pair across the two engines, not one module — cleaning belongs to the stored
  schema, numbering belongs to the rendered report.
- **Commercial Segment Watch** — the primary rendered email zone, grouped by
  `commercial_segment`.
- **Additional Articles to Explore** — the optional-discovery appendix
  (`ReportModel.additional_articles`): suppression-surviving rows scoring at
  or above the supporting threshold (code default 4; production 3) that are
  not visible cards — the
  weak-relevance band plus cap overflow — ranked deterministically
  (non-template rows before **low-exposure template** rows, then impact,
  then recency) and capped at `reporting.max_additional_articles` (default 10). Rendered
  compactly below Commercial Segment Watch, without the "So what" narrative.
  Never affects `surfaced_count`. Rows shown here are excluded from the
  `weak_relevance` count (but still counted in the broader
  `below_impact_threshold`). Enterprise / Cross-Segment rows below
  `enterprise_min_impact` never reach it (delivery suppression rule 1) — except
  **low-exposure template** rows (below), which rule 1 exempts while they are
  below the visible threshold. Rows from an **appendix-excluded category**
  (`reporting.appendix_exclude_categories` — production: the `macro_*`
  discovery groups, issue #73) never reach it either, whatever their score in
  the band: those groups feed the Macroeconomic Outlook's source pack, not
  headline rows. Appendix-only — a macro-group row at or above the visible
  threshold is still a card, and ingestion is untouched; each excluded
  appendix-band row is ledgered as `appendix_excluded_category`.
- **Low-exposure template** — one of the two honest RULE 6 So-What openers,
  `Adjacent market — …` / `Limited direct exposure — …`
  (`prompts.LOW_EXPOSURE_TEMPLATE_PREFIXES`, one definition shared by the
  prompt and `report._is_low_exposure_template`). The prompt binds them to the
  supporting band (`prompts.low_exposure_score_band`, derived from `Scoring`)
  on the promise that they reach the appendix; because an adjacent-market row
  is Enterprise / Cross-Segment by construction, rule 1 exempts template rows
  below the visible threshold (issue #65). Never a visible card; in the
  appendix always ranked after every non-template row, and in delivery
  dedup (rules 6/7) always processed after every non-template row so a
  duplicate contest never goes to a template — last-resort reading that
  fills the appendix only when there is room and can never displace a
  segment-specific row.
- **Report model** (`report.py`, `ReportModel`) — the assembled daily report as
  plain frozen data: `variant` (`daily` / `no_news`), the final segment groups
  (capped only when configured; caps default to `null` = uncapped),
  `additional_articles` (the optional-discovery appendix — see below),
  `macro_outlook` (the renderable Macroeconomic Outlook, or `None`),
  `citations` (the **citation set** — the email's one numbering space),
  `surfaced_count` (this run's visible-card count — the value the write-back
  will record) and `screened_count` (a **recorded count**), the
  delivery-side suppression
  ledger (including the derived `below_impact_threshold` and `weak_relevance`
  counts), the **macro summary** (its typed read face, carried whole), and the
  thematic synthesis paragraphs.
  Produced by `assemble_report` (pure decision pipeline: delivery suppression →
  visibility filter → segment grouping → optional per-segment cap → optional
  total cap → appendix selection → weak-relevance accounting). Consumed by the
  **renderer** (`renderer.render_report`) and the `daily_summaries` write-back.
  `delivery_engine.prepare_report(rows, macro_summary, run=run)` runs assembly itself
  (there is no model-in/model-out effectful call), then performs the run's two
  side effects — write-back + thematic synthesis — exactly once, after
  assembly and before rendering; both are skipped for `no_news`. Rendering a
  model whose synthesis is empty **is** the bullets-only fallback.
- **Pure module** — a module whose functions decide and never act: same
  inputs, same result, with no clock, environment, configuration file,
  network or seam behind them. Structurally: it imports none of the seams,
  `config` or the **run instant**, makes no ambient clock read, and does no
  I/O except what it declares (the two file-backed parsers — the **targets
  catalogue** over `targets.yaml` and the ZoomInfo relevance gate's loader
  of `target_metadata.yaml` — read one file each and nothing else). Callers hand a pure module the values it
  needs (a config dict, a date string, a run's elapsed seconds), which is
  what lets its tests use dict literals and zero patches. Which modules are
  pure, and that they stay so, is pinned structurally in
  `tests/test_purity.py` — the table there is the list.
  *Avoid*: helper module, utility module.
- **Renderer** (`renderer.py`, `render_report`) — the pure function from a
  **report model** (plus the header date and the test-mode flag the caller
  derives from the **run instant**) to the email's HTML. Owns the layout — the
  fixed section order: executive summary, Macroeconomic Outlook, Commercial
  Segment Watch, Additional Articles, Sources, the test-mode QA block — and the
  two rendering rules: **every value the email carries — from a row, the
  summary row, the synthesis, config or the caller — is HTML-escaped, whether
  or not its source is trusted**, and **every `href` passes the http/https guard**
  (an unsafe URL renders its text unlinked, never dropped). Deterministic: same
  model, date and mode → same bytes; no clock, config, seam or log. The
  producing half of the pair whose sending half is the **mailer seam** — the
  renderer makes the HTML, `send_email` addresses it, the mailer transports it.
  *Avoid*: template, view, email builder.
- **Candidate gauntlet** — the ordered per-candidate decision sequence
  ingestion runs on every discovered candidate: duplicate URL → semantic
  duplicate → unscrapable domain → provider relevance gate → scrape →
  synthesis → store. Lives in `ingestion_engine.process_candidate(candidate,
  target, ctx)`; every drop is a recorded suppression (record + provider-yield
  bump are one inseparable call). The run-level limits (pipeline deadline,
  scrape cap, tail reserve) are **not** part of the gauntlet — they are the
  **run budget**, asked at the two checkpoints around it.
- **Candidate outcome** — the gauntlet's verdict for one candidate, as plain
  frozen data: `Stored` (persisted), `Suppressed(reason)` (dropped; `reason`
  is an ingestion ledger taxonomy code, including `synthesis_failed` for an
  unusable LLM response), or `Error` (a technical store failure — an error,
  not a suppression). There is no run-terminating outcome by design.
- **Target** (`targets.py`) — one unit of discovery work the ingestion loop
  runs, as a plain dict (like Insight and candidates): an **entity target** (one
  active company under an entity-mode group — one Serper query per name) or a
  **concept target** (one active concept-mode group — one combined OR query).
  Carries `name`, `category` (its group key — the discovery category rows are
  stored under), `search_mode`, the pre-built `query`, the discovery settings,
  and for entities the ZoomInfo id/flag and the resolution hints
  (`domain` / `hq_country` / `hq_state`) the enrichment utility reads.
  **Targets catalogue** — `load_targets(path)` (and `parse_targets(text)`, the
  same parser over text already in hand), the one parser of
  `targets.yaml`; every consumer (the ingestion engine, the enrichment and
  probe scripts) reads targets through it. Shape errors in the control file
  (a document that is not valid YAML, an entity without a name, an unknown
  `search_mode`, a concept group with no `include_any`, a non-list where a
  list belongs) raise `TargetsError`
  naming the group — the run fails at t=0 like a missing secret does, instead
  of silently dropping coverage. Policy rules (tier order, macro groups last)
  are not validated here; they are pins against the shipped file.
  **Entity entry** (`entity_entries(text, *, source)`) — one entity as the control file
  *lists* it, active or not, in file order: what the file says, as against a
  target, which is what the loop runs. Every entity target is an entry; an
  inactive entity is an entry and never a target. An entry is knowledge about
  the file and decides nothing; whoever edits the file composes a plan from
  entries — the catalogue never plans.
  *Avoid*: group (a group is a section of the YAML; a target is what the loop
  runs), search target, inactive target (there is no such thing — that is an
  entry).
- **Run budget** (`run_budget.py`, `RunBudget`) — what one ingestion run may
  spend, as a frozen value: the two hard limits — the **scrape cap** (attempted
  scrapes, the API-cost guard) and the **pipeline deadline** (elapsed wall
  clock, the CI-timeout guard) — plus the **tail reserve**, the slice of each
  held back so the concept/macro groups at the bottom of `targets.yaml` always
  get their discovery pass. The slot reserve is position-aware: at each entity
  target it is the concept demand still *ahead* in file order (a concept
  target's demand is its `results_per_entity`), so front-loaded priority
  concepts that have already run are never reserved twice; the clock reserve
  is a constant. Time-agnostic — the loop owns the stopwatch and passes
  `elapsed`. Entity coverage is what the reserve sacrifices because dedup
  absorbs day-over-day re-discoveries; concept/macro coverage would otherwise
  starve silently.
  *Avoid*: protected tail budget, tail demand (both name the tail reserve).
- **Budget verdict** — the run budget's answer at a checkpoint, as plain frozen
  data: `Proceed`, `SkipEntity(reason)` (don't *start* this entity target —
  `scrape_slots` / `wall_clock`; the reserve never cuts a started target,
  only a hard limit does), or `Stop(reason)` (`deadline` / `scrape_cap` —
  end the run at the single teardown). Two checkpoints ask it: **before a target** (both hard
  limits, and the tail reserve for entity targets — so a target at the cap
  never spends discovery it cannot scrape) and **before a candidate** (the
  hard limits only). The run-level twin of the **candidate outcome**; like it,
  the value decides and never logs.
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
