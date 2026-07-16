# Macroeconomic Outlook and Additional Articles Design

**Date:** 2026-07-16
**Status:** Approved (amended 2026-07-16 after design review)
**Repository:** `Phife726/market-pulse`

## Objective

Extend Market-Pulse in two independent production-safe increments, plus one
report-behavior change folded into the first:

1. **Uncap the main report**: every article at or above the visible threshold
   (6) that survives delivery suppression appears as a card. The per-segment
   and total caps become optional config knobs, off by default.
2. Add an optional-discovery appendix that surfaces up to 10 quality-screened
   articles scoring 4 or 5 without weakening the decision-grade main report.
3. Add a conditional Macroeconomic Outlook based on dedicated macro searches
   and explicit Americhem segment implications.

## Success criteria

- The main report shows **all** suppression-surviving articles at or above the
  visible threshold — no article scoring 6+ is silently dropped.
- Recipients can optionally browse no more than 10 near-threshold articles.
- Score-5 appendix items always rank ahead of score-4 items.
- Quality-suppressed or duplicate items never enter the appendix.
- The macro section appears only when material macro evidence exists.
- Every macro statement connects to an Americhem demand, cost, capacity,
  margin, or segment implication, **and cites at least one source**.
- Both features are deterministic, testable, and independently reversible.

## Constraints and invariants

- Reuse `Scoring.is_weak_relevance` as the appendix eligibility band. Do not
  add a competing threshold definition anywhere.
- Keep `reporting.visible_impact_threshold` as the main report boundary.
- Preserve the existing delivery suppression pass before appendix eligibility.
- Preserve existing ingestion deduplication, scrape limits, and runtime
  deadline.
- Do not modify `delivery_engine_old.py`.
- Do not show a section when it has zero qualifying content.
- Do not fabricate macro implications. A macro signal must cite the source
  pack; the model may only interpret Americhem relevance of cited sources.
- Segment labels everywhere (`affected_segments` included) are the canonical
  `insight.VALID_COMMERCIAL_SEGMENTS` labels — never informal variants.

## PR strategy

### PR 1: Uncapped report + Additional Articles to Explore

Standalone PR because it changes production email behavior and
scoring-threshold interpretation. The uncap change and the appendix land
together: both answer "what appears in the email", and the uncap makes the
appendix band exactly the 4–5 score range (nothing 6+ is ever capped out, so
there are no orphaned visible articles to consider for the appendix).

### PR 2: Macroeconomic Outlook

Standalone PR because it changes search coverage, ingestion volume, prompt
behavior, and production report content.

The two PRs should not be bundled. They have different rollback and
validation risks.

## PR 1 design

### Uncapped visible report

- `reporting.max_visible_articles_per_segment` and
  `reporting.max_total_visible_articles` become **optional**: `null` (or an
  absent key) means no cap. The built-in defaults in `report.py` change from
  3/12 to uncapped, and `market_pulse_config.yaml` sets both to `null` with a
  comment explaining how to re-impose a cap if the report gets noisy.
- Re-imposing a cap is a config-only rollback — no code change.
- `ReportModel` invariants update accordingly: caps hold *when configured*.

### Appendix eligibility

An item qualifies when all conditions hold:

- It survives `_apply_delivery_suppression`.
- `Scoring.is_weak_relevance(row)` is true (supporting ≤ effective impact
  < visible — the same `insight.effective_impact` read the rest of the report
  uses, legacy `sentiment_score` fallback included).
- It has a non-blank headline and source URL.
- Its `url_hash` is not in any final main-report group.

This naturally targets scores 4 and 5 under the current configuration.

Conditions that are **guaranteed by construction** and therefore neither
implemented nor tested at report level: scrape failures, LLM discards, and
ingestion duplicates never reach `daily_intelligence`, so `assemble_report`
never sees them; delivery-side headline dedupe runs inside the suppression
pass, before eligibility.

**Deliberate consequence, documented and pinned by a test:** delivery
suppression rule 1 drops Enterprise / Cross-Segment rows scoring below
`enterprise_min_impact` (7), so no Enterprise / Cross-Segment article — and
no article whose segment defaulted there — can ever appear in the appendix.
This is the intended quality gate, not a bug.

### Ranking

Deterministic sort order (no segment priority — segment key order in config
exists for prompt stability, not ranking):

1. Effective impact descending (every 5 precedes every 4).
2. Recency descending: `published_at` when parseable, else the row's
   `created_at`.
3. Normalized (case-folded, stripped) headline ascending.
4. `url_hash` ascending as the final total tie-breaker.

### Cap

- New config: `reporting.max_additional_articles: 10` (a report-assembly
  knob, read in `report.py` beside the other reporting caps — not a scoring
  threshold).
- Do not backfill unused slots with items below the supporting threshold.
- Omit the section when the final list is empty.

### Report model

Extend `ReportModel` with an immutable `additional_articles: tuple[dict, ...]`
field populated by `assemble_report`; empty for the `no_news` variant.

`surfaced_count` continues to count main visible cards only. Additional
articles are optional-discovery content and must not alter the headline
surfaced count.

### Rendering

Add a bottom-of-email section titled **Additional Articles to Explore**,
below Commercial Segment Watch and above the Sources footer / QA block.

Each row displays:

- linked headline (href passed through `_safe_http_url`)
- commercial segment
- impact score
- publisher/domain
- publication date **only when `published_at` is present** (the scrape
  timestamp is used for sorting, never displayed as a publication date)

All untrusted values (headline, segment, domain, URL) are HTML-escaped — the
appendix is deliberately stricter than the legacy card renderer.

Do not display the full "So what" narrative. The compact treatment preserves
the distinction between surfaced intelligence and optional reading.

### Suppression accounting

- `weak_relevance` counts a qualifying 4–5 row only when it appears in
  **neither** the main groups **nor** the appendix (i.e. it was pushed out by
  the appendix cap or lacked a usable headline/URL).
- `below_impact_threshold` is **unchanged**: it still counts every
  suppression-surviving row below the visible threshold, including rows the
  appendix now displays. It describes the visible-card decision, not
  end-to-end hiding. This overlap is documented so QA totals read correctly.

## PR 2 design: Macroeconomic Outlook

### Search coverage

Restructure discovery in `targets.yaml`:

- **Replace** the existing generic `economic` concept group (its terms are
  subsumed) with dedicated macro concept groups covering:
  - manufacturing PMI and industrial production
  - construction starts, building permits, and nonresidential construction
  - automotive production and sales
  - consumer spending and durable goods
  - CPI, PPI, interest rates, and credit conditions
  - energy, freight, and feedstock costs
  - business investment and capital spending
- Place the macro groups **last** in `targets.yaml`. Targets are processed in
  file order, so on a deadline-limited run the macro searches are sacrificed
  before entity coverage — graceful degradation by construction.
- Keep query counts controlled so the existing scrape cap and deadline remain
  effective.

### Classification

Macro articles retain `signal_type: Macro` and use the existing commercial
segment taxonomy. A macro item may map to a single segment or
`Enterprise / Cross-Segment` when no segment dominates.

Note: macro headlines often match the generic-market-report suppression
patterns ("market outlook", "market forecast") with empty entities, so many
macro rows will be suppressed **as cards** — that is fine and expected; the
outlook section is synthesized at ingestion from stored rows and does not
depend on card visibility. A test pins that a card-suppressed macro article
can still be cited by the outlook.

### Materiality gate

Enforced in two layers:

1. **Prompt**: a macro item is report-material only when it indicates a
   meaningful demand inflection, cost or margin pressure, capacity or
   investment constraint, credit or liquidity pressure, logistics or
   feedstock disruption, or a material contradiction to the current
   commercial outlook. Generic economic commentary without a defensible
   Americhem implication is excluded.
2. **Validator (deterministic)**: a signal without at least one valid
   `citation_source_ids` entry (validated against the source pack, same
   cleaning as executive bullets) is dropped. Source grounding is a
   structural guarantee, not a prompt hope.

### Synthesis output

Extend the existing `generate_macro_summary` LLM call (one call, one source
pack, one enumeration — a malformed `macro_outlook` key degrades to `None`
while the executive bullets survive, because validation is per-key).

Structured result:

- `current_condition`: one concise sentence
- `signals`: zero or more rows containing:
  - `indicator` (non-empty string)
  - `direction` — validated against a new `VALID_MACRO_DIRECTIONS` enum owned
    in `prompts.py` (e.g. Rising | Stable | Declining), same
    one-definition discipline as `VALID_MACRO_CONDITIONS`
  - `americhem_implication` (non-empty string)
  - `affected_segments` — non-empty list validated against
    `insight.VALID_COMMERCIAL_SEGMENTS` **exact labels** (construction
    framing belongs in the implication text; "Building & Construction" is not
    a segment)
  - `citation_source_ids` — at least one valid pack id (see materiality gate)

The section is rendered only when at least one validated material signal
remains.

### Source pack and citations

- `_rank_macro_articles` reserves a quota for `signal_type == "Macro"` rows
  (up to 10 of the 40 pack slots when present), so macro articles — which
  tend to score mid-range on materiality — cannot be crowded out of the
  citable pack on heavy news days.
- `executive_sources` packing extends to the **union** of sources cited by
  surviving executive bullets and surviving macro signals, so every rendered
  citation id resolves.
- Delivery's citation display map enumerates executive bullets **then** macro
  signals — one numbering space shared by the inline markers in both sections
  and the single Sources list at the bottom of the email.

### Persistence

- Add a nullable `macro_outlook` JSONB column to `daily_summaries` via
  idempotent migration `005`; update `schema.sql` for fresh installs.
- **Required, not flag-gated** (same as migration 004): apply the migration
  *before* deploying the code, or `upsert_summary` crashes ingestion. The
  rollout order is stated in the migration header and CLAUDE.md.
- Persist `None` (or an empty validated structure) when no material signal
  exists.
- The test-mode production-row fallback (`fetch_macro_summary`) carries
  `macro_outlook` along automatically — it lives in the same row.

### Rendering

Place **Macroeconomic Outlook** after the executive summary and before
Commercial Segment Watch.

Use a compact email-safe table or stacked rows. Each signal states the
operational implication, not merely the economic indicator. Section absent
for `None`, empty, or zero-signal outlooks. All untrusted text escaped.

### Failure behavior

- Dedicated macro search failure degrades gracefully and does not fail the
  full pipeline.
- Invalid or incomplete structured macro synthesis yields no macro section.
- Deadline exits still call `generate_macro_summary` with whatever was stored
  (existing behavior) — the outlook degrades to fewer or zero signals.
- Main report generation and delivery continue unchanged.

## Testing strategy

### PR 1

- uncapped: with caps null, all suppression-surviving 6+ articles render;
  an integer cap still enforces (knob retained)
- score 5 precedes score 4
- score below supporting threshold is excluded
- visible articles are not duplicated in appendix
- quality-suppressed rows are excluded (including the pinned
  Enterprise / Cross-Segment consequence)
- cap is exactly 10
- empty section is omitted
- deterministic tie-breaking (headline, then url_hash)
- surfaced count excludes appendix
- weak-relevance accounting counts only rows in neither main groups nor
  appendix; below_impact_threshold semantics unchanged
- appendix renderer escapes untrusted values and guards hrefs

### PR 2

- dedicated macro targets are loaded and queried; the old `economic` group is
  gone; macro groups are last in file order
- existing dedupe and scrape controls still apply
- immaterial macro evidence produces no section
- a signal without citations is dropped (materiality gate)
- `affected_segments` validated against canonical labels; invalid segment
  rejected
- `direction` validated against the enum
- macro quota: Macro-signal rows survive into the source pack alongside 40+
  higher-scoring rows
- a card-suppressed macro article can still be cited by the outlook
- executive_sources contains the union of bullet-cited and signal-cited
  sources; display numbering is consistent across both sections
- invalid LLM output degrades to no section; bullets survive a malformed
  macro_outlook key
- macro search/provider failure does not fail delivery

## Out of scope

- recipient-specific preferences or opt-in configuration
- a web dashboard or hosted article archive
- feedback buttons or model-retraining workflow
- changing the visible threshold itself (still 6)
- showing all filtered or failed URLs
- historical backfill
- locale-aware macro discovery (same deferral as the regional groups)
