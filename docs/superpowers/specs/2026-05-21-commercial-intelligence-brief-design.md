# Commercial Intelligence Brief — Design

**Date:** 2026-05-21
**Status:** Revised after spec review (pending re-approval)
**Sprint goal:** Convert Market-Pulse from a filtered article digest into a decision-grade Americhem commercial intelligence brief.

---

## 1. Context

PR #5 introduced impact scoring (`americhem_impact_score`), sentiment tags (`Negative/Neutral/Positive`), a `strategic_segment` taxonomy, threshold filtering, and a Jason-only test workflow. The output is closer to commercially useful but still reads as an AI-generated market digest:

- Executive summary is dense prose.
- `macro_sentiment` defaults to "Cautiously Optimistic" every day.
- `strategic_segment` collapses commercial end-markets (Healthcare, Packaging) and signal types (Regulatory, Competitive) into one field.
- Peripheral Signals leaks noise to stakeholders (recent examples: Shaw sustainability duplicate, extension-cord listing, AI disaster images).
- The header reads "N items today," framing volume as bloat instead of screening rigor.
- Suppression decisions are logged but never stored — Jason has no audit trail for QA.

This sprint addresses all six product gaps as one schema + prompt + delivery redesign: **B (schema + prompt contract redesign) with a small C overlay (delivery-side final suppression pass plus suppression-debug accounting)** — no ZoomInfo, no separate Supabase, no recipient personalization, no platform refactor.

---

## 2. Success criteria

1. A GMM or Sales leader can scan the email in under 90 seconds.
2. Articles are grouped by Americhem commercial segments (9-value taxonomy), not signal types.
3. Executive summary is 3 fixed-label BLUF bullets (Market pressure / Supply chain watch / Commercial action).
4. The macro label is drawn from a 9-value constrained enum and varies day-to-day with the underlying signal mix.
5. Duplicate, irrelevant, and low-value content is **filtered out** before stakeholder delivery — not just counted. Peripheral Signals does not appear in production.
6. Test pipeline exposes a QA / suppression-debug section visible only to Jason via `MARKET_PULSE_RUN_MODE=test`, broken down by every reason a row was hidden.
7. The header reframes volume as rigor: `{surfaced} surfaced signals from {screened} screened items · Dominant condition: {condition}`.
8. Test runs do not corrupt production state: `daily_summaries` is keyed on `(run_date, run_mode)`.

---

## 3. Out of scope

- ZoomInfo or any account-data enrichment.
- A separate Supabase project for test data.
- Per-recipient personalization or a recipient-preferences surface.
- A web dashboard or any UI outside the daily email.
- Refactoring `ingestion_engine.py` / `delivery_engine.py` into multiple packages.
- Backfilling historical `daily_intelligence` rows with the new `commercial_segment` / `signal_type` fields.

---

## 4. Content architecture

The email has three zones; the third is gated by `MARKET_PULSE_RUN_MODE`.

```
HEADER
  Market-Pulse: Daily Intelligence
  Thursday, May 21, 2026 · 6 surfaced signals from 87 screened items
  Dominant condition: Competitive Pressure

EXECUTIVE SUMMARY
  • Market pressure:    Techmer, Teknor Apex, and KRAIBURG are increasing specialty
    materials claims across transparent, film additive, and TPE applications.
  • Supply chain watch: Mitsubishi and Asahi Kasei restructuring signals may create
    volatility in polymer and additive availability.
  • Commercial action:  Prioritize compliant additive platforms, healthcare TPE
    differentiation, and engineered resin support for aerospace/defense prototyping.

COMMERCIAL SEGMENT WATCH
  Healthcare
    Impact 6/10 · Positive · Signal: Technology
    KRAIBURG TPE targets wrist orthoses applications.
    So what: Supports demand for regulated healthcare elastomer and color/additive solutions.

  Packaging
    Impact 6/10 · Positive · Signal: Sustainability
    Sirmax and De'Longhi launch 70% recycled-plastic coffee maker.
    So what: Raises customer expectations for recycled-content aesthetics and performance.

  Transportation - Aerospace
    Impact 6/10 · Positive · Signal: Customer
    CTC names engineering leader for advanced manufacturing.
    So what: Supports prototyping demand for engineered materials in aerospace/defense.

— — — (production stops here) — — —

QA · SUPPRESSION SUMMARY                     (test mode only)
  Screened: 87 · Surfaced: 6 · Suppressed: 81
  By reason: duplicate URL (23) · semantic duplicate (4) · LLM discard (12)
             · below impact threshold (22) · scrape failed (18)
             · duplicate headline (1) · semantic duplicate headline (1)
             · product listing (5) · job posting (3) · generic market report (4)
             · unrelated color result (2) · cross-segment low impact (3)
             · weak relevance (2)
  Last 10 suppressed: …
```

**Decisions baked into this architecture:**

- **No separate Critical Disruptions section.** Legacy rows with `sentiment_score <= 3` get promoted into the commercial-segment view with a Critical badge on the card. Avoids empty-section edge cases and matches the target structure.
- **No Peripheral Signals in production.** Ungrouped 4–5 impact and below-threshold legacy rows route to the test-only QA section as `weak_relevance`, not to recipients.
- **Header reframes volume as rigor.** Both `surfaced_count` and `screened_count` live on `daily_summaries`.
- **Multi-article thematic synthesis paragraphs stay.** Where 2+ articles share a commercial segment, the LLM still writes one synthesis paragraph; it now sits under a commercial-segment heading, not a signal-type heading.

---

## 5. Data model / schema

### 5.1 Migration 002 — additive columns

```sql
-- migrations/002_split_segment_and_structured_summary.sql
-- Safe to run multiple times.

alter table daily_intelligence
  add column if not exists commercial_segment text,
  add column if not exists signal_type text;

create index if not exists idx_daily_intelligence_commercial_segment
  on daily_intelligence (commercial_segment);

create index if not exists idx_daily_intelligence_signal_type
  on daily_intelligence (signal_type);

alter table daily_summaries
  add column if not exists run_mode text not null default 'production',
  add column if not exists dominant_condition text,
  add column if not exists executive_bullets jsonb,
  add column if not exists screened_count integer,
  add column if not exists surfaced_count integer,
  add column if not exists suppression_breakdown jsonb,
  add column if not exists suppression_samples jsonb;

-- Replace the single-key uniqueness with (run_date, run_mode) so test runs
-- never overwrite production summary rows.
drop index if exists idx_daily_summaries_run_date_unique;

create unique index if not exists idx_daily_summaries_run_date_mode_unique
  on daily_summaries (run_date, run_mode);
```

`schema.sql` is updated to match — including the `run_mode` default and the composite unique index — so a fresh DB initializes correctly from one file.

The migration's `default 'production'` ensures existing rows backfill to `'production'` automatically when the column is added, preserving the meaning of historical data.

### 5.2 Field semantics

**`daily_intelligence.commercial_segment`** — one of nine values; the end-market the article most affects for Americhem. See §6.3 for the full descriptions written into config; the table below summarises each label's scope and the principal "do not confuse with" rule.

| Value | Scope summary | Do not confuse with |
| --- | --- | --- |
| `Healthcare` | Medical devices, pharma packaging, diagnostics, biocompatible/sterilizable polymers, clean-room compounding, FDA/EMA-regulated material signals. | General consumer products that happen to be plastic. |
| `Fibers` | Synthetic fibers, nonwovens, performance fabrics, carpet fiber, geotextiles, fiber-grade polyester/nylon/PP/acrylic. | Fabric-finished consumer goods (route to Packaging or Industrial). |
| `Transportation - Automotive` | Light vehicles (passenger car, SUV, light truck), automotive OEMs, Tier-1/Tier-2 suppliers, interiors/exteriors/under-hood, EV/HEV materials, ADAS/lighting/fluid systems. | Heavy trucks (→ Non-Automotive), aerospace (→ Aerospace). |
| `Transportation - Non-Automotive` | Off-highway, Class 4–8 trucks, buses, agriculture equipment, construction equipment, rail, marine, recreational vehicles, motorcycles. | Light vehicles (→ Automotive), aerospace (→ Aerospace). |
| `Transportation - Aerospace` | Commercial and defense aerospace (fixed-wing, rotorcraft), space, UAS/UAM/eVTOL. Includes FAR 25.853 / OEM-spec qualification and defense procurement signals. | Ground transportation segments. |
| `Industrial` | Industrial plastics, durable goods, wire and cable, building products, construction materials, agricultural applications, electrical infrastructure. | Healthcare devices and automotive end-uses (those have their own segments). |
| `Packaging` | Flexible and rigid packaging, consumer-goods packaging, food contact, brand-owner demand signals, sustainable packaging legislation, converter and brand-owner sourcing shifts. | Industrial packaging that is structurally part of a durable good (→ Industrial). |
| `Engineered Resins` | High-performance compounds and resin platforms where mechanical, thermal, flame, electrical, chemical, or processing performance drives selection — custom compounds, material qualification, resin-technology signals not tied to one end-market. | A signal clearly anchored to one end-market (route to that end-market instead). |
| `Enterprise / Cross-Segment` | Spans multiple commercial segments or addresses Americhem-wide topics (corporate finance, multi-segment supplier moves, generic industry trends). | Anything where a single end-market dominates the article. |

**`daily_intelligence.signal_type`** — one of eight values; the *kind* of signal. Full descriptions live in config (§6.3).

| Value | Scope summary |
|---|---|
| `Competitive` | Competitor moves: pricing, capacity, product launches, M&A, executive hires, plant openings/closures by named competitors (Avient, Techmer PM, Ampacet, etc.). |
| `Customer` | Customer or named brand-owner signals — expansions, contractions, OEM sourcing, plant moves, product launches that affect Americhem demand. |
| `Regulatory` | Government/agency/standards-body actions — PFAS, EPR, REACH, Prop 65, FDA medical device rules, chemical reporting requirements. |
| `Sustainability` | Recycled-content mandates, circularity initiatives, carbon-reduction targets, EPR programs, ESG-driven material substitution. |
| `Supply Chain` | Resin pricing, monomer/feedstock supply, pigment/additive availability, logistics, port/rail disruptions, force majeure, supplier capacity changes. |
| `Technology` | New chemistry, processes, manufacturing innovations, materials-science research, novel additive platforms, recycling-technology breakthroughs. |
| `Macro` | Industry-wide demand/pricing shifts, macroeconomic indicators (GDP, manufacturing PMI, construction starts) that affect plastics consumption. |
| `Other` | Use only when none of the seven categories above clearly fit; prefer a named type whenever possible. |

**`daily_summaries.run_mode`** — `'production'` or `'test'`. Defaults to `'production'`. Production ingestion writes `'production'`; test workflow (with `MARKET_PULSE_RUN_MODE=test`) writes `'test'`. Composite unique index on `(run_date, run_mode)` ensures test runs cannot overwrite production rows.

**`daily_summaries.dominant_condition`** — one of nine values that summarises the overall commercial weather:

```
Competitive Pressure · Supply Volatility · Demand Expansion · Demand Softness ·
Regulatory Pressure · Sustainability Pull · Commercial Opportunity ·
Mixed / Watch · Low Signal
```

Defaults to `Mixed / Watch` if the LLM returns an invalid label. Defaults to `Low Signal` when fewer than 3 articles surfaced.

**`daily_summaries.executive_bullets`** — JSONB array of exactly 3 objects with fixed labels in this order:

```json
[
  {"label": "Market pressure",    "body": "<one sentence>"},
  {"label": "Supply chain watch", "body": "<one sentence>"},
  {"label": "Commercial action",  "body": "<one sentence>"}
]
```

Labels are part of the prompt template; the LLM only writes the bodies. Validation rejects any structure that doesn't match this shape; on rejection, delivery falls back to the legacy `executive_summary` text.

**Low-signal day handling.** When `dominant_condition == "Low Signal"`, the `Commercial action` body MUST be the literal string `"No action required."`. The prompt instructs this explicitly, and the validator coerces non-conforming output to that exact string. The other two bullets are still required (they describe the lack of signal, e.g., "Market pressure: No material competitive activity detected"). This prevents fake-urgency action bullets on quiet days.

**`daily_summaries.screened_count`** — integer; **total URLs discovered during the run before suppression and deduplication** (i.e., `stats["urls_discovered"]`). Includes URLs that were later skipped as duplicates, failed to scrape, or were LLM-discarded. The number is a UX signal of rigor, not a precision metric.

**`daily_summaries.surfaced_count`** — integer; visible cards rendered by delivery (after the final suppression pass — §7.3). Written by `delivery_engine.py` via an `update()` on the matching `(run_date, run_mode)` row immediately before rendering.

**`daily_summaries.suppression_breakdown`** — JSONB object keyed by reason code. The full reason set:

```json
{
  "duplicate_url":                       23,
  "semantic_duplicate":                   4,
  "llm_discard":                         12,
  "scrape_failed":                       18,
  "below_impact_threshold":              22,
  "weak_relevance":                       2,
  "duplicate_headline":                   1,
  "semantic_duplicate_headline":          1,
  "product_listing":                      5,
  "job_posting":                          3,
  "generic_market_report":                4,
  "unrelated_color_result":               2,
  "enterprise_cross_segment_low_impact":  3
}
```

**Ownership of each reason:**

| Reason | Source | Stage |
| --- | --- | --- |
| `duplicate_url` | ingestion | URL hash already in DB |
| `semantic_duplicate` | ingestion | rapidfuzz token_sort_ratio ≥ 88 against last-72h headlines |
| `llm_discard` | ingestion | LLM returns `"americhem_impact": "DISCARD"` |
| `scrape_failed` | ingestion | Firecrawl + fallback both fail or return below `min_article_length` |
| `below_impact_threshold` | delivery | `americhem_impact_score` below `visible_impact_threshold` |
| `weak_relevance` | delivery | Effective impact 4–5 and ungrouped (the old "Peripheral" pool) |
| `duplicate_headline` | delivery | Exact headline match within the day's visible candidates |
| `semantic_duplicate_headline` | delivery | rapidfuzz token_sort_ratio ≥ `headline_duplicate_threshold` (default 90) within the day's visible candidates |
| `product_listing` | delivery | URL or title matches the product-listing patterns in config |
| `job_posting` | delivery | URL or title matches the job-posting patterns in config; override allowed when `recommended_action == "Escalate to leadership"` |
| `generic_market_report` | delivery | Title matches the generic-market-report patterns AND no Americhem-targeted entity is named in `entities_mentioned` |
| `unrelated_color_result` | delivery | Title contains color/colour terms AND no plastics-relevance term appears in title, `americhem_impact`, or `entities_mentioned` |
| `enterprise_cross_segment_low_impact` | delivery | `commercial_segment == "Enterprise / Cross-Segment"` AND `americhem_impact_score < 7` |

`generate_macro_summary()` upserts the ingestion-written subset; `delivery_engine.py` updates the delivery-written subset on the same row via the `(run_date, run_mode)` key.

**`daily_summaries.suppression_samples`** — JSONB array, capped at 10 across the whole run, FIFO. Each entry:

```json
{"reason": "product_listing", "url": "https://...", "title": "Pretty plastic tote — 24 ct"}
```

Both ingestion and delivery append into the same buffer. The buffer is upserted/updated alongside `suppression_breakdown`. When more than 10 items have been suppressed in a run, the cap keeps the most recent 10 (a representative slice for Jason's QA, not an exhaustive log).

### 5.3 Legacy columns

`daily_intelligence.strategic_segment` and `daily_intelligence.include_in_report` are **kept** but new rows leave them NULL. They are read only by the legacy-fallback path in delivery (see §7.2).

`daily_summaries.executive_summary` and `daily_summaries.macro_sentiment` are **kept** and continue to be populated by ingestion (derived from the structured fields) so historical reads and external consumers don't break.

---

## 6. Ingestion prompt contract

### 6.1 Per-article prompt — `ingestion_engine.py`

RULE 4 is split into RULE 4 (commercial segment) and RULE 5 (signal type). Old RULE 5 and RULE 6 renumber to 6 and 7. The "six rules" wording in the prompt body becomes "seven rules."

**RULE 4 — COMMERCIAL SEGMENT** (text generated from `commercial_segments:` in config; each segment renders as `label: description`). The full descriptions are in §6.3.

**RULE 5 — SIGNAL TYPE** (text generated from `signal_types:` in config). Full descriptions in §6.3.

**Output JSON schema** (per article):

```json
{
  "headline": "<≤12 words>",
  "source_publication": "<publisher>",
  "article_summary": "<2-3 sentences, ≤50 words, factual only>",
  "americhem_impact": "<BLUF So What>",
  "sentiment_score": <1-10>,
  "sentiment_tag": "<Negative|Neutral|Positive>",
  "americhem_impact_score": <1-10>,
  "impact_rationale": "<≤15 words>",
  "commercial_segment": "<exact label from RULE 4>",
  "signal_type": "<exact label from RULE 5>",
  "sentiment_rationale": "<≤10 words>",
  "recommended_action": "<No action | Monitor | Flag to procurement | Share with sales | Escalate to leadership>",
  "source_url": "<must match user prompt URL exactly>",
  "entities_mentioned": [...]
}
```

`strategic_segment` is **removed** from the output schema. `synthesize_insight()` validates `commercial_segment` against the config list (default `"Enterprise / Cross-Segment"` on invalid) and `signal_type` against the config list (default `"Other"` on invalid).

### 6.2 Macro summary prompt — `generate_macro_summary()`

The function is rewritten to produce structured output:

System prompt (sketch):

```
You are a senior Americhem commercial intelligence analyst writing the morning brief
for GMMs and Sales leaders. Output ONLY a JSON object with two keys.

1. dominant_condition — pick exactly one value from this list that best describes
   today's overall commercial weather across the digest:
     Competitive Pressure, Supply Volatility, Demand Expansion, Demand Softness,
     Regulatory Pressure, Sustainability Pull, Commercial Opportunity,
     Mixed / Watch, Low Signal

2. executive_bullets — exactly three objects, in this order, with these exact labels:
     {"label": "Market pressure",    "body": "<one sentence, ≤30 words>"}
     {"label": "Supply chain watch", "body": "<one sentence, ≤30 words>"}
     {"label": "Commercial action",  "body": "<one sentence, ≤30 words>"}

   Each body must reference specific named entities or segments from the digest.
   Do NOT hedge ("may", "could", "potentially") without a specific data point.
   Do NOT write generic statements ("monitor closely", "remain vigilant").

   Low-signal special case:
   If dominant_condition is "Low Signal", the Commercial action body MUST be the
   literal string "No action required." The other two bullets MUST describe the
   absence of meaningful signal (e.g., "Market pressure: No material competitive
   activity detected in today's monitoring window.").
```

User prompt continues to pass the digest of stored articles (headlines + impact + so-what).

Validation in `generate_macro_summary()`:

- `dominant_condition` not in the 9-value list → coerce to `"Mixed / Watch"`.
- `executive_bullets` not a list of exactly 3 objects with `label` and `body` keys, or labels in the wrong order/case → discard the bullets and leave the field NULL (delivery falls back to legacy `executive_summary`).
- If `dominant_condition == "Low Signal"`, coerce the third bullet's body to `"No action required."` regardless of what the LLM returned.
- Legacy `executive_summary` is **still populated** by joining the three bullet bodies into one paragraph for backward compatibility. Legacy `macro_sentiment` is **still populated** with the `dominant_condition` value.

**Upsert behaviour.** `generate_macro_summary()` upserts on `(run_date, run_mode)` rather than `run_date`. `run_mode` is read once from `os.environ.get("MARKET_PULSE_RUN_MODE", "")` and mapped to `'test'` if it equals `"test"` (case-insensitive), else `'production'`.

### 6.3 Config — `market_pulse_config.yaml`

Three new blocks added; legacy `strategic_segments:` block is **kept** for one release cycle (read by nothing new; serves as documentation while old rows are still in the lookback window). A new `delivery_suppression:` block holds the heuristics used in §7.3.

```yaml
commercial_segments:
  healthcare:
    label: "Healthcare"
    description: >
      Medical devices, pharmaceutical packaging, diagnostics equipment, regulated
      healthcare polymer applications, biocompatible and sterilizable plastics,
      clean-room compounding. Includes FDA/EMA regulatory signals specific to
      medical materials. Do NOT use for general consumer products that merely
      happen to be plastic.

  fibers:
    label: "Fibers"
    description: >
      Synthetic fibers, textiles, nonwovens, filaments, yarn, performance fabric,
      carpet fiber, geotextiles. Polyester, nylon, polypropylene, and acrylic fiber
      chains. Includes fiber-grade resin pricing and end-market apparel/home-textiles
      demand signals. Route finished consumer textile products to Packaging or
      Industrial instead.

  transportation_automotive:
    label: "Transportation - Automotive"
    description: >
      Light vehicles (passenger car, SUV, light truck), automotive OEMs, Tier-1
      and Tier-2 suppliers; interiors, exteriors, under-hood, electrification
      (EV/HEV), ADAS materials, body panels, lighting, fluid systems. Includes
      automotive-industry pricing, demand, and regulatory signals. Route heavy
      trucks/buses to Non-Automotive; route aircraft/space to Aerospace.

  transportation_non_automotive:
    label: "Transportation - Non-Automotive"
    description: >
      Off-highway and heavy vehicles: Class 4–8 trucks, buses, agriculture
      equipment, construction equipment, rail, marine, recreational vehicles,
      motorcycles. Excludes passenger cars (→ Automotive) and aerospace (→
      Aerospace).

  transportation_aerospace:
    label: "Transportation - Aerospace"
    description: >
      Commercial and defense aerospace (fixed-wing, rotorcraft), space, and
      advanced air mobility (UAS, UAM, eVTOL). Includes aerospace materials
      qualification (FAR 25.853 flame, OEM specs) and defense procurement
      signals. Do NOT use for ground transportation.

  industrial:
    label: "Industrial"
    description: >
      Industrial plastics, durable goods, wire and cable, building products,
      construction materials, agricultural applications, electrical infrastructure.
      Includes general industrial demand cycles and construction-driven plastics
      consumption. Route healthcare devices and automotive end-uses to their own
      segments.

  packaging:
    label: "Packaging"
    description: >
      Flexible and rigid packaging, consumer-goods packaging, brand-owner demand
      signals, food contact materials, sustainable packaging trends, packaging
      legislation, converter and brand-owner sourcing shifts. Route industrial
      packaging that is structurally part of a durable good to Industrial.

  engineered_resins:
    label: "Engineered Resins"
    description: >
      High-performance compounds and resin platforms where mechanical, thermal,
      flame, electrical, chemical, or processing performance drives material
      selection. Use for engineered polymers, custom compounds, material
      qualification, and resin-technology signals that are NOT clearly tied to
      one downstream end-market. If the article is clearly about an end-market
      application (a healthcare device using a custom compound, an automotive
      interior using PA66), prefer that end-market segment over Engineered Resins.

  enterprise_cross_segment:
    label: "Enterprise / Cross-Segment"
    description: >
      Spans multiple commercial segments or addresses Americhem-wide topics
      (corporate finance, multi-segment supplier moves, generic industry trends).
      Use only when no single end-market dominates the article.

signal_types:
  competitive:
    label: "Competitive"
    description: >
      Direct competitor moves — pricing, capacity, product launches, M&A activity,
      executive hires, plant openings/closures by named competitors (Avient,
      Techmer PM, Ampacet, RTP, Penn Color, KRAIBURG, etc.).
  customer:
    label: "Customer"
    description: >
      Americhem customer or named brand-owner signals — expansions, contractions,
      sourcing changes, product launches, plant moves. Includes OEM sourcing
      decisions and downstream brand shifts.
  regulatory:
    label: "Regulatory"
    description: >
      Government, agency, or standards-body actions affecting chemicals, plastics,
      packaging, or end-markets. PFAS, EPR, REACH, Prop 65, FDA medical device
      rules, chemical reporting requirements.
  sustainability:
    label: "Sustainability"
    description: >
      Recycled-content mandates, circularity initiatives, carbon-reduction targets,
      EPR programs, ESG commitments by brand owners, sustainability-driven material
      substitution.
  supply_chain:
    label: "Supply Chain"
    description: >
      Resin pricing, monomer/feedstock supply, pigment and additive availability,
      logistics, port/rail disruptions, force majeure events, supplier capacity
      changes.
  technology:
    label: "Technology"
    description: >
      New chemistry, processes, manufacturing innovations, materials-science
      research, novel additive platforms, recycling-technology breakthroughs.
  macro:
    label: "Macro"
    description: >
      Industry-wide demand or pricing shifts, macroeconomic indicators (GDP,
      manufacturing PMI, construction starts) that affect plastics consumption.
  other:
    label: "Other"
    description: >
      Use only when none of the seven categories above clearly fit; prefer a named
      type whenever possible.

macro_conditions:
  - Competitive Pressure
  - Supply Volatility
  - Demand Expansion
  - Demand Softness
  - Regulatory Pressure
  - Sustainability Pull
  - Commercial Opportunity
  - Mixed / Watch
  - Low Signal

executive_bullet_labels:
  - Market pressure
  - Supply chain watch
  - Commercial action

delivery_suppression:
  enable_duplicate_headline: true
  enable_semantic_duplicate_headline: true
  headline_duplicate_threshold: 90   # rapidfuzz token_sort_ratio
  enable_product_listing: true
  enable_job_posting: true
  job_posting_override_action: "Escalate to leadership"
  enable_generic_market_report: true
  enable_unrelated_color_result: true
  enable_enterprise_low_impact: true
  enterprise_min_impact: 7
  url_patterns_product_listing:
    - "/product/"
    - "/shop/"
    - "/listing/"
    - "/store/"
    - "/p/"
    - "amazon.com"
    - "ebay.com"
    - "etsy.com"
  url_patterns_job_posting:
    - "/jobs/"
    - "/careers/"
    - "/hiring/"
    - "linkedin.com/jobs"
    - "indeed.com"
    - "glassdoor.com"
  title_patterns_generic_market_report:
    - "market size"
    - "market report"
    - "market forecast"
    - "market analysis"
    - "market outlook"
    - "growth analysis"
    - "CAGR"
    - "to reach $"
  color_terms:
    - "color"
    - "colour"
    - "colors"
    - "colours"
  plastics_relevance_terms:
    - "plastic"
    - "polymer"
    - "resin"
    - "compound"
    - "masterbatch"
    - "additive"
    - "colorant"
    - "pigment"
    - "compounding"
    - "thermoplastic"
    - "elastomer"
```

`_build_segment_rule()` is replaced by `_build_commercial_segment_rule()` and `_build_signal_type_rule()`. The `{rule4}` placeholder in the system prompt is replaced by `{rule4}\n\n{rule5}` (with rule 5 being the signal-type rule). The existing rule numbering shifts: current RULE 5 ("RIGOROUS IMPACT STATEMENT") → RULE 6; current RULE 6 ("DOMAIN RELEVANCE FIREWALL") → RULE 7.

---

## 7. Delivery rendering

### 7.1 New / renamed helpers in `delivery_engine.py`

- `_run_mode() -> str` — returns `'test'` or `'production'` from `MARKET_PULSE_RUN_MODE`. Replaces the boolean-returning `_is_test_mode()` for callsites that need the string value; `_is_test_mode()` is kept as a thin wrapper for backward compatibility.
- `_commercial_segment_of(row) -> str` — returns `row["commercial_segment"]` if non-empty, else maps `row["strategic_segment"]` via the static fallback dict in §7.2; falls back to `"Enterprise / Cross-Segment"`.
- `_signal_type_of(row) -> str` — returns `row["signal_type"]` if non-empty, else `"Other"`.
- `_apply_delivery_suppression(rows, config) -> (kept_rows, suppression_counts, samples)` — the new final guardrail pass; see §7.3.
- `_group_by_commercial_segment(items)` — replaces `_group_for_thematic`; keys off `_commercial_segment_of()`.
- `_render_segment_watch_section(groups, synthesis)` — replaces `_render_thematic_section`. Renders one block per commercial segment. Article rows show `Impact: X/10 · {sentiment_tag} · Signal: {signal_type}` followed by the linked headline and "So what: …" (americhem_impact).
- `_render_executive_bullets(bullets) -> str` — renders the 3-bullet exec summary from `daily_summaries.executive_bullets`.
- `_render_qa_debug_section(summary_row) -> str` — gated by `_is_test_mode()`; renders counts, breakdown, and the samples list.
- `_render_peripheral_section()` — **deleted**. Its contents (ungrouped 4–5s) become `weak_relevance` accounting in the QA section.

### 7.2 Legacy `strategic_segment` → commercial_segment fallback map

Used only when a row has no `commercial_segment` (i.e., old rows from before this sprint):

| Legacy `strategic_segment` | Fallback `commercial_segment` |
|---|---|
| `Healthcare` | `Healthcare` |
| `Fibers` | `Fibers` |
| `Packaging` | `Packaging` |
| `Industrial` | `Industrial` |
| `Raw Materials / Supply Chain` | `Enterprise / Cross-Segment` |
| `Regulatory / Sustainability` | `Enterprise / Cross-Segment` |
| `Competitive / Customer Signal` | `Enterprise / Cross-Segment` |
| `Broader Americhem` | `Enterprise / Cross-Segment` |
| `(NULL or unknown)` | `Enterprise / Cross-Segment` |

This is a known low-fidelity mapping; it stops mattering once the 24h / 72h lookback rolls past the deploy date.

**We intentionally do not infer `commercial_segment` from the original discovery `category`** (the trigger group in `targets.yaml`). Those categories are discovery-oriented controls — entities are grouped by search intent, not by Americhem commercial segment. Inferring from them would produce confident-but-wrong mappings (e.g., a "competitors" trigger doesn't tell us whether the resulting article is about Healthcare or Packaging). Accept the 72-hour lossy window instead.

### 7.3 Final visible-candidate suppression pass

After fetching rows from `daily_intelligence` and before grouping, delivery runs a deterministic suppression pass that applies the rules in `market_pulse_config.yaml` → `delivery_suppression:`. Each rule is evaluated in this order, and the first matching rule wins (a row is counted once, by the reason that fired first):

1. **`enterprise_cross_segment_low_impact`** — if `_commercial_segment_of(row) == "Enterprise / Cross-Segment"` AND `americhem_impact_score < enterprise_min_impact` (default 7). This catches generic plastics-industry items that pass the visible threshold but have no clear segment fit.
2. **`product_listing`** — if `source_url` contains any of `url_patterns_product_listing`.
3. **`job_posting`** — if `source_url` contains any of `url_patterns_job_posting`, UNLESS `recommended_action == job_posting_override_action` (default `"Escalate to leadership"`). The override exists for high-profile executive moves the LLM has already flagged for escalation.
4. **`generic_market_report`** — if `headline` (case-insensitive) contains any of `title_patterns_generic_market_report` AND `entities_mentioned` is empty. The check is intentionally narrow: if the LLM populated `entities_mentioned` with anything (a company, chemical, or region), we trust that signal and keep the row. This drops "Global Polypropylene Market 2026–2032" boilerplate (where the LLM typically returns an empty entity list) while keeping "Avient acquires X to enter polypropylene compounding." We accept that a market-report headline whose LLM-extracted entities consist only of commodity chemicals (e.g., `["polypropylene"]`) will slip through; refining this beyond the empty-list check is deferred to a future sprint.
5. **`unrelated_color_result`** — if `headline` (case-insensitive) contains any term from `color_terms` AND none of `plastics_relevance_terms` appears in `headline`, `americhem_impact`, or `entities_mentioned` (case-insensitive). This catches "what extension cord color means" and "uniform color distribution in hand lamps" without dropping legitimate colorant/masterbatch articles.
6. **`duplicate_headline`** — exact case-insensitive equality with any previously kept row in this run.
7. **`semantic_duplicate_headline`** — rapidfuzz `token_sort_ratio` against the running set of kept-row headlines ≥ `headline_duplicate_threshold` (default 90). Higher than ingestion's 88 because by delivery time we have full headlines, not Serper titles, and a tighter threshold avoids false positives within the same segment.

Each suppression bumps the appropriate counter and appends `{reason, source_url, headline}` to the run's `suppression_samples` buffer (capped at 10 across all sources).

Order rationale:

- The enterprise-low-impact rule runs **first** because it is the cheapest and most discriminating — it eliminates a known noise source before pattern matching.
- Pattern-based filters (product/job/market-report/color) run before headline-dedupe so that two product listings with the same title aren't both suppressed under the wrong reason.
- Headline dedup runs **last** so the kept set against which we compare is already free of pattern-blocked items.

**Override hooks** — each rule has an `enable_*` flag in config so the heuristics can be tuned or disabled per-rule without code changes. If a rule turns out to over-suppress in production, Jason can set the flag to `false` and re-run the test workflow.

**Out of scope for this pass:** no LLM call, no per-row classification beyond pattern matching. The point is to be a deterministic guardrail, not a second classifier — the LLM has already provided `commercial_segment`, `americhem_impact_score`, and `entities_mentioned`, and we trust those.

### 7.4 Pipeline order in `generate_html_email()`

```
1. data = fetch_todays_intelligence()                          # filter by created_at >= cutoff
2. kept, sup_counts, sup_samples = _apply_delivery_suppression(data, config)   # NEW
3. visible = [row for row in kept if _effective_impact(row) >= visible_impact_threshold]
4. below_threshold_count = len(kept) - len(visible)            # delivery-side count
5. groups = _group_by_commercial_segment(visible)
6. groups, capped_out = _apply_caps(groups, max_per_segment, max_total_visible)
7. weak_relevance_count = count of rows in kept that have effective impact 4-5 and aren't in any final group
8. update daily_summaries on (run_date, run_mode):
     - surfaced_count = number of rendered cards
     - extend suppression_breakdown with sup_counts + below_threshold + weak_relevance
     - extend suppression_samples with sup_samples (still capped at 10)
9. render header + executive_bullets + segment_watch + (test-only) qa_debug
```

### 7.5 Header & exec-summary rendering

`generate_html_email()`:

- Read `screened_count`, `dominant_condition`, `executive_bullets`, `suppression_breakdown`, and `suppression_samples` from `fetch_macro_summary()` using the matching `(run_date, run_mode)` key.
- Compute `surfaced_count` from the final visible-card list (after threshold filtering and caps).
- Right before sending, write `surfaced_count` and the delivery-side suppression entries back to the matching `daily_summaries` row via an `update()` filtered by both `run_date` and `run_mode`. Non-critical write: on failure, log and continue (don't fail the email).
- Subtitle line: `{today_str} · {surfaced} surfaced signals from {screened} screened items`.
- `dominant_condition` replaces the green `macro_sentiment` badge. Same styling treatment, different copy.
- If `executive_bullets` is present and well-formed, render the 3-bullet layout. Otherwise render the legacy `executive_summary` paragraph.

**Null-safe fallbacks** for header counts:

- If `screened_count` is NULL on the summary row, the header uses `len(data)` (the row count returned by `fetch_todays_intelligence()`). This avoids rendering `from None screened items`.
- If `surfaced_count` is NULL on the summary row at fetch time (the row will be NULL until delivery's `update()` lands), the header uses the size of the visible-card list computed in this run.
- If `dominant_condition` is NULL, the header omits the `· Dominant condition: …` clause cleanly rather than printing `None`.

### 7.6 Per-card meta strip

Replace the current bottom-right "Impact: 8/10 · Negative" treatment with a single horizontal meta strip placed under the headline:

```
Impact: 8/10 · Negative · Signal: Supply Chain
```

`signal_type` is rendered as plain text (not a pill) to avoid visual clutter. `commercial_segment` is implicit in the section heading and not repeated on the card.

For legacy rows (no `signal_type`), omit the `· Signal: …` clause cleanly.

For legacy rows with `sentiment_score <= 3` (the old "Critical" path), append a `· CRITICAL` badge (red text) to the meta strip. This is the only place "Critical" surfaces in the new design.

---

## 8. Suppression and dedupe logic

Suppression now operates as a **two-layer system**:

1. **Ingestion-side suppression** — drops rows before they reach the DB. Catches the noise the LLM and dedupe can identify pre-storage (duplicate URLs, semantic-duplicate headlines, LLM `DISCARD`, scrape failures).
2. **Delivery-side final suppression pass** — runs the §7.3 deterministic guardrail against stored rows immediately before grouping. Catches product listings, job postings, generic market reports, unrelated color results, and low-impact Enterprise / Cross-Segment items that slipped past ingestion. Also handles delivery-only logic (threshold filtering, weak relevance, headline dedup across the surfaced set).

Both layers contribute to `suppression_breakdown` and `suppression_samples` on `daily_summaries`. The combined record is what powers the QA section.

### 8.1 Ingestion-side accounting

`execute_pipeline()` in `ingestion_engine.py` tracks both counts and samples:

- Counts already exist in `stats`. Reason keys captured by ingestion: `duplicate_url`, `semantic_duplicate`, `llm_discard`, `scrape_failed`. Delivery-side keys are written separately and never overlap.
- New rolling buffer `suppression_samples: list[dict]` capped at 10 items, FIFO. Each entry: `{"reason": "<reason_code>", "url": <raw_url>, "title": <serper_title>}`.
- Captured at each existing suppression point:
  - `url_already_processed()` returns True → `duplicate_url`
  - `is_semantic_duplicate()` returns True → `semantic_duplicate`
  - `insight.get("americhem_impact") == "DISCARD"` → `llm_discard`
  - `scrape_article()` returns None → `scrape_failed`
- `generate_macro_summary()` accepts the suppression counts and samples; upserts them to `daily_summaries.suppression_breakdown` and `.suppression_samples` (under the row keyed by `(run_date, run_mode)`). Also writes `screened_count = stats["urls_discovered"]`.

### 8.2 Delivery-side accounting

In `generate_html_email()` (the pipeline order is in §7.4):

- `_apply_delivery_suppression()` returns counts and samples for the seven pattern-based reason codes.
- After threshold filtering and grouping, count `below_impact_threshold` and `weak_relevance` directly.
- Merge all delivery-side counts into the existing `suppression_breakdown` JSON on the row; extend `suppression_samples` with whatever the suppression pass appended (the combined buffer is still capped at 10 — last-N preserved).
- `update()` is filtered by both `run_date` and `run_mode`. If the row doesn't exist (e.g., delivery is running standalone via the test workflow with `run_ingestion=false`), the update is a no-op and we log a warning.

### 8.3 Why this is on `daily_summaries`, not a new table

A separate `suppressed_signals` table is the cleaner long-term design, but:

- The use case today is QA-only — Jason needs to see whether the system is improving, not run analytics.
- JSONB on the existing row covers it without new ops surface.
- Promotion path is clean: if we later need historical suppression analytics, we copy the JSONB columns into a new normalized table during a future sprint.

---

## 9. Test-mode debug behavior

Gating: a single boolean, `_is_test_mode()`, already exists. The QA debug section renders only when this returns True.

Placement: below the segment-watch section, separated by a horizontal rule. Styled in muted gray (≤12px) with the existing `TEST RUN · Jason-only QA output` amber banner already at the top of the email.

Content:

```
QA · Suppression Summary
Screened: 87 · Surfaced: 6 · Suppressed: 81

By reason:
  duplicate URL                            23
  semantic duplicate                        4
  LLM discard                              12
  scrape failed                            18
  below impact threshold                   22
  weak relevance (4–5, ungrouped)           2
  duplicate headline                        1
  semantic duplicate headline               1
  product listing                           5
  job posting                               3
  generic market report                     4
  unrelated color result                    2
  Enterprise / Cross-Segment, low impact    3

Last 10 suppressed items:
  [LLM discard]               "Best extension cord colors"        — example.com/...
  [duplicate URL]             "Shaw sustainability report 2025"   — example.com/...
  [product listing]           "Pretty plastic tote — 24 ct"       — example.com/...
  [generic market report]     "Global polypropylene market 2032"  — example.com/...
  [unrelated color result]    "Uniform color distribution lamp"   — example.com/...
  [enterprise low impact]     "Vibe coding phenomenology models"  — example.com/...
  …
```

The labels in the email use friendly phrasing; the internal reason codes (snake_case) remain the keys in `suppression_breakdown`. A small label map in delivery handles the translation.

Production renders an empty string for this section. There is no toggle in production config to enable it — the only switch is `MARKET_PULSE_RUN_MODE=test`, which is set by the test workflow alone.

---

## 10. Backward compatibility

Three compatibility surfaces:

1. **Old `daily_intelligence` rows** (`strategic_segment` set, `commercial_segment` / `signal_type` NULL):
   - `_commercial_segment_of()` maps via the table in §7.2.
   - `_signal_type_of()` returns `"Other"`.
   - Meta strip on the card omits the `· Signal: …` clause cleanly.
   - This fallback is exercised for at most 72 hours after deploy (Monday's lookback window).

2. **Old `daily_summaries` rows** (`run_mode` NULL → backfilled to `'production'` by migration default; `dominant_condition` / `executive_bullets` NULL):
   - Delivery falls back to `macro_sentiment` for the header badge.
   - Delivery falls back to `executive_summary` prose for the body.
   - Header uses the null-safe fallbacks in §7.5 if `screened_count` / `surfaced_count` are NULL.
   - Matters for at most one day after deploy, since `fetch_macro_summary()` reads only the latest row for the current `run_mode`.

3. **Test fixtures using `strategic_segment`**:
   - Existing tests keep passing via the fallback path.
   - New tests cover `commercial_segment` / `signal_type` directly.

No data backfill is required. No production secret rotation is required.

---

## 11. Testing strategy

New and updated tests in `tests/test_pipeline.py`.

### 11.1 Per-article contract

- `test_synthesize_insight_validates_commercial_segment`: valid label preserved; invalid → `"Enterprise / Cross-Segment"`; missing → default.
- `test_synthesize_insight_validates_signal_type`: same pattern with `"Other"`.
- `test_synthesize_insight_drops_strategic_segment_field`: when the LLM still returns `strategic_segment`, it is ignored — the payload upserted to Supabase does not include it.
- `test_synthesize_insight_system_prompt_includes_segments_and_signals`: assert both the commercial segments block and the signal types block are in the system message, with full descriptions present (not just labels).

### 11.2 Macro summary contract

- `test_generate_macro_summary_dominant_condition_enum`: valid value preserved; invalid → `"Mixed / Watch"`; missing → defaults via the article-count rule (`"Low Signal"` when <3 articles).
- `test_generate_macro_summary_executive_bullets_valid`: 3-object list preserved with `Market pressure` / `Supply chain watch` / `Commercial action` labels in that order.
- `test_generate_macro_summary_executive_bullets_invalid`: wrong count / wrong labels / wrong shape → `executive_bullets` written as NULL; legacy `executive_summary` still populated.
- `test_generate_macro_summary_low_signal_forces_no_action`: when `dominant_condition == "Low Signal"`, the third bullet body is coerced to `"No action required."` regardless of LLM output.
- `test_generate_macro_summary_writes_screened_count`: `screened_count` written from `stats["urls_discovered"]`.
- `test_generate_macro_summary_writes_suppression_breakdown`: counts dict written; samples list capped at 10.
- `test_generate_macro_summary_upsert_uses_run_date_and_run_mode`: upsert key is `(run_date, run_mode)`; `MARKET_PULSE_RUN_MODE=test` produces a row with `run_mode='test'` that does not collide with a production row of the same date.

### 11.3 Suppression tracking (ingestion)

- `test_pipeline_records_duplicate_url_reason`: a URL hash collision adds to `duplicate_url`.
- `test_pipeline_records_semantic_duplicate_reason`: a fuzzy headline match adds to `semantic_duplicate`.
- `test_pipeline_records_llm_discard_reason`: a DISCARD response adds to `llm_discard`.
- `test_pipeline_records_scrape_failed_reason`: Firecrawl + fallback failure adds to `scrape_failed`.
- `test_suppression_samples_buffer_caps_at_10_fifo`: 15 items in → 10 items out, last-N preserved.

### 11.4 Delivery-side suppression pass

- `test_delivery_suppression_exact_duplicate_headline`: two visible candidates with identical headlines → second is suppressed under `duplicate_headline`.
- `test_delivery_suppression_semantic_duplicate_headline`: two candidates with rapidfuzz score ≥ threshold → second is suppressed under `semantic_duplicate_headline`.
- `test_delivery_suppression_product_listing_url`: row with `source_url` containing `/product/` → suppressed under `product_listing`.
- `test_delivery_suppression_job_posting_url`: row with `linkedin.com/jobs` URL → suppressed under `job_posting`.
- `test_delivery_suppression_job_posting_escalate_override`: row with job-posting URL AND `recommended_action == "Escalate to leadership"` → kept.
- `test_delivery_suppression_generic_market_report`: title `"Global Polypropylene Market 2026–2032"` with no named entity → suppressed under `generic_market_report`.
- `test_delivery_suppression_generic_market_report_kept_when_entity_present`: same title but `entities_mentioned=["Avient"]` → kept.
- `test_delivery_suppression_unrelated_color_result`: title `"What extension cord colors mean"` with no plastics-relevance terms → suppressed under `unrelated_color_result`.
- `test_delivery_suppression_unrelated_color_result_kept_when_plastics_term_present`: title `"New masterbatch colors for automotive interiors"` → kept.
- `test_delivery_suppression_enterprise_low_impact`: row with `commercial_segment="Enterprise / Cross-Segment"` and `americhem_impact_score=5` → suppressed under `enterprise_cross_segment_low_impact`.
- `test_delivery_suppression_enterprise_high_impact_kept`: same segment with score 8 → kept.
- `test_delivery_suppression_first_match_wins`: a row that matches both `product_listing` and `duplicate_headline` is counted once under the first matching reason (product_listing per ordering).
- `test_delivery_suppression_disabled_via_config`: setting `enable_product_listing: false` allows a product listing through.

### 11.5 Delivery rendering

- `test_generate_html_email_groups_by_commercial_segment`: two articles with `commercial_segment="Healthcare"` group together under a Healthcare heading.
- `test_generate_html_email_falls_back_to_strategic_segment_for_legacy_rows`: a row with only `strategic_segment="Healthcare"` and no `commercial_segment` still groups under Healthcare via the fallback map.
- `test_generate_html_email_legacy_competitive_segment_maps_to_enterprise`: a row with `strategic_segment="Competitive / Customer Signal"` maps to `"Enterprise / Cross-Segment"`.
- `test_generate_html_email_no_peripheral_section_in_production`: `MARKET_PULSE_RUN_MODE` unset → string `"PERIPHERAL SIGNALS"` not present in HTML.
- `test_generate_html_email_no_critical_section`: `"CRITICAL DISRUPTIONS"` not present in HTML; legacy `sentiment_score=2` rows still appear, under their commercial segment, with a `CRITICAL` badge in the meta strip.
- `test_generate_html_email_qa_debug_section_in_test_mode`: `MARKET_PULSE_RUN_MODE=test` → QA section with screened/surfaced counts and at least one reason line is present, including delivery-side reasons.
- `test_generate_html_email_qa_debug_section_absent_in_production`: production HTML does not contain "QA · Suppression Summary".
- `test_generate_html_email_renders_executive_bullets`: when `executive_bullets` is well-formed, the 3-bullet layout appears with the `Market pressure` / `Supply chain watch` / `Commercial action` labels; legacy paragraph does not.
- `test_generate_html_email_falls_back_to_legacy_executive_summary`: when `executive_bullets` is NULL or malformed, the legacy paragraph appears.
- `test_generate_html_email_header_shows_surfaced_and_screened`: header contains `"6 surfaced signals from 87 screened items"`.
- `test_generate_html_email_header_shows_dominant_condition`: header contains the condition string from the summary row.
- `test_generate_html_email_header_omits_condition_when_null`: when `dominant_condition` is NULL the header omits the clause without rendering `None`.
- `test_generate_html_email_header_falls_back_to_len_data_when_screened_null`: `screened_count` NULL → header uses `len(data)`.

### 11.6 Run-mode isolation

- `test_run_mode_test_does_not_clobber_production`: writing a `'test'` row for date D and a `'production'` row for date D produces two distinct rows; neither overwrites the other.
- `test_fetch_macro_summary_filters_by_run_mode`: production delivery fetches the `'production'` row even when a `'test'` row exists for the same date; vice versa for test delivery.

### 11.7 Card meta strip

- `test_render_card_meta_strip_new_style`: `Impact: 8/10 · Negative · Signal: Supply Chain` rendered as a single line.
- `test_render_card_meta_strip_legacy_omits_signal_clause`: legacy row (no `signal_type`) renders `Impact: 8/10 · Negative` without trailing `· Signal: …`.
- `test_render_card_meta_strip_critical_badge_for_legacy_low_score`: legacy `sentiment_score=2` row gets `· CRITICAL` appended.

### 11.8 Update existing tests

- Tests that assert `"Critical Disruptions"` or `"Thematic Intelligence"` section labels need updating to `"Commercial Segment Watch"` and the dropped-section assertions above. The two "Critical headline" and "Peripheral headline" routing tests are rewritten to reflect the new architecture: critical legacy → segment watch with badge; peripheral 4–5 → QA section (test mode) or hidden (production).
- Tests that mock `fetch_macro_summary()` need to provide `run_mode` and the new structured fields where relevant.

---

## 12. Migration plan

1. **Apply migration 002** via the Supabase SQL editor. Idempotent; safe to apply before code deploys. Verify that:
   - `daily_summaries.run_mode` exists with default `'production'`.
   - The old unique index on `run_date` is dropped.
   - The new unique index on `(run_date, run_mode)` is in place.
   - Existing rows have `run_mode = 'production'` after the default backfill.
2. **Update `schema.sql`** so a fresh DB initializes correctly from one file. Should match migration 002's column set and indexes.
3. **Deploy ingestion + delivery + config together.** Backward compatibility ensures the first run after deploy (with mostly legacy rows in lookback) still renders.
4. **Run the test workflow** with `run_ingestion=true` and `send_email=true`. Verify:
   - The test run creates a `run_mode='test'` row on today's date.
   - The QA section renders with both ingestion and delivery suppression counts.
   - Production's existing summary row (if any) for today is **untouched**.
5. **Monitor the first production run** the next weekday. Verify: header counts non-zero, `dominant_condition` is not "Cautiously Optimistic," all visible articles have a commercial segment heading, the production summary row has `run_mode='production'`, and the test row (if any) from earlier QA is still present and distinct.

---

## 13. Known limitations

- **The fallback map for legacy rows is low-fidelity** for `Competitive / Customer Signal` and `Regulatory / Sustainability` — they all bucket to `Enterprise / Cross-Segment`. This is acceptable because the lookback window flushes legacy rows within 3 days. Inferring from the original discovery `category` is intentionally avoided (discovery categories are not commercial-segment controlled values).
- **Delivery-side heuristics are deterministic, not exhaustive.** The pattern lists in `delivery_suppression` will miss novel noise patterns. Mitigation: every suppression rule has an `enable_*` flag plus tunable pattern lists in YAML, and the QA section surfaces what was actually suppressed so we can adjust the patterns iteratively.
- **`surfaced_count` is delivery-written.** If delivery fails, the count on the row is whatever ingestion wrote (NULL). The null-safe header fallback uses the in-process visible-card count in that case.
- **`suppression_samples` is capped at 10.** Jason gets a representative slice, not an exhaustive list. If we need exhaustiveness later, that's the trigger for promoting the JSONB blob to a normalized table.
- **`run_mode` does not isolate `daily_intelligence`**, only `daily_summaries`. A test ingestion run still writes article rows into the same `daily_intelligence` table that production reads. This is acceptable because article rows are content-addressed by URL hash (no overwrite risk) and because removing test rows is out of scope this sprint. Future work could add `run_mode` to `daily_intelligence` if needed, but the cost/benefit doesn't justify it now.

---

## 14. Decisions index

| Decision | Choice | Rationale |
|---|---|---|
| Approach scope | B + small C (schema + prompt + delivery-suppression + debug accounting) | User-specified; pure delivery mapping can't fix the LLM's confused taxonomy, and observability without filtering doesn't fix the trust problem |
| Segment vs. signal | Two separate columns | One field doing two jobs is the root cause of weak grouping |
| Legacy `strategic_segment` | Kept; new rows write NULL; no inference from discovery category | No backfill; lookback window flushes within 72h; category is discovery-oriented, not commercial-segment controlled |
| Critical Disruptions section | Removed; replaced with per-card badge | Avoids empty-section edge cases; matches target structure |
| Peripheral Signals section | Removed from production; reaccounted in QA | Stakeholders don't see noise; Jason still sees rigor |
| Delivery-side final suppression pass | Deterministic guardrail in `_apply_delivery_suppression()` | Observability alone doesn't prevent visible noise; pattern-based filters catch what the LLM's `DISCARD` rule misses |
| Suppression storage | JSONB on `daily_summaries` | QA-only need; new table is over-engineering |
| Dominant condition | 9-value enum, validated in code | Stops "Cautiously Optimistic" defaulting |
| Executive bullet labels | `Market pressure` / `Supply chain watch` / `Commercial action` | "Market pressure" avoids forcing a competitive framing every day; allows demand/regulatory/sustainability emphasis under the same label |
| Low-signal day handling | Coerce third bullet body to `"No action required."` | Prevents fake-urgency action bullets on quiet days |
| `run_mode` column on `daily_summaries` | Unique index on `(run_date, run_mode)` | Test runs must not overwrite production summary rows; lightweight alternative to a separate Supabase project |
| `screened_count` definition | Total URLs discovered before suppression and deduplication | Matches the user-facing "screened items" framing |
| `surfaced_count` write | Delivery, via `update()` on the `(run_date, run_mode)` row | Only delivery knows the final visible count |
| Header counts source | `daily_summaries` (screened from ingestion, surfaced from delivery), with null-safe fallbacks | Single source of truth per run, with graceful degradation |
