# Commercial Intelligence Brief — Design

**Date:** 2026-05-21
**Status:** Approved (pending spec review)
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

This sprint addresses all six product gaps as a single schema + prompt + delivery redesign. It is **B (schema + prompt contract redesign) with a small C overlay (suppression-debug accounting)** — no ZoomInfo, no separate Supabase, no recipient personalization, no platform refactor.

---

## 2. Success criteria

1. A GMM or Sales leader can scan the email in under 90 seconds.
2. Articles are grouped by Americhem commercial segments (9-value taxonomy), not signal types.
3. Executive summary is 3 fixed-label BLUF bullets (Competitive pressure / Supply chain watch / Commercial action).
4. The macro label is drawn from a 9-value constrained enum and varies day-to-day with the underlying signal mix.
5. Duplicate and low-value content is suppressed before stakeholder delivery; Peripheral Signals does not appear in production.
6. Test pipeline exposes a QA / suppression-debug section visible only to Jason via `MARKET_PULSE_RUN_MODE=test`.
7. The header reframes volume as rigor: `{surfaced} surfaced signals from {screened} screened items · Dominant condition: {condition}`.

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
  • Competitive pressure: Techmer, Teknor Apex, and KRAIBURG are increasing specialty
    materials claims across transparent, film additive, and TPE applications.
  • Supply chain watch:   Mitsubishi and Asahi Kasei restructuring signals may create
    volatility in polymer and additive availability.
  • Commercial action:    Prioritize compliant additive platforms, healthcare TPE
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
             · below impact threshold (22) · scrape failed (18) · weak relevance (2)
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
  add column if not exists dominant_condition text,
  add column if not exists executive_bullets jsonb,
  add column if not exists screened_count integer,
  add column if not exists surfaced_count integer,
  add column if not exists suppression_breakdown jsonb,
  add column if not exists suppression_samples jsonb;
```

`schema.sql` is updated to match so a fresh DB initializes correctly from a single file.

### 5.2 Field semantics

**`daily_intelligence.commercial_segment`** — one of nine values; the end-market the article most affects for Americhem:

| Value | Notes |
|---|---|
| `Healthcare` | Medical devices, pharma packaging, biocompatible polymers |
| `Fibers` | Synthetic fibers, nonwovens, performance fabrics |
| `Transportation - Automotive` | Light-vehicle OEMs, automotive interiors/under-hood |
| `Transportation - Non-Automotive` | Off-highway, rail, marine, heavy equipment |
| `Transportation - Aerospace` | Aerospace and defense, including UAM/eVTOL |
| `Industrial` | Industrial plastics, durable goods, wire and cable, construction |
| `Packaging` | Flexible and rigid packaging, food contact, brand-owner signals |
| `Engineered Resins` | High-performance compounds, custom resin platforms |
| `Enterprise / Cross-Segment` | Spans multiple segments or no dominant fit |

**`daily_intelligence.signal_type`** — one of eight values; the *kind* of signal:

| Value | Notes |
|---|---|
| `Competitive` | Competitor move, product, pricing, M&A |
| `Customer` | Customer expansion/contraction, OEM sourcing, plant openings |
| `Regulatory` | PFAS, EPR, chemical compliance, reporting requirements |
| `Sustainability` | Recycled-content mandates, circularity, carbon targets |
| `Supply Chain` | Resin pricing, force majeure, logistics, supplier capacity |
| `Technology` | New chemistry, processes, materials science breakthroughs |
| `Macro` | Industry-wide demand or pricing shifts |
| `Other` | Catch-all when none of the above clearly fit |

**`daily_summaries.dominant_condition`** — one of nine values that summarises the overall commercial weather:

```
Competitive Pressure · Supply Volatility · Demand Expansion · Demand Softness ·
Regulatory Pressure · Sustainability Pull · Commercial Opportunity ·
Mixed / Watch · Low Signal
```

Defaults to `Mixed / Watch` if the LLM returns an invalid label. Defaults to `Low Signal` when fewer than 3 articles surfaced.

**`daily_summaries.executive_bullets`** — JSONB array of exactly 3 objects with fixed labels:

```json
[
  {"label": "Competitive pressure", "body": "<one sentence>"},
  {"label": "Supply chain watch",   "body": "<one sentence>"},
  {"label": "Commercial action",    "body": "<one sentence>"}
]
```

Labels are part of the prompt template; the LLM only writes the bodies. Validation rejects any structure that doesn't match this shape; on rejection, delivery falls back to the legacy `executive_summary` text.

**`daily_summaries.screened_count`** — integer; URLs the ingestion engine evaluated for the run (`stats["urls_discovered"]`).

**`daily_summaries.surfaced_count`** — integer; visible cards rendered by delivery. Written by `delivery_engine.py` via an `update()` on the row immediately before rendering.

**`daily_summaries.suppression_breakdown`** — JSONB object keyed by reason:

```json
{
  "duplicate_url":             23,
  "semantic_duplicate":         4,
  "llm_discard":               12,
  "below_impact_threshold":    22,
  "scrape_failed":             18,
  "weak_relevance":             2
}
```

`duplicate_url`, `semantic_duplicate`, `llm_discard`, and `scrape_failed` are written by ingestion. `below_impact_threshold` and `weak_relevance` are written by delivery via the same `update()` that sets `surfaced_count`.

**`daily_summaries.suppression_samples`** — JSONB array, capped at 10, FIFO across the run:

```json
[
  {"reason": "llm_discard",     "url": "https://...", "title": "Best extension cord colors"},
  {"reason": "duplicate_url",   "url": "https://...", "title": "Shaw sustainability report 2025"},
  …
]
```

### 5.3 Legacy columns

`daily_intelligence.strategic_segment` and `daily_intelligence.include_in_report` are **kept** but new rows leave them NULL. They are read only by the legacy-fallback path in delivery (see §8).

`daily_summaries.executive_summary` and `daily_summaries.macro_sentiment` are **kept** and continue to be populated by ingestion (derived from the structured fields) so that historical reads and external consumers don't break.

---

## 6. Ingestion prompt contract

### 6.1 Per-article prompt — `ingestion_engine.py`

RULE 4 is split into RULE 4 (commercial segment) and RULE 5 (signal type). Old RULE 5 and RULE 6 renumber to 6 and 7. The "six rules" wording in the prompt body becomes "seven rules."

**RULE 4 — COMMERCIAL SEGMENT** (text generated from `commercial_segments:` in config):

```
Assign the single best-fit commercial segment for the affected end-market:
  Healthcare: Medical devices, pharma packaging, biocompatible polymers…
  Fibers: Synthetic fibers, nonwovens, performance fabrics…
  Transportation - Automotive: Light-vehicle OEMs, interiors, under-hood…
  Transportation - Non-Automotive: Off-highway, rail, marine, heavy equipment…
  Transportation - Aerospace: Aerospace and defense…
  Industrial: Industrial plastics, durable goods, wire and cable, construction…
  Packaging: Flexible and rigid packaging, food contact, brand-owner signals…
  Engineered Resins: High-performance compounds, custom resin platforms…
  Enterprise / Cross-Segment: Spans multiple segments or no dominant fit…
Choose "Enterprise / Cross-Segment" only when the article spans multiple segments.
```

**RULE 5 — SIGNAL TYPE** (text generated from `signal_types:` in config):

```
Assign the single kind of signal this article represents:
  Competitive: Competitor moves, products, pricing, M&A.
  Customer: Customer expansion/contraction, OEM sourcing, plant openings.
  Regulatory: PFAS, EPR, chemical compliance, reporting requirements.
  Sustainability: Recycled-content mandates, circularity, carbon targets.
  Supply Chain: Resin pricing, force majeure, logistics, supplier capacity.
  Technology: New chemistry, processes, materials science breakthroughs.
  Macro: Industry-wide demand or pricing shifts.
  Other: Use only when none of the above clearly fit.
```

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
     {"label": "Competitive pressure", "body": "<one sentence, ≤30 words>"}
     {"label": "Supply chain watch",   "body": "<one sentence, ≤30 words>"}
     {"label": "Commercial action",    "body": "<one sentence, ≤30 words>"}

   Each body must reference specific named entities or segments from the digest.
   Do NOT hedge ("may", "could", "potentially") without a specific data point.
   Do NOT write generic statements ("monitor closely", "remain vigilant").
```

User prompt continues to pass the digest of stored articles (headlines + impact + so-what).

Validation in `generate_macro_summary()`:

- `dominant_condition` not in the 9-value list → coerce to `"Mixed / Watch"`.
- `executive_bullets` not a list of exactly 3 objects with `label` and `body` keys, or labels in the wrong order → discard the bullets and leave the field NULL (delivery falls back to legacy `executive_summary`).
- Legacy `executive_summary` is **still populated** by joining the three bullet bodies into one paragraph for backward compatibility. Legacy `macro_sentiment` is **still populated** with the `dominant_condition` value.

### 6.3 Config — `market_pulse_config.yaml`

Three new blocks added; legacy `strategic_segments:` block is **kept** for one release cycle (read by nothing new; serves as documentation while old rows are still in the lookback window).

```yaml
commercial_segments:
  healthcare:                    { label: "Healthcare",                    description: "..." }
  fibers:                        { label: "Fibers",                        description: "..." }
  transportation_automotive:     { label: "Transportation - Automotive",   description: "..." }
  transportation_non_automotive: { label: "Transportation - Non-Automotive", description: "..." }
  transportation_aerospace:      { label: "Transportation - Aerospace",    description: "..." }
  industrial:                    { label: "Industrial",                    description: "..." }
  packaging:                     { label: "Packaging",                     description: "..." }
  engineered_resins:             { label: "Engineered Resins",             description: "..." }
  enterprise_cross_segment:      { label: "Enterprise / Cross-Segment",    description: "..." }

signal_types:
  competitive:    { label: "Competitive",    description: "..." }
  customer:       { label: "Customer",       description: "..." }
  regulatory:     { label: "Regulatory",     description: "..." }
  sustainability: { label: "Sustainability", description: "..." }
  supply_chain:   { label: "Supply Chain",   description: "..." }
  technology:     { label: "Technology",     description: "..." }
  macro:          { label: "Macro",          description: "..." }
  other:          { label: "Other",          description: "..." }

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
  - Competitive pressure
  - Supply chain watch
  - Commercial action
```

`_build_segment_rule()` is replaced by `_build_commercial_segment_rule()` and `_build_signal_type_rule()`. The `{rule4}` placeholder in the system prompt is replaced by `{rule4}\n\n{rule5}` (with rule 5 being the signal-type rule). The existing rule numbering shifts: current RULE 5 ("RIGOROUS IMPACT STATEMENT") → RULE 6; current RULE 6 ("DOMAIN RELEVANCE FIREWALL") → RULE 7.

---

## 7. Delivery rendering

### 7.1 New / renamed helpers in `delivery_engine.py`

- `_commercial_segment_of(row) -> str` — returns `row["commercial_segment"]` if non-empty, else maps `row["strategic_segment"]` via a static fallback dict; falls back to `"Enterprise / Cross-Segment"`.
- `_signal_type_of(row) -> str` — returns `row["signal_type"]` if non-empty, else `"Other"`.
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

### 7.3 Header & exec-summary rendering

`generate_html_email()`:

- Compute `surfaced_count` from the final visible-card list (after threshold filtering and caps).
- Read `screened_count`, `dominant_condition`, and `executive_bullets` from `fetch_macro_summary()` (the function returns the whole row).
- Right before sending, write `surfaced_count`, `below_impact_threshold`, and `weak_relevance` counts back to today's `daily_summaries` row via an `update()`. This is a single non-critical write; on failure, log and continue (don't fail the email).
- Subtitle line: `{today_str} · {surfaced} surfaced signals from {screened} screened items`.
- `dominant_condition` replaces the green `macro_sentiment` badge. Same styling treatment, different copy.
- If `executive_bullets` is present and well-formed, render the 3-bullet layout. Otherwise render the legacy `executive_summary` paragraph.

### 7.4 Per-card meta strip

Replace the current bottom-right "Impact: 8/10 · Negative" treatment with a single horizontal meta strip placed under the headline:

```
Impact: 8/10 · Negative · Signal: Supply Chain
```

`signal_type` is rendered as plain text (not a pill) to avoid visual clutter. `commercial_segment` is implicit in the section heading and not repeated on the card.

For legacy rows (no `signal_type`), omit the `· Signal: …` clause cleanly.

For legacy rows with `sentiment_score <= 3` (the old "Critical" path), append a `· CRITICAL` badge (red text) to the meta strip. This is the only place "Critical" surfaces in the new design.

---

## 8. Suppression and dedupe logic

No new filters. Existing suppression is made **observable and auditable**.

### 8.1 Ingestion-side accounting

`execute_pipeline()` in `ingestion_engine.py` tracks both counts and samples:

- Counts already exist in `stats` — add `weak_relevance` and `below_impact_threshold` keys (they remain 0 from ingestion's side; delivery writes them).
- New rolling buffer `suppression_samples: list[dict]` capped at 10 items, FIFO. Each entry: `{"reason": "<reason_code>", "url": <raw_url>, "title": <serper_title>}`.
- Captured at each existing suppression point:
  - `url_already_processed()` returns True → `duplicate_url`
  - `is_semantic_duplicate()` returns True → `semantic_duplicate`
  - `insight.get("americhem_impact") == "DISCARD"` → `llm_discard`
  - `scrape_article()` returns None → `scrape_failed`
- `generate_macro_summary()` accepts the suppression counts and samples; writes them to `daily_summaries.suppression_breakdown` and `.suppression_samples`. Also writes `screened_count = stats["urls_discovered"]`.

### 8.2 Delivery-side accounting

In `generate_html_email()`:

- Count rows passed in from `fetch_todays_intelligence()` whose effective impact is **below** `visible_impact_threshold` → `below_impact_threshold`.
- Count rows whose effective impact is 4–5 and that didn't make it into any segment group → `weak_relevance` (this is the old "Peripheral" pool, now hidden).
- Combine with `surfaced_count` and write back to `daily_summaries` via `update()` on the run-date row. If the update fails, log a warning and continue.

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
  duplicate URL                 23
  semantic duplicate             4
  LLM discard / weak relevance  12
  below impact threshold        22
  scrape failed                 18
  weak relevance (4–5, ungrouped) 2

Last 10 suppressed items:
  [LLM discard]      "Best extension cord colors"      — example.com/...
  [duplicate URL]    "Shaw sustainability report 2025" — example.com/...
  [weak relevance]   "AI disaster images go viral"     — example.com/...
  …
```

Production renders an empty string for this section. There is no toggle in production config to enable it — the only switch is `MARKET_PULSE_RUN_MODE=test`, which is set by the test workflow alone.

---

## 10. Backward compatibility

Three compatibility surfaces:

1. **Old `daily_intelligence` rows** (`strategic_segment` set, `commercial_segment` / `signal_type` NULL):
   - `_commercial_segment_of()` maps via the table in §7.2.
   - `_signal_type_of()` returns `"Other"`.
   - Meta strip on the card omits the `· Signal: …` clause cleanly.
   - This fallback is exercised for at most 72 hours after deploy (Monday's lookback window).

2. **Old `daily_summaries` rows** (`dominant_condition` / `executive_bullets` NULL):
   - Delivery falls back to `macro_sentiment` for the header badge.
   - Delivery falls back to `executive_summary` prose for the body.
   - Matters for at most one day after deploy, since `fetch_macro_summary()` reads only the latest row.

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
- `test_synthesize_insight_system_prompt_includes_segments_and_signals`: assert both the commercial segments block and the signal types block are in the system message.

### 11.2 Macro summary contract

- `test_generate_macro_summary_dominant_condition_enum`: valid value preserved; invalid → `"Mixed / Watch"`; missing → defaults via the article-count rule (`"Low Signal"` when <3 articles).
- `test_generate_macro_summary_executive_bullets_valid`: 3-object list preserved.
- `test_generate_macro_summary_executive_bullets_invalid`: wrong count / wrong labels / wrong shape → `executive_bullets` written as NULL; legacy `executive_summary` still populated.
- `test_generate_macro_summary_writes_screened_count`: `screened_count` written from `stats["urls_discovered"]`.
- `test_generate_macro_summary_writes_suppression_breakdown`: counts dict written; samples list capped at 10.

### 11.3 Suppression tracking

- `test_pipeline_records_duplicate_url_reason`: a URL hash collision adds to `duplicate_url`.
- `test_pipeline_records_semantic_duplicate_reason`: a fuzzy headline match adds to `semantic_duplicate`.
- `test_pipeline_records_llm_discard_reason`: a DISCARD response adds to `llm_discard`.
- `test_pipeline_records_scrape_failed_reason`: Firecrawl + fallback failure adds to `scrape_failed`.
- `test_suppression_samples_buffer_caps_at_10_fifo`: 15 items in → 10 items out, last-N preserved.

### 11.4 Delivery rendering

- `test_generate_html_email_groups_by_commercial_segment`: two articles with `commercial_segment="Healthcare"` group together under a Healthcare heading.
- `test_generate_html_email_falls_back_to_strategic_segment_for_legacy_rows`: a row with only `strategic_segment="Healthcare"` and no `commercial_segment` still groups under Healthcare via the fallback map.
- `test_generate_html_email_legacy_competitive_segment_maps_to_enterprise`: a row with `strategic_segment="Competitive / Customer Signal"` maps to `"Enterprise / Cross-Segment"`.
- `test_generate_html_email_no_peripheral_section_in_production`: `MARKET_PULSE_RUN_MODE` unset → string `"PERIPHERAL SIGNALS"` not present in HTML.
- `test_generate_html_email_no_critical_section`: `"CRITICAL DISRUPTIONS"` not present in HTML; legacy `sentiment_score=2` rows still appear, under their commercial segment, with a `CRITICAL` badge in the meta strip.
- `test_generate_html_email_qa_debug_section_in_test_mode`: `MARKET_PULSE_RUN_MODE=test` → QA section with screened/surfaced counts and at least one reason line is present.
- `test_generate_html_email_qa_debug_section_absent_in_production`: production HTML does not contain "QA · Suppression Summary".
- `test_generate_html_email_renders_executive_bullets`: when `executive_bullets` is well-formed, the 3-bullet layout appears; legacy paragraph does not.
- `test_generate_html_email_falls_back_to_legacy_executive_summary`: when `executive_bullets` is NULL or malformed, the legacy paragraph appears.
- `test_generate_html_email_header_shows_surfaced_and_screened`: header contains `"6 surfaced signals from 87 screened items"`.
- `test_generate_html_email_header_shows_dominant_condition`: header contains the condition string from the summary row.

### 11.5 Card meta strip

- `test_render_card_meta_strip_new_style`: `Impact: 8/10 · Negative · Signal: Supply Chain` rendered as a single line.
- `test_render_card_meta_strip_legacy_omits_signal_clause`: legacy row (no `signal_type`) renders `Impact: 8/10 · Negative` without trailing `· Signal: …`.
- `test_render_card_meta_strip_critical_badge_for_legacy_low_score`: legacy `sentiment_score=2` row gets `· CRITICAL` appended.

### 11.6 Update existing tests

- Tests that assert `"Critical Disruptions"` or `"Thematic Intelligence"` section labels need updating to `"Commercial Segment Watch"` and the dropped-section assertions above. The two "Critical headline" and "Peripheral headline" routing tests are rewritten to reflect the new architecture: critical legacy → segment watch with badge; peripheral 4–5 → QA section (test mode) or hidden (production).

---

## 12. Migration plan

1. **Apply migration 002** via the Supabase SQL editor. Idempotent; safe to apply before code deploys.
2. **Update `schema.sql`** so a fresh DB initializes correctly from one file. Should match migration 002's column set.
3. **Deploy ingestion + delivery + config together.** Backward compatibility ensures the first run after deploy (with mostly legacy rows in lookback) still renders.
4. **Run the test workflow** with `run_ingestion=true` and `send_email=true` to validate the new structured fields populate and the QA section renders.
5. **Monitor the first production run** the next weekday. Verify: header counts non-zero, dominant_condition is not "Cautiously Optimistic," all visible articles have a commercial segment heading.

---

## 13. Known limitations

- **The fallback map for legacy rows is low-fidelity** for `Competitive / Customer Signal` and `Regulatory / Sustainability` — they all bucket to `Enterprise / Cross-Segment`. This is acceptable because the lookback window flushes legacy rows within 3 days.
- **Test runs write to the same `daily_summaries` row as production** because the test workflow uses the production Supabase URL. A test run on a weekday morning will overwrite that day's production summary. This is a pre-existing constraint, called out so it's not surprising; addressing it is explicitly out of scope this sprint.
- **`surfaced_count` is delivery-written.** If delivery fails, the count on the row is whatever ingestion wrote (NULL). The header in any backfill / replay tool would render `0 surfaced signals from N screened items`, which is technically correct.
- **`suppression_samples` is capped at 10.** Jason gets a representative slice, not an exhaustive list. If we need exhaustiveness later, that's the trigger for promoting the JSONB blob to a normalized table.

---

## 14. Decisions index

| Decision | Choice | Rationale |
|---|---|---|
| Approach scope | B + small C (schema + prompt + suppression debug) | User-specified; pure delivery mapping can't fix the LLM's confused taxonomy |
| Segment vs. signal | Two separate columns | One field doing two jobs is the root cause of weak grouping |
| Legacy `strategic_segment` | Kept; new rows write NULL | No backfill; lookback window flushes within 72h |
| Critical Disruptions section | Removed; replaced with per-card badge | Avoids empty-section edge cases; matches target structure |
| Peripheral Signals section | Removed from production; reaccounted in QA | Stakeholders don't see noise; Jason still sees rigor |
| Suppression storage | JSONB on `daily_summaries` | QA-only need; new table is over-engineering |
| Dominant condition | 9-value enum, validated in code | Stops "Cautiously Optimistic" defaulting |
| Executive bullets | 3 fixed labels + LLM-written bodies | Predictable structure; labels can't drift |
| `surfaced_count` write | Delivery, via `update()` on the run-date row | Only delivery knows the final visible count |
| Header counts source | `daily_summaries` (screened from ingestion, surfaced from delivery) | Single source of truth per run |
