# Commercial Intelligence Brief — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert Market-Pulse from a filtered article digest into a decision-grade Americhem commercial intelligence brief: split `strategic_segment` into `commercial_segment` + `signal_type`, replace the freeform macro summary with a constrained `dominant_condition` enum + 3-bullet executive structure, add a deterministic delivery-side suppression pass with reason-code accounting, and isolate test runs from production via a `run_mode` column on `daily_summaries`.

**Architecture:** Additive Migration 002 (no data backfill). Ingestion-side LLM contract grows two new fields per article and two new structured fields on the macro summary. Delivery applies a deterministic guardrail pass before grouping, then renders by commercial segment with a meta strip showing signal type. Suppression counts and samples land on `daily_summaries` for QA visibility — production hides the QA section entirely, test mode reveals it.

**Tech Stack:** Python 3.10 · Supabase (Postgres + JSONB) · OpenAI `gpt-5.4-nano` (existing model) · pytest · `rapidfuzz` for headline dedupe · GitHub Actions for cron and manual test workflows.

**Spec reference:** [docs/superpowers/specs/2026-05-21-commercial-intelligence-brief-design.md](../specs/2026-05-21-commercial-intelligence-brief-design.md)

---

## File map

| File | Action | Responsibility |
| --- | --- | --- |
| `migrations/002_split_segment_and_structured_summary.sql` | Create | DDL: new columns on `daily_intelligence` and `daily_summaries`; replace single-key unique index with `(run_date, run_mode)`. |
| `schema.sql` | Modify | Mirror migration 002 so a fresh DB initializes from one file. |
| `market_pulse_config.yaml` | Modify | Add `commercial_segments`, `signal_types`, `macro_conditions`, `executive_bullet_labels`, `delivery_suppression` blocks. Keep legacy `strategic_segments` block unchanged. |
| `ingestion_engine.py` | Modify | Split `_build_segment_rule()` into commercial + signal builders; validate new fields in `synthesize_insight()`; rewrite `generate_macro_summary()` for structured output with Low-Signal coercion; record suppression counts/samples; upsert keyed on `(run_date, run_mode)`. |
| `delivery_engine.py` | Modify | New helpers `_run_mode()`, `_commercial_segment_of()`, `_signal_type_of()`, `_apply_delivery_suppression()`, `_group_by_commercial_segment()`, `_render_segment_watch_section()`, `_render_executive_bullets()`, `_render_qa_debug_section()`. Remove `_render_peripheral_section()` from production path. Add null-safe header rendering and `(run_date, run_mode)`-filtered fetch/update. |
| `tests/test_pipeline.py` | Modify | Add ~45 new tests across per-article contract, macro summary, ingestion suppression, delivery suppression, run-mode isolation, rendering, null-safe header, QA debug section. Update legacy tests that assert removed `Critical Disruptions` / `Thematic Intelligence` / `Peripheral Signals` labels. |

**Out-of-scope safety reminders** (from user review, fold into each relevant task):

- Inspect the actual existing `daily_summaries` index/constraint name in Supabase before finalizing the migration `drop index` statement (Task 1).
- `surfaced_count` is computed from the final post-suppression, post-cap visible-card list — not `len(groups)`, not pre-cap rows (Task 11).
- Production QA/debug must be fully hidden unless `MARKET_PULSE_RUN_MODE=test` (Task 14).
- Preserve 24h/72h backward compatibility for legacy rows in every delivery test (Tasks 8, 10, 12).
- Run `pytest tests/` and confirm all tests pass before claiming completion (Task 15).

---

## Task 1: Migration 002 and `schema.sql`

**Files:**

- Create: `migrations/002_split_segment_and_structured_summary.sql`
- Modify: `schema.sql`

- [ ] **Step 1: Verify the current uniqueness constraint name on `daily_summaries.run_date`**

In Supabase SQL editor (or via psql), run:

```sql
select indexname, indexdef
from pg_indexes
where schemaname = 'public' and tablename = 'daily_summaries';
```

Confirm the index name matches `idx_daily_summaries_run_date_unique`. If Supabase or a prior migration created uniqueness as a table constraint or under a different auto-generated name, capture the actual name and use it in Step 2 in place of `idx_daily_summaries_run_date_unique`. If a constraint exists instead of an index, use `alter table daily_summaries drop constraint <name>;` rather than `drop index`.

- [ ] **Step 2: Write the migration file**

Create `migrations/002_split_segment_and_structured_summary.sql`:

```sql
-- Migration 002: Split commercial segment from signal type; add structured macro
-- summary fields; add run_mode isolation; capture suppression counts and samples.
-- Apply via Supabase SQL editor or psql. Safe to run multiple times.

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

-- Replace single-key uniqueness on run_date with a composite unique index on
-- (run_date, run_mode) so test runs never overwrite production summary rows.
-- If the existing uniqueness was created as a constraint rather than an index,
-- substitute the constraint-drop syntax verified in Step 1.
drop index if exists idx_daily_summaries_run_date_unique;

create unique index if not exists idx_daily_summaries_run_date_mode_unique
  on daily_summaries (run_date, run_mode);
```

- [ ] **Step 3: Update `schema.sql` to match the post-migration state**

Modify `schema.sql` so a fresh DB initializes correctly from one file. Three changes:

Add to the `daily_intelligence` create-table block (right after the existing relevance fields, before the closing paren):

```sql
    -- Commercial intelligence brief fields (migration 002)
    commercial_segment text,
    signal_type text,
```

Add to the `daily_summaries` create-table block:

```sql
    run_mode text not null default 'production',
    dominant_condition text,
    executive_bullets jsonb,
    screened_count integer,
    surfaced_count integer,
    suppression_breakdown jsonb,
    suppression_samples jsonb
```

Replace the existing block:

```sql
create unique index if not exists idx_daily_summaries_run_date_unique
    on daily_summaries (run_date);
```

with:

```sql
create unique index if not exists idx_daily_summaries_run_date_mode_unique
    on daily_summaries (run_date, run_mode);

create index if not exists idx_daily_intelligence_commercial_segment
    on daily_intelligence (commercial_segment);

create index if not exists idx_daily_intelligence_signal_type
    on daily_intelligence (signal_type);
```

- [ ] **Step 4: Apply the migration in Supabase**

In the Supabase SQL editor, paste the contents of `migrations/002_split_segment_and_structured_summary.sql` and execute. Then re-run the verification query from Step 1 to confirm the new composite unique index exists and the old single-key one is gone.

- [ ] **Step 5: Commit**

```bash
git add migrations/002_split_segment_and_structured_summary.sql schema.sql
git commit -m "feat(db): migration 002 — commercial_segment, signal_type, run_mode, structured macro summary, suppression accounting"
```

---

## Task 2: Add config blocks to `market_pulse_config.yaml`

**Files:**

- Modify: `market_pulse_config.yaml`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_pipeline.py`:

```python
import yaml

def test_config_has_commercial_segments_and_signal_types():
    """market_pulse_config.yaml must expose the new commercial_segments,
    signal_types, macro_conditions, executive_bullet_labels, and
    delivery_suppression blocks with the expected labels."""
    with open("market_pulse_config.yaml") as fh:
        cfg = yaml.safe_load(fh)

    segments = {s["label"] for s in cfg["commercial_segments"].values()}
    assert segments == {
        "Healthcare", "Fibers",
        "Transportation - Automotive", "Transportation - Non-Automotive",
        "Transportation - Aerospace",
        "Industrial", "Packaging", "Engineered Resins",
        "Enterprise / Cross-Segment",
    }

    signals = {s["label"] for s in cfg["signal_types"].values()}
    assert signals == {
        "Competitive", "Customer", "Regulatory", "Sustainability",
        "Supply Chain", "Technology", "Macro", "Other",
    }

    assert cfg["macro_conditions"] == [
        "Competitive Pressure", "Supply Volatility", "Demand Expansion",
        "Demand Softness", "Regulatory Pressure", "Sustainability Pull",
        "Commercial Opportunity", "Mixed / Watch", "Low Signal",
    ]

    assert cfg["executive_bullet_labels"] == [
        "Market pressure", "Supply chain watch", "Commercial action",
    ]

    sup = cfg["delivery_suppression"]
    assert sup["enable_duplicate_headline"] is True
    assert sup["headline_duplicate_threshold"] == 90
    assert sup["enterprise_min_impact"] == 7
    assert "linkedin.com/jobs" in sup["url_patterns_job_posting"]
    assert "market size" in sup["title_patterns_generic_market_report"]
    assert "masterbatch" in sup["plastics_relevance_terms"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/test_pipeline.py::test_config_has_commercial_segments_and_signal_types -v`
Expected: FAIL with `KeyError: 'commercial_segments'`.

- [ ] **Step 3: Append the new YAML blocks**

Append to `market_pulse_config.yaml` (keep the existing `strategic_segments:` block in place — do not remove it):

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
      Off-highway and heavy vehicles: Class 4-8 trucks, buses, agriculture
      equipment, construction equipment, rail, marine, recreational vehicles,
      motorcycles. Excludes passenger cars (-> Automotive) and aerospace
      (-> Aerospace).

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
      Direct competitor moves - pricing, capacity, product launches, M&A activity,
      executive hires, plant openings/closures by named competitors (Avient,
      Techmer PM, Ampacet, RTP, Penn Color, KRAIBURG, etc.).
  customer:
    label: "Customer"
    description: >
      Americhem customer or named brand-owner signals - expansions, contractions,
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
  headline_duplicate_threshold: 90
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

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/test_pipeline.py::test_config_has_commercial_segments_and_signal_types -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add market_pulse_config.yaml tests/test_pipeline.py
git commit -m "feat(config): add commercial_segments, signal_types, macro_conditions, delivery_suppression"
```

---

## Task 3: Split RULE 4 — commercial segment + signal type prompt builders

**Files:**

- Modify: `ingestion_engine.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py`:

```python
def test_build_commercial_segment_rule_injects_labels_and_descriptions():
    """_build_commercial_segment_rule must include all 9 labels and their full
    descriptions from config."""
    from ingestion_engine import _build_commercial_segment_rule
    cfg = {
        "commercial_segments": {
            "healthcare": {"label": "Healthcare", "description": "Med devices."},
            "fibers": {"label": "Fibers", "description": "Synthetic fiber chains."},
        }
    }
    rule_text = _build_commercial_segment_rule(cfg)
    assert "RULE 4 — COMMERCIAL SEGMENT" in rule_text
    assert "Healthcare" in rule_text
    assert "Med devices." in rule_text
    assert "Fibers" in rule_text
    assert "Synthetic fiber chains." in rule_text


def test_build_signal_type_rule_injects_labels_and_descriptions():
    """_build_signal_type_rule must include all 8 labels and descriptions."""
    from ingestion_engine import _build_signal_type_rule
    cfg = {
        "signal_types": {
            "competitive": {"label": "Competitive", "description": "Comp moves."},
            "regulatory": {"label": "Regulatory", "description": "Gov actions."},
        }
    }
    rule_text = _build_signal_type_rule(cfg)
    assert "RULE 5 — SIGNAL TYPE" in rule_text
    assert "Competitive" in rule_text
    assert "Comp moves." in rule_text
    assert "Regulatory" in rule_text
    assert "Gov actions." in rule_text


def test_system_prompt_includes_both_segment_and_signal_rules():
    """The assembled system prompt must contain both new rules with their
    descriptions, not just the labels."""
    from ingestion_engine import _build_system_prompt
    cfg = {
        "commercial_segments": {
            "engineered_resins": {
                "label": "Engineered Resins",
                "description": "High-performance compounds.",
            },
        },
        "signal_types": {
            "supply_chain": {
                "label": "Supply Chain",
                "description": "Resin pricing, force majeure.",
            },
        },
    }
    prompt = _build_system_prompt(cfg)
    assert "RULE 4 — COMMERCIAL SEGMENT" in prompt
    assert "RULE 5 — SIGNAL TYPE" in prompt
    assert "Engineered Resins" in prompt
    assert "High-performance compounds." in prompt
    assert "Supply Chain" in prompt
    assert "Resin pricing, force majeure." in prompt
    assert "seven rules" in prompt
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py::test_build_commercial_segment_rule_injects_labels_and_descriptions tests/test_pipeline.py::test_build_signal_type_rule_injects_labels_and_descriptions tests/test_pipeline.py::test_system_prompt_includes_both_segment_and_signal_rules -v`
Expected: FAIL — `_build_commercial_segment_rule` / `_build_signal_type_rule` don't exist.

- [ ] **Step 3: Replace the segment-rule builder with two builders**

In `ingestion_engine.py`, delete `_build_segment_rule()` (lines 47-69) and the `_FALLBACK_SEGMENT_LIST` constant (lines 28-31). Replace with:

```python
_FALLBACK_COMMERCIAL_SEGMENT_LIST = (
    "Healthcare | Fibers | Transportation - Automotive | "
    "Transportation - Non-Automotive | Transportation - Aerospace | "
    "Industrial | Packaging | Engineered Resins | Enterprise / Cross-Segment"
)

_FALLBACK_SIGNAL_TYPE_LIST = (
    "Competitive | Customer | Regulatory | Sustainability | "
    "Supply Chain | Technology | Macro | Other"
)


def _build_commercial_segment_rule(config: dict) -> str:
    """Return RULE 4 text with commercial segment labels and descriptions from config."""
    segments = config.get("commercial_segments") or {}
    if not segments:
        block = _FALLBACK_COMMERCIAL_SEGMENT_LIST
    else:
        lines = []
        for seg in segments.values():
            if not isinstance(seg, dict):
                continue
            label = seg.get("label", "")
            desc = (seg.get("description") or "").strip().replace("\n", " ")
            if label:
                lines.append(f"  {label}: {desc}" if desc else f"  {label}")
        block = "\n".join(lines) if lines else _FALLBACK_COMMERCIAL_SEGMENT_LIST

    return f"""RULE 4 — COMMERCIAL SEGMENT:
Assign the single best-fit commercial segment for the affected end-market:

{block}

Choose "Enterprise / Cross-Segment" only when the article spans multiple segments
or addresses Americhem-wide topics with no single end-market dominating."""


def _build_signal_type_rule(config: dict) -> str:
    """Return RULE 5 text with signal type labels and descriptions from config."""
    signals = config.get("signal_types") or {}
    if not signals:
        block = _FALLBACK_SIGNAL_TYPE_LIST
    else:
        lines = []
        for sig in signals.values():
            if not isinstance(sig, dict):
                continue
            label = sig.get("label", "")
            desc = (sig.get("description") or "").strip().replace("\n", " ")
            if label:
                lines.append(f"  {label}: {desc}" if desc else f"  {label}")
        block = "\n".join(lines) if lines else _FALLBACK_SIGNAL_TYPE_LIST

    return f"""RULE 5 — SIGNAL TYPE:
Assign the single kind of signal this article represents:

{block}

Prefer a named type over "Other" whenever possible."""
```

- [ ] **Step 4: Update `_SYSTEM_PROMPT_BASE` and `_build_system_prompt()`**

Rewrite the system prompt to reflect the renumbered rules. In `ingestion_engine.py`, change:

```python
_SYSTEM_PROMPT_BASE = """You are an expert market intelligence analyst for AmI (Americhem Intelligence),
... [opening]
Your job is to analyze news articles and extract structured intelligence. You MUST enforce all
six rules below before generating any output.
```

to:

```python
_SYSTEM_PROMPT_BASE = """You are an expert market intelligence analyst for AmI (Americhem Intelligence),
... [opening unchanged]
Your job is to analyze news articles and extract structured intelligence. You MUST enforce all
seven rules below before generating any output.
```

Then replace the existing RULE 4 block (the `{rule4}` placeholder line) with `{rule4}\n\n{rule5}`. The current rule numbering elsewhere in the prompt body stays the same: old RULE 5 ("RIGOROUS IMPACT STATEMENT") becomes RULE 6, old RULE 6 ("DOMAIN RELEVANCE FIREWALL") becomes RULE 7. Update those two heading lines from `RULE 5 —` to `RULE 6 —` and `RULE 6 —` to `RULE 7 —`.

In the output JSON schema (within `_SYSTEM_PROMPT_BASE`), replace:

```text
  "strategic_segment": "<exactly one segment label from Rule 4>",
```

with:

```text
  "commercial_segment": "<exactly one label from Rule 4>",
  "signal_type": "<exactly one label from Rule 5>",
```

Then update `_build_system_prompt()`:

```python
def _build_system_prompt(config: dict) -> str:
    """Assemble the full system prompt, injecting commercial segment and signal type taxonomies."""
    rule4 = _build_commercial_segment_rule(config)
    rule5 = _build_signal_type_rule(config)
    return _SYSTEM_PROMPT_BASE.replace("{rule4}", rule4).replace("{rule5}", rule5)
```

- [ ] **Step 5: Run the new tests + the existing segment-prompt test**

Run: `pytest tests/test_pipeline.py -v -k "commercial_segment or signal_type or build_segment_rule or system_prompt"`
Expected: the three new tests PASS. The old `test_build_segment_rule_injects_config_labels_and_descriptions` and `test_build_segment_rule_falls_back_to_defaults_on_empty_config` FAIL (the function no longer exists). The `test_synthesize_insight_includes_config_segments_in_system_prompt` will also fail because it expects the old `strategic_segments` config key.

- [ ] **Step 6: Delete the obsolete tests**

In `tests/test_pipeline.py`, delete:

- `test_build_segment_rule_injects_config_labels_and_descriptions`
- `test_build_segment_rule_falls_back_to_defaults_on_empty_config`
- `test_synthesize_insight_includes_config_segments_in_system_prompt`

The replacement coverage lives in the three tests added in Step 1.

Also delete the now-unused import line at the top of the section: `from ingestion_engine import _build_system_prompt, _build_segment_rule` — keep only `_build_system_prompt` and add a separate import line for the new functions when needed.

- [ ] **Step 7: Re-run the full test file**

Run: `pytest tests/test_pipeline.py -v`
Expected: all tests PASS except those that depend on per-article validation of the new fields (those come in Task 4).

- [ ] **Step 8: Commit**

```bash
git add ingestion_engine.py tests/test_pipeline.py
git commit -m "feat(ingestion): split strategic_segment prompt into commercial_segment (RULE 4) and signal_type (RULE 5)"
```

---

## Task 4: Validate `commercial_segment` and `signal_type` in `synthesize_insight()`

**Files:**

- Modify: `ingestion_engine.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py`:

```python
def _make_openai_mock_with_new_fields(**overrides) -> MagicMock:
    """OpenAI mock that returns the new-style per-article payload."""
    base = {
        "headline": "Test Headline",
        "americhem_impact": "Direct effect on compounding margin.",
        "sentiment_score": 5,
        "sentiment_tag": "Neutral",
        "americhem_impact_score": 7,
        "impact_rationale": "Direct feedstock cost effect.",
        "commercial_segment": "Healthcare",
        "signal_type": "Technology",
        "source_url": "https://news.com/article",
        "entities_mentioned": ["Avient"],
    }
    base.update(overrides)
    msg = MagicMock(); msg.content = json.dumps(base)
    choice = MagicMock(); choice.message = msg
    completion = MagicMock(); completion.choices = [choice]
    client = MagicMock()
    client.chat.completions.create.return_value = completion
    return client


@pytest.mark.parametrize(
    "valid_segment",
    [
        "Healthcare", "Fibers",
        "Transportation - Automotive", "Transportation - Non-Automotive",
        "Transportation - Aerospace",
        "Industrial", "Packaging", "Engineered Resins",
        "Enterprise / Cross-Segment",
    ],
)
def test_synthesize_insight_preserves_valid_commercial_segment(valid_segment):
    mock = _make_openai_mock_with_new_fields(commercial_segment=valid_segment)
    with patch("ingestion_engine._get_openai", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert result["commercial_segment"] == valid_segment


@pytest.mark.parametrize("bad_segment", [None, "", "  ", "NotASegment", 42])
def test_synthesize_insight_defaults_invalid_commercial_segment(bad_segment):
    mock = _make_openai_mock_with_new_fields(commercial_segment=bad_segment)
    with patch("ingestion_engine._get_openai", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert result["commercial_segment"] == "Enterprise / Cross-Segment"


@pytest.mark.parametrize(
    "valid_signal",
    ["Competitive", "Customer", "Regulatory", "Sustainability",
     "Supply Chain", "Technology", "Macro", "Other"],
)
def test_synthesize_insight_preserves_valid_signal_type(valid_signal):
    mock = _make_openai_mock_with_new_fields(signal_type=valid_signal)
    with patch("ingestion_engine._get_openai", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert result["signal_type"] == valid_signal


@pytest.mark.parametrize("bad_signal", [None, "", "BAD", 42])
def test_synthesize_insight_defaults_invalid_signal_type(bad_signal):
    mock = _make_openai_mock_with_new_fields(signal_type=bad_signal)
    with patch("ingestion_engine._get_openai", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert result["signal_type"] == "Other"


def test_synthesize_insight_drops_strategic_segment_field():
    """If the LLM still returns strategic_segment, it must not appear in the result."""
    mock = _make_openai_mock_with_new_fields(strategic_segment="LegacyValue")
    with patch("ingestion_engine._get_openai", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert "strategic_segment" not in result
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k "commercial_segment or signal_type or strategic_segment"`
Expected: FAIL — the validation logic doesn't exist yet; current code still treats `strategic_segment`.

- [ ] **Step 3: Add validation constants and rewrite the relevant block in `synthesize_insight()`**

In `ingestion_engine.py`, near the other `_VALID_*` constants, add:

```python
_VALID_COMMERCIAL_SEGMENTS: frozenset[str] = frozenset({
    "Healthcare", "Fibers",
    "Transportation - Automotive", "Transportation - Non-Automotive",
    "Transportation - Aerospace",
    "Industrial", "Packaging", "Engineered Resins",
    "Enterprise / Cross-Segment",
})

_VALID_SIGNAL_TYPES: frozenset[str] = frozenset({
    "Competitive", "Customer", "Regulatory", "Sustainability",
    "Supply Chain", "Technology", "Macro", "Other",
})
```

In `synthesize_insight()`, locate the existing strategic_segment validation:

```python
    if not (insight.get("strategic_segment") or "").strip():
        insight["strategic_segment"] = "Broader Americhem"
```

Replace with:

```python
    # commercial_segment validation (RULE 4)
    seg = (insight.get("commercial_segment") or "").strip()
    if seg not in _VALID_COMMERCIAL_SEGMENTS:
        insight["commercial_segment"] = "Enterprise / Cross-Segment"
    else:
        insight["commercial_segment"] = seg

    # signal_type validation (RULE 5)
    sig = (insight.get("signal_type") or "").strip() if isinstance(insight.get("signal_type"), str) else ""
    if sig not in _VALID_SIGNAL_TYPES:
        insight["signal_type"] = "Other"
    else:
        insight["signal_type"] = sig

    # Drop legacy strategic_segment if the LLM still returns it.
    insight.pop("strategic_segment", None)
```

- [ ] **Step 4: Update `execute_pipeline()` payload construction**

In `ingestion_engine.py`, locate the existing `payload = {...}` dict inside `execute_pipeline()`. Replace the `"strategic_segment": insight.get("strategic_segment", "Broader Americhem"),` line with:

```python
                "commercial_segment": insight.get("commercial_segment", "Enterprise / Cross-Segment"),
                "signal_type": insight.get("signal_type", "Other"),
```

(Remove the `"strategic_segment"` key entirely — new rows leave that column NULL per the spec.)

- [ ] **Step 5: Run the new tests**

Run: `pytest tests/test_pipeline.py -v -k "commercial_segment or signal_type or strategic_segment"`
Expected: all PASS. Some existing tests that assert `strategic_segment` validation will now fail — those are addressed below.

- [ ] **Step 6: Delete obsolete legacy-segment tests**

In `tests/test_pipeline.py`, delete:

- `test_strategic_segment_default` (parametrized)
- `test_strategic_segment_preserved_when_valid`

Their coverage is replaced by the new tests in Step 1.

- [ ] **Step 7: Run the test file**

Run: `pytest tests/test_pipeline.py -v`
Expected: tests for ingestion validation PASS. Delivery tests that still reference `strategic_segment` may pass via the legacy fallback (Task 8) but some integration tests in Task 11/15 will fail until the rest of delivery is updated.

- [ ] **Step 8: Commit**

```bash
git add ingestion_engine.py tests/test_pipeline.py
git commit -m "feat(ingestion): validate commercial_segment and signal_type; drop strategic_segment from payload"
```

---

## Task 5: Structured macro summary — `dominant_condition` + `executive_bullets` + Low-Signal coercion

**Files:**

- Modify: `ingestion_engine.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py`:

```python
def _make_macro_mock(payload: dict) -> MagicMock:
    msg = MagicMock(); msg.content = json.dumps(payload)
    choice = MagicMock(); choice.message = msg
    completion = MagicMock(); completion.choices = [choice]
    client = MagicMock(); client.chat.completions.create.return_value = completion
    return client


def _make_articles(n: int) -> list[dict]:
    return [
        {"category": "competitors", "headline": f"H{i}",
         "sentiment_score": 5, "americhem_impact": f"Impact {i}."}
        for i in range(n)
    ]


def _capture_upsert(mock_supabase) -> dict:
    """Return the dict that was passed to .upsert()."""
    return mock_supabase.table.return_value.upsert.call_args[0][0]


def test_generate_macro_summary_writes_dominant_condition_when_valid():
    payload = {
        "dominant_condition": "Competitive Pressure",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "Body A."},
            {"label": "Supply chain watch", "body": "Body B."},
            {"label": "Commercial action",  "body": "Body C."},
        ],
    }
    mock_supa = MagicMock()
    with patch("ingestion_engine._get_openai", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._get_supabase", return_value=mock_supa):
        assert generate_macro_summary(_make_articles(5)) is True
    row = _capture_upsert(mock_supa)
    assert row["dominant_condition"] == "Competitive Pressure"
    assert row["executive_bullets"] == payload["executive_bullets"]
    # Legacy fields still populated for backward compat:
    assert row["macro_sentiment"] == "Competitive Pressure"
    assert row["executive_summary"]  # joined paragraph


def test_generate_macro_summary_coerces_invalid_dominant_condition():
    payload = {
        "dominant_condition": "NonExistentCondition",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
    }
    mock_supa = MagicMock()
    with patch("ingestion_engine._get_openai", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._get_supabase", return_value=mock_supa):
        generate_macro_summary(_make_articles(5))
    row = _capture_upsert(mock_supa)
    assert row["dominant_condition"] == "Mixed / Watch"


def test_generate_macro_summary_defaults_low_signal_when_few_articles():
    """When fewer than 3 articles are passed in and the LLM omits a valid condition,
    default to Low Signal."""
    payload = {"executive_bullets": [
        {"label": "Market pressure",    "body": "Quiet day."},
        {"label": "Supply chain watch", "body": "Quiet day."},
        {"label": "Commercial action",  "body": "Anything."},
    ]}
    mock_supa = MagicMock()
    with patch("ingestion_engine._get_openai", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._get_supabase", return_value=mock_supa):
        generate_macro_summary(_make_articles(2))
    row = _capture_upsert(mock_supa)
    assert row["dominant_condition"] == "Low Signal"


def test_generate_macro_summary_low_signal_coerces_action_body():
    payload = {
        "dominant_condition": "Low Signal",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "Quiet day."},
            {"label": "Supply chain watch", "body": "Quiet day."},
            {"label": "Commercial action",  "body": "Sales should call every customer."},
        ],
    }
    mock_supa = MagicMock()
    with patch("ingestion_engine._get_openai", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._get_supabase", return_value=mock_supa):
        generate_macro_summary(_make_articles(2))
    row = _capture_upsert(mock_supa)
    assert row["executive_bullets"][2]["body"] == "No action required."


@pytest.mark.parametrize("bad_bullets", [
    None,                                              # missing key
    [],                                                # wrong count
    [{"label": "Market pressure", "body": "A."}],      # wrong count
    [{"label": "X", "body": "A."},                     # wrong labels
     {"label": "Supply chain watch", "body": "B."},
     {"label": "Commercial action", "body": "C."}],
    [{"label": "Market pressure", "body": "A."},       # wrong order
     {"label": "Commercial action", "body": "B."},
     {"label": "Supply chain watch", "body": "C."}],
    [{"body": "A."},                                   # missing key
     {"label": "Supply chain watch", "body": "B."},
     {"label": "Commercial action", "body": "C."}],
    "not a list",                                      # wrong type
])
def test_generate_macro_summary_invalid_bullets_set_null(bad_bullets):
    payload = {"dominant_condition": "Mixed / Watch", "executive_bullets": bad_bullets}
    mock_supa = MagicMock()
    with patch("ingestion_engine._get_openai", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._get_supabase", return_value=mock_supa):
        generate_macro_summary(_make_articles(5))
    row = _capture_upsert(mock_supa)
    assert row["executive_bullets"] is None
    # Legacy executive_summary still populated so delivery has a fallback:
    assert row["executive_summary"]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k "macro_summary and (dominant_condition or executive_bullets or low_signal or invalid_bullets)"`
Expected: FAIL — the new structured logic isn't implemented yet.

- [ ] **Step 3: Add validation constants**

In `ingestion_engine.py`, near the other constants:

```python
_VALID_MACRO_CONDITIONS: frozenset[str] = frozenset({
    "Competitive Pressure", "Supply Volatility", "Demand Expansion",
    "Demand Softness", "Regulatory Pressure", "Sustainability Pull",
    "Commercial Opportunity", "Mixed / Watch", "Low Signal",
})

_EXEC_BULLET_LABELS: tuple[str, ...] = (
    "Market pressure", "Supply chain watch", "Commercial action",
)
```

- [ ] **Step 4: Rewrite `generate_macro_summary()`**

Replace the existing `generate_macro_summary()` body with:

```python
def generate_macro_summary(articles: list[dict]) -> bool:
    """Generate a structured macro summary from today's stored articles.

    Writes dominant_condition (constrained enum) and executive_bullets (3-bullet
    JSON) to daily_summaries. Also populates legacy executive_summary and
    macro_sentiment columns for backward compatibility.
    """
    if not articles:
        logger.warning("No articles to summarize — skipping macro summary generation.")
        return False

    client = _get_openai()

    article_digest = "\n".join(
        f"- [{a.get('category', '').upper()}] {a.get('headline', '')} "
        f"(Impact {a.get('americhem_impact_score', a.get('sentiment_score', ''))}/10): "
        f"{a.get('americhem_impact', '')}"
        for a in articles
    )

    macro_conditions_text = ", ".join(sorted(_VALID_MACRO_CONDITIONS))
    label_a, label_b, label_c = _EXEC_BULLET_LABELS

    system_prompt = (
        "You are a senior Americhem commercial intelligence analyst writing the morning brief\n"
        "for GMMs and Sales leaders. Output ONLY a JSON object with two keys.\n\n"
        "1. dominant_condition — pick exactly one value from this list that best describes\n"
        "   today's overall commercial weather across the digest:\n"
        f"     {macro_conditions_text}\n\n"
        "2. executive_bullets — exactly three objects, in this order, with these exact labels:\n"
        f'     {{"label": "{label_a}",    "body": "<one sentence, <=30 words>"}}\n'
        f'     {{"label": "{label_b}", "body": "<one sentence, <=30 words>"}}\n'
        f'     {{"label": "{label_c}",  "body": "<one sentence, <=30 words>"}}\n\n'
        '   Each body must reference specific named entities or segments from the digest.\n'
        '   Do NOT hedge ("may", "could", "potentially") without a specific data point.\n'
        '   Do NOT write generic statements ("monitor closely", "remain vigilant").\n\n'
        '   Low-signal special case:\n'
        '   If dominant_condition is "Low Signal", the Commercial action body MUST be the\n'
        '   literal string "No action required." The other two bullets MUST describe the\n'
        '   absence of meaningful signal.'
    )

    user_prompt = (
        f"Today's market intelligence digest for Americhem ({len(articles)} articles):\n\n"
        f"{article_digest}\n\nOutput ONLY the JSON object."
    )

    try:
        completion = client.chat.completions.create(
            model=OPENAI_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
        )
        content = completion.choices[0].message.content
        if content is None:
            raise ValueError("OpenAI returned empty content for macro summary")
        parsed = json.loads(content)
    except Exception as exc:
        logger.error("Failed to generate macro summary from OpenAI: %s", exc)
        return False

    # Validate dominant_condition.
    cond_raw = parsed.get("dominant_condition")
    if cond_raw not in _VALID_MACRO_CONDITIONS:
        cond = "Low Signal" if len(articles) < 3 else "Mixed / Watch"
    else:
        cond = cond_raw

    # Validate executive_bullets.
    bullets_raw = parsed.get("executive_bullets")
    bullets = _validate_executive_bullets(bullets_raw)

    # Low Signal: force the third bullet body.
    if bullets is not None and cond == "Low Signal":
        bullets[2] = {"label": _EXEC_BULLET_LABELS[2], "body": "No action required."}

    # Build legacy fields for backward compat.
    if bullets is not None:
        executive_summary = " ".join(f"{b['label']}: {b['body']}" for b in bullets)
    else:
        executive_summary = "Macro summary unavailable today."

    try:
        from datetime import date
        supabase = _get_supabase()
        supabase.table("daily_summaries").upsert(
            {
                "run_date": date.today().isoformat(),
                "run_mode": _run_mode(),
                "dominant_condition": cond,
                "executive_bullets": bullets,
                "executive_summary": executive_summary,
                "macro_sentiment": cond,
            },
            on_conflict="run_date,run_mode",
        ).execute()
        logger.info("Macro summary upserted — condition: %s", cond)
        return True
    except Exception as exc:
        logger.error("Failed to upsert macro summary to Supabase: %s", exc)
        return False


def _validate_executive_bullets(raw) -> Optional[list[dict]]:
    """Return the bullets list if valid; None otherwise (delivery falls back to prose)."""
    if not isinstance(raw, list) or len(raw) != 3:
        return None
    expected_labels = _EXEC_BULLET_LABELS
    cleaned: list[dict] = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            return None
        label = item.get("label")
        body = item.get("body")
        if label != expected_labels[i]:
            return None
        if not isinstance(body, str) or not body.strip():
            return None
        cleaned.append({"label": label, "body": body.strip()})
    return cleaned
```

Also add a `_run_mode()` helper near the top of `ingestion_engine.py`:

```python
def _run_mode() -> str:
    """Return 'test' when MARKET_PULSE_RUN_MODE=test (case-insensitive), else 'production'."""
    return "test" if os.environ.get("MARKET_PULSE_RUN_MODE", "").strip().lower() == "test" else "production"
```

- [ ] **Step 5: Delete obsolete tests for the old macro summary contract**

In `tests/test_pipeline.py`, delete `test_generate_macro_summary_success` (it asserts the freeform `executive_summary` and `macro_sentiment` text — the new tests in Step 1 cover both the new structured fields and the legacy backfill).

Keep `test_generate_macro_summary_empty_articles` and `test_generate_macro_summary_uses_gpt_5_4_nano` — they still apply.

- [ ] **Step 6: Run all macro summary tests**

Run: `pytest tests/test_pipeline.py -v -k "macro_summary"`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add ingestion_engine.py tests/test_pipeline.py
git commit -m "feat(ingestion): structured macro summary with dominant_condition enum and executive_bullets"
```

---

## Task 6: Ingestion-side suppression accounting

**Files:**

- Modify: `ingestion_engine.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py`:

```python
def test_generate_macro_summary_persists_suppression_breakdown_and_samples():
    """generate_macro_summary must accept counts and samples and persist them."""
    counts = {"duplicate_url": 3, "llm_discard": 2}
    samples = [
        {"reason": "llm_discard", "url": "https://x.com/1", "title": "Bad article"},
    ]
    mock_supa = MagicMock()
    payload = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
    }
    with patch("ingestion_engine._get_openai", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._get_supabase", return_value=mock_supa):
        generate_macro_summary(
            _make_articles(5),
            screened_count=87,
            suppression_breakdown=counts,
            suppression_samples=samples,
        )
    row = _capture_upsert(mock_supa)
    assert row["screened_count"] == 87
    assert row["suppression_breakdown"] == counts
    assert row["suppression_samples"] == samples


def test_record_suppression_caps_samples_at_10_fifo():
    """The suppression samples buffer must cap at 10 items, keeping the most recent."""
    from ingestion_engine import _record_suppression

    counts: dict = {}
    samples: list = []
    for i in range(15):
        _record_suppression(
            counts, samples,
            reason="duplicate_url",
            url=f"https://x.com/{i}",
            title=f"Title {i}",
        )
    assert counts["duplicate_url"] == 15
    assert len(samples) == 10
    # Most recent 10 (5..14) should be retained.
    assert samples[0]["title"] == "Title 5"
    assert samples[-1]["title"] == "Title 14"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k "suppression_breakdown or record_suppression"`
Expected: FAIL — `_record_suppression` does not exist; `generate_macro_summary` doesn't accept the new kwargs.

- [ ] **Step 3: Add the `_record_suppression` helper**

In `ingestion_engine.py`, near the top of the file (alongside other helpers):

```python
_SUPPRESSION_SAMPLES_CAP = 10


def _record_suppression(
    counts: dict,
    samples: list,
    *,
    reason: str,
    url: str,
    title: str,
) -> None:
    """Increment the counter for `reason` and append a sample (FIFO cap = 10)."""
    counts[reason] = counts.get(reason, 0) + 1
    samples.append({"reason": reason, "url": url, "title": title})
    if len(samples) > _SUPPRESSION_SAMPLES_CAP:
        # Drop the oldest item so the buffer holds the most-recent 10.
        del samples[0]
```

- [ ] **Step 4: Extend `generate_macro_summary()` signature and persistence**

Modify the function signature:

```python
def generate_macro_summary(
    articles: list[dict],
    *,
    screened_count: Optional[int] = None,
    suppression_breakdown: Optional[dict] = None,
    suppression_samples: Optional[list] = None,
) -> bool:
```

In the upsert call, include the new fields:

```python
        supabase.table("daily_summaries").upsert(
            {
                "run_date": date.today().isoformat(),
                "run_mode": _run_mode(),
                "dominant_condition": cond,
                "executive_bullets": bullets,
                "executive_summary": executive_summary,
                "macro_sentiment": cond,
                "screened_count": screened_count,
                "suppression_breakdown": suppression_breakdown or {},
                "suppression_samples": suppression_samples or [],
            },
            on_conflict="run_date,run_mode",
        ).execute()
```

- [ ] **Step 5: Wire `_record_suppression` into `execute_pipeline()`**

In `ingestion_engine.py` `execute_pipeline()`, initialise a samples buffer alongside `stats`:

```python
    stored_articles_buffer: list[dict] = []
    suppression_samples: list[dict] = []
```

Then at each suppression call site, replace bare `stats[...] += 1` lines with `_record_suppression` calls. Specifically:

`url_already_processed()` True branch:

```python
            if url_already_processed(url_hash):
                logger.info("Duplicate — skipping: %s", normalized)
                _record_suppression(
                    stats, suppression_samples,
                    reason="duplicate_url", url=raw_url, title=serper_title,
                )
                continue
```

(Note: the existing `stats["urls_skipped_duplicate"]` key changes to `stats["duplicate_url"]`. Either change the key name or keep both — for clarity, **replace** the legacy stat-key names with the new reason codes so `stats` becomes the same dict you pass into `generate_macro_summary()` as `suppression_breakdown`.)

The mapping from current keys to reason codes:

- `urls_skipped_duplicate` → `duplicate_url`
- `urls_skipped_semantic_duplicate` → `semantic_duplicate`
- `urls_skipped_discard` → `llm_discard`
- `urls_skipped_too_short` → `scrape_failed`

`is_semantic_duplicate()` True branch:

```python
            is_dup, matched, score = is_semantic_duplicate(serper_title, seen_headlines)
            if is_dup:
                logger.warning("SEMANTIC_DUPLICATE — skipped: '%s' ~ '%s' | score: %d",
                               serper_title, matched, score)
                _record_suppression(
                    stats, suppression_samples,
                    reason="semantic_duplicate", url=raw_url, title=serper_title,
                )
                continue
```

`scrape_article()` is None:

```python
            article_text = scrape_article(raw_url, min_article_length)
            if article_text is None:
                _record_suppression(
                    stats, suppression_samples,
                    reason="scrape_failed", url=raw_url, title=serper_title,
                )
                time.sleep(1.5)
                continue
```

`DISCARD` branch:

```python
            if insight.get("americhem_impact") == "DISCARD":
                logger.info("DISCARD — false positive: %s", normalized)
                _record_suppression(
                    stats, suppression_samples,
                    reason="llm_discard", url=raw_url, title=serper_title,
                )
                time.sleep(1.5)
                continue
```

Initialise `stats` with the reason-code keys (and keep `urls_discovered`, `scrapes_attempted`, `insights_stored`, `errors`):

```python
    stats = {
        "urls_discovered": 0,
        "scrapes_attempted": 0,
        "insights_stored": 0,
        "errors": 0,
        # Reason-code counters (also become daily_summaries.suppression_breakdown)
        "duplicate_url": 0,
        "semantic_duplicate": 0,
        "llm_discard": 0,
        "scrape_failed": 0,
    }
```

Update `_log_stats()` to match the renamed keys:

```python
def _log_stats(stats: dict) -> None:
    logger.info(
        "Pipeline complete — discovered: %d | duplicates skipped: %d | "
        "semantic duplicates: %d | scrape failed: %d | discards: %d | "
        "scrapes attempted: %d | stored: %d | errors: %d",
        stats["urls_discovered"],
        stats.get("duplicate_url", 0),
        stats.get("semantic_duplicate", 0),
        stats.get("scrape_failed", 0),
        stats.get("llm_discard", 0),
        stats["scrapes_attempted"],
        stats["insights_stored"],
        stats["errors"],
    )
```

Finally, pass the new args to `generate_macro_summary()` at both call sites (the three places it is invoked in `execute_pipeline()`):

```python
    generate_macro_summary(
        stored_articles_buffer,
        screened_count=stats["urls_discovered"],
        suppression_breakdown={k: v for k, v in stats.items()
                               if k in {"duplicate_url", "semantic_duplicate",
                                        "llm_discard", "scrape_failed"}},
        suppression_samples=suppression_samples,
    )
```

- [ ] **Step 6: Update an existing test that uses the old stat keys**

The deadline-early-exit test `test_execute_pipeline_deadline_calls_log_stats_and_macro_summary` checks that `_log_stats` and `generate_macro_summary` are called — it should still pass because the call signature change is keyword-only. Verify by running:

Run: `pytest tests/test_pipeline.py::test_execute_pipeline_deadline_calls_log_stats_and_macro_summary -v`
Expected: PASS.

- [ ] **Step 7: Run the suppression-accounting tests**

Run: `pytest tests/test_pipeline.py -v -k "suppression_breakdown or record_suppression"`
Expected: all PASS.

- [ ] **Step 8: Run the full ingestion test slice**

Run: `pytest tests/test_pipeline.py -v -k "ingestion or macro or synthesize or pipeline or scrape"`
Expected: all PASS.

- [ ] **Step 9: Commit**

```bash
git add ingestion_engine.py tests/test_pipeline.py
git commit -m "feat(ingestion): record suppression reasons + samples, persist to daily_summaries"
```

---

## Task 7: Run-mode isolation in `daily_summaries`

**Files:**

- Modify: `delivery_engine.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py`:

```python
def test_fetch_macro_summary_filters_by_run_mode_production(monkeypatch):
    """Production delivery must fetch the production row even when a test row exists."""
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    from delivery_engine import fetch_macro_summary

    mock_supa = MagicMock()
    captured = {}

    def fake_select(*a, **kw):
        captured["select"] = (a, kw)
        return mock_supa.table.return_value

    mock_supa.table.return_value.select.side_effect = fake_select
    mock_supa.table.return_value.eq.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.gte.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.order.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.limit.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.execute.return_value = MagicMock(
        data=[{"run_date": "2026-05-21", "run_mode": "production",
               "executive_summary": "Prod summary", "macro_sentiment": "Stable"}]
    )

    with patch("delivery_engine._get_supabase", return_value=mock_supa):
        result = fetch_macro_summary()

    # eq() must have been called with run_mode='production'.
    eq_calls = mock_supa.table.return_value.eq.call_args_list
    assert any(c.args == ("run_mode", "production") for c in eq_calls), \
        f"Expected eq('run_mode', 'production') in {eq_calls}"
    assert result["executive_summary"] == "Prod summary"


def test_fetch_macro_summary_filters_by_run_mode_test(monkeypatch):
    """Test delivery must fetch the test row."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    from delivery_engine import fetch_macro_summary

    mock_supa = MagicMock()
    mock_supa.table.return_value.eq.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.gte.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.order.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.limit.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.select.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.execute.return_value = MagicMock(data=[])

    with patch("delivery_engine._get_supabase", return_value=mock_supa):
        fetch_macro_summary()

    eq_calls = mock_supa.table.return_value.eq.call_args_list
    assert any(c.args == ("run_mode", "test") for c in eq_calls), \
        f"Expected eq('run_mode', 'test') in {eq_calls}"


def test_run_mode_helper():
    """_run_mode() returns 'test' when env=test; 'production' otherwise; case-insensitive."""
    from delivery_engine import _run_mode
    import os
    os.environ["MARKET_PULSE_RUN_MODE"] = "test"
    assert _run_mode() == "test"
    os.environ["MARKET_PULSE_RUN_MODE"] = "TEST"
    assert _run_mode() == "test"
    os.environ["MARKET_PULSE_RUN_MODE"] = ""
    assert _run_mode() == "production"
    del os.environ["MARKET_PULSE_RUN_MODE"]
    assert _run_mode() == "production"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k "fetch_macro_summary_filters or run_mode_helper"`
Expected: FAIL — `_run_mode` doesn't exist in `delivery_engine`; `fetch_macro_summary` doesn't filter by run_mode.

- [ ] **Step 3: Add `_run_mode()` to `delivery_engine.py`**

In `delivery_engine.py`, near `_is_test_mode()`:

```python
def _run_mode() -> str:
    """Return 'test' when MARKET_PULSE_RUN_MODE=test (case-insensitive), else 'production'."""
    return "test" if os.environ.get("MARKET_PULSE_RUN_MODE", "").strip().lower() == "test" else "production"
```

Keep `_is_test_mode()` as a thin wrapper so existing callers don't break:

```python
def _is_test_mode() -> bool:
    return _run_mode() == "test"
```

- [ ] **Step 4: Update `fetch_macro_summary()` to filter by run_mode**

Replace the current `fetch_macro_summary()` body. Replace:

```python
        result = (
            supabase.table("daily_summaries")
            .select("run_date, executive_summary, macro_sentiment")
            .gte("run_date", min_run_date)
            .order("run_date", desc=True)
            .limit(1)
            .execute()
        )
```

with:

```python
        result = (
            supabase.table("daily_summaries")
            .select(
                "run_date, run_mode, executive_summary, macro_sentiment, "
                "dominant_condition, executive_bullets, screened_count, "
                "surfaced_count, suppression_breakdown, suppression_samples"
            )
            .eq("run_mode", _run_mode())
            .gte("run_date", min_run_date)
            .order("run_date", desc=True)
            .limit(1)
            .execute()
        )
```

- [ ] **Step 5: Run the tests**

Run: `pytest tests/test_pipeline.py -v -k "fetch_macro_summary_filters or run_mode_helper"`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat(delivery): filter macro summary fetch by run_mode; isolate test from production"
```

---

## Task 8: `_commercial_segment_of()` and `_signal_type_of()` helpers

**Files:**

- Modify: `delivery_engine.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py`:

```python
def test_commercial_segment_of_prefers_new_field():
    from delivery_engine import _commercial_segment_of
    row = {"commercial_segment": "Healthcare", "strategic_segment": "Industrial"}
    assert _commercial_segment_of(row) == "Healthcare"


def test_commercial_segment_of_falls_back_to_strategic_segment():
    from delivery_engine import _commercial_segment_of
    cases = {
        "Healthcare": "Healthcare",
        "Fibers": "Fibers",
        "Packaging": "Packaging",
        "Industrial": "Industrial",
        "Raw Materials / Supply Chain": "Enterprise / Cross-Segment",
        "Regulatory / Sustainability": "Enterprise / Cross-Segment",
        "Competitive / Customer Signal": "Enterprise / Cross-Segment",
        "Broader Americhem": "Enterprise / Cross-Segment",
    }
    for legacy, expected in cases.items():
        row = {"strategic_segment": legacy}
        assert _commercial_segment_of(row) == expected, f"{legacy} -> {expected}"


def test_commercial_segment_of_handles_null_strategic_segment():
    from delivery_engine import _commercial_segment_of
    assert _commercial_segment_of({}) == "Enterprise / Cross-Segment"
    assert _commercial_segment_of({"strategic_segment": None}) == "Enterprise / Cross-Segment"
    assert _commercial_segment_of({"strategic_segment": ""}) == "Enterprise / Cross-Segment"
    assert _commercial_segment_of({"strategic_segment": "UnknownValue"}) == "Enterprise / Cross-Segment"


def test_signal_type_of_prefers_new_field():
    from delivery_engine import _signal_type_of
    assert _signal_type_of({"signal_type": "Regulatory"}) == "Regulatory"


def test_signal_type_of_falls_back_to_other():
    from delivery_engine import _signal_type_of
    assert _signal_type_of({}) == "Other"
    assert _signal_type_of({"signal_type": None}) == "Other"
    assert _signal_type_of({"signal_type": ""}) == "Other"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k "commercial_segment_of or signal_type_of"`
Expected: FAIL — functions don't exist.

- [ ] **Step 3: Add the helpers to `delivery_engine.py`**

```python
_LEGACY_STRATEGIC_SEGMENT_MAP: dict[str, str] = {
    "Healthcare":                    "Healthcare",
    "Fibers":                        "Fibers",
    "Packaging":                     "Packaging",
    "Industrial":                    "Industrial",
    "Raw Materials / Supply Chain":  "Enterprise / Cross-Segment",
    "Regulatory / Sustainability":   "Enterprise / Cross-Segment",
    "Competitive / Customer Signal": "Enterprise / Cross-Segment",
    "Broader Americhem":             "Enterprise / Cross-Segment",
}


def _commercial_segment_of(row: dict) -> str:
    """Return commercial_segment if set; else map legacy strategic_segment; else default."""
    seg = (row.get("commercial_segment") or "").strip()
    if seg:
        return seg
    legacy = (row.get("strategic_segment") or "").strip()
    return _LEGACY_STRATEGIC_SEGMENT_MAP.get(legacy, "Enterprise / Cross-Segment")


def _signal_type_of(row: dict) -> str:
    """Return signal_type if set on the row; else 'Other'."""
    sig = (row.get("signal_type") or "").strip()
    return sig if sig else "Other"
```

- [ ] **Step 4: Run the tests**

Run: `pytest tests/test_pipeline.py -v -k "commercial_segment_of or signal_type_of"`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat(delivery): _commercial_segment_of() and _signal_type_of() with legacy fallback"
```

---

## Task 9: `_apply_delivery_suppression()` — seven-rule guardrail pass

**Files:**

- Modify: `delivery_engine.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py`:

```python
def _supp_config(**overrides) -> dict:
    """Default delivery_suppression config for tests; overrides applied on top."""
    base = {
        "enable_duplicate_headline": True,
        "enable_semantic_duplicate_headline": True,
        "headline_duplicate_threshold": 90,
        "enable_product_listing": True,
        "enable_job_posting": True,
        "job_posting_override_action": "Escalate to leadership",
        "enable_generic_market_report": True,
        "enable_unrelated_color_result": True,
        "enable_enterprise_low_impact": True,
        "enterprise_min_impact": 7,
        "url_patterns_product_listing": ["/product/", "amazon.com"],
        "url_patterns_job_posting": ["linkedin.com/jobs", "/careers/"],
        "title_patterns_generic_market_report": ["market size", "market report"],
        "color_terms": ["color", "colour"],
        "plastics_relevance_terms": ["plastic", "polymer", "masterbatch", "colorant"],
    }
    base.update(overrides)
    return {"delivery_suppression": base}


def _row(**overrides) -> dict:
    base = {
        "url_hash": overrides.get("url_hash", "abc"),
        "source_url": "https://example.com/article",
        "headline": "Default Headline",
        "americhem_impact": "Effect.",
        "americhem_impact_score": 8,
        "sentiment_tag": "Neutral",
        "commercial_segment": "Healthcare",
        "signal_type": "Customer",
        "entities_mentioned": ["Acme"],
        "recommended_action": "Monitor",
    }
    base.update(overrides)
    return base


def test_apply_delivery_suppression_drops_enterprise_low_impact():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(commercial_segment="Enterprise / Cross-Segment", americhem_impact_score=5)]
    kept, counts, samples = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert counts == {"enterprise_cross_segment_low_impact": 1}
    assert samples[0]["reason"] == "enterprise_cross_segment_low_impact"


def test_apply_delivery_suppression_keeps_enterprise_high_impact():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(commercial_segment="Enterprise / Cross-Segment", americhem_impact_score=8)]
    kept, counts, _ = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert counts == {}


def test_apply_delivery_suppression_drops_product_listing():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(source_url="https://example.com/product/widget")]
    kept, counts, _ = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert counts == {"product_listing": 1}


def test_apply_delivery_suppression_drops_job_posting():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(source_url="https://www.linkedin.com/jobs/12345")]
    kept, counts, _ = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert counts == {"job_posting": 1}


def test_apply_delivery_suppression_job_posting_escalate_override():
    """A job-posting URL with recommended_action='Escalate to leadership' is kept."""
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(source_url="https://www.linkedin.com/jobs/ceo-move",
                 recommended_action="Escalate to leadership")]
    kept, counts, _ = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert counts == {}


def test_apply_delivery_suppression_drops_generic_market_report_no_entities():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(headline="Global Polypropylene Market Size 2026-2032",
                 entities_mentioned=[])]
    kept, counts, _ = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert counts == {"generic_market_report": 1}


def test_apply_delivery_suppression_keeps_generic_market_report_with_entities():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(headline="Global Polypropylene Market 2026 Report",
                 entities_mentioned=["Avient"])]
    kept, counts, _ = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert counts == {}


def test_apply_delivery_suppression_drops_unrelated_color_result():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(headline="What extension cord colors mean",
                 americhem_impact="No plastics relevance.",
                 entities_mentioned=["DIY Network"])]
    kept, counts, _ = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert counts == {"unrelated_color_result": 1}


def test_apply_delivery_suppression_keeps_color_result_with_plastics_term():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(headline="New masterbatch colors for automotive interiors",
                 americhem_impact="Drives masterbatch demand.",
                 entities_mentioned=["BASF"])]
    kept, counts, _ = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert counts == {}


def test_apply_delivery_suppression_drops_exact_duplicate_headline():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(url_hash="a", headline="Plant fire halts production"),
            _row(url_hash="b", headline="Plant fire halts production")]
    kept, counts, _ = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert kept[0]["url_hash"] == "a"
    assert counts == {"duplicate_headline": 1}


def test_apply_delivery_suppression_drops_semantic_duplicate_headline():
    from delivery_engine import _apply_delivery_suppression
    rows = [
        _row(url_hash="a", headline="Plant fire halts production at BASF site"),
        _row(url_hash="b", headline="BASF plant fire halts production at site"),
    ]
    kept, counts, _ = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert counts == {"semantic_duplicate_headline": 1}


def test_apply_delivery_suppression_first_match_wins():
    """A row matching both product_listing and generic_market_report is counted once,
    under product_listing (which is checked first in the rule order)."""
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(source_url="https://amazon.com/product/123",
                 headline="Plastic Market Report 2026",
                 entities_mentioned=[])]
    kept, counts, _ = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert counts == {"product_listing": 1}  # NOT generic_market_report


def test_apply_delivery_suppression_disabled_rule_allows_through():
    from delivery_engine import _apply_delivery_suppression
    cfg = _supp_config(enable_product_listing=False)
    rows = [_row(source_url="https://example.com/product/widget")]
    kept, counts, _ = _apply_delivery_suppression(rows, cfg)
    assert len(kept) == 1
    assert counts == {}


def test_apply_delivery_suppression_samples_capped_at_10():
    from delivery_engine import _apply_delivery_suppression
    rows = [
        _row(url_hash=f"h{i}", source_url=f"https://amazon.com/product/{i}",
             headline=f"Product {i}")
        for i in range(15)
    ]
    kept, counts, samples = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert counts["product_listing"] == 15
    assert len(samples) == 10
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k "apply_delivery_suppression"`
Expected: FAIL — function doesn't exist.

- [ ] **Step 3: Implement the suppression pass**

Add to `delivery_engine.py`:

```python
from rapidfuzz.fuzz import token_sort_ratio as _token_sort_ratio

_DELIVERY_SAMPLES_CAP = 10


def _matches_any_pattern(haystack: str, patterns: list[str]) -> bool:
    """Case-insensitive substring match: True if any pattern appears in haystack."""
    h = (haystack or "").lower()
    return any(p.lower() in h for p in patterns or [])


def _contains_any_term(text: str, terms: list[str]) -> bool:
    """True if any of `terms` appears in `text` (case-insensitive substring)."""
    t = (text or "").lower()
    return any(term.lower() in t for term in terms or [])


def _apply_delivery_suppression(
    rows: list[dict],
    config: dict,
) -> tuple[list[dict], dict, list[dict]]:
    """Run the deterministic seven-rule guardrail over fetched rows.

    Returns (kept_rows, counts_by_reason, samples_capped_at_10).
    First matching rule wins; a row is counted once.
    """
    sup_cfg = config.get("delivery_suppression") or {}
    counts: dict[str, int] = {}
    samples: list[dict] = []
    kept: list[dict] = []
    kept_headlines: list[str] = []

    threshold = int(sup_cfg.get("headline_duplicate_threshold", 90))
    enterprise_min_impact = int(sup_cfg.get("enterprise_min_impact", 7))
    override_action = sup_cfg.get("job_posting_override_action", "Escalate to leadership")

    product_patterns   = sup_cfg.get("url_patterns_product_listing", [])
    job_patterns       = sup_cfg.get("url_patterns_job_posting", [])
    market_patterns    = sup_cfg.get("title_patterns_generic_market_report", [])
    color_terms        = sup_cfg.get("color_terms", [])
    plastics_terms     = sup_cfg.get("plastics_relevance_terms", [])

    def _suppress(reason: str, row: dict) -> None:
        counts[reason] = counts.get(reason, 0) + 1
        samples.append({
            "reason": reason,
            "url": row.get("source_url", ""),
            "title": row.get("headline", ""),
        })
        if len(samples) > _DELIVERY_SAMPLES_CAP:
            del samples[0]

    for row in rows:
        url = row.get("source_url", "") or ""
        headline = row.get("headline", "") or ""
        americhem_impact = row.get("americhem_impact", "") or ""
        entities = row.get("entities_mentioned") or []
        entities_text = " ".join(str(e) for e in entities)
        action = row.get("recommended_action", "")

        # Rule 1: Enterprise / Cross-Segment with low impact
        if sup_cfg.get("enable_enterprise_low_impact", True):
            segment = _commercial_segment_of(row)
            score = int(row.get("americhem_impact_score") or 0)
            if segment == "Enterprise / Cross-Segment" and score < enterprise_min_impact:
                _suppress("enterprise_cross_segment_low_impact", row)
                continue

        # Rule 2: Product listing URL
        if sup_cfg.get("enable_product_listing", True) and _matches_any_pattern(url, product_patterns):
            _suppress("product_listing", row)
            continue

        # Rule 3: Job posting URL (unless escalated)
        if sup_cfg.get("enable_job_posting", True) and _matches_any_pattern(url, job_patterns):
            if action != override_action:
                _suppress("job_posting", row)
                continue

        # Rule 4: Generic market report title with empty entities
        if sup_cfg.get("enable_generic_market_report", True):
            if _matches_any_pattern(headline, market_patterns) and not entities:
                _suppress("generic_market_report", row)
                continue

        # Rule 5: Unrelated color result
        if sup_cfg.get("enable_unrelated_color_result", True):
            if _contains_any_term(headline, color_terms):
                relevance_haystack = f"{headline} {americhem_impact} {entities_text}"
                if not _contains_any_term(relevance_haystack, plastics_terms):
                    _suppress("unrelated_color_result", row)
                    continue

        # Rule 6: Exact duplicate headline
        if sup_cfg.get("enable_duplicate_headline", True):
            if headline and any(h.lower() == headline.lower() for h in kept_headlines):
                _suppress("duplicate_headline", row)
                continue

        # Rule 7: Semantic duplicate headline
        if sup_cfg.get("enable_semantic_duplicate_headline", True) and kept_headlines and headline:
            scores = [_token_sort_ratio(headline, h) for h in kept_headlines]
            if scores and max(scores) >= threshold:
                _suppress("semantic_duplicate_headline", row)
                continue

        kept.append(row)
        if headline:
            kept_headlines.append(headline)

    return kept, counts, samples
```

- [ ] **Step 4: Run the suppression-pass tests**

Run: `pytest tests/test_pipeline.py -v -k "apply_delivery_suppression"`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat(delivery): deterministic 7-rule suppression guardrail with reason-code accounting"
```

---

## Task 10: `_group_by_commercial_segment` + `_render_segment_watch_section` + meta strip

**Files:**

- Modify: `delivery_engine.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py`:

```python
def test_group_by_commercial_segment_keys_off_new_field():
    from delivery_engine import _group_by_commercial_segment
    rows = [
        {"url_hash": "a", "commercial_segment": "Healthcare",
         "americhem_impact_score": 8, "headline": "A"},
        {"url_hash": "b", "commercial_segment": "Healthcare",
         "americhem_impact_score": 7, "headline": "B"},
        {"url_hash": "c", "commercial_segment": "Packaging",
         "americhem_impact_score": 6, "headline": "C"},
    ]
    groups = _group_by_commercial_segment(rows)
    assert set(groups.keys()) == {"Healthcare", "Packaging"}
    assert len(groups["Healthcare"]) == 2


def test_group_by_commercial_segment_uses_legacy_fallback():
    from delivery_engine import _group_by_commercial_segment
    rows = [
        {"url_hash": "a", "strategic_segment": "Healthcare",
         "americhem_impact_score": 8, "headline": "A"},
        {"url_hash": "b", "strategic_segment": "Competitive / Customer Signal",
         "americhem_impact_score": 7, "headline": "B"},
    ]
    groups = _group_by_commercial_segment(rows)
    assert "Healthcare" in groups
    assert "Enterprise / Cross-Segment" in groups


def test_render_segment_watch_section_displays_meta_strip_with_signal():
    from delivery_engine import _render_segment_watch_section
    groups = {
        "Healthcare": [{
            "url_hash": "a",
            "headline": "Test Card Headline",
            "source_url": "https://news.com/a",
            "americhem_impact": "Direct demand effect.",
            "americhem_impact_score": 8,
            "sentiment_tag": "Positive",
            "signal_type": "Customer",
            "commercial_segment": "Healthcare",
            "recommended_action": "Monitor",
        }],
    }
    html = _render_segment_watch_section(groups, synthesis={})
    assert "HEALTHCARE" in html.upper()
    assert "Test Card Headline" in html
    assert "Impact: 8/10" in html
    assert "Positive" in html
    assert "Signal: Customer" in html
    assert "Direct demand effect." in html


def test_render_segment_watch_section_omits_signal_for_legacy_row():
    from delivery_engine import _render_segment_watch_section
    groups = {
        "Healthcare": [{
            "url_hash": "a",
            "headline": "Legacy Row Headline",
            "source_url": "https://news.com/a",
            "americhem_impact": "Effect.",
            "americhem_impact_score": 7,
            "sentiment_tag": "Neutral",
            "strategic_segment": "Healthcare",
            # no signal_type
        }],
    }
    html = _render_segment_watch_section(groups, synthesis={})
    assert "Impact: 7/10" in html
    assert "Signal:" not in html


def test_render_segment_watch_section_critical_badge_for_legacy_low_score():
    from delivery_engine import _render_segment_watch_section
    groups = {
        "Enterprise / Cross-Segment": [{
            "url_hash": "a",
            "headline": "Critical legacy headline",
            "source_url": "https://news.com/a",
            "americhem_impact": "Effect.",
            "sentiment_score": 2,
            "strategic_segment": "Broader Americhem",
        }],
    }
    html = _render_segment_watch_section(groups, synthesis={})
    assert "CRITICAL" in html


def test_render_segment_watch_section_renders_synthesis_paragraph():
    from delivery_engine import _render_segment_watch_section
    groups = {
        "Packaging": [
            {"url_hash": "a", "headline": "A", "source_url": "https://x/a",
             "americhem_impact": "X.", "americhem_impact_score": 7,
             "sentiment_tag": "Neutral", "signal_type": "Sustainability",
             "commercial_segment": "Packaging"},
            {"url_hash": "b", "headline": "B", "source_url": "https://x/b",
             "americhem_impact": "Y.", "americhem_impact_score": 6,
             "sentiment_tag": "Neutral", "signal_type": "Sustainability",
             "commercial_segment": "Packaging"},
        ]
    }
    synth = {"Packaging": "Brand-owners are shifting toward recycled content."}
    html = _render_segment_watch_section(groups, synth)
    assert "Brand-owners are shifting toward recycled content." in html
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k "group_by_commercial_segment or render_segment_watch"`
Expected: FAIL — functions don't exist yet.

- [ ] **Step 3: Implement grouping**

Add to `delivery_engine.py`:

```python
def _group_by_commercial_segment(items: list[dict]) -> dict[str, list[dict]]:
    """Bucket items by their resolved commercial segment (new field or legacy fallback)."""
    from collections import defaultdict
    buckets: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        buckets[_commercial_segment_of(item)].append(item)
    return dict(buckets)
```

- [ ] **Step 4: Implement the new render function**

Add to `delivery_engine.py`:

```python
def _render_meta_strip(item: dict) -> str:
    """Return the inline meta strip HTML span: 'Impact: X/10 · Tag · Signal: Y · [CRITICAL]'."""
    score = item.get("americhem_impact_score")
    tag = item.get("sentiment_tag") or ""

    if score is not None and tag:
        score_html = (
            f'<span style="color:{_BRAND_NAVY};font-weight:600;">'
            f'Impact: {int(score)}/10</span>'
        )
        tag_color = _SENTIMENT_TAG_COLORS.get(tag, "#6B7280")
        tag_html = (
            f'<span style="color:#9CA3AF;">&nbsp;&#9679;&nbsp;</span>'
            f'<span style="color:{tag_color};font-weight:600;">{tag}</span>'
        )
        signal = (item.get("signal_type") or "").strip()
        signal_html = (
            f'<span style="color:#9CA3AF;">&nbsp;&#9679;&nbsp;Signal: {signal}</span>'
            if signal else ""
        )
    else:
        # Legacy row: use sentiment_score for the score display.
        legacy_score = item.get("sentiment_score") or 5
        score_word, score_color = _sentiment_word(int(legacy_score))
        score_html = (
            f'<span style="color:{score_color};font-weight:600;">{score_word}</span>'
            f'<span style="color:#9CA3AF;">&nbsp;&#9679;&nbsp;Score: {legacy_score}/10</span>'
        )
        tag_html = ""
        signal_html = ""

    # CRITICAL badge for legacy low-sentiment rows.
    critical_html = ""
    if score is None:
        legacy_sentiment = item.get("sentiment_score")
        if legacy_sentiment is not None and int(legacy_sentiment) <= 3:
            critical_html = (
                '<span style="color:#9CA3AF;">&nbsp;&#9679;&nbsp;</span>'
                '<span style="color:#DC2626;font-weight:700;">CRITICAL</span>'
            )

    return f'{score_html}{tag_html}{signal_html}{critical_html}'


def _render_segment_watch_section(
    groups: dict[str, list[dict]],
    synthesis: dict[str, str],
) -> str:
    """Render the Commercial Segment Watch section.

    Each commercial segment becomes its own block. Within a segment, if a synthesis
    paragraph exists, it appears above the article cards.
    """
    if not groups:
        return ""

    ordered = sorted(
        groups.items(),
        key=lambda kv: -max(int(a.get("americhem_impact_score") or a.get("sentiment_score") or 0)
                            for a in kv[1]),
    )

    blocks_html = ""
    for segment_label, articles in ordered:
        para = synthesis.get(segment_label, "")
        para_html = (
            f'<p style="margin:0 0 10px 0;font-size:13px;color:#1a2a45;'
            f"font-family:Georgia,'Times New Roman',serif;line-height:1.65;\">"
            f'{para}</p>'
        ) if para else ""

        cards_html = ""
        articles_sorted = sorted(
            articles,
            key=lambda x: -int(x.get("americhem_impact_score") or x.get("sentiment_score") or 0),
        )
        for art in articles_sorted:
            meta = _render_meta_strip(art)
            headline = art.get("headline", "")
            source_url = art.get("source_url", "#")
            americhem_impact = art.get("americhem_impact", "")
            so_what_html = (
                f'<p style="margin:4px 0 0 0;font-size:13px;color:#374151;'
                f"font-family:Georgia,'Times New Roman',serif;line-height:1.55;\">"
                f'<strong style="color:{_BRAND_NAVY};">So what:</strong> {americhem_impact}</p>'
                if americhem_impact else ""
            )
            cards_html += (
                f'<tr><td style="padding:6px 0 10px 0;">'
                f'<p style="margin:0 0 4px 0;font-size:11px;color:#6B7280;'
                f'font-family:Arial,sans-serif;">{meta}</p>'
                f'<a href="{source_url}" style="font-size:14px;font-weight:700;'
                f'color:{_BRAND_NAVY};font-family:Arial,sans-serif;'
                f'text-decoration:none;line-height:1.35;">{headline}</a>'
                f'{so_what_html}'
                f'</td></tr>'
            )

        blocks_html += (
            f'<tr><td style="padding:18px 0 0 0;">'
            f'<p style="margin:0 0 8px 0;font-size:12px;font-weight:700;'
            f'letter-spacing:1px;text-transform:uppercase;color:{_BRAND_NAVY};'
            f'font-family:Arial,sans-serif;">{segment_label}</p>'
            f'{para_html}'
            f'<table width="100%" cellpadding="0" cellspacing="0" border="0">{cards_html}</table>'
            f'</td></tr>'
        )

    return f"""
      <tr>
        <td style="padding:24px 32px 4px 32px;">
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="padding-bottom:10px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="font-size:11px;font-weight:700;letter-spacing:1.5px;
                                text-transform:uppercase;color:{_BRAND_NAVY};
                                font-family:Arial,sans-serif;white-space:nowrap;
                                padding-right:12px;">
                      COMMERCIAL SEGMENT WATCH
                    </td>
                    <td style="border-bottom:1px solid {_BRAND_NAVY};width:100%;"></td>
                  </tr>
                </table>
              </td>
            </tr>
            {blocks_html}
          </table>
        </td>
      </tr>"""
```

- [ ] **Step 5: Run the tests**

Run: `pytest tests/test_pipeline.py -v -k "group_by_commercial_segment or render_segment_watch"`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat(delivery): group by commercial_segment; render_segment_watch_section with meta strip"
```

---

## Task 11: Pipeline order, `surfaced_count`, delivery-side count write-back

**Files:**

- Modify: `delivery_engine.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py`:

```python
def test_generate_html_email_surfaced_count_is_post_cap(monkeypatch):
    """surfaced_count must reflect the final visible-card list AFTER per-segment caps."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    # 5 articles, all Healthcare, all impact 8. With max_per_segment=2, only 2
    # should be surfaced. surfaced_count must be 2 — not 5, not len(groups)=1.
    rows = [
        {"url_hash": f"h{i}", "commercial_segment": "Healthcare",
         "americhem_impact_score": 8, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": f"HC {i}",
         "americhem_impact": "Effect.", "source_url": f"https://x/{i}",
         "entities_mentioned": ["Acme"]}
        for i in range(5)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 2,
            "max_total_visible_articles": 12,
        }
    }

    captured = {}
    mock_supa = MagicMock()
    mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()

    def fake_update(payload):
        captured["update"] = payload
        return mock_supa.table.return_value.update.return_value

    mock_supa.table.return_value.update.side_effect = fake_update

    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value=config), \
         patch("delivery_engine._get_supabase", return_value=mock_supa):
        generate_html_email(rows)

    assert captured["update"]["surfaced_count"] == 2


def test_generate_html_email_writes_delivery_suppression_counts_back(monkeypatch):
    """Delivery must write below_impact_threshold and weak_relevance into
    suppression_breakdown via update()."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    rows = [
        # impact=4 -> below threshold (threshold=6)
        {"url_hash": "low", "commercial_segment": "Healthcare",
         "americhem_impact_score": 4, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": "Below threshold",
         "americhem_impact": ".", "source_url": "https://x/1",
         "entities_mentioned": ["Acme"]},
        # impact=5 -> weak relevance (above threshold base of 4, below visible threshold)
        # Actually impact=5 is below threshold=6 -> classifies as below_impact_threshold
        {"url_hash": "high", "commercial_segment": "Packaging",
         "americhem_impact_score": 8, "sentiment_tag": "Positive",
         "signal_type": "Customer", "headline": "Surfaced",
         "americhem_impact": ".", "source_url": "https://x/2",
         "entities_mentioned": ["Acme"]},
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 3,
            "max_total_visible_articles": 12,
        }
    }
    captured = {}
    mock_supa = MagicMock()

    def fake_update(payload):
        captured["update"] = payload
        return mock_supa.table.return_value.update.return_value

    mock_supa.table.return_value.update.side_effect = fake_update
    mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()

    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value=config), \
         patch("delivery_engine._get_supabase", return_value=mock_supa):
        generate_html_email(rows)

    breakdown = captured["update"]["suppression_breakdown"]
    assert breakdown["below_impact_threshold"] == 1
    assert captured["update"]["surfaced_count"] == 1


def test_generate_html_email_update_filtered_by_run_date_and_run_mode(monkeypatch):
    """The update() call must be filtered by run_date AND run_mode."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    rows = [{
        "url_hash": "a", "commercial_segment": "Healthcare",
        "americhem_impact_score": 8, "sentiment_tag": "Neutral",
        "signal_type": "Customer", "headline": "H", "americhem_impact": ".",
        "source_url": "https://x/a", "entities_mentioned": ["Acme"],
    }]

    eq_calls = []
    mock_supa = MagicMock()

    def fake_eq(col, val):
        eq_calls.append((col, val))
        return mock_supa.table.return_value.update.return_value

    mock_supa.table.return_value.update.return_value.eq.side_effect = fake_eq
    mock_supa.table.return_value.update.return_value.eq.return_value.eq.side_effect = fake_eq
    mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()

    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}), \
         patch("delivery_engine._get_supabase", return_value=mock_supa):
        generate_html_email(rows)

    keys = {c[0] for c in eq_calls}
    assert "run_date" in keys
    assert "run_mode" in keys
    # run_mode value must be 'test'
    rm_calls = [c for c in eq_calls if c[0] == "run_mode"]
    assert any(c[1] == "test" for c in rm_calls)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k "surfaced_count or writes_delivery_suppression or update_filtered_by_run"`
Expected: FAIL — pipeline order isn't updated, update() isn't called yet.

- [ ] **Step 3: Rewrite `generate_html_email()` pipeline order**

In `delivery_engine.py`, replace the body of `generate_html_email()` with the new pipeline. Keep the existing header/footer template HTML; only restructure the data-flow section. The new core flow:

```python
def generate_html_email(
    data: list[dict],
    macro_summary: dict | None = None,
) -> str:
    config = _load_mp_config()
    reporting_cfg          = config.get("reporting", {})
    visible_threshold: int = _config_int(reporting_cfg, "visible_impact_threshold", 6)
    max_per_segment: int   = _config_int(reporting_cfg, "max_visible_articles_per_segment", 3)
    max_total_visible: int = _config_int(reporting_cfg, "max_total_visible_articles", 12)

    # 1. Final guardrail suppression pass (delivery-side patterns + dedupe).
    kept, delivery_sup_counts, delivery_sup_samples = _apply_delivery_suppression(data, config)

    # 2. Visibility filter.
    visible_pool = [r for r in kept if _effective_impact(r) >= visible_threshold]
    below_threshold_count = len(kept) - len(visible_pool)

    # 3. Group by commercial segment.
    groups_full = _group_by_commercial_segment(visible_pool)

    # 4. Per-segment cap.
    groups = {
        seg: sorted(arts, key=lambda x: _effective_impact(x), reverse=True)[:max_per_segment]
        for seg, arts in groups_full.items()
    }
    # 5. Total visible cap (drop lowest-impact across all groups until count <= cap).
    total = sum(len(arts) for arts in groups.values())
    if total > max_total_visible:
        all_visible = sorted(
            [(seg, a) for seg, arts in groups.items() for a in arts],
            key=lambda kv: _effective_impact(kv[1]),
            reverse=True,
        )[:max_total_visible]
        selected_hashes = {a.get("url_hash") for _, a in all_visible}
        groups = {seg: [a for a in arts if a.get("url_hash") in selected_hashes]
                  for seg, arts in groups.items()}
        groups = {seg: arts for seg, arts in groups.items() if arts}

    # 6. Compute weak_relevance: rows in `kept` with effective impact 4-5 that
    # didn't make it into any final group (the old Peripheral pool).
    final_hashes = {a.get("url_hash") for arts in groups.values() for a in arts}
    weak_relevance_count = sum(
        1 for r in kept
        if 4 <= _effective_impact(r) <= 5
        and r.get("url_hash") not in final_hashes
    )

    # 7. surfaced_count is the FINAL visible-card count (post-cap).
    surfaced_count = sum(len(arts) for arts in groups.values())

    # 8. Write delivery-side counts + surfaced_count back to today's row.
    _update_delivery_summary_counts(
        surfaced_count=surfaced_count,
        delivery_counts={
            **delivery_sup_counts,
            "below_impact_threshold": below_threshold_count,
            "weak_relevance": weak_relevance_count,
        },
        delivery_samples=delivery_sup_samples,
    )

    # 9. Thematic synthesis paragraphs (existing helper, now keyed off the new groups).
    multi_article_groups = {seg: arts for seg, arts in groups.items() if len(arts) >= 2}
    synthesis = synthesize_thematic_paragraphs(multi_article_groups)

    # 10. Build the HTML body.
    sections_html = _render_segment_watch_section(groups, synthesis)

    # Header / executive bullets / QA section come from Tasks 12-14.
    exec_html = _render_exec_summary(macro_summary)

    today_str = datetime.now().strftime("%A, %B %d, %Y")
    # Header counts (Task 13 makes these null-safe; until then use straight reads).
    screened = (macro_summary or {}).get("screened_count")
    if screened is None:
        screened = len(data)
    dominant_condition = (macro_summary or {}).get("dominant_condition") or ""

    macro_badge_html = ""
    if dominant_condition:
        macro_badge_html = (
            f'<span style="background-color:rgba(127,176,105,0.2);'
            f'color:{_BRAND_GREEN};border:1px solid rgba(127,176,105,0.4);'
            f'padding:3px 12px;border-radius:20px;font-size:11px;font-weight:600;'
            f'font-family:Arial,sans-serif;letter-spacing:0.5px;">'
            f'{dominant_condition}</span>'
        )

    _test_mode = _is_test_mode()
    title_prefix = "[TEST] " if _test_mode else ""
    test_banner_row = _TEST_BANNER_ROW if _test_mode else ""

    qa_html = _render_qa_debug_section(macro_summary) if _test_mode else ""

    # Subtitle: "{date} · N surfaced signals from M screened items"
    subtitle = (
        f"{today_str} &nbsp;&middot;&nbsp; "
        f"{surfaced_count} surfaced signals from {screened} screened items"
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="color-scheme" content="light">
  <title>Americhem Market-Pulse: Daily Intelligence</title>
</head>
<body style="margin:0;padding:0;background-color:#F3F4F6;
             font-family:Arial,sans-serif;-webkit-text-size-adjust:100%;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0"
         style="background-color:#F3F4F6;padding:24px 0;">
    <tr><td align="center">
      <table width="640" cellpadding="0" cellspacing="0" border="0"
             style="max-width:640px;background-color:#ffffff;
                    border:0.5px solid #E5E7EB;border-radius:8px;overflow:hidden;">
        <tr><td>
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="background-color:{_BRAND_NAVY};padding:20px 32px 0 32px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="width:1%;white-space:nowrap;padding-right:16px;">
                      <img src="{_LOGO_URL}" alt="Americhem" width="140"
                           style="display:block;height:auto;max-height:40px;background-color:#ffffff;padding:3px 8px;border-radius:3px;">
                    </td>
                    <td style="width:1%;white-space:nowrap;padding-right:16px;">
                      <div style="width:1px;height:32px;background-color:rgba(255,255,255,0.25);"></div>
                    </td>
                    <td>
                      <p style="margin:0;font-size:11px;font-weight:700;letter-spacing:1.5px;color:{_BRAND_GREEN};font-family:Arial,sans-serif;text-transform:uppercase;">Market Intelligence</p>
                      <p style="margin:2px 0 0 0;font-size:18px;font-weight:700;color:#ffffff;font-family:Arial,sans-serif;line-height:1.2;">{title_prefix}Market-Pulse: Daily Intelligence</p>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr><td style="background-color:{_BRAND_GREEN};height:3px;font-size:0;line-height:0;">&nbsp;</td></tr>
            {test_banner_row}
            <tr>
              <td style="background-color:{_BRAND_NAVY_DARK};padding:10px 32px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="font-size:12px;color:rgba(255,255,255,0.65);font-family:Arial,sans-serif;">{subtitle}</td>
                    <td align="right">{macro_badge_html}</td>
                  </tr>
                </table>
              </td>
            </tr>
          </table>
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            {exec_html}
            {sections_html}
            {qa_html}
            <tr><td style="height:24px;"></td></tr>
          </table>
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="border-top:0.5px solid #E5E7EB;background-color:#FAFAFA;padding:16px 32px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="font-size:11px;color:#9CA3AF;font-family:Arial,sans-serif;">
                      Generated by <strong style="color:{_BRAND_NAVY};">Americhem Market-Pulse</strong> &nbsp;&middot;&nbsp; Powered by OpenAI &amp; Supabase
                    </td>
                    <td align="right">
                      <img src="{_LOGO_URL}" alt="Americhem" width="80" style="display:block;height:auto;opacity:0.4;">
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
          </table>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""
```

- [ ] **Step 4: Implement `_update_delivery_summary_counts()`**

Add to `delivery_engine.py`:

```python
def _update_delivery_summary_counts(
    *,
    surfaced_count: int,
    delivery_counts: dict,
    delivery_samples: list,
) -> None:
    """Update today's daily_summaries row (filtered by run_mode) with delivery-side stats.

    Non-critical: failures are logged but do not raise.
    """
    try:
        from datetime import date as _date
        supabase = _get_supabase()

        # Fetch the existing breakdown/samples so we can merge ours in.
        existing = (
            supabase.table("daily_summaries")
            .select("suppression_breakdown, suppression_samples")
            .eq("run_date", _date.today().isoformat())
            .eq("run_mode", _run_mode())
            .limit(1)
            .execute()
        )
        rows = existing.data or []
        prior_breakdown = (rows[0].get("suppression_breakdown") if rows else None) or {}
        prior_samples   = (rows[0].get("suppression_samples")   if rows else None) or []

        merged_breakdown = dict(prior_breakdown)
        for k, v in delivery_counts.items():
            merged_breakdown[k] = merged_breakdown.get(k, 0) + int(v)

        merged_samples = list(prior_samples) + list(delivery_samples)
        if len(merged_samples) > _DELIVERY_SAMPLES_CAP:
            merged_samples = merged_samples[-_DELIVERY_SAMPLES_CAP:]

        supabase.table("daily_summaries").update({
            "surfaced_count": surfaced_count,
            "suppression_breakdown": merged_breakdown,
            "suppression_samples": merged_samples,
        }).eq("run_date", _date.today().isoformat()).eq("run_mode", _run_mode()).execute()
    except Exception as exc:
        logger.warning("Failed to update delivery counts on daily_summaries: %s", exc)
```

- [ ] **Step 5: Run the new tests**

Run: `pytest tests/test_pipeline.py -v -k "surfaced_count or writes_delivery_suppression or update_filtered_by_run"`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat(delivery): wire pipeline order; surfaced_count = post-cap; write delivery counts back"
```

---

## Task 12: `_render_executive_bullets()` + dominant-condition badge + Low Signal handling

**Files:**

- Modify: `delivery_engine.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py`:

```python
def test_render_executive_bullets_renders_three_labeled_bullets():
    from delivery_engine import _render_executive_bullets
    bullets = [
        {"label": "Market pressure",    "body": "Techmer raised prices."},
        {"label": "Supply chain watch", "body": "Mitsubishi restructuring."},
        {"label": "Commercial action",  "body": "Prioritize additives."},
    ]
    html = _render_executive_bullets(bullets)
    assert "Market pressure" in html
    assert "Supply chain watch" in html
    assert "Commercial action" in html
    assert "Techmer raised prices." in html
    assert "Mitsubishi restructuring." in html
    assert "Prioritize additives." in html


def test_render_exec_summary_uses_structured_bullets_when_present():
    from delivery_engine import _render_exec_summary
    macro = {
        "dominant_condition": "Competitive Pressure",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
        "executive_summary": "Should not be used.",
    }
    html = _render_exec_summary(macro)
    assert "Market pressure" in html
    assert "A." in html
    assert "Should not be used." not in html
    assert "Competitive Pressure" in html  # condition badge


def test_render_exec_summary_falls_back_to_legacy_when_bullets_null():
    from delivery_engine import _render_exec_summary
    macro = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": None,
        "executive_summary": "Legacy prose summary used.",
    }
    html = _render_exec_summary(macro)
    assert "Legacy prose summary used." in html
    assert "Market pressure" not in html


def test_render_exec_summary_no_summary_returns_empty():
    from delivery_engine import _render_exec_summary
    assert _render_exec_summary(None) == ""
    assert _render_exec_summary({}) == ""
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k "render_executive_bullets or render_exec_summary"`
Expected: FAIL — bullets not implemented; old `_render_exec_summary` doesn't read structured fields.

- [ ] **Step 3: Implement bullets + rewrite `_render_exec_summary()`**

In `delivery_engine.py`, replace `_render_exec_summary()` and add `_render_executive_bullets()`:

```python
def _render_executive_bullets(bullets: list[dict]) -> str:
    """Render the 3-bullet executive summary body.

    Each bullet uses bold label + body text. Labels are taken from the data,
    which guarantees they match the configured executive_bullet_labels.
    """
    items_html = ""
    for b in bullets:
        label = b.get("label", "")
        body = b.get("body", "")
        items_html += (
            f'<tr><td style="padding:2px 0;font-size:13px;color:#1a2a45;'
            f"font-family:Georgia,'Times New Roman',serif;line-height:1.55;\">"
            f'&bull;&nbsp;<strong>{label}:</strong> {body}'
            f'</td></tr>'
        )
    return (
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0">'
        f'{items_html}</table>'
    )


def _render_exec_summary(macro_summary: dict | None) -> str:
    """Render the Executive Summary row.

    Prefers structured executive_bullets; falls back to legacy executive_summary prose.
    Returns empty string when no summary data is present.
    """
    if not macro_summary:
        return ""

    bullets = macro_summary.get("executive_bullets")
    legacy_text = macro_summary.get("executive_summary") or ""
    condition = macro_summary.get("dominant_condition") or macro_summary.get("macro_sentiment") or ""

    if bullets:
        body_html = _render_executive_bullets(bullets)
    elif legacy_text:
        body_html = (
            f'<p style="margin:0;font-size:14px;color:#1a2a45;'
            f"font-family:Georgia,'Times New Roman',serif;line-height:1.65;\">"
            f'{legacy_text}</p>'
        )
    else:
        return ""

    badge_html = ""
    if condition:
        badge_html = (
            f'&nbsp;<span style="background-color:{_BRAND_NAVY};color:#ffffff;'
            f'padding:2px 10px;border-radius:20px;font-size:10px;font-weight:600;'
            f'letter-spacing:0.5px;">{condition}</span>'
        )

    return f"""
      <tr>
        <td style="padding:24px 32px 0 32px;">
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="background-color:#EEF2FF;border-left:3px solid {_BRAND_NAVY};
                          border-radius:0 6px 6px 0;padding:16px 20px;">
                <p style="margin:0 0 8px 0;font-size:10px;font-weight:700;
                           letter-spacing:1.5px;color:{_BRAND_NAVY};
                           font-family:Arial,sans-serif;text-transform:uppercase;">
                  Executive Summary{badge_html}
                </p>
                {body_html}
              </td>
            </tr>
          </table>
        </td>
      </tr>"""
```

- [ ] **Step 4: Run the tests**

Run: `pytest tests/test_pipeline.py -v -k "render_executive_bullets or render_exec_summary"`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat(delivery): render structured executive bullets with dominant-condition badge"
```

---

## Task 13: Null-safe header rendering

**Files:**

- Modify: `delivery_engine.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py`:

```python
def test_header_falls_back_to_len_data_when_screened_null(monkeypatch):
    """When screened_count is NULL, header uses len(data)."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    rows = [
        {"url_hash": f"h{i}", "commercial_segment": "Healthcare",
         "americhem_impact_score": 8, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": f"H {i}",
         "americhem_impact": ".", "source_url": f"https://x/{i}",
         "entities_mentioned": ["Acme"]}
        for i in range(7)
    ]
    macro = {"executive_bullets": [
        {"label": "Market pressure",    "body": "A."},
        {"label": "Supply chain watch", "body": "B."},
        {"label": "Commercial action",  "body": "C."},
    ], "dominant_condition": "Competitive Pressure",
       "screened_count": None, "surfaced_count": None}

    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._get_supabase", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email(rows, macro_summary=macro)

    assert "from 7 screened items" in html
    assert "from None screened items" not in html


def test_header_omits_dominant_condition_clause_when_null(monkeypatch):
    """When dominant_condition is NULL, the badge clause is omitted (no 'None')."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    rows = [{"url_hash": "a", "commercial_segment": "Healthcare",
             "americhem_impact_score": 8, "sentiment_tag": "Neutral",
             "signal_type": "Customer", "headline": "H",
             "americhem_impact": ".", "source_url": "https://x/a",
             "entities_mentioned": ["Acme"]}]
    macro = {"executive_bullets": None, "executive_summary": "Fallback prose.",
             "dominant_condition": None, "macro_sentiment": None,
             "screened_count": 5, "surfaced_count": 1}
    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._get_supabase", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email(rows, macro_summary=macro)

    # Body still renders, but the badge after "Executive Summary" must be absent.
    assert "Dominant condition: None" not in html
    assert ">None<" not in html  # no literal "None" rendered anywhere
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k "screened_null or dominant_condition_clause_when_null"`
Expected: FAIL — current `generate_html_email()` may render `None` in some null paths.

- [ ] **Step 3: Audit and fix null fallbacks in `generate_html_email()`**

The implementation in Task 11 already covers `screened` fallback (`if screened is None: screened = len(data)`) and skips the badge when `dominant_condition` is empty. Verify in the file that:

- `screened = (macro_summary or {}).get("screened_count")` followed by `if screened is None: screened = len(data)` is present.
- `dominant_condition = (macro_summary or {}).get("dominant_condition") or ""` followed by `if dominant_condition:` gate on the badge HTML is present.
- The subtitle string never substitutes a literal `None`.

Add one more null fallback for `surfaced_count` when called with a pre-populated `macro_summary` containing `surfaced_count`: rely on the in-process computation (`surfaced_count = sum(len(arts) for arts in groups.values())`) and ignore any value on `macro_summary` so the row is always accurate for this run.

- [ ] **Step 4: Run the tests**

Run: `pytest tests/test_pipeline.py -v -k "screened_null or dominant_condition_clause_when_null"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat(delivery): null-safe header (screened, surfaced, dominant_condition)"
```

---

## Task 14: `_render_qa_debug_section()` gated by test mode

**Files:**

- Modify: `delivery_engine.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py`:

```python
def test_qa_debug_section_appears_in_test_mode(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    rows = [{"url_hash": "a", "commercial_segment": "Healthcare",
             "americhem_impact_score": 8, "sentiment_tag": "Neutral",
             "signal_type": "Customer", "headline": "H",
             "americhem_impact": ".", "source_url": "https://x/a",
             "entities_mentioned": ["Acme"]}]
    macro = {
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
        "dominant_condition": "Competitive Pressure",
        "screened_count": 87,
        "surfaced_count": 1,
        "suppression_breakdown": {
            "duplicate_url": 23,
            "llm_discard": 12,
            "product_listing": 5,
            "job_posting": 3,
        },
        "suppression_samples": [
            {"reason": "product_listing", "url": "https://amazon.com/product/1",
             "title": "Pretty plastic tote"},
            {"reason": "llm_discard", "url": "https://news.com/extension-cord",
             "title": "Best extension cord colors"},
        ],
    }
    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._get_supabase", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email(rows, macro_summary=macro)

    assert "QA" in html
    assert "Suppression Summary" in html
    assert "duplicate URL" in html or "duplicate_url" in html
    assert "product listing" in html or "product_listing" in html
    assert "Pretty plastic tote" in html
    assert "Best extension cord colors" in html


def test_qa_debug_section_absent_in_production(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    rows = [{"url_hash": "a", "commercial_segment": "Healthcare",
             "americhem_impact_score": 8, "sentiment_tag": "Neutral",
             "signal_type": "Customer", "headline": "H",
             "americhem_impact": ".", "source_url": "https://x/a",
             "entities_mentioned": ["Acme"]}]
    macro = {
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
        "dominant_condition": "Competitive Pressure",
        "screened_count": 87,
        "surfaced_count": 1,
        "suppression_breakdown": {"duplicate_url": 23, "product_listing": 5},
        "suppression_samples": [{"reason": "product_listing",
                                 "url": "https://amazon.com/product/1",
                                 "title": "Pretty plastic tote"}],
    }
    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._get_supabase", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email(rows, macro_summary=macro)

    assert "Suppression Summary" not in html
    assert "Pretty plastic tote" not in html


def test_render_qa_debug_section_uses_friendly_labels():
    from delivery_engine import _render_qa_debug_section
    macro = {
        "screened_count": 87,
        "surfaced_count": 6,
        "suppression_breakdown": {
            "duplicate_url": 23,
            "semantic_duplicate": 4,
            "llm_discard": 12,
            "enterprise_cross_segment_low_impact": 3,
        },
        "suppression_samples": [
            {"reason": "duplicate_url", "url": "https://x/1", "title": "Dup"},
        ],
    }
    html = _render_qa_debug_section(macro)
    # Friendly labels rather than raw snake_case in the prose
    assert "duplicate URL" in html
    assert "semantic duplicate" in html
    assert "LLM discard" in html
    assert "Enterprise / Cross-Segment" in html
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -v -k "qa_debug or render_qa"`
Expected: FAIL — `_render_qa_debug_section` doesn't exist.

- [ ] **Step 3: Implement `_render_qa_debug_section()`**

Add to `delivery_engine.py`:

```python
_QA_REASON_LABELS: dict[str, str] = {
    "duplicate_url":                       "duplicate URL",
    "semantic_duplicate":                  "semantic duplicate",
    "llm_discard":                         "LLM discard",
    "scrape_failed":                       "scrape failed",
    "below_impact_threshold":              "below impact threshold",
    "weak_relevance":                      "weak relevance (4-5, ungrouped)",
    "duplicate_headline":                  "duplicate headline",
    "semantic_duplicate_headline":         "semantic duplicate headline",
    "product_listing":                     "product listing",
    "job_posting":                         "job posting",
    "generic_market_report":               "generic market report",
    "unrelated_color_result":              "unrelated color result",
    "enterprise_cross_segment_low_impact": "Enterprise / Cross-Segment, low impact",
}


def _render_qa_debug_section(macro_summary: dict | None) -> str:
    """Render the QA suppression summary block. Only called in test mode."""
    if not macro_summary:
        return ""

    screened = macro_summary.get("screened_count")
    surfaced = macro_summary.get("surfaced_count")
    breakdown = macro_summary.get("suppression_breakdown") or {}
    samples = macro_summary.get("suppression_samples") or []

    suppressed_total = sum(int(v) for v in breakdown.values())

    rows_html = ""
    # Stable display order: by ingestion-style first then delivery-style.
    display_order = [
        "duplicate_url", "semantic_duplicate", "llm_discard", "scrape_failed",
        "below_impact_threshold", "weak_relevance",
        "duplicate_headline", "semantic_duplicate_headline",
        "product_listing", "job_posting", "generic_market_report",
        "unrelated_color_result", "enterprise_cross_segment_low_impact",
    ]
    for code in display_order:
        if code in breakdown:
            label = _QA_REASON_LABELS.get(code, code)
            rows_html += (
                f'<tr><td style="padding:2px 0;font-size:12px;color:#374151;'
                f'font-family:Arial,sans-serif;">'
                f'&nbsp;&nbsp;{label}'
                f'</td><td align="right" style="padding:2px 0;font-size:12px;'
                f'color:#374151;font-family:Arial,sans-serif;">{breakdown[code]}</td></tr>'
            )

    samples_html = ""
    for s in samples[-10:]:
        reason_code = s.get("reason", "")
        reason_label = _QA_REASON_LABELS.get(reason_code, reason_code)
        title = s.get("title", "")
        url = s.get("url", "")
        samples_html += (
            f'<tr><td style="padding:2px 0;font-size:11px;color:#6B7280;'
            f'font-family:monospace;">'
            f'[{reason_label}] "{title}" — {url}'
            f'</td></tr>'
        )

    return f"""
      <tr>
        <td style="padding:24px 32px 4px 32px;">
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="padding-bottom:10px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="font-size:11px;font-weight:700;letter-spacing:1.5px;
                                text-transform:uppercase;color:#9CA3AF;
                                font-family:Arial,sans-serif;white-space:nowrap;
                                padding-right:12px;">
                      QA &middot; Suppression Summary
                    </td>
                    <td style="border-bottom:1px solid #E5E7EB;width:100%;"></td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td>
                <p style="margin:0 0 8px 0;font-size:12px;color:#374151;
                           font-family:Arial,sans-serif;">
                  Screened: {screened if screened is not None else '?'} &nbsp;&middot;&nbsp;
                  Surfaced: {surfaced if surfaced is not None else '?'} &nbsp;&middot;&nbsp;
                  Suppressed: {suppressed_total}
                </p>
                <p style="margin:8px 0 4px 0;font-size:11px;color:#6B7280;
                           font-family:Arial,sans-serif;text-transform:uppercase;
                           letter-spacing:1px;">
                  By reason
                </p>
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  {rows_html}
                </table>
                <p style="margin:12px 0 4px 0;font-size:11px;color:#6B7280;
                           font-family:Arial,sans-serif;text-transform:uppercase;
                           letter-spacing:1px;">
                  Last 10 suppressed items
                </p>
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  {samples_html}
                </table>
              </td>
            </tr>
          </table>
        </td>
      </tr>"""
```

- [ ] **Step 4: Run the tests**

Run: `pytest tests/test_pipeline.py -v -k "qa_debug or render_qa"`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat(delivery): QA suppression summary section, gated by MARKET_PULSE_RUN_MODE=test"
```

---

## Task 15: Update legacy tests + full-suite verification

**Files:**

- Modify: `tests/test_pipeline.py`

- [ ] **Step 1: Delete or rewrite tests that assert removed sections / sentinel labels**

In `tests/test_pipeline.py`, find and update each of these:

1. `test_render_card_omits_article_summary` and `test_render_card_excludes_article_summary` — both still test `_render_card` directly. After Task 10, the segment-watch render path no longer uses `_render_card` in the main flow. `_render_card` may still exist as a helper if you didn't delete it. If you deleted `_render_card`, delete both tests. If you kept it for any legacy path, leave both tests in place.
2. `test_render_card_suppresses_monitor_action`, `test_render_card_shows_escalation_action`, `test_render_card_shows_impact_score_and_sentiment_tag`, `test_render_card_falls_back_to_sentiment_score_for_old_rows` — same disposition as #1: delete if `_render_card` no longer exists; keep otherwise.
3. `test_generate_html_email_all_critical_no_thematic_section` — the old assertion was `"THEMATIC INTELLIGENCE" not in html` (correct under the new design) and `"PERIPHERAL SIGNALS" not in html` (still correct). Rewrite to assert the new behaviour: critical legacy rows appear under Commercial Segment Watch with a CRITICAL badge.

```python
def test_generate_html_email_legacy_critical_appears_with_badge(monkeypatch):
    """Legacy sentiment_score<=3 rows appear in Commercial Segment Watch with CRITICAL badge."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    data = [
        {"url_hash": "c0", "sentiment_score": 2, "category": "suppliers",
         "headline": "Critical Headline 0", "americhem_impact": "Disruption.",
         "entities_mentioned": ["BASF"], "source_url": "https://x/0",
         "strategic_segment": "Broader Americhem"},
    ]
    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._get_supabase", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email(data)
    assert "COMMERCIAL SEGMENT WATCH" in html
    assert "Critical Headline 0" in html
    assert "CRITICAL" in html
    assert "PERIPHERAL SIGNALS" not in html
    assert "CRITICAL DISRUPTIONS" not in html
```

4. `test_generate_html_email_routes_to_thematic_with_two_plus` — rewrite to assert segment-watch grouping:

```python
def test_generate_html_email_routes_two_plus_to_segment_watch(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    data = [
        {"url_hash": "a", "commercial_segment": "Healthcare",
         "americhem_impact_score": 7, "sentiment_tag": "Positive",
         "signal_type": "Customer", "headline": "Avient Healthcare A",
         "americhem_impact": "Effect.", "source_url": "https://x/a",
         "entities_mentioned": ["Avient"]},
        {"url_hash": "b", "commercial_segment": "Healthcare",
         "americhem_impact_score": 8, "sentiment_tag": "Positive",
         "signal_type": "Customer", "headline": "Techmer Healthcare B",
         "americhem_impact": "Effect.", "source_url": "https://x/b",
         "entities_mentioned": ["Techmer"]},
    ]
    mock_synth = _make_synthesis_mock({"Healthcare": "Synthesis paragraph here."})
    with patch("delivery_engine._get_openai", return_value=mock_synth), \
         patch("delivery_engine._get_supabase", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email(data)
    assert "COMMERCIAL SEGMENT WATCH" in html
    assert "HEALTHCARE" in html.upper()
    assert "Synthesis paragraph here." in html
```

5. `test_generate_html_email_routes_single_low_to_peripheral` — invert and rename:

```python
def test_generate_html_email_single_low_relevance_hidden_in_production(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    data = [{"url_hash": "x", "commercial_segment": "Packaging",
             "americhem_impact_score": 5, "sentiment_tag": "Neutral",
             "signal_type": "Customer", "headline": "Peripheral Headline",
             "americhem_impact": ".", "source_url": "https://x/p",
             "entities_mentioned": ["Acme"]}]
    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._get_supabase", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email(data)
    assert "Peripheral Headline" not in html
    assert "PERIPHERAL SIGNALS" not in html
```

6. `test_generate_html_email_per_segment_cap`, `test_generate_html_email_total_articles_cap`, `test_generate_html_email_capped_articles_do_not_reappear` — these test the new pipeline. Update each test's article fixtures to use the new field names (`commercial_segment` instead of `strategic_segment` where applicable) and ensure they pass against the new render. Specifically, the fixtures already use `_make_new_article` which sets `strategic_segment` — change that helper to set `commercial_segment` instead:

```python
def _make_new_article(
    url_hash: str,
    americhem_impact_score: int,
    commercial_segment: str = "Industrial",
    sentiment_tag: str = "Neutral",
    signal_type: str = "Customer",
    headline: str = "Test Headline",
) -> dict:
    return {
        "url_hash": url_hash,
        "americhem_impact_score": americhem_impact_score,
        "sentiment_tag": sentiment_tag,
        "signal_type": signal_type,
        "impact_rationale": "Direct feedstock cost effect.",
        "commercial_segment": commercial_segment,
        "headline": headline,
        "americhem_impact": "Some impact.",
        "entities_mentioned": ["TestCorp"],
        "source_url": "https://news.com/article",
        "category": "markets",
        "recommended_action": "Monitor",
    }
```

Update all call sites in the test file that pass `strategic_segment=...` to instead pass `commercial_segment=...`.

7. `test_generate_html_email_filters_below_impact_threshold` — still valid; ensure it uses `commercial_segment` not `strategic_segment` in fixtures.

8. `test_generate_html_email_test_mode_prefixes_header` and `test_generate_html_email_production_mode_unchanged` — still valid; should already pass.

- [ ] **Step 2: Delete the obsolete thematic-section tests**

In `tests/test_pipeline.py`, delete tests that exercise removed helpers:

- `test_group_for_thematic_*` (4 tests) — replaced by `test_group_by_commercial_segment_*`
- `test_collect_thin_entries_*` (3 tests) — function deleted
- `test_collect_peripheral_*` (3 tests) — function deleted
- `test_render_peripheral_section_*` (4 tests) — function deleted
- `test_render_thematic_section_*` (5 tests) — replaced by `test_render_segment_watch_section_*`
- `test_synthesize_thematic_paragraphs_*` (5 tests) — function kept (still used) but the call site changed; verify these still pass against the new groups parameter. If they do, keep them.

Also delete the legacy helper `_make_article` if no remaining test calls it. (`_make_synthesis_mock` is still used.)

- [ ] **Step 3: Verify all tests pass**

Run: `pytest tests/test_pipeline.py -v`
Expected: every test PASSES. If anything fails, fix it before continuing.

Run: `pytest tests/ -v`
Expected: full directory passes.

- [ ] **Step 4: Manual smoke check — render a sample email**

Create a quick script (do not commit it) to render a sample email locally so you can eyeball the layout:

```bash
python - <<'PY'
from unittest.mock import patch, MagicMock
from delivery_engine import generate_html_email

rows = [
    {"url_hash": "a", "commercial_segment": "Healthcare",
     "americhem_impact_score": 7, "sentiment_tag": "Positive",
     "signal_type": "Technology",
     "headline": "KRAIBURG TPE targets wrist orthoses",
     "americhem_impact": "Supports regulated healthcare elastomer demand.",
     "source_url": "https://x/1", "entities_mentioned": ["KRAIBURG TPE"]},
    {"url_hash": "b", "commercial_segment": "Packaging",
     "americhem_impact_score": 6, "sentiment_tag": "Positive",
     "signal_type": "Sustainability",
     "headline": "Sirmax launches 70% recycled-plastic coffee maker",
     "americhem_impact": "Raises customer expectations for recycled content.",
     "source_url": "https://x/2", "entities_mentioned": ["Sirmax", "De'Longhi"]},
]
macro = {
    "dominant_condition": "Competitive Pressure",
    "executive_bullets": [
        {"label": "Market pressure",    "body": "Techmer, Teknor Apex, and KRAIBURG are increasing specialty materials claims."},
        {"label": "Supply chain watch", "body": "Mitsubishi and Asahi Kasei restructuring signals may create volatility."},
        {"label": "Commercial action",  "body": "Prioritize compliant additive platforms and aerospace prototyping support."},
    ],
    "executive_summary": "Combined summary text.",
    "screened_count": 87,
    "surfaced_count": 2,
    "suppression_breakdown": {"duplicate_url": 23, "product_listing": 5, "llm_discard": 12},
    "suppression_samples": [
        {"reason": "llm_discard", "url": "https://x/extension", "title": "Best extension cord colors"},
    ],
}
with patch("delivery_engine._get_openai", return_value=MagicMock()), \
     patch("delivery_engine._get_supabase", return_value=MagicMock()), \
     patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}, "delivery_suppression": {}}):
    html = generate_html_email(rows, macro_summary=macro)
open("/tmp/market_pulse_sample.html", "w").write(html)
print("Wrote /tmp/market_pulse_sample.html")
PY
```

Open `/tmp/market_pulse_sample.html` in a browser. Verify:

- Header reads `2 surfaced signals from 87 screened items`.
- Dominant condition badge says `Competitive Pressure`.
- Three executive bullets render with bold labels.
- Two commercial segment blocks (Healthcare, Packaging) appear.
- Meta strip on each card shows `Impact: X/10 · {Tag} · Signal: Y`.
- No "CRITICAL DISRUPTIONS" header, no "PERIPHERAL SIGNALS" header.
- QA section does NOT appear (default = production).

Re-run with `MARKET_PULSE_RUN_MODE=test` and verify QA section now appears with friendly labels.

- [ ] **Step 5: Run the full test suite one last time**

Run: `pytest tests/` (no `-v`, just the summary)
Expected: `X passed, 0 failed`.

- [ ] **Step 6: Commit**

```bash
git add tests/test_pipeline.py
git commit -m "test(pipeline): update legacy tests for new commercial-segment architecture"
```

---

## Plan self-review

Spec coverage:

- §1 Context — no implementation work.
- §2 Success criteria — covered across all tasks (especially Tasks 9, 11, 12, 13, 14).
- §3 Out of scope — observed throughout.
- §4 Content architecture — Tasks 10, 11, 12.
- §5 Data model — Tasks 1 (migration), 5 (macro fields), 6 (suppression fields), 7 (run_mode).
- §6 Ingestion prompt — Tasks 3, 4, 5, 6.
- §6.3 Config — Task 2.
- §7.1 Helpers — Task 8 (`_commercial_segment_of`, `_signal_type_of`), Task 7 (`_run_mode`), Task 9 (`_apply_delivery_suppression`), Task 10 (grouping + segment watch + meta strip), Task 12 (executive bullets), Task 14 (QA debug).
- §7.2 Legacy fallback — Task 8.
- §7.3 Final suppression pass — Task 9.
- §7.4 Pipeline order — Task 11.
- §7.5 Header & exec summary — Tasks 11, 12, 13.
- §7.6 Per-card meta strip — Task 10.
- §8 Suppression — Tasks 6 (ingestion), 9 + 11 (delivery).
- §9 Test-mode debug — Task 14.
- §10 Backward compatibility — Task 8 (commercial_segment fallback), Task 12 (legacy executive_summary), Task 13 (null safety).
- §11 Testing — every task includes tests; Task 15 covers the legacy cleanup.
- §12 Migration plan — Task 1, with Step 1 verifying the existing index name (user caution #1 addressed).
- §13 Known limitations — documented in spec; nothing to implement.
- §14 Decisions index — documented in spec; nothing to implement.

User cautions checked:

- ✅ #1 Inspect existing index name — Task 1 Step 1.
- ✅ #2 `surfaced_count` from post-suppression post-cap — Task 11 Step 3 (`surfaced_count = sum(len(arts) for arts in groups.values())` AFTER caps).
- ✅ #3 Production QA hidden — Task 14 (`qa_html = _render_qa_debug_section(...) if _test_mode else ""`).
- ✅ #4 Backward compatibility — Tasks 8, 12, 13.
- ✅ #5 Run `pytest tests/` before claiming done — Task 15 Step 5.

Placeholder scan: no TBD / TODO / "add appropriate error handling" / "similar to Task N" instances. Every code step contains the actual code.

Type consistency: function names used consistently across tasks (`_commercial_segment_of`, `_signal_type_of`, `_apply_delivery_suppression`, `_group_by_commercial_segment`, `_render_segment_watch_section`, `_render_executive_bullets`, `_render_qa_debug_section`, `_run_mode`, `_record_suppression`, `_update_delivery_summary_counts`). Suppression reason codes are identical across ingestion (Task 6), delivery suppression (Task 9), and rendering (Task 14).
