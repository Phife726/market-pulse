# Macroeconomic Outlook and Additional Articles Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Uncap the main report at the visible threshold, add a capped score-4/5 optional-discovery appendix, and add a conditional source-grounded Macroeconomic Outlook — through two independently reviewable PRs.

**Architecture:** Extend the pure `report.py` assembly seam and `ReportModel` for optional caps and appendix selection, then render in `delivery_engine.py`. Separately extend configured discovery targets and the existing prompt/LLM seams for structured macro synthesis, preserving current dedupe, repository, and delivery behavior.

**Design:** `docs/superpowers/specs/2026-07-16-macro-outlook-additional-articles-design.md` (the amended design is authoritative where this plan is ambiguous).

**Tech Stack:** Python, pytest, dataclasses, YAML configuration, Supabase repository seam, OpenAI JSON prompt seam, HTML email rendering, GitHub Actions.

---

## PR 1: Uncapped report + Additional Articles to Explore

### Task 1: Make the visible-report caps optional and uncapped by default

**Files:**
- Modify: `report.py` (cap resolution + steps 4–5 of `assemble_report`)
- Modify: `market_pulse_config.yaml`
- Test: `tests/test_pipeline.py`

**Step 1: Write the failing tests**

Construct rows with dict literals, no mocks:

- With `max_visible_articles_per_segment: null` and
  `max_total_visible_articles: null`, five score-7 rows in one segment all
  appear in that group (no per-segment drop) and fifteen visible rows across
  segments all survive (no total drop).
- With an integer cap in config, capping still enforces exactly as today
  (knob retained).
- With `config=None` (built-in defaults), the report is uncapped.

**Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -k "uncapped or cap" -v`

Expected: FAIL — `_config_int` coerces `null` to the warning path and the
built-in defaults are 3/12.

**Step 3: Implement**

Add an optional-int config reader in `report.py` (absent key or `null` →
`None` → skip the cap; integer → cap as today). Change the built-in defaults
for both caps to `None`. Guard steps 4 and 5 of `assemble_report` on the cap
being non-None. Update the `ReportModel` docstring invariant: caps hold when
configured.

In `market_pulse_config.yaml`, set both keys to `null` with a comment
explaining that an integer re-imposes the cap (config-only rollback if the
report gets noisy).

**Step 4: Run the tests to verify they pass**

Run: `pytest tests/test_pipeline.py -k "uncapped or cap" -v` then `pytest tests/`

Expected: PASS (fix any existing tests that assumed default caps of 3/12).

**Step 5: Commit**

```bash
git add report.py market_pulse_config.yaml tests/test_pipeline.py
git commit -m "feat(report): show all visible articles by default; caps become optional"
```

### Task 2: Appendix config and report-model field

**Files:**
- Modify: `market_pulse_config.yaml`
- Modify: `report.py` (`ReportModel`)
- Test: `tests/test_pipeline.py`

**Step 1: Write the failing tests**

- `assemble_report(rows, config=config).additional_articles` exists and is a
  tuple; empty for the `no_news` variant.
- The cap resolves to 10 from `reporting.max_additional_articles` via the
  report-level reader (this is a report-assembly knob, NOT a scoring
  threshold — the test lives in the report tests, not `tests/test_scoring.py`).

**Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -k "additional_articles" -v`

Expected: FAIL — `ReportModel` lacks the field.

**Step 3: Implement minimal**

```yaml
reporting:
  max_additional_articles: 10
```

```python
additional_articles: tuple[dict, ...] = ()
```

Populate as empty everywhere for now (selection comes in Task 3). Keep
`supporting_impact_threshold: 4` and `visible_impact_threshold: 6` unchanged.

**Step 4: Run the tests**

Run: `pytest tests/test_pipeline.py -k "additional_articles" -v`

Expected: field/config assertions PASS; selection assertions still FAIL.

**Step 5: Commit**

```bash
git add market_pulse_config.yaml report.py tests/test_pipeline.py
git commit -m "refactor(report): add additional-articles model field and cap config"
```

### Task 3: Deterministic appendix eligibility and ranking

**Files:**
- Modify: `report.py`
- Test: `tests/test_pipeline.py`

**Step 1: Write failing behavior tests**

Cover (dict literals, no mocks):

- eligible scores 4 and 5; score 3 excluded; score 6 stays in main groups and
  never duplicates into the appendix
- every score-5 item precedes every score-4 item
- blank headline or blank source URL excluded
- delivery-suppressed rows excluded (e.g. product-listing URL)
- **pinned deliberate consequence**: an Enterprise / Cross-Segment score-5 row
  is suppressed by rule 1 (`enterprise_min_impact` 7) and therefore never
  reaches the appendix
- maximum `max_additional_articles` (10)
- deterministic ordering: recency (`published_at` when parseable, else
  `created_at`) inside a score band, then normalized headline, then `url_hash`

Do NOT write tests for scrape failures, LLM discards, or ingestion
duplicates — those rows never reach `daily_intelligence`, so the conditions
are vacuous at report level.

**Step 2: Run tests to verify failure**

Run: `pytest tests/test_pipeline.py -k "additional_articles" -v`

Expected: FAIL — selection not implemented.

**Step 3: Implement minimal pure helpers**

Eligibility reuses the one existing band definition — no re-implemented
threshold comparison:

```python
def _is_usable_additional_article(row: dict, scorer: Scoring) -> bool:
    return (
        scorer.is_weak_relevance(row)
        and bool((row.get("headline") or "").strip())
        and bool((row.get("source_url") or "").strip())
    )
```

Sort key (no segment priority — config key order exists for prompt stability,
not ranking):

1. negative effective impact
2. negative recency timestamp (`published_at` when parseable, else
   `created_at`; unparseable → epoch 0)
3. normalized headline
4. `url_hash`

Select only from rows that survived `_apply_delivery_suppression`, exclude
`url_hash`es present in the final groups, cap at `max_additional_articles`,
store as a tuple.

**Step 4: Run focused tests**

Run: `pytest tests/test_pipeline.py -k "additional_articles" -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add report.py tests/test_pipeline.py
git commit -m "feat(report): select near-threshold additional articles"
```

### Task 4: Suppression accounting semantics

**Files:**
- Modify: `report.py` (weak-relevance accounting)
- Test: `tests/test_pipeline.py`

**Step 1: Write failing tests**

- Appendix-displayed score-4/5 items are NOT counted as `weak_relevance`.
- Eligible items pushed out by the 10-item cap ARE counted.
- `below_impact_threshold` is UNCHANGED: it still counts every
  suppression-surviving row below the visible threshold, including
  appendix-displayed rows (it describes the visible-card decision; the
  overlap is deliberate and documented).

**Step 2: Run tests**

Run: `pytest tests/test_pipeline.py -k "weak_relevance" -v`

Expected: FAIL under current accounting.

**Step 3: Implement minimal correction**

Union the final-group hashes with the appendix hashes; count `weak_relevance`
only for qualifying rows in neither set. Add a comment stating the
`below_impact_threshold` overlap is intentional.

**Step 4: Run tests**

Run: `pytest tests/test_pipeline.py -k "weak_relevance or additional_articles" -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add report.py tests/test_pipeline.py
git commit -m "fix(report): align weak relevance with appendix visibility"
```

### Task 5: Render the compact appendix

**Files:**
- Modify: `delivery_engine.py` (new `_render_additional_articles_section`, wired into `render_report`)
- Test: `tests/test_pipeline.py`

**Step 1: Write failing renderer tests**

- section title "Additional Articles to Explore" appears when items exist;
  absent when `additional_articles` is empty
- each row renders linked headline, segment, impact score, and domain
- publication date renders only when `published_at` is present (never the
  scrape timestamp)
- the `americhem_impact` narrative does NOT render in appendix rows
- headline/segment/domain are HTML-escaped; a `javascript:` source URL is
  neutralized (href passes through `_safe_http_url`)
- section sits below Commercial Segment Watch and above the Sources footer

**Step 2: Run tests**

Run: `pytest tests/test_pipeline.py -k "additional_articles and render" -v`

Expected: FAIL — renderer does not exist.

**Step 3: Implement renderer**

Add `_render_additional_articles_section(items)` using email-safe inline
styles and the existing brand constants; insert its output in `render_report`
between the segment-watch section and the sources/QA/footer content. Escape
all untrusted values and guard hrefs with `_safe_http_url` (deliberately
stricter than the legacy `_render_card`).

**Step 4: Run focused tests**

Run: `pytest tests/test_pipeline.py -k "additional_articles or render_report" -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat(delivery): render additional articles appendix"
```

### Task 6: PR 1 docs and regression verification

**Files:**
- Modify: `CLAUDE.md`, `CONTEXT.md`

**Step 1: Update architecture documentation**

Document: optional caps (`null` = uncapped, the new default),
`ReportModel.additional_articles`, appendix cap semantics, `surfaced_count`
excluding appendix items, the `below_impact_threshold`/`weak_relevance`
accounting split, and the Enterprise / Cross-Segment appendix consequence.

**Step 2: Run all tests**

Run: `pytest tests/`

Expected: PASS.

**Step 3: Run prompt dump smoke check**

Run: `python scripts/show_prompts.py > /tmp/market-pulse-prompts.txt`

Expected: exit 0 with no prompt changes (PR 1 touches no prompts).

**Step 4: Review diff**

Run: `git diff --check && git status --short`

Expected: no whitespace errors and only intended files changed.

**Step 5: Commit docs**

```bash
git add CLAUDE.md CONTEXT.md
git commit -m "docs: document uncapped report and additional-articles appendix"
```

Open PR 1 with scope explicitly limited to report shape (uncap + optional
discovery). Defer all macro-search changes.

---

## PR 2: Macroeconomic Outlook

Begin only after PR 1 is merged or rebased cleanly.

### Task 7: Restructure macro discovery targets

**Files:**
- Modify: `targets.yaml`
- Test: `tests/test_pipeline.py` (target-loading tests)

**Step 1: Write failing target-loading tests**

- Active macro concept groups exist for: manufacturing/industrial production,
  construction, automotive production, consumer demand/durable goods,
  inflation/rates/credit, energy/freight/feedstocks, business investment.
- The old generic `economic` group is GONE (its terms are subsumed — do not
  run both).
- The macro groups are the LAST groups in file order (targets process in file
  order, so deadline-limited runs sacrifice macro before entity coverage —
  pin this deliberately).

**Step 2: Run tests**

Run: `pytest tests/ -k "targets and macro" -v`

Expected: FAIL.

**Step 3: Implement**

Replace `economic` with the dedicated concept groups at the bottom of
`targets.yaml`, using the current concept-target schema and controlled query
counts (reuse the existing `exclude_any` hygiene terms). Add a file comment
explaining the ordering contract.

**Step 4: Run tests**

Run: `pytest tests/ -k "targets and macro" -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add targets.yaml tests/
git commit -m "feat(discovery): restructure economic group into dedicated macro targets"
```

### Task 8: Macro-outlook prompt contract

**Files:**
- Modify: `prompts.py`
- Test: `tests/test_prompts.py`

**Step 1: Write failing prompt-contract tests**

Pin the JSON shape:

```json
{
  "current_condition": "...",
  "signals": [
    {
      "indicator": "Manufacturing PMI",
      "direction": "Declining",
      "americhem_implication": "...",
      "affected_segments": ["Industrial"],
      "citation_source_ids": [1]
    }
  ]
}
```

Contract tests (no fakes, no patching — same style as the existing
`test_prompts.py`):

- the prompt promises `direction` values from a new `VALID_MACRO_DIRECTIONS`
  enum owned in `prompts.py` (e.g. Rising | Stable | Declining)
- the prompt promises `affected_segments` values that are EXACTLY the
  canonical `insight.VALID_COMMERCIAL_SEGMENTS` labels
  ("Transportation - Automotive", not "Automotive"; construction framing goes
  in the implication text — "Building & Construction" is not a segment)
- the prompt requires at least one citation per signal and excludes generic
  macro commentary without a demand/cost/capacity/margin/segment implication
- `_rank_macro_articles` reserves a quota for `signal_type == "Macro"` rows
  (up to 10 of the `MAX_MACRO_SUMMARY_SOURCE_PACK_ARTICLES` slots when
  present) so macro articles survive into the citable pack on heavy days;
  digest `[n]` markers and pack ids still come from one enumeration

**Step 2: Run tests**

Run: `pytest tests/test_prompts.py -k "macro" -v`

Expected: FAIL.

**Step 3: Implement**

Extend `macro_prompt` (one LLM call — one source pack, one enumeration, one
failure path). Add `VALID_MACRO_DIRECTIONS` beside `VALID_MACRO_CONDITIONS`.
Implement the quota inside `_rank_macro_articles` deterministically.

**Step 4: Run prompt tests**

Run: `pytest tests/test_prompts.py -k "macro" -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add prompts.py tests/test_prompts.py
git commit -m "feat(prompts): define macro outlook contract"
```

### Task 9: Validate and persist macro-outlook output

**Files:**
- Modify: `ingestion_engine.py` (`generate_macro_summary` + a `_validate_macro_outlook` helper)
- Modify: `schema.sql`
- Create: `migrations/005_add_macro_outlook.sql` (idempotent)
- Test: `tests/test_pipeline.py`, repository seam tests

**Step 1: Write failing validation tests**

- valid material signal accepted
- signal with EMPTY `citation_source_ids` dropped (the deterministic
  materiality gate — source grounding is structural)
- out-of-range citation id cleaned; a signal whose citations all clean away
  is dropped
- blank `indicator` or `americhem_implication` rejected
- `direction` outside `VALID_MACRO_DIRECTIONS` rejected
- `affected_segments` entry outside `VALID_COMMERCIAL_SEGMENTS` rejected;
  empty list rejected
- zero surviving signals persists `None`/empty — no section
- malformed `macro_outlook` key degrades to `None` while executive bullets
  survive (per-key validation)
- LLM `None` degrades exactly as today

**Step 2: Write failing persistence tests**

- the structured outlook round-trips through `InMemoryIntelligenceRepo`
- `executive_sources` packs the UNION of sources cited by surviving bullets
  and surviving macro signals, so every rendered citation id resolves

**Step 3: Run tests**

Run: `pytest tests/ -k "macro_outlook or macro_summary" -v`

Expected: FAIL.

**Step 4: Add schema and migration**

`005_add_macro_outlook.sql`: nullable `macro_outlook` JSONB on
`daily_summaries`, idempotent; update `schema.sql` for fresh installs. State
in the migration header: **required, not flag-gated — apply before deploying
this code or `upsert_summary` crashes ingestion** (same rollout contract as
migration 004).

**Step 5: Implement validation and persistence**

Validation lives at the domain caller (consistent with the LLM seam
contract), importing the enums from `prompts.py` and `insight.py` — prompt
promises and validator checks stay one definition.

**Step 6: Run focused tests**

Run: `pytest tests/ -k "macro_outlook or macro_summary" -v`

Expected: PASS.

**Step 7: Commit**

```bash
git add ingestion_engine.py schema.sql migrations/ tests/
git commit -m "feat(ingestion): persist validated macro outlook"
```

### Task 10: Carry macro outlook through the report model

**Files:**
- Modify: `report.py`
- Test: `tests/test_pipeline.py`

**Step 1: Write failing tests**

- `ReportModel` carries validated macro-outlook data from `macro_summary`
- malformed, empty, or missing data becomes `None`
- the `no_news` variant carries `None`

**Step 2: Run tests**

Run: `pytest tests/test_pipeline.py -k "macro_outlook and report" -v`

Expected: FAIL.

**Step 3: Implement**

Add a nullable immutable field extracted in `assemble_report`. No LLM calls
or database access in `report.py` — purity preserved.

**Step 4: Run tests**

Run: `pytest tests/test_pipeline.py -k "macro_outlook and report" -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add report.py tests/test_pipeline.py
git commit -m "feat(report): carry macro outlook data"
```

### Task 11: Render the conditional macro section

**Files:**
- Modify: `delivery_engine.py`
- Test: `tests/test_pipeline.py`

**Step 1: Write failing renderer tests**

- section appears between executive summary and Commercial Segment Watch
- section absent for `None` or zero signals
- current condition renders once; each signal renders indicator, direction,
  implication, affected segments
- citation display numbering is ONE space across the whole email: the display
  map enumerates executive bullets THEN macro signals, and the bottom Sources
  list covers both (extend `_citation_display_map` / callers)
- a card-suppressed macro article (generic-market-report pattern, empty
  entities) can still be cited by the outlook — pin the independence of card
  visibility and outlook citation
- all untrusted text escaped; hrefs guarded

**Step 2: Run tests**

Run: `pytest tests/test_pipeline.py -k "macro_outlook and render" -v`

Expected: FAIL.

**Step 3: Implement `_render_macro_outlook_section`**

Compact email-safe HTML. Do not repeat article cards or generic prose.

**Step 4: Run tests**

Run: `pytest tests/test_pipeline.py -k "macro_outlook or render_report" -v`

Expected: PASS.

**Step 5: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat(delivery): render macroeconomic outlook"
```

### Task 12: Graceful degradation and runtime controls

**Files:**
- Test: `tests/test_pipeline.py`

**Step 1: Add failure-path tests**

- macro provider returns no URLs → normal report unaffected
- scrape failure for all macro candidates → normal report unaffected
- LLM returns `None` → no outlook, bullets path unchanged
- invalid structured response → no outlook, bullets survive
- pipeline deadline reached before macro targets complete →
  `generate_macro_summary` still runs on whatever was stored (existing
  deadline behavior) and the outlook degrades to fewer/zero signals
- test-mode production-row fallback carries `macro_outlook` (same row)

**Step 2: Run focused tests**

Run: `pytest tests/test_pipeline.py -k "macro and degrade" -v`

Expected: PASS after any minimal fixes.

**Step 3: Run all tests**

Run: `pytest tests/`

Expected: PASS.

**Step 4: Run static repository checks**

Run: `git diff --check && python -m compileall -q .`

Expected: exit 0.

**Step 5: Commit**

```bash
git add tests/ ingestion_engine.py delivery_engine.py
git commit -m "test: verify macro outlook degradation"
```

### Task 13: Documentation and PR 2

**Files:**
- Modify: `CLAUDE.md`, `CONTEXT.md`
- Modify: `.env.example` only if a feature flag is introduced

**Step 1: Document the architecture**

Explain: dedicated macro targets (last in file order, deadline-sacrificed
first), the two-layer materiality gate (prompt + mandatory citations), the
structured output and its enums, the macro source-pack quota, the
executive_sources union, migration 005's required-before-deploy rollout
order, rendering location, and failure behavior.

**Step 2: Run final verification**

```bash
pytest tests/
python scripts/show_prompts.py > /tmp/market-pulse-prompts.txt
git diff --check
git status --short
```

Expected: all tests pass, prompt dump succeeds (macro prompt changes are
intended — diff them), no whitespace errors, only intended changes remain.

**Step 3: Commit**

```bash
git add CLAUDE.md CONTEXT.md .env.example
git commit -m "docs: document macroeconomic outlook"
```

**Step 4: Open PR 2**

State explicitly that this PR changes discovery and production intelligence
behavior, that migration 005 must be applied before merge-deploy, and that
recipient-specific personalization and web archives remain deferred.
