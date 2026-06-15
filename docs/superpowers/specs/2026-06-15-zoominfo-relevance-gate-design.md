# ZoomInfo Metadata-Backed Relevance Gate — Design

**Date:** 2026-06-15
**Status:** Approved design, pending implementation plan
**Scope decision:** Standalone PR consuming the reviewed `target_metadata.yaml` shipped in PR #25. Adds one new pure module plus narrow wiring; no ingestion/delivery refactor, no ZoomInfo request-shape change.

## Problem

ZoomInfo News Enrich returns company news keyed by `zoominfo_company_id`, so each
candidate is *already linked* to a target company. But the linkage is occasionally
loose for overloaded tokens — most acutely "RTP", which ZoomInfo may associate with
real-time-payments / casino / Research-Triangle-Park articles that have nothing to do
with RTP Company (the plastics compounder). Today the only relevance filter is the
downstream LLM (`americhem_impact_score`), which runs *after* the expensive Firecrawl
scrape and OpenAI synthesis. We want to drop obvious company-mismatch candidates
**before** that spend, using the reviewed, human-curated `target_metadata.yaml`.

## Findings that shaped the design

1. **ZoomInfo candidates are pre-linked by company id.** `discover_zoominfo_candidates()`
   returns provider-neutral candidates carrying `title`, `description`,
   `source_publication`, `categories`, and `zoominfo_company_id`. Because the company
   association already exists, the gate must be a *targeted false-positive suppressor*,
   **not** a second entity resolver. Absence of identity text alone must never drop a
   candidate.

2. **The candidate loop has a clear pre-scrape seam.** In `execute_pipeline()`
   (`ingestion_engine.py`), each candidate flows: duplicate-URL check → semantic-duplicate
   check → `scrapes_attempted += 1` → `scrape_article()` → `synthesize_insight()` → store.
   The gate belongs between the semantic-duplicate check and `scrapes_attempted += 1`, so a
   drop skips all paid work.

3. **Metadata keys equal target names.** `target_metadata.yaml` is keyed by the
   `targets.yaml` entity name (Avient, BASF, SABIC, RTP Company, Plastipak), which is
   `target["name"]` in the loop. The join is a direct dict lookup — no resolution needed.

4. **Only RTP carries `exclude_terms` today.** Avient/BASF/SABIC/Plastipak have empty
   `exclude_terms`, so the gate can only ever drop RTP candidates with the current metadata.
   The mechanism is general; the curated data scopes its effect.

5. **The repo gates new behavior behind default-off flags** (`ZOOMINFO_NEWS_ENABLED`,
   `STORE_DISCOVERY_METADATA`) and validates in the Jason-only test pipeline before flipping
   production. The gate follows the same pattern.

## Scope

One new pure module (`relevance_gate.py`), one new test module
(`tests/test_relevance_gate.py`), and narrow additive edits to `ingestion_engine.py` and
`suppression_ledger.py`, plus a one-line code-count note in CLAUDE.md. **No delivery
changes. No ZoomInfo request-shape changes. No concept groups. The dry-run workflow is
retained** until the gate has been validated once.

## Design

### 1. `relevance_gate.py` — pure module (zero network/file I/O in the core)

Mirrors `target_enricher.py` / `suppression_ledger.py`: deterministic and fully
unit-testable.

- `load_target_metadata(path="target_metadata.yaml") -> dict[str, dict]`
  Thin loader returning `{target_key: record}`. **Reads swallow exceptions and return
  `{}`** (missing/unparseable file silently disables the gate; ingestion never crashes),
  matching the repo's read-swallow philosophy.

- `evaluate(*, title: str, description: str, record: dict) -> GateDecision`
  Pure. `GateDecision` is a frozen dataclass: `drop: bool`, `reason: Optional[str]`,
  `matched_exclude: Optional[str]`, `matched_identity: Optional[str]`.

**Drop logic (exclude-hit + identity rescue):**

1. `text = f"{title} {description}"`.
2. `identity_terms = [canonical_name] + company_identity_terms + manual_aliases`
   (non-empty, deduped). If **any** identity term matches `text` → **keep**
   (`drop=False`, `matched_identity` set). Rescue wins even when an exclude term is also
   present.
3. Else if **any** `exclude_term` matches `text` → **drop**
   (`reason="zoominfo_company_mismatch"`, `matched_exclude` set).
4. Else → **keep**. Absence of identity text alone never drops.

**Matching semantics:** case-insensitive, word-boundary-aware. A term matches via regex
`\b` + `re.escape(term)` (internal whitespace runs replaced with `\s+`) + `\b`, so
`slots` does not match inside a larger word and `RTP Co` does not match `RTP Cox`
(while the distinct term `RTP Company` still matches `RTP Company`). Empty/missing term
lists contribute no matches.

### 2. Wiring in `ingestion_engine.py` (no broad refactor)

- Add `_relevance_gate_enabled()` reading `ZOOMINFO_RELEVANCE_GATE_ENABLED` via the
  existing `_TRUTHY_ENV_VALUES` set. **Default off.**
- In `execute_pipeline()`, after `load_targets()`, load the metadata once but only when
  the flag is on: `target_metadata = relevance_gate.load_target_metadata(...)` (empty
  dict otherwise — no file dependency when disabled).
- In the candidate loop, immediately **after the semantic-duplicate `continue` and before
  `scrapes_attempted += 1`**, apply the gate only when: `provider == "zoominfo"` **and**
  the flag is on **and** a metadata record exists for `entity_name` with
  `metadata_record_status == "active"`. On `decision.drop`:
  - `logger.info("RELEVANCE_GATE drop (%s): exclude=%r no identity rescue | %s", provider, decision.matched_exclude, normalized)`
  - `_bump(provider, "relevance_dropped")`
  - `suppression_ledger = suppression_ledger.record("zoominfo_company_mismatch", url=raw_url, title=candidate_title)`
  - `continue` (skips scrape/synthesis/store).

  A missing record, non-active record, Serper candidate, or flag-off path is a no-op
  (keep).

### 3. Counters / reason code

- `suppression_ledger.py`: append `("zoominfo_company_mismatch", "ZoomInfo company
  mismatch")` to `_INGESTION_REASONS` (now 5 ingestion-owned codes). It is ingestion-owned;
  a delivery ledger calling `record()` with it must still raise.
- `ingestion_engine.py`: add a `relevance_dropped` key to `_new_provider_yield()` and the
  `_log_provider_yield()` format string, so gate drops are visible distinctly from
  `discards` (LLM) and `duplicates`.
- CLAUDE.md: update the "4 ingestion-owned" count to 5.

### 4. Rollout & validation

- `ZOOMINFO_RELEVANCE_GATE_ENABLED=false` by default. When false: no drops, production
  behavior unchanged.
- Validation path: merge flag-off → run tests → enable only in the test pipeline
  (`ZOOMINFO_RELEVANCE_GATE_ENABLED=true`, `MARKET_PULSE_RUN_MODE=test`) → inspect
  `provider_yield` counters, suppression-ledger counts, and `RELEVANCE_GATE` log lines →
  if RTP false positives drop and no good candidates are suppressed, enable in the
  production scheduled workflow via a **separate small config/workflow-var PR**.
- **No full shadow mode** in this PR — the rule is narrow enough that flag-off plus a
  test-run validation pass is sufficient. The gate is never on in production by default.

## Testing

- `tests/test_relevance_gate.py` (pure):
  - Each RTP exclude term (`real-time payments`, `RTP Network`, `return to player`,
    `casino`, `slots`, `Research Triangle Park`, `RTP Global`, `Rain Tree Photonics`)
    drops when no identity rescue is present.
  - `RTP Company` / `RTP Co` in the text rescues (keep) even when an exclude term is also
    present.
  - No exclude term → keep, even with no identity text.
  - Record with empty `exclude_terms` (e.g. Avient) → never drops.
  - Case-insensitivity; word-boundary (a substring inside a larger word does not
    false-match; `Research Triangle Park` as a phrase does).
  - `load_target_metadata` on a missing/bad path returns `{}` (gate disabled, no raise).
- `tests/test_pipeline.py` (or the ZoomInfo test module): a Serper candidate is never
  gated; flag-off is a no-op; flag-on drops a planted RTP false-positive candidate before
  `scrape_article` is called (assert scrape not invoked / suppression recorded).
- `suppression_ledger` test: `zoominfo_company_mismatch` is ingestion-owned — a delivery
  ledger `record()` with it raises `ValueError`.

## Guardrails

- No ingestion/delivery broad refactor; edits are additive and localized.
- No ZoomInfo request-shape changes.
- No relevance logic applied to Serper candidates or concept groups.
- Gate off in production by default; validated in the test pipeline first.
- Dry-run metadata workflow retained until the gate is validated once.

## Follow-up

A separate small config/workflow-var PR flips `ZOOMINFO_RELEVANCE_GATE_ENABLED=true` in
the production scheduled workflow after the test-pipeline validation pass. Removal of the
metadata dry-run workflow is deferred until after that validation.
