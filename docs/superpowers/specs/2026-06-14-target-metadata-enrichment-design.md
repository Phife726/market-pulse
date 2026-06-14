# Target Metadata Enrichment Utility — Design

**Date:** 2026-06-14
**Status:** Approved design, pending implementation plan
**Scope decision:** Separate prerequisite PR (Option B) — not part of PR #17, not bundled with the future relevance gate.

## Problem

The future ZoomInfo "relevance gate" will need per-target company-identity metadata
(canonical ZoomInfo name, HQ revenue/employee ranges, primary industry, industries,
HQ country/state) to decide whether a discovered article genuinely concerns the target
company. We must not hand-maintain that metadata for every target. We need a scalable,
reviewable way to populate it from ZoomInfo, while keeping daily ingestion free of any
automatic enrichment — ingestion consumes only checked-in, human-reviewed metadata.

## Findings that shaped the design

1. **There is no relevance gate yet.** Today's ZoomInfo integration
   (`zoominfo_client.py`) is discovery-only: it calls one endpoint (`news/enrich`) for a
   known `zoominfo_company_id`. Downstream relevance/impact filtering is entirely
   LLM-driven (`americhem_impact_score`) and reads none of the firmographic fields above.
   This utility therefore produces metadata for *future* consumption — a genuine
   prerequisite, not a co-change.

2. **The needed ZoomInfo endpoints are net-new and of unverified entitlement.** PR #17
   only proved OAuth + News Enrich + companyId-based news. Populating firmographics needs
   the **Company Enrich** endpoint; resolving targets that lack an ID needs the **Company
   Search** endpoint. Per ZoomInfo's Client Credentials docs, requested scopes must be a
   subset of the DevPortal app's granted scopes and invalid scopes error out — so
   entitlement is **per-endpoint** and must be probed, never assumed from a working token.

3. **`targets.yaml` is hand-curated and comment-rich.** Only entity-mode targets carry
   `zoominfo_company_id` (5 today: Avient, RTP Company, Plastipak, SABIC, BASF). Concept
   groups are Serper-only and out of scope. There are no per-target `exclude_terms` or
   alias fields today.

These force **Option B** (isolated prerequisite PR) and a **companion-file** layout so
enrichment can never touch curated data.

## Scope (Option B)

One new script, one new companion-file format, one new pure module, one new test module,
and a README section. **No changes to the ingestion or delivery hot paths.** The only
edit to existing runtime code is two additive functions in `zoominfo_client.py`.

**PR strategy — standalone prerequisite PR, one milestone only.** This is not bundled with
the ZoomInfo relevance gate. The implementation plan retires exactly one milestone: *create
a safe, reviewable metadata enrichment utility and companion metadata file for future
relevance filtering.*

## Architecture (Approach A: thin online layer + pure transform core)

Mirrors the repo's existing "pure module + I/O seam" pattern
(`suppression_ledger.py`, `daily_intelligence_repo.py`).

| Unit | Responsibility | I/O |
|---|---|---|
| `zoominfo_client.py` (extended) | Two additive functions: `resolve_company(...)`, `enrich_company(company_id)`. Reuse `_resolve_access_token()` and the existing swallow-and-log discipline. | Network only |
| `target_enricher.py` (new, pure) | Transform raw ZoomInfo responses + prior metadata → proposed metadata with status/confidence and conservative helper terms. **Zero network, zero file I/O.** | None |
| `scripts/enrich_targets.py` (new, CLI) | Orchestration: load → resolve → enrich → transform → diff/write. All I/O lives here. | Files + network (via the two modules) |
| `target_metadata.yaml` (new, machine-written companion file) | Reviewed metadata, keyed by entity name. Read by future ingestion; never enriched at ingestion time. | — |

### Client extension (additive, non-breaking)

```python
def resolve_company(*, domain=None, name=None, hq_country=None, hq_state=None) -> Optional[dict]
def enrich_company(company_id: int) -> Optional[dict]
```

Both reuse `_resolve_access_token()`. Both follow the existing contract: never raise;
on 401/403/invalid-scope/5xx/transport/malformed-body, log with `%s` placeholders and
return `None`. Endpoint URLs override-able via env vars
(`ZOOMINFO_ENRICH_ENDPOINT`, `ZOOMINFO_SEARCH_ENDPOINT`) like the existing
`ZOOMINFO_NEWS_ENDPOINT`.

## Data flow

```
scripts/enrich_targets.py
  ├─ load entity targets from targets.yaml      (name, zoominfo_company_id?)
  ├─ load existing target_metadata.yaml         (prior machine fields + curated fields)
  │
  └─ per entity target:
        ┌─ RESOLUTION (only when no zoominfo_company_id), first hit wins:
        │    1. resolve_company(domain=…)                 → confidence high
        │    2. resolve_company(name=…, hq_country/state) → confidence medium
        │    3. resolve_company(name=…)                   → confidence low
        ├─ ENRICHMENT: enrich_company(company_id)          (Company Enrich firmographics)
        └─ TRANSFORM (pure): build_proposed_metadata(target, prior_meta, resolve, enrich)
  │
  ├─ DRY-RUN (default): print unified diff (prior file vs proposed file) to stdout; write nothing
  └─ --write: merge-preserve curated fields, stamp last_refreshed, write target_metadata.yaml
```

A pre-curated `zoominfo_company_id` skips resolution entirely and goes straight to enrichment.

## Status & confidence rules (computed in the pure module)

`zoominfo_metadata_status`:

| status | condition |
|---|---|
| `verified` | Enrich succeeded for a **pre-curated** company ID, OR a `high`-confidence (domain) resolution |
| `needs_review` | Resolved by name+HQ (medium) or name-only (low) — a human must confirm the match |
| `missing` | No ID and resolution returned zero candidates (not an error — just no match) |
| `error` | 401/403/invalid-scope/5xx/transport failure — entitlement or outage |

`zoominfo_metadata_confidence`:

| confidence | source of the company ID |
|---|---|
| `high` | Pre-curated `zoominfo_company_id`, OR unique domain/website resolution |
| `medium` | Exact name + HQ-hint resolution |
| `low` | Name-only resolution, or multiple candidates (top picked, flagged) |

**Invariants:**
- `error` **never aborts the run** and **never overwrites prior good metadata**. On
  `error`, the existing machine block is preserved; only `zoominfo_metadata_status: error`
  and `last_refreshed` are stamped. A transient entitlement blip cannot wipe verified data.
- Entitlement is judged **per endpoint**. A token that works for News but 403s on Enrich
  yields `error` for enrichment and proves nothing about news access.

## Helper-term generation (conservative by default)

**`company_identity_terms`** — pure string operations, no external data:
`{canonical_name, targets.yaml name, de-suffixed canonical}`, deduped case-insensitively.

- Suffix strip list: `Inc, Inc., Corp, Corp., Corporation, LLC, Ltd, Ltd., GmbH, SE, AG, Co, Co., Company, Group, Holdings, plc`.
- **De-suffix guardrail:** emit the de-suffixed form *only if* the result still has **≥2 word tokens OR ≥6 characters**. This prevents reducing a name to a short overloaded acronym.
  - `Avient Corporation` → `Avient` (6 chars) → kept → `["Avient Corporation", "Avient"]`
  - `RTP Company` → `RTP` (3 chars, 1 token) → **suppressed** → `["RTP Company"]`
  - `BASF SE` → `BASF` (4 chars, 1 token) → **suppressed** → `["BASF SE"]`
- **No auto-generated acronyms, tickers, or guessed abbreviations.** Short acronyms (RTP,
  3M, GAF) are exactly where false positives occur (RTP = real-time payments, Research
  Triangle Park, RTP Global, return to player…). They are **human-curated only**, added to
  `manual_aliases`.

**`industry_relevance_terms`** — from a small, checked-in dict in `target_enricher.py`
mapping ZoomInfo `primaryIndustry`/`industries` → curated term lists. Only mapped
industries emit terms; an unmapped industry emits **nothing** and sets `industry_unmapped:
true` so a human can extend the map. Starter map (~8 entries, plastics/chemicals domain):

```python
INDUSTRY_TERM_MAP = {
    "Plastics & Rubber Manufacturing":          ["plastics", "polymer", "resin"],
    "Chemicals Manufacturing":                  ["chemicals", "specialty chemicals"],
    "Plastics Material & Resin Manufacturing":  ["resin", "thermoplastics", "compounding"],
    "Packaging & Containers":                   ["packaging"],
    "Automotive":                               ["automotive", "mobility"],
    "Building Materials":                       ["building materials", "construction"],
    "Paints, Coatings & Adhesives":             ["coatings", "pigments", "masterbatch"],
    "Textiles & Apparel":                       ["fibers", "textiles"],
}
```

**Out of scope for this utility (future relevance gate's job):** combining short acronyms
with context terms (resin, thermoplastic, compounding) to make overloaded names safe. This
utility emits only "boring" identity terms and preserves curated risky ones — it stays
reviewable rather than clever.

## `target_metadata.yaml` schema

Keyed by **entity name** for readability, with an explicit `target_key` field inside each
record as the stable join/migration path if the human-readable key ever changes.

```yaml
# MACHINE-MANAGED by scripts/enrich_targets.py. Edit ONLY the human-curated
# fields (manual_aliases, exclude_terms); the enricher preserves them on re-runs.
version: 1
targets:
  Avient:
    # ── machine-written (overwritten on each --write) ──
    target_key: "Avient"                       # stable join key; mirrors the map key today,
                                               # survives a future rename of the human-readable key
    metadata_record_status: active             # active | orphaned (machine field, not just a comment)
    zoominfo_company_id: 357374413
    canonical_name: "Avient Corporation"
    hq_revenue_range: "$1B - $5B"
    employee_range: "5,000 - 10,000"
    primary_industry: "Plastics & Rubber Manufacturing"
    industries: ["Plastics & Rubber Manufacturing", "Chemicals Manufacturing"]
    hq_country: "United States"
    hq_state: "Ohio"
    company_identity_terms: ["Avient Corporation", "Avient"]
    industry_relevance_terms: ["plastics", "polymer", "resin"]
    industry_unmapped: false
    zoominfo_metadata_status: verified         # verified | needs_review | missing | error
    zoominfo_metadata_confidence: high         # high | medium | low
    zoominfo_metadata_last_refreshed: "2026-06-14"
    # ── human-curated (NEVER overwritten; merge-preserved) ──
    manual_aliases: []        # risky acronyms etc., added by hand (e.g. RTP)
    exclude_terms: []         # manual negative terms
```

**Merge-preserve rule:** on `--write`, read the existing file; for each target overwrite
only the machine block and copy `target_key`, `manual_aliases`, and `exclude_terms` through
verbatim. New targets get a `target_key` equal to the map key and empty curated lists.
Targets present in `targets.yaml` get `metadata_record_status: active`.

**Orphan rule:** a record whose `target_key` no longer matches any active entity in
`targets.yaml` is **flagged-and-kept** — set `metadata_record_status: orphaned` (a real
machine field, the authoritative signal) and optionally append a human-readable `# orphaned`
comment. Records are **never auto-deleted**; a human decides. If an orphaned target later
reappears in `targets.yaml`, the status flips back to `active`.

## CLI

```
python scripts/enrich_targets.py                 # dry-run: unified diff to stdout, writes nothing
python scripts/enrich_targets.py --write         # apply changes to target_metadata.yaml
python scripts/enrich_targets.py --only "Avient" # restrict to one target
python scripts/enrich_targets.py --targets PATH --out PATH   # path overrides
```

- **Dry-run is the default;** `--write` is the only way to mutate the file.
- Online when creds are present; degrades to `error` status per the rules above when an
  endpoint is unentitled or down. The run never crashes on an API failure.

## Testing (mocked only — no live ZoomInfo)

Follows the house style in `tests/test_zoominfo.py` (`monkeypatch`, `MagicMock` responses,
token-cache reset fixture).

- **Pure `target_enricher.py`** — the bulk of coverage, no mocking needed:
  - status/confidence matrix (pre-curated, domain, name+HQ, name-only, zero-candidate, error)
  - de-suffix guardrail (Avient kept; RTP/BASF suppressed)
  - `company_identity_terms` dedup
  - `industry_relevance_terms` mapping + unmapped→empty+flag
  - merge-preserve keeps `target_key`/`manual_aliases`/`exclude_terms`
  - orphan flagging: a record missing from `targets.yaml` gets
    `metadata_record_status: orphaned` and is kept (asserted on the field, not a comment);
    reappearing target flips back to `active`
  - `error` preserves prior machine block
- **Client extension** — `resolve_company`/`enrich_company` with mocked `requests`:
  happy path, 401/403→`None`, invalid-scope→`None`, 5xx→`None`, malformed body→`None`,
  token reuse.
- **CLI** — dry-run prints a diff and writes nothing; `--write` writes; mocked client.

## Documentation

README section: purpose, the per-endpoint entitlement caveat, dry-run/`--write` usage, the
"daily ingestion consumes reviewed metadata only — never enriches" boundary, and how to add
curated `manual_aliases`/`exclude_terms` and extend `INDUSTRY_TERM_MAP`.

## Out of scope

- The relevance gate itself (separate future work).
- Any ingestion/delivery behavior change.
- Enrichment of concept-mode groups.
- Auto-curation of acronym aliases.
```
