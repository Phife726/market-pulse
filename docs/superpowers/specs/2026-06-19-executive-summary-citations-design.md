# Executive Summary Source Citations — Design

**Date:** 2026-06-19
**Status:** Approved (pending spec review)
**Scope:** Standalone PR. Product-facing behavior change + schema change.

## Objective

Add validated, persistent, reader-facing citations to the executive summary
bullets. Each bullet ends with a grouped numbered citation (e.g. `[1, 2, 3]`)
where each number links to the supporting article's URL; a "Sources" footer
lists every cited source as `[n] headline — domain`. Numbers are sequential
across the summary. Citations are **validated LLM citations**: the model cites
source IDs from a numbered pack it is shown, and code enforces that every
rendered citation maps to a real source.

The guiding principle is **no fake precision**: validated citations only,
deterministic source selection, and clean fallback (no citation UI) when there
are no valid citations.

## Current State (verified against code)

- The executive summary is **generated in ingestion** (`ingestion_engine.generate_macro_summary`)
  and **rendered in delivery** (`delivery_engine._render_exec_summary` /
  `_render_executive_bullets`). Citations must therefore survive the DB
  round-trip via the `daily_summaries` row.
- `generate_macro_summary(articles)` today builds a digest of
  `headline + impact + score` and **strips source URLs** before the LLM call.
  There is no link between any summary statement and the articles behind it.
- `executive_bullets` is already a list of `{label, body}` objects (not plain
  strings), validated by `_validate_executive_bullets()` (exactly 3, fixed
  label order, non-empty body). Adding citations is **additive** to that shape.
- `daily_summaries` columns relevant here: `executive_summary` (legacy prose),
  `executive_bullets` (jsonb), `dominant_condition`, `macro_sentiment`.
- `daily_intelligence` sortable fields: `americhem_impact_score` (materiality,
  nullable), `sentiment_score` (fallback materiality), `created_at` (recency),
  `headline`, `url_hash`, `source_url`, `commercial_segment`. There is **no**
  `published_at` column.
- Existing migrations go up to `003_add_discovery_metadata.sql`, so the new
  migration is `004`.

## Decisions

| Decision | Choice |
| --- | --- |
| Source attribution | Validated LLM citations (model cites pack IDs; code validates) |
| Display numbering | Sequential, renumbered by **first appearance** across bullets |
| Inline format | Grouped: `[1, 2, 3]` (brackets/commas plain text; each number a link) |
| Footer | `[n] headline — domain`, linked, in display order |
| Footer entry detail | Number + headline + domain |
| Zero-citation case | **Omit all citation UI** (no inline numbers, no footer). No "Sources reviewed" line. |
| Source-pack cap | Tunable, default **40**, deterministic selection by materiality |
| Citation field name | `citation_source_ids` (not generic `source_ids`) |
| Citation IDs | Internal digest IDs only — not durable identifiers |
| Hover tooltip | Best-effort `title=URL` only; footer is the real traceability path |

## Constants

```python
MAX_MACRO_SUMMARY_SOURCE_PACK_ARTICLES = 40   # cap on citable articles
MAX_EXECUTIVE_BULLET_CITATIONS = 3            # cap per bullet after dedupe
```

## Data Flow

```
ingestion_engine.generate_macro_summary(articles)
  1. Build the source pack (deterministic order, capped):
       sort articles by materiality desc, created_at desc, headline asc, url_hash asc
       take first MAX_MACRO_SUMMARY_SOURCE_PACK_ARTICLES
       assign id = 1..N
       pack entry = {id, headline, url, domain, segment, score}
  2. Digest lines are prefixed with their pack id: "[3] [CATEGORY] headline (Impact 8/10): ..."
  3. LLM returns bullets: {label, body, citation_source_ids:[int]}
  4. Validate (see below) -> bullets carry only cleaned citation_source_ids
  5. executive_sources = pack entries whose id was cited by >=1 surviving bullet
  6. upsert_summary(executive_bullets, executive_sources, ...)

delivery_engine._render_exec_summary
  7. Build display_map: raw cited id -> 1..N by order of first appearance across bullets
  8. Inline citation after each body: [d1, d2, ...] where each di links to its URL
  9. "Sources" footer: [n] headline — domain, linked, ordered by display number
 10. Zero valid citations across the whole summary -> render bullets exactly as today, no footer
```

## Component Changes

### `ingestion_engine.py`

- **Source pack builder** (new helper, e.g. `_build_macro_source_pack(articles)`):
  - Deterministic sort: `effective_impact(row)` desc, `created_at` desc,
    `headline` asc, `url_hash` asc. Reuse `insight.effective_impact` for
    materiality (handles the `americhem_impact_score` → `sentiment_score`
    fallback). Define an explicit, stable tie-breaker so the **same article set
    always yields the same pack IDs** — this is a tested invariant.
  - Cap at `MAX_MACRO_SUMMARY_SOURCE_PACK_ARTICLES`.
  - Each entry: `{id, headline, url, domain, segment, score}`.
- **Digest**: prefix each line with `[id]` matching the pack so the LLM cites
  the same numbers it sees.
- **Prompt**: instruct the model to add `citation_source_ids` to each bullet —
  1–3 ids drawn from the digest line numbers that support the statement. A
  **Low Signal** bullet (and any bullet with no supporting source) uses `[]`.
- **`_validate_executive_bullets()`** (extended): keep the existing 3-bullet,
  fixed-label-order, non-empty-body checks. Additionally, for each bullet:
  - coerce `citation_source_ids` to a list; keep only `int`s present in the
    pack id set; dedupe preserving order; truncate to
    `MAX_EXECUTIVE_BULLET_CITATIONS`.
  - missing / wrong-type / all-invalid → `[]`.
  - **Only cleaned ids are stored.** Never persist raw invalid ids
    (`[3, 999, "abc"]` → `[3]`).

### Storage — `schema.sql` + `migrations/004_add_executive_sources.sql`

- New column on `daily_summaries`: `executive_sources jsonb`.
- Each cited source persisted as
  `{id, headline, url, domain, segment, score}` (richer than render needs, for
  auditability of what the model was allowed to cite).
- `executive_bullets` keeps its shape; the added `citation_source_ids` lives
  inside each bullet object (jsonb — no DDL needed for that part).
- `schema.sql` updated for fresh-DB initialization; migration `004` for
  existing DBs (`add column if not exists executive_sources jsonb`).
- The selection policy/constants are **not** persisted as a column in this PR
  (deferred); they live in code with clear names and tests.

### `daily_intelligence_repo.py`

- Add `executive_sources` to the `fetch_latest_summary` select list.
- `upsert_summary` passes the new key through (dict-based; just include it).

### `delivery_engine.py`

- `_render_executive_bullets(bullets, sources)` gains the sources map and:
  - builds `display_map` (raw id → sequential display number by first
    appearance across bullets in order),
  - appends a grouped inline citation `[d1, d2, d3]` after each body, where each
    number is `<a href=URL title=URL>d</a>` styled small/superscript navy to
    match the serif block. Brackets and commas are plain text.
  - a bullet with no valid ids gets **no** inline citation.
- New `_render_sources_footer(sources, display_map)`:
  - one row per cited source, `[n] <headline> — <domain>`, the whole entry
    linked to the URL, ordered by display number.
  - omitted entirely when there are no cited sources.
- **URL safety** (new): before rendering any `href`, validate the scheme — allow
  only `http://` and `https://`; anything else is dropped/neutralized (no link,
  fall back to plain text). All headline + URL strings pass through
  `html.escape`.
- **Domain extraction**: `urlparse(url).netloc.lower()` minus a leading `www.`.
  Empty/malformed → graceful fallback, e.g. `[n] Headline unavailable — source link`.
  Rendering must never crash on a bad URL.

## Backward Compatibility

Delivery must render legacy summaries exactly as today (no citation UI) for all
of these states:

- `executive_sources` missing / `null` / `[]`
- `executive_bullets` are old-style strings
- `executive_bullets` are objects without `citation_source_ids`
- legacy prose path (`executive_summary` text, no bullets)

In every legacy case: render the summary as it renders today, with no inline
numbers and no footer.

## Fallback Policy (honest, no fabrication)

- Per-bullet: citations render only where valid ids exist. Mixed states are
  fine (bullet 1 + 3 cited, bullet 2 not) — footer lists only cited sources.
- Whole summary with zero valid citations: no inline numbers, no footer.
- Never synthesize heuristic citations to fill gaps.

## Testing

**Validation (ingestion, `_validate_executive_bullets` + pack builder):**
- valid ids within pack are kept;
- ids outside the pack are dropped;
- duplicate ids deduped (order preserved);
- more than `MAX_EXECUTIVE_BULLET_CITATIONS` ids truncated;
- missing / non-list / non-int `citation_source_ids` → `[]`;
- Low Signal bullets → `[]`;
- **deterministic pack invariant**: same article set → identical pack ids
  (including tie-break ordering); cap honored at 40.

**Rendering (delivery):**
- inline grouped `[1, 2]` + footer render in sequential display order;
- renumbering maps raw ids → 1..N by first appearance correctly across bullets;
- mixed per-bullet citation state renders correctly;
- zero-citation summary renders clean with no inline numbers and no footer;
- `html.escape` neutralizes a malicious headline/URL;
- non-http(s) URL scheme is dropped (no `href` injection);
- malformed URL → graceful footer fallback, no crash;
- all legacy-state summaries render exactly as today.

## Scope Boundary

**In this PR:** source-pack construction, prompt/schema change, citation
validation, `executive_sources` column + migration `004`, repo persistence +
fetch updates, delivery inline + footer rendering, URL scheme validation,
backward compatibility, ingestion + rendering tests.

**Deferred:** factual claim-vs-article verification; heuristic / NLP citation
matching; citation confidence scoring; per-sentence citations inside prose;
persisting an `executive_source_policy` column; broad LLM-module refactor; dead
card-path cleanup.
