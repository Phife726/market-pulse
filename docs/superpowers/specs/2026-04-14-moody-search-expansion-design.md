# Design Spec: Moody's News Edge Search Expansion

**Date:** 2026-04-14
**Status:** Approved — ready for implementation
**Author:** Claude Code (brainstorming session with user)

---

## 1. Goal

Expand the market-pulse pipeline's search coverage to replicate the 13-group structure of Americhem's Moody's News Edge subscription. The current system has ~20 entities across 5 flat categories. The new system covers ~200 named entities and keyword topic groups across 13 categories, delivering up to 150 processed articles per day.

---

## 2. Background & Constraints

- Serper.dev and Firecrawl are on paid tiers (quota constraint removed).
- `MAX_DAILY_SCRAPES = 20` will be raised to `150`.
- The ingestion pipeline must remain a single sequential script — no parallelism changes.
- Non-technical editors must still be able to add/remove entities by editing only `targets.yaml`.
- No changes to `delivery_engine.py`, `schema.sql`, or the `todays_intelligence` view.

---

## 3. Chosen Approach: Approach B — Full Schema Redesign

Two search modes coexist in `targets.yaml`:

| Mode | Behavior | Groups using it |
|---|---|---|
| `entity` | One Serper query per active company name; group-level `include_all` / `exclude_any` appended | competitors, customers, suppliers |
| `concept` | All `include_any` terms collapsed into one OR query per group | industry, economic, healthcare, transportation, building_construction, fibers, packaging, electronics, engineered_resins, performance_compounds |

---

## 4. `targets.yaml` Schema

### Top-level structure

```yaml
discovery:
  results_per_entity: 2
  lookback_hours: 24
  min_article_length: 500

<group_name>:
  search_mode: entity | concept
  include_all: []        # terms ANDed into every query for this group
  exclude_any: []        # terms prepended with - in the query; Moody's-internal
                         # identifiers are silently filtered by ingestion_engine.py
  # entity mode only:
  entities:
    - name: <Company Name>
      active: true | false
  # concept mode only:
  active: true | false
  include_any: []        # terms ORed into a single combined query
```

### Entity mode example

```yaml
customers:
  search_mode: entity
  include_all: []
  exclude_any:
    - patents
    - "securities analyst reports"
  entities:
    - name: Shaw Industries
      active: true
```

Produces query: `"Shaw Industries" -"patents" -"securities analyst reports"`

### Concept mode example

```yaml
industry:
  search_mode: concept
  active: true
  include_any:
    - "plastics industry"
    - "chemical industry"
    - compounding
  include_all: []
  exclude_any:
    - tenders
```

Produces query: `("plastics industry" OR "chemical industry" OR "compounding") -"tenders"`

---

## 5. `ingestion_engine.py` Changes

### 5.1 — Raise `MAX_DAILY_SCRAPES`

```python
MAX_DAILY_SCRAPES = 150   # was 20
```

### 5.2 — Add `_MOODY_INTERNAL_EXCLUDES` frozenset

Platform-level Moody's source identifiers that have no Google equivalent. Silently dropped during query construction.

```python
_MOODY_INTERNAL_EXCLUDES: frozenset[str] = frozenset({
    "source set 238658",
    "PR wires",
    "Targeted News Search",
    "US Federal News",
    "specific Asia PR feed",
    "specific processing feeds",
    "Financial Times feeds",
    "financial markups",
})
```

### 5.3 — New `build_query()` helper

```python
def build_query(
    mode: str,
    name: Optional[str] = None,
    include_any: Optional[list[str]] = None,
    include_all: Optional[list[str]] = None,
    exclude_any: Optional[list[str]] = None,
) -> str:
    parts: list[str] = []

    if mode == "entity":
        parts.append(f'"{name}"')
    elif mode == "concept":
        if include_any:
            or_terms = " OR ".join(f'"{t}"' for t in include_any)
            parts.append(f"({or_terms})")

    for term in (include_all or []):
        parts.append(f'"{term}"')

    for term in (exclude_any or []):
        if term not in _MOODY_INTERNAL_EXCLUDES:
            parts.append(f'-"{term}"')

    return " ".join(parts)
```

### 5.4 — Rewrite `load_targets()`

Replace the hardcoded 5-category tuple with a generic loop over all YAML groups. Returned target dicts gain a `query` field (the pre-built Serper string). The `name` field remains for logging and the `trigger_entity` column in Supabase.

```python
def load_targets(config_path: str) -> list[dict]:
    with open(config_path, "r") as fh:
        config = yaml.safe_load(fh)
    discovery = config.get("discovery", {})
    results_per_entity: int = discovery.get("results_per_entity", 2)
    lookback_hours: int = discovery.get("lookback_hours", 24)
    min_article_length: int = discovery.get("min_article_length", 500)

    targets: list[dict] = []
    for group_name, group_cfg in config.items():
        if group_name == "discovery" or not isinstance(group_cfg, dict):
            continue
        mode = group_cfg.get("search_mode", "entity")
        include_all: list[str] = group_cfg.get("include_all", [])
        exclude_any: list[str] = group_cfg.get("exclude_any", [])

        if mode == "entity":
            for entity in group_cfg.get("entities", []):
                if not entity.get("active", False):
                    continue
                targets.append({
                    "name": entity["name"],
                    "category": group_name,
                    "query": build_query("entity", name=entity["name"],
                                         include_all=include_all, exclude_any=exclude_any),
                    "results_per_entity": results_per_entity,
                    "lookback_hours": lookback_hours,
                    "min_article_length": min_article_length,
                })
        elif mode == "concept":
            if not group_cfg.get("active", False):
                continue
            targets.append({
                "name": group_name,
                "category": group_name,
                "query": build_query("concept",
                                     include_any=group_cfg.get("include_any", []),
                                     include_all=include_all, exclude_any=exclude_any),
                "results_per_entity": results_per_entity,
                "lookback_hours": lookback_hours,
                "min_article_length": min_article_length,
            })

    logger.info("Loaded %d active targets from %s", len(targets), config_path)
    return targets
```

### 5.5 — Update `discover_urls()` and `execute_pipeline()`

`discover_urls()`: rename `entity_name` parameter to `query`; truncate log message.

```python
def discover_urls(query: str, lookback_hours: int, results_per_entity: int) -> list[tuple[str, str]]:
    payload = {"q": query, ...}
    logger.info("Discovered %d URL(s) for query '%s'", len(results), query[:80])
```

`execute_pipeline()`: one line change.

```python
raw_results = discover_urls(target["query"], lookback_hours, results_per_entity)
```

`synthesize_insight()` still receives `target["name"]` as `trigger_entity` — human-readable, not the raw query string.

---

## 6. Full `targets.yaml` Content

```yaml
# ============================================================
# Market-Pulse Control Panel — Americhem
# Moody's News Edge Replacement
# ============================================================
# ENTITY MODE  — one Serper query per active company name
# CONCEPT MODE — one combined OR query per group
#
# To pause an entity:     set active: false (no Python change needed)
# To add an entity:       append to entities: list
# To add a concept group: new top-level key, search_mode: concept
#
# exclude_any: Moody's platform identifiers (e.g. "source set 238658",
#   "PR wires") are silently ignored by ingestion_engine.py —
#   only real search terms become -"term" operators.
# ============================================================

discovery:
  results_per_entity: 2
  lookback_hours: 24
  min_article_length: 500

# ── ENTITY MODE GROUPS ────────────────────────────────────────

competitors:
  search_mode: entity
  include_all: []
  exclude_any: []
  entities:
    - name: Audia Elastomers
      active: true
    - name: Aurora Fine Chemicals
      active: true
    - name: Liochem
      active: true
    - name: Polymax
      active: true
    - name: Primex Color Compounding
      active: true
    - name: Avient
      active: true
    - name: Standridge Color
      active: true
    - name: Ampacet
      active: true
    - name: Teknor Apex
      active: true
    - name: Mitsubishi Chemical
      active: true
    - name: Uniform Color
      active: true
    - name: Akro-Mils
      active: true
    - name: RTP Company
      active: true
    - name: Penn Color
      active: true
    - name: Techmer PM
      active: true
    - name: Kraiburg TPE
      active: true
    - name: Grafe Advanced Polymers
      active: true

customers:
  search_mode: entity
  include_all: []
  exclude_any:
    - patents
    - "securities analyst reports"
  entities:
    - name: Cavanaugh Building Corporation
      active: true
    - name: Closure Systems International
      active: true
    - name: Cocona
      active: true
    - name: CS Manufacturing
      active: true
    - name: Fiberon
      active: true
    - name: GAF Roofing
      active: true
    - name: Homeland Vinyl
      active: true
    - name: Inoac Automotive
      active: true
    - name: Integrated Fiber Solutions
      active: true
    - name: Marglen Industries
      active: true
    - name: Marquis Industries
      active: true
    - name: Mitchell Plastics
      active: true
    - name: Motus Integrated
      active: true
    - name: NYX Inc
      active: true
    - name: Plast-O-Foam
      active: true
    - name: Plastipak
      active: true
    - name: Polymer Process Development
      active: true
    - name: TruNorth Composites
      active: true
    - name: Kinderhook Industries
      active: true
    - name: Shaw Industries
      active: true
    - name: Advanced Composites
      active: true
    - name: Magna International
      active: true
    - name: Kimberly-Clark
      active: true
    - name: Ply Gem
      active: true
    - name: Mohawk Industries
      active: true
    - name: Pilkington
      active: true
    - name: AdvanSix
      active: true
    - name: Saint-Gobain Glass
      active: true
    - name: Performance Plastics
      active: true
    - name: Summit Polymers
      active: true
    - name: Yanfeng Global
      active: true
    - name: Asahi Glass
      active: true
    - name: Mannington Mills
      active: true
    - name: Koch Industries
      active: true
    - name: SABIC
      active: true
    - name: Farathane
      active: true
    - name: 3M
      active: true
    - name: Universal Fiber
      active: true
    - name: Associated Materials
      active: true
    - name: Bonar
      active: true
    - name: Ascend Performance Materials
      active: true
    - name: Radici Group
      active: true
    - name: Lexmark
      active: true
    - name: Fuyao Automotive Glass
      active: true
    - name: Tokai Rika
      active: true
    - name: Freudenberg
      active: true
    - name: Wilbert
      active: true
    - name: Indorama Ventures
      active: true
    - name: Mayco Industries
      active: true

suppliers:
  search_mode: entity
  include_all: []
  exclude_any:
    - patents
    - "securities analyst reports"
  entities:
    - name: AdvanSix Resin
      active: true
    - name: AM Stabilizers
      active: true
    - name: Amco Polymers
      active: true
    - name: Baerlocher
      active: true
    - name: BASF
      active: true
    - name: Cal-Chem
      active: true
    - name: Channel Prime Alliance
      active: true
    - name: DCL Corp
      active: true
    - name: Dynasol
      active: true
    - name: ExxonMobil Chemical
      active: true
    - name: Ferro Pigments
      active: true
    - name: Heucotech
      active: true
    - name: Huntsman Corporation
      active: true
    - name: Interspersal
      active: true
    - name: Milliken
      active: true
    - name: Mitsui Plastics
      active: true
    - name: Nycoa
      active: true
    - name: Shepherd Color
      active: true
    - name: Shintech
      active: true
    - name: Univar Solutions
      active: true
    - name: Chemours
      active: true
    - name: Nexeo Solutions
      active: true
    - name: Oxy Vinyls
      active: true
    - name: Clariant
      active: true
    - name: Sun Chemical
      active: true
    - name: Kronos Worldwide
      active: true
    - name: Kuraray
      active: true
    - name: Lanxess
      active: true
    - name: Cytec Industries
      active: true
    - name: Trinseo
      active: true
    - name: Barentz
      active: true
    - name: Orion Engineered Carbons
      active: true
    - name: Venator Materials
      active: true
    - name: BYK Additives
      active: true
    - name: Dow
      active: true
    - name: Cabot Corporation
      active: true
    - name: Formosa Plastics
      active: true
    - name: Chase Plastics
      active: true
    - name: Lubrizol
      active: true
    - name: Omya
      active: true
    - name: Galata Chemicals
      active: true
    - name: Honeywell
      active: true

# ── CONCEPT MODE GROUPS ───────────────────────────────────────

industry:
  search_mode: concept
  active: true
  include_any:
    - "plastics industry"
    - "chemical industry"
    - "floriculture consumer goods"
    - compounding
    - extrusion
  include_all: []
  exclude_any:
    - tenders

economic:
  search_mode: concept
  active: true
  include_any:
    - "economic indicators"
    - "labor market"
    - "hiring layoffs"
    - "economic forecast"
    - "global economy"
  include_all: []
  exclude_any:
    - Xinhua
    - "Anadolu Agency"
    - Mondaq

healthcare:
  search_mode: concept
  active: true
  include_any:
    - "Beckman Coulter"
    - "Aptar Pharma"
    - Bespak
    - "Companion Medical"
    - "Credence Medsystems"
    - "GE Healthcare"
    - "medical grade plastics"
    - "biocompatible plastics"
    - "sterilizable plastics"
    - "autoclavable plastics"
    - "clean compounding"
  include_all: []
  exclude_any:
    - patents
    - "securities analyst reports"

transportation:
  search_mode: concept
  active: true
  include_any:
    - "automotive industry"
  include_all: []
  exclude_any: []

# NOTE: Moody's spec listed "automotive industry" here — copy-paste error confirmed.
# Replaced with building/construction-specific terms.
building_construction:
  search_mode: concept
  active: true
  include_any:
    - "building materials"
    - "construction industry"
    - "building products"
    - "commercial construction"
    - "residential construction"
  include_all: []
  exclude_any: []

fibers:
  search_mode: concept
  active: true
  include_any:
    - "synthetic fiber"
    - "performance fiber"
    - polyester
    - polyethylene
    - polypropylene
    - nylon
    - acrylic
  include_all:
    - textiles          # prevents generic resin/chemical hits
  exclude_any: []

packaging:
  search_mode: concept
  active: true
  include_any:
    - "packaging manufacturing"
  include_all: []
  exclude_any:
    - "Pizza Hut"

electronics:
  search_mode: concept
  active: true
  include_any:
    - "electronics industry"
  include_all: []
  exclude_any:
    - "video game industry"
    - "video games"

# NOTE: Moody's spec uses "acetyl" — corrected to "acetal" (polyacetal/POM polymer).
# PPE kept per spec; include_all: polymer suppresses personal-protective-equipment noise.
# engineered_resins  = raw material supply/pricing context (include_all: polymer)
# performance_compounds = processing/formulation context (include_all: compounding)
engineered_resins:
  search_mode: concept
  active: true
  include_any:
    - acetal
    - nylon
    - PBT
    - polycarbonate
    - PEEK
    - PEI
    - PES
    - PET
    - polyolefin
    - PPE
    - PPS
    - PSU
    - TPE
    - TPU
    - thermoplastics
  include_all:
    - polymer
  exclude_any:
    - patents

performance_compounds:
  search_mode: concept
  active: true
  include_any:
    - acetal
    - nylon
    - PBT
    - polycarbonate
    - PEEK
    - PEI
    - PES
    - PET
    - polyolefin
    - PPE
    - PPS
    - PSU
    - TPE
    - TPU
    - thermoplastics
  include_all:
    - compounding
  exclude_any:
    - patents
```

---

## 7. Daily Search Volume

| Group | Mode | Serper queries | Raw candidates |
|---|---|---|---|
| competitors | entity | 17 | 34 |
| customers | entity | 48 | 96 |
| suppliers | entity | 41 | 82 |
| industry | concept | 1 | 2 |
| economic | concept | 1 | 2 |
| healthcare | concept | 1 | 2 |
| transportation | concept | 1 | 2 |
| building_construction | concept | 1 | 2 |
| fibers | concept | 1 | 2 |
| packaging | concept | 1 | 2 |
| electronics | concept | 1 | 2 |
| engineered_resins | concept | 1 | 2 |
| performance_compounds | concept | 1 | 2 |
| **Total** | | **116 queries** | **232 candidates** |

After dedup, semantic filtering, and length checks: ~130–160 reach the LLM.
`MAX_DAILY_SCRAPES = 150` handles the tail end gracefully.

---

## 8. Test Changes

| Test | Action |
|---|---|
| `test_url_normalization` | No change |
| `test_url_hash_collision` | No change |
| `test_sentiment_clamping` | No change |
| `test_load_targets_filters_inactive` | **Rewrite** — YAML fixture and assertions updated to new schema |
| `test_build_query_entity_mode` | **New** — entity with/without include_all and exclude_any |
| `test_build_query_concept_mode` | **New** — OR construction, include_all AND, exclude filtering |
| `test_build_query_filters_moody_internal` | **New** — verify internal identifiers are dropped |

---

## 9. Supabase Migration Notes

No schema changes required. The 13 new `category` values (`industry`, `economic`, `healthcare`, etc.) are stored as plain strings in the existing `daily_intelligence` table. Rows written under the old category names (`raw_materials`, `markets`) will remain in the DB and continue to appear in the `todays_intelligence` view — they do not conflict with new rows. `delivery_engine.py` groups by `alert_tier` (derived from `sentiment_score`), not by `category`, so no delivery logic changes are needed.

---

## 10. Query Length Budget

Largest concept group query: `engineered_resins` / `performance_compounds` — 15 `include_any` terms + 1 `include_all` + 1 `exclude_any` ≈ ~220 chars. Serper.dev's `q` field accepts up to 2,048 chars in the POST body. All groups are well within limits. Future expansion headroom: ~1,800 chars remaining on the largest groups.

---

## 11. Known Gaps / Future Enhancements

| Gap | Impact | Mitigation |
|---|---|---|
| Per-entity `exclude_any` not supported (e.g., suppress "South Africa" only for Mayco) | Minor noise for some entities | LLM DISCARD rule handles false positives |
| Concept groups get only 2 results/run (`results_per_entity` applies globally) | Low concept group recall | Raise `results_per_entity` to 3–5 if needed; the 150 cap still holds |
| Short acronyms (PPE, PET, PPS) may generate false positives | Higher DISCARD rate for engineered_resins / performance_compounds | `include_all: polymer` / `compounding` mitigates; LLM Rule 1 handles remainder |
