# Implementation Plan: Moody's Search Expansion

**Goal:** Expand the market-pulse pipeline from ~20 flat entities to 13 groups covering ~200 named entities and keyword topics, replicating the Moody's News Edge report structure.

**Spec:** `docs/superpowers/specs/2026-04-14-moody-search-expansion-design.md`

**Architecture overview:**
- `targets.yaml` — new entity/concept group schema; only file non-technical editors touch
- `ingestion_engine.py` — add `_MOODY_INTERNAL_EXCLUDES`, `build_query()`, rewrite `load_targets()`, rename `discover_urls()` param, update `execute_pipeline()` call site, raise `MAX_DAILY_SCRAPES`
- `tests/test_pipeline.py` — rewrite 2 existing `load_targets` tests, remove 1 obsolete test, add 5 new tests

**Tech stack:** Python 3.11, pytest, PyYAML, Serper.dev (news search), Firecrawl (scraper), OpenAI gpt-5.4-nano, Supabase

---

## File Map

| File | Change |
|---|---|
| `targets.yaml` | Full replacement — 13-group entity/concept schema |
| `ingestion_engine.py` | Add `_MOODY_INTERNAL_EXCLUDES` + `build_query()`; rewrite `load_targets()`; rename `discover_urls()` param; update `execute_pipeline()` call; raise `MAX_DAILY_SCRAPES` |
| `tests/test_pipeline.py` | Rewrite 2 tests, remove 1, add 5 new tests; add `build_query` to imports |

---

## Task 1 — Add `build_query` tests (TDD: write failing tests first)

**Add `build_query` to the import block and write 3 new failing tests.**

### Step 1a — Add `build_query` to test imports

Edit the import block at the top of `tests/test_pipeline.py` (lines 11–20):

```python
from ingestion_engine import (
    _TextExtractor,
    _scrape_fallback,
    build_query,              # ← add this
    compute_url_hash,
    generate_macro_summary,
    load_targets,
    normalize_url,
    scrape_article,
    synthesize_insight,
)
```

### Step 1b — Append the 3 new tests to `tests/test_pipeline.py`

Add after line 649 (end of file):

```python
# ---------------------------------------------------------------------------
# 17. build_query()
# ---------------------------------------------------------------------------

def test_build_query_entity_mode_bare():
    """Entity mode with no include_all or exclude_any produces a quoted name."""
    result = build_query("entity", name="Shaw Industries")
    assert result == '"Shaw Industries"'


def test_build_query_entity_mode_with_excludes():
    """Entity mode exclude_any terms become -\"term\" operators."""
    result = build_query(
        "entity",
        name="Shaw Industries",
        include_all=[],
        exclude_any=["patents", "securities analyst reports"],
    )
    assert '"Shaw Industries"' in result
    assert '-"patents"' in result
    assert '-"securities analyst reports"' in result


def test_build_query_concept_mode():
    """Concept mode ORs all include_any terms and ANDs include_all."""
    result = build_query(
        "concept",
        include_any=["plastics industry", "chemical industry", "compounding"],
        include_all=["business"],
        exclude_any=[],
    )
    assert '("plastics industry" OR "chemical industry" OR "compounding")' in result
    assert '"business"' in result


def test_build_query_filters_moody_internal_excludes():
    """Moody's platform identifiers in exclude_any must be silently dropped."""
    result = build_query(
        "concept",
        include_any=["plastics industry"],
        include_all=[],
        exclude_any=["source set 238658", "PR wires", "Targeted News Search", "tenders"],
    )
    assert "source set 238658" not in result
    assert "PR wires" not in result
    assert "Targeted News Search" not in result
    assert '-"tenders"' in result   # real term must survive


def test_build_query_concept_mode_no_include_all():
    """Concept mode with empty include_all produces no spurious quoted terms."""
    result = build_query(
        "concept",
        include_any=["automotive industry"],
        include_all=[],
        exclude_any=[],
    )
    assert result == '("automotive industry")'
```

### Step 1c — Confirm tests fail

```bash
cd /workspaces/market-pulse && pytest tests/test_pipeline.py::test_build_query_entity_mode_bare tests/test_pipeline.py::test_build_query_entity_mode_with_excludes tests/test_pipeline.py::test_build_query_concept_mode tests/test_pipeline.py::test_build_query_filters_moody_internal_excludes tests/test_pipeline.py::test_build_query_concept_mode_no_include_all -v
```

Expected: `ImportError: cannot import name 'build_query' from 'ingestion_engine'`

---

## Task 2 — Implement `_MOODY_INTERNAL_EXCLUDES` and `build_query()` in `ingestion_engine.py`

### Step 2a — Add `_MOODY_INTERNAL_EXCLUDES` constant

Add after line 21 (`_SEMANTIC_DUPLICATE_THRESHOLD: int = 88`):

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

### Step 2b — Add `build_query()` function

Add immediately after `_MOODY_INTERNAL_EXCLUDES` (before `_TextExtractor`):

```python
def build_query(
    mode: str,
    name: Optional[str] = None,
    include_any: Optional[list[str]] = None,
    include_all: Optional[list[str]] = None,
    exclude_any: Optional[list[str]] = None,
) -> str:
    """Build a Serper.dev search query string from group field semantics.

    Args:
        mode: ``"entity"`` for a named-company search; ``"concept"`` for a
            keyword OR query.
        name: Company name used in entity mode.
        include_any: Terms ORed together in concept mode.
        include_all: Terms ANDed into every query.
        exclude_any: Terms prepended with ``-``; Moody's platform identifiers
            in ``_MOODY_INTERNAL_EXCLUDES`` are silently dropped.

    Returns:
        A query string ready to pass as Serper's ``q`` parameter.
    """
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

### Step 2c — Confirm the 5 new tests pass

```bash
cd /workspaces/market-pulse && pytest tests/test_pipeline.py::test_build_query_entity_mode_bare tests/test_pipeline.py::test_build_query_entity_mode_with_excludes tests/test_pipeline.py::test_build_query_concept_mode tests/test_pipeline.py::test_build_query_filters_moody_internal_excludes tests/test_pipeline.py::test_build_query_concept_mode_no_include_all -v
```

Expected: `5 passed`

### Step 2d — Commit

```bash
cd /workspaces/market-pulse && git add ingestion_engine.py tests/test_pipeline.py && git commit -m "feat: add build_query() helper and _MOODY_INTERNAL_EXCLUDES constant"
```

---

## Task 3 — Rewrite `load_targets()` tests for new schema

**Update existing tests before touching `load_targets()` so the failing tests drive the implementation.**

### Step 3a — Replace the 3 old `load_targets` tests in `tests/test_pipeline.py`

Replace the entire section 4 block (lines 99–185) with the following. The changes are:
- `test_load_targets_filters_inactive` — updated YAML fixture uses new entity/concept schema; asserts `query` field is present
- `test_load_targets_returns_expected_fields` — updated YAML fixture; asserts `query` field
- `test_raw_materials_category_loaded` — **deleted** (raw_materials category is removed)
- Two new tests added: concept group loading, inactive concept group suppression

```python
# ---------------------------------------------------------------------------
# 4. load_targets
# ---------------------------------------------------------------------------

def test_load_targets_filters_inactive(tmp_path):
    """Inactive entities in entity-mode groups must not appear in results."""
    config_yaml = textwrap.dedent(
        """\
        competitors:
          search_mode: entity
          include_all: []
          exclude_any: []
          entities:
            - name: ActiveCorp
              active: true
            - name: InactiveCorp
              active: false
        discovery:
          results_per_entity: 2
          lookback_hours: 24
          min_article_length: 500
        """
    )
    config_file = tmp_path / "targets.yaml"
    config_file.write_text(config_yaml)
    targets = load_targets(str(config_file))
    names = [t["name"] for t in targets]
    assert "ActiveCorp" in names
    assert "InactiveCorp" not in names


def test_load_targets_returns_expected_fields(tmp_path):
    """Entity-mode target dicts must contain name, category, query, and discovery fields."""
    config_yaml = textwrap.dedent(
        """\
        competitors:
          search_mode: entity
          include_all: []
          exclude_any: []
          entities:
            - name: Avient
              active: true
        discovery:
          results_per_entity: 3
          lookback_hours: 48
          min_article_length: 300
        """
    )
    config_file = tmp_path / "targets.yaml"
    config_file.write_text(config_yaml)
    targets = load_targets(str(config_file))
    assert len(targets) == 1
    t = targets[0]
    assert t["name"] == "Avient"
    assert t["category"] == "competitors"
    assert t["query"] == '"Avient"'
    assert t["results_per_entity"] == 3
    assert t["lookback_hours"] == 48
    assert t["min_article_length"] == 300


def test_load_targets_concept_group(tmp_path):
    """Active concept-mode groups produce a single target with an OR query."""
    config_yaml = textwrap.dedent(
        """\
        industry:
          search_mode: concept
          active: true
          include_any:
            - "plastics industry"
            - "chemical industry"
          include_all: []
          exclude_any:
            - tenders
        discovery:
          results_per_entity: 2
          lookback_hours: 24
          min_article_length: 500
        """
    )
    config_file = tmp_path / "targets.yaml"
    config_file.write_text(config_yaml)
    targets = load_targets(str(config_file))
    assert len(targets) == 1
    t = targets[0]
    assert t["name"] == "industry"
    assert t["category"] == "industry"
    assert '("plastics industry" OR "chemical industry")' in t["query"]
    assert '-"tenders"' in t["query"]


def test_load_targets_inactive_concept_group(tmp_path):
    """Concept-mode groups with active: false must not appear in results."""
    config_yaml = textwrap.dedent(
        """\
        industry:
          search_mode: concept
          active: false
          include_any:
            - "plastics industry"
          include_all: []
          exclude_any: []
        discovery:
          results_per_entity: 2
          lookback_hours: 24
          min_article_length: 500
        """
    )
    config_file = tmp_path / "targets.yaml"
    config_file.write_text(config_yaml)
    targets = load_targets(str(config_file))
    assert targets == []


def test_load_targets_entity_excludes_applied_to_query(tmp_path):
    """Group-level exclude_any must appear as -\"term\" in every entity query."""
    config_yaml = textwrap.dedent(
        """\
        customers:
          search_mode: entity
          include_all: []
          exclude_any:
            - patents
            - "securities analyst reports"
          entities:
            - name: Shaw Industries
              active: true
        discovery:
          results_per_entity: 2
          lookback_hours: 24
          min_article_length: 500
        """
    )
    config_file = tmp_path / "targets.yaml"
    config_file.write_text(config_yaml)
    targets = load_targets(str(config_file))
    assert len(targets) == 1
    q = targets[0]["query"]
    assert '"Shaw Industries"' in q
    assert '-"patents"' in q
    assert '-"securities analyst reports"' in q
```

### Step 3b — Confirm tests fail

```bash
cd /workspaces/market-pulse && pytest tests/test_pipeline.py -k "load_targets" -v
```

Expected: `test_load_targets_filters_inactive FAILED`, `test_load_targets_returns_expected_fields FAILED`, and the 3 new tests `FAILED` (load_targets doesn't understand new schema yet). The deleted `test_raw_materials_category_loaded` no longer appears.

---

## Task 4 — Rewrite `load_targets()` and update `discover_urls()` call site atomically

> **Important:** Tasks 4 and 5 from the original outline are merged here into one commit.
> After `load_targets()` is rewritten, concept-group targets carry a `query` field but
> `execute_pipeline()` would still pass `target["name"]` (e.g. `"industry"`) to
> `discover_urls()` — wrong for concept groups. Both changes must land in the same commit
> to avoid a broken intermediate state.

## Task 4 — Rewrite `load_targets()` in `ingestion_engine.py`

### Step 4a — Replace `load_targets()` (lines 76–96 in ingestion_engine.py)

```python
def load_targets(config_path: str) -> list[dict]:
    """Load active search targets from a YAML config file.

    Supports two search modes:
    - ``entity``: one Serper query per active company name under ``entities:``.
    - ``concept``: one combined OR query for the whole group (``active: true``
      required at group level).

    Returns:
        List of target dicts, each containing ``name``, ``category``,
        ``query`` (pre-built Serper query string), and discovery settings.
    """
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
        mode: str = group_cfg.get("search_mode", "entity")
        include_all: list[str] = group_cfg.get("include_all", [])
        exclude_any: list[str] = group_cfg.get("exclude_any", [])

        if mode == "entity":
            for entity in group_cfg.get("entities", []):
                if not entity.get("active", False):
                    continue
                targets.append({
                    "name": entity["name"],
                    "category": group_name,
                    "query": build_query(
                        "entity",
                        name=entity["name"],
                        include_all=include_all,
                        exclude_any=exclude_any,
                    ),
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
                "query": build_query(
                    "concept",
                    include_any=group_cfg.get("include_any", []),
                    include_all=include_all,
                    exclude_any=exclude_any,
                ),
                "results_per_entity": results_per_entity,
                "lookback_hours": lookback_hours,
                "min_article_length": min_article_length,
            })

    logger.info("Loaded %d active targets from %s", len(targets), config_path)
    return targets
```

### Step 4b — Confirm all `load_targets` tests pass

```bash
cd /workspaces/market-pulse && pytest tests/test_pipeline.py -k "load_targets" -v
```

Expected: `7 passed` (2 rewritten + 3 new + the 2 already passing non-load_targets tests are unaffected).

### Step 4c — Confirm full test suite still passes

```bash
cd /workspaces/market-pulse && pytest tests/ -v
```

Expected: all previously passing tests still pass.

### Step 4d — Also rename `entity_name` → `query` in `discover_urls()` and update `execute_pipeline()` in the same edit session

```python
# Before
def discover_urls(entity_name: str, lookback_hours: int, results_per_entity: int) -> list[tuple[str, str]]:
    ...
    payload = {"q": entity_name, "num": results_per_entity, "tbs": f"qdr:h{lookback_hours}"}
    ...
    logger.info("Discovered %d URL(s) for '%s'", len(results), entity_name)

# After
def discover_urls(query: str, lookback_hours: int, results_per_entity: int) -> list[tuple[str, str]]:
    ...
    payload = {"q": query, "num": results_per_entity, "tbs": f"qdr:h{lookback_hours}"}
    ...
    logger.info("Discovered %d URL(s) for query '%s'", len(results), query[:80])
```

### Step 4e — Update `execute_pipeline()` call site (line 462)

```python
# Before
raw_results = discover_urls(entity_name, lookback_hours, results_per_entity)

# After
raw_results = discover_urls(target["query"], lookback_hours, results_per_entity)
```

### Step 4f — Confirm full test suite passes

```bash
cd /workspaces/market-pulse && pytest tests/ -v
```

Expected: all tests pass.

### Step 4g — Commit (all four changes together)

```bash
cd /workspaces/market-pulse && git add ingestion_engine.py tests/test_pipeline.py && git commit -m "feat: rewrite load_targets() for entity/concept schema; rename discover_urls param to query"
```

---

## Task 5 — Raise `MAX_DAILY_SCRAPES` to 150

### Step 5a — Update the constant (line 19)

```python
# Before
MAX_DAILY_SCRAPES = 20

# After
MAX_DAILY_SCRAPES = 150
```

### Step 5b — Confirm tests pass

```bash
cd /workspaces/market-pulse && pytest tests/ -v
```

Expected: all tests pass.

### Step 5c — Commit

```bash
cd /workspaces/market-pulse && git add ingestion_engine.py && git commit -m "feat: raise MAX_DAILY_SCRAPES from 20 to 150 for paid-tier quota"
```

---

## Task 6 — Replace `targets.yaml` with full 13-group Moody's schema

### Step 6a — Overwrite `targets.yaml` with the full content below

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

# NOTE: Moody's spec listed "automotive industry" here — confirmed copy-paste error.
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

### Step 6b — Smoke-test that `load_targets` can parse the new file

```bash
cd /workspaces/market-pulse && python -c "
from ingestion_engine import load_targets
targets = load_targets('targets.yaml')
print(f'Total active targets: {len(targets)}')
categories = {}
for t in targets:
    categories[t['category']] = categories.get(t['category'], 0) + 1
for cat, count in sorted(categories.items()):
    print(f'  {cat}: {count}')
"
```

Expected output (approximately):
```
Total active targets: 116
  building_construction: 1
  competitors: 17
  customers: 48
  economic: 1
  electronics: 1
  engineered_resins: 1
  fibers: 1
  healthcare: 1
  industry: 1
  packaging: 1
  performance_compounds: 1
  suppliers: 41
  transportation: 1
```

### Step 6c — Run full test suite

```bash
cd /workspaces/market-pulse && pytest tests/ -v
```

Expected: all tests pass (test fixtures use `tmp_path`, not the real `targets.yaml`).

### Step 6d — Commit

```bash
cd /workspaces/market-pulse && git add targets.yaml && git commit -m "feat: replace targets.yaml with 13-group Moody's search expansion schema"
```

---

## Task 7 — Final verification

```bash
cd /workspaces/market-pulse && pytest tests/ -v --tb=short
```

Expected: all tests green. Review the summary line — confirm no new failures vs. pre-implementation baseline.

---

## Completion Checklist

- [ ] `build_query()` and `_MOODY_INTERNAL_EXCLUDES` added to `ingestion_engine.py`
- [ ] `load_targets()` rewritten to handle entity and concept modes
- [ ] `discover_urls()` parameter renamed `entity_name` → `query`
- [ ] `execute_pipeline()` updated to use `target["query"]`
- [ ] `MAX_DAILY_SCRAPES` raised to 150
- [ ] `targets.yaml` replaced with full 13-group schema (116 active targets)
- [ ] 5 new `build_query` tests added and passing
- [ ] 2 existing `load_targets` tests rewritten for new schema
- [ ] 3 new `load_targets` tests added and passing
- [ ] `test_raw_materials_category_loaded` removed (category no longer exists)
- [ ] Full test suite green
