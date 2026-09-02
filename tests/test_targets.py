"""The two non-technical control files as shipped — `targets.yaml` and
`market_pulse_config.yaml` — and `targets.py`, the catalogue: `load_targets` /
`parse_targets` (the one parser of the first file — over a path or over text in
hand — fail-fast on shape errors, invalid YAML included), `entity_entries` (the
same walk's listing of every entity entry, active or not), `build_query`.

Contract tests here read the REAL control files: group order (the four-tier
degradation policy), the macro groups, the priority-segment split, per-group
`results_per_entity`, and the config↔targets / config↔prompt parity pins.
"""

import textwrap
from functools import cache
from pathlib import Path

import pytest
import yaml
from run_budget import RunBudget
from targets import ENTITY_OPTIONAL_KEYS, TargetsError, build_query, entity_entries, load_targets, parse_targets
from tests.conftest import stub_target

_TARGETS_PATH = Path(__file__).resolve().parents[1] / "targets.yaml"
_CONFIG_PATH = _TARGETS_PATH.with_name("market_pulse_config.yaml")


# The real control files, parsed once per session — lazily, so a broken file
# fails the pins that read it by name instead of erroring the whole module at
# collection. Absolute paths, so the file runs from any working directory;
# every reader is read-only.
@cache
def _real_targets_yaml() -> dict:
    return yaml.safe_load(_TARGETS_PATH.read_text())


@cache
def _real_config_yaml() -> dict:
    return yaml.safe_load(_CONFIG_PATH.read_text())


@cache
def _real_targets() -> list[dict]:
    return load_targets(str(_TARGETS_PATH))


def _load(tmp_path: Path, body: str) -> list[dict]:
    """Write `body` (dedented) as a tmp targets.yaml and load it."""
    path = tmp_path / "targets.yaml"
    path.write_text(textwrap.dedent(body))
    return load_targets(str(path))


# ===========================================================================
# load_targets
# ===========================================================================


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
    targets = _load(tmp_path, config_yaml)
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
    targets = _load(tmp_path, config_yaml)
    assert len(targets) == 1
    t = targets[0]
    assert t["name"] == "Avient"
    assert t["category"] == "competitors"
    assert t["query"] == '"Avient"'
    assert t["results_per_entity"] == 3
    assert t["lookback_hours"] == 48
    assert t["min_article_length"] == 300
    assert t["search_mode"] == "entity"


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
    targets = _load(tmp_path, config_yaml)
    assert len(targets) == 1
    t = targets[0]
    assert t["name"] == "industry"
    assert t["category"] == "industry"
    assert t["search_mode"] == "concept"
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
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
        discovery:
          results_per_entity: 2
          lookback_hours: 24
          min_article_length: 500
        """
    )
    targets = _load(tmp_path, config_yaml)
    assert [t["name"] for t in targets] == ["Avient"]


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
    targets = _load(tmp_path, config_yaml)
    assert len(targets) == 1
    q = targets[0]["query"]
    assert '"Shaw Industries"' in q
    assert '-"patents"' in q
    assert '-"securities analyst reports"' in q


# ===========================================================================
# Shape validation — a malformed control file fails at load, naming the group
# ===========================================================================


@pytest.mark.parametrize("body, needle", [
    pytest.param("", "mapping of groups", id="empty-file"),
    pytest.param("- competitors\n- customers\n", "mapping of groups", id="list-root"),
    pytest.param("""\
        competitors:
          search_mode: entity
          entities:
            - active: true
        """, "competitors", id="entity-without-name"),
    pytest.param("""\
        competitors:
          search_mode: hybrid
          entities: []
        """, "hybrid", id="unknown-search-mode"),
    pytest.param("""\
        healthcare:
          search_mode: concept
          active: true
        """, "include_any", id="concept-without-include_any"),
    pytest.param("""\
        healthcare:
          search_mode: concept
          active: false
          include_any: []
        """, "include_any", id="concept-with-empty-include_any-even-inactive"),
    pytest.param("""\
        healthcare:
          search_mode: concept
          active: true
          include_any: "medical device polymers"
        """, "include_any", id="include_any-not-a-list"),
    pytest.param("""\
        competitors:
          search_mode: entity
          entities: Avient
        """, "entities", id="entities-not-a-list"),
    pytest.param("""\
        competitors:
          search_mode: entity
          entities:
        """, "entities", id="entities-present-but-null"),
    pytest.param("""\
        healthcare:
          search_mode: concept
          active: true
          include_any:
          include_all: ["x"]
        """, "include_any", id="include_any-present-but-null"),
    pytest.param("""\
        discovery:
          results_per_entity: "2"
        competitors:
          search_mode: entity
          entities: []
        """, "results_per_entity", id="discovery-setting-quoted"),
    pytest.param("""\
        healthcare:
          search_mode: concept
          active: false
          results_per_entity: four
          include_any: ["x"]
        """, "results_per_entity", id="group-override-not-an-int-even-inactive"),
    pytest.param("""\
        discovery: fast
        competitors:
          search_mode: entity
          entities: [{name: Avient, active: true}]
        """, "discovery", id="discovery-not-a-mapping"),
    pytest.param("""\
        competitors:
          - name: Avient
            active: true
        """, "competitors", id="entities-written-directly-under-the-group"),
    pytest.param("""\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: "false"
        """, "active", id="active-quoted"),
    pytest.param("""\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_news: "no"
        """, "zoominfo_news", id="zoominfo_news-quoted"),
    pytest.param("""\
        competitors:
          search_mode: entity
          entities:
            - name: 2024
              active: true
        """, "name", id="name-not-a-string"),
    pytest.param("""\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: false
        """, "no active targets", id="nothing-active"),
    pytest.param("""\
        competitors:
          entities:
            - name: A
          zoominfo_company_id: 1
            - name: B
        """, "not valid YAML", id="not-yaml"),
])
def test_load_targets_rejects_shape_errors_naming_the_group(tmp_path, body, needle):
    with pytest.raises(TargetsError, match=needle):
        _load(tmp_path, body)


def test_load_targets_carries_optional_entity_keys_or_their_defaults(tmp_path):
    """The ZoomInfo fields and the resolution hints (`domain` / `hq_country` /
    `hq_state`, read by the enrichment utility) ride along on the same target
    the engine runs — one parser; absent keys are None (news defaults on)."""
    by_name = {t["name"]: t for t in _load(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 12345678
              zoominfo_news: false
              domain: avient.com
              hq_country: US
            - name: Clariant
              active: true
        """)}
    optional = lambda target: {k: target[k] for k in ENTITY_OPTIONAL_KEYS}  # noqa: E731
    assert optional(by_name["Avient"]) == {
        "zoominfo_company_id": 12345678, "zoominfo_news": False,
        "domain": "avient.com", "hq_country": "US", "hq_state": None,
    }
    assert optional(by_name["Clariant"]) == ENTITY_OPTIONAL_KEYS


def test_stub_target_mirrors_the_loaders_key_set(tmp_path):
    """`tests/conftest.stub_target` documents itself as 'a target dict as
    `load_targets` maps it' — pin the key set for both modes, so a key the
    loader gains reaches every harness-driven test."""
    loaded = _load(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
        healthcare:
          search_mode: concept
          active: true
          include_any: ["medical polymers"]
        """)
    by_mode = {t["search_mode"]: t for t in loaded}
    assert set(stub_target("X")) == set(by_mode["entity"])
    assert set(stub_target("X", search_mode="concept")) == set(by_mode["concept"])


def test_load_targets_ignores_a_stray_top_level_scalar(tmp_path):
    """Top-level scalars (a note, a version stamp) are not groups."""
    assert _load(tmp_path, """\
        version: 3
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
        """)[0]["name"] == "Avient"


# ===========================================================================
# Per-group results_per_entity override (priority-segment discovery volume)
# ===========================================================================


def test_concept_group_results_per_entity_override(tmp_path):
    """A concept group may declare its own results_per_entity; a group without
    one inherits the global discovery value."""
    config_yaml = textwrap.dedent(
        """\
        priority_segment:
          search_mode: concept
          active: true
          results_per_entity: 4
          include_any:
            - "building products plastics"
          include_all: []
          exclude_any: []
        plain_segment:
          search_mode: concept
          active: true
          include_any:
            - "polymer additives"
          include_all: []
          exclude_any: []
        discovery:
          results_per_entity: 2
          lookback_hours: 24
          min_article_length: 500
        """
    )
    targets = _load(tmp_path, config_yaml)
    by_name = {t["name"]: t for t in targets}
    assert by_name["priority_segment"]["results_per_entity"] == 4
    assert by_name["plain_segment"]["results_per_entity"] == 2


def test_entity_group_ignores_stray_results_per_entity(tmp_path):
    """The override is concept-only: a stray group-level results_per_entity on
    an entity group is ignored — entity targets keep the global value."""
    config_yaml = textwrap.dedent(
        """\
        competitors:
          search_mode: entity
          results_per_entity: 9
          include_all: []
          exclude_any: []
          entities:
            - name: Avient
              active: true
        discovery:
          results_per_entity: 2
          lookback_hours: 24
          min_article_length: 500
        """
    )
    targets = _load(tmp_path, config_yaml)
    assert targets[0]["results_per_entity"] == 2


# ===========================================================================
# Dedicated macroeconomic discovery targets
# ===========================================================================


_MACRO_GROUP_KEYS = [
    "macro_manufacturing",
    "macro_construction",
    "macro_automotive",
    "macro_consumer_demand",
    "macro_inflation_rates",
    "macro_energy_freight",
    "macro_business_investment",
]


def test_targets_yaml_has_active_macro_concept_groups():
    """The dedicated macro concept groups are present, active, and load as
    concept targets covering the seven macro domains."""
    targets = _real_targets()
    categories = {t["category"] for t in targets}
    assert set(_MACRO_GROUP_KEYS) <= categories


def test_targets_yaml_generic_economic_group_removed():
    """The old generic `economic` group is absorbed by the dedicated macro
    groups and no longer exists (do not run both)."""
    cfg = _real_targets_yaml()
    assert "economic" not in cfg


def test_targets_yaml_macro_groups_are_last_in_file_order():
    """Macro groups occupy the final positions in file order — targets process
    in file order, so a deadline-limited run sacrifices macro before entity
    coverage (graceful degradation by construction)."""
    cfg = _real_targets_yaml()
    keys = [k for k in cfg if k != "discovery"]
    assert keys[-len(_MACRO_GROUP_KEYS):] == _MACRO_GROUP_KEYS


def test_macro_groups_are_concept_mode():
    """Each macro group is a concept-mode group (one combined OR query)."""
    cfg = _real_targets_yaml()
    for key in _MACRO_GROUP_KEYS:
        assert cfg[key]["search_mode"] == "concept"
        assert cfg[key]["active"] is True


# ===========================================================================
# Priority-segment discovery: transportation split + B&C retune
# ===========================================================================


_TRANSPORTATION_SPLIT_KEYS = [
    "transportation_automotive",
    "transportation_aerospace",
    "transportation_non_automotive",
]


_PRIORITY_SEGMENT_KEYS = [
    "healthcare",
    "fibers",
    "building_construction",
    "transportation_automotive",
    "transportation_aerospace",
    "transportation_non_automotive",
    "packaging",
    "engineered_resins",
]


def test_targets_yaml_transportation_split_into_three():
    """The combined `transportation` group is replaced by three separate active
    concept targets whose keys mirror the commercial_segments config keys."""
    cfg = _real_targets_yaml()
    assert "transportation" not in cfg
    targets = _real_targets()
    concept_names = {t["name"] for t in targets if t["search_mode"] == "concept"}
    assert set(_TRANSPORTATION_SPLIT_KEYS) <= concept_names


def test_targets_yaml_priority_segments_have_raised_volume():
    """Each priority-segment concept group carries results_per_entity 4; the
    global default stays 2 for everything else."""
    targets = _real_targets()
    by_name = {t["name"]: t for t in targets}
    for key in _PRIORITY_SEGMENT_KEYS:
        assert by_name[key]["results_per_entity"] == 4, key


def test_targets_yaml_macro_groups_stay_at_global_volume():
    """Macro groups must NOT be raised — that would inflate the tail reserve
    and shrink entity coverage."""
    targets = _real_targets()
    by_name = {t["name"]: t for t in targets}
    for key in _MACRO_GROUP_KEYS:
        assert by_name[key]["results_per_entity"] == 2, key


def test_targets_yaml_building_construction_excludes_real_estate():
    """building_construction has a non-empty query, carries no real-estate term
    as a positive (include_any) match, and excludes real-estate noise."""
    cfg = _real_targets_yaml()
    bc = cfg["building_construction"]
    include_blob = " ".join(bc.get("include_any", [])).lower()
    assert include_blob.strip()
    for noise in ("for sale", "sold", "real estate", "home listing"):
        assert noise not in include_blob
    excludes = {e.lower() for e in bc.get("exclude_any", [])}
    assert {"for sale", "real estate", "home listing"} <= excludes
    # Sale phrases must be real-estate-specific, not the standalone verb "sold":
    # build_query() turns each exclude into -"term", and a bare -"sold" would
    # drop legitimate building-products manufacturer moves ("sold its roofing
    # business"). Home-sale phrasing still catches the original noise.
    assert "sold" not in excludes
    assert any("sold" in e for e in excludes)
    targets = _real_targets()
    bc_query = next(t["query"] for t in targets if t["name"] == "building_construction")
    assert bc_query.strip()


def test_targets_yaml_all_concept_queries_nonempty():
    """Every active concept target must produce a non-empty, well-formed query."""
    targets = _real_targets()
    for t in targets:
        if t["search_mode"] == "concept":
            assert t["query"].strip(), t["name"]
            assert t["query"].count("(") == t["query"].count(")"), t["name"]


# ===========================================================================
# Target priority order
# ===========================================================================


# Tier 1 — priority commercial segments, in the exact processing order they
# must occupy at the top of the file. (This ordering is intentionally distinct
# from _PRIORITY_SEGMENT_KEYS, which asserts set membership + raised volume, not
# position.)
_TIER1_PRIORITY_ORDER = [
    "healthcare",
    "fibers",
    "building_construction",
    "transportation_automotive",
    "packaging",
    "transportation_aerospace",
    "transportation_non_automotive",
    "engineered_resins",
]


def test_tier1_priority_segments_are_first_eight_in_order():
    """The first eight loaded targets are the Tier 1 priority segments in the
    exact required order — so a budget-exhausted run keeps them first."""
    targets = _real_targets()
    first_eight = [t["name"] for t in targets[:8]]
    assert first_eight == _TIER1_PRIORITY_ORDER


def test_all_entity_targets_follow_every_tier1_target():
    """Every entity target sits below every Tier 1 target — Tier 2 (entities)
    is sacrificed before Tier 1 when the budget runs out mid-list."""
    targets = _real_targets()
    tier1_indices = [
        i for i, t in enumerate(targets) if t["name"] in _TIER1_PRIORITY_ORDER
    ]
    entity_indices = [
        i for i, t in enumerate(targets) if t["search_mode"] == "entity"
    ]
    assert entity_indices, "expected some entity targets"
    assert min(entity_indices) > max(tier1_indices)


def test_macro_groups_are_the_trailing_loaded_targets():
    """The final loaded targets are exactly the macro_* groups (macro-last
    invariant), so macro coverage is sacrificed first of all."""
    targets = _real_targets()
    trailing = [t["name"] for t in targets[-len(_MACRO_GROUP_KEYS):]]
    assert trailing == _MACRO_GROUP_KEYS


def test_known_inactive_entities_stay_absent():
    """Reordering must not resurrect paused/duplicate entities."""
    targets = _real_targets()
    names = {t["name"] for t in targets}
    for inactive in ("Polymax", "Performance Plastics", "Lexmark", "AdvanSix Resin"):
        assert inactive not in names


def test_all_loaded_queries_nonempty_after_reorder():
    """Every loaded target (entity and concept) still produces a query."""
    targets = _real_targets()
    assert targets
    for t in targets:
        assert t["query"].strip(), t["name"]


# ===========================================================================
# Run-budget headroom against the shipped targets.yaml
# ===========================================================================


def test_tail_reserve_defaults_leave_headroom_for_tail_groups():
    """Against the real targets.yaml: the total concept demand is the eight
    priority groups at 4 plus the thirteen other concept/macro groups at 2 —
    the per-group override must reach the reserve — and it leaves the entity
    tier a majority of the cap; the clock reserve sits inside the deadline."""
    budget = RunBudget.for_targets(_real_targets())
    assert budget.concept_demand_ahead[0] == 8 * 4 + 13 * 2
    assert budget.concept_demand_ahead[0] < budget.max_scrapes / 2
    assert 0 < budget.tail_reserve_seconds < budget.deadline_seconds


# ===========================================================================
# market_pulse_config.yaml ↔ targets.yaml parity
# ===========================================================================


def test_config_appendix_exclusions_match_the_macro_groups_in_targets():
    """Parity guard (issue #73): the production exclusion list is exactly the
    set of macro_* concept groups in targets.yaml, so adding a macro group
    without listing it here fails CI instead of leaking its rows into the
    appendix — and a stale entry for a removed group is caught too."""
    cfg = _real_config_yaml()
    targets = _real_targets_yaml()
    macro_groups = {k for k in targets if k.startswith("macro_")}
    assert macro_groups, "targets.yaml has no macro_* groups"
    assert set(cfg["reporting"]["appendix_exclude_categories"]) == macro_groups


# ===========================================================================
# market_pulse_config.yaml — taxonomy blocks
# ===========================================================================


def test_config_has_commercial_segments_and_signal_types():
    """market_pulse_config.yaml must expose the new commercial_segments,
    signal_types, macro_conditions, executive_bullet_labels, and
    delivery_suppression blocks with the expected labels."""

    from insight import VALID_COMMERCIAL_SEGMENTS
    cfg = _real_config_yaml()

    segments = {s["label"] for s in cfg["commercial_segments"].values()}
    assert segments == set(VALID_COMMERCIAL_SEGMENTS)

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


def test_config_pins_the_appendix_levers_and_the_prompt_band():
    """The production YAML is the only lever for appendix breadth (PR #62) and
    the RULE 6 template band is derived from it (issue #65): pin both so a
    silent edit or a rollback to the code defaults is visible in CI, and
    prove the assembled production prompt binds the templates to 3–4."""

    import prompts
    cfg = _real_config_yaml()

    rep = cfg["reporting"]
    assert rep["supporting_impact_threshold"] == 3
    assert rep["visible_impact_threshold"] == 6
    assert rep["max_additional_articles"] == 20
    assert rep["max_visible_articles_per_segment"] == 5
    # On: safe because the appendix ranks template rows last (PR #70).
    assert cfg["delivery_suppression"]["enable_low_exposure_template_exemption"] is True

    system = prompts.insight_prompt(
        cfg, article_text="Body.", source_url="https://news.com/a",
        trigger_entity="Dow", category="competitors",
    ).system
    assert "americhem_impact_score of 3 or 4" in system
    assert "never below 3" in system
    assert "Set americhem_impact_score to 4" in system


# ===========================================================================
# targets.yaml configuration contract
# ===========================================================================




_NEW_CONCEPT_GROUPS = [
    "masterbatch_additives_innovation",
    "europe_polymer_signals",
    "asia_pacific_polymer_signals",
]


# Baseline recorded before the search-coverage expansion: 107 active entity
# targets across competitors/customers/suppliers, 10 active concept groups.
_BASELINE_ACTIVE_ENTITY_TARGETS = 107


_BASELINE_ACTIVE_CONCEPT_TARGETS = 10


def _concept_targets(targets: list[dict]) -> list[dict]:
    return [t for t in targets if "zoominfo_news" not in t]


def test_targets_yaml_parses_and_discovery_settings_locked():
    """Discovery tuning must not drift as part of coverage changes."""
    config = _real_targets_yaml()
    assert config["discovery"]["results_per_entity"] == 2
    assert config["discovery"]["lookback_hours"] == 24
    assert config["discovery"]["min_article_length"] == 500


def test_targets_yaml_new_concept_groups_exist_and_are_active():
    config = _real_targets_yaml()
    for group in _NEW_CONCEPT_GROUPS:
        assert group in config, f"missing concept group: {group}"
        assert config[group]["search_mode"] == "concept"
        assert config[group]["active"] is True


def test_targets_yaml_entity_groups_precede_new_concept_groups():
    """All entity-mode groups must appear before the three new concept groups."""
    config = _real_targets_yaml()
    keys = [k for k in config if k != "discovery"]
    entity_idx = [
        i for i, k in enumerate(keys)
        if config[k].get("search_mode", "entity") == "entity"
    ]
    new_idx = [keys.index(g) for g in _NEW_CONCEPT_GROUPS]
    assert max(entity_idx) < min(new_idx)


def test_targets_yaml_floriculture_term_absent():
    raw = _TARGETS_PATH.read_text()
    assert "floriculture consumer goods" not in raw


def test_targets_yaml_fibers_no_mandatory_textiles():
    """fibers must not force every result to contain 'textiles'."""
    config = _real_targets_yaml()
    assert config["fibers"]["include_all"] == []


def test_targets_yaml_innovation_query_contents():
    targets = {t["name"]: t for t in _real_targets()}
    query = targets["masterbatch_additives_innovation"]["query"]
    assert '"new functional additive"' in query
    assert '"new additive masterbatch"' in query
    assert '"new color masterbatch"' in query
    assert '-"market report"' in query


def test_targets_yaml_regional_queries_have_geographic_anchors():
    targets = {t["name"]: t for t in _real_targets()}
    europe = targets["europe_polymer_signals"]["query"]
    assert '"European masterbatch"' in europe
    assert '"EU plastics regulation"' in europe
    apac = targets["asia_pacific_polymer_signals"]["query"]
    assert '"China polymer additives"' in apac
    assert '"India masterbatch"' in apac
    assert '"Asia Pacific masterbatch"' in apac


def test_targets_yaml_active_concept_targets_count():
    """Concept-target count = the #38 baseline (10) + 3 innovation/regional
    groups, minus the absorbed generic `economic` group, plus the 7 dedicated
    macro groups, plus 2 net from the transportation split (1 combined group
    replaced by 3)."""
    concepts = _concept_targets(_real_targets())
    assert len(concepts) == _BASELINE_ACTIVE_CONCEPT_TARGETS + 3 - 1 + len(_MACRO_GROUP_KEYS) + 2


def test_targets_yaml_entity_targets_unchanged():
    """Coverage expansion must not touch entity monitoring."""
    targets = _real_targets()
    entities = [t for t in targets if "zoominfo_news" in t]
    assert len(entities) == _BASELINE_ACTIVE_ENTITY_TARGETS


def test_targets_yaml_new_concept_groups_carry_no_zoominfo_ids():
    config = _real_targets_yaml()
    targets = {t["name"]: t for t in _real_targets()}
    for group in _NEW_CONCEPT_GROUPS:
        assert "zoominfo_company_id" not in config[group]
        assert "zoominfo_company_id" not in targets[group]


# ===========================================================================
# build_query()
# ===========================================================================


def test_build_query_entity_mode_bare():
    """Entity mode with no include_all or exclude_any produces a quoted name."""
    result = build_query(name="Shaw Industries")
    assert result == '"Shaw Industries"'


def test_build_query_entity_mode_with_excludes():
    """Entity mode exclude_any terms become -\"term\" operators."""
    result = build_query(name="Shaw Industries",
        include_all=[],
        exclude_any=["patents", "securities analyst reports"],
    )
    assert '"Shaw Industries"' in result
    assert '-"patents"' in result
    assert '-"securities analyst reports"' in result


def test_build_query_concept_mode():
    """Concept mode ORs all include_any terms and ANDs include_all."""
    result = build_query(include_any=["plastics industry", "chemical industry", "compounding"],
        include_all=["business"],
        exclude_any=[],
    )
    assert '("plastics industry" OR "chemical industry" OR "compounding")' in result
    assert '"business"' in result


def test_build_query_filters_moody_internal_excludes():
    """Moody's platform identifiers in exclude_any must be silently dropped."""
    result = build_query(include_any=["plastics industry"],
        include_all=[],
        exclude_any=["source set 238658", "PR wires", "Targeted News Search", "tenders"],
    )
    assert "source set 238658" not in result
    assert "PR wires" not in result
    assert "Targeted News Search" not in result
    assert '-"tenders"' in result   # real term must survive


def test_build_query_concept_mode_no_include_all():
    """Concept mode with empty include_all produces no spurious quoted terms."""
    result = build_query(include_any=["automotive industry"],
        include_all=[],
        exclude_any=[],
    )
    assert result == '("automotive industry")'


# ===========================================================================
# parse_targets — the catalogue over text already in hand
# ===========================================================================


def test_parse_targets_is_load_targets_without_the_file():
    """One parser, two entry points: the file loader is the text parser plus
    its one read, so a writer holding proposed text (scripts/sync_zoominfo_ids)
    validates through the same rules the cron applies at t=0."""
    text = _TARGETS_PATH.read_text(encoding="utf-8")
    assert parse_targets(text, source=str(_TARGETS_PATH)) == _real_targets()


def test_parse_targets_names_its_source_in_the_error():
    with pytest.raises(TargetsError, match=r"^targets\.yaml \(proposed\): expected a mapping"):
        parse_targets("- just\n- a list\n", source="targets.yaml (proposed)")



# ===========================================================================
# entity_entries — every entity entry in file order, active or not
# ===========================================================================

_ENTRIES_DOC = """\
    competitors:
      search_mode: entity
      entities:
        - name: Avient
          active: true
          zoominfo_company_id: 357374413
        - name: "Quoted Co"   # trailing comment
          active: True
        - name: Paused Co
          active: false
        - name: Placeholder Co
          active: true
          zoominfo_company_id: null
    industry:
      search_mode: concept
      active: true
      include_any: [plastics]
    customers:
      search_mode: entity
      entities:
        - active: true
          name: Late Name Co
    """


def test_entity_entries_lists_every_entity_in_file_order_including_inactive():
    entries = entity_entries(textwrap.dedent(_ENTRIES_DOC), source="t")
    assert [e["name"] for e in entries] == [
        "Avient", "Quoted Co", "Paused Co", "Placeholder Co", "Late Name Co"]
    assert [e["ordinal"] for e in entries] == [0, 1, 2, 3, 4]
    assert [e["group"] for e in entries] == ["competitors"] * 4 + ["customers"]
    assert [e["active"] for e in entries] == [True, True, False, True, True]


def test_entity_entries_distinguish_an_absent_id_from_a_null_placeholder():
    by_name = {e["name"]: e for e in entity_entries(textwrap.dedent(_ENTRIES_DOC), source="t")}
    assert by_name["Avient"]["zoominfo_company_id"] == 357374413 and by_name["Avient"]["has_id_key"]
    assert by_name["Quoted Co"]["zoominfo_company_id"] is None and not by_name["Quoted Co"]["has_id_key"]
    assert by_name["Placeholder Co"]["zoominfo_company_id"] is None and by_name["Placeholder Co"]["has_id_key"]


def test_entity_entries_share_the_catalogues_entity_validation(tmp_path):
    """Same walk as parse_targets: an entity the loader rejects is rejected
    here with the same message, not silently listed."""
    body = "competitors:\n  search_mode: entity\n  entities:\n    - active: true\n"
    with pytest.raises(TargetsError, match="entity #1: needs a non-empty string 'name'"):
        parse_targets(body, source="t")
    with pytest.raises(TargetsError, match="entity #1: needs a non-empty string 'name'"):
        entity_entries(body, source="t")


def test_entity_entries_decide_nothing():
    """Read-only knowledge about the file: no key says 'fill me'. The sync
    script composes its plan from these; the catalogue does not plan."""
    keys = set(entity_entries(textwrap.dedent(_ENTRIES_DOC), source="t")[0])
    assert keys == {"ordinal", "group", "name", "active", "zoominfo_company_id",
                    "has_id_key"}


def test_shipped_targets_yaml_entries_agree_with_the_loaded_entity_targets():
    """Active entries and loaded entity targets are the same population, in the
    same order."""
    entries = entity_entries(_TARGETS_PATH.read_text(encoding="utf-8"), source="targets.yaml")
    active = [(e["group"], e["name"], e["zoominfo_company_id"]) for e in entries if e["active"]]
    loaded = [(t["category"], t["name"], t["zoominfo_company_id"])
              for t in _real_targets() if t["search_mode"] == "entity"]
    assert active == loaded
    assert len(entries) >= len(active) > 0
