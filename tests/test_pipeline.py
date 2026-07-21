"""
Smoke tests for ingestion_engine.py and delivery_engine.py.
No live API calls — all external clients are mocked.
"""
import json
import textwrap
from unittest.mock import MagicMock, patch

import pytest

from llm import FakeLLM

from ingestion_engine import (
    _TextExtractor,
    _is_unscrapable_domain,
    _scrape_fallback,
    _validate_executive_bullets,
    build_query,
    compute_url_hash,
    discover_urls,
    execute_pipeline,
    generate_macro_summary,
    load_targets,
    normalize_url,
    scrape_article,
    synthesize_insight,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_openai_mock(sentiment_score: int | float) -> FakeLLM:
    return FakeLLM(returns={
        "headline": "Test Headline",
        "americhem_impact": "Test impact on Americhem.",
        "sentiment_score": sentiment_score,
        "source_url": "https://news.com/article",
        "entities_mentioned": ["Avient"],
    })


# ---------------------------------------------------------------------------
# 1. URL normalisation
# ---------------------------------------------------------------------------

def test_url_normalization():
    result = normalize_url("https://news.com/a?utm=1#sec")
    assert result == "https://news.com/a"


def test_url_normalization_preserves_path():
    result = normalize_url("https://news.com/section/article-slug")
    assert result == "https://news.com/section/article-slug"


# ---------------------------------------------------------------------------
# 2. Hash collision
# ---------------------------------------------------------------------------

def test_compute_url_hash_collision():
    clean = "https://news.com/article"
    polluted = "https://news.com/article?utm_source=newsletter&utm_medium=email&utm_campaign=weekly"
    assert compute_url_hash(normalize_url(clean)) == compute_url_hash(normalize_url(polluted))


# ---------------------------------------------------------------------------
# 3. Sentiment score clamping
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw_score, expected",
    [
        (0,  1),
        (15, 10),
    ],
)
def test_sentiment_clamp(raw_score: int, expected: int):
    with patch("ingestion_engine._llm", return_value=_make_openai_mock(raw_score)):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["sentiment_score"] == expected


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
    config_file = tmp_path / "targets.yaml"
    config_file.write_text(config_yaml)
    targets = load_targets(str(config_file))
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


# ---------------------------------------------------------------------------
# Per-group results_per_entity override (priority-segment discovery volume)
# ---------------------------------------------------------------------------

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
    config_file = tmp_path / "targets.yaml"
    config_file.write_text(config_yaml)
    targets = load_targets(str(config_file))
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
    config_file = tmp_path / "targets.yaml"
    config_file.write_text(config_yaml)
    targets = load_targets(str(config_file))
    assert targets[0]["results_per_entity"] == 2


def test_tail_scrape_demand_reflects_per_group_override(tmp_path):
    """_tail_scrape_demand sums each concept target's own results_per_entity,
    so a per-group override raises the derived tail reserve automatically."""
    import ingestion_engine

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
    config_file = tmp_path / "targets.yaml"
    config_file.write_text(config_yaml)
    targets = load_targets(str(config_file))
    assert ingestion_engine._tail_scrape_demand(targets) == 4 + 2


# ---------------------------------------------------------------------------
# Dedicated macroeconomic discovery targets (PR 2, Task 7)
# ---------------------------------------------------------------------------

_MACRO_GROUP_KEYS = [
    "macro_manufacturing",
    "macro_construction",
    "macro_automotive",
    "macro_consumer_demand",
    "macro_inflation_rates",
    "macro_energy_freight",
    "macro_business_investment",
]


def _load_targets_yaml() -> dict:
    import yaml
    with open("targets.yaml") as fh:
        return yaml.safe_load(fh)


def test_targets_yaml_has_active_macro_concept_groups():
    """The dedicated macro concept groups are present, active, and load as
    concept targets covering the seven macro domains."""
    targets = load_targets("targets.yaml")
    categories = {t["category"] for t in targets}
    assert set(_MACRO_GROUP_KEYS) <= categories


def test_targets_yaml_generic_economic_group_removed():
    """The old generic `economic` group is absorbed by the dedicated macro
    groups and no longer exists (do not run both)."""
    cfg = _load_targets_yaml()
    assert "economic" not in cfg


def test_targets_yaml_macro_groups_are_last_in_file_order():
    """Macro groups occupy the final positions in file order — targets process
    in file order, so a deadline-limited run sacrifices macro before entity
    coverage (graceful degradation by construction)."""
    cfg = _load_targets_yaml()
    keys = [k for k in cfg if k != "discovery"]
    assert keys[-len(_MACRO_GROUP_KEYS):] == _MACRO_GROUP_KEYS


def test_macro_groups_are_concept_mode():
    """Each macro group is a concept-mode group (one combined OR query)."""
    cfg = _load_targets_yaml()
    for key in _MACRO_GROUP_KEYS:
        assert cfg[key]["search_mode"] == "concept"
        assert cfg[key]["active"] is True


# ---------------------------------------------------------------------------
# Priority-segment discovery: transportation split + B&C retune (real file)
# ---------------------------------------------------------------------------

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
    cfg = _load_targets_yaml()
    assert "transportation" not in cfg
    targets = load_targets("targets.yaml")
    concept_names = {t["name"] for t in targets if t["search_mode"] == "concept"}
    assert set(_TRANSPORTATION_SPLIT_KEYS) <= concept_names


def test_targets_yaml_priority_segments_have_raised_volume():
    """Each priority-segment concept group carries results_per_entity 4; the
    global default stays 2 for everything else."""
    targets = load_targets("targets.yaml")
    by_name = {t["name"]: t for t in targets}
    for key in _PRIORITY_SEGMENT_KEYS:
        assert by_name[key]["results_per_entity"] == 4, key


def test_targets_yaml_macro_groups_stay_at_global_volume():
    """Macro groups must NOT be raised — that would inflate the tail reserve
    and shrink entity coverage."""
    targets = load_targets("targets.yaml")
    by_name = {t["name"]: t for t in targets}
    for key in _MACRO_GROUP_KEYS:
        assert by_name[key]["results_per_entity"] == 2, key


def test_targets_yaml_building_construction_excludes_real_estate():
    """building_construction has a non-empty query, carries no real-estate term
    as a positive (include_any) match, and excludes real-estate noise."""
    cfg = _load_targets_yaml()
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
    targets = load_targets("targets.yaml")
    bc_query = next(t["query"] for t in targets if t["name"] == "building_construction")
    assert bc_query.strip()


def test_targets_yaml_all_concept_queries_nonempty():
    """Every active concept target must produce a non-empty, well-formed query."""
    targets = load_targets("targets.yaml")
    for t in targets:
        if t["search_mode"] == "concept":
            assert t["query"].strip(), t["name"]
            assert t["query"].count("(") == t["query"].count(")"), t["name"]


# ---------------------------------------------------------------------------
# Ingestion target priority order (Dispatch 2 — four-tier discovery order).
# YAML file order IS the graceful-degradation policy: a heavy news day that
# exhausts the scrape/time budget mid-run drops the lowest tiers first, so the
# highest-value commercial-segment coverage survives. Pin only the relational
# invariants below — do not snapshot the full target list.
# ---------------------------------------------------------------------------

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
    targets = load_targets("targets.yaml")
    first_eight = [t["name"] for t in targets[:8]]
    assert first_eight == _TIER1_PRIORITY_ORDER


def test_all_entity_targets_follow_every_tier1_target():
    """Every entity target sits below every Tier 1 target — Tier 2 (entities)
    is sacrificed before Tier 1 when the budget runs out mid-list."""
    targets = load_targets("targets.yaml")
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
    targets = load_targets("targets.yaml")
    trailing = [t["name"] for t in targets[-len(_MACRO_GROUP_KEYS):]]
    assert trailing == _MACRO_GROUP_KEYS


def test_known_inactive_entities_stay_absent():
    """Reordering must not resurrect paused/duplicate entities."""
    targets = load_targets("targets.yaml")
    names = {t["name"] for t in targets}
    for inactive in ("Polymax", "Performance Plastics", "Lexmark", "AdvanSix Resin"):
        assert inactive not in names


def test_all_loaded_queries_nonempty_after_reorder():
    """Every loaded target (entity and concept) still produces a query."""
    targets = load_targets("targets.yaml")
    assert targets
    for t in targets:
        assert t["query"].strip(), t["name"]


# ---------------------------------------------------------------------------
# 5. DISCARD signal
# ---------------------------------------------------------------------------

def test_discard_signal_detected():
    insight = {"americhem_impact": "DISCARD"}
    assert insight.get("americhem_impact") == "DISCARD"


# ---------------------------------------------------------------------------
# 6. recommended_action soft default
# ---------------------------------------------------------------------------

def _make_openai_mock_no_action(sentiment_score: int) -> FakeLLM:
    return FakeLLM(returns={
        "headline": "Test Headline",
        "americhem_impact": "Test impact on Americhem.",
        "sentiment_score": sentiment_score,
        "source_url": "https://news.com/article",
        "entities_mentioned": ["Avient"],
    })


def _make_openai_mock_invalid_action(sentiment_score: int) -> FakeLLM:
    return FakeLLM(returns={
        "headline": "Test Headline",
        "americhem_impact": "Test impact on Americhem.",
        "sentiment_score": sentiment_score,
        "recommended_action": "Do something weird",
        "source_url": "https://news.com/article",
        "entities_mentioned": ["Avient"],
    })


@pytest.mark.parametrize("mock_fn", [_make_openai_mock_no_action, _make_openai_mock_invalid_action])
def test_recommended_action_default(mock_fn):
    with patch("ingestion_engine._llm", return_value=mock_fn(5)):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["recommended_action"] == "Monitor"


# ---------------------------------------------------------------------------
# 8. article_summary soft default
# ---------------------------------------------------------------------------

def test_article_summary_default():
    with patch("ingestion_engine._llm", return_value=_make_openai_mock(5)):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["article_summary"] == ""


def test_synthesize_insight_uses_low_temperature():
    # Model + json-format are the adapter's contract (see test_llm.py); the caller
    # owns the temperature it requests across the seam.
    fake = _make_openai_mock(5)

    with patch("ingestion_engine._llm", return_value=fake):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )

    assert result is not None
    assert fake.calls[-1]["temperature"] == 0.2


def test_generate_macro_summary_uses_macro_temperature():
    fake = FakeLLM(returns={
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Body."},
            {"label": "Supply chain watch", "body": "Body."},
            {"label": "Commercial action", "body": "Body."},
        ],
    })
    fake_repo = InMemoryIntelligenceRepo()

    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        result = generate_macro_summary(
            [
                {
                    "category": "competitors",
                    "headline": "Headline",
                    "sentiment_score": 5,
                    "americhem_impact": "Impact.",
                }
            ]
        )

    assert result is True
    assert fake.calls[-1]["temperature"] == 0.3


# ---------------------------------------------------------------------------
# 9. _render_card() article_summary rendering
# ---------------------------------------------------------------------------

from delivery_engine import (
    _render_card,
    synthesize_thematic_paragraphs,
    assemble_report,
    prepare_report,
    render_report,
)
from report import _config_int

_TODAY_STR = "Thursday, July 02, 2026"


def test_render_card_omits_article_summary():
    """article_summary must never appear in card HTML regardless of content."""
    item = {
        "headline": "Test Headline",
        "source_url": "https://news.com/article",
        "americhem_impact": "Some impact.",
        "category": "competitors",
        "sentiment_score": 5,
        "source_publication": "Reuters",
        "sentiment_rationale": "Neutral article.",
        "recommended_action": "Monitor",
        "article_summary": "BASF announced a new plant in Germany.",
    }
    html = _render_card(item)
    assert "BASF announced a new plant in Germany." not in html


# ---------------------------------------------------------------------------
# 10. send_email() HTTP retry behaviour
# ---------------------------------------------------------------------------

import time as _time

import requests as _requests

from delivery_engine import send_email as _send_email


def _email_env(monkeypatch) -> None:
    monkeypatch.setenv("SMTP_PASS", "re_test_key")
    monkeypatch.setenv("SENDER_EMAIL", "noreply@test.com")
    monkeypatch.setenv("RECIPIENT_EMAILS", "user@test.com")


def test_send_email_retries_on_429_then_succeeds(monkeypatch):
    _email_env(monkeypatch)
    monkeypatch.setattr(_time, "sleep", lambda s: None)

    attempt = {"count": 0}

    def fake_post(*args, **kwargs):
        attempt["count"] += 1
        resp = MagicMock()
        if attempt["count"] == 1:
            resp.status_code = 429
            resp.raise_for_status = MagicMock()
        else:
            resp.status_code = 200
            resp.raise_for_status = MagicMock()
        return resp

    monkeypatch.setattr(_requests, "post", fake_post)
    _send_email("<html>test</html>")
    assert attempt["count"] == 2


def test_send_email_raises_immediately_on_auth_failure(monkeypatch):
    _email_env(monkeypatch)
    monkeypatch.setattr(_time, "sleep", lambda s: None)

    attempt = {"count": 0}

    def fake_post(*args, **kwargs):
        attempt["count"] += 1
        resp = MagicMock()
        resp.status_code = 403
        resp.ok = False
        http_err = _requests.HTTPError()
        http_err.response = resp
        resp.raise_for_status = MagicMock(side_effect=http_err)
        return resp

    monkeypatch.setattr(_requests, "post", fake_post)

    with pytest.raises(_requests.HTTPError):
        _send_email("<html>test</html>")

    assert attempt["count"] == 1  # must NOT have retried


# ---------------------------------------------------------------------------
# 11. _TextExtractor — visible text extraction
# ---------------------------------------------------------------------------

def test_text_extractor_strips_tags():
    """_TextExtractor must return visible text with HTML tags removed."""
    html = "<html><body><p>Hello <b>World</b></p></body></html>"
    extractor = _TextExtractor()
    extractor.feed(html)
    text = extractor.get_text()
    assert "Hello" in text
    assert "World" in text
    assert "<" not in text


def test_text_extractor_skips_script_and_style():
    """_TextExtractor must ignore script/style/noscript/nav/footer/header/aside/form content."""
    html = (
        "<html><head><style>body{color:red}</style></head>"
        "<body><script>alert(1)</script><p>Article text here.</p>"
        "<footer>Copyright 2026</footer>"
        "<aside>Subscribe now</aside>"
        "<form>Enter email</form></body></html>"
    )
    extractor = _TextExtractor()
    extractor.feed(html)
    text = extractor.get_text()
    assert "Article text here." in text
    assert "alert" not in text
    assert "body{color:red}" not in text
    assert "Copyright 2026" not in text
    assert "Subscribe now" not in text
    assert "Enter email" not in text


# ---------------------------------------------------------------------------
# 12. _scrape_fallback — direct-HTTP fallback
# ---------------------------------------------------------------------------

def test_scrape_fallback_returns_text_on_success():
    """_scrape_fallback must return extracted text when the HTTP request succeeds."""
    mock_resp = MagicMock()
    mock_resp.text = "<html><body><p>Chemical plant update with details.</p></body></html>"
    mock_resp.raise_for_status = MagicMock()

    with patch("ingestion_engine.requests.get", return_value=mock_resp):
        result = _scrape_fallback("https://example.com/article")

    assert result is not None
    assert "Chemical plant update" in result


def test_scrape_fallback_returns_none_on_request_error():
    """_scrape_fallback must return None when the HTTP request fails."""
    import requests as _req
    with patch("ingestion_engine.requests.get", side_effect=_req.exceptions.ConnectionError("refused")):
        result = _scrape_fallback("https://example.com/article")
    assert result is None


# ---------------------------------------------------------------------------
# 13. scrape_article — 402 triggers fallback
# ---------------------------------------------------------------------------

def _make_http_error(status_code: int) -> MagicMock:
    """Return a requests.HTTPError mock with the given status code."""
    import requests as _req
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    err = _req.exceptions.HTTPError(response=mock_resp)
    return err


def test_scrape_article_uses_fallback_on_402(monkeypatch):
    """scrape_article must invoke the fallback when Firecrawl returns HTTP 402."""
    import requests as _req

    # Firecrawl returns 402
    firecrawl_resp = MagicMock()
    firecrawl_resp.raise_for_status.side_effect = _make_http_error(402)

    # Fallback returns long enough text
    fallback_text = "A" * 600

    monkeypatch.setenv("FIRECRAWL_API_KEY", "test_key")

    with patch("ingestion_engine.requests.post", return_value=firecrawl_resp), \
         patch("ingestion_engine._scrape_fallback", return_value=fallback_text) as mock_fallback:
        result = scrape_article("https://example.com/article", min_length=500)

    mock_fallback.assert_called_once_with("https://example.com/article")
    assert result == fallback_text


def test_scrape_article_returns_none_when_fallback_content_too_short(monkeypatch):
    """scrape_article must return None when fallback text is below min_length."""
    import requests as _req

    firecrawl_resp = MagicMock()
    firecrawl_resp.raise_for_status.side_effect = _make_http_error(402)

    monkeypatch.setenv("FIRECRAWL_API_KEY", "test_key")

    with patch("ingestion_engine.requests.post", return_value=firecrawl_resp), \
         patch("ingestion_engine._scrape_fallback", return_value="too short"):
        result = scrape_article("https://example.com/article", min_length=500)

    assert result is None


def test_scrape_article_returns_none_when_fallback_fails(monkeypatch):
    """scrape_article must return None when both Firecrawl (402) and fallback fail."""
    import requests as _req

    firecrawl_resp = MagicMock()
    firecrawl_resp.raise_for_status.side_effect = _make_http_error(402)

    monkeypatch.setenv("FIRECRAWL_API_KEY", "test_key")

    with patch("ingestion_engine.requests.post", return_value=firecrawl_resp), \
         patch("ingestion_engine._scrape_fallback", return_value=None):
        result = scrape_article("https://example.com/article", min_length=500)

    assert result is None


def test_scrape_article_no_fallback_on_non_402_error(monkeypatch):
    """scrape_article must NOT invoke the fallback for non-402 Firecrawl errors."""
    import requests as _req

    firecrawl_resp = MagicMock()
    firecrawl_resp.raise_for_status.side_effect = _make_http_error(500)

    monkeypatch.setenv("FIRECRAWL_API_KEY", "test_key")

    with patch("ingestion_engine.requests.post", return_value=firecrawl_resp), \
         patch("ingestion_engine._scrape_fallback") as mock_fallback:
        result = scrape_article("https://example.com/article", min_length=500)

    mock_fallback.assert_not_called()
    assert result is None


# ---------------------------------------------------------------------------
# 14. _render_card() — no ACTION line (deliberately dropped in the 2026-05-21
#     commercial-brief redesign; recommended_action is a suppression override,
#     not reader-facing copy)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("action", ["Monitor", "Escalate to leadership"])
def test_render_card_never_renders_action_line(action):
    """The shipped card shows no ACTION line for any recommended_action."""
    item = {
        "headline": "Plant fire halts BASF production",
        "source_url": "https://news.com/article",
        "americhem_impact": "Direct feedstock disruption risk.",
        "category": "suppliers",
        "sentiment_score": 2,
        "recommended_action": action,
    }
    html = _render_card(item)
    assert "ACTION:" not in html


# ---------------------------------------------------------------------------
# 15. generate_macro_summary()
# ---------------------------------------------------------------------------

from ingestion_engine import generate_macro_summary


def test_generate_macro_summary_empty_articles_persists_accounting_only_row():
    """Zero stored articles must still persist the run's ingestion accounting
    (issue #43): an accounting-only daily_summaries row carrying screened_count
    and the suppression breakdown/samples, returning False (no summary was
    generated). Content columns are OMITTED from the payload — not written as
    null — so a same-day retry can never wipe an earlier full summary."""
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._repo", lambda: fake_repo):
        result = generate_macro_summary(
            [],
            screened_count=17,
            suppression_breakdown={"duplicate_url": 9, "unscrapable_domain": 2},
            suppression_samples=[{"reason": "duplicate_url", "url": "u", "title": "t"}],
        )
    assert result is False
    stored = fake_repo.fetch_latest_summary(run_mode="production", min_date="2000-01-01")
    assert stored is not None
    assert stored["screened_count"] == 17
    assert stored["suppression_breakdown"] == {"duplicate_url": 9, "unscrapable_domain": 2}
    assert stored["suppression_samples"] == [
        {"reason": "duplicate_url", "url": "u", "title": "t"}
    ]
    for content_key in ("executive_summary", "macro_sentiment", "dominant_condition",
                        "executive_bullets", "macro_outlook", "executive_sources"):
        assert content_key not in stored, f"{content_key} must be omitted, not written"


# ---------------------------------------------------------------------------
# Macro-outlook validation + persistence (PR 2, Task 9)
# ---------------------------------------------------------------------------

from ingestion_engine import _validate_macro_outlook

_MACRO_VALID_IDS = frozenset({1, 2, 3})


def _macro_signal(**over) -> dict:
    sig = {
        "indicator": "Manufacturing PMI",
        "direction": "Declining",
        "americhem_implication": "Downside risk for industrial resin demand.",
        "affected_segments": ["Industrial"],
        "citation_source_ids": [1],
    }
    sig.update(over)
    return sig


def _macro_outlook(**over) -> dict:
    out = {"current_condition": "Manufacturing demand mixed.", "signals": [_macro_signal()]}
    out.update(over)
    return out


def test_validate_macro_outlook_accepts_material_signal():
    result = _validate_macro_outlook(_macro_outlook(), _MACRO_VALID_IDS)
    assert result is not None
    assert result["current_condition"] == "Manufacturing demand mixed."
    assert len(result["signals"]) == 1
    assert result["signals"][0]["direction"] == "Declining"
    assert result["signals"][0]["affected_segments"] == ["Industrial"]
    assert result["signals"][0]["citation_source_ids"] == [1]


def test_validate_macro_outlook_empty_signals_is_none():
    assert _validate_macro_outlook(_macro_outlook(signals=[]), _MACRO_VALID_IDS) is None


def test_validate_macro_outlook_non_dict_is_none():
    assert _validate_macro_outlook(None, _MACRO_VALID_IDS) is None
    assert _validate_macro_outlook("nope", _MACRO_VALID_IDS) is None


def test_validate_macro_outlook_blank_current_condition_is_none():
    assert _validate_macro_outlook(_macro_outlook(current_condition="  "), _MACRO_VALID_IDS) is None


def test_validate_macro_outlook_drops_signal_without_citation():
    """Materiality gate: an uncitable signal is dropped; a lone uncitable signal
    yields no section."""
    out = _macro_outlook(signals=[_macro_signal(citation_source_ids=[])])
    assert _validate_macro_outlook(out, _MACRO_VALID_IDS) is None


def test_validate_macro_outlook_drops_signal_with_only_invalid_citations():
    out = _macro_outlook(signals=[_macro_signal(citation_source_ids=[99])])
    assert _validate_macro_outlook(out, _MACRO_VALID_IDS) is None


def test_validate_macro_outlook_rejects_invalid_direction():
    out = _macro_outlook(signals=[_macro_signal(direction="Sideways")])
    assert _validate_macro_outlook(out, _MACRO_VALID_IDS) is None


def test_validate_macro_outlook_rejects_invalid_segment():
    out = _macro_outlook(signals=[_macro_signal(affected_segments=["Consumer Goods"])])
    assert _validate_macro_outlook(out, _MACRO_VALID_IDS) is None


def test_validate_macro_outlook_accepts_building_construction_segment():
    out = _macro_outlook(signals=[_macro_signal(affected_segments=["Building & Construction"])])
    assert _validate_macro_outlook(out, _MACRO_VALID_IDS) is not None


def test_validate_macro_outlook_rejects_blank_fields():
    assert _validate_macro_outlook(
        _macro_outlook(signals=[_macro_signal(indicator="  ")]), _MACRO_VALID_IDS) is None
    assert _validate_macro_outlook(
        _macro_outlook(signals=[_macro_signal(americhem_implication="")]), _MACRO_VALID_IDS) is None


def test_validate_macro_outlook_keeps_only_valid_signals():
    """A mix of valid + invalid signals keeps only the valid ones."""
    out = _macro_outlook(signals=[
        _macro_signal(indicator="Manufacturing PMI"),
        _macro_signal(direction="Sideways"),                       # bad direction
        _macro_signal(indicator="Construction starts", citation_source_ids=[2]),
    ])
    result = _validate_macro_outlook(out, _MACRO_VALID_IDS)
    assert [s["indicator"] for s in result["signals"]] == ["Manufacturing PMI", "Construction starts"]


def test_validate_macro_outlook_truncates_at_cap():
    """The validator keeps at most MAX_MACRO_OUTLOOK_SIGNALS signals, and the
    product cap is 3 (reduced from 6 on 2026-07-17 for report density)."""
    from prompts import MAX_MACRO_OUTLOOK_SIGNALS

    assert MAX_MACRO_OUTLOOK_SIGNALS == 3
    signals = [_macro_signal(indicator=f"Indicator {i}") for i in range(5)]
    result = _validate_macro_outlook(_macro_outlook(signals=signals), _MACRO_VALID_IDS)
    assert [s["indicator"] for s in result["signals"]] == [
        "Indicator 0", "Indicator 1", "Indicator 2",
    ]


def _macro_articles() -> list[dict]:
    return [
        {"category": "macro_manufacturing", "headline": "Manufacturing PMI slips into contraction",
         "americhem_impact_score": 9, "americhem_impact": "Industrial demand softening.",
         "signal_type": "Macro", "url_hash": "m1", "source_url": "https://x/1"},
        {"category": "macro_construction", "headline": "Housing starts fall for third month",
         "americhem_impact_score": 8, "americhem_impact": "Building products demand risk.",
         "signal_type": "Macro", "url_hash": "m2", "source_url": "https://x/2"},
        {"category": "competitors", "headline": "Competitor opens new compounding line",
         "americhem_impact_score": 7, "americhem_impact": "Capacity pressure.",
         "signal_type": "Competitive", "url_hash": "c1", "source_url": "https://x/3"},
    ]


def test_generate_macro_summary_persists_macro_outlook_and_union_sources():
    """generate_macro_summary validates + persists macro_outlook and packs
    executive_sources as the UNION of bullet-cited and signal-cited sources."""
    fake = FakeLLM(returns={
        "dominant_condition": "Demand Softness",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Industrial demand cooling.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Feedstock steady.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Engage key accounts.", "citation_source_ids": []},
        ],
        "macro_outlook": {
            "current_condition": "Industrial and construction demand both softening.",
            "signals": [
                {"indicator": "Housing starts", "direction": "Declining",
                 "americhem_implication": "Weakness in Building & Construction-adjacent volumes.",
                 "affected_segments": ["Industrial"], "citation_source_ids": [2]},
            ],
        },
    })
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        result = generate_macro_summary(_macro_articles())

    assert result is True
    stored = fake_repo.fetch_latest_summary(run_mode="production", min_date="2000-01-01")
    assert stored["macro_outlook"] is not None
    assert [s["indicator"] for s in stored["macro_outlook"]["signals"]] == ["Housing starts"]
    # executive_sources is the union of bullet-cited (1) and signal-cited (2).
    assert {s["id"] for s in stored["executive_sources"]} == {1, 2}


def test_generate_macro_summary_llm_none_persists_accounting_only_row():
    """An LLM transport failure (None) yields False and no summary content —
    but the run's ingestion accounting must still be persisted (issue #43),
    exactly as on a zero-yield run."""
    fake = FakeLLM(returns=None)
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        result = generate_macro_summary(
            _macro_articles(),
            screened_count=5,
            suppression_breakdown={"scrape_failed": 1},
        )
    assert result is False
    stored = fake_repo.fetch_latest_summary(run_mode="production", min_date="2000-01-01")
    assert stored is not None
    assert stored["screened_count"] == 5
    assert stored["suppression_breakdown"] == {"scrape_failed": 1}
    assert "executive_summary" not in stored
    assert "executive_bullets" not in stored


def test_generate_macro_summary_zero_yield_retry_keeps_earlier_content():
    """Same-day retry: a morning run wrote a full summary; an afternoon retry
    that stores zero articles refreshes the accounting columns WITHOUT wiping
    the morning's summary content (column-subset upsert)."""
    fake = FakeLLM(returns={
        "dominant_condition": "Demand Softness",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Industrial demand cooling.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Feedstock steady.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Engage key accounts.", "citation_source_ids": []},
        ],
    })
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        assert generate_macro_summary(
            _macro_articles(), screened_count=40,
            suppression_breakdown={"duplicate_url": 3},
        ) is True
    with patch("ingestion_engine._repo", lambda: fake_repo):
        assert generate_macro_summary(
            [], screened_count=12,
            suppression_breakdown={"duplicate_url": 12},
        ) is False
    stored = fake_repo.fetch_latest_summary(run_mode="production", min_date="2000-01-01")
    assert stored["dominant_condition"] == "Demand Softness"
    assert len(stored["executive_bullets"]) == 3
    assert stored["screened_count"] == 12
    assert stored["suppression_breakdown"] == {"duplicate_url": 12}


def test_generate_macro_summary_malformed_outlook_keeps_bullets():
    """A malformed macro_outlook key degrades to None while the executive
    bullets survive — per-key validation, one call, independent failure."""
    fake = FakeLLM(returns={
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Industrial steady.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Feedstock steady.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Engage.", "citation_source_ids": []},
        ],
        "macro_outlook": "totally not an object",
    })
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        result = generate_macro_summary(_macro_articles())
    assert result is True
    stored = fake_repo.fetch_latest_summary(run_mode="production", min_date="2000-01-01")
    assert stored["macro_outlook"] is None
    assert stored["executive_bullets"] is not None
    assert len(stored["executive_bullets"]) == 3


def test_macro_outlook_cites_card_suppressed_article():
    """A macro article suppressed as a card (generic-market-report title, no
    entities) can still be cited by the outlook — the outlook renders from the
    summary row, independent of card visibility."""
    suppressed = _make_new_article("supp", 8, commercial_segment="Industrial",
                                   headline="Global polymer market outlook to reach $50 billion")
    suppressed["entities_mentioned"] = []
    visible = _make_new_article("vis", 8, commercial_segment="Packaging",
                                headline="Packaging supply disruption raises converter costs")
    macro = {
        "macro_outlook": {
            "current_condition": "Industrial demand softening.",
            "signals": [
                {"indicator": "Manufacturing PMI", "direction": "Declining",
                 "americhem_implication": "Downside risk for industrial compound demand.",
                 "affected_segments": ["Industrial"], "citation_source_ids": [7]},
            ],
        },
        "executive_sources": [
            {"id": 7, "headline": "Global polymer market outlook report",
             "url": "https://s/7", "domain": "m.com"},
        ],
    }
    config = {
        "reporting": {"visible_impact_threshold": 6},
        "delivery_suppression": {"title_patterns_generic_market_report": ["market outlook", "to reach $"]},
    }
    model = assemble_report([suppressed, visible], macro_summary=macro, config=config)
    card_hashes = {a["url_hash"] for arts in model.groups.values() for a in arts}
    assert "supp" not in card_hashes                      # suppressed as a card
    html = render_report(model, today_str=_TODAY_STR)
    assert _MACRO_TITLE in html                           # outlook still renders
    assert "Global polymer market outlook report" in html  # cited in Sources footer


def test_fetch_macro_summary_passes_macro_outlook_through(monkeypatch):
    """Delivery's fetch_macro_summary returns the row verbatim, so macro_outlook
    (incl. the test-mode production-row fallback) is carried along for free."""
    import delivery_engine
    from datetime import date
    fake_repo = InMemoryIntelligenceRepo()
    fake_repo.upsert_summary({
        "run_date": date.today().isoformat(),
        "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "Mixed / Watch",
        "macro_outlook": _VALID_MACRO_OUTLOOK,
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake_repo)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    summary = delivery_engine.fetch_macro_summary()
    assert summary["macro_outlook"] == _VALID_MACRO_OUTLOOK


def test_generate_macro_summary_persists_none_when_no_material_signal():
    """When macro_outlook has no material signal, None is persisted (no section)."""
    fake = FakeLLM(returns={
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Steady.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Steady.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Steady.", "citation_source_ids": []},
        ],
        "macro_outlook": {"current_condition": "Quiet.", "signals": []},
    })
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        generate_macro_summary(_macro_articles())
    stored = fake_repo.fetch_latest_summary(run_mode="production", min_date="2000-01-01")
    assert stored["macro_outlook"] is None



# ---------------------------------------------------------------------------
# 16. _render_card() — article_summary must not appear in rendered HTML
# ---------------------------------------------------------------------------

def test_render_card_excludes_article_summary():
    """article_summary must not appear in rendered card HTML."""
    item = {
        "headline": "Test headline",
        "source_url": "https://example.com",
        "americhem_impact": "Some impact.",
        "category": "markets",
        "sentiment_score": 5,
        "article_summary": "This is the article summary text.",
    }
    html = _render_card(item)
    assert "This is the article summary text." not in html


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


# ===========================================================================
# Thematic synthesis helpers — shared fixture
# ===========================================================================

def _make_article(
    url_hash: str,
    score: int,
    category: str | None,
    headline: str = "Test Headline",
) -> dict:
    return {
        "url_hash": url_hash,
        "sentiment_score": score,
        "category": category,
        "headline": headline,
        "americhem_impact": "Some impact.",
        "entities_mentioned": ["TestCorp"],
        "source_url": "https://news.com/article",
    }


# ---------------------------------------------------------------------------
# Task 4 — synthesize_thematic_paragraphs
# ---------------------------------------------------------------------------

def _make_synthesis_mock(paragraphs: dict) -> FakeLLM:
    return FakeLLM(returns=paragraphs)


def test_synthesize_thematic_paragraphs_returns_paragraphs():
    """Returns dict of {category: paragraph} on success."""
    groups = {
        "competitors": [
            _make_article("a", 8, "competitors"),
            _make_article("b", 7, "competitors"),
        ]
    }
    expected = {"competitors": "Avient and Techmer raised prices."}
    mock_client = _make_synthesis_mock(expected)

    with patch("delivery_engine._llm", return_value=mock_client):
        result = synthesize_thematic_paragraphs(groups)

    assert result == expected


def test_synthesize_thematic_paragraphs_passes_grouped_text_to_seam():
    """The caller sends one request carrying the grouped category text."""
    groups = {
        "suppliers": [
            _make_article("a", 4, "suppliers"),
            _make_article("b", 5, "suppliers"),
        ]
    }
    fake = _make_synthesis_mock({"suppliers": "Supply chain tightening."})

    with patch("delivery_engine._llm", return_value=fake):
        synthesize_thematic_paragraphs(groups)

    assert len(fake.calls) == 1
    assert "CATEGORY: suppliers" in fake.calls[-1]["user"]


def test_synthesize_thematic_paragraphs_empty_groups():
    """Returns {} immediately without touching the seam when groups is empty."""
    fake = FakeLLM()

    with patch("delivery_engine._llm", return_value=fake):
        result = synthesize_thematic_paragraphs({})

    assert fake.calls == []
    assert result == {}


def test_synthesize_thematic_paragraphs_graceful_degradation():
    """Returns {} when the seam yields no usable response — does not re-raise."""
    groups = {
        "competitors": [
            _make_article("a", 7, "competitors"),
            _make_article("b", 8, "competitors"),
        ]
    }
    # The seam swallows transport/parse failures and returns None.
    with patch("delivery_engine._llm", return_value=FakeLLM(returns=None)):
        result = synthesize_thematic_paragraphs(groups)

    assert result == {}


# ---------------------------------------------------------------------------
# Task 7 — report assembly + rendering integration
# ---------------------------------------------------------------------------

def test_report_legacy_critical_old_sections_gone():
    """Legacy sentiment_score<=3 rows fall below the visible threshold (6) via the
    sentiment_score fallback, and the pre-redesign section labels never render."""
    data = [
        {"url_hash": "c0", "sentiment_score": 2, "category": "suppliers",
         "headline": "Legacy critical headline about plant fire",
         "americhem_impact": "Disruption.",
         "entities_mentioned": ["BASF"], "source_url": "https://x/0",
         "commercial_segment": "Enterprise / Cross-Segment"},
    ]
    # Note: this legacy row has no americhem_impact_score, so the visibility filter
    # uses sentiment_score=2 -> effective_impact <= 3, which is BELOW the visible
    # threshold (6). So the row will not surface in the segment watch under the
    # current threshold filter. What we DO assert: the old section labels are gone
    # and Peripheral Signals is hidden in production. The CRITICAL badge behaviour
    # is unit-tested directly via test_render_segment_watch_section_critical_badge_for_legacy_low_score.
    model = assemble_report(data, config={"reporting": {"visible_impact_threshold": 6}})
    html = render_report(model, today_str=_TODAY_STR)
    assert "PERIPHERAL SIGNALS" not in html
    assert "CRITICAL DISRUPTIONS" not in html
    assert "THEMATIC INTELLIGENCE" not in html


def test_report_routes_two_plus_to_segment_watch():
    """Two articles in the same commercial_segment produce a Commercial Segment
    Watch block with a synthesis paragraph."""
    data = [
        {"url_hash": "a", "commercial_segment": "Healthcare",
         "americhem_impact_score": 7, "sentiment_tag": "Positive",
         "signal_type": "Customer", "headline": "Avient expands healthcare polymer line",
         "americhem_impact": "Effect.", "source_url": "https://x/a",
         "entities_mentioned": ["Avient"]},
        {"url_hash": "b", "commercial_segment": "Healthcare",
         "americhem_impact_score": 8, "sentiment_tag": "Positive",
         "signal_type": "Customer", "headline": "Techmer launches sterilizable compound line",
         "americhem_impact": "Effect.", "source_url": "https://x/b",
         "entities_mentioned": ["Techmer"]},
    ]
    model = assemble_report(data, config={"reporting": {"visible_impact_threshold": 6}})
    assert model.synthesis_candidates() == {"Healthcare": model.groups["Healthcare"]}
    html = render_report(
        model.with_synthesis({"Healthcare": "Synthesis paragraph here."}),
        today_str=_TODAY_STR,
    )
    assert "COMMERCIAL SEGMENT WATCH" in html
    assert "Healthcare" in html
    assert "Synthesis paragraph here." in html
    assert "THEMATIC INTELLIGENCE" not in html


def test_report_single_low_relevance_not_a_visible_card():
    """An impact-5 article is never a visible card (no Peripheral Signals
    section), but IS surfaced in the optional-discovery appendix — so it is
    not counted as weak_relevance."""
    data = [{"url_hash": "x", "commercial_segment": "Packaging",
             "americhem_impact_score": 5, "sentiment_tag": "Neutral",
             "signal_type": "Customer",
             "headline": "Low relevance packaging signal",
             "americhem_impact": ".", "source_url": "https://x/p",
             "entities_mentioned": ["Acme"]}]
    model = assemble_report(data, config={"reporting": {"visible_impact_threshold": 6}})
    assert model.groups == {}
    assert [a["url_hash"] for a in model.additional_articles] == ["x"]
    assert model.ledger.breakdown.get("weak_relevance", 0) == 0
    html = render_report(model, today_str=_TODAY_STR)
    assert "PERIPHERAL SIGNALS" not in html


# ---------------------------------------------------------------------------
# Pipeline deadline early-exit
# ---------------------------------------------------------------------------

from ingestion_engine import execute_pipeline


def test_execute_pipeline_deadline_calls_log_stats_and_macro_summary(monkeypatch, tmp_path):
    """When the pipeline deadline is exceeded mid-batch, _log_stats and
    generate_macro_summary must be called before the function returns."""
    import textwrap
    import ingestion_engine

    # Write a minimal targets.yaml with one active entity
    config_yaml = textwrap.dedent(
        """\
        competitors:
          search_mode: entity
          include_all: []
          exclude_any: []
          entities:
            - name: TestCorp
              active: true
        discovery:
          results_per_entity: 2
          lookback_hours: 24
          min_article_length: 500
        """
    )
    cfg_file = tmp_path / "targets.yaml"
    cfg_file.write_text(config_yaml)

    call_count = {"n": 0}

    def fake_monotonic():
        call_count["n"] += 1
        # First call (pipeline_start assignment) returns 0; subsequent calls
        # return a value past the deadline so the mid-batch check fires.
        if call_count["n"] == 1:
            return 0.0
        return float(ingestion_engine.PIPELINE_DEADLINE_SECONDS + 1)

    monkeypatch.setattr(ingestion_engine.time, "monotonic", fake_monotonic)

    # Provide one discovered URL so the inner loop is entered
    monkeypatch.setattr(
        ingestion_engine,
        "discover_urls",
        lambda *a, **kw: [("https://example.com/article", "Test Title")],
    )

    mock_log_stats = MagicMock()
    mock_macro = MagicMock(return_value=True)
    monkeypatch.setattr(ingestion_engine, "_log_stats", mock_log_stats)
    monkeypatch.setattr(ingestion_engine, "generate_macro_summary", mock_macro)
    monkeypatch.setattr(ingestion_engine, "_hydrate_seen_headlines", lambda: set())

    # Run from the tmp targets file
    monkeypatch.chdir(tmp_path)

    execute_pipeline()

    mock_log_stats.assert_called_once()
    mock_macro.assert_called_once()


# ===========================================================================
# Protected tail budget — concept/macro groups must always get discovery
# ===========================================================================

def _reserve_target(name: str, search_mode: str) -> dict:
    return {
        "name": name,
        "category": name,
        "query": f'"{name}"',
        "results_per_entity": 2,
        "lookback_hours": 24,
        "min_article_length": 500,
        "search_mode": search_mode,
    }


def _run_reserve_pipeline(monkeypatch, targets: list[dict]) -> list[str]:
    """Run execute_pipeline over fake targets (one candidate each, every scrape
    succeeds and stores) and return the trigger entities stored, in order."""
    import ingestion_engine

    monkeypatch.setattr(ingestion_engine, "load_targets", lambda path: targets)
    monkeypatch.setattr(
        ingestion_engine, "discover_candidates",
        lambda target: [{
            "url": f"https://example.com/{target['name']}",
            "title": f"News about {target['name']}",
            "provider": "serper",
        }],
    )
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h: False)
    monkeypatch.setattr(
        ingestion_engine, "is_semantic_duplicate", lambda title, seen: (False, "", 0))
    monkeypatch.setattr(ingestion_engine, "scrape_article", lambda url, m: "text " * 200)
    monkeypatch.setattr(
        ingestion_engine, "synthesize_insight",
        lambda text, url, entity, category: {
            "headline": f"Headline {entity}",
            "americhem_impact": "Impact.",
            "sentiment_score": 5,
            "source_url": url,
            "entities_mentioned": [],
        },
    )
    stored: list[str] = []
    monkeypatch.setattr(
        ingestion_engine, "store_insight",
        lambda payload: stored.append(payload["trigger_entity"]),
    )
    monkeypatch.setattr(ingestion_engine, "_hydrate_seen_headlines", lambda: set())
    monkeypatch.setattr(ingestion_engine, "generate_macro_summary", MagicMock(return_value=True))
    monkeypatch.setattr(ingestion_engine.time, "sleep", lambda s: None)

    execute_pipeline()
    return stored


def test_tail_reserve_skips_entity_targets_when_scrape_budget_low(monkeypatch):
    """When remaining scrape slots fall to the reserve, remaining ENTITY targets
    are skipped but concept targets still run — concept/macro coverage is
    protected from entity-tail starvation."""
    import ingestion_engine

    # One concept target × results_per_entity=2 → derived reserve of 2.
    monkeypatch.setattr(ingestion_engine, "MAX_DAILY_SCRAPES", 3)
    stored = _run_reserve_pipeline(monkeypatch, [
        _reserve_target("EntityA", "entity"),
        _reserve_target("EntityB", "entity"),
        _reserve_target("concept_group", "concept"),
    ])
    # EntityA consumes the single unreserved slot; EntityB is skipped by the
    # reserve; the concept group spends the reserved budget.
    assert stored == ["EntityA", "concept_group"]


def test_tail_reserve_covers_full_configured_tail_demand(monkeypatch):
    """The slot reserve is DERIVED from the configured tail demand
    (sum of results_per_entity over concept targets), not a fixed constant —
    so every concept/macro group gets a discovery pass even when each earlier
    concept target consumes its full candidate budget."""
    import ingestion_engine

    # Two concept targets × results_per_entity=2 → demand 4; cap 5 leaves
    # exactly one unreserved slot for the entity tier.
    monkeypatch.setattr(ingestion_engine, "MAX_DAILY_SCRAPES", 5)
    stored = _run_reserve_pipeline(monkeypatch, [
        _reserve_target("EntityA", "entity"),
        _reserve_target("EntityB", "entity"),
        _reserve_target("concept_one", "concept"),
        _reserve_target("concept_two", "concept"),
    ])
    assert stored == ["EntityA", "concept_one", "concept_two"]


def test_tail_reserve_excludes_front_loaded_concepts(monkeypatch):
    """A concept group positioned BEFORE the entity tier (Tier 1 priority
    segments) must NOT be counted in the entity gate's reserve — it has already
    run, so counting it over-reserves and skips entity targets that the budget
    could still afford. The reserve protects only the concept demand still
    AHEAD of the current target."""
    import ingestion_engine

    # concept_front runs first (1 scrape), then two entities, then concept_tail.
    # Static all-concepts reserve = 4 (both concepts) → entity threshold MAX-4=0
    # → both entities wrongly skipped. Position-aware reserve at the entities =
    # only concept_tail (2) → threshold MAX-2=2 → EntityA survives.
    monkeypatch.setattr(ingestion_engine, "MAX_DAILY_SCRAPES", 4)
    stored = _run_reserve_pipeline(monkeypatch, [
        _reserve_target("concept_front", "concept"),
        _reserve_target("EntityA", "entity"),
        _reserve_target("EntityB", "entity"),
        _reserve_target("concept_tail", "concept"),
    ])
    assert stored == ["concept_front", "EntityA", "concept_tail"]


def test_tail_reserve_skips_entity_targets_when_wall_clock_low(monkeypatch):
    """When remaining wall-clock falls to the time reserve, remaining ENTITY
    targets are skipped but concept targets still run."""
    import ingestion_engine

    monkeypatch.setattr(ingestion_engine, "PIPELINE_DEADLINE_SECONDS", 100)
    monkeypatch.setattr(ingestion_engine, "TAIL_RESERVE_SECONDS", 50)
    # First call anchors pipeline_start at 0; everything after runs at t=60 —
    # past the entity cutoff (100-50=50) but inside the hard deadline (100).
    call_count = {"n": 0}

    def fake_monotonic():
        call_count["n"] += 1
        return 0.0 if call_count["n"] == 1 else 60.0

    monkeypatch.setattr(ingestion_engine.time, "monotonic", fake_monotonic)
    stored = _run_reserve_pipeline(monkeypatch, [
        _reserve_target("EntityA", "entity"),
        _reserve_target("concept_group", "concept"),
    ])
    assert stored == ["concept_group"]


def test_tail_reserve_defaults_leave_headroom_for_tail_groups():
    """Against the real targets.yaml, the derived slot reserve must be nonzero
    (there are concept/macro groups to protect) and leave the entity tier a
    majority of the cap; the time reserve must sit inside the deadline."""
    import ingestion_engine

    targets = load_targets("targets.yaml")
    demand = ingestion_engine._tail_scrape_demand(targets)
    assert 0 < demand < ingestion_engine.MAX_DAILY_SCRAPES / 2
    assert 0 < ingestion_engine.TAIL_RESERVE_SECONDS < ingestion_engine.PIPELINE_DEADLINE_SECONDS


def test_tail_scrape_demand_is_zero_without_concept_targets():
    """No concept targets → nothing to protect → entity targets never skipped."""
    import ingestion_engine

    assert ingestion_engine._tail_scrape_demand(
        [_reserve_target("EntityA", "entity")]) == 0


# ===========================================================================
# Relevance upgrade — new field validation in synthesize_insight()
# ===========================================================================

def _make_openai_mock_with_fields(**overrides) -> FakeLLM:
    """Return a FakeLLM that outputs a minimal valid insight plus overrides."""
    base = {
        "headline": "Test Headline",
        "americhem_impact": "Direct impact on compounding margins.",
        "sentiment_score": 5,
        "sentiment_tag": "Neutral",
        "americhem_impact_score": 7,
        "impact_rationale": "Directly affects masterbatch feedstock cost.",
        "strategic_segment": "Raw Materials / Supply Chain",
        "source_url": "https://news.com/article",
        "entities_mentioned": ["Avient"],
    }
    base.update(overrides)
    return FakeLLM(returns=base)


# ---------------------------------------------------------------------------
# Sentiment tag validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_tag", ["NEGATIVE", "negative", "Bad", "", None, 42])
def test_synthesize_insight_defaults_invalid_sentiment_tag(bad_tag):
    """Any invalid sentiment_tag must be replaced with 'Neutral'."""
    mock_client = _make_openai_mock_with_fields(sentiment_tag=bad_tag)
    with patch("ingestion_engine._llm", return_value=mock_client):
        result = synthesize_insight(
            article_text="Article text.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["sentiment_tag"] == "Neutral"


@pytest.mark.parametrize("valid_tag", ["Negative", "Neutral", "Positive"])
def test_synthesize_insight_preserves_valid_sentiment_tag(valid_tag):
    """Valid sentiment_tag values must be preserved unchanged."""
    mock_client = _make_openai_mock_with_fields(sentiment_tag=valid_tag)
    with patch("ingestion_engine._llm", return_value=mock_client):
        result = synthesize_insight(
            article_text="Article text.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["sentiment_tag"] == valid_tag


# ---------------------------------------------------------------------------
# americhem_impact_score clamping
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw_impact, expected",
    [
        (0,   1),
        (-5,  1),
        (11,  10),
        (100, 10),
    ],
)
def test_impact_score_clamped(raw_impact, expected):
    """americhem_impact_score must be clamped to the 1–10 range."""
    mock_client = _make_openai_mock_with_fields(americhem_impact_score=raw_impact)
    with patch("ingestion_engine._llm", return_value=mock_client):
        result = synthesize_insight(
            article_text="Article text.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["americhem_impact_score"] == expected


@pytest.mark.parametrize("bad_value", [None, "high"])
def test_impact_score_defaults_on_bad_value(bad_value):
    """Non-convertible or missing americhem_impact_score defaults to 5."""
    mock_client = _make_openai_mock_with_fields(americhem_impact_score=bad_value)
    with patch("ingestion_engine._llm", return_value=mock_client):
        result = synthesize_insight(
            article_text="Article text.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["americhem_impact_score"] == 5



# ---------------------------------------------------------------------------
# Threshold filtering in assemble_report()
# ---------------------------------------------------------------------------

def _make_new_article(
    url_hash: str,
    americhem_impact_score: int,
    commercial_segment: str = "Enterprise / Cross-Segment",
    sentiment_tag: str = "Neutral",
    headline: str = "Test Headline",
) -> dict:
    """Build a fully-populated new-style article with all relevance fields."""
    return {
        "url_hash": url_hash,
        "americhem_impact_score": americhem_impact_score,
        "sentiment_tag": sentiment_tag,
        "impact_rationale": "Direct feedstock cost effect.",
        "commercial_segment": commercial_segment,
        "headline": headline,
        "americhem_impact": "Some impact.",
        "entities_mentioned": ["TestCorp"],
        "source_url": "https://news.com/article",
        "category": "markets",
        # No sentinel_score — new-style row
    }


def test_assemble_report_filters_below_impact_threshold():
    """Articles with americhem_impact_score below the threshold must not appear in the report.
    Use a non-Enterprise segment so the Enterprise-low-impact suppression rule
    doesn't claim the row first — this exercises the visibility filter itself."""
    low_impact = _make_new_article("low", americhem_impact_score=3, headline="Low Impact Headline",
                                   commercial_segment="Packaging")
    high_impact = _make_new_article("high", americhem_impact_score=8, headline="High Impact Headline",
                                    commercial_segment="Packaging")

    model = assemble_report([low_impact, high_impact],
                            config={"reporting": {"visible_impact_threshold": 6}})

    kept = {a["url_hash"] for arts in model.groups.values() for a in arts}
    assert kept == {"high"}
    assert model.ledger.breakdown["below_impact_threshold"] == 1
    html = render_report(model, today_str=_TODAY_STR)
    assert "High Impact Headline" in html
    assert "Low Impact Headline" not in html


def test_report_macro_outlook_sliced_to_cap():
    """daily_summaries rows stored before the cap reduction may hold up to 6
    signals; assemble_report slices to MAX_MACRO_OUTLOOK_SIGNALS so QA
    re-renders (run_ingestion=false) comply immediately."""
    from prompts import MAX_MACRO_OUTLOOK_SIGNALS

    signals = [
        {
            "indicator": f"Indicator {i}",
            "direction": "Declining",
            "americhem_implication": "Downside risk for resin demand.",
            "affected_segments": ["Industrial"],
            "citation_source_ids": [1],
        }
        for i in range(6)
    ]
    macro_summary = {
        "macro_outlook": {"current_condition": "Manufacturing demand mixed.",
                          "signals": signals},
    }
    rows = [_make_new_article("a", 8, commercial_segment="Packaging",
                              headline="Packaging demand firms on brand-owner restocking")]
    model = assemble_report(rows, macro_summary=macro_summary)

    assert [s["indicator"] for s in model.macro_outlook["signals"]] == [
        "Indicator 0", "Indicator 1", "Indicator 2",
    ]
    assert len(model.macro_outlook["signals"]) == MAX_MACRO_OUTLOOK_SIGNALS


def test_legacy_outlook_render_lists_no_orphan_sources():
    """A daily_summaries row stored before the cap reduction may hold 6 signals
    citing 6 distinct sources. The rendered outlook body shows only the sliced
    3 signals, so the exec-summary citation numbering and the bottom Sources
    footer must list ONLY those 3 cited sources — no orphan [4][5][6] footer
    entries with no inline marker anywhere in the visible email (QA
    run_ingestion=false re-render scenario)."""
    from prompts import MAX_MACRO_OUTLOOK_SIGNALS

    signals = [
        {
            "indicator": f"Indicator {i + 1}",
            "direction": "Declining",
            "americhem_implication": f"Downside risk number {i + 1} for resin demand.",
            "affected_segments": ["Industrial"],
            "citation_source_ids": [i + 1],
        }
        for i in range(6)
    ]
    sources = [
        {"id": i + 1, "headline": f"Macro source {i + 1}",
         "url": f"https://s/{i + 1}", "domain": f"src{i + 1}.com"}
        for i in range(6)
    ]
    macro_summary = {
        "dominant_condition": "Demand Softness",
        # Bullets cite nothing, so citation numbering starts with the signals.
        "executive_bullets": [
            {"label": "Market pressure", "body": "Industrial demand cooling.", "citation_source_ids": []},
            {"label": "Supply chain watch", "body": "Feedstock steady.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Engage key accounts.", "citation_source_ids": []},
        ],
        "macro_outlook": {"current_condition": "Manufacturing demand mixed.",
                          "signals": signals},
        "executive_sources": sources,
    }
    rows = [_make_new_article("v", 8, commercial_segment="Packaging",
                              headline="Packaging demand firms on brand-owner restocking")]
    model = assemble_report(rows, macro_summary=macro_summary, config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)

    # The kept (sliced) sources appear in the Sources footer.
    for i in range(MAX_MACRO_OUTLOOK_SIGNALS):
        assert f"Macro source {i + 1}" in html
        assert f"src{i + 1}.com" in html
    # The sliced-off sources have no inline marker, so they must NOT appear as
    # orphan Sources footer entries.
    for i in range(MAX_MACRO_OUTLOOK_SIGNALS, 6):
        assert f"Macro source {i + 1}" not in html
        assert f"src{i + 1}.com" not in html


def test_assemble_report_groups_by_commercial_segment():
    """Two new-style articles with the same commercial_segment are grouped under that label."""
    # Use genuinely distinct headlines so delivery suppression doesn't flag them
    # as semantic duplicates (token_sort_ratio threshold is 88).
    art_a = _make_new_article("a", 8, commercial_segment="Healthcare",
                              headline="Hospital network consolidation squeezes specialty polymer demand")
    art_b = _make_new_article("b", 7, commercial_segment="Healthcare",
                              headline="FDA clears new medical-grade compound for implantable devices")

    model = assemble_report([art_a, art_b],
                            config={"reporting": {"visible_impact_threshold": 6}})
    assert [a["url_hash"] for a in model.groups["Healthcare"]] == ["a", "b"]

    html = render_report(
        model.with_synthesis({"Healthcare": "Healthcare synthesis paragraph."}),
        today_str=_TODAY_STR,
    )
    assert "HEALTHCARE" in html.upper()
    assert "Healthcare synthesis paragraph." in html


# ---------------------------------------------------------------------------
# _render_card() — sentiment_tag and americhem_impact_score display
# ---------------------------------------------------------------------------

def test_render_card_shows_impact_score_and_sentiment_tag():
    """When americhem_impact_score and sentiment_tag are present, the card shows
    'Impact: X/10' and the tag label, NOT the old 'Score: X/10' format."""
    item = {
        "headline": "Plant closure disrupts supply",
        "source_url": "https://news.com/article",
        "americhem_impact": "Feedstock shortfall for masterbatch lines.",
        "americhem_impact_score": 8,
        "sentiment_tag": "Negative",
        "impact_rationale": "Direct feedstock cost increase.",
        "commercial_segment": "Raw Materials / Supply Chain",
        "source_publication": "Chemical Week",
        "recommended_action": "Flag to procurement",
        "category": "markets",
    }
    html = _render_card(item)
    assert "Impact: 8/10" in html
    assert "Negative" in html
    assert "Score:" not in html


def test_render_card_falls_back_to_sentiment_score_for_old_rows():
    """Old-style rows without new fields must render the legacy 'Score: X/10' display."""
    item = {
        "headline": "Old Article",
        "source_url": "https://news.com/article",
        "americhem_impact": "Some impact.",
        "sentiment_score": 6,
        "source_publication": "Reuters",
        "recommended_action": "Monitor",
        "category": "markets",
    }
    html = _render_card(item)
    assert "Score: 6/10" in html
    assert "Impact:" not in html


# Note: the card no longer carries an in-card segment badge — segment is the
# block header in _render_segment_watch_section. Grouping by commercial_segment
# (and ignoring the legacy strategic_segment field) is covered by
# test_group_by_commercial_segment_keys_off_new_field and the insight tests.


# ===========================================================================
# Article cap enforcement
# ===========================================================================

def test_assemble_report_per_segment_cap():
    """No more than max_per_segment articles from the same segment survive assembly."""
    # 5 Healthcare articles with genuinely distinct headlines (avoids semantic-duplicate
    # suppression which fires at token_sort_ratio >= 88).
    _hc_headlines = [
        "Hospital network merger squeezes specialty polymer volumes",
        "FDA clears new implantable-grade compound for cardiac devices",
        "Aging population drives record demand for medical-grade resins",
        "Generic drug expansion pressures premium plastics pricing",
        "Supply disruption at key resin plant delays surgical kit output",
    ]
    articles = [
        _make_new_article(
            f"h{i}", americhem_impact_score=10 - i,
            commercial_segment="Healthcare",
            headline=_hc_headlines[i],
        )
        for i in range(5)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 3,
            "max_total_visible_articles": 12,
        }
    }
    model = assemble_report(articles, config=config)

    # Top 3 by impact score (h0=10, h1=9, h2=8) survive; h3 (7) and h4 (6) are capped.
    assert [a["url_hash"] for a in model.groups["Healthcare"]] == ["h0", "h1", "h2"]
    assert model.surfaced_count == 3


def test_assemble_report_total_articles_cap():
    """Total visible articles must not exceed max_total_visible_articles."""
    # 7 segments × 2 articles = 14 articles, all impact=8
    segments = [
        "Healthcare", "Fibers", "Packaging", "Industrial",
        "Raw Materials / Supply Chain", "Regulatory / Sustainability",
        "Competitive / Customer Signal",
    ]
    articles = [
        _make_new_article(
            f"s{si}_{ai}", americhem_impact_score=8,
            commercial_segment=seg,
            headline=f"Seg{si} Art{ai}",
        )
        for si, seg in enumerate(segments)
        for ai in range(2)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 3,
            "max_total_visible_articles": 10,
        }
    }
    model = assemble_report(articles, config=config)

    assert model.surfaced_count <= 10
    assert sum(len(arts) for arts in model.groups.values()) == model.surfaced_count


def test_report_capped_articles_flow_into_appendix():
    """Articles dropped by the per-segment cap reappear in the Additional
    Articles appendix — never as visible cards. (Flipped 2026-07-17: the cap
    previously dropped overflow entirely.)"""
    # Genuinely distinct headlines — token_sort_ratio >= 88 would otherwise
    # suppress them as semantic duplicates before the cap runs.
    _hc_headlines = [
        "Hospital network merger squeezes specialty polymer volumes",
        "FDA clears new implantable-grade compound for cardiac devices",
        "Aging population drives record demand for medical-grade resins",
        "Generic drug expansion pressures premium plastics pricing",
    ]
    articles = [
        _make_new_article(
            f"h{i}", americhem_impact_score=10 - i,
            commercial_segment="Healthcare",
            headline=_hc_headlines[i],
        )
        for i in range(4)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 3,
            "max_total_visible_articles": 12,
        }
    }
    model = assemble_report(articles, config=config)

    # Top 3 by impact are cards; h3 (impact=7) is capped out but not lost.
    assert [a["url_hash"] for a in model.groups["Healthcare"]] == ["h0", "h1", "h2"]
    assert [a["url_hash"] for a in model.additional_articles] == ["h3"]
    assert model.surfaced_count == 3

    html = render_report(model, today_str=_TODAY_STR)
    assert _hc_headlines[3] in html


# ---------------------------------------------------------------------------
# Additional Articles appendix — model field and cap config (Task 2)
# ---------------------------------------------------------------------------

def test_report_model_has_additional_articles_tuple():
    """ReportModel carries an additional_articles tuple; empty on the daily
    variant until selection lands, and always empty on no_news."""
    daily = assemble_report(
        [_make_new_article("a", 8, commercial_segment="Packaging",
                           headline="Packaging demand firms on brand-owner restocking")],
        config={"reporting": {"visible_impact_threshold": 6}},
    )
    assert isinstance(daily.additional_articles, tuple)

    no_news = assemble_report([], config={"reporting": {"visible_impact_threshold": 6}})
    assert no_news.variant == "no_news"
    assert no_news.additional_articles == ()


def test_max_additional_articles_default_is_ten():
    """The appendix cap resolves to 10 by default and honors an override — it is
    a report-assembly knob, read in report.py (not a scoring threshold)."""
    from report import _max_additional_articles
    assert _max_additional_articles({}) == 10
    assert _max_additional_articles({"max_additional_articles": 5}) == 5


_APPENDIX_CFG = {"reporting": {"visible_impact_threshold": 6}}


def _appendix_hashes(model) -> list[str]:
    return [a["url_hash"] for a in model.additional_articles]


def test_appendix_selects_scores_4_and_5_excludes_3_and_6():
    """Scores 4 and 5 populate the appendix; 3 is below the band; 6 stays a
    visible card and never duplicates into the appendix."""
    rows = [
        _make_new_article("s6", 6, commercial_segment="Packaging",
                          headline="Visible card at the six threshold holds firm"),
        _make_new_article("s5", 5, commercial_segment="Packaging",
                          headline="Near-threshold five signal worth optional reading"),
        _make_new_article("s4", 4, commercial_segment="Packaging",
                          headline="Marginal four signal for the curious reader"),
        _make_new_article("s3", 3, commercial_segment="Packaging",
                          headline="Below-band three signal should never appear"),
    ]
    model = assemble_report(rows, config=_APPENDIX_CFG)

    group_hashes = {a["url_hash"] for arts in model.groups.values() for a in arts}
    assert "s6" in group_hashes
    assert _appendix_hashes(model) == ["s5", "s4"]
    assert "s6" not in _appendix_hashes(model)
    assert "s3" not in _appendix_hashes(model)


def test_appendix_score_5_ranks_before_score_4():
    """Every score-5 item precedes every score-4 item regardless of insertion
    order."""
    rows = [
        _make_new_article("a4", 4, commercial_segment="Packaging",
                          headline="Alpha four ranked strictly after every five"),
        _make_new_article("b5", 5, commercial_segment="Packaging",
                          headline="Bravo five ranked ahead of any four signal"),
        _make_new_article("c4", 4, commercial_segment="Industrial",
                          headline="Charlie four also trails the five band"),
        _make_new_article("d5", 5, commercial_segment="Industrial",
                          headline="Delta five leads the near-threshold pack"),
    ]
    model = assemble_report(rows, config=_APPENDIX_CFG)
    scores = [a["americhem_impact_score"] for a in model.additional_articles]
    assert scores == [5, 5, 4, 4]


def test_appendix_excludes_blank_headline_or_url():
    """A weak-relevance row without a usable headline or source URL is excluded."""
    good = _make_new_article("good", 5, commercial_segment="Packaging",
                             headline="Usable near-threshold signal with a real link")
    blank_headline = _make_new_article("bh", 5, commercial_segment="Packaging",
                                       headline="   ")
    no_url = _make_new_article("nu", 5, commercial_segment="Packaging",
                               headline="Weak signal that lost its source url somehow")
    no_url["source_url"] = ""
    model = assemble_report([good, blank_headline, no_url], config=_APPENDIX_CFG)
    assert _appendix_hashes(model) == ["good"]


def test_appendix_excludes_delivery_suppressed_rows():
    """A product-listing URL is suppressed before eligibility, so it never
    reaches the appendix even at a qualifying score."""
    listing = _make_new_article("list", 5, commercial_segment="Packaging",
                                headline="Shop our new masterbatch color range online")
    listing["source_url"] = "https://vendor.com/product/masterbatch-blue"
    real = _make_new_article("real", 5, commercial_segment="Packaging",
                             headline="Genuine near-threshold packaging demand note")
    config = {
        "reporting": {"visible_impact_threshold": 6},
        "delivery_suppression": {"url_patterns_product_listing": ["/product/"]},
    }
    model = assemble_report([listing, real], config=config)
    assert _appendix_hashes(model) == ["real"]


def test_appendix_never_includes_enterprise_cross_segment_low_impact():
    """Pinned deliberate consequence: delivery suppression rule 1 drops
    Enterprise / Cross-Segment rows below enterprise_min_impact (7), so a
    score-5 cross-segment row can never appear in the appendix."""
    cross = _make_new_article("cross", 5,
                              commercial_segment="Enterprise / Cross-Segment",
                              headline="Cross-segment corporate note below the bar")
    keep = _make_new_article("keep", 5, commercial_segment="Packaging",
                             headline="Packaging-specific near-threshold signal kept")
    model = assemble_report([cross, keep], config=_APPENDIX_CFG)
    assert _appendix_hashes(model) == ["keep"]


def test_appendix_capped_at_max():
    """No more than max_additional_articles rows enter the appendix."""
    headlines = [
        "Recycled content mandate reshapes flexible film sourcing",
        "Feedstock naphtha spread widens across Gulf Coast crackers",
        "Nonwoven wipes producer books capacity through winter",
        "Colorant supplier flags titanium dioxide allocation risk",
        "Automotive interior program shifts to bio-based softeners",
        "Carpet tile demand rebounds on office refurbishment cycle",
        "Barrier resin qualification opens new pouch applications",
        "Compounder adds twin-screw line for engineered grades",
        "Pigment dispersion lead times ease after port backlog clears",
        "Agricultural mulch film season starts with firmer pricing",
        "Wire jacketing compound tightens on copper build-out",
        "Medical tubing extruder wins implantable device contract",
    ]
    rows = [
        _make_new_article(f"x{i}", 5, commercial_segment="Packaging", headline=h)
        for i, h in enumerate(headlines)
    ]
    config = {"reporting": {"visible_impact_threshold": 6, "max_additional_articles": 10}}
    model = assemble_report(rows, config=config)
    assert len(model.additional_articles) == 10


def test_appendix_deterministic_recency_then_headline_then_hash():
    """Within a score band, order is recency desc (published_at, else
    created_at), then normalized headline asc, then url_hash asc."""
    newer = _make_new_article("z_hash", 5, commercial_segment="Packaging",
                              headline="Zulu newer signal by publication timestamp")
    newer["published_at"] = "2026-07-16T09:00:00+00:00"
    older = _make_new_article("a_hash", 5, commercial_segment="Packaging",
                              headline="Alpha older signal by publication timestamp")
    older["published_at"] = "2026-07-15T09:00:00+00:00"
    # Two undated rows tie on recency -> ordered by normalized headline, then hash.
    undated_b = _make_new_article("h2", 5, commercial_segment="Packaging",
                                  headline="Betamax undated near-threshold packaging note")
    undated_a = _make_new_article("h1", 5, commercial_segment="Packaging",
                                  headline="Anchor undated near-threshold packaging note")
    model = assemble_report([undated_b, older, undated_a, newer], config=_APPENDIX_CFG)
    # newer (dated) and older (dated) lead by recency desc; undated tie last,
    # ordered by headline (Anchor < Betamax).
    assert _appendix_hashes(model) == ["z_hash", "a_hash", "h1", "h2"]


def test_appendix_recency_ignores_unparseable_published_at():
    """A non-ISO published_at must not be used for recency: it falls back to
    created_at, so it can't spuriously outrank a real recent date. (Aligns the
    selector with the renderer, which already drops unparseable published_at.)"""
    garbage = _make_new_article("garbage", 5, commercial_segment="Packaging",
                                headline="Bogus timestamp near-threshold packaging note")
    garbage["published_at"] = "Yesterday"                       # unparseable
    garbage["created_at"] = "2026-07-10T00:00:00+00:00"         # real, older
    real_recent = _make_new_article("recent", 5, commercial_segment="Industrial",
                                    headline="Genuinely recent near-threshold industrial note")
    real_recent["published_at"] = "2026-07-15T00:00:00+00:00"   # real, newer
    model = assemble_report([garbage, real_recent], config=_APPENDIX_CFG)
    # real_recent (Jul 15) must lead; garbage falls back to created_at (Jul 10).
    assert _appendix_hashes(model) == ["recent", "garbage"]


def test_appendix_ranks_cap_overflow_ahead_of_weak_relevance():
    """Capped-out visible-band rows (impact >= 6) precede weak-relevance
    (4-5) rows in the appendix — the existing impact-desc sort, wider band."""
    articles = [
        _make_new_article("v0", 10, commercial_segment="Healthcare",
                          headline="Hospital network merger squeezes specialty polymer volumes"),
        _make_new_article("v1", 9, commercial_segment="Healthcare",
                          headline="FDA clears new implantable-grade compound for cardiac devices"),
        _make_new_article("v2", 7, commercial_segment="Healthcare",
                          headline="Aging population drives record demand for medical-grade resins"),
        _make_new_article("w0", 5, commercial_segment="Packaging",
                          headline="Beverage brands trial mono-material caps in European pilot"),
    ]
    config = {"reporting": {"visible_impact_threshold": 6,
                            "max_visible_articles_per_segment": 2}}
    model = assemble_report(articles, config=config)

    assert [a["url_hash"] for a in model.groups["Healthcare"]] == ["v0", "v1"]
    assert [a["url_hash"] for a in model.additional_articles] == ["v2", "w0"]


def test_appendix_overflow_does_not_alter_ledger_counts():
    """Capped-out rows are displayed, not suppressed: they never enter
    weak_relevance, and below_impact_threshold still counts only
    suppression-surviving below-visible rows."""
    articles = [
        _make_new_article("v0", 10, commercial_segment="Healthcare",
                          headline="Hospital network merger squeezes specialty polymer volumes"),
        _make_new_article("v1", 7, commercial_segment="Healthcare",
                          headline="FDA clears new implantable-grade compound for cardiac devices"),
        _make_new_article("w0", 4, commercial_segment="Packaging",
                          headline="Beverage brands trial mono-material caps in European pilot"),
    ]
    config = {"reporting": {"visible_impact_threshold": 6,
                            "max_visible_articles_per_segment": 1}}
    model = assemble_report(articles, config=config)

    # w0 is the only below-visible survivor; v1 (visible-band, capped) is not counted.
    assert model.ledger.breakdown["below_impact_threshold"] == 1
    # w0 is shown in the appendix, so it is not "shown nowhere".
    assert model.ledger.breakdown.get("weak_relevance", 0) == 0
    assert model.surfaced_count == 1
    assert [a["url_hash"] for a in model.additional_articles] == ["v1", "w0"]


_APPENDIX_ACCT_HEADLINES = [
    "Recycled content mandate reshapes flexible film sourcing",
    "Feedstock naphtha spread widens across Gulf Coast crackers",
    "Nonwoven wipes producer books capacity through winter",
    "Colorant supplier flags titanium dioxide allocation risk",
    "Automotive interior program shifts to bio-based softeners",
    "Carpet tile demand rebounds on office refurbishment cycle",
    "Barrier resin qualification opens new pouch applications",
    "Compounder adds twin-screw line for engineered grades",
    "Pigment dispersion lead times ease after port backlog clears",
    "Agricultural mulch film season starts with firmer pricing",
    "Wire jacketing compound tightens on copper build-out",
    "Medical tubing extruder wins implantable device contract",
]


def test_appendix_displayed_rows_not_counted_weak_relevance():
    """A score-5 row shown in the appendix is not counted as weak_relevance."""
    row = _make_new_article("shown", 5, commercial_segment="Packaging",
                            headline="Near-threshold packaging note shown in appendix")
    model = assemble_report([row], config=_APPENDIX_CFG)
    assert _appendix_hashes(model) == ["shown"]
    # record_count is a no-op at 0, so the key is simply absent.
    assert model.ledger.breakdown.get("weak_relevance", 0) == 0


def test_appendix_capped_out_rows_counted_weak_relevance():
    """Eligible score-5 rows pushed out by the appendix cap are counted as
    weak_relevance (in neither the main groups nor the appendix)."""
    rows = [
        _make_new_article(f"w{i}", 5, commercial_segment="Packaging", headline=h)
        for i, h in enumerate(_APPENDIX_ACCT_HEADLINES)  # 12 rows
    ]
    config = {"reporting": {"visible_impact_threshold": 6, "max_additional_articles": 10}}
    model = assemble_report(rows, config=config)
    assert len(model.additional_articles) == 10
    assert model.ledger.breakdown["weak_relevance"] == 2


def test_below_impact_threshold_unchanged_by_appendix():
    """below_impact_threshold still counts every suppression-surviving row below
    the visible threshold, including rows the appendix now displays."""
    rows = [
        _make_new_article("s5", 5, commercial_segment="Packaging",
                          headline="Five-band signal that lands in the appendix"),
        _make_new_article("s4", 4, commercial_segment="Packaging",
                          headline="Four-band signal that also lands in appendix"),
        _make_new_article("s3", 3, commercial_segment="Packaging",
                          headline="Three-band signal below the supporting floor"),
    ]
    model = assemble_report(rows, config=_APPENDIX_CFG)
    # All three are below the visible threshold (6) and survive suppression.
    assert model.ledger.breakdown["below_impact_threshold"] == 3
    # Two of them are surfaced in the appendix — that overlap is intentional.
    assert len(model.additional_articles) == 2


# ---------------------------------------------------------------------------
# Macro outlook carried through the report model (PR 2, Task 10)
# ---------------------------------------------------------------------------

_VALID_MACRO_OUTLOOK = {
    "current_condition": "Industrial demand softening as construction cools.",
    "signals": [
        {"indicator": "Manufacturing PMI", "direction": "Declining",
         "americhem_implication": "Downside risk for engineered-resin demand.",
         "affected_segments": ["Industrial"], "citation_source_ids": [1]},
    ],
}


def test_report_model_carries_macro_outlook():
    row = _make_new_article("v", 8, commercial_segment="Packaging",
                            headline="Visible packaging card to make a daily model")
    macro = {"dominant_condition": "Demand Softness", "macro_outlook": _VALID_MACRO_OUTLOOK}
    model = assemble_report([row], macro_summary=macro, config=_APPENDIX_CFG)
    assert model.macro_outlook == _VALID_MACRO_OUTLOOK


def test_report_model_macro_outlook_none_when_absent():
    row = _make_new_article("v", 8, commercial_segment="Packaging",
                            headline="Visible packaging card with no macro outlook")
    model = assemble_report([row], macro_summary={"dominant_condition": "Mixed / Watch"},
                            config=_APPENDIX_CFG)
    assert model.macro_outlook is None


def test_report_model_macro_outlook_none_when_malformed():
    row = _make_new_article("v", 8, commercial_segment="Packaging",
                            headline="Visible packaging card with malformed outlook")
    for bad in ({}, {"current_condition": "x", "signals": []},
                {"current_condition": "  ", "signals": [{"indicator": "PMI"}]},
                {"signals": [{"indicator": "PMI"}]}, "nope", None):
        model = assemble_report([row], macro_summary={"macro_outlook": bad},
                                config=_APPENDIX_CFG)
        assert model.macro_outlook is None, bad


def test_report_model_no_news_macro_outlook_none():
    model = assemble_report([], macro_summary={"macro_outlook": _VALID_MACRO_OUTLOOK},
                            config=_APPENDIX_CFG)
    assert model.variant == "no_news"
    assert model.macro_outlook is None


# ---------------------------------------------------------------------------
# Macroeconomic Outlook — rendering (PR 2, Task 11)
# ---------------------------------------------------------------------------

_MACRO_TITLE = "MACROECONOMIC OUTLOOK"


def _macro_summary_with_outlook() -> dict:
    return {
        "dominant_condition": "Demand Softness",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Industrial demand cooling.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Feedstock steady.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Engage key accounts.", "citation_source_ids": []},
        ],
        "macro_outlook": {
            "current_condition": "Industrial and construction demand both softening.",
            "signals": [
                {"indicator": "Housing starts", "direction": "Declining",
                 "americhem_implication": "Weakness in building-products volumes.",
                 "affected_segments": ["Industrial"], "citation_source_ids": [2]},
            ],
        },
        "executive_sources": [
            {"id": 1, "headline": "Industrial PMI slips", "url": "https://s/1", "domain": "s.com"},
            {"id": 2, "headline": "Housing starts fall", "url": "https://s/2", "domain": "t.com"},
        ],
    }


def test_macro_section_renders_between_exec_and_segment_watch():
    visible = _make_new_article("v", 8, commercial_segment="Packaging",
                                headline="High-impact packaging supply disruption card")
    model = assemble_report([visible], macro_summary=_macro_summary_with_outlook(),
                            config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    assert _MACRO_TITLE in html
    assert "Industrial and construction demand both softening." in html
    assert "Housing starts" in html
    assert "Declining" in html
    assert "Weakness in building-products volumes." in html
    assert "Industrial" in html
    i_exec = html.find("Executive Summary")
    i_macro = html.find(_MACRO_TITLE)
    i_watch = html.find("COMMERCIAL SEGMENT WATCH")
    assert i_exec != -1 and i_macro != -1 and i_watch != -1
    assert i_exec < i_macro < i_watch


def test_macro_section_absent_when_none():
    visible = _make_new_article("v", 8, commercial_segment="Packaging",
                                headline="High-impact packaging card with no outlook")
    model = assemble_report([visible], macro_summary={"dominant_condition": "Mixed / Watch"},
                            config=_APPENDIX_CFG)
    assert model.macro_outlook is None
    html = render_report(model, today_str=_TODAY_STR)
    assert _MACRO_TITLE not in html


def test_macro_section_current_condition_rendered_once():
    model = assemble_report(
        [_make_new_article("v", 8, commercial_segment="Packaging",
                           headline="Packaging card to accompany the macro outlook")],
        macro_summary=_macro_summary_with_outlook(), config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    assert html.count("Industrial and construction demand both softening.") == 1


def test_macro_section_shares_one_citation_numbering_space():
    """Bullets cite source 1, the macro signal cites source 2: the exec summary
    shows [1], the macro section shows [2], and the single Sources footer lists
    both — one numbering space (bullets enumerated, then signals)."""
    model = assemble_report(
        [_make_new_article("v", 8, commercial_segment="Packaging",
                           headline="Packaging card next to the macro outlook here")],
        macro_summary=_macro_summary_with_outlook(), config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    # Both cited sources resolve in the bottom Sources list.
    assert "Industrial PMI slips" in html
    assert "Housing starts fall" in html
    # Footer numbering covers both ids.
    assert "[1]" in html and "[2]" in html
    # The macro signal's marker links to source 2's url.
    assert 'href="https://s/2"' in html


def test_section_header_is_full_width_not_squeezed():
    """Section headers span the full width with an underline — no `nowrap`
    title cell that a narrow client squeezes into a 3-line wrap."""
    from delivery_engine import _section_header_row
    hdr = _section_header_row("Additional Articles to Explore",
                              title_color="#5a6678", rule_color="#E5E7EB")
    assert "Additional Articles to Explore" in hdr
    assert "white-space:nowrap" not in hdr
    assert "border-bottom:1px solid #E5E7EB" in hdr


def test_section_headers_render_without_nowrap():
    """The rendered email's section titles are not placed in nowrap cells."""
    macro = _macro_summary_with_outlook()
    model = assemble_report(
        [_make_new_article("v", 8, commercial_segment="Packaging",
                           headline="High-impact packaging card for header test"),
         _make_new_article("w", 5, commercial_segment="Industrial",
                           headline="Near-threshold industrial reading for appendix here")],
        macro_summary=macro, config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    for title in ("MACROECONOMIC OUTLOOK", "COMMERCIAL SEGMENT WATCH",
                  "Additional Articles to Explore"):
        # Each title sits in a full-width underlined header cell, not a
        # nowrap+padding-right two-cell layout.
        i = html.find(title)
        assert i != -1
        header_open = html.rfind("<td", 0, i)
        assert "white-space:nowrap" not in html[header_open:i]


def test_macro_section_direction_styling_is_valence_neutral():
    """Direction must not be risk-colored: 'Rising' is adverse for cost-side
    indicators (inflation, energy, freight) but favorable for demand-side ones,
    and the signal carries no good/bad field — so green/red would invert the
    risk on cost rows. Valence lives in the implication text, not the color."""
    macro = {
        "macro_outlook": {
            "current_condition": "Input costs climbing while demand holds.",
            "signals": [
                {"indicator": "Producer prices", "direction": "Rising",
                 "americhem_implication": "Margin pressure through resin, energy, and freight costs.",
                 "affected_segments": ["Industrial"], "citation_source_ids": [1]},
                {"indicator": "Housing starts", "direction": "Declining",
                 "americhem_implication": "Weakness in building-products volumes.",
                 "affected_segments": ["Industrial"], "citation_source_ids": [1]},
            ],
        },
        "executive_sources": [
            {"id": 1, "headline": "PPI climbs", "url": "https://s/1", "domain": "s.com"},
        ],
    }
    model = assemble_report(
        [_make_new_article("v", 8, commercial_segment="Packaging", sentiment_tag="Neutral",
                           headline="Neutral-tag packaging card beside the outlook")],
        macro_summary=macro, config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    macro_section = html[html.find(_MACRO_TITLE):html.find("COMMERCIAL SEGMENT WATCH")]
    # No sentiment green/red inside the macro section — direction is neutral.
    assert "#16A34A" not in macro_section    # green (would imply Rising = good)
    assert "#DC2626" not in macro_section    # red (would imply Declining = bad)
    assert "Rising" in macro_section and "Declining" in macro_section


def test_macro_section_escapes_untrusted_text():
    macro = _macro_summary_with_outlook()
    macro["macro_outlook"]["signals"][0]["americhem_implication"] = "<script>alert('x')</script> risk"
    model = assemble_report(
        [_make_new_article("v", 8, commercial_segment="Packaging",
                           headline="Packaging card with an XSS-y macro outlook")],
        macro_summary=macro, config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    assert "<script>alert" not in html
    assert "&lt;script&gt;" in html


# ---------------------------------------------------------------------------
# Additional Articles appendix — rendering (Task 5)
# ---------------------------------------------------------------------------

_APPENDIX_TITLE = "Additional Articles to Explore"


def test_appendix_renders_when_items_present():
    """The appendix section shows title, linked headline, segment, impact, and
    source for each row."""
    row = _make_new_article("a", 5, commercial_segment="Packaging",
                            headline="Near-threshold packaging demand firms up")
    row["source_publication"] = "Plastics News"
    model = assemble_report([row], config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    assert _APPENDIX_TITLE in html
    assert "Near-threshold packaging demand firms up" in html
    assert "Packaging" in html
    assert "Impact: 5/10" in html
    assert "Plastics News" in html
    assert 'href="https://news.com/article"' in html


def test_appendix_absent_when_empty():
    """No appendix section renders when there are no additional articles."""
    row = _make_new_article("v", 8, commercial_segment="Packaging",
                            headline="Visible high-impact packaging card only")
    model = assemble_report([row], config=_APPENDIX_CFG)
    assert model.additional_articles == ()
    html = render_report(model, today_str=_TODAY_STR)
    assert _APPENDIX_TITLE not in html


def test_appendix_shows_date_only_when_published_at():
    """Publication date renders only from published_at, never a scrape timestamp."""
    dated = _make_new_article("d", 5, commercial_segment="Packaging",
                              headline="Dated near-threshold packaging signal here")
    dated["published_at"] = "2026-07-15T09:00:00+00:00"
    dated["created_at"] = "2026-07-16T23:59:00+00:00"  # scrape time — must NOT show
    undated = _make_new_article("u", 5, commercial_segment="Industrial",
                                headline="Undated near-threshold industrial signal")
    undated["created_at"] = "2026-07-16T23:59:00+00:00"
    model = assemble_report([dated, undated], config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    assert "Jul 15, 2026" in html          # published_at of the dated row
    assert "Jul 16, 2026" not in html      # scrape timestamp never displayed


def test_appendix_omits_so_what_narrative():
    """The appendix does not render the americhem_impact 'So what' narrative."""
    row = _make_new_article("a", 5, commercial_segment="Packaging",
                            headline="Near-threshold packaging note for appendix")
    row["americhem_impact"] = "UNIQUE_SO_WHAT_NARRATIVE_TOKEN"
    model = assemble_report([row], config=_APPENDIX_CFG)
    assert _appendix_hashes(model) == ["a"]
    html = render_report(model, today_str=_TODAY_STR)
    assert _APPENDIX_TITLE in html
    assert "UNIQUE_SO_WHAT_NARRATIVE_TOKEN" not in html


def test_appendix_escapes_untrusted_and_guards_href():
    """Headline/source are HTML-escaped and a non-http(s) URL is neutralized."""
    row = _make_new_article("a", 5, commercial_segment="Packaging",
                            headline="<script>alert('x')</script> resin note")
    row["source_url"] = "javascript:alert(1)"
    model = assemble_report([row], config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    assert "<script>alert" not in html
    assert "&lt;script&gt;" in html
    assert 'href="javascript:' not in html


def test_appendix_renders_below_segment_watch_above_sources():
    """Section order: Commercial Segment Watch -> Additional Articles -> Sources."""
    visible = _make_new_article("v", 8, commercial_segment="Packaging",
                                headline="High-impact packaging supply disruption card")
    weak = _make_new_article("w", 5, commercial_segment="Industrial",
                             headline="Near-threshold industrial reading for appendix")
    macro = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Pressure body.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Supply body.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Action body.", "citation_source_ids": []},
        ],
        "executive_sources": [
            {"id": 1, "headline": "Source one", "url": "https://s/1", "domain": "s.com"},
        ],
    }
    model = assemble_report([visible, weak], macro_summary=macro, config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    i_watch = html.find("COMMERCIAL SEGMENT WATCH")
    i_appendix = html.find(_APPENDIX_TITLE)
    i_sources = html.find(">Sources<")
    assert i_watch != -1 and i_appendix != -1 and i_sources != -1
    assert i_watch < i_appendix < i_sources


# ---------------------------------------------------------------------------
# Uncapped-by-default report: caps are optional knobs (null / absent = no cap)
# ---------------------------------------------------------------------------

_UNCAPPED_HC_HEADLINES = [
    "Hospital network merger squeezes specialty polymer volumes",
    "FDA clears new implantable-grade compound for cardiac devices",
    "Aging population drives record demand for medical-grade resins",
    "Generic drug expansion pressures premium plastics pricing",
    "Supply disruption at key resin plant delays surgical kit output",
]


def test_assemble_report_uncapped_per_segment_when_null():
    """With max_visible_articles_per_segment: null, every visible article in a
    segment survives — no per-segment drop."""
    articles = [
        _make_new_article(
            f"h{i}", americhem_impact_score=10 - i,
            commercial_segment="Healthcare",
            headline=_UNCAPPED_HC_HEADLINES[i],
        )
        for i in range(5)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": None,
            "max_total_visible_articles": None,
        }
    }
    model = assemble_report(articles, config=config)
    assert len(model.groups["Healthcare"]) == 5
    assert model.surfaced_count == 5


def test_assemble_report_uncapped_total_when_null():
    """With max_total_visible_articles: null, all visible articles across
    segments survive — no total drop."""
    # 14 genuinely distinct headlines (semantic-duplicate suppression fires at
    # token_sort_ratio >= 88, so near-identical headlines would collapse).
    specs = [
        ("Healthcare", "Hospital merger reshapes specialty polymer procurement"),
        ("Healthcare", "FDA clears implantable-grade compound for cardiac devices"),
        ("Fibers", "Nonwoven hygiene demand lifts polypropylene fiber orders"),
        ("Fibers", "Carpet mill restart tightens solution-dyed yarn supply"),
        ("Packaging", "Brand owners accelerate recyclable flexible film pledges"),
        ("Packaging", "Food-contact resin shortage delays beverage closures"),
        ("Industrial", "Wire-and-cable buildout drives jacketing compound volumes"),
        ("Industrial", "Agricultural film season opens with firmer additive pricing"),
        ("Transportation - Automotive", "EV interior programs shift to flame-retardant grades"),
        ("Transportation - Automotive", "Tier-one supplier books record under-hood resin demand"),
        ("Transportation - Aerospace", "Rotorcraft OEM qualifies new flame-rated cabin polymer"),
        ("Transportation - Aerospace", "Defense procurement lifts high-temperature composite orders"),
        ("Engineered Resins", "PEEK capacity expansion eases medical-device lead times"),
        ("Engineered Resins", "Glass-filled nylon pricing climbs on feedstock tightness"),
    ]
    articles = [
        _make_new_article(
            f"u{i}", americhem_impact_score=8,
            commercial_segment=seg, headline=headline,
        )
        for i, (seg, headline) in enumerate(specs)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": None,
            "max_total_visible_articles": None,
        }
    }
    model = assemble_report(articles, config=config)
    assert model.surfaced_count == 14


def test_assemble_report_uncapped_by_default():
    """Built-in defaults (config=None) impose no caps: all 5 visible articles
    in one segment survive."""
    articles = [
        _make_new_article(
            f"h{i}", americhem_impact_score=8,
            commercial_segment="Healthcare",
            headline=_UNCAPPED_HC_HEADLINES[i],
        )
        for i in range(5)
    ]
    model = assemble_report(articles, config=None)
    assert len(model.groups["Healthcare"]) == 5


def test_assemble_report_integer_cap_still_enforced():
    """An integer cap in config still caps — the knob is retained for rollback."""
    articles = [
        _make_new_article(
            f"h{i}", americhem_impact_score=10 - i,
            commercial_segment="Healthcare",
            headline=_UNCAPPED_HC_HEADLINES[i],
        )
        for i in range(5)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 3,
            "max_total_visible_articles": 12,
        }
    }
    model = assemble_report(articles, config=config)
    assert [a["url_hash"] for a in model.groups["Healthcare"]] == ["h0", "h1", "h2"]
    assert model.surfaced_count == 3


# ===========================================================================
# Negative moderate-impact: impact score drives filtering, not sentiment tone
# ===========================================================================

def test_assemble_report_excludes_negative_low_impact_new_style():
    """A Negative-sentiment article with low americhem_impact_score must be excluded.
    Filtering is by impact score, not tone — this validates the invariant."""
    neg_low = _make_new_article(
        "neg_low", americhem_impact_score=4,
        sentiment_tag="Negative",
        headline="Negative Low Impact Headline",
    )
    pos_high = _make_new_article(
        "pos_high", americhem_impact_score=8,
        sentiment_tag="Positive",
        headline="Positive High Impact Headline",
    )
    model = assemble_report([neg_low, pos_high],
                            config={"reporting": {"visible_impact_threshold": 6}})
    html = render_report(model, today_str=_TODAY_STR)

    assert "Positive High Impact Headline" in html
    assert "Negative Low Impact Headline" not in html


def test_report_shows_negative_high_impact():
    """A Negative-sentiment article with high americhem_impact_score MUST appear.
    A high-impact supply disruption (Negative) is more important than a positive routine signal."""
    neg_high = _make_new_article(
        "neg_high", americhem_impact_score=9,
        sentiment_tag="Negative",
        commercial_segment="Raw Materials / Supply Chain",
        headline="Negative High Impact Supply Disruption",
    )
    model = assemble_report([neg_high],
                            config={"reporting": {"visible_impact_threshold": 6}})
    html = render_report(model, today_str=_TODAY_STR)

    assert "Negative High Impact Supply Disruption" in html
    assert "Negative" in html


# ---------------------------------------------------------------------------
# _config_int coercion
# ---------------------------------------------------------------------------

def test_config_int_returns_int_for_numeric_value():
    cfg = {"visible_impact_threshold": 7}
    assert _config_int(cfg, "visible_impact_threshold", 6) == 7


def test_config_int_coerces_string_to_int():
    """YAML authors may quote numbers; ensure we still get an int."""
    cfg = {"visible_impact_threshold": "8"}
    assert _config_int(cfg, "visible_impact_threshold", 6) == 8


# ---------------------------------------------------------------------------
# Task 8 — _commercial_segment_of and _signal_type_of helpers
# ---------------------------------------------------------------------------

def test_commercial_segment_of_returns_commercial_segment():
    from insight import commercial_segment as _commercial_segment_of
    assert _commercial_segment_of({"commercial_segment": "Healthcare"}) == "Healthcare"


def test_commercial_segment_of_strips_whitespace():
    from insight import commercial_segment as _commercial_segment_of
    assert _commercial_segment_of({"commercial_segment": " Packaging "}) == "Packaging"


def test_commercial_segment_of_ignores_legacy_strategic_segment():
    """The legacy strategic_segment fallback was removed — rows with only that
    field must route to the default Enterprise / Cross-Segment bucket."""
    from insight import commercial_segment as _commercial_segment_of
    assert _commercial_segment_of({"strategic_segment": "Healthcare"}) == "Enterprise / Cross-Segment"


def test_commercial_segment_of_defaults_when_missing():
    from insight import commercial_segment as _commercial_segment_of
    assert _commercial_segment_of({}) == "Enterprise / Cross-Segment"
    assert _commercial_segment_of({"commercial_segment": None}) == "Enterprise / Cross-Segment"
    assert _commercial_segment_of({"commercial_segment": ""}) == "Enterprise / Cross-Segment"


def test_commercial_segment_of_defaults_for_whitespace_only():
    """A whitespace-only commercial_segment must default to Enterprise / Cross-Segment,
    not produce a blank segment bucket."""
    from insight import commercial_segment as _commercial_segment_of
    assert _commercial_segment_of({"commercial_segment": "   "}) == "Enterprise / Cross-Segment"


def test_signal_type_of_prefers_new_field():
    from insight import signal_type as _signal_type_of
    assert _signal_type_of({"signal_type": "Regulatory"}) == "Regulatory"


def test_signal_type_of_falls_back_to_other():
    from insight import signal_type as _signal_type_of
    assert _signal_type_of({}) == "Other"
    assert _signal_type_of({"signal_type": None}) == "Other"
    assert _signal_type_of({"signal_type": ""}) == "Other"


def test_config_int_returns_default_for_missing_key():
    assert _config_int({}, "visible_impact_threshold", 6) == 6


def test_config_int_returns_default_and_warns_for_bad_value(caplog):
    import logging
    cfg = {"visible_impact_threshold": "high"}
    with caplog.at_level(logging.WARNING, logger="delivery_engine"):
        result = _config_int(cfg, "visible_impact_threshold", 6)
    assert result == 6
    assert "visible_impact_threshold" in caplog.text


# ===========================================================================
# MARKET_PULSE_RUN_MODE — test-mode markings
# ===========================================================================

def test_send_email_test_mode_prefixes_subject(monkeypatch):
    """In test mode, the Resend payload subject must start with '[TEST] '."""
    _email_env(monkeypatch)
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    monkeypatch.setattr(_time, "sleep", lambda s: None)

    captured = {}
    def fake_post(*args, **kwargs):
        captured["payload"] = kwargs["json"]
        resp = MagicMock(); resp.status_code = 200; resp.ok = True
        resp.raise_for_status = MagicMock()
        return resp

    monkeypatch.setattr(_requests, "post", fake_post)
    _send_email("<html>x</html>")
    assert captured["payload"]["subject"].startswith("[TEST] ")


def test_send_email_production_mode_subject_unchanged(monkeypatch):
    """When MARKET_PULSE_RUN_MODE is unset, the subject must have no [TEST] prefix."""
    _email_env(monkeypatch)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    monkeypatch.setattr(_time, "sleep", lambda s: None)

    captured = {}
    def fake_post(*args, **kwargs):
        captured["payload"] = kwargs["json"]
        resp = MagicMock(); resp.status_code = 200; resp.ok = True
        resp.raise_for_status = MagicMock()
        return resp

    monkeypatch.setattr(_requests, "post", fake_post)
    _send_email("<html>x</html>")
    assert "[TEST]" not in captured["payload"]["subject"]


def test_send_email_recipient_list_is_only_recipient_emails_env(monkeypatch):
    """Recipient invariant: send_email() builds the Resend 'to' list strictly from the
    RECIPIENT_EMAILS env var and never falls back to any hardcoded address. This is
    the safety guarantee that lets the workflow swap recipient pools by env var alone.
    """
    monkeypatch.setenv("SMTP_PASS", "re_test_key")
    monkeypatch.setenv("SENDER_EMAIL", "noreply@test.com")
    monkeypatch.setenv("RECIPIENT_EMAILS", "jphifer@americhem.com")
    monkeypatch.setattr(_time, "sleep", lambda s: None)

    captured = {}
    def fake_post(*args, **kwargs):
        captured["payload"] = kwargs["json"]
        resp = MagicMock(); resp.status_code = 200; resp.ok = True
        resp.raise_for_status = MagicMock()
        return resp

    monkeypatch.setattr(_requests, "post", fake_post)
    _send_email("<html>x</html>")
    assert captured["payload"]["to"] == ["jphifer@americhem.com"]


def test_render_report_test_mode_prefixes_header():
    """With test_mode=True, render_report() must include [TEST] in the title and
    a visible TEST RUN banner in the rendered HTML."""
    model = assemble_report([_make_new_article("h", 8, headline="Some Headline")])
    html = render_report(model, today_str=_TODAY_STR, test_mode=True)
    assert "[TEST]" in html
    assert "TEST RUN" in html
    assert "Jason-only QA output" in html


def test_render_report_production_mode_unchanged():
    """With test_mode=False (the default), the rendered HTML must contain
    no [TEST] markers or TEST RUN banner."""
    model = assemble_report([_make_new_article("h", 8, headline="Some Headline")])
    html = render_report(model, today_str=_TODAY_STR)
    assert "[TEST]" not in html
    assert "TEST RUN" not in html


def test_no_news_email_test_mode_marks_header():
    """The no-news variant HTML must carry [TEST] and the TEST RUN banner in test mode."""
    model = assemble_report([])
    assert model.variant == "no_news"
    html = render_report(model, today_str=_TODAY_STR, test_mode=True)
    assert "[TEST]" in html
    assert "TEST RUN" in html
    assert "No significant market events" in html


def _make_openai_mock_with_new_fields(**overrides) -> FakeLLM:
    """FakeLLM that returns the new-style per-article payload."""
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
    return FakeLLM(returns=base)


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
    with patch("ingestion_engine._llm", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert result["commercial_segment"] == valid_segment


@pytest.mark.parametrize("bad_segment", [None, "", "  ", "NotASegment", 42])
def test_synthesize_insight_defaults_invalid_commercial_segment(bad_segment):
    mock = _make_openai_mock_with_new_fields(commercial_segment=bad_segment)
    with patch("ingestion_engine._llm", return_value=mock):
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
    with patch("ingestion_engine._llm", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert result["signal_type"] == valid_signal


@pytest.mark.parametrize("bad_signal", [None, "", "BAD", 42])
def test_synthesize_insight_defaults_invalid_signal_type(bad_signal):
    mock = _make_openai_mock_with_new_fields(signal_type=bad_signal)
    with patch("ingestion_engine._llm", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert result["signal_type"] == "Other"


def test_synthesize_insight_drops_strategic_segment_field():
    """If the LLM still returns strategic_segment, it must not appear in the result."""
    mock = _make_openai_mock_with_new_fields(strategic_segment="LegacyValue")
    with patch("ingestion_engine._llm", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert "strategic_segment" not in result


def test_config_has_commercial_segments_and_signal_types():
    """market_pulse_config.yaml must expose the new commercial_segments,
    signal_types, macro_conditions, executive_bullet_labels, and
    delivery_suppression blocks with the expected labels."""
    import yaml

    from insight import VALID_COMMERCIAL_SEGMENTS
    with open("market_pulse_config.yaml") as fh:
        cfg = yaml.safe_load(fh)

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


# ===========================================================================
# Task 5 — structured macro summary (dominant_condition + executive_bullets)
# ===========================================================================

def _make_macro_mock(payload: dict) -> FakeLLM:
    return FakeLLM(returns=payload)


def _make_articles(n: int) -> list[dict]:
    return [
        {"category": "competitors", "headline": f"H{i}",
         "sentiment_score": 5, "americhem_impact": f"Impact {i}."}
        for i in range(n)
    ]


def _capture_summary(fake_repo) -> dict:
    """Return the most recent summary row stored in the fake repo."""
    from datetime import date
    row = fake_repo.get_delivery_state(
        run_date=date.today().isoformat(),
        run_mode=_ingestion_run_mode(),
    )
    assert row is not None, "No summary row was upserted"
    return row


def _ingestion_run_mode() -> str:
    from ingestion_engine import _run_mode
    return _run_mode()


def test_generate_macro_summary_writes_dominant_condition_when_valid():
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    payload = {
        "dominant_condition": "Competitive Pressure",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "Body A."},
            {"label": "Supply chain watch", "body": "Body B."},
            {"label": "Commercial action",  "body": "Body C."},
        ],
    }
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        assert generate_macro_summary(_make_articles(5)) is True
    row = _capture_summary(fake_repo)
    assert row["dominant_condition"] == "Competitive Pressure"
    # Each bullet gains citation_source_ids (empty when LLM returns none)
    expected_bullets = [
        {"label": "Market pressure",    "body": "Body A.", "citation_source_ids": []},
        {"label": "Supply chain watch", "body": "Body B.", "citation_source_ids": []},
        {"label": "Commercial action",  "body": "Body C.", "citation_source_ids": []},
    ]
    assert row["executive_bullets"] == expected_bullets
    # Legacy fields still populated for backward compat:
    assert row["macro_sentiment"] == "Competitive Pressure"
    assert row["executive_summary"]  # joined paragraph


def test_generate_macro_summary_coerces_invalid_dominant_condition():
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    payload = {
        "dominant_condition": "NonExistentCondition",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
    }
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        generate_macro_summary(_make_articles(5))
    row = _capture_summary(fake_repo)
    assert row["dominant_condition"] == "Mixed / Watch"


def test_generate_macro_summary_defaults_low_signal_when_few_articles():
    """When fewer than 3 articles are passed in and the LLM omits a valid condition,
    default to Low Signal."""
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    payload = {"executive_bullets": [
        {"label": "Market pressure",    "body": "Quiet day."},
        {"label": "Supply chain watch", "body": "Quiet day."},
        {"label": "Commercial action",  "body": "Anything."},
    ]}
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        generate_macro_summary(_make_articles(2))
    row = _capture_summary(fake_repo)
    assert row["dominant_condition"] == "Low Signal"


def test_generate_macro_summary_low_signal_coerces_action_body():
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    payload = {
        "dominant_condition": "Low Signal",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "Quiet day."},
            {"label": "Supply chain watch", "body": "Quiet day."},
            {"label": "Commercial action",  "body": "Sales should call every customer."},
        ],
    }
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        generate_macro_summary(_make_articles(2))
    row = _capture_summary(fake_repo)
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
    [{"body": "A."},                                   # missing label key
     {"label": "Supply chain watch", "body": "B."},
     {"label": "Commercial action", "body": "C."}],
    "not a list",                                      # wrong type
])
def test_generate_macro_summary_invalid_bullets_set_null(bad_bullets):
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    payload = {"dominant_condition": "Mixed / Watch", "executive_bullets": bad_bullets}
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        generate_macro_summary(_make_articles(5))
    row = _capture_summary(fake_repo)
    assert row["executive_bullets"] is None
    # Legacy executive_summary still populated so delivery has a fallback:
    assert row["executive_summary"]


def test_generate_macro_summary_persists_validated_citations():
    fake = FakeLLM(returns={
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Pricing firm.", "citation_source_ids": [1, 99]},
            {"label": "Supply chain watch", "body": "Freight rising.", "citation_source_ids": [2, 2]},
            {"label": "Commercial action", "body": "Watch packaging.", "citation_source_ids": []},
        ],
    })
    fake_repo = InMemoryIntelligenceRepo()
    articles = [
        {"category": "competitors", "headline": "Alpha", "americhem_impact_score": 9,
         "americhem_impact": "x", "source_url": "https://a.com/1", "url_hash": "h1",
         "commercial_segment": "Healthcare"},
        {"category": "competitors", "headline": "Bravo", "americhem_impact_score": 7,
         "americhem_impact": "y", "source_url": "https://b.com/2", "url_hash": "h2",
         "commercial_segment": "Auto"},
    ]

    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        assert generate_macro_summary(articles) is True

    stored = fake_repo.fetch_latest_summary(run_mode="production", min_date="2000-01-01")
    bullets = stored["executive_bullets"]
    assert bullets[0]["citation_source_ids"] == [1]   # 99 dropped (not in pack)
    assert bullets[1]["citation_source_ids"] == [2]   # deduped
    assert bullets[2]["citation_source_ids"] == []
    # executive_sources holds only cited ids (1 and 2), with full metadata.
    src_ids = sorted(s["id"] for s in stored["executive_sources"])
    assert src_ids == [1, 2]
    assert {s["domain"] for s in stored["executive_sources"]} == {"a.com", "b.com"}


def test_generate_macro_summary_numbers_the_digest():
    fake = FakeLLM(returns={
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "A.", "citation_source_ids": []},
            {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
        ],
    })
    fake_repo = InMemoryIntelligenceRepo()
    articles = [
        {"category": "competitors", "headline": "TopMateriality", "americhem_impact_score": 9,
         "americhem_impact": "x", "source_url": "https://a.com/1", "url_hash": "h1"},
    ]
    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        generate_macro_summary(articles)

    user_prompt = fake.calls[-1]["user"]
    assert "[1]" in user_prompt and "TopMateriality" in user_prompt


# ===========================================================================
# Task 6 — ingestion-side suppression accounting
# ===========================================================================

def test_generate_macro_summary_persists_suppression_breakdown_and_samples():
    """generate_macro_summary must accept counts and samples and persist them."""
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    counts = {"duplicate_url": 3, "llm_discard": 2}
    samples = [
        {"reason": "llm_discard", "url": "https://x.com/1", "title": "Bad article"},
    ]
    fake_repo = InMemoryIntelligenceRepo()
    payload = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
    }
    with patch("ingestion_engine._llm", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        generate_macro_summary(
            _make_articles(5),
            screened_count=87,
            suppression_breakdown=counts,
            suppression_samples=samples,
        )
    row = _capture_summary(fake_repo)
    assert row["screened_count"] == 87
    assert row["suppression_breakdown"] == counts
    assert row["suppression_samples"] == samples


# ===========================================================================
# Task 7 — run-mode isolation in delivery fetch_macro_summary()
# ===========================================================================

def test_fetch_macro_summary_filters_by_run_mode_production(monkeypatch):
    """Production delivery must fetch the production row even when a test row exists."""
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    from delivery_engine import fetch_macro_summary
    from datetime import date
    today = date.today().isoformat()

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "Prod summary", "macro_sentiment": "Stable",
    })
    fake.upsert_summary({
        "run_date": today, "run_mode": "test",
        "executive_summary": "Test summary", "macro_sentiment": "Stable",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    result = fetch_macro_summary()
    assert result is not None
    assert result["run_mode"] == "production"
    assert result["executive_summary"] == "Prod summary"


def test_fetch_macro_summary_filters_by_run_mode_test(monkeypatch):
    """Test delivery must fetch the test row, not the production row."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    from delivery_engine import fetch_macro_summary
    from datetime import date
    today = date.today().isoformat()

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "Prod summary", "macro_sentiment": "Stable",
    })
    fake.upsert_summary({
        "run_date": today, "run_mode": "test",
        "executive_summary": "Test summary", "macro_sentiment": "Stable",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    result = fetch_macro_summary()
    assert result is not None
    assert result["run_mode"] == "test"
    assert result["executive_summary"] == "Test summary"


def test_fetch_macro_summary_test_mode_falls_back_to_production_row(monkeypatch):
    """A delivery-only test run (run_ingestion=false) has no test-mode macro
    row — it must fall back to the production row read-only, so the QA
    re-render carries the executive summary and citation sources."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    from delivery_engine import fetch_macro_summary
    from datetime import date
    today = date.today().isoformat()

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "Prod summary", "macro_sentiment": "Stable",
        "executive_sources": [{"id": 1, "headline": "H", "url": "https://s/1",
                               "domain": "s.com", "segment": "Packaging", "score": 9}],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    result = fetch_macro_summary()
    assert result is not None
    assert result["run_mode"] == "production"
    assert result["executive_sources"]


def test_fetch_macro_summary_test_mode_prefers_newer_production_over_stale_test_row(monkeypatch):
    """A test row from YESTERDAY (run_ingestion=true QA run the day before)
    must not shadow TODAY's production row — the re-render would pair today's
    articles with stale executive bullets/citations."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    from delivery_engine import fetch_macro_summary
    from datetime import date, timedelta as _td
    today = date.today().isoformat()
    yesterday = (date.today() - _td(days=1)).isoformat()

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": yesterday, "run_mode": "test",
        "executive_summary": "Stale test summary", "macro_sentiment": "Stable",
    })
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "Fresh prod summary", "macro_sentiment": "Stable",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    result = fetch_macro_summary()
    assert result is not None
    assert result["executive_summary"] == "Fresh prod summary"


def test_fetch_macro_summary_test_mode_keeps_test_row_on_run_date_tie(monkeypatch):
    """Recency ties prefer the test row — covers the date-rollover grace
    (test ingestion writes at 23:59, delivery reads at 00:01: both candidate
    rows carry yesterday's run_date and the minutes-old test row must win)."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    from delivery_engine import fetch_macro_summary
    from datetime import date, timedelta as _td
    yesterday = (date.today() - _td(days=1)).isoformat()

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": yesterday, "run_mode": "test",
        "executive_summary": "Rollover test summary", "macro_sentiment": "Stable",
    })
    fake.upsert_summary({
        "run_date": yesterday, "run_mode": "production",
        "executive_summary": "Prod summary", "macro_sentiment": "Stable",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    result = fetch_macro_summary()
    assert result is not None
    assert result["executive_summary"] == "Rollover test summary"


def test_fetch_macro_summary_test_mode_accounting_only_test_row_does_not_shadow_production(monkeypatch):
    """A zero-yield test ingestion run persists an accounting-only test row
    (issue #43). On a run-date tie it must NOT shadow a content-full production
    row — content-fullness is compared before recency, so the QA re-render
    keeps the executive summary."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    from delivery_engine import fetch_macro_summary
    from datetime import date
    today = date.today().isoformat()

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": today, "run_mode": "test",
        "screened_count": 9, "suppression_breakdown": {"duplicate_url": 9},
        "suppression_samples": [],
    })
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "Prod summary", "macro_sentiment": "Stable",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    result = fetch_macro_summary()
    assert result is not None
    assert result["executive_summary"] == "Prod summary"


def test_fetch_macro_summary_test_mode_accounting_only_production_row_does_not_shadow_test(monkeypatch):
    """The mirror direction: a strictly-newer accounting-only production row
    (zero-yield production run today) must not shadow yesterday's content-full
    test row — pre-#43 no production row would have existed at all."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    from delivery_engine import fetch_macro_summary
    from datetime import date, timedelta as _td
    today = date.today().isoformat()
    yesterday = (date.today() - _td(days=1)).isoformat()

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": yesterday, "run_mode": "test",
        "executive_summary": "Rollover test summary", "macro_sentiment": "Stable",
    })
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "screened_count": 14, "suppression_breakdown": {"scrape_failed": 14},
        "suppression_samples": [],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    result = fetch_macro_summary()
    assert result is not None
    assert result["executive_summary"] == "Rollover test summary"


def test_fetch_macro_summary_test_mode_returns_accounting_only_row_when_no_content_anywhere(monkeypatch):
    """When the only candidate is an accounting-only test row, return it — the
    QA debug section still renders that day's suppression accounting."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    from delivery_engine import fetch_macro_summary
    from datetime import date
    today = date.today().isoformat()

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": today, "run_mode": "test",
        "screened_count": 6, "suppression_breakdown": {"unscrapable_domain": 6},
        "suppression_samples": [],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    result = fetch_macro_summary()
    assert result is not None
    assert result["screened_count"] == 6


def test_render_report_tolerates_accounting_only_macro_summary(monkeypatch):
    """A summary-less row (zero-yield ingestion day, issue #43) renders without
    crashing: no Executive Summary block, no Macroeconomic Outlook, but the
    QA suppression summary and the screened count in the subtitle still come
    from the row's accounting."""
    summary = {
        "run_date": "2026-07-17", "run_mode": "test",
        "screened_count": 21,
        "suppression_breakdown": {"duplicate_url": 5},
        "suppression_samples": [{"reason": "duplicate_url", "url": "https://x/d", "title": "Dup"}],
    }
    rows = [
        {"url_hash": "v1", "commercial_segment": "Packaging",
         "americhem_impact_score": 7, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": "Visible packaging signal",
         "americhem_impact": "Effect.", "source_url": "https://x/v1",
         "entities_mentioned": ["Acme"]},
    ]
    model = assemble_report(rows, summary, config={"reporting": {"visible_impact_threshold": 6}})
    html = render_report(model, today_str=_TODAY_STR, test_mode=True)
    assert "Executive Summary" not in html
    assert "MACROECONOMIC OUTLOOK" not in html
    assert "Suppression Summary" in html
    assert "21 screened items" in html


def test_fetch_macro_summary_production_never_reads_test_rows(monkeypatch):
    """The fallback is one-directional: production delivery with only a test
    row available must return None, not leak QA data into production mail."""
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    from delivery_engine import fetch_macro_summary
    from datetime import date
    today = date.today().isoformat()

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": today, "run_mode": "test",
        "executive_summary": "Test summary", "macro_sentiment": "Stable",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    assert fetch_macro_summary() is None


def test_run_mode_helper(monkeypatch):
    """_run_mode() returns 'test' when env=test; 'production' otherwise; case-insensitive."""
    from delivery_engine import _run_mode
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    assert _run_mode() == "test"
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "TEST")
    assert _run_mode() == "test"
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "")
    assert _run_mode() == "production"
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    assert _run_mode() == "production"


# ===========================================================================
# Task 9 — _apply_delivery_suppression()
# ===========================================================================

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
    from report import _apply_delivery_suppression
    rows = [_row(commercial_segment="Enterprise / Cross-Segment", americhem_impact_score=5)]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"enterprise_cross_segment_low_impact": 1}
    assert ledger.samples[0].to_dict()["reason"] == "enterprise_cross_segment_low_impact"


def test_apply_delivery_suppression_keeps_enterprise_high_impact():
    from report import _apply_delivery_suppression
    rows = [_row(commercial_segment="Enterprise / Cross-Segment", americhem_impact_score=8)]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {}


def test_apply_delivery_suppression_drops_product_listing():
    from report import _apply_delivery_suppression
    rows = [_row(source_url="https://example.com/product/widget")]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"product_listing": 1}


def test_apply_delivery_suppression_drops_job_posting():
    from report import _apply_delivery_suppression
    rows = [_row(source_url="https://www.linkedin.com/jobs/12345")]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"job_posting": 1}


def test_apply_delivery_suppression_job_posting_escalate_override():
    """A job-posting URL with recommended_action='Escalate to leadership' is kept."""
    from report import _apply_delivery_suppression
    rows = [_row(source_url="https://www.linkedin.com/jobs/ceo-move",
                 recommended_action="Escalate to leadership")]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {}


def test_apply_delivery_suppression_drops_generic_market_report_no_entities():
    from report import _apply_delivery_suppression
    rows = [_row(headline="Global Polypropylene Market Size 2026-2032",
                 entities_mentioned=[])]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"generic_market_report": 1}


def test_apply_delivery_suppression_keeps_generic_market_report_with_entities():
    from report import _apply_delivery_suppression
    rows = [_row(headline="Global Polypropylene Market 2026 Report",
                 entities_mentioned=["Avient"])]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {}


def test_apply_delivery_suppression_drops_unrelated_color_result():
    from report import _apply_delivery_suppression
    rows = [_row(headline="What extension cord colors mean",
                 americhem_impact="No plastics relevance.",
                 entities_mentioned=["DIY Network"])]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"unrelated_color_result": 1}


def test_apply_delivery_suppression_keeps_color_result_with_plastics_term():
    from report import _apply_delivery_suppression
    rows = [_row(headline="New masterbatch colors for automotive interiors",
                 americhem_impact="Drives masterbatch demand.",
                 entities_mentioned=["BASF"])]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {}


def test_apply_delivery_suppression_drops_exact_duplicate_headline():
    from report import _apply_delivery_suppression
    rows = [_row(url_hash="a", headline="Plant fire halts production"),
            _row(url_hash="b", headline="Plant fire halts production")]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert kept[0]["url_hash"] == "a"
    assert dict(ledger.breakdown) == {"duplicate_headline": 1}


def test_apply_delivery_suppression_drops_semantic_duplicate_headline():
    from report import _apply_delivery_suppression
    rows = [
        _row(url_hash="a", headline="Plant fire halts production at BASF site"),
        _row(url_hash="b", headline="BASF plant fire halts production at site"),
    ]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {"semantic_duplicate_headline": 1}


def test_apply_delivery_suppression_first_match_wins():
    """A row matching both product_listing and generic_market_report is counted once,
    under product_listing (which is checked first in the rule order)."""
    from report import _apply_delivery_suppression
    rows = [_row(source_url="https://amazon.com/product/123",
                 headline="Plastic Market Report 2026",
                 entities_mentioned=[])]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"product_listing": 1}  # NOT generic_market_report


def test_apply_delivery_suppression_disabled_rule_allows_through():
    from report import _apply_delivery_suppression
    cfg = _supp_config(enable_product_listing=False)
    rows = [_row(source_url="https://example.com/product/widget")]
    kept, ledger = _apply_delivery_suppression(rows, cfg)
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {}


def test_apply_delivery_suppression_samples_capped_at_10():
    from report import _apply_delivery_suppression
    rows = [
        _row(url_hash=f"h{i}", source_url=f"https://amazon.com/product/{i}",
             headline=f"Product {i}")
        for i in range(15)
    ]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert ledger.breakdown["product_listing"] == 15
    assert len(ledger.samples) == 10


# ---------------------------------------------------------------------------
# Task 10: _group_by_commercial_segment + _render_segment_watch_section
# ---------------------------------------------------------------------------

def test_group_by_commercial_segment_keys_off_new_field():
    from report import _group_by_commercial_segment
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


def test_group_by_commercial_segment_defaults_when_field_missing():
    from report import _group_by_commercial_segment
    rows = [
        {"url_hash": "a", "americhem_impact_score": 8, "headline": "A"},
        {"url_hash": "b", "commercial_segment": "Packaging",
         "americhem_impact_score": 7, "headline": "B"},
    ]
    groups = _group_by_commercial_segment(rows)
    assert "Enterprise / Cross-Segment" in groups
    assert "Packaging" in groups


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
            "commercial_segment": "Healthcare",
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
            "commercial_segment": "Enterprise / Cross-Segment",
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


# ---------------------------------------------------------------------------
# Task 11: report pipeline integration tests
# ---------------------------------------------------------------------------

def test_prepare_report_surfaced_count_is_post_cap(monkeypatch):
    """The written-back surfaced_count must reflect the final visible-card list AFTER per-segment caps."""
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    from datetime import date

    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
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

    fake = InMemoryIntelligenceRepo()
    today = date.today().isoformat()
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": {}, "suppression_samples": [],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    with patch("delivery_engine._llm", return_value=FakeLLM()):
        prepare_report(rows, None, config=config)

    stored = fake.get_delivery_state(run_date=today, run_mode="production")
    assert stored is not None, "Expected an update() call to daily_summaries"
    assert stored["surfaced_count"] == 2


def test_prepare_report_writes_delivery_suppression_counts_back(monkeypatch):
    """Delivery must write below_impact_threshold into suppression_breakdown via update()."""
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    from datetime import date

    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    rows = [
        {"url_hash": "low", "commercial_segment": "Healthcare",
         "americhem_impact_score": 4, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": "Below threshold",
         "americhem_impact": ".", "source_url": "https://x/1",
         "entities_mentioned": ["Acme"]},
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
    fake = InMemoryIntelligenceRepo()
    today = date.today().isoformat()
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": {}, "suppression_samples": [],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    with patch("delivery_engine._llm", return_value=FakeLLM()):
        prepare_report(rows, None, config=config)

    stored = fake.get_delivery_state(run_date=today, run_mode="production")
    breakdown = stored["suppression_breakdown"]
    assert breakdown["below_impact_threshold"] == 1
    assert stored["surfaced_count"] == 1


def test_prepare_report_update_filtered_by_run_date_and_run_mode(monkeypatch):
    """The update() call must be filtered by run_date AND run_mode."""
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    from datetime import date

    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    rows = [{
        "url_hash": "a", "commercial_segment": "Healthcare",
        "americhem_impact_score": 8, "sentiment_tag": "Neutral",
        "signal_type": "Customer", "headline": "H", "americhem_impact": ".",
        "source_url": "https://x/a", "entities_mentioned": ["Acme"],
    }]

    fake = InMemoryIntelligenceRepo()
    today = date.today().isoformat()
    fake.upsert_summary({
        "run_date": today, "run_mode": "test",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": {}, "suppression_samples": [],
    })
    update_calls = []
    real_update = fake.update_delivery_counts

    def spy_update(*, run_date, run_mode, surfaced_count, ledger_row):
        update_calls.append({"run_date": run_date, "run_mode": run_mode})
        return real_update(
            run_date=run_date, run_mode=run_mode,
            surfaced_count=surfaced_count, ledger_row=ledger_row,
        )
    fake.update_delivery_counts = spy_update
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    with patch("delivery_engine._llm", return_value=FakeLLM()):
        prepare_report(rows, None, config={"reporting": {"visible_impact_threshold": 6}})

    assert update_calls, f"Expected update_delivery_counts call. calls={update_calls}"
    keys = set()
    for call in update_calls:
        keys.update(call.keys())
    assert "run_date" in keys, f"calls: {update_calls}"
    assert "run_mode" in keys, f"calls: {update_calls}"
    rm_values = [c["run_mode"] for c in update_calls]
    assert any(v == "test" for v in rm_values), f"Expected run_mode='test' in {rm_values}"


def test_prepare_report_synthesis_sees_only_final_capped_groups(monkeypatch):
    """Thematic synthesis must receive ONLY the final capped groups with 2+
    articles — capped-out rows and single-article segments never reach the LLM."""
    from daily_intelligence_repo import InMemoryIntelligenceRepo

    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    hc_headlines = [
        "Hospital network merger squeezes specialty polymer volumes",
        "FDA clears new implantable-grade compound for cardiac devices",
        "Aging population drives record demand for medical-grade resins",
    ]
    rows = [
        {"url_hash": f"h{i}", "commercial_segment": "Healthcare",
         "americhem_impact_score": 9 - i, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": hc_headlines[i],
         "americhem_impact": f"Healthcare impact {i}.",
         "source_url": f"https://x/h{i}", "entities_mentioned": ["Acme"]}
        for i in range(3)
    ] + [
        {"url_hash": "p0", "commercial_segment": "Packaging",
         "americhem_impact_score": 8, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": "Single packaging signal",
         "americhem_impact": "Packaging impact.",
         "source_url": "https://x/p0", "entities_mentioned": ["Acme"]},
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 2,
            "max_total_visible_articles": 12,
        }
    }
    fake_llm = FakeLLM(returns={"Healthcare": "Healthcare synthesis."})
    monkeypatch.setattr("delivery_engine._repo", lambda: InMemoryIntelligenceRepo())

    with patch("delivery_engine._llm", return_value=fake_llm):
        model = prepare_report(rows, None, config=config)

    assert len(fake_llm.calls) == 1
    user = fake_llm.calls[-1]["user"]
    assert "CATEGORY: Healthcare" in user
    assert "Healthcare impact 0." in user and "Healthcare impact 1." in user
    assert "Healthcare impact 2." not in user      # capped out by max_per_segment=2
    assert "CATEGORY: Packaging" not in user       # single-article group, no synthesis
    assert model.synthesis == {"Healthcare": "Healthcare synthesis."}


def test_prepare_report_no_news_skips_write_back_and_llm(monkeypatch):
    """The no_news variant performs neither side effect: no daily_summaries
    write-back and no LLM call (the no-news path never wrote back)."""
    repo_touched: list[bool] = []

    def spy_repo():
        repo_touched.append(True)
        from daily_intelligence_repo import InMemoryIntelligenceRepo
        return InMemoryIntelligenceRepo()

    monkeypatch.setattr("delivery_engine._repo", spy_repo)
    fake_llm = FakeLLM()

    with patch("delivery_engine._llm", return_value=fake_llm):
        model = prepare_report([], None, config={})

    assert model.variant == "no_news"
    assert repo_touched == []
    assert fake_llm.calls == []


def _seed_delivery_repo(run_mode: str):
    """InMemory repo with two visible Healthcare rows and today's summary row."""
    from datetime import date
    fake = InMemoryIntelligenceRepo()
    headlines = [
        "Hospital network merger squeezes specialty polymer volumes",
        "FDA clears new implantable-grade compound for cardiac devices",
    ]
    for i, headline in enumerate(headlines):
        fake.upsert_insight({
            "url_hash": f"wire{i}", "headline": headline,
            "americhem_impact_score": 8, "sentiment_tag": "Neutral",
            "signal_type": "Customer", "commercial_segment": "Healthcare",
            "americhem_impact": "Wiring effect.", "source_url": f"https://x/wire{i}",
            "entities_mentioned": ["Acme"],
        })
    today = date.today().isoformat()
    fake.upsert_summary({
        "run_date": today, "run_mode": run_mode,
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": {}, "suppression_samples": [],
    })
    return fake, today


def test_delivery_execute_pipeline_wires_prepare_render_and_env(monkeypatch):
    """End-to-end wiring of delivery's entrypoint: fetch → prepare_report
    (write-back + synthesis, exactly once) → render_report with test_mode
    resolved from MARKET_PULSE_RUN_MODE → send_email. Pins the composition
    itself: swapping prepare_report for assemble_report (write-back silently
    lost) or dropping the env→test_mode wiring must fail this test."""
    import delivery_engine

    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    fake, today = _seed_delivery_repo("test")
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    sent: dict = {}
    monkeypatch.setattr("delivery_engine.send_email",
                        lambda html: sent.__setitem__("html", html))

    fake_llm = FakeLLM(returns={"Healthcare": "Wired synthesis paragraph."})
    with patch("delivery_engine._llm", return_value=fake_llm), \
         patch("delivery_engine._load_mp_config",
               return_value={"reporting": {"visible_impact_threshold": 6}}):
        delivery_engine.execute_pipeline()

    html = sent["html"]
    # env → render wiring: MARKET_PULSE_RUN_MODE=test marks the HTML body.
    assert "[TEST]" in html
    assert "TEST RUN" in html
    # prepare_report ran: its synthesis reached the rendered email...
    assert "Wired synthesis paragraph." in html
    # ...and its write-back landed on today's daily_summaries row.
    stored = fake.get_delivery_state(run_date=today, run_mode="test")
    assert stored is not None and stored["surfaced_count"] == 2


def test_delivery_only_test_run_renders_exec_summary_without_touching_prod_row(monkeypatch):
    """The run_ingestion=false QA scenario: only a PRODUCTION macro row exists
    (test-mode ingestion never ran). The test-mode delivery must still render
    the executive summary + sources from it, and its write-back must be a
    silent no-op on the production row."""
    import copy
    import delivery_engine

    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    fake, today = _seed_delivery_repo("production")   # production row only — no test row
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "dominant_condition": "Supply Volatility",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
            {"label": "Commercial action",  "body": "C.", "citation_source_ids": []},
        ],
        "executive_sources": [{"id": 1, "headline": "Resin prices climb",
                               "url": "https://s/1", "domain": "s.com",
                               "segment": "Packaging", "score": 9}],
        "suppression_breakdown": {"duplicate_url": 3}, "suppression_samples": [],
    })
    prod_row_before = copy.deepcopy(fake.get_delivery_state(run_date=today, run_mode="production"))
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    sent: dict = {}
    monkeypatch.setattr("delivery_engine.send_email",
                        lambda html: sent.__setitem__("html", html))

    with patch("delivery_engine._llm", return_value=FakeLLM(returns={})), \
         patch("delivery_engine._load_mp_config",
               return_value={"reporting": {"visible_impact_threshold": 6}}):
        delivery_engine.execute_pipeline()

    html = sent["html"]
    assert "Executive Summary" in html
    assert "Market pressure" in html
    assert "Resin prices climb" in html                 # cited source in the footer
    assert "[TEST]" in html and "TEST RUN" in html      # still marked as QA output
    # Production accounting untouched: write-back keyed run_mode='test' matched
    # no row (silent no-op), and no test row was created.
    assert fake.get_delivery_state(run_date=today, run_mode="production") == prod_row_before
    assert fake.get_delivery_state(run_date=today, run_mode="test") is None


def test_delivery_execute_pipeline_production_env_ships_unmarked_html(monkeypatch):
    """The inverse wiring check: with MARKET_PULSE_RUN_MODE unset, the sent
    HTML carries no test markers (a hardcoded test_mode=True must fail here)."""
    import delivery_engine

    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    fake, _today = _seed_delivery_repo("production")
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    sent: dict = {}
    monkeypatch.setattr("delivery_engine.send_email",
                        lambda html: sent.__setitem__("html", html))

    with patch("delivery_engine._llm", return_value=FakeLLM()), \
         patch("delivery_engine._load_mp_config",
               return_value={"reporting": {"visible_impact_threshold": 6}}):
        delivery_engine.execute_pipeline()

    assert "[TEST]" not in sent["html"]
    assert "TEST RUN" not in sent["html"]


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


# ===========================================================================
# Task 13 — Null-safe header fallbacks (screened_count, dominant_condition)
# ===========================================================================

def test_header_falls_back_to_len_data_when_screened_null():
    """When screened_count is NULL, header uses len(data)."""
    rows = [
        {"url_hash": f"h{i}", "commercial_segment": "Healthcare",
         "americhem_impact_score": 8, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": f"Distinct Healthcare News {i}",
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

    model = assemble_report(rows, macro, config={"reporting": {"visible_impact_threshold": 6}})
    html = render_report(model, today_str=_TODAY_STR)

    assert "from 7 screened items" in html
    assert "from None screened items" not in html


def test_header_omits_dominant_condition_clause_when_null():
    """When dominant_condition is NULL, the badge clause is omitted (no literal 'None')."""
    rows = [{"url_hash": "a", "commercial_segment": "Healthcare",
             "americhem_impact_score": 8, "sentiment_tag": "Neutral",
             "signal_type": "Customer", "headline": "Some Distinct Headline",
             "americhem_impact": ".", "source_url": "https://x/a",
             "entities_mentioned": ["Acme"]}]
    macro = {"executive_bullets": None, "executive_summary": "Fallback prose.",
             "dominant_condition": None, "macro_sentiment": None,
             "screened_count": 5, "surfaced_count": 1}

    model = assemble_report(rows, macro, config={"reporting": {"visible_impact_threshold": 6}})
    html = render_report(model, today_str=_TODAY_STR)

    # The literal string 'None' must not appear anywhere as a rendered value.
    assert ">None<" not in html
    assert "Dominant condition: None" not in html


# ===========================================================================
# Task 14 — QA suppression-summary section
# ===========================================================================

def test_qa_debug_section_appears_in_test_mode():
    rows = [{"url_hash": "a", "commercial_segment": "Healthcare",
             "americhem_impact_score": 8, "sentiment_tag": "Neutral",
             "signal_type": "Customer", "headline": "Some Distinct QA Headline",
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

    model = assemble_report(rows, macro, config={"reporting": {"visible_impact_threshold": 6}})
    html = render_report(model, today_str=_TODAY_STR, test_mode=True)

    assert "QA" in html
    assert "Suppression Summary" in html
    # Friendly labels expected (Task 14 spec uses friendly forms in the email).
    assert "duplicate URL" in html
    assert "product listing" in html
    assert "Pretty plastic tote" in html
    assert "Best extension cord colors" in html


def test_qa_debug_section_absent_in_production():
    rows = [{"url_hash": "a", "commercial_segment": "Healthcare",
             "americhem_impact_score": 8, "sentiment_tag": "Neutral",
             "signal_type": "Customer", "headline": "Production Distinct Headline",
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

    model = assemble_report(rows, macro, config={"reporting": {"visible_impact_threshold": 6}})
    html = render_report(model, today_str=_TODAY_STR, test_mode=False)

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
    assert "duplicate URL" in html
    assert "semantic duplicate" in html
    assert "LLM discard" in html
    assert "Enterprise / Cross-Segment" in html


def test_render_qa_debug_section_includes_relevance_gate_drops():
    """The ZoomInfo relevance-gate code must get a labeled breakdown row so its
    count is visible during the test-pipeline validation run (not just folded
    into the suppressed total)."""
    from delivery_engine import _render_qa_debug_section
    macro = {
        "screened_count": 40,
        "surfaced_count": 5,
        "suppression_breakdown": {"zoominfo_company_mismatch": 3},
        "suppression_samples": [],
    }
    html = _render_qa_debug_section(macro)
    assert "ZoomInfo company mismatch" in html
    assert ">3</td>" in html


# ===========================================================================
# PR #7 fix — idempotent suppression breakdown on same-day retries
# ===========================================================================

def test_update_delivery_summary_counts_overwrites_delivery_keys(monkeypatch):
    """Delivery-owned keys must be REPLACED, not added, on retry. Ingestion-owned
    keys must be preserved unchanged."""
    from suppression_ledger import SuppressionLedger
    from delivery_engine import _update_delivery_summary_counts
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    from datetime import date

    prior = {
        "duplicate_url": 10,            # ingestion-owned
        "semantic_duplicate": 2,        # ingestion-owned
        "below_impact_threshold": 22,   # delivery-owned (must be replaced)
        "weak_relevance": 7,            # delivery-owned (must be replaced)
    }

    fake = InMemoryIntelligenceRepo()
    today = date.today().isoformat()
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": prior,
        "suppression_samples": [],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)

    ledger = (SuppressionLedger.for_delivery()
              .record_count("below_impact_threshold", 5)
              .record_count("weak_relevance", 2))

    _update_delivery_summary_counts(surfaced_count=6, ledger=ledger)

    stored = fake.get_delivery_state(run_date=today, run_mode="production")
    merged = stored["suppression_breakdown"]
    # Ingestion-owned keys preserved unchanged:
    assert merged["duplicate_url"] == 10
    assert merged["semantic_duplicate"] == 2
    # Delivery-owned keys REPLACED (not added):
    assert merged["below_impact_threshold"] == 5, "delivery-owned count must be overwritten, not added"
    assert merged["weak_relevance"] == 2


def test_update_delivery_summary_counts_idempotent_on_retry(monkeypatch):
    """Two consecutive calls with the same ledger must produce the same
    final breakdown — no doubling."""
    from suppression_ledger import SuppressionLedger
    from delivery_engine import _update_delivery_summary_counts
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    from datetime import date

    fake = InMemoryIntelligenceRepo()
    today = date.today().isoformat()
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": {"duplicate_url": 10},
        "suppression_samples": [],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)

    ledger = (SuppressionLedger.for_delivery()
              .record_count("below_impact_threshold", 22)
              .record("product_listing", url="https://amazon.com/p/1", title="Plastic tote")
              .record_count("product_listing", 4))  # total product_listing = 5

    _update_delivery_summary_counts(surfaced_count=6, ledger=ledger)
    stored = fake.get_delivery_state(run_date=today, run_mode="production")
    first_breakdown = dict(stored["suppression_breakdown"])
    first_samples = list(stored["suppression_samples"])

    _update_delivery_summary_counts(surfaced_count=6, ledger=ledger)
    stored = fake.get_delivery_state(run_date=today, run_mode="production")
    second_breakdown = dict(stored["suppression_breakdown"])
    second_samples = list(stored["suppression_samples"])

    assert first_breakdown == second_breakdown, \
        f"Retry must be idempotent. First={first_breakdown} Second={second_breakdown}"
    assert second_breakdown["below_impact_threshold"] == 22, "must not double"
    assert second_breakdown["product_listing"] == 5, "must not double"
    assert second_breakdown["duplicate_url"] == 10, "ingestion-owned key preserved"
    assert first_samples == second_samples, \
        f"Retry must not duplicate samples. First={first_samples} Second={second_samples}"


def test_update_delivery_summary_counts_preserves_unknown_prior_keys(monkeypatch):
    """Unknown keys in the existing breakdown (e.g., future codes) must be preserved."""
    from suppression_ledger import SuppressionLedger
    from delivery_engine import _update_delivery_summary_counts
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    from datetime import date

    prior = {"some_future_reason": 99, "duplicate_url": 5}
    fake = InMemoryIntelligenceRepo()
    today = date.today().isoformat()
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": prior,
        "suppression_samples": [],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)

    ledger = SuppressionLedger.for_delivery().record_count("below_impact_threshold", 2)

    _update_delivery_summary_counts(surfaced_count=1, ledger=ledger)

    stored = fake.get_delivery_state(run_date=today, run_mode="production")
    merged = stored["suppression_breakdown"]
    assert merged["some_future_reason"] == 99
    assert merged["duplicate_url"] == 5
    assert merged["below_impact_threshold"] == 2


def test_delivery_suppression_idempotent_on_same_day_retry(monkeypatch):
    """Running delivery twice in the same day with the same inputs must
    produce identical persisted breakdown and samples."""
    from suppression_ledger import SuppressionLedger
    from delivery_engine import _update_delivery_summary_counts
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    from datetime import date

    fake = InMemoryIntelligenceRepo()
    today = date.today().isoformat()
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": {},
        "suppression_samples": [],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)

    ledger = (SuppressionLedger.for_delivery()
              .record("duplicate_headline", url="u", title="t")
              .record_count("below_impact_threshold", 3))

    _update_delivery_summary_counts(surfaced_count=5, ledger=ledger)
    stored = fake.get_delivery_state(run_date=today, run_mode="production")
    first_breakdown = dict(stored["suppression_breakdown"])
    first_samples = list(stored["suppression_samples"])

    _update_delivery_summary_counts(surfaced_count=5, ledger=ledger)
    stored = fake.get_delivery_state(run_date=today, run_mode="production")
    second_breakdown = dict(stored["suppression_breakdown"])
    second_samples = list(stored["suppression_samples"])

    assert first_breakdown == second_breakdown, \
        f"Retry must be idempotent. First={first_breakdown} Second={second_breakdown}"
    assert first_samples == second_samples, \
        f"Retry must not duplicate samples. First={first_samples} Second={second_samples}"


# ---------------------------------------------------------------------------
# 7. English-output rule — prompt-contract tests
# ---------------------------------------------------------------------------



def test_synthesize_insight_non_english_body_keeps_english_directive():
    """Regression: a Chinese article body must reach synthesize_insight with the
    English-output directive intact in the system prompt, and the source-language
    body must be forwarded verbatim in the user prompt (no client-side translation)."""
    chinese_body = "中文测试文本 — Teknor Apex 推出含 70% PCR 的 Crealen R PP 汽车内饰再生材料。"

    fake = _make_openai_mock(5)
    with patch("ingestion_engine._llm", return_value=fake):
        result = synthesize_insight(
            article_text=chinese_body,
            source_url="https://example.cn/article",
            trigger_entity="Teknor Apex",
            category="competitors",
        )

    assert result is not None
    system_message = fake.calls[-1]["system"]
    user_message = fake.calls[-1]["user"]
    from prompts import ENGLISH_OUTPUT_RULE
    assert ENGLISH_OUTPUT_RULE in system_message
    assert chinese_body in user_message, (
        "Source-language article body should be forwarded verbatim to the LLM; "
        "no client-side translation should occur."
    )


def test_generate_macro_summary_ships_prompts_module_text_across_seam():
    """Seam-crossing check: the system/user text generate_macro_summary sends
    through the LLM seam is exactly what prompts.macro_prompt assembles — an
    engine-side prompt override cannot pass unnoticed."""
    import prompts
    from ingestion_engine import generate_macro_summary
    from daily_intelligence_repo import InMemoryIntelligenceRepo as _Repo

    articles = [{"category": "competitors", "headline": "Stub headline",
                 "sentiment_score": 5, "americhem_impact": "Stub impact."}]
    fake = FakeLLM(returns=None)
    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: _Repo()):
        generate_macro_summary(articles)

    mp = prompts.macro_prompt(articles)
    assert fake.calls[-1]["system"] == mp.system
    assert fake.calls[-1]["user"] == mp.user


def test_synthesize_thematic_ships_prompts_module_text_across_seam():
    """Seam-crossing check: the thematic system/user text crossing the LLM seam
    is exactly what prompts.thematic_prompt assembles."""
    import prompts

    groups = {"Healthcare": [
        _make_article("a", 8, "competitors"),
        _make_article("b", 7, "competitors"),
    ]}
    fake = FakeLLM(returns=None)
    with patch("delivery_engine._llm", return_value=fake):
        synthesize_thematic_paragraphs(groups)

    spec = prompts.thematic_prompt(groups)
    assert fake.calls[-1]["system"] == spec.system
    assert fake.calls[-1]["user"] == spec.user


# ---------------------------------------------------------------------------
# Repository wiring — ingestion paths route through _repo()
# ---------------------------------------------------------------------------

from daily_intelligence_repo import InMemoryIntelligenceRepo


def test_url_already_processed_routes_through_repo(monkeypatch):
    """url_already_processed returns True iff the InMemory fake reports a hit."""
    from ingestion_engine import url_already_processed
    fake = InMemoryIntelligenceRepo()
    fake.upsert_insight({"url_hash": "abc123", "headline": "Test"})
    monkeypatch.setattr("ingestion_engine._repo", lambda: fake)
    assert url_already_processed("abc123") is True
    assert url_already_processed("never_seen") is False


def test_hydrate_seen_headlines_routes_through_repo(monkeypatch):
    """_hydrate_seen_headlines returns the fake's recent headlines."""
    from ingestion_engine import _hydrate_seen_headlines
    fake = InMemoryIntelligenceRepo()
    fake.upsert_insight({"url_hash": "a", "headline": "Alpha"})
    fake.upsert_insight({"url_hash": "b", "headline": "Beta"})
    monkeypatch.setattr("ingestion_engine._repo", lambda: fake)
    assert _hydrate_seen_headlines() == {"Alpha", "Beta"}


def test_store_insight_routes_through_repo(monkeypatch):
    """store_insight upserts via the repo and returns the fake's stored row."""
    from ingestion_engine import store_insight
    fake = InMemoryIntelligenceRepo()
    monkeypatch.setattr("ingestion_engine._repo", lambda: fake)
    store_insight({"url_hash": "abc", "headline": "Stored"})
    rows = fake.fetch_recent(hours=24)
    assert rows[0]["headline"] == "Stored"


def test_store_insight_raises_on_repo_write_failure(monkeypatch):
    """The repo's write methods raise; store_insight propagates."""
    from ingestion_engine import store_insight
    failing = MagicMock()
    failing.upsert_insight.side_effect = RuntimeError("write blew up")
    monkeypatch.setattr("ingestion_engine._repo", lambda: failing)
    with pytest.raises(RuntimeError, match="write blew up"):
        store_insight({"url_hash": "abc", "headline": "x"})


def test_generate_macro_summary_routes_through_repo(monkeypatch):
    """The summary upsert hits repo.upsert_summary, not Supabase directly."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    fake = InMemoryIntelligenceRepo()
    monkeypatch.setattr("ingestion_engine._repo", lambda: fake)

    # Inject a FakeLLM returning a valid macro summary.
    fake_llm = FakeLLM(returns={
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Some pressure body text."},
            {"label": "Supply chain watch", "body": "Some supply watch text."},
            {"label": "Commercial action", "body": "Some commercial text."},
        ],
    })

    with patch("ingestion_engine._llm", return_value=fake_llm):
        result = generate_macro_summary([
            {"category": "competitors", "headline": "x",
             "sentiment_score": 5, "americhem_impact": "y"}
        ])

    assert result is True
    from datetime import date
    stored = fake.get_delivery_state(run_date=date.today().isoformat(), run_mode="production")
    assert stored is not None
    assert stored["dominant_condition"] == "Mixed / Watch"


def test_generate_macro_summary_propagates_repo_write_failure(monkeypatch):
    """If repo.upsert_summary raises, the function raises."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    failing = MagicMock()
    failing.upsert_summary.side_effect = RuntimeError("DB down")
    monkeypatch.setattr("ingestion_engine._repo", lambda: failing)

    fake_llm = FakeLLM(returns={
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "x"},
            {"label": "Supply chain watch", "body": "y"},
            {"label": "Commercial action", "body": "z"},
        ],
    })

    with patch("ingestion_engine._llm", return_value=fake_llm):
        with pytest.raises(RuntimeError, match="DB down"):
            generate_macro_summary([
                {"category": "competitors", "headline": "x",
                 "sentiment_score": 5, "americhem_impact": "y"}
            ])


# ---------------------------------------------------------------------------
# Repository wiring — delivery paths route through _repo()
# ---------------------------------------------------------------------------

from datetime import datetime


def test_fetch_todays_intelligence_routes_through_repo(monkeypatch):
    """fetch_todays_intelligence returns repo.fetch_recent rows verbatim
    (alert_tier decoration is no longer this function's job)."""
    from delivery_engine import fetch_todays_intelligence
    fake = InMemoryIntelligenceRepo()
    fake.upsert_insight({
        "url_hash": "a", "headline": "Alpha",
        "americhem_impact_score": 8, "sentiment_score": 7,
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    rows = fetch_todays_intelligence()
    assert len(rows) == 1
    assert rows[0]["headline"] == "Alpha"
    assert "alert_tier" not in rows[0]   # decoration moved to caller


def test_fetch_todays_intelligence_uses_72h_on_monday(monkeypatch):
    """Monday detection still drives the lookback parameter."""
    import delivery_engine
    fake = MagicMock(spec=InMemoryIntelligenceRepo)
    fake.fetch_recent.return_value = []
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    # Force "today" to be a Monday for this test.
    fixed_monday = datetime(2026, 5, 25, 9, 0, 0)  # Monday
    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_monday
    monkeypatch.setattr(delivery_engine, "datetime", _FixedDateTime)

    delivery_engine.fetch_todays_intelligence()
    fake.fetch_recent.assert_called_once_with(hours=72)


def test_fetch_macro_summary_routes_through_repo(monkeypatch):
    """fetch_macro_summary returns repo.fetch_latest_summary verbatim."""
    from delivery_engine import fetch_macro_summary
    fake = InMemoryIntelligenceRepo()
    from datetime import date
    today = date.today().isoformat()
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "today's summary", "macro_sentiment": "x",
        "dominant_condition": "Mixed / Watch",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    got = fetch_macro_summary()
    assert got is not None
    assert got["executive_summary"] == "today's summary"


def test_fetch_macro_summary_returns_none_when_missing(monkeypatch):
    from delivery_engine import fetch_macro_summary
    fake = InMemoryIntelligenceRepo()
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    assert fetch_macro_summary() is None


def test_update_delivery_summary_counts_merges_with_prior(monkeypatch):
    """The same-day-retry merge: prior delivery counts are preserved through
    ingestion-owned codes; new delivery-owned codes overwrite."""
    from delivery_engine import _update_delivery_summary_counts
    from suppression_ledger import SuppressionLedger
    from datetime import date

    fake = InMemoryIntelligenceRepo()
    today = date.today().isoformat()
    # Seed a prior row mimicking ingestion having already written.
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": {"duplicate_url": 5, "below_impact_threshold": 9},
        "suppression_samples": [{"reason": "duplicate_url", "url": "u", "title": "t"}],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)

    new_ledger = (
        SuppressionLedger.for_delivery()
        .record_count("below_impact_threshold", 3)
        .record_count("product_listing", 1)
    )
    _update_delivery_summary_counts(surfaced_count=4, ledger=new_ledger)

    got = fake.get_delivery_state(run_date=today, run_mode="production")
    assert got["surfaced_count"] == 4
    # Ingestion-owned code preserved from prior.
    assert got["suppression_breakdown"]["duplicate_url"] == 5
    # Delivery-owned code overwritten by this run.
    assert got["suppression_breakdown"]["below_impact_threshold"] == 3
    assert got["suppression_breakdown"]["product_listing"] == 1


def test_update_delivery_summary_counts_swallows_write_failure(monkeypatch, caplog):
    """A failed metadata write must not block the email — preserves the
    existing 'Non-critical' operational decision."""
    from delivery_engine import _update_delivery_summary_counts
    from suppression_ledger import SuppressionLedger

    failing = MagicMock()
    failing.get_delivery_state.return_value = None
    failing.update_delivery_counts.side_effect = RuntimeError("DB down")
    monkeypatch.setattr("delivery_engine._repo", lambda: failing)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)

    # Should not raise.
    _update_delivery_summary_counts(
        surfaced_count=0,
        ledger=SuppressionLedger.for_delivery(),
    )
    assert "Failed to update delivery counts" in caplog.text


def test_update_delivery_summary_counts_aborts_write_on_prior_read_failure(monkeypatch, caplog):
    """If require_delivery_state raises, the caller must NOT call
    update_delivery_counts — otherwise the write would overwrite prior
    ingestion-owned suppression state with an empty ledger."""
    from delivery_engine import _update_delivery_summary_counts
    from suppression_ledger import SuppressionLedger

    class _ReadFailingRepo:
        def require_delivery_state(self, *, run_date, run_mode):
            raise RuntimeError("read failed")

        def update_delivery_counts(self, *args, **kwargs):
            raise AssertionError(
                "update_delivery_counts must NOT be called after prior-state "
                "read failure — otherwise prior suppression state is overwritten"
            )

    monkeypatch.setattr("delivery_engine._repo", lambda: _ReadFailingRepo())
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)

    # Must not raise.
    _update_delivery_summary_counts(
        surfaced_count=4,
        ledger=SuppressionLedger.for_delivery(),
    )
    assert "Failed to update delivery counts" in caplog.text


def test_update_delivery_summary_counts_writes_when_no_prior_row(monkeypatch):
    """When require_delivery_state returns None (no prior row), the write
    must still proceed — that's the fresh-row path, not a failure."""
    from delivery_engine import _update_delivery_summary_counts
    from suppression_ledger import SuppressionLedger
    from datetime import date

    fake = InMemoryIntelligenceRepo()
    today = date.today().isoformat()
    # Seed a row so update_delivery_counts has somewhere to write
    # (the in-memory fake's update is silent no-op without a row, mimicking
    # Supabase UPDATE-WHERE-no-match). For the fresh-row case in production,
    # daily_summaries already has an ingestion-written row before delivery
    # rendering — we mimic that here without seeding any suppression state.
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)

    ledger = SuppressionLedger.for_delivery().record_count("below_impact_threshold", 2)
    _update_delivery_summary_counts(surfaced_count=3, ledger=ledger)

    got = fake.get_delivery_state(run_date=today, run_mode="production")
    assert got["surfaced_count"] == 3
    assert got["suppression_breakdown"] == {"below_impact_threshold": 2}


def test_effective_impact_falls_back_when_americhem_impact_score_is_malformed():
    """Bad americhem_impact_score → fall back to sentiment_score."""
    from insight import effective_impact as _effective_impact
    from scoring import tier as _alert_tier
    row = {"americhem_impact_score": "bad", "sentiment_score": 8}
    assert _effective_impact(row) == 8
    assert _alert_tier(row) == "STRATEGIC"


def test_effective_impact_uses_default_when_both_scores_malformed():
    """Bad in both fields → default to 5 (routine), do not raise."""
    from insight import effective_impact as _effective_impact
    from scoring import tier as _alert_tier
    row = {"americhem_impact_score": "bad", "sentiment_score": "also bad"}
    assert _effective_impact(row) == 5
    assert _alert_tier(row) == "ROUTINE"


def test_effective_impact_uses_default_when_both_scores_missing():
    """Missing scores → default to 5 (unchanged behavior)."""
    from insight import effective_impact as _effective_impact
    from scoring import tier as _alert_tier
    row = {"headline": "test"}
    assert _effective_impact(row) == 5
    assert _alert_tier(row) == "ROUTINE"


# ---------------------------------------------------------------------------
# _validate_executive_bullets — citation_source_ids cleaning
# ---------------------------------------------------------------------------

def _raw_bullets(a_ids, b_ids, c_ids):
    return [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": a_ids},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": b_ids},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": c_ids},
    ]


def test_validate_bullets_keeps_only_in_pack_ids():
    out = _validate_executive_bullets(_raw_bullets([1, 99], [2], []), frozenset({1, 2}))
    assert out[0]["citation_source_ids"] == [1]   # 99 not in pack -> dropped
    assert out[1]["citation_source_ids"] == [2]
    assert out[2]["citation_source_ids"] == []


def test_validate_bullets_dedupes_preserving_order():
    out = _validate_executive_bullets(_raw_bullets([2, 1, 2, 1], [], []), frozenset({1, 2}))
    assert out[0]["citation_source_ids"] == [2, 1]


def test_validate_bullets_caps_citations_per_bullet():
    out = _validate_executive_bullets(_raw_bullets([1, 2, 3, 4], [], []), frozenset({1, 2, 3, 4}))
    assert out[0]["citation_source_ids"] == [1, 2, 3]   # MAX_EXECUTIVE_BULLET_CITATIONS


def test_validate_bullets_garbage_citations_become_empty():
    raw = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": "nope"},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": [None, "x", True, 1.5]},
        {"label": "Commercial action", "body": "C."},  # key missing entirely
    ]
    out = _validate_executive_bullets(raw, frozenset({1, 2}))
    assert out[0]["citation_source_ids"] == []
    assert out[1]["citation_source_ids"] == []   # bool True excluded, non-ints excluded
    assert out[2]["citation_source_ids"] == []


def test_validate_bullets_rejects_wrong_label_order():
    raw = [
        {"label": "Supply chain watch", "body": "A.", "citation_source_ids": []},
        {"label": "Market pressure", "body": "B.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    assert _validate_executive_bullets(raw, frozenset()) is None


from delivery_engine import (
    _safe_http_url,
    _citation_display_map,
    _render_executive_bullets,
    _render_sources_footer,
)


def _src(id, headline="H", url="https://x.com/a", domain="x.com"):
    return {"id": id, "headline": headline, "url": url, "domain": domain,
            "segment": "Auto", "score": 7}


def test_safe_http_url_allows_http_and_https():
    assert _safe_http_url("https://x.com/a") == "https://x.com/a"
    assert _safe_http_url("http://x.com/a") == "http://x.com/a"


def test_safe_http_url_rejects_other_schemes():
    assert _safe_http_url("javascript:alert(1)") == ""
    assert _safe_http_url("data:text/html,x") == ""
    assert _safe_http_url("") == ""
    assert _safe_http_url(None) == ""


def test_citation_display_map_renumbers_by_first_appearance():
    bullets = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": [5, 8]},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": [8, 2]},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    sources = [_src(5), _src(8), _src(2)]
    assert _citation_display_map(bullets, sources) == {5: 1, 8: 2, 2: 3}


def test_citation_display_map_ignores_ids_without_a_source():
    bullets = [{"label": "Market pressure", "body": "A.", "citation_source_ids": [5, 99]}]
    assert _citation_display_map(bullets, [_src(5)]) == {5: 1}


def test_render_bullets_inline_citation_is_grouped_and_linked():
    bullets = [
        {"label": "Market pressure", "body": "Pricing firm.", "citation_source_ids": [5, 8]},
        {"label": "Supply chain watch", "body": "Freight up.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "Watch.", "citation_source_ids": []},
    ]
    sources = [_src(5, url="https://a.com/x"), _src(8, url="https://b.com/y")]
    dmap = _citation_display_map(bullets, sources)
    html_out = _render_executive_bullets(bullets, sources, dmap)
    assert "Pricing firm." in html_out
    assert 'href="https://a.com/x"' in html_out
    assert 'title="https://a.com/x"' in html_out
    assert ">1</a>" in html_out and ">2</a>" in html_out
    # Grouped: a comma separates the two numbers, enclosed in brackets.
    assert "[" in html_out and ", " in html_out and "]" in html_out


def test_render_bullets_no_citation_when_empty():
    bullets = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": []},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    html_out = _render_executive_bullets(bullets, [], {})
    assert "<a" not in html_out


def test_render_bullets_escapes_malicious_url_and_headline():
    bullets = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": [1]},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    # javascript: scheme must be dropped -> number rendered as plain text, no href.
    sources = [_src(1, url="javascript:alert(1)")]
    dmap = _citation_display_map(bullets, sources)
    html_out = _render_executive_bullets(bullets, sources, dmap)
    assert "javascript:alert(1)" not in html_out
    assert "href=" not in html_out
    assert ">1<" in html_out or "[1]" in html_out  # number still shown, just unlinked


def test_render_sources_footer_orders_and_escapes():
    bullets = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": [8, 5]},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    sources = [
        _src(5, headline="Resin <b>up</b>", url="https://a.com/x", domain="a.com"),
        _src(8, headline="Freight", url="https://b.com/y", domain="b.com"),
    ]
    dmap = _citation_display_map(bullets, sources)
    footer = _render_sources_footer(sources, dmap)
    # Display order follows first appearance: 8 -> [1], 5 -> [2].
    assert footer.index("Freight") < footer.index("Resin")
    assert "b.com" in footer and "a.com" in footer
    assert "<b>up</b>" not in footer        # escaped
    assert "&lt;b&gt;up&lt;/b&gt;" in footer


def test_render_sources_footer_empty_when_no_citations():
    assert _render_sources_footer([], {}) == ""


def test_render_sources_footer_handles_missing_url_gracefully():
    bullets = [{"label": "Market pressure", "body": "A.", "citation_source_ids": [1]}]
    sources = [_src(1, headline="", url="", domain="")]
    dmap = _citation_display_map(bullets, sources)
    footer = _render_sources_footer(sources, dmap)
    assert footer != ""               # does not crash, still renders a row
    assert "href=" not in footer      # no valid URL -> unlinked


def test_render_bullets_escapes_html_metacharacters_in_body():
    bullets = [
        {"label": "Market pressure", "body": "Margins fell <5% as AT&T cut orders.",
         "citation_source_ids": []},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    html_out = _render_executive_bullets(bullets, [], {})
    assert "<5%" not in html_out
    assert "&lt;5%" in html_out
    assert "AT&amp;T" in html_out


def test_render_marker_mixes_linked_and_unlinked_by_url_safety():
    bullets = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": [1, 2]},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    sources = [
        _src(1, url="javascript:alert(1)"),   # unsafe -> plain text [1]
        _src(2, url="https://safe.com/y"),     # safe -> linked [2]
    ]
    dmap = _citation_display_map(bullets, sources)
    html_out = _render_executive_bullets(bullets, sources, dmap)
    assert 'href="https://safe.com/y"' in html_out   # id 2 linked
    assert ">2</a>" in html_out
    assert "javascript:alert(1)" not in html_out      # id 1 not linked
    # id 1's display number 1 appears as plain text inside the marker, not as a link
    assert ">1</a>" not in html_out


from delivery_engine import _render_exec_summary


def test_exec_summary_renders_inline_citations_and_footer():
    macro = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Pricing firm.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Freight up.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Watch.", "citation_source_ids": []},
        ],
        "executive_sources": [
            {"id": 1, "headline": "Resin prices climb", "url": "https://reuters.com/x",
             "domain": "reuters.com", "segment": "Auto", "score": 8},
        ],
    }
    html_out = _render_exec_summary(macro)
    # Inline citation marker + link stay in the executive summary block...
    assert "Pricing firm." in html_out
    assert 'href="https://reuters.com/x"' in html_out
    assert ">1</a>" in html_out
    # ...but the Sources list itself now lives in its own bottom-of-email section,
    # NOT inside the executive summary block.
    assert "Sources" not in html_out
    assert "Resin prices climb" not in html_out


def test_exec_summary_legacy_row_renders_without_footer():
    # Old row: bullets without citation_source_ids, no executive_sources.
    macro = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action", "body": "C."},
        ],
    }
    html_out = _render_exec_summary(macro)
    assert "A." in html_out
    assert "Sources" not in html_out
    assert "<a" not in html_out


def test_exec_summary_prose_fallback_unchanged():
    macro = {"executive_summary": "Prose summary.", "dominant_condition": "Low Signal"}
    html_out = _render_exec_summary(macro)
    assert "Prose summary." in html_out
    assert "Sources" not in html_out


def test_exec_summary_legacy_string_bullets_fall_back_to_prose():
    # Legacy/malformed row: executive_bullets is a truthy list of strings (not
    # dicts) AND prose is present. The structured citation path would render
    # blank "• :" rows, so we must fall through to the legacy prose instead.
    macro = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": ["Market pressure: pricing firm.", "Freight up.", "Watch."],
        "executive_summary": "Prose summary stands in.",
    }
    html_out = _render_exec_summary(macro)
    assert "Prose summary stands in." in html_out
    assert "Sources" not in html_out
    assert "<a" not in html_out


from delivery_engine import _render_sources_section


def _macro_with_citations():
    return {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Pricing firm.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Freight up.", "citation_source_ids": [2]},
            {"label": "Commercial action", "body": "Watch.", "citation_source_ids": []},
        ],
        "executive_sources": [
            {"id": 1, "headline": "Resin prices climb", "url": "https://reuters.com/x",
             "domain": "reuters.com", "segment": "Auto", "score": 8},
            {"id": 2, "headline": "Freight rates spike", "url": "https://icis.com/y",
             "domain": "icis.com", "segment": "Auto", "score": 7},
        ],
    }


def test_render_sources_section_renders_footer_when_cited():
    html_out = _render_sources_section(_macro_with_citations())
    assert "Sources" in html_out
    assert "Resin prices climb" in html_out
    assert "reuters.com" in html_out
    assert 'href="https://reuters.com/x"' in html_out
    # Wrapped as a full-width email row so it sits in the outer email table.
    assert "<tr>" in html_out and "<td" in html_out


def test_render_sources_section_empty_for_legacy_and_uncited():
    # No structured bullets / no executive_sources -> no bottom Sources section.
    assert _render_sources_section(None) == ""
    assert _render_sources_section({"executive_summary": "Prose."}) == ""
    assert _render_sources_section({
        "executive_bullets": ["string bullet"],
        "executive_summary": "Prose.",
    }) == ""
    assert _render_sources_section({
        "executive_bullets": [
            {"label": "Market pressure", "body": "A.", "citation_source_ids": []},
            {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
        ],
        "executive_sources": [
            {"id": 1, "headline": "Unused", "url": "https://x.com/a", "domain": "x.com"},
        ],
    }) == ""


def test_sources_section_numbering_matches_inline_markers():
    macro = _macro_with_citations()
    exec_html = _render_exec_summary(macro)
    sources_html = _render_sources_section(macro)
    # Inline markers show [1] and [2]; the footer lists [1] and [2] for the same
    # sources (shared deterministic display map).
    assert ">1</a>" in exec_html and ">2</a>" in exec_html
    assert "[1]" in sources_html and "[2]" in sources_html
    assert sources_html.index("Resin prices climb") < sources_html.index("Freight rates spike")


def test_report_places_sources_at_bottom():
    macro = _macro_with_citations()
    data = [{
        "headline": "Packaging market update",  # distinct from the source headline
        "source_url": "https://example.com/card",
        "americhem_impact": "Pricing pressure on packaging.",
        "americhem_impact_score": 8,
        "sentiment_tag": "Negative",
        "commercial_segment": "Packaging",
        "signal_type": "Pricing",
    }]
    html = render_report(assemble_report(data, macro), today_str=_TODAY_STR)

    # The cited-source headline now appears only in the bottom Sources section
    # (it was removed from the executive summary block), exactly once.
    assert html.count("Resin prices climb") == 1
    assert "Sources" in html
    # Sources block sits AFTER the executive summary block (moved to the bottom).
    assert html.index("Executive Summary") < html.index("Resin prices climb")
    assert html.index("Pricing firm.") < html.index("Resin prices climb")


def test_exec_summary_sources_present_but_none_cited_renders_no_footer():
    # executive_sources is non-empty, but no bullet cites any id -> empty display
    # map -> no inline markers and no orphan Sources footer.
    macro = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "A.", "citation_source_ids": []},
            {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
        ],
        "executive_sources": [
            {"id": 1, "headline": "Unused", "url": "https://x.com/a",
             "domain": "x.com", "segment": "Auto", "score": 7},
        ],
    }
    html_out = _render_exec_summary(macro)
    assert "A." in html_out
    assert "Sources" not in html_out
    assert "<a" not in html_out
    assert "Unused" not in html_out   # uncited source never leaks into output


# ---------------------------------------------------------------------------
# 18. discover_urls — client-side truncation to results_per_entity
# ---------------------------------------------------------------------------

def test_discover_urls_truncates_to_results_per_entity(monkeypatch):
    """Serper's news endpoint returns pages of 10 regardless of the `num`
    param — the client must enforce results_per_entity itself."""
    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"news": [
                {"link": f"https://example.com/article-{i}", "title": f"Headline {i}"}
                for i in range(10)
            ]}

    monkeypatch.setenv("SERPER_API_KEY", "test_key")
    monkeypatch.setattr("ingestion_engine.requests.post", lambda *a, **k: FakeResponse())

    results = discover_urls("test query", 24, 2)

    assert len(results) == 2
    assert results[0] == ("https://example.com/article-0", "Headline 0")
    assert results[1] == ("https://example.com/article-1", "Headline 1")


# ---------------------------------------------------------------------------
# targets.yaml configuration contract (reads the real control file)
# ---------------------------------------------------------------------------

from pathlib import Path as _Path

import yaml as _yaml

_TARGETS_PATH = _Path(__file__).resolve().parents[1] / "targets.yaml"

_NEW_CONCEPT_GROUPS = [
    "masterbatch_additives_innovation",
    "europe_polymer_signals",
    "asia_pacific_polymer_signals",
]

# Baseline recorded before the search-coverage expansion: 107 active entity
# targets across competitors/customers/suppliers, 10 active concept groups.
_BASELINE_ACTIVE_ENTITY_TARGETS = 107
_BASELINE_ACTIVE_CONCEPT_TARGETS = 10


def _load_real_config() -> dict:
    with open(_TARGETS_PATH, "r") as fh:
        return _yaml.safe_load(fh)


def _load_real_targets() -> list[dict]:
    return load_targets(str(_TARGETS_PATH))


def _concept_targets(targets: list[dict]) -> list[dict]:
    return [t for t in targets if "zoominfo_news" not in t]


def test_targets_yaml_parses_and_discovery_settings_locked():
    """Discovery tuning must not drift as part of coverage changes."""
    config = _load_real_config()
    assert config["discovery"]["results_per_entity"] == 2
    assert config["discovery"]["lookback_hours"] == 24
    assert config["discovery"]["min_article_length"] == 500


def test_targets_yaml_new_concept_groups_exist_and_are_active():
    config = _load_real_config()
    for group in _NEW_CONCEPT_GROUPS:
        assert group in config, f"missing concept group: {group}"
        assert config[group]["search_mode"] == "concept"
        assert config[group]["active"] is True


def test_targets_yaml_entity_groups_precede_new_concept_groups():
    """All entity-mode groups must appear before the three new concept groups."""
    config = _load_real_config()
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
    config = _load_real_config()
    assert config["fibers"]["include_all"] == []


def test_targets_yaml_innovation_query_contents():
    targets = {t["name"]: t for t in _load_real_targets()}
    query = targets["masterbatch_additives_innovation"]["query"]
    assert '"new functional additive"' in query
    assert '"new additive masterbatch"' in query
    assert '"new color masterbatch"' in query
    assert '-"market report"' in query


def test_targets_yaml_regional_queries_have_geographic_anchors():
    targets = {t["name"]: t for t in _load_real_targets()}
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
    concepts = _concept_targets(_load_real_targets())
    assert len(concepts) == _BASELINE_ACTIVE_CONCEPT_TARGETS + 3 - 1 + len(_MACRO_GROUP_KEYS) + 2


def test_targets_yaml_entity_targets_unchanged():
    """Coverage expansion must not touch entity monitoring."""
    targets = _load_real_targets()
    entities = [t for t in targets if "zoominfo_news" in t]
    assert len(entities) == _BASELINE_ACTIVE_ENTITY_TARGETS


def test_targets_yaml_new_concept_groups_carry_no_zoominfo_ids():
    config = _load_real_config()
    targets = {t["name"]: t for t in _load_real_targets()}
    for group in _NEW_CONCEPT_GROUPS:
        assert "zoominfo_company_id" not in config[group]
        assert "zoominfo_company_id" not in targets[group]


# ---------------------------------------------------------------------------
# 19. Pre-scrape unscrapable-domain filter
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("url,expected", [
    ("https://www.linkedin.com/posts/somebody-123", True),
    ("https://linkedin.com/pulse/x", True),
    ("https://uk.linkedin.com/jobs/view/1", True),     # country subdomain
    ("https://www.amazon.com/dp/B0ABC123", True),
    ("https://www.homedepot.com/p/product/12345", True),
    ("https://www.reuters.com/markets/some-article/", False),
    ("https://notlinkedin.com/article", False),        # suffix must be dot-anchored
    ("not a url", False),                              # malformed → let the scraper decide
    ("https://corporate.walmart.com/news/2026/earnings", False),   # retail newsroom subdomain
    ("https://corporate.homedepot.com/newsroom/some-story", False),
    ("https://m.facebook.com/story.php?id=1", True),               # social subdomains still suffix-matched
])
def test_is_unscrapable_domain(url, expected):
    assert _is_unscrapable_domain(url) is expected


def test_execute_pipeline_skips_unscrapable_domain_before_scraping(monkeypatch):
    """An unscrapable-domain candidate must be suppressed pre-scrape: no
    Firecrawl attempt, and the ledger records unscrapable_domain."""
    import ingestion_engine as ie

    target = {
        "name": "Acme", "category": "competitor", "query": '"Acme"',
        "lookback_hours": 24, "results_per_entity": 2, "min_article_length": 500,
    }
    candidate = {
        "url": "https://www.linkedin.com/posts/acme-update",
        "title": "Acme update", "provider": "serper",
    }
    summary_kwargs = {}

    monkeypatch.setattr(ie, "load_targets", lambda path: [target])
    monkeypatch.setattr(ie, "discover_candidates", lambda t: [candidate])
    monkeypatch.setattr(ie, "_hydrate_seen_headlines", lambda: set())
    monkeypatch.setattr(ie, "url_already_processed", lambda h: False)
    monkeypatch.setattr(
        ie, "scrape_article",
        lambda *a, **k: pytest.fail("scrape_article must not be called for an unscrapable domain"),
    )
    monkeypatch.setattr(
        ie, "generate_macro_summary",
        lambda buffer, screened_count, **kwargs: summary_kwargs.update(kwargs),
    )

    execute_pipeline()

    assert summary_kwargs["suppression_breakdown"] == {"unscrapable_domain": 1}
    assert summary_kwargs["suppression_samples"] == [{
        "reason": "unscrapable_domain",
        "url": "https://www.linkedin.com/posts/acme-update",
        "title": "Acme update",
    }]


def test_render_qa_debug_section_includes_unscrapable_domain():
    """The unscrapable_domain code must get a labeled breakdown row in the QA
    debug section (not just fold into the suppressed total)."""
    from delivery_engine import _render_qa_debug_section
    macro = {
        "screened_count": 40,
        "surfaced_count": 5,
        "suppression_breakdown": {"unscrapable_domain": 4},
        "suppression_samples": [],
    }
    html = _render_qa_debug_section(macro)
    assert "unscrapable domain" in html
    assert ">4</td>" in html


# ---------------------------------------------------------------------------
# 20. scrape_article — wall-clock ceiling actually bounds wall-clock
# ---------------------------------------------------------------------------

def test_scrape_article_returns_promptly_after_wall_clock_timeout(monkeypatch):
    """After the wall-clock timeout fires, scrape_article must return without
    waiting for the hung request thread (the old `with ThreadPoolExecutor`
    pattern blocked in shutdown(wait=True) until the thread finished)."""
    import time as _time
    import threading
    import ingestion_engine as ie

    def hanging_post(*args, **kwargs):
        # Use threading.Event().wait instead of time.sleep in case tests
        # globally monkeypatch time.sleep to a no-op.
        threading.Event().wait(2.0)
        raise AssertionError("hung request should have been abandoned")

    monkeypatch.setenv("FIRECRAWL_API_KEY", "test_key")
    monkeypatch.setattr(ie, "FIRECRAWL_WALL_CLOCK_TIMEOUT", 0.2)
    monkeypatch.setattr("ingestion_engine.requests.post", hanging_post)

    start = _time.monotonic()
    result = ie.scrape_article("https://example.com/article", min_length=500)
    elapsed = _time.monotonic() - start

    assert result is None
    assert elapsed < 1.0  # must not wait out the 2s hung thread
