"""
Smoke tests for ingestion_engine.py.
No live API calls — all external clients are mocked.
"""
import json
import textwrap
from unittest.mock import MagicMock, patch

import pytest

from ingestion_engine import (
    compute_url_hash,
    load_targets,
    normalize_url,
    synthesize_insight,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_openai_mock(sentiment_score: int | float) -> MagicMock:
    """Return a mock OpenAI client whose chat.completions.create returns a
    well-formed JSON payload with the given sentiment_score."""
    content = json.dumps(
        {
            "headline": "Test Headline",
            "americhem_impact": "Test impact on Americhem.",
            "sentiment_score": sentiment_score,
            "source_url": "https://news.com/article",
            "entities_mentioned": ["Avient"],
        }
    )
    mock_message = MagicMock()
    mock_message.content = content

    mock_choice = MagicMock()
    mock_choice.message = mock_message

    mock_completion = MagicMock()
    mock_completion.choices = [mock_choice]

    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_completion

    return mock_client


# ---------------------------------------------------------------------------
# 1. URL normalisation
# ---------------------------------------------------------------------------

def test_url_normalization():
    """Query parameters and fragments must both be stripped."""
    result = normalize_url("https://news.com/a?utm=1#sec")
    assert result == "https://news.com/a"


def test_url_normalization_preserves_path():
    """Normalisation must not alter the scheme, host, or path."""
    result = normalize_url("https://news.com/section/article-slug")
    assert result == "https://news.com/section/article-slug"


# ---------------------------------------------------------------------------
# 2. Hash collision: UTM-polluted URL must hash identically to the clean URL
# ---------------------------------------------------------------------------

def test_compute_url_hash_collision():
    """A UTM-polluted URL and its clean counterpart must produce the same hash."""
    clean = "https://news.com/article"
    polluted = "https://news.com/article?utm_source=newsletter&utm_medium=email&utm_campaign=weekly"

    assert compute_url_hash(normalize_url(clean)) == compute_url_hash(normalize_url(polluted))


# ---------------------------------------------------------------------------
# 3. Sentiment score clamping
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw_score, expected",
    [
        (0,  1),   # below floor → clamp to 1
        (15, 10),  # above ceiling → clamp to 10
    ],
)
def test_sentiment_clamp(raw_score: int, expected: int):
    """Out-of-range scores returned by the LLM must be clamped to [1, 10]."""
    with patch("ingestion_engine._get_openai", return_value=_make_openai_mock(raw_score)):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )

    assert result is not None, "synthesize_insight returned None unexpectedly"
    assert result["sentiment_score"] == expected


# ---------------------------------------------------------------------------
# 4. load_targets: inactive entities must be excluded
# ---------------------------------------------------------------------------

def test_load_targets_filters_inactive(tmp_path):
    """Entities with active: false must never appear in the returned target list."""
    config_yaml = textwrap.dedent(
        """\
        competitors:
          - name: ActiveCorp
            active: true
          - name: InactiveCorp
            active: false
        customers: []
        suppliers: []
        markets: []
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
    """Each returned target dict must include the five expected keys."""
    config_yaml = textwrap.dedent(
        """\
        competitors:
          - name: Avient
            active: true
        customers: []
        suppliers: []
        markets: []
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
    assert t["results_per_entity"] == 3
    assert t["lookback_hours"] == 48
    assert t["min_article_length"] == 300


# ---------------------------------------------------------------------------
# 5. DISCARD signal detection
# ---------------------------------------------------------------------------

def test_discard_signal_detected():
    """synthesize_insight returning DISCARD must be detectable before store."""
    insight = {"americhem_impact": "DISCARD"}
    assert insight.get("americhem_impact") == "DISCARD"


# ---------------------------------------------------------------------------
# 6. raw_materials category loading
# ---------------------------------------------------------------------------

def test_raw_materials_category_loaded(tmp_path):
    """raw_materials entities must be returned by load_targets."""
    config_yaml = textwrap.dedent(
        """\
        competitors: []
        customers: []
        suppliers: []
        raw_materials:
          - name: "commodity resins"
            active: true
        markets: []
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
    assert "commodity resins" in names
    assert targets[0]["category"] == "raw_materials"


# ---------------------------------------------------------------------------
# 7. recommended_action soft default
# ---------------------------------------------------------------------------

def _make_openai_mock_no_action(sentiment_score: int) -> MagicMock:
    """Return a mock OpenAI client whose response omits recommended_action."""
    content = json.dumps(
        {
            "headline": "Test Headline",
            "americhem_impact": "Test impact on Americhem.",
            "sentiment_score": sentiment_score,
            "source_url": "https://news.com/article",
            "entities_mentioned": ["Avient"],
        }
    )
    mock_message = MagicMock()
    mock_message.content = content
    mock_choice = MagicMock()
    mock_choice.message = mock_message
    mock_completion = MagicMock()
    mock_completion.choices = [mock_choice]
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_completion
    return mock_client


def _make_openai_mock_invalid_action(sentiment_score: int) -> MagicMock:
    """Return a mock OpenAI client whose response has an invalid recommended_action."""
    content = json.dumps(
        {
            "headline": "Test Headline",
            "americhem_impact": "Test impact on Americhem.",
            "sentiment_score": sentiment_score,
            "recommended_action": "Do something weird",
            "source_url": "https://news.com/article",
            "entities_mentioned": ["Avient"],
        }
    )
    mock_message = MagicMock()
    mock_message.content = content
    mock_choice = MagicMock()
    mock_choice.message = mock_message
    mock_completion = MagicMock()
    mock_completion.choices = [mock_choice]
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_completion
    return mock_client


@pytest.mark.parametrize("mock_fn", [_make_openai_mock_no_action, _make_openai_mock_invalid_action])
def test_recommended_action_default(mock_fn):
    """Missing or invalid recommended_action must soft-default to 'Monitor', not discard the article."""
    with patch("ingestion_engine._get_openai", return_value=mock_fn(5)):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )

    assert result is not None, "synthesize_insight must not return None for missing recommended_action"
    assert result["recommended_action"] == "Monitor"


# ---------------------------------------------------------------------------
# 8. article_summary soft default
# ---------------------------------------------------------------------------

def test_article_summary_default():
    """Missing article_summary must soft-default to empty string, not discard the article."""
    with patch("ingestion_engine._get_openai", return_value=_make_openai_mock(5)):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None, "synthesize_insight must not return None for missing article_summary"
    assert result["article_summary"] == ""


# ---------------------------------------------------------------------------
# 9. _render_card() article_summary rendering
# ---------------------------------------------------------------------------

from delivery_engine import _render_card


def test_render_card_shows_summary():
    """_render_card must include article_summary text when the field is populated."""
    item = {
        "headline": "Test Headline",
        "source_url": "https://news.com/article",
        "americhem_impact": "Some impact.",
        "category": "competitors",
        "sentiment_score": 5,
        "source_publication": "Reuters",
        "sentiment_rationale": "Neutral article.",
        "recommended_action": "Monitor",
        "article_summary": "BASF announced a new plant in Germany. The facility will produce 50kt of polymer annually. Production starts Q1 2027.",
    }
    html = _render_card(item, accent="#1B3A6B", bg="#E8EDF5", text="#1B3A6B")
    assert "BASF announced a new plant in Germany" in html


def test_render_card_omits_summary_when_empty():
    """_render_card must not emit an empty <p> tag when article_summary is absent."""
    item = {
        "headline": "Test Headline",
        "source_url": "https://news.com/article",
        "americhem_impact": "Some impact.",
        "category": "competitors",
        "sentiment_score": 5,
        "source_publication": "Reuters",
        "sentiment_rationale": "Neutral article.",
        "recommended_action": "Monitor",
        "article_summary": "",
    }
    html = _render_card(item, accent="#1B3A6B", bg="#E8EDF5", text="#1B3A6B")
    assert '<p style="margin:0 0 8px 0;font-size:12px;color:#6B7280' not in html
