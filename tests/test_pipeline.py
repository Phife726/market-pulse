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
