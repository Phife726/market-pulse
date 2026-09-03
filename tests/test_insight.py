"""Tests for the Insight schema (insight.py).

The clamp/default/validation rules and the field readers are tested here, at
their canonical home, instead of only through ``synthesize_insight``. The
ingestion test still proves the wiring (LLM seam → normalize); these prove the
rules themselves.
"""

from datetime import datetime, timezone

import pytest

import insight


def _raw(**overrides) -> dict:
    base = {
        "headline": "H",
        "americhem_impact": "Impact.",
        "sentiment_score": 5,
        "source_url": "https://news.com/a",
        "entities_mentioned": ["Avient"],
    }
    base.update(overrides)
    return base


# --- is_discard -------------------------------------------------------------

def test_is_discard_true_for_sentinel():
    assert insight.is_discard({"americhem_impact": "DISCARD"}) is True


def test_is_discard_false_for_normal_row():
    assert insight.is_discard(_raw()) is False


# --- normalize: required keys ----------------------------------------------

def test_normalize_returns_none_when_required_key_missing():
    raw = _raw()
    del raw["headline"]
    assert insight.normalize(raw) is None


def test_normalize_returns_dict_for_minimal_valid_input():
    assert insight.normalize(_raw()) is not None


# --- normalize: score clamping ---------------------------------------------

def test_normalize_clamps_sentiment_score_high():
    assert insight.normalize(_raw(sentiment_score=99))["sentiment_score"] == 10


def test_normalize_clamps_sentiment_score_low():
    assert insight.normalize(_raw(sentiment_score=0))["sentiment_score"] == 1


def test_normalize_defaults_bad_sentiment_score():
    assert insight.normalize(_raw(sentiment_score="x"))["sentiment_score"] == 5


def test_normalize_clamps_impact_score():
    assert insight.normalize(_raw(americhem_impact_score=42))["americhem_impact_score"] == 10


def test_normalize_defaults_missing_impact_score():
    assert insight.normalize(_raw())["americhem_impact_score"] == 5


# --- normalize: taxonomy validation ----------------------------------------

def test_normalize_defaults_invalid_sentiment_tag():
    assert insight.normalize(_raw(sentiment_tag="Bullish"))["sentiment_tag"] == "Neutral"


def test_normalize_preserves_valid_sentiment_tag():
    assert insight.normalize(_raw(sentiment_tag="Negative"))["sentiment_tag"] == "Negative"


def test_normalize_defaults_invalid_commercial_segment():
    assert insight.normalize(_raw(commercial_segment="Widgets"))["commercial_segment"] == "Enterprise / Cross-Segment"


def test_normalize_preserves_and_strips_valid_segment():
    assert insight.normalize(_raw(commercial_segment="  Healthcare "))["commercial_segment"] == "Healthcare"


def test_normalize_preserves_building_construction_segment():
    assert insight.normalize(_raw(commercial_segment="Building & Construction"))["commercial_segment"] == "Building & Construction"


def test_normalize_defaults_invalid_signal_type():
    assert insight.normalize(_raw(signal_type="Vibes"))["signal_type"] == "Other"


def test_normalize_preserves_valid_signal_type():
    assert insight.normalize(_raw(signal_type="Supply Chain"))["signal_type"] == "Supply Chain"


def test_normalize_defaults_invalid_action():
    assert insight.normalize(_raw(recommended_action="Panic"))["recommended_action"] == "Monitor"


def test_normalize_preserves_valid_action():
    assert insight.normalize(_raw(recommended_action="Escalate to leadership"))["recommended_action"] == "Escalate to leadership"


# --- normalize: structural defaults ----------------------------------------

def test_normalize_drops_legacy_strategic_segment():
    assert "strategic_segment" not in insight.normalize(_raw(strategic_segment="Raw Materials"))


def test_normalize_coerces_non_list_entities():
    assert insight.normalize(_raw(entities_mentioned="Avient"))["entities_mentioned"] == []


def test_normalize_sets_default_string_fields():
    result = insight.normalize(_raw())
    for field in ("impact_rationale", "source_publication", "sentiment_rationale", "article_summary"):
        assert result[field] == ""


# --- readers ----------------------------------------------------------------

def test_effective_impact_prefers_impact_score():
    assert insight.effective_impact({"americhem_impact_score": 8, "sentiment_score": 2}) == 8


def test_effective_impact_falls_back_to_sentiment_score():
    assert insight.effective_impact({"americhem_impact_score": None, "sentiment_score": 6}) == 6


def test_effective_impact_falls_back_on_malformed_impact_score():
    assert insight.effective_impact({"americhem_impact_score": "bad", "sentiment_score": 7}) == 7


def test_effective_impact_defaults_when_both_missing_or_malformed():
    assert insight.effective_impact({}) == 5
    assert insight.effective_impact({"americhem_impact_score": "bad", "sentiment_score": "bad"}) == 5


def test_commercial_segment_defaults_when_blank_or_missing():
    for row in ({}, {"commercial_segment": None}, {"commercial_segment": ""}, {"commercial_segment": "   "}):
        assert insight.commercial_segment(row) == "Enterprise / Cross-Segment"


def test_commercial_segment_strips_whitespace():
    assert insight.commercial_segment({"commercial_segment": " Packaging "}) == "Packaging"


def test_commercial_segment_ignores_legacy_strategic_segment():
    """The pre-relevance-upgrade field is not a fallback: only commercial_segment counts."""
    assert insight.commercial_segment({"strategic_segment": "Healthcare"}) == "Enterprise / Cross-Segment"


def test_commercial_segment_returns_value():
    assert insight.commercial_segment({"commercial_segment": "Packaging"}) == "Packaging"


def test_signal_type_defaults_when_missing():
    for row in ({}, {"signal_type": None}, {"signal_type": ""}):
        assert insight.signal_type(row) == "Other"


def test_signal_type_returns_value():
    assert insight.signal_type({"signal_type": "Regulatory"}) == "Regulatory"


# ===========================================================================
# Shared field-value readers (source_domain / parse_timestamp)
# ===========================================================================


def test_source_domain_strips_www_port_and_case():
    """The registrable host a citation or an appendix row displays."""
    assert insight.source_domain("https://www.example.com/a?utm=1") == "example.com"
    assert insight.source_domain("https://news.example.co.uk:8080/x") == "news.example.co.uk"
    assert insight.source_domain("HTTPS://WWW.Example.COM/a") == "example.com"


def test_source_domain_empty_when_absent_or_unparseable():
    """Total: the callers render the result into an email and a source pack,
    off rows whose jsonb-derived fields may hold anything."""
    assert insight.source_domain("") == ""
    assert insight.source_domain(None) == ""
    assert insight.source_domain("not a url") == ""
    assert insight.source_domain(12345) == ""


def test_parse_timestamp_accepts_iso_with_or_without_z():
    """A trailing Z and surrounding whitespace both parse."""
    expected = datetime(2026, 8, 28, 10, 0, tzinfo=timezone.utc)
    assert insight.parse_timestamp("2026-08-28T10:00:00+00:00") == expected
    assert insight.parse_timestamp("2026-08-28T10:00:00Z") == expected
    assert insight.parse_timestamp("  2026-08-28T10:00:00Z  ") == expected


@pytest.mark.parametrize("raw, expected", [
    # Postgres strips trailing zeros from the microseconds, and fromisoformat
    # before 3.11 accepts only 3 or 6 fractional digits — the shape CI (3.10)
    # caught silently parsing to None in the repo layer. The delivery window
    # and the appendix read the same columns, so they need the same tolerance.
    ("2026-08-26T10:44:12.5+00:00",      datetime(2026, 8, 26, 10, 44, 12, 500000, tzinfo=timezone.utc)),
    ("2026-08-26T10:44:12.12+00:00",     datetime(2026, 8, 26, 10, 44, 12, 120000, tzinfo=timezone.utc)),
    ("2026-08-26T10:44:12.123+00:00",    datetime(2026, 8, 26, 10, 44, 12, 123000, tzinfo=timezone.utc)),
    ("2026-08-26T10:44:12.123456+00:00", datetime(2026, 8, 26, 10, 44, 12, 123456, tzinfo=timezone.utc)),
    ("2026-08-26T10:44:12.5Z",           datetime(2026, 8, 26, 10, 44, 12, 500000, tzinfo=timezone.utc)),
    ("2026-08-26T10:44:12.5",            datetime(2026, 8, 26, 10, 44, 12, 500000)),
])
def test_parse_timestamp_accepts_every_postgres_fraction_width(raw, expected):
    """One tolerance set for every reader of a stored timestamp."""
    assert insight.parse_timestamp(raw) == expected


def test_parse_timestamp_none_when_absent_or_unparseable():
    """A scraped non-date, a blank, or a non-string yields None."""
    assert insight.parse_timestamp("Yesterday") is None
    assert insight.parse_timestamp("") is None
    assert insight.parse_timestamp("   ") is None
    assert insight.parse_timestamp(None) is None
    assert insight.parse_timestamp(20260828) is None
