"""Tests for the Insight schema (insight.py).

The clamp/default/validation rules and the field readers are tested here, at
their canonical home, instead of only through ``synthesize_insight``. The
ingestion test still proves the wiring (LLM seam → normalize); these prove the
rules themselves.
"""

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


def test_effective_impact_defaults_when_both_missing():
    assert insight.effective_impact({}) == 5


def test_commercial_segment_defaults_when_blank():
    assert insight.commercial_segment({"commercial_segment": "   "}) == "Enterprise / Cross-Segment"


def test_commercial_segment_returns_value():
    assert insight.commercial_segment({"commercial_segment": "Packaging"}) == "Packaging"


def test_signal_type_defaults_when_missing():
    assert insight.signal_type({}) == "Other"


def test_signal_type_returns_value():
    assert insight.signal_type({"signal_type": "Regulatory"}) == "Regulatory"


# ===========================================================================
# Moved from the pipeline monolith; consolidated with the rules above in a follow-up
# ===========================================================================


def test_discard_signal_detected():
    insight = {"americhem_impact": "DISCARD"}
    assert insight.get("americhem_impact") == "DISCARD"


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
