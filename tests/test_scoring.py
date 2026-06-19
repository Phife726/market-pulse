"""Tests for the relevance scoring thresholds (scoring.py).

The visible / weak-relevance bands, the alert tiers, and the legacy-critical
badge rule are pinned here, at their canonical home, instead of being asserted
only through a rendered email.
"""

import scoring
from scoring import Scoring


# --- Scoring.from_config ----------------------------------------------------

def test_from_config_uses_defaults_when_absent():
    s = Scoring.from_config({})
    assert (s.visible, s.supporting) == (6, 4)


def test_from_config_reads_reporting_overrides():
    s = Scoring.from_config({"reporting": {"visible_impact_threshold": 7, "supporting_impact_threshold": 5}})
    assert (s.visible, s.supporting) == (7, 5)


def test_from_config_coerces_string_threshold():
    s = Scoring.from_config({"reporting": {"visible_impact_threshold": "8"}})
    assert s.visible == 8


def test_from_config_defaults_on_bad_threshold(caplog):
    s = Scoring.from_config({"reporting": {"visible_impact_threshold": "high"}})
    assert s.visible == 6


# --- is_visible -------------------------------------------------------------

def test_is_visible_at_threshold():
    assert Scoring(visible=6, supporting=4).is_visible({"americhem_impact_score": 6}) is True


def test_is_visible_below_threshold():
    assert Scoring(visible=6, supporting=4).is_visible({"americhem_impact_score": 5}) is False


def test_is_visible_uses_sentiment_fallback():
    assert Scoring(visible=6, supporting=4).is_visible({"sentiment_score": 8}) is True


# --- is_weak_relevance ------------------------------------------------------

def test_weak_relevance_inside_band():
    s = Scoring(visible=6, supporting=4)
    assert s.is_weak_relevance({"americhem_impact_score": 4}) is True
    assert s.is_weak_relevance({"americhem_impact_score": 5}) is True


def test_weak_relevance_excludes_visible_and_below_supporting():
    s = Scoring(visible=6, supporting=4)
    assert s.is_weak_relevance({"americhem_impact_score": 6}) is False  # visible
    assert s.is_weak_relevance({"americhem_impact_score": 3}) is False  # below supporting


# --- tier -------------------------------------------------------------------

def test_tier_critical():
    assert scoring.tier({"americhem_impact_score": 3}) == "CRITICAL"


def test_tier_strategic():
    assert scoring.tier({"americhem_impact_score": 8}) == "STRATEGIC"


def test_tier_routine():
    assert scoring.tier({"americhem_impact_score": 5}) == "ROUTINE"


# --- is_legacy_critical -----------------------------------------------------

def test_legacy_critical_true_for_low_sentiment_legacy_row():
    assert scoring.is_legacy_critical({"sentiment_score": 2}) is True


def test_legacy_critical_false_when_materiality_score_present():
    # A modern row carries americhem_impact_score; the legacy badge never applies.
    assert scoring.is_legacy_critical({"americhem_impact_score": 9, "sentiment_score": 2}) is False


def test_legacy_critical_false_for_higher_sentiment():
    assert scoring.is_legacy_critical({"sentiment_score": 4}) is False


def test_legacy_critical_false_when_no_sentiment():
    assert scoring.is_legacy_critical({}) is False
