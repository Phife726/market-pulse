# tests/test_suppression_ledger.py
from suppression_ledger import (
    SAMPLES_CAP,
    INGESTION_CODES,
    DELIVERY_CODES,
    side_of,
    label_for,
)


def test_taxonomy_partitions():
    assert "duplicate_url" in INGESTION_CODES
    assert "below_impact_threshold" in DELIVERY_CODES
    assert INGESTION_CODES.isdisjoint(DELIVERY_CODES)
    assert len(INGESTION_CODES) == 4
    assert len(DELIVERY_CODES) == 9


def test_samples_cap_is_ten():
    assert SAMPLES_CAP == 10


def test_side_of_returns_correct_side():
    assert side_of("duplicate_url") == "ingestion"
    assert side_of("enterprise_cross_segment_low_impact") == "delivery"


def test_label_for_returns_human_label():
    assert label_for("duplicate_url") == "duplicate URL"
    assert label_for("enterprise_cross_segment_low_impact") == "Enterprise / Cross-Segment, low impact"
