# tests/test_suppression_ledger.py
from suppression_ledger import (
    SAMPLES_CAP,
    INGESTION_CODES,
    DELIVERY_CODES,
    side_of,
    label_for,
    SuppressionLedger,
    SuppressionSample,
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


def test_ledger_for_ingestion_starts_empty():
    led = SuppressionLedger.for_ingestion()
    assert led.side == "ingestion"
    assert led.breakdown == {}
    assert led.samples == ()


def test_ledger_for_delivery_starts_empty():
    led = SuppressionLedger.for_delivery()
    assert led.side == "delivery"
    assert led.breakdown == {}
    assert led.samples == ()


def test_sample_is_frozen():
    s = SuppressionSample(reason="duplicate_url", url="u", title="t")
    with __import__("pytest").raises(Exception):
        s.reason = "other"  # frozen dataclass


import pytest


def test_record_increments_count_and_appends_sample():
    led = SuppressionLedger.for_ingestion().record(
        "duplicate_url", url="https://x/1", title="T1",
    )
    assert led.breakdown == {"duplicate_url": 1}
    assert led.samples == (SuppressionSample("duplicate_url", "https://x/1", "T1"),)


def test_record_returns_new_instance_does_not_mutate_original():
    led1 = SuppressionLedger.for_ingestion()
    led2 = led1.record("duplicate_url", url="u", title="t")
    assert led1.breakdown == {}
    assert led1.samples == ()
    assert led2.breakdown == {"duplicate_url": 1}


def test_record_wrong_side_raises_value_error():
    led = SuppressionLedger.for_ingestion()
    with pytest.raises(ValueError, match="not owned by ingestion"):
        led.record("below_impact_threshold", url="u", title="t")


def test_record_unknown_reason_raises_value_error():
    led = SuppressionLedger.for_delivery()
    with pytest.raises(ValueError, match="unknown reason"):
        led.record("totally_made_up_code", url="u", title="t")


def test_record_dedupes_identical_sample_but_count_still_grows():
    led = SuppressionLedger.for_ingestion()
    led = led.record("duplicate_url", url="https://x/1", title="T1")
    led = led.record("duplicate_url", url="https://x/1", title="T1")
    led = led.record("duplicate_url", url="https://x/1", title="T1")
    assert led.breakdown == {"duplicate_url": 3}
    # Sample is deduped — only one survives
    assert len(led.samples) == 1


def test_record_caps_samples_at_ten_fifo():
    led = SuppressionLedger.for_ingestion()
    for i in range(15):
        led = led.record("duplicate_url", url=f"https://x/{i}", title=f"T{i}")
    assert len(led.samples) == 10
    # Last 10 survived — first 5 evicted
    assert led.samples[0].url == "https://x/5"
    assert led.samples[-1].url == "https://x/14"
    # Count is full 15
    assert led.breakdown == {"duplicate_url": 15}


def test_record_count_increments_breakdown_only():
    led = SuppressionLedger.for_delivery().record_count("below_impact_threshold", 7)
    assert led.breakdown == {"below_impact_threshold": 7}
    assert led.samples == ()


def test_record_count_zero_is_noop():
    led1 = SuppressionLedger.for_delivery()
    led2 = led1.record_count("weak_relevance", 0)
    assert led2 == led1
    assert led2.breakdown == {}


def test_record_count_negative_raises():
    led = SuppressionLedger.for_delivery()
    with pytest.raises(ValueError, match="must be non-negative"):
        led.record_count("weak_relevance", -1)


def test_record_count_wrong_side_raises():
    led = SuppressionLedger.for_ingestion()
    with pytest.raises(ValueError, match="not owned by ingestion"):
        led.record_count("below_impact_threshold", 3)


def test_record_count_unknown_reason_raises():
    led = SuppressionLedger.for_delivery()
    with pytest.raises(ValueError, match="unknown reason"):
        led.record_count("not_a_thing", 1)


def test_record_count_accumulates_with_prior_record_calls():
    led = (SuppressionLedger.for_delivery()
           .record("duplicate_headline", url="u", title="t")
           .record_count("duplicate_headline", 4))
    assert led.breakdown == {"duplicate_headline": 5}
    assert len(led.samples) == 1
