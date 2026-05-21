"""Pure in-process module owning the suppression reason taxonomy, samples
cap, and same-day-retry merge semantics. Performs zero I/O."""
from typing import Literal

Side = Literal["ingestion", "delivery"]

SAMPLES_CAP: int = 10

_INGESTION_REASONS: tuple[tuple[str, str], ...] = (
    ("duplicate_url",      "duplicate URL"),
    ("semantic_duplicate", "semantic duplicate"),
    ("llm_discard",        "LLM discard"),
    ("scrape_failed",      "scrape failed"),
)
_DELIVERY_REASONS: tuple[tuple[str, str], ...] = (
    ("below_impact_threshold",              "below impact threshold"),
    ("weak_relevance",                      "weak relevance (4-5, ungrouped)"),
    ("duplicate_headline",                  "duplicate headline"),
    ("semantic_duplicate_headline",         "semantic duplicate headline"),
    ("product_listing",                     "product listing"),
    ("job_posting",                         "job posting"),
    ("generic_market_report",               "generic market report"),
    ("unrelated_color_result",              "unrelated color result"),
    ("enterprise_cross_segment_low_impact", "Enterprise / Cross-Segment, low impact"),
)

INGESTION_CODES: frozenset[str] = frozenset(c for c, _ in _INGESTION_REASONS)
DELIVERY_CODES:  frozenset[str] = frozenset(c for c, _ in _DELIVERY_REASONS)

_LABELS: dict[str, str] = dict(_INGESTION_REASONS + _DELIVERY_REASONS)
_SIDE_OF: dict[str, Side] = (
    {c: "ingestion" for c, _ in _INGESTION_REASONS}
    | {c: "delivery" for c, _ in _DELIVERY_REASONS}
)


def side_of(reason: str) -> Side:
    """Return which side owns `reason`. Raises KeyError if unknown."""
    return _SIDE_OF[reason]


def label_for(reason: str) -> str:
    """Return the human-readable label for `reason`. Falls back to the code itself if unknown (forward-compat for future codes)."""
    return _LABELS.get(reason, reason)
