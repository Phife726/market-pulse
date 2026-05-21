"""Pure in-process module owning the suppression reason taxonomy, samples
cap, and same-day-retry merge semantics. Performs zero I/O."""
from dataclasses import dataclass, field
from typing import Literal, Mapping

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


@dataclass(frozen=True)
class SuppressionSample:
    """One suppressed-item record. Persisted shape is {reason, url, title}."""
    reason: str
    url: str
    title: str

    def to_dict(self) -> dict:
        return {"reason": self.reason, "url": self.url, "title": self.title}


@dataclass(frozen=True)
class SuppressionLedger:
    """Side-tagged immutable accumulator. Build via for_ingestion()/for_delivery()."""
    side: Side
    breakdown: Mapping[str, int] = field(default_factory=dict)
    samples:   tuple[SuppressionSample, ...] = field(default_factory=tuple)

    @classmethod
    def for_ingestion(cls) -> "SuppressionLedger":
        return cls(side="ingestion")

    @classmethod
    def for_delivery(cls) -> "SuppressionLedger":
        return cls(side="delivery")

    def record(self, reason: str, *, url: str, title: str) -> "SuppressionLedger":
        """Return a new ledger with `reason` incremented and a sample appended.
        Samples are deduped by (reason, url, title) and FIFO-capped at SAMPLES_CAP.
        Raises ValueError if `reason` is unknown or not owned by this ledger's side."""
        if reason not in _SIDE_OF:
            raise ValueError(f"unknown reason: {reason!r}")
        if _SIDE_OF[reason] != self.side:
            raise ValueError(
                f"reason {reason!r} not owned by {self.side} "
                f"(owned by {_SIDE_OF[reason]})"
            )
        new_breakdown = dict(self.breakdown)
        new_breakdown[reason] = new_breakdown.get(reason, 0) + 1

        sample = SuppressionSample(reason=reason, url=url, title=title)
        existing = tuple(s for s in self.samples if (s.reason, s.url, s.title) != (reason, url, title))
        new_samples = existing + (sample,)
        if len(new_samples) > SAMPLES_CAP:
            new_samples = new_samples[-SAMPLES_CAP:]
        return SuppressionLedger(side=self.side, breakdown=new_breakdown, samples=new_samples)
