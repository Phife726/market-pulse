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

    def record_count(self, reason: str, n: int) -> "SuppressionLedger":
        """Return a new ledger with `reason` incremented by `n`. No sample appended.
        n must be >= 0; n == 0 is a no-op. Raises ValueError on negative n,
        unknown reason, or wrong-side reason."""
        if n < 0:
            raise ValueError(f"n must be non-negative, got {n}")
        if n == 0:
            return self
        if reason not in _SIDE_OF:
            raise ValueError(f"unknown reason: {reason!r}")
        if _SIDE_OF[reason] != self.side:
            raise ValueError(
                f"reason {reason!r} not owned by {self.side} "
                f"(owned by {_SIDE_OF[reason]})"
            )
        new_breakdown = dict(self.breakdown)
        new_breakdown[reason] = new_breakdown.get(reason, 0) + n
        return SuppressionLedger(side=self.side, breakdown=new_breakdown, samples=self.samples)

    def merge_with(self, prior: "SuppressionLedger") -> "SuppressionLedger":
        """Combine this delivery run with the `prior` persisted state for
        same-day-retry idempotency.

        - Ingestion-owned codes: taken from `prior` (delivery never touches them).
        - Delivery-owned codes:  taken from `self` (overwrite, do not sum).
        - Unknown future codes:  taken from `prior` (forward-compat).
        - Samples: prior + self, deduped by (reason, url, title), FIFO-capped.

        Only callable on a delivery ledger; raises RuntimeError otherwise."""
        if self.side != "delivery":
            raise RuntimeError("merge_with is delivery-only")

        merged_breakdown: dict[str, int] = {}
        # 1. Start with prior, dropping delivery-owned codes (we'll overwrite).
        for code, count in prior.breakdown.items():
            if code not in DELIVERY_CODES:
                merged_breakdown[code] = count
        # 2. Overlay self's delivery-owned counts.
        for code, count in self.breakdown.items():
            merged_breakdown[code] = count

        # Samples: prior-first ordering, dedupe by (reason, url, title), cap.
        seen: set[tuple[str, str, str]] = set()
        merged_samples: list[SuppressionSample] = []
        for s in tuple(prior.samples) + tuple(self.samples):
            key = (s.reason, s.url, s.title)
            if key in seen:
                continue
            seen.add(key)
            merged_samples.append(s)
        if len(merged_samples) > SAMPLES_CAP:
            merged_samples = merged_samples[-SAMPLES_CAP:]

        return SuppressionLedger(
            side="delivery",
            breakdown=merged_breakdown,
            samples=tuple(merged_samples),
        )

    def to_row(self) -> dict:
        """Return the persisted-shape dict ready for daily_summaries upsert.
        Keys match the column names: 'suppression_breakdown', 'suppression_samples'."""
        return {
            "suppression_breakdown": dict(self.breakdown),
            "suppression_samples": [s.to_dict() for s in self.samples],
        }

    @classmethod
    def from_row(cls, side: Side, row: Mapping | None) -> "SuppressionLedger":
        """Reconstruct a ledger from a daily_summaries row (or None → empty).
        Tolerates missing keys, non-list samples, and missing sample fields."""
        if not row:
            return cls(side=side)
        breakdown_raw = row.get("suppression_breakdown") or {}
        samples_raw   = row.get("suppression_samples") or []
        breakdown = {str(k): int(v) for k, v in dict(breakdown_raw).items()}
        samples = tuple(
            SuppressionSample(
                reason=str(s.get("reason", "")),
                url=str(s.get("url", "")),
                title=str(s.get("title", "")),
            )
            for s in samples_raw
            if isinstance(s, dict)
        )
        return cls(side=side, breakdown=breakdown, samples=samples)
