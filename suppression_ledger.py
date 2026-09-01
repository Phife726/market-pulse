"""Pure in-process module owning the suppression reason taxonomy, samples
cap, and same-day-retry merge semantics, plus the two faces of that
taxonomy: `SuppressionLedger`, the side-tagged accumulator both engines
record into, and `SuppressionAccounting`, the side-less reader of what a
persisted row records. Performs zero I/O."""
from dataclasses import dataclass, field
from typing import Literal, Mapping

Side = Literal["ingestion", "delivery"]

SAMPLES_CAP: int = 10

_INGESTION_REASONS: tuple[tuple[str, str], ...] = (
    ("duplicate_url",            "duplicate URL"),
    ("semantic_duplicate",       "semantic duplicate"),
    ("llm_discard",              "LLM discard"),
    ("scrape_failed",            "scrape failed"),
    ("synthesis_failed",         "LLM synthesis failed"),
    ("unscrapable_domain",       "unscrapable domain"),
    ("zoominfo_company_mismatch", "ZoomInfo company mismatch"),
)
_DELIVERY_REASONS: tuple[tuple[str, str], ...] = (
    ("below_impact_threshold",              "below impact threshold"),
    ("weak_relevance",                      "weak relevance (below visible, ungrouped)"),
    ("duplicate_headline",                  "duplicate headline"),
    ("semantic_duplicate_headline",         "semantic duplicate headline"),
    ("product_listing",                     "product listing"),
    ("job_posting",                         "job posting"),
    ("generic_market_report",               "generic market report"),
    ("unrelated_color_result",              "unrelated color result"),
    ("enterprise_cross_segment_low_impact", "Enterprise / Cross-Segment, low impact"),
    ("appendix_excluded_category",          "appendix-excluded category (macro group)"),
)

INGESTION_CODES: frozenset[str] = frozenset(c for c, _ in _INGESTION_REASONS)
DELIVERY_CODES:  frozenset[str] = frozenset(c for c, _ in _DELIVERY_REASONS)
# Every code in stable reading order: ingestion-side first, then delivery-side,
# each in taxonomy order. The one ordered view of the taxonomy — anything that
# enumerates reasons for display (the QA breakdown strip) iterates this instead
# of keeping its own copy, so a new code cannot be left off a hand-copied list.
ALL_CODES: tuple[str, ...] = tuple(c for c, _ in _INGESTION_REASONS + _DELIVERY_REASONS)

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

    @classmethod
    def from_dict(cls, raw: Mapping) -> "SuppressionSample":
        """The read half of to_dict. A missing or explicitly-null field reads as
        '' — never the literal 'None', which is what a bare str(None) would put
        in front of a QA reader."""
        return cls(
            reason=str(raw.get("reason") or ""),
            url=str(raw.get("url") or ""),
            title=str(raw.get("title") or ""),
        )


@dataclass(frozen=True)
class SuppressionAccounting:
    """The side-less read face of a `daily_summaries` row's two suppression
    columns — what the row *records*, as opposed to what a run *accumulates*.

    A persisted row is merged and two-sided (delivery's write-back keeps
    ingestion's codes), so it has no single owning side; `SuppressionLedger`
    is the write face and stays side-tagged. This is the one parser of the
    persisted shape: `SuppressionLedger.from_row` delegates to it, so the
    same-day-retry merge and the renderer's QA block read by identical rules.

    Tolerant on purpose, because both consumers take the row as the database
    left it: missing keys, a null or non-list samples value, a non-dict
    sample, a null sample field, and an uninterpretable count — dropped, not
    raised, since raising would skip a whole day's delivery accounting over
    one meaningless code, and crash the QA email that exists to show it.

    Not capped: `SAMPLES_CAP` is a write policy (`record` / `merge_with`
    enforce it) and pre-capping here would change `merge_with`'s dedupe over
    an over-cap legacy row. Readers wanting the display slice ask `recent()`.
    """
    breakdown: Mapping[str, int] = field(default_factory=dict)
    samples:   tuple[SuppressionSample, ...] = field(default_factory=tuple)

    @classmethod
    def from_row(cls, row: Mapping | None) -> "SuppressionAccounting":
        if not row:
            return cls()
        # The two columns are jsonb: a ragged row can hand us the wrong container
        # as easily as the wrong value, and both consumers must survive it — the
        # merge path would otherwise skip a whole day's accounting, and the QA
        # email would die rendering the very row it exists to show.
        breakdown_raw = row.get("suppression_breakdown") or {}
        samples_raw   = row.get("suppression_samples") or []
        if not isinstance(samples_raw, (list, tuple)):
            samples_raw = ()
        try:
            pairs = list(dict(breakdown_raw).items())
        except (TypeError, ValueError):
            pairs = []
        breakdown: dict[str, int] = {}
        for code, count in pairs:
            try:
                breakdown[str(code)] = int(count)
            except (TypeError, ValueError):
                continue
        samples = tuple(
            SuppressionSample.from_dict(s) for s in samples_raw if isinstance(s, dict)
        )
        return cls(breakdown=breakdown, samples=samples)

    def recent(self, n: int = SAMPLES_CAP) -> tuple[SuppressionSample, ...]:
        """The newest `n` samples — the FIFO slice `record` and `merge_with`
        keep on the way out, offered here so a display doesn't re-implement the
        cap policy against a write constant."""
        return self.samples[-n:] if n else ()


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
        Samples are deduped by (reason, url, title) with first-occurrence preserved
        (the first sighting is the authoritative sample — matches merge_with's
        semantics) and FIFO-capped at SAMPLES_CAP. Count increments regardless.
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

        key = (reason, url, title)
        if any((s.reason, s.url, s.title) == key for s in self.samples):
            new_samples = self.samples
        else:
            sample = SuppressionSample(reason=reason, url=url, title=title)
            new_samples = self.samples + (sample,)
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
        """The accounting reader plus the side tag the caller is about to act
        as — not the side of the codes in the row, which is merged and
        two-sided. Parsing and its tolerances live in `SuppressionAccounting`."""
        acc = SuppressionAccounting.from_row(row)
        return cls(side=side, breakdown=acc.breakdown, samples=acc.samples)
