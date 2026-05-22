# Suppression Ledger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace scattered suppression-accounting code in `ingestion_engine.py` and `delivery_engine.py` with a single in-process `SuppressionLedger` module that owns the reason taxonomy, samples cap, and same-day retry merge semantics.

**Architecture:** New pure-data module `suppression_ledger.py`. Frozen-dataclass `SuppressionLedger` carries a side tag (`"ingestion"` or `"delivery"`), an immutable breakdown, and immutable samples. Three recording verbs (`record`, `record_count`, `merge_with`) return new ledgers. Persistence helpers (`to_row`/`from_row`) emit and consume the existing `daily_summaries` JSON shape — no DB migration. The ledger performs zero I/O.

**Tech Stack:** Python 3.14 (project default), stdlib only (`dataclasses`, `typing`). No new third-party deps.

**Out of scope (do not touch in this plan):**
- The `daily_summaries.suppression_breakdown` / `suppression_samples` column shapes.
- The set of reason codes (still exactly the 13 listed below).
- The `_LEGACY_STRATEGIC_SEGMENT_MAP` fallback (tracked separately in issue #8 — Candidate B2).
- Any `generate_html_email` orchestration changes (Candidate A — separate plan).

---

## File Structure

| File | Role |
|---|---|
| Create: `suppression_ledger.py` | Pure data module — reason taxonomy, `SuppressionSample` and `SuppressionLedger` dataclasses, `label_for`/`side_of` helpers |
| Create: `tests/test_suppression_ledger.py` | Unit tests for the ledger interface and merge invariants |
| Modify: `ingestion_engine.py` | Replace `_record_suppression` (line 201) and `_SUPPRESSION_SAMPLES_CAP` (line 198); update callsites at 869, 879, 888, 901; update flush sites at 841–848, 854–861, 942–948 |
| Modify: `delivery_engine.py` | Replace `_DELIVERY_SAMPLES_CAP` (line 251), `_apply_delivery_suppression` inline `_suppress` closure (lines 291–299), `_INGESTION_SUPPRESSION_KEYS` / `_DELIVERY_SUPPRESSION_KEYS` (lines 398, 407), `_QA_REASON_LABELS` (line 419), and the merge logic in `_update_delivery_summary_counts` (lines 638–693). Update QA-render label lookups at lines 460, 472 |

## Canonical reason taxonomy (used in multiple tasks — copy verbatim)

```python
# In suppression_ledger.py:
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
```

---

## Checkpoint 1 — Ledger module and tests (TDD)

Build the module bottom-up with red-green-commit. Each task creates one or more failing tests, then the minimum implementation to pass.

### Task 1.1: Module skeleton + reason taxonomy

**Files:**
- Create: `suppression_ledger.py`
- Create: `tests/test_suppression_ledger.py`

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test, confirm failure**

Run: `pytest tests/test_suppression_ledger.py -v`
Expected: `ImportError: cannot import name 'SAMPLES_CAP' from 'suppression_ledger'`

- [ ] **Step 3: Create the module with the minimum to pass**

```python
# suppression_ledger.py
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
```

- [ ] **Step 4: Run tests, confirm pass**

Run: `pytest tests/test_suppression_ledger.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add suppression_ledger.py tests/test_suppression_ledger.py
git commit -m "feat(suppression): add reason taxonomy and module skeleton"
```

---

### Task 1.2: `SuppressionSample` + `SuppressionLedger` value type

**Files:**
- Modify: `suppression_ledger.py`
- Modify: `tests/test_suppression_ledger.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_suppression_ledger.py — append
from suppression_ledger import SuppressionLedger, SuppressionSample


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
```

- [ ] **Step 2: Run, confirm failure**

Run: `pytest tests/test_suppression_ledger.py -v -k "ledger_for or sample_is_frozen"`
Expected: 3 ImportError-driven failures.

- [ ] **Step 3: Implement the value types**

```python
# suppression_ledger.py — append after the helpers
from dataclasses import dataclass, field
from typing import Mapping


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
```

- [ ] **Step 4: Run, confirm pass**

Run: `pytest tests/test_suppression_ledger.py -v`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add suppression_ledger.py tests/test_suppression_ledger.py
git commit -m "feat(suppression): add SuppressionSample and SuppressionLedger value types"
```

---

### Task 1.3: `record()` — count + sample, with side validation

**Files:** as above

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_suppression_ledger.py — append
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
```

- [ ] **Step 2: Run, confirm failure**

Run: `pytest tests/test_suppression_ledger.py -v -k "record"`
Expected: 4 AttributeError failures (`record` not defined).

- [ ] **Step 3: Implement `record()`**

```python
# suppression_ledger.py — add as method of SuppressionLedger
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
        # Dedupe by identity tuple; FIFO cap
        existing = tuple(s for s in self.samples if (s.reason, s.url, s.title) != (reason, url, title))
        new_samples = existing + (sample,)
        if len(new_samples) > SAMPLES_CAP:
            new_samples = new_samples[-SAMPLES_CAP:]
        return SuppressionLedger(side=self.side, breakdown=new_breakdown, samples=new_samples)
```

- [ ] **Step 4: Run, confirm pass**

Run: `pytest tests/test_suppression_ledger.py -v`
Expected: 11 passed.

- [ ] **Step 5: Commit**

```bash
git add suppression_ledger.py tests/test_suppression_ledger.py
git commit -m "feat(suppression): add record() with side validation"
```

---

### Task 1.4: Dedupe + samples cap behaviour

**Files:** as above

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_suppression_ledger.py — append
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
```

- [ ] **Step 2: Run, confirm pass (already implemented above)**

Run: `pytest tests/test_suppression_ledger.py -v -k "dedupes or caps"`
Expected: 2 passed (validates Task 1.3's implementation handled these correctly).

If either fails, fix the implementation in Task 1.3 before continuing.

- [ ] **Step 3: Commit**

```bash
git add tests/test_suppression_ledger.py
git commit -m "test(suppression): cover dedupe and FIFO cap invariants"
```

---

### Task 1.5: `record_count()` — aggregate counters with no sample

**Files:** as above

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_suppression_ledger.py — append
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
```

- [ ] **Step 2: Run, confirm failure**

Run: `pytest tests/test_suppression_ledger.py -v -k "record_count"`
Expected: 6 AttributeError failures.

- [ ] **Step 3: Implement `record_count()`**

```python
# suppression_ledger.py — add as method of SuppressionLedger
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
```

- [ ] **Step 4: Run, confirm pass**

Run: `pytest tests/test_suppression_ledger.py -v`
Expected: 19 passed.

- [ ] **Step 5: Commit**

```bash
git add suppression_ledger.py tests/test_suppression_ledger.py
git commit -m "feat(suppression): add record_count() for aggregate counters"
```

---

### Task 1.6: `merge_with()` — same-day retry semantics

**Files:** as above

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_suppression_ledger.py — append
def test_merge_with_preserves_prior_ingestion_codes_on_delivery_merge():
    prior = (SuppressionLedger.for_ingestion()
             .record("duplicate_url", url="u1", title="t1")
             .record("scrape_failed", url="u2", title="t2"))
    new = (SuppressionLedger.for_delivery()
           .record_count("below_impact_threshold", 3))
    merged = new.merge_with(prior)
    assert merged.side == "delivery"
    # Ingestion codes survive untouched
    assert merged.breakdown["duplicate_url"] == 1
    assert merged.breakdown["scrape_failed"] == 1
    # Delivery code is what `new` set it to
    assert merged.breakdown["below_impact_threshold"] == 3


def test_merge_with_overwrites_prior_delivery_codes_on_retry():
    prior_run = (SuppressionLedger.for_delivery()
                 .record_count("below_impact_threshold", 99))
    second_run = (SuppressionLedger.for_delivery()
                  .record_count("below_impact_threshold", 7))
    merged = second_run.merge_with(prior_run)
    # Second run overwrites — not 99 + 7
    assert merged.breakdown["below_impact_threshold"] == 7


def test_merge_with_preserves_unknown_future_codes_from_prior():
    # Simulate a prior row that has a code the ledger doesn't know yet
    prior = SuppressionLedger(
        side="ingestion",
        breakdown={"some_future_code": 5},
        samples=(),
    )
    new = SuppressionLedger.for_delivery().record_count("weak_relevance", 2)
    merged = new.merge_with(prior)
    assert merged.breakdown["some_future_code"] == 5
    assert merged.breakdown["weak_relevance"] == 2


def test_merge_with_samples_dedupes_across_runs():
    prior = (SuppressionLedger.for_ingestion()
             .record("duplicate_url", url="u1", title="t1"))
    new = (SuppressionLedger.for_delivery()
           .record("duplicate_headline", url="u2", title="t2"))
    # Same sample exists in prior; new run records it again on the delivery side
    # (simulating an item that triggered both ingestion-side dedupe and
    # delivery-side dedupe across retries)
    new_with_dupe = new.record("duplicate_headline", url="u2", title="t2")
    merged = new_with_dupe.merge_with(prior)
    # Each unique (reason, url, title) appears exactly once
    sample_keys = [(s.reason, s.url, s.title) for s in merged.samples]
    assert len(sample_keys) == len(set(sample_keys))
    assert ("duplicate_url", "u1", "t1") in sample_keys
    assert ("duplicate_headline", "u2", "t2") in sample_keys


def test_merge_with_caps_samples_at_ten_after_merge():
    prior = SuppressionLedger.for_ingestion()
    for i in range(8):
        prior = prior.record("duplicate_url", url=f"u{i}", title=f"t{i}")
    new = SuppressionLedger.for_delivery()
    for i in range(8):
        new = new.record("duplicate_headline", url=f"d{i}", title=f"dt{i}")
    merged = new.merge_with(prior)
    assert len(merged.samples) == 10


def test_merge_with_on_ingestion_ledger_raises():
    prior = SuppressionLedger.for_delivery()
    ingestion_ledger = SuppressionLedger.for_ingestion()
    with pytest.raises(RuntimeError, match="merge_with is delivery-only"):
        ingestion_ledger.merge_with(prior)


def test_idempotent_same_day_delivery_retry():
    """The load-bearing invariant: running merge twice with the same input
    produces the same output as running it once."""
    prior = (SuppressionLedger.for_ingestion()
             .record("duplicate_url", url="u1", title="t1")
             .record("scrape_failed", url="u2", title="t2"))
    new = (SuppressionLedger.for_delivery()
           .record("duplicate_headline", url="u3", title="t3")
           .record_count("below_impact_threshold", 5))
    once = new.merge_with(prior)
    twice = new.merge_with(once)  # Simulating a retry where prior == once
    assert once.breakdown == twice.breakdown
    assert {(s.reason, s.url, s.title) for s in once.samples} == \
           {(s.reason, s.url, s.title) for s in twice.samples}
```

- [ ] **Step 2: Run, confirm failure**

Run: `pytest tests/test_suppression_ledger.py -v -k "merge or idempotent"`
Expected: 7 AttributeError failures.

- [ ] **Step 3: Implement `merge_with()`**

```python
# suppression_ledger.py — add as method of SuppressionLedger
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
```

- [ ] **Step 4: Run, confirm pass**

Run: `pytest tests/test_suppression_ledger.py -v`
Expected: 26 passed.

- [ ] **Step 5: Commit**

```bash
git add suppression_ledger.py tests/test_suppression_ledger.py
git commit -m "feat(suppression): add merge_with() with same-day retry idempotency"
```

---

### Task 1.7: `to_row()` / `from_row()` — persistence shape compat

**Files:** as above

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_suppression_ledger.py — append
def test_to_row_emits_persisted_shape():
    led = (SuppressionLedger.for_ingestion()
           .record("duplicate_url", url="https://x/1", title="T1")
           .record_count("scrape_failed", 3))
    row = led.to_row()
    assert row == {
        "suppression_breakdown": {"duplicate_url": 1, "scrape_failed": 3},
        "suppression_samples": [
            {"reason": "duplicate_url", "url": "https://x/1", "title": "T1"},
        ],
    }


def test_from_row_reconstructs_ledger():
    row = {
        "suppression_breakdown": {"duplicate_url": 2, "below_impact_threshold": 5},
        "suppression_samples": [
            {"reason": "duplicate_url", "url": "https://x/1", "title": "T1"},
        ],
    }
    led = SuppressionLedger.from_row("ingestion", row)
    assert led.side == "ingestion"
    assert led.breakdown == {"duplicate_url": 2, "below_impact_threshold": 5}
    assert led.samples == (SuppressionSample("duplicate_url", "https://x/1", "T1"),)


def test_from_row_handles_none_or_missing_keys():
    empty = SuppressionLedger.from_row("delivery", None)
    assert empty.side == "delivery"
    assert empty.breakdown == {}
    assert empty.samples == ()

    partial = SuppressionLedger.from_row("delivery", {"suppression_breakdown": {"weak_relevance": 1}})
    assert partial.breakdown == {"weak_relevance": 1}
    assert partial.samples == ()


def test_to_row_then_from_row_roundtrip():
    led1 = (SuppressionLedger.for_delivery()
            .record("duplicate_headline", url="u", title="t")
            .record_count("below_impact_threshold", 4))
    led2 = SuppressionLedger.from_row("delivery", led1.to_row())
    assert led1 == led2
```

- [ ] **Step 2: Run, confirm failure**

Run: `pytest tests/test_suppression_ledger.py -v -k "to_row or from_row or roundtrip"`
Expected: 4 AttributeError failures.

- [ ] **Step 3: Implement `to_row()` and `from_row()`**

```python
# suppression_ledger.py — add as method/classmethod of SuppressionLedger
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
```

- [ ] **Step 4: Run, confirm pass**

Run: `pytest tests/test_suppression_ledger.py -v`
Expected: 30 passed.

- [ ] **Step 5: Commit**

```bash
git add suppression_ledger.py tests/test_suppression_ledger.py
git commit -m "feat(suppression): add to_row/from_row persistence helpers"
```

---

### Checkpoint 1 verification

- [ ] Run the full test suite to confirm the ledger module is self-contained and existing tests still pass.

Run: `python -m py_compile suppression_ledger.py && python -m pytest tests/ -x --tb=short -q`
Expected: all tests pass (existing 166 + new ~30 = ~196 total).

---

## Checkpoint 2 — Wire Ingestion

Replace `_record_suppression` + `_SUPPRESSION_SAMPLES_CAP` with the ledger. Persisted shape must stay identical — verify by comparing the dicts passed into `generate_macro_summary`.

### Task 2.1: Replace `_record_suppression` with ledger record calls

**Files:**
- Modify: `ingestion_engine.py` lines 198–214 (constants + `_record_suppression`)
- Modify: `ingestion_engine.py` lines 798–949 (`execute_pipeline` — three flush sites and four call sites)

- [ ] **Step 1: Replace the constants and helper**

Delete `_SUPPRESSION_SAMPLES_CAP` (line 198) and `_record_suppression` (lines 201–213). Add at the top of the module (next to other imports):

```python
from suppression_ledger import SuppressionLedger
```

- [ ] **Step 2: Update `execute_pipeline` to use the ledger**

In `execute_pipeline()` (line 798):

1. Replace the `suppression_samples: list[dict] = []` initialization (line 815) with:
   ```python
   suppression_ledger = SuppressionLedger.for_ingestion()
   ```

2. Remove the four reason-code keys from the `stats` dict initializer (lines 808–813) — they're now owned by the ledger:
   ```python
   stats = {
       "urls_discovered": 0,
       "scrapes_attempted": 0,
       "insights_stored": 0,
       "errors": 0,
   }
   ```

3. Replace each of the four `_record_suppression(...)` calls (lines 869, 879, 888, 901) with the immutable assignment pattern. Example for line 869:
   ```python
   suppression_ledger = suppression_ledger.record(
       "duplicate_url", url=raw_url, title=serper_title,
   )
   ```
   Apply the same shape to lines 879 (`semantic_duplicate`), 888 (`scrape_failed`), 901 (`llm_discard`).

4. Replace each of the three flush calls to `generate_macro_summary` (lines 841–848, 854–861, 942–948). Each currently passes:
   ```python
   suppression_breakdown={k: v for k, v in stats.items()
                          if k in {"duplicate_url", "semantic_duplicate",
                                   "llm_discard", "scrape_failed"}},
   suppression_samples=suppression_samples,
   ```
   Replace with:
   ```python
   **suppression_ledger.to_row(),
   ```
   (Note: this passes `suppression_breakdown=...` and `suppression_samples=...` as kwargs from `to_row()`'s output.)

5. Update `_log_stats` (line 782). It reads the four removed reason-code keys from `stats`. Change the signature and body:

   ```python
   def _log_stats(stats: dict, breakdown: dict[str, int]) -> None:
       logger.info(
           "Pipeline complete — discovered: %d | duplicates skipped: %d | "
           "semantic duplicates: %d | scrape failed: %d | discards: %d | "
           "scrapes attempted: %d | stored: %d | errors: %d",
           stats["urls_discovered"],
           breakdown.get("duplicate_url", 0),
           breakdown.get("semantic_duplicate", 0),
           breakdown.get("scrape_failed", 0),
           breakdown.get("llm_discard", 0),
           stats["scrapes_attempted"],
           stats["insights_stored"],
           stats["errors"],
       )
   ```

   Update its two callers (line 840 and 941 in the original) to pass the ledger's breakdown:

   ```python
   _log_stats(stats, suppression_ledger.breakdown)
   ```

- [ ] **Step 3: Run the full test suite**

Run: `python -m py_compile ingestion_engine.py suppression_ledger.py && python -m pytest tests/ -x --tb=short -q`
Expected: all tests pass. If existing pipeline tests fail (e.g. `test_execute_pipeline_deadline_calls_log_stats_and_macro_summary`), inspect the failures — they likely reference the old `suppression_breakdown` kwarg shape, which `to_row()` preserves exactly. If a test was mocking `_record_suppression`, update it to assert against the ledger's recorded state instead.

- [ ] **Step 4: Verify the persisted shape with a one-off probe**

Add a temporary debug print (or use an existing test's fixture) to inspect the kwargs passed to `generate_macro_summary`. The `suppression_breakdown` value must be `dict[str, int]` and `suppression_samples` must be `list[dict]` with `reason`/`url`/`title` keys. Remove the probe after verification.

- [ ] **Step 5: Commit**

```bash
git add ingestion_engine.py
git commit -m "refactor(ingestion): use SuppressionLedger for suppression accounting"
```

---

## Checkpoint 3 — Wire Delivery (recording side)

Replace the inline `_suppress` closure inside `_apply_delivery_suppression` and remove the partition frozensets and `_QA_REASON_LABELS`.

### Task 3.1: Replace `_apply_delivery_suppression` accumulator with ledger

**Files:**
- Modify: `delivery_engine.py` lines 251 (`_DELIVERY_SAMPLES_CAP`), 266–365 (`_apply_delivery_suppression`)

- [ ] **Step 1: Update the function signature and inner accumulator**

Delete `_DELIVERY_SAMPLES_CAP` (line 251). Add the import at the top of the module:

```python
from suppression_ledger import SuppressionLedger, label_for
```

Rewrite `_apply_delivery_suppression` to return a ledger instead of `(counts, samples)`. New signature:

```python
def _apply_delivery_suppression(
    rows: list[dict],
    config: dict,
) -> tuple[list[dict], SuppressionLedger]:
    """Run the deterministic seven-rule guardrail over fetched rows.
    Returns (kept_rows, ledger). First matching rule wins; each suppressed
    row is counted once and contributes at most one sample (deduped)."""
```

Inside, replace the `counts: dict[str, int] = {}`, `samples: list[dict] = []`, and `_suppress(...)` closure with a single ledger variable, and rewrite each `_suppress("reason_code", row)` call site as:

```python
ledger = ledger.record(
    "reason_code",
    url=row.get("source_url", "") or "",
    title=row.get("headline", "") or "",
)
```

Initialize `ledger = SuppressionLedger.for_delivery()` at the top of the function.

Return `(kept, ledger)` instead of `(kept, counts, samples)`.

- [ ] **Step 2: Update the callsite in `generate_html_email`**

In `generate_html_email` (around line 1023):

```python
# Old
kept, delivery_sup_counts, delivery_sup_samples = _apply_delivery_suppression(data, config)

# New
kept, delivery_ledger = _apply_delivery_suppression(data, config)
```

The downstream merge call (Task 4.1) will consume `delivery_ledger` directly.

- [ ] **Step 3: Run the full test suite**

Run: `python -m py_compile delivery_engine.py && python -m pytest tests/ -x --tb=short -q`
Expected: most tests pass; one or two integration tests may need fixture updates if they assert on the tuple shape. If so, update those tests to unpack the ledger.

- [ ] **Step 4: Commit**

```bash
git add delivery_engine.py
git commit -m "refactor(delivery): use SuppressionLedger in _apply_delivery_suppression"
```

---

### Task 3.2: Remove the partition frozensets and `_QA_REASON_LABELS`; route QA labels through `label_for`

**Files:**
- Modify: `delivery_engine.py` lines 396–433 (`_INGESTION_SUPPRESSION_KEYS`, `_DELIVERY_SUPPRESSION_KEYS`, `_QA_REASON_LABELS`)
- Modify: `delivery_engine.py` lines 460, 472 (QA-render label lookups)

- [ ] **Step 1: Delete the three constants**

Delete `_INGESTION_SUPPRESSION_KEYS`, `_DELIVERY_SUPPRESSION_KEYS`, and `_QA_REASON_LABELS` (lines 398–433).

- [ ] **Step 2: Update the QA renderer to use `label_for`**

In `_render_qa_debug_section` (line 460):

```python
# Old
label = _QA_REASON_LABELS.get(code, code)

# New
label = label_for(code)
```

And at line 472:

```python
# Old
reason_label = _QA_REASON_LABELS.get(reason_code, reason_code)

# New
reason_label = label_for(reason_code)
```

- [ ] **Step 3: Run the full test suite**

Run: `python -m py_compile delivery_engine.py && python -m pytest tests/ -x --tb=short -q`
Expected: all tests pass. The label lookups produce identical strings (the canonical taxonomy was the source for `_QA_REASON_LABELS`).

- [ ] **Step 4: Commit**

```bash
git add delivery_engine.py
git commit -m "refactor(delivery): drop key partitions and QA labels — sourced from ledger module"
```

---

## Checkpoint 4 — Merge cleanup

Rewrite `_update_delivery_summary_counts` so the merge logic lives in the ledger, not in delivery comments.

### Task 4.1: Refactor `_update_delivery_summary_counts` to use the ledger

**Files:**
- Modify: `delivery_engine.py` lines 638–693 (`_update_delivery_summary_counts`)
- Modify: `delivery_engine.py` around lines 1054–1075 (the caller in `generate_html_email`)

- [ ] **Step 1: Write a failing test for retry idempotency end-to-end**

Add a test that confirms the wired-up delivery path is idempotent at the integration level (the ledger's unit test covers the invariant, but we want one integration check).

```python
# tests/test_pipeline.py — append in the appropriate section
def test_delivery_suppression_idempotent_on_same_day_retry(monkeypatch):
    """Running delivery twice in the same day with the same inputs must
    produce identical persisted breakdown and samples."""
    from suppression_ledger import SuppressionLedger
    from delivery_engine import _update_delivery_summary_counts

    captured = []
    mock_supa = MagicMock()
    def capture_update(**kwargs):
        captured.append(kwargs)
        m = MagicMock()
        m.eq.return_value.eq.return_value.execute.return_value = MagicMock()
        return m
    mock_supa.table.return_value.update.side_effect = capture_update
    # First read returns empty prior; second read returns what was just written
    reads = [MagicMock(data=[]), MagicMock(data=[{"suppression_breakdown": {}, "suppression_samples": []}])]
    mock_supa.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.side_effect = reads

    ledger = (SuppressionLedger.for_delivery()
              .record("duplicate_headline", url="u", title="t")
              .record_count("below_impact_threshold", 3))

    with patch("delivery_engine._get_supabase", return_value=mock_supa):
        _update_delivery_summary_counts(surfaced_count=5, ledger=ledger)
        _update_delivery_summary_counts(surfaced_count=5, ledger=ledger)

    assert len(captured) == 2
    # Second run produces same breakdown + samples as first
    assert captured[0]["suppression_breakdown"] == captured[1]["suppression_breakdown"]
    assert captured[0]["suppression_samples"]   == captured[1]["suppression_samples"]
```

- [ ] **Step 2: Run, confirm failure**

Run: `pytest tests/test_pipeline.py -v -k "idempotent_on_same_day"`
Expected: signature mismatch failure (`_update_delivery_summary_counts` still takes old kwargs).

- [ ] **Step 3: Refactor `_update_delivery_summary_counts`**

Replace the whole function (lines 638–693) with:

```python
def _update_delivery_summary_counts(
    *,
    surfaced_count: int,
    ledger: SuppressionLedger,
) -> None:
    """Update today's daily_summaries row with delivery-side surfaced count
    and merged suppression accounting. Idempotent on same-day retry — the
    merge semantics live in SuppressionLedger.merge_with().

    Non-critical: failures are logged but do not raise."""
    try:
        from datetime import date as _date
        supabase = _get_supabase()

        existing = (
            supabase.table("daily_summaries")
            .select("suppression_breakdown, suppression_samples")
            .eq("run_date", _date.today().isoformat())
            .eq("run_mode", _run_mode())
            .limit(1)
            .execute()
        )
        prior_row = (existing.data or [None])[0]
        prior = SuppressionLedger.from_row("delivery", prior_row)
        merged = ledger.merge_with(prior)

        supabase.table("daily_summaries").update({
            "surfaced_count": surfaced_count,
            **merged.to_row(),
        }).eq("run_date", _date.today().isoformat()).eq("run_mode", _run_mode()).execute()
    except Exception as exc:
        logger.warning("Failed to update delivery counts on daily_summaries: %s", exc)
```

- [ ] **Step 4: Update the caller in `generate_html_email`**

In `generate_html_email` around line 1054–1075, the current caller looks like:

```python
_update_delivery_summary_counts(
    surfaced_count=surfaced_count,
    delivery_counts={
        **delivery_sup_counts,
        "below_impact_threshold": below_threshold_count,
        "weak_relevance": weak_relevance_count,
    },
    delivery_samples=delivery_sup_samples,
)
```

Replace with:

```python
delivery_ledger = (delivery_ledger
                   .record_count("below_impact_threshold", below_threshold_count)
                   .record_count("weak_relevance", weak_relevance_count))
_update_delivery_summary_counts(
    surfaced_count=surfaced_count,
    ledger=delivery_ledger,
)
```

- [ ] **Step 5: Run, confirm pass**

Run: `pytest tests/test_pipeline.py -v -k "idempotent_on_same_day"`
Expected: PASS.

- [ ] **Step 6: Run the full test suite**

Run: `python -m py_compile ingestion_engine.py delivery_engine.py suppression_ledger.py && python -m pytest tests/ -x --tb=short -q`
Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "refactor(delivery): move merge semantics into SuppressionLedger"
```

---

## Final verification

- [ ] **Confirm all removed symbols are gone from the source tree:**

Run:
```bash
grep -nE "_SUPPRESSION_SAMPLES_CAP|_DELIVERY_SAMPLES_CAP|_INGESTION_SUPPRESSION_KEYS|_DELIVERY_SUPPRESSION_KEYS|_QA_REASON_LABELS|_record_suppression" ingestion_engine.py delivery_engine.py
```
Expected: no matches.

- [ ] **Confirm no orphan references in tests:**

Run:
```bash
grep -nE "_SUPPRESSION_SAMPLES_CAP|_DELIVERY_SAMPLES_CAP|_INGESTION_SUPPRESSION_KEYS|_DELIVERY_SUPPRESSION_KEYS|_QA_REASON_LABELS|_record_suppression" tests/
```
Expected: no matches.

- [ ] **Run the full suite a final time:**

```bash
python -m py_compile ingestion_engine.py delivery_engine.py suppression_ledger.py
python -m pytest tests/ -x --tb=short
```
Expected: all tests pass.

- [ ] **Run a manual diff review** of the full branch against `main` before opening a PR:

```bash
git diff main..HEAD --stat
git diff main..HEAD -- suppression_ledger.py ingestion_engine.py delivery_engine.py tests/
```

Expected: one new module, two engines stripped of suppression accounting internals, one new test file, no behavior change at the integration level.

---

## Success criteria summary

- [x] `suppression_ledger.py` introduced as pure in-process module (no Supabase imports).
- [x] Persisted shapes unchanged: `suppression_breakdown: dict[str, int]`, `suppression_samples: list[{reason,url,title}]`.
- [x] Ingestion and delivery both use the ledger.
- [x] Retry merge invariant covered by unit tests (Task 1.6) and one integration test (Task 4.1).
- [x] Old constants removed: `_SUPPRESSION_SAMPLES_CAP`, `_DELIVERY_SAMPLES_CAP`, `_INGESTION_SUPPRESSION_KEYS`, `_DELIVERY_SUPPRESSION_KEYS`, `_QA_REASON_LABELS`.
- [x] No Supabase I/O in the ledger (verifiable: `grep -n supabase suppression_ledger.py` returns empty).
