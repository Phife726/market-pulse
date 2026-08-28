"""The run budget — what one ingestion run may spend, as a frozen value
(see CONTEXT.md: **Run budget**, **Budget verdict**).

Pure: every verdict is a function of the budget's limits, the position-aware
concept demand, and the two numbers the loop passes (`elapsed`,
`scrapes_attempted`). No clock, no I/O — the loop owns the stopwatch. The
shipped-config headroom guard (real `targets.yaml`) lives in
`tests/test_pipeline.py` beside the other real-config invariants.
"""
from dataclasses import FrozenInstanceError
from functools import partial

import pytest

import run_budget
from run_budget import Proceed, RunBudget, SkipEntity, Stop
from tests.conftest import stub_target

# Small limits so the arithmetic in the assertions reads at a glance.
_budget = partial(RunBudget.for_targets, max_scrapes=10, deadline_seconds=100, tail_reserve_seconds=50)

TARGETS = [
    stub_target("concept_front", search_mode="concept", results_per_entity=4),  # ahead: 6
    stub_target("EntityA"),                                                     # ahead: 2
    stub_target("EntityB"),                                                     # ahead: 2
    stub_target("concept_tail", search_mode="concept"),                         # ahead: 2
]


# ---------------------------------------------------------------------------
# Construction — the position-aware concept demand
# ---------------------------------------------------------------------------

def test_for_targets_reads_the_module_limits_by_default():
    budget = RunBudget.for_targets([])
    assert budget.max_scrapes == run_budget.MAX_DAILY_SCRAPES
    assert budget.deadline_seconds == run_budget.PIPELINE_DEADLINE_SECONDS
    assert budget.tail_reserve_seconds == run_budget.TAIL_RESERVE_SECONDS


def test_concept_demand_ahead_is_the_suffix_sum_of_concept_demand():
    """Element i is the worst-case scrape demand of targets[i:]; entity
    targets contribute nothing, concept targets their own results_per_entity
    (a per-group override, not the global default)."""
    assert _budget(TARGETS).concept_demand_ahead == (6, 2, 2, 2, 0)


def test_concept_demand_ahead_is_zero_without_concept_targets():
    budget = _budget([stub_target("EntityA"), stub_target("EntityB")])
    assert budget.concept_demand_ahead == (0, 0, 0)


def test_anything_but_a_concept_target_is_an_entity():
    """One classification rule: `search_mode == "concept"` contributes demand;
    every other (or missing) mode is an entity the reserve may skip."""
    budget = _budget([
        stub_target("concept", search_mode="concept"),
        stub_target("entity"),
        stub_target("unknown", search_mode="hybrid"),
        {**stub_target("missing"), "search_mode": None},
    ])
    assert budget.entity_at == (False, True, True, True)


def test_budget_is_frozen():
    budget = _budget([])
    with pytest.raises(FrozenInstanceError):
        budget.max_scrapes = 1  # type: ignore[misc]


# ---------------------------------------------------------------------------
# The hard limits — shared by both checkpoints
# ---------------------------------------------------------------------------

def _at_entity_a(budget: RunBudget, *, elapsed: float, scrapes_attempted: int) -> Proceed | SkipEntity | Stop:
    return budget.before_target(
        elapsed=elapsed, scrapes_attempted=scrapes_attempted, target_index=1)


def _at_candidate(budget: RunBudget, *, elapsed: float, scrapes_attempted: int) -> Proceed | Stop:
    return budget.before_candidate(elapsed=elapsed, scrapes_attempted=scrapes_attempted)


@pytest.mark.parametrize("checkpoint", [_at_candidate, _at_entity_a])
@pytest.mark.parametrize("elapsed, scrapes_attempted, expected", [
    (99.9, 7, Proceed()),  # 7 < the slot reserve at EntityA (10 - 2)
    (100.0, 0, Stop("deadline")),
    (0.0, 10, Stop("scrape_cap")),
    (500.0, 50, Stop("deadline")),  # deadline reported when both are hit
])
def test_hard_limits_apply_at_both_checkpoints(checkpoint, elapsed, scrapes_attempted, expected):
    # No clock reserve, so only the hard limits can produce these verdicts.
    budget = _budget(TARGETS, tail_reserve_seconds=0)
    assert checkpoint(budget, elapsed=elapsed, scrapes_attempted=scrapes_attempted) == expected


def test_before_target_stops_at_the_scrape_cap_before_discovery():
    """A target at the cap must not spend discovery it cannot scrape — the
    cap fires here, before `discover_candidates`, not only per candidate."""
    budget = _budget(TARGETS)
    assert budget.before_target(elapsed=0.0, scrapes_attempted=10, target_index=3) == Stop("scrape_cap")


def test_before_target_hard_limits_take_precedence_over_the_reserve():
    budget = _budget(TARGETS)
    assert _at_entity_a(budget, elapsed=60.0, scrapes_attempted=10) == Stop("scrape_cap")


# ---------------------------------------------------------------------------
# The tail reserve — a start-of-target decision for entity targets
# ---------------------------------------------------------------------------

def test_before_candidate_never_applies_the_tail_reserve():
    """The reserve never cuts a started target: the candidate checkpoint
    ignores it even when the run is inside the reserve on both axes."""
    budget = _budget(TARGETS)
    assert _at_candidate(budget, elapsed=60.0, scrapes_attempted=8) == Proceed()


def test_before_target_skips_an_entity_when_slots_fall_to_the_demand_ahead():
    """At EntityA the reserve is the concept demand still AHEAD (2), not the
    total (6): concept_front has already run."""
    budget = _budget(TARGETS)
    assert _at_entity_a(budget, elapsed=0.0, scrapes_attempted=7) == Proceed()
    assert _at_entity_a(budget, elapsed=0.0, scrapes_attempted=8) == SkipEntity("scrape_slots")


def test_before_target_skips_an_entity_when_the_clock_falls_to_the_reserve():
    budget = _budget(TARGETS)
    assert _at_entity_a(budget, elapsed=49.9, scrapes_attempted=0) == Proceed()
    assert _at_entity_a(budget, elapsed=50.0, scrapes_attempted=0) == SkipEntity("wall_clock")


def test_before_target_reports_slots_when_both_reserves_are_hit():
    budget = _budget(TARGETS)
    assert _at_entity_a(budget, elapsed=60.0, scrapes_attempted=8) == SkipEntity("scrape_slots")


def test_before_target_never_skips_a_concept_target():
    """Concept targets are what the reserve protects: they run until a hard
    limit actually fires."""
    budget = _budget(TARGETS)
    assert budget.before_target(elapsed=60.0, scrapes_attempted=9, target_index=3) == Proceed()


def test_entity_with_no_concept_demand_ahead_runs_until_the_cap():
    """Nothing left to protect → the reserve threshold equals the cap."""
    budget = _budget([stub_target("EntityA")])
    assert budget.before_target(elapsed=0.0, scrapes_attempted=9, target_index=0) == Proceed()
    assert budget.before_target(elapsed=0.0, scrapes_attempted=10, target_index=0) == Stop("scrape_cap")
