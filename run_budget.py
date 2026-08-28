"""The run budget — what one ingestion run may spend, as a frozen value
(see CONTEXT.md: **Run budget**, **Budget verdict**).

Pure and time-agnostic: the loop in ``ingestion_engine.execute_pipeline``
owns the stopwatch and passes ``elapsed``; the value decides and never logs.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import accumulate
from typing import Optional

MAX_DAILY_SCRAPES = 180
PIPELINE_DEADLINE_SECONDS = 1800  # stop ingestion after 30 min to stay inside the 40-min CI limit
# The clock half of the tail reserve — sized for ~40+ scrape attempts at the
# observed ~9s each. The slot half is derived per run (concept_demand_ahead).
TAIL_RESERVE_SECONDS = 360


@dataclass(frozen=True)
class Proceed:
    """Inside every applicable limit — carry on."""


@dataclass(frozen=True)
class SkipEntity:
    """Don't *start* this entity target: the tail reserve is reached on
    `reason` (``scrape_slots`` / ``wall_clock``)."""
    reason: str


@dataclass(frozen=True)
class Stop:
    """A hard limit is reached (``deadline`` / ``scrape_cap``) — end the run."""
    reason: str


@dataclass(frozen=True)
class RunBudget:
    max_scrapes: int
    deadline_seconds: float
    tail_reserve_seconds: float
    # Suffix sums of concept scrape demand: element i is the worst-case demand
    # of targets[i:] (every concept target yields its full results_per_entity
    # as new scrapable URLs). Length len(targets) + 1; element 0 is the total.
    concept_demand_ahead: tuple[int, ...]
    # Whether targets[i] is an entity target — the ones the tail reserve skips.
    # Not derivable from the sums (a concept target may declare zero demand).
    entity_at: tuple[bool, ...]

    @classmethod
    def for_targets(
        cls,
        targets: list[dict],
        *,
        max_scrapes: int = MAX_DAILY_SCRAPES,
        deadline_seconds: float = PIPELINE_DEADLINE_SECONDS,
        tail_reserve_seconds: float = TAIL_RESERVE_SECONDS,
    ) -> RunBudget:
        """Classify each target once: a concept target contributes its
        results_per_entity to the demand ahead; anything else is an entity
        (`None` demand)."""
        demand = [
            int(t.get("results_per_entity", 0)) if t.get("search_mode") == "concept" else None
            for t in targets
        ]
        return cls(
            max_scrapes=max_scrapes,
            deadline_seconds=deadline_seconds,
            tail_reserve_seconds=tail_reserve_seconds,
            concept_demand_ahead=tuple(accumulate(reversed([d or 0 for d in demand]), initial=0))[::-1],
            entity_at=tuple(d is None for d in demand),
        )

    def _hard_limit(self, elapsed: float, scrapes_attempted: int) -> Optional[Stop]:
        if elapsed >= self.deadline_seconds:
            return Stop("deadline")
        if scrapes_attempted >= self.max_scrapes:
            return Stop("scrape_cap")
        return None

    def before_candidate(self, *, elapsed: float, scrapes_attempted: int) -> Proceed | Stop:
        """The per-candidate checkpoint: the hard limits only. The tail
        reserve never cuts a started target — only a hard limit does."""
        return self._hard_limit(elapsed, scrapes_attempted) or Proceed()

    def before_target(
        self, *, elapsed: float, scrapes_attempted: int, target_index: int,
    ) -> Proceed | SkipEntity | Stop:
        """The per-target checkpoint: the hard limits first (so a target at
        the cap never spends discovery it cannot scrape), then the tail
        reserve for entity targets — the slot reserve is only the concept
        demand still AHEAD of ``target_index``."""
        stop = self._hard_limit(elapsed, scrapes_attempted)
        if stop is not None:
            return stop
        if not self.entity_at[target_index]:
            return Proceed()
        if scrapes_attempted >= self.max_scrapes - self.concept_demand_ahead[target_index]:
            return SkipEntity("scrape_slots")
        if elapsed >= self.deadline_seconds - self.tail_reserve_seconds:
            return SkipEntity("wall_clock")
        return Proceed()
