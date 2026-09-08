"""Relevance scoring — one home for the impact thresholds the report applies.

`insight.effective_impact` answers *what* an article's materiality score is.
This module answers *what that score means for the report*: is it visible, is it
weak-relevance supporting context, which alert tier, does a legacy row earn a
CRITICAL badge. Those cut-offs used to be scattered as magic numbers across
`generate_html_email`, `_apply_delivery_suppression`, `_render_meta_strip`, and
`_alert_tier` (3, 4–5, 6, 8); here they are named and resolved in one place.

Scope is the reporting relevance thresholds only. The suppression policy's
`enterprise_min_impact` stays with the suppression rules, and the legacy
directional sentiment *word* mapping stays with the renderer — neither is a
materiality threshold.

The visible / supporting thresholds are configurable
(`reporting.visible_impact_threshold` / `supporting_impact_threshold`); the
CRITICAL / STRATEGIC tier edges are fixed constants.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from insight import effective_impact

logger = logging.getLogger(__name__)

# Fixed tier edges (not configurable).
CRITICAL_MAX = 3      # effective impact <= 3 → CRITICAL tier / legacy critical badge
STRATEGIC_MIN = 8     # effective impact >= 8 → STRATEGIC tier

# Defaults when config is absent.
DEFAULT_VISIBLE = 6
DEFAULT_SUPPORTING = 4


def _coerce_int(cfg: dict, key: str, default: int) -> int:
    try:
        return int(cfg.get(key, default))
    except (TypeError, ValueError):
        logger.warning("Invalid %s %r in reporting config; using default %d.", key, cfg.get(key), default)
        return default


def _coerce_watch(cfg: dict, visible: int) -> Optional[int]:
    """reporting.watch_impact_threshold: absent / null / uninterpretable means
    no Watch band (the config-only rollback), and so does a value at or above
    the visible threshold — that band would be empty by construction, which is
    worth a warning rather than a silently missing section."""
    raw = cfg.get("watch_impact_threshold")
    if raw is None:
        return None
    try:
        watch = int(raw)
    except (TypeError, ValueError):
        logger.warning("Invalid watch_impact_threshold %r in reporting config; no Watch band.", raw)
        return None
    if watch >= visible:
        logger.warning(
            "watch_impact_threshold %d is not below visible_impact_threshold %d; no Watch band.",
            watch, visible,
        )
        return None
    return watch


@dataclass(frozen=True)
class Scoring:
    """Resolved reporting thresholds plus the predicates that read them."""

    visible: int = DEFAULT_VISIBLE
    supporting: int = DEFAULT_SUPPORTING
    # The Watch band's lower edge (reporting.watch_impact_threshold): rows in
    # [watch, visible) are the Watch List — shown with their So-What between
    # the cards and the appendix. None means no Watch band.
    watch: Optional[int] = None

    @classmethod
    def from_config(cls, config: dict) -> "Scoring":
        rep = (config or {}).get("reporting", {})
        visible = _coerce_int(rep, "visible_impact_threshold", DEFAULT_VISIBLE)
        return cls(
            visible=visible,
            supporting=_coerce_int(rep, "supporting_impact_threshold", DEFAULT_SUPPORTING),
            watch=_coerce_watch(rep, visible),
        )

    def is_visible(self, row: dict) -> bool:
        """True when the row clears the visible-card threshold."""
        return effective_impact(row) >= self.visible

    def is_weak_relevance(self, row: dict) -> bool:
        """True when the row is supporting context — above supporting, below visible."""
        return self.supporting <= effective_impact(row) < self.visible

    def is_watch(self, row: dict) -> bool:
        """True when the row sits in the Watch band — at or above the watch
        threshold, below visible. Always False when no band is configured."""
        return self.watch is not None and self.watch <= effective_impact(row) < self.visible


def tier(row: dict) -> str:
    """Alert tier label for a row: CRITICAL / STRATEGIC / ROUTINE."""
    impact = effective_impact(row)
    if impact <= CRITICAL_MAX:
        return "CRITICAL"
    if impact >= STRATEGIC_MIN:
        return "STRATEGIC"
    return "ROUTINE"


def is_legacy_critical(row: dict) -> bool:
    """True for a legacy row (no materiality score) flagged critical by low directional sentiment."""
    if row.get("americhem_impact_score") is not None:
        return False
    sentiment = row.get("sentiment_score")
    if sentiment is None:
        return False
    try:
        return int(sentiment) <= CRITICAL_MAX
    except (TypeError, ValueError):
        return False
