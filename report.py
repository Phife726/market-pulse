"""Pure module owning the report model and report assembly.

The **report model** (`ReportModel`) is the plain-data seam between the two
halves of delivery: `assemble_report` (the decision pipeline: delivery
suppression -> visibility filter -> segment grouping -> per-segment cap ->
total cap -> weak-relevance accounting) produces it, and the pure renderer
(`delivery_engine.render_report`) plus the daily_summaries write-back consume
it. Same species as `insight.py` / `scoring.py`: no I/O, no clock, no env
reads, and no imports of the repo or LLM seams — purity is enforced by the
import graph.

Rendering a model whose `synthesis` is empty IS the bullets-only fallback;
`delivery_engine.prepare_report` fills it via the LLM seam.
"""
import logging
from dataclasses import dataclass, field, replace
from typing import Literal, Optional

from rapidfuzz.fuzz import token_sort_ratio as _token_sort_ratio

from insight import (
    effective_impact as _effective_impact,
    commercial_segment as _commercial_segment_of,
)
from scoring import Scoring
from suppression_ledger import SuppressionLedger

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReportModel:
    """The assembled daily report as plain frozen data.

    Invariants (guaranteed by assemble_report):
    - variant == "no_news" iff the input rows were empty; suppressed-to-empty
      still yields "daily" (full chrome, zero cards, write-back still owed).
    - groups values are materiality-descending; per-segment and total caps hold
      when configured (a null / absent cap means show every visible article).
    - surfaced_count == sum(len(arts) for arts in groups.values()).
    - ledger is the complete delivery-side accounting, including the derived
      below_impact_threshold and weak_relevance counts — the write-back
      consumes it verbatim.
    """
    variant: Literal["daily", "no_news"]
    groups: dict[str, list[dict]]
    surfaced_count: int
    screened_count: int
    ledger: SuppressionLedger
    macro_summary: Optional[dict]
    synthesis: dict[str, str] = field(default_factory=dict)
    # Optional-discovery appendix: suppression-surviving score-4/5 rows not
    # shown as visible cards. Never counted in surfaced_count. Empty on no_news.
    additional_articles: tuple[dict, ...] = ()

    def synthesis_candidates(self) -> dict[str, list[dict]]:
        """Final capped groups with 2+ Insights — the only legal input to
        thematic synthesis."""
        return {seg: arts for seg, arts in self.groups.items() if len(arts) >= 2}

    def with_synthesis(self, synthesis: dict[str, str]) -> "ReportModel":
        return replace(self, synthesis=synthesis)


def _config_int(cfg: dict, key: str, default: int) -> int:
    """Read an int from a config sub-dict, coercing strings and warning on bad values."""
    try:
        return int(cfg.get(key, default))
    except (TypeError, ValueError):
        logger.warning("Invalid config value for reporting.%s; using %d", key, default)
        return default


DEFAULT_MAX_ADDITIONAL_ARTICLES = 10


def _max_additional_articles(reporting_cfg: dict) -> int:
    """Resolve the appendix cap (reporting.max_additional_articles, default 10).
    A report-assembly knob, read here beside the visible-card caps — not a
    scoring threshold."""
    return _config_int(reporting_cfg, "max_additional_articles", DEFAULT_MAX_ADDITIONAL_ARTICLES)


def _config_optional_int(cfg: dict, key: str) -> Optional[int]:
    """Read an optional cap from a config sub-dict.

    Returns None when the key is absent or explicitly null (meaning: no cap).
    An integer (or int-coercible string) returns that cap. A malformed value
    warns and falls back to None (uncapped) — a bad cap must not silently
    shrink the report."""
    if cfg.get(key) is None:
        return None
    try:
        return int(cfg[key])
    except (TypeError, ValueError):
        logger.warning("Invalid config value for reporting.%s; leaving uncapped", key)
        return None


def _matches_any_pattern(haystack: str, patterns: list[str]) -> bool:
    """Case-insensitive substring match: True if any pattern appears in haystack."""
    h = (haystack or "").lower()
    return any(p.lower() in h for p in patterns or [])


def _contains_any_term(text: str, terms: list[str]) -> bool:
    """True if any of `terms` appears in `text` (case-insensitive substring)."""
    t = (text or "").lower()
    return any(term.lower() in t for term in terms or [])


def _apply_delivery_suppression(
    rows: list[dict],
    config: dict,
) -> tuple[list[dict], SuppressionLedger]:
    """Run the deterministic seven-rule guardrail over fetched rows.

    Returns (kept_rows, ledger). First matching rule wins; each suppressed
    row is counted once and contributes at most one sample (deduped).
    """
    sup_cfg = config.get("delivery_suppression") or {}
    ledger = SuppressionLedger.for_delivery()
    kept: list[dict] = []
    kept_headlines: list[str] = []

    threshold = int(sup_cfg.get("headline_duplicate_threshold", 90))
    enterprise_min_impact = int(sup_cfg.get("enterprise_min_impact", 7))
    override_action = sup_cfg.get("job_posting_override_action", "Escalate to leadership")

    product_patterns   = sup_cfg.get("url_patterns_product_listing", [])
    job_patterns       = sup_cfg.get("url_patterns_job_posting", [])
    market_patterns    = sup_cfg.get("title_patterns_generic_market_report", [])
    color_terms        = sup_cfg.get("color_terms", [])
    plastics_terms     = sup_cfg.get("plastics_relevance_terms", [])

    for row in rows:
        url = row.get("source_url", "") or ""
        headline = row.get("headline", "") or ""
        entities = row.get("entities_mentioned") or []
        entities_text = " ".join(str(e) for e in entities)
        action = row.get("recommended_action", "")

        # Rule 1: Enterprise / Cross-Segment with low impact
        if sup_cfg.get("enable_enterprise_low_impact", True):
            segment = _commercial_segment_of(row)
            score = int(row.get("americhem_impact_score") or 0)
            if segment == "Enterprise / Cross-Segment" and score < enterprise_min_impact:
                ledger = ledger.record(
                    "enterprise_cross_segment_low_impact",
                    url=url,
                    title=headline,
                )
                continue

        # Rule 2: Product listing URL
        if sup_cfg.get("enable_product_listing", True) and _matches_any_pattern(url, product_patterns):
            ledger = ledger.record("product_listing", url=url, title=headline)
            continue

        # Rule 3: Job posting URL (unless escalated)
        if sup_cfg.get("enable_job_posting", True) and _matches_any_pattern(url, job_patterns):
            if action != override_action:
                ledger = ledger.record("job_posting", url=url, title=headline)
                continue

        # Rule 4: Generic market report title with empty entities
        if sup_cfg.get("enable_generic_market_report", True):
            if _matches_any_pattern(headline, market_patterns) and not entities:
                ledger = ledger.record("generic_market_report", url=url, title=headline)
                continue

        # Rule 5: Unrelated color result
        if sup_cfg.get("enable_unrelated_color_result", True):
            if _contains_any_term(headline, color_terms):
                # Check headline and entities only — not americhem_impact, which may
                # contain negating language like "No plastics relevance." that would
                # cause a false substring match on "plastic".
                relevance_haystack = f"{headline} {entities_text}"
                if not _contains_any_term(relevance_haystack, plastics_terms):
                    ledger = ledger.record("unrelated_color_result", url=url, title=headline)
                    continue

        # Rule 6: Exact duplicate headline (case-insensitive)
        if sup_cfg.get("enable_duplicate_headline", True):
            if headline and any(h.lower() == headline.lower() for h in kept_headlines):
                ledger = ledger.record("duplicate_headline", url=url, title=headline)
                continue

        # Rule 7: Semantic duplicate headline
        # Lowercase before comparison — rapidfuzz 3.x no longer auto-lowercases,
        # so case differences in proper nouns would otherwise deflate the score.
        if sup_cfg.get("enable_semantic_duplicate_headline", True) and kept_headlines and headline:
            hl_lower = headline.lower()
            scores = [_token_sort_ratio(hl_lower, h.lower()) for h in kept_headlines]
            if scores and max(scores) >= threshold:
                ledger = ledger.record("semantic_duplicate_headline", url=url, title=headline)
                continue

        kept.append(row)
        if headline:
            kept_headlines.append(headline)

    return kept, ledger


def _is_usable_additional_article(row: dict, scorer: Scoring) -> bool:
    """True when a row qualifies for the optional-discovery appendix: it is in
    the weak-relevance band (supporting <= impact < visible — the same band the
    rest of the report uses) and carries a non-blank headline and source URL.

    Delivery-suppression survival and 'not already a visible card' are enforced
    by the caller (it selects from `kept` minus the final-group hashes)."""
    return (
        scorer.is_weak_relevance(row)
        and bool((row.get("headline") or "").strip())
        and bool((row.get("source_url") or "").strip())
    )


def _appendix_recency_token(row: dict) -> str:
    """Recency sort token: published_at when present, else created_at, else ''.
    ISO-8601 timestamptz strings sort lexicographically in chronological order,
    so descending string order is newest-first — no clock read, fully pure."""
    for key in ("published_at", "created_at"):
        val = row.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def _select_additional_articles(
    kept: list[dict],
    final_hashes: set,
    scorer: Scoring,
    cap: int,
) -> tuple[dict, ...]:
    """Pick the appendix rows from suppression survivors not shown as cards.

    Deterministic order (applied as stable sorts, least-significant first):
    url_hash asc -> normalized headline asc -> recency desc -> effective impact
    desc. So every score-5 precedes every score-4, ties break by recency then
    headline then hash. Capped at `cap`."""
    pool = [
        r for r in kept
        if r.get("url_hash") not in final_hashes and _is_usable_additional_article(r, scorer)
    ]
    pool.sort(key=lambda r: ((r.get("headline") or "").strip().casefold(),
                             r.get("url_hash") or ""))
    pool.sort(key=_appendix_recency_token, reverse=True)
    pool.sort(key=lambda r: _effective_impact(r), reverse=True)
    return tuple(pool[:cap])


def _group_by_commercial_segment(items: list[dict]) -> dict[str, list[dict]]:
    """Bucket items by commercial_segment; rows without one default to Enterprise / Cross-Segment."""
    from collections import defaultdict
    buckets: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        buckets[_commercial_segment_of(item)].append(item)
    return dict(buckets)


def _resolve_screened_count(macro_summary: Optional[dict], rows: list[dict]) -> int:
    screened = (macro_summary or {}).get("screened_count")
    return len(rows) if screened is None else screened


def assemble_report(
    rows: list[dict],
    macro_summary: Optional[dict] = None,
    config: Optional[dict] = None,
) -> ReportModel:
    """Run the full decision pipeline over fetched Insight rows.

    Pure and deterministic. config is the parsed market_pulse_config.yaml dict;
    None means built-in defaults, never a file read. Never raises on malformed
    rows — field reads fall back exactly as the renderer's defensive reads did.
    """
    config = config or {}

    if not rows:
        return ReportModel(
            variant="no_news",
            groups={},
            surfaced_count=0,
            screened_count=_resolve_screened_count(macro_summary, rows),
            ledger=SuppressionLedger.for_delivery(),
            macro_summary=macro_summary,
        )

    reporting_cfg                   = config.get("reporting", {}) or {}
    # Caps are optional: None (null / absent key) means show every visible
    # article. An integer re-imposes the cap — a config-only rollback if the
    # report gets noisy.
    max_per_segment: Optional[int]  = _config_optional_int(reporting_cfg, "max_visible_articles_per_segment")
    max_total_visible: Optional[int] = _config_optional_int(reporting_cfg, "max_total_visible_articles")
    scorer = Scoring.from_config(config)

    # 1. Final guardrail suppression pass (delivery-side patterns + dedupe).
    kept, ledger = _apply_delivery_suppression(rows, config)

    # 2. Visibility filter.
    visible_pool = [r for r in kept if scorer.is_visible(r)]
    below_threshold_count = len(kept) - len(visible_pool)

    # 3. Group by commercial segment.
    groups_full = _group_by_commercial_segment(visible_pool)

    # 4. Per-segment cap (highest-impact articles first within each group).
    #    Sort even when uncapped so within-segment order stays materiality-desc.
    groups = {
        seg: sorted(arts, key=lambda x: _effective_impact(x), reverse=True)[:max_per_segment]
        for seg, arts in groups_full.items()
    }

    # 5. Total visible cap across all groups (drop lowest-impact until count <= cap).
    total = sum(len(arts) for arts in groups.values())
    if max_total_visible is not None and total > max_total_visible:
        all_visible = sorted(
            [(seg, a) for seg, arts in groups.items() for a in arts],
            key=lambda kv: _effective_impact(kv[1]),
            reverse=True,
        )[:max_total_visible]
        selected_hashes = {a.get("url_hash") for _, a in all_visible}
        groups = {seg: [a for a in arts if a.get("url_hash") in selected_hashes]
                  for seg, arts in groups.items()}
        groups = {seg: arts for seg, arts in groups.items() if arts}

    # 6. weak_relevance: rows in `kept` with effective impact 4-5 that didn't
    # make it into any final group (the old Peripheral pool, now hidden).
    final_hashes = {a.get("url_hash") for arts in groups.values() for a in arts}
    weak_relevance_count = sum(
        1 for r in kept
        if scorer.is_weak_relevance(r)
        and r.get("url_hash") not in final_hashes
    )

    # 7. surfaced_count is the FINAL visible-card count (post-cap, post-grouping).
    surfaced_count = sum(len(arts) for arts in groups.values())

    # 8. Optional-discovery appendix: suppression survivors in the weak-relevance
    #    band not shown as cards. Does NOT alter surfaced_count.
    additional_articles = _select_additional_articles(
        kept, final_hashes, scorer, _max_additional_articles(reporting_cfg),
    )

    ledger = (ledger
              .record_count("below_impact_threshold", below_threshold_count)
              .record_count("weak_relevance", weak_relevance_count))

    return ReportModel(
        variant="daily",
        groups=groups,
        surfaced_count=surfaced_count,
        screened_count=_resolve_screened_count(macro_summary, rows),
        ledger=ledger,
        macro_summary=macro_summary,
        additional_articles=additional_articles,
    )
