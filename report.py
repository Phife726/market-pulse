"""Pure module owning the report model and report assembly.

The **report model** (`ReportModel`) is the plain-data seam between the two
halves of delivery: `assemble_report` (the decision pipeline: delivery
suppression -> visibility filter -> segment grouping -> per-segment cap ->
total cap -> weak-relevance accounting) produces it, and the pure renderer
(`renderer.render_report`) plus the daily_summaries write-back consume
it. Same species as `insight.py` / `scoring.py`: no I/O, no clock, no env
reads, and no imports of the repo or LLM seams — purity is pinned
structurally in tests/test_purity.py.

Rendering a model whose `synthesis` is empty IS the bullets-only fallback;
`delivery_engine.prepare_report` fills it via the LLM seam.
"""
import logging
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Literal, Optional

from rapidfuzz.fuzz import token_sort_ratio as _token_sort_ratio

from insight import (
    effective_impact as _effective_impact,
    commercial_segment as _commercial_segment_of,
    VALID_COMMERCIAL_SEGMENTS,
)
from scoring import Scoring
from prompts import LOW_EXPOSURE_TEMPLATE_PREFIXES
from macro_summary import MacroSummary
from suppression_ledger import SuppressionLedger

logger = logging.getLogger(__name__)


def _citation_display_map(bullets: Optional[list[dict]], sources: Optional[list[dict]],
                          signals: Optional[list[dict]] = None) -> dict[int, int]:
    """Map raw cited source id -> sequential display number (1..N), ordered by
    first appearance across bullets then macro-outlook signals. Only ids that
    have a matching source entry
    are numbered, so legacy rows (no executive_sources) yield an empty map."""
    src_ids = {s["id"] for s in (sources or []) if isinstance(s, dict) and "id" in s}
    order: list = []

    def _collect(items):
        for it in items or []:
            if not isinstance(it, dict):
                continue
            for cid in it.get("citation_source_ids") or []:
                if cid in src_ids and cid not in order:
                    order.append(cid)

    # Bullets first, then macro-outlook signals — one shared numbering space
    # across the executive summary, the macro section, and the Sources footer.
    _collect(bullets)
    _collect(signals)
    return {cid: n for n, cid in enumerate(order, start=1)}


@dataclass(frozen=True)
class CitationSet:
    """The email's single citation numbering space, as plain frozen data.

    Owns the bullets-then-signals ordering, the id -> display-number map, the
    id -> source lookup, and the order the Sources footer lists sources in.
    Built once per report (`from_summary`, via `assemble_report`) and carried on
    `ReportModel.citations`, so the executive summary, the Macroeconomic Outlook
    and the Sources footer share one numbering by construction rather than by
    three renderers deriving equivalent inputs.

    Falsy when nothing is numbered — the legacy identity: a row with no
    `executive_sources` (or no resolvable citation) renders no inline markers
    and no footer, exactly as before.

    Constructing one directly from a (sources, display_map) pair is the legacy
    door, kept for the leaf renderers whose pair-shaped signatures predate this
    type; `from_summary` is the only place the numbering rule is applied.
    """
    sources: tuple[dict, ...] = ()
    display_map: dict[int, int] = field(default_factory=dict)
    # Derived id -> source index. Not an input: it is the one place the
    # src_by_id lookup the renderers used to each rebuild now lives.
    _by_id: dict = field(init=False, repr=False, compare=False, default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "_by_id",
            {s["id"]: s for s in self.sources if isinstance(s, dict) and "id" in s},
        )

    def __bool__(self) -> bool:
        return bool(self.display_map)

    def display_number(self, cited_id) -> Optional[int]:
        """The 1-based display number for a cited source id, or None when the id
        resolves to no source (so the renderer omits it)."""
        return self.display_map.get(cited_id)

    def source(self, cited_id) -> dict:
        """The source entry for a cited id, or {} when there is none."""
        return self._by_id.get(cited_id) or {}

    def ordered(self) -> list[tuple[int, dict]]:
        """(display_number, source) pairs in footer order — 1..N."""
        return [(n, self.source(cid))
                for cid, n in sorted(self.display_map.items(), key=lambda kv: kv[1])]

    @classmethod
    def from_summary(cls, macro_summary: Optional[MacroSummary],
                     macro_outlook: Optional[dict] = None) -> "CitationSet":
        """The one numbering rule, applied to a stored daily_summaries row.

        `macro_outlook` is the *renderable* outlook — pass the report model's
        (already extracted, already segment-merged) one; omitted, the value's
        own unmapped one is used. Either way only signals the email actually
        shows get numbered, so the footer can never list a source no inline
        marker references. Display segment labels never participate in
        numbering, so both spellings yield the same numbers — and a row with no
        renderable outlook is None either way, so passing an extracted None is
        indistinguishable from omitting the argument.
        """
        summary = macro_summary or MacroSummary()
        if macro_outlook is None:
            macro_outlook = summary.outlook
        signals = (macro_outlook or {}).get("signals") or []
        return cls(summary.sources,
                   _citation_display_map(summary.bullets, summary.sources, signals))


EMPTY_CITATIONS = CitationSet()


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
    ledger: SuppressionLedger
    macro_summary: Optional[MacroSummary]
    synthesis: dict[str, str] = field(default_factory=dict)
    # Optional-discovery appendix: suppression-surviving rows at/above the
    # supporting threshold not shown as visible cards — the weak-relevance band
    # plus rows capped out of their segment. Never counted in surfaced_count.
    # Empty on no_news.
    additional_articles: tuple[dict, ...] = ()
    # Renderable Macroeconomic Outlook pulled from macro_summary — a dict with a
    # non-empty current_condition and >=1 signal, else None. None on no_news.
    macro_outlook: Optional[dict] = None
    # The email's one citation numbering space, covering the executive bullets
    # and the rendered macro-outlook signals. Empty on no_news and on legacy
    # rows with no cited sources.
    citations: CitationSet = EMPTY_CITATIONS

    @property
    def screened_count(self) -> Optional[int]:
        """The carried macro summary's recorded screened count (see
        CONTEXT.md: recorded counts) — derived, so it cannot drift from its
        one source; None when no row was found or the row records none."""
        return self.macro_summary.screened_count if self.macro_summary else None

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


def _appendix_exclude_categories(reporting_cfg: dict) -> frozenset[str]:
    """Resolve reporting.appendix_exclude_categories — the discovery
    categories (targets.yaml group keys, e.g. the macro_* groups) whose rows
    never reach the Additional Articles appendix (issue #73). Exact,
    case-sensitive group keys; absent / null / empty means no exclusion. A
    malformed value (not a list) warns and excludes nothing — a bad config
    must not silently shrink the appendix."""
    raw = reporting_cfg.get("appendix_exclude_categories")
    if raw is None:
        return frozenset()
    if not isinstance(raw, (list, tuple, set, frozenset)):
        logger.warning(
            "Invalid config value for reporting.appendix_exclude_categories "
            "(expected a list); excluding nothing"
        )
        return frozenset()
    return frozenset(str(c).strip() for c in raw if str(c).strip())


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


def _is_low_exposure_template(row: dict) -> bool:
    """True when the So-What opens with one of the RULE 6 honest low-exposure
    templates (`prompts.LOW_EXPOSURE_TEMPLATE_PREFIXES`). Case-insensitive,
    tolerant of leading whitespace/quotes; a So-What that merely contains the
    words is not a template."""
    text = row.get("americhem_impact") or ""
    if not isinstance(text, str):
        return False
    opener = text.lstrip().lstrip("\"'\u201c\u2018").casefold()
    return any(opener.startswith(prefix.casefold()) for prefix in LOW_EXPOSURE_TEMPLATE_PREFIXES)


def _apply_delivery_suppression(
    rows: list[dict],
    config: dict,
    scorer: Optional[Scoring] = None,
) -> tuple[list[dict], SuppressionLedger]:
    """Run the deterministic seven-rule guardrail over fetched rows.

    Returns (kept_rows, ledger). First matching rule wins; each suppressed
    row is counted once and contributes at most one sample (deduped).
    `scorer` supplies the visible threshold rule 1's template exemption reads;
    None resolves it from config.

    Rules 6/7 keep the first row seen of a duplicate pair. Rows arrive
    impact-desc, so a score-4 RULE 6 template row would precede — and
    displace — a score-3 segment-specific duplicate, before the appendix gets
    to rank templates last. Template rows are therefore processed after every
    non-template row (a duplicate contest never goes to a template); `kept`
    is returned in the caller's order so nothing downstream (first-seen
    section order, samples) sees the reordering.
    """
    sup_cfg = config.get("delivery_suppression") or {}
    scorer = scorer or Scoring.from_config(config)
    exempt_templates = bool(sup_cfg.get("enable_low_exposure_template_exemption", True))
    ledger = SuppressionLedger.for_delivery()
    kept_indexed: list[tuple[int, dict]] = []
    kept_headlines: list[str] = []
    ordered = sorted(enumerate(rows), key=lambda ir: _is_low_exposure_template(ir[1]))

    threshold = int(sup_cfg.get("headline_duplicate_threshold", 90))
    enterprise_min_impact = int(sup_cfg.get("enterprise_min_impact", 7))
    override_action = sup_cfg.get("job_posting_override_action", "Escalate to leadership")

    product_patterns   = sup_cfg.get("url_patterns_product_listing", [])
    job_patterns       = sup_cfg.get("url_patterns_job_posting", [])
    market_patterns    = sup_cfg.get("title_patterns_generic_market_report", [])
    color_terms        = sup_cfg.get("color_terms", [])
    plastics_terms     = sup_cfg.get("plastics_relevance_terms", [])

    for index, row in ordered:
        url = row.get("source_url", "") or ""
        headline = row.get("headline", "") or ""
        entities = row.get("entities_mentioned") or []
        entities_text = " ".join(str(e) for e in entities)
        action = row.get("recommended_action", "")

        # Rule 1: Enterprise / Cross-Segment with low impact. A RULE 6
        # low-exposure template row is exempt while it is below the visible
        # threshold (issue #65): the prompt binds those templates to the
        # supporting band on the promise that they reach the appendix, and an
        # adjacent-market row is Enterprise by construction. The exemption can
        # only ever add appendix rows — an over-scored template row at or
        # above visible is still rule-1 noise, never a card.
        if sup_cfg.get("enable_enterprise_low_impact", True):
            segment = _commercial_segment_of(row)
            score = int(row.get("americhem_impact_score") or 0)
            exempt = (
                exempt_templates
                and not scorer.is_visible(row)
                and _is_low_exposure_template(row)
            )
            if segment == "Enterprise / Cross-Segment" and score < enterprise_min_impact and not exempt:
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

        kept_indexed.append((index, row))
        if headline:
            kept_headlines.append(headline)

    kept = [row for _, row in sorted(kept_indexed, key=lambda ir: ir[0])]
    return kept, ledger


def _is_usable_additional_article(row: dict, scorer: Scoring) -> bool:
    """True when a row qualifies for the optional-discovery appendix: it scores
    at or above the supporting threshold — the weak-relevance band plus
    visible-band rows that lost their card slot to a cap — and carries a
    non-blank headline and source URL.

    Delivery-suppression survival and 'not already a visible card' are enforced
    by the caller (it selects from `kept` minus the final-group hashes)."""
    return (
        (scorer.is_weak_relevance(row) or scorer.is_visible(row))
        and bool((row.get("headline") or "").strip())
        and bool((row.get("source_url") or "").strip())
    )


def _parses_as_datetime(val: str) -> bool:
    """True when val is a parseable ISO-8601 datetime (tolerating a trailing Z).
    Pure — parsing reads no clock."""
    s = val.strip()
    try:
        datetime.fromisoformat(s[:-1] + "+00:00" if s.endswith("Z") else s)
        return True
    except (ValueError, TypeError):
        return False


def _appendix_recency_token(row: dict) -> str:
    """Recency sort token: published_at when it parses as a datetime, else
    created_at when it parses, else ''. ISO-8601 timestamptz strings sort
    lexicographically in chronological order, so descending string order is
    newest-first. The parse guard keeps a non-ISO scraped value (e.g.
    'Yesterday') from spuriously ranking above real dates — and matches the
    renderer, which already drops an unparseable published_at. No clock read."""
    for key in ("published_at", "created_at"):
        val = row.get(key)
        if isinstance(val, str) and val.strip() and _parses_as_datetime(val):
            return val.strip()
    return ""


def _partition_appendix_exclusions(
    kept: list[dict],
    final_hashes: set,
    scorer: Scoring,
    exclude_categories: frozenset[str],
) -> tuple[list[dict], list[dict]]:
    """Split suppression survivors into (eligible, excluded) for the appendix.

    `excluded` holds exactly the rows the appendix would otherwise have
    considered — usable appendix-band rows not shown as cards — whose
    `category` is in `exclude_categories` (issue #73: the macro_* groups feed
    the Macroeconomic Outlook, not headline rows). Rows hidden for another
    reason (below the band, a visible card) are never in `excluded`, so the
    ledger reason names the appendix decision alone. Order is preserved."""
    eligible: list[dict] = []
    excluded: list[dict] = []
    for row in kept:
        if (
            exclude_categories
            and (row.get("category") or "") in exclude_categories
            and row.get("url_hash") not in final_hashes
            and _is_usable_additional_article(row, scorer)
        ):
            excluded.append(row)
        else:
            eligible.append(row)
    return eligible, excluded


def _select_additional_articles(
    kept: list[dict],
    final_hashes: set,
    scorer: Scoring,
    cap: int,
) -> tuple[dict, ...]:
    """Pick the appendix rows from suppression survivors not shown as cards.

    Deterministic order (applied as stable sorts, least-significant first):
    url_hash asc -> normalized headline asc -> recency desc -> effective impact
    desc -> non-template before template. So cap overflow (score 6+) precedes
    every score-5, which precedes every score-4; ties break by recency then
    headline then hash — and every RULE 6 low-exposure template row
    (`_is_low_exposure_template`) ranks after every non-template row whatever
    its score, so template rows fill the appendix only when there is room and
    are the first the cap pushes out. Production data (2026-08-27) showed the
    cap binds daily and is decided within the score-4 tier by recency, where
    macro-group template rows — stored last, so newest — would otherwise
    displace segment-specific rows. Capped at `cap`."""
    pool = [
        r for r in kept
        if r.get("url_hash") not in final_hashes and _is_usable_additional_article(r, scorer)
    ]
    pool.sort(key=lambda r: ((r.get("headline") or "").strip().casefold(),
                             r.get("url_hash") or ""))
    pool.sort(key=_appendix_recency_token, reverse=True)
    pool.sort(key=lambda r: _effective_impact(r), reverse=True)
    pool.sort(key=_is_low_exposure_template)
    return tuple(pool[:cap])


def _group_by_commercial_segment(items: list[dict]) -> dict[str, list[dict]]:
    """Bucket items by commercial_segment; rows without one default to Enterprise / Cross-Segment."""
    from collections import defaultdict
    buckets: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        buckets[_commercial_segment_of(item)].append(item)
    return dict(buckets)


def _segment_display_map(reporting_cfg: dict) -> dict[str, str]:
    """Build the canonical-segment -> display-label lookup from
    reporting.segment_display_groups (display label -> [canonical labels]).

    Display-only: the LLM taxonomy and stored `commercial_segment` values stay
    granular; this collapses several canonical segments under one reader-facing
    header. Absent/empty/malformed config yields {} (no merge — a config-only
    rollback). Canonical labels that are not real segments
    (`insight.VALID_COMMERCIAL_SEGMENTS`) are ignored so a config typo cannot
    crash delivery."""
    raw = reporting_cfg.get("segment_display_groups")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, str] = {}
    for display_label, canonicals in raw.items():
        if not isinstance(display_label, str) or not isinstance(canonicals, (list, tuple)):
            continue
        for canon in canonicals:
            if isinstance(canon, str) and canon in VALID_COMMERCIAL_SEGMENTS:
                out[canon] = display_label
    return out


def _merge_display_groups(
    groups: dict[str, list[dict]],
    display_map: dict[str, str],
) -> dict[str, list[dict]]:
    """Collapse grouped rows onto their display labels, preserving first-seen
    order. A canonical segment absent from the map keeps its own name. Applied
    AFTER grouping and BEFORE the per-segment cap, so the cap governs the merged
    group (the combined section cannot balloon past the cap)."""
    if not display_map:
        return groups
    merged: dict[str, list[dict]] = {}
    for seg, arts in groups.items():
        label = display_map.get(seg, seg)
        merged.setdefault(label, []).extend(arts)
    return merged


def _with_display_segment(row: dict, display_map: dict[str, str]) -> dict:
    """Return `row` unchanged unless its commercial_segment maps to a distinct
    display label, in which case a shallow copy carrying the display label is
    returned (purity: the caller's input rows are never mutated). Used to make
    the Additional Articles appendix show the same merged header the visible
    cards do."""
    seg = row.get("commercial_segment")
    if not isinstance(seg, str):
        return row
    label = display_map.get(seg)
    if label is None or label == seg:
        return row
    return {**row, "commercial_segment": label}


def _map_signal_segments(signal: dict, display_map: dict[str, str]) -> dict:
    """Map a macro-outlook signal's affected_segments through the display map
    (deduping collisions, preserving order), returning a copy only when
    something changed. Validation stays canonical (it runs at ingestion in
    `macro_summary.validate_macro_outlook`); this is a render-consistency remap
    of the stored row, so it never touches the validation contract."""
    segs = signal.get("affected_segments")
    if not isinstance(segs, list):
        return signal
    mapped: list = []
    for s in segs:
        d = display_map.get(s, s) if isinstance(s, str) else s
        if d not in mapped:
            mapped.append(d)
    if mapped == segs:
        return signal
    return {**signal, "affected_segments": mapped}


def _display_mapped_outlook(
    outlook: Optional[dict],
    display_map: dict[str, str],
) -> Optional[dict]:
    """A renderable outlook with its affected_segments remapped to display
    labels. The defensive *read* of the stored row lives on MacroSummary; this
    adds only the segment display merge, which is report policy — the taxonomy
    and the ingestion-time validation stay canonical."""
    signals = (outlook or {}).get("signals")
    # Defensive even though MacroSummary.from_row guarantees a signals list:
    # the value is public, all-defaulted and frozen, so a hand-built one (or a
    # future producer) can present an outlook this never validated.
    if not outlook or not display_map or not isinstance(signals, list):
        return outlook
    return {**outlook,
            "signals": [_map_signal_segments(sig if isinstance(sig, dict) else {}, display_map)
                        for sig in signals]}


def assemble_report(
    rows: list[dict],
    macro_summary: Optional[MacroSummary] = None,
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
    kept, ledger = _apply_delivery_suppression(rows, config, scorer)

    # 2. Visibility filter.
    visible_pool = [r for r in kept if scorer.is_visible(r)]
    below_threshold_count = len(kept) - len(visible_pool)

    # 3. Group by commercial segment, then apply the display-only merge (e.g.
    #    the two ground-transportation segments -> "Transportation — Vehicles").
    #    The merge is post-grouping / pre-cap so the per-segment cap governs the
    #    combined section; the stored taxonomy stays granular (re-splittable with
    #    zero re-classification).
    display_map = _segment_display_map(reporting_cfg)
    groups_full = _merge_display_groups(
        _group_by_commercial_segment(visible_pool), display_map
    )

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

    final_hashes = {a.get("url_hash") for arts in groups.values() for a in arts}

    # 6. Optional-discovery appendix: suppression survivors at/above the
    #    supporting threshold not shown as cards — the weak-relevance band plus
    #    rows capped out of their segment. Does NOT alter surfaced_count.
    #    Rows from an excluded discovery category (the macro_* groups, issue
    #    #73) are kept out of the appendix — appendix-only: they were already
    #    a visible card if scored high enough, and they fed the outlook's
    #    source pack at ingestion — and the drop is recorded with samples.
    appendix_pool, appendix_excluded = _partition_appendix_exclusions(
        kept, final_hashes, scorer, _appendix_exclude_categories(reporting_cfg),
    )
    for row in appendix_excluded:
        ledger = ledger.record(
            "appendix_excluded_category",
            url=row.get("source_url") or "",
            title=row.get("headline") or "",
        )
    additional_articles = _select_additional_articles(
        appendix_pool, final_hashes, scorer, _max_additional_articles(reporting_cfg),
    )
    # Show the merged display label in the appendix segment column too, so the
    # reader sees one consistent header for the merged segment across cards and
    # appendix. Copy-on-write — the caller's rows are never mutated.
    additional_articles = tuple(
        _with_display_segment(r, display_map) for r in additional_articles
    )

    # 7. weak_relevance: weak-relevance rows (supporting..visible, exclusive)
    #    shown NOWHERE — neither a
    #    visible card nor the appendix (e.g. pushed out by the appendix cap).
    #    (below_impact_threshold above is deliberately broader: it counts every
    #    suppression-surviving below-visible row, including ones the appendix
    #    now displays — it describes the visible-card decision, not end hiding.)
    shown_hashes = final_hashes | {a.get("url_hash") for a in additional_articles}
    weak_relevance_count = sum(
        1 for r in kept
        if scorer.is_weak_relevance(r)
        and r.get("url_hash") not in shown_hashes
    )

    # 8. surfaced_count is the FINAL visible-card count (post-cap, post-grouping).
    surfaced_count = sum(len(arts) for arts in groups.values())

    ledger = (ledger
              .record_count("below_impact_threshold", below_threshold_count)
              .record_count("weak_relevance", weak_relevance_count))

    # 9. The citation set: one numbering space for the executive bullets and the
    #    outlook signals that will actually render, so every renderer reads the
    #    same numbers instead of deriving its own.
    macro_outlook = _display_mapped_outlook(
        macro_summary.outlook if macro_summary else None, display_map)
    citations = CitationSet.from_summary(macro_summary, macro_outlook)

    return ReportModel(
        variant="daily",
        groups=groups,
        surfaced_count=surfaced_count,
        ledger=ledger,
        macro_summary=macro_summary,
        additional_articles=additional_articles,
        macro_outlook=macro_outlook,
        citations=citations,
    )
