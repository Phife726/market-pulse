"""report.py — the pure report-assembly decision pipeline.

Every test here asserts on the `ReportModel` (or a step of `assemble_report`)
with dict literals and zero patches: delivery suppression, visibility, segment
grouping and the display merge, the caps, the appendix, the ledger fold, the
citation set. Tests that render the model to HTML live in
`test_delivery_engine.py` — a test lives with the module whose output it asserts on.
"""

from report import _config_int, assemble_report, _citation_display_map
from tests.conftest import (
    _APPENDIX_CFG,
    _VALID_MACRO_OUTLOOK,
    _appendix_hashes,
    _make_new_article,
    _src,
)


# ===========================================================================
# Threshold filtering in assemble_report()
# ===========================================================================


def test_report_macro_outlook_sliced_to_cap():
    """daily_summaries rows stored before the cap reduction may hold up to 6
    signals; assemble_report slices to MAX_MACRO_OUTLOOK_SIGNALS so QA
    re-renders (run_ingestion=false) comply immediately."""
    from prompts import MAX_MACRO_OUTLOOK_SIGNALS

    signals = [
        {
            "indicator": f"Indicator {i}",
            "direction": "Declining",
            "americhem_implication": "Downside risk for resin demand.",
            "affected_segments": ["Industrial"],
            "citation_source_ids": [1],
        }
        for i in range(6)
    ]
    macro_summary = {
        "macro_outlook": {"current_condition": "Manufacturing demand mixed.",
                          "signals": signals},
    }
    rows = [_make_new_article("a", 8, commercial_segment="Packaging",
                              headline="Packaging demand firms on brand-owner restocking")]
    model = assemble_report(rows, macro_summary=macro_summary)

    assert [s["indicator"] for s in model.macro_outlook["signals"]] == [
        "Indicator 0", "Indicator 1", "Indicator 2",
    ]
    assert len(model.macro_outlook["signals"]) == MAX_MACRO_OUTLOOK_SIGNALS


# ===========================================================================
# Article cap enforcement
# ===========================================================================


def test_assemble_report_per_segment_cap():
    """No more than max_per_segment articles from the same segment survive assembly."""
    # 5 Healthcare articles with genuinely distinct headlines (avoids semantic-duplicate
    # suppression which fires at token_sort_ratio >= 88).
    _hc_headlines = [
        "Hospital network merger squeezes specialty polymer volumes",
        "FDA clears new implantable-grade compound for cardiac devices",
        "Aging population drives record demand for medical-grade resins",
        "Generic drug expansion pressures premium plastics pricing",
        "Supply disruption at key resin plant delays surgical kit output",
    ]
    articles = [
        _make_new_article(
            f"h{i}", americhem_impact_score=10 - i,
            commercial_segment="Healthcare",
            headline=_hc_headlines[i],
        )
        for i in range(5)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 3,
            "max_total_visible_articles": 12,
        }
    }
    model = assemble_report(articles, config=config)

    # Top 3 by impact score (h0=10, h1=9, h2=8) survive; h3 (7) and h4 (6) are capped.
    assert [a["url_hash"] for a in model.groups["Healthcare"]] == ["h0", "h1", "h2"]
    assert model.surfaced_count == 3


def test_assemble_report_total_articles_cap():
    """Total visible articles must not exceed max_total_visible_articles."""
    # 7 segments × 2 articles = 14 articles, all impact=8
    segments = [
        "Healthcare", "Fibers", "Packaging", "Industrial",
        "Raw Materials / Supply Chain", "Regulatory / Sustainability",
        "Competitive / Customer Signal",
    ]
    articles = [
        _make_new_article(
            f"s{si}_{ai}", americhem_impact_score=8,
            commercial_segment=seg,
            headline=f"Seg{si} Art{ai}",
        )
        for si, seg in enumerate(segments)
        for ai in range(2)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 3,
            "max_total_visible_articles": 10,
        }
    }
    model = assemble_report(articles, config=config)

    assert model.surfaced_count <= 10
    assert sum(len(arts) for arts in model.groups.values()) == model.surfaced_count


# ===========================================================================
# Additional Articles appendix — model field and cap config
# ===========================================================================


def test_report_model_has_additional_articles_tuple():
    """ReportModel carries an additional_articles tuple; empty on the daily
    variant until selection lands, and always empty on no_news."""
    daily = assemble_report(
        [_make_new_article("a", 8, commercial_segment="Packaging",
                           headline="Packaging demand firms on brand-owner restocking")],
        config={"reporting": {"visible_impact_threshold": 6}},
    )
    assert isinstance(daily.additional_articles, tuple)

    no_news = assemble_report([], config={"reporting": {"visible_impact_threshold": 6}})
    assert no_news.variant == "no_news"
    assert no_news.additional_articles == ()


def test_max_additional_articles_default_is_ten():
    """The appendix cap resolves to 10 by default and honors an override — it is
    a report-assembly knob, read in report.py (not a scoring threshold)."""
    from report import _max_additional_articles
    assert _max_additional_articles({}) == 10
    assert _max_additional_articles({"max_additional_articles": 5}) == 5


def test_appendix_selects_scores_4_and_5_excludes_3_and_6():
    """Scores 4 and 5 populate the appendix; 3 is below the band; 6 stays a
    visible card and never duplicates into the appendix."""
    rows = [
        _make_new_article("s6", 6, commercial_segment="Packaging",
                          headline="Visible card at the six threshold holds firm"),
        _make_new_article("s5", 5, commercial_segment="Packaging",
                          headline="Near-threshold five signal worth optional reading"),
        _make_new_article("s4", 4, commercial_segment="Packaging",
                          headline="Marginal four signal for the curious reader"),
        _make_new_article("s3", 3, commercial_segment="Packaging",
                          headline="Below-band three signal should never appear"),
    ]
    model = assemble_report(rows, config=_APPENDIX_CFG)

    group_hashes = {a["url_hash"] for arts in model.groups.values() for a in arts}
    assert "s6" in group_hashes
    assert _appendix_hashes(model) == ["s5", "s4"]
    assert "s6" not in _appendix_hashes(model)
    assert "s3" not in _appendix_hashes(model)


def test_appendix_score_5_ranks_before_score_4():
    """Every score-5 item precedes every score-4 item regardless of insertion
    order."""
    rows = [
        _make_new_article("a4", 4, commercial_segment="Packaging",
                          headline="Alpha four ranked strictly after every five"),
        _make_new_article("b5", 5, commercial_segment="Packaging",
                          headline="Bravo five ranked ahead of any four signal"),
        _make_new_article("c4", 4, commercial_segment="Industrial",
                          headline="Charlie four also trails the five band"),
        _make_new_article("d5", 5, commercial_segment="Industrial",
                          headline="Delta five leads the near-threshold pack"),
    ]
    model = assemble_report(rows, config=_APPENDIX_CFG)
    scores = [a["americhem_impact_score"] for a in model.additional_articles]
    assert scores == [5, 5, 4, 4]


def test_appendix_excludes_blank_headline_or_url():
    """A weak-relevance row without a usable headline or source URL is excluded."""
    good = _make_new_article("good", 5, commercial_segment="Packaging",
                             headline="Usable near-threshold signal with a real link")
    blank_headline = _make_new_article("bh", 5, commercial_segment="Packaging",
                                       headline="   ")
    no_url = _make_new_article("nu", 5, commercial_segment="Packaging",
                               headline="Weak signal that lost its source url somehow")
    no_url["source_url"] = ""
    model = assemble_report([good, blank_headline, no_url], config=_APPENDIX_CFG)
    assert _appendix_hashes(model) == ["good"]


def test_appendix_excludes_delivery_suppressed_rows():
    """A product-listing URL is suppressed before eligibility, so it never
    reaches the appendix even at a qualifying score."""
    listing = _make_new_article("list", 5, commercial_segment="Packaging",
                                headline="Shop our new masterbatch color range online")
    listing["source_url"] = "https://vendor.com/product/masterbatch-blue"
    real = _make_new_article("real", 5, commercial_segment="Packaging",
                             headline="Genuine near-threshold packaging demand note")
    config = {
        "reporting": {"visible_impact_threshold": 6},
        "delivery_suppression": {"url_patterns_product_listing": ["/product/"]},
    }
    model = assemble_report([listing, real], config=config)
    assert _appendix_hashes(model) == ["real"]


def test_appendix_never_includes_non_template_enterprise_cross_segment_low_impact():
    """Pinned deliberate consequence: delivery suppression rule 1 drops
    Enterprise / Cross-Segment rows below enterprise_min_impact (7), so a
    score-5 cross-segment row with an ordinary So-What can never appear in
    the appendix. (The RULE 6 low-exposure templates are the one exemption —
    see the mirror test below.)"""
    cross = _make_new_article("cross", 5,
                              commercial_segment="Enterprise / Cross-Segment",
                              headline="Cross-segment corporate note below the bar")
    keep = _make_new_article("keep", 5, commercial_segment="Packaging",
                             headline="Packaging-specific near-threshold signal kept")
    model = assemble_report([cross, keep], config=_APPENDIX_CFG)
    assert _appendix_hashes(model) == ["keep"]


def test_appendix_includes_low_exposure_template_enterprise_rows():
    """Issue #65: a RULE 6 template row ('Adjacent market — …' / 'Limited
    direct exposure — …') is by construction Enterprise / Cross-Segment and
    scored in the supporting band; the prompt promises it reaches the
    appendix, so rule 1 must not drop it. It ranks below every higher-scoring
    row and never becomes a visible card."""
    adjacent = {**_make_new_article("adjacent", 3,
                                    commercial_segment="Enterprise / Cross-Segment",
                                    headline="Recycled PET bottle demand climbs in Europe"),
                "americhem_impact": "Adjacent market — no direct Americhem participation indicated."}
    limited = {**_make_new_article("limited", 4,
                                   commercial_segment="Enterprise / Cross-Segment",
                                   headline="Bank raises passive stake in Huntsman"),
               "americhem_impact": "Limited direct exposure — passive shareholding change only."}
    keep = _make_new_article("keep", 5, commercial_segment="Packaging",
                             headline="Packaging-specific near-threshold signal kept")
    config = {"reporting": {"visible_impact_threshold": 6, "supporting_impact_threshold": 3}}
    model = assemble_report([adjacent, limited, keep], config=config)
    assert _appendix_hashes(model) == ["keep", "limited", "adjacent"]
    assert model.surfaced_count == 0
    assert dict(model.ledger.breakdown).get("enterprise_cross_segment_low_impact", 0) == 0


_TEMPLATE_CFG = {"reporting": {"visible_impact_threshold": 6,
                               "supporting_impact_threshold": 3}}


def _template_row(url_hash, score, *, segment="Enterprise / Cross-Segment",
                  headline, created_at=None, opener="Limited direct exposure"):
    row = {**_make_new_article(url_hash, score, commercial_segment=segment, headline=headline),
           "americhem_impact": f"{opener} — {headline.lower()} only."}
    if created_at:
        row["created_at"] = created_at
    return row


def test_appendix_ranks_low_exposure_template_rows_after_every_non_template_row():
    """Template rows are optional reading of last resort: a score-4 template
    row ranks below a score-3 segment-specific row, whatever the segment or
    recency. (Production data: the cap binds daily and is decided within the
    score-4 tier by recency, where macro-group template rows — stored last,
    so newest — would otherwise win.)"""
    packaging3 = {**_make_new_article("pkg3", 3, commercial_segment="Packaging",
                                      headline="Barrier film converter adds a line"),
                  "created_at": "2026-08-27T10:30:00+00:00"}
    template4 = _template_row("tpl4", 4, headline="Bitcoin futures top eighty thousand",
                              created_at="2026-08-27T10:45:00+00:00")
    model = assemble_report([template4, packaging3], config=_TEMPLATE_CFG)
    assert _appendix_hashes(model) == ["pkg3", "tpl4"]


def test_appendix_ranks_template_rows_last_regardless_of_segment():
    """The rule keys on the So-What, not the segment: a Packaging row that
    admits limited exposure is also last-resort reading."""
    pkg_template = _template_row("pkgtpl", 5, segment="Packaging",
                                 headline="Pouch maker names a new regional director")
    industrial3 = _make_new_article("ind3", 3, commercial_segment="Industrial",
                                    headline="Rotomolder books winter capacity")
    model = assemble_report([pkg_template, industrial3], config=_TEMPLATE_CFG)
    assert _appendix_hashes(model) == ["ind3", "pkgtpl"]


def test_appendix_cap_pushes_template_rows_out_first():
    """On a full day the cap drops template rows before any non-template row —
    they fill the appendix only when there is room."""
    real = [
        {**_make_new_article("r1", 4, commercial_segment="Packaging",
                             headline="Recycled content mandate reshapes film sourcing"),
         "created_at": "2026-08-27T10:20:00+00:00"},
        {**_make_new_article("r2", 4, commercial_segment="Healthcare",
                             headline="Medical tubing extruder wins device contract"),
         "created_at": "2026-08-27T10:25:00+00:00"},
    ]
    template = _template_row("tpl", 4, headline="Dow edges lower as Nasdaq rises",
                             created_at="2026-08-27T10:50:00+00:00")
    config = {"reporting": {**_TEMPLATE_CFG["reporting"], "max_additional_articles": 2}}
    model = assemble_report([template, *real], config=config)
    assert _appendix_hashes(model) == ["r2", "r1"]
    assert dict(model.ledger.breakdown)["weak_relevance"] == 1


def test_appendix_template_rows_keep_impact_then_recency_order_among_themselves():
    rows = [
        _template_row("t3old", 3, headline="Zinzino chief sells shares",
                      created_at="2026-08-27T10:10:00+00:00"),
        _template_row("t4old", 4, headline="Honeywell leadership changes spark views",
                      created_at="2026-08-27T10:10:00+00:00"),
        _template_row("t4new", 4, headline="Tanker freight rates surge on tightness",
                      created_at="2026-08-27T10:50:00+00:00", opener="Adjacent market"),
    ]
    model = assemble_report(rows, config=_TEMPLATE_CFG)
    assert _appendix_hashes(model) == ["t4new", "t4old", "t3old"]


def test_appendix_capped_at_max():
    """No more than max_additional_articles rows enter the appendix."""
    headlines = [
        "Recycled content mandate reshapes flexible film sourcing",
        "Feedstock naphtha spread widens across Gulf Coast crackers",
        "Nonwoven wipes producer books capacity through winter",
        "Colorant supplier flags titanium dioxide allocation risk",
        "Automotive interior program shifts to bio-based softeners",
        "Carpet tile demand rebounds on office refurbishment cycle",
        "Barrier resin qualification opens new pouch applications",
        "Compounder adds twin-screw line for engineered grades",
        "Pigment dispersion lead times ease after port backlog clears",
        "Agricultural mulch film season starts with firmer pricing",
        "Wire jacketing compound tightens on copper build-out",
        "Medical tubing extruder wins implantable device contract",
    ]
    rows = [
        _make_new_article(f"x{i}", 5, commercial_segment="Packaging", headline=h)
        for i, h in enumerate(headlines)
    ]
    config = {"reporting": {"visible_impact_threshold": 6, "max_additional_articles": 10}}
    model = assemble_report(rows, config=config)
    assert len(model.additional_articles) == 10


def test_appendix_deterministic_recency_then_headline_then_hash():
    """Within a score band, order is recency desc (published_at, else
    created_at), then normalized headline asc, then url_hash asc."""
    newer = _make_new_article("z_hash", 5, commercial_segment="Packaging",
                              headline="Zulu newer signal by publication timestamp")
    newer["published_at"] = "2026-07-16T09:00:00+00:00"
    older = _make_new_article("a_hash", 5, commercial_segment="Packaging",
                              headline="Alpha older signal by publication timestamp")
    older["published_at"] = "2026-07-15T09:00:00+00:00"
    # Two undated rows tie on recency -> ordered by normalized headline, then hash.
    undated_b = _make_new_article("h2", 5, commercial_segment="Packaging",
                                  headline="Betamax undated near-threshold packaging note")
    undated_a = _make_new_article("h1", 5, commercial_segment="Packaging",
                                  headline="Anchor undated near-threshold packaging note")
    model = assemble_report([undated_b, older, undated_a, newer], config=_APPENDIX_CFG)
    # newer (dated) and older (dated) lead by recency desc; undated tie last,
    # ordered by headline (Anchor < Betamax).
    assert _appendix_hashes(model) == ["z_hash", "a_hash", "h1", "h2"]


def test_appendix_recency_ignores_unparseable_published_at():
    """A non-ISO published_at must not be used for recency: it falls back to
    created_at, so it can't spuriously outrank a real recent date. (Aligns the
    selector with the renderer, which already drops unparseable published_at.)"""
    garbage = _make_new_article("garbage", 5, commercial_segment="Packaging",
                                headline="Bogus timestamp near-threshold packaging note")
    garbage["published_at"] = "Yesterday"                       # unparseable
    garbage["created_at"] = "2026-07-10T00:00:00+00:00"         # real, older
    real_recent = _make_new_article("recent", 5, commercial_segment="Industrial",
                                    headline="Genuinely recent near-threshold industrial note")
    real_recent["published_at"] = "2026-07-15T00:00:00+00:00"   # real, newer
    model = assemble_report([garbage, real_recent], config=_APPENDIX_CFG)
    # real_recent (Jul 15) must lead; garbage falls back to created_at (Jul 10).
    assert _appendix_hashes(model) == ["recent", "garbage"]


def test_appendix_ranks_cap_overflow_ahead_of_weak_relevance():
    """Capped-out visible-band rows (impact >= 6) precede weak-relevance
    (4-5) rows in the appendix — the existing impact-desc sort, wider band."""
    articles = [
        _make_new_article("v0", 10, commercial_segment="Healthcare",
                          headline="Hospital network merger squeezes specialty polymer volumes"),
        _make_new_article("v1", 9, commercial_segment="Healthcare",
                          headline="FDA clears new implantable-grade compound for cardiac devices"),
        _make_new_article("v2", 7, commercial_segment="Healthcare",
                          headline="Aging population drives record demand for medical-grade resins"),
        _make_new_article("w0", 5, commercial_segment="Packaging",
                          headline="Beverage brands trial mono-material caps in European pilot"),
    ]
    config = {"reporting": {"visible_impact_threshold": 6,
                            "max_visible_articles_per_segment": 2}}
    model = assemble_report(articles, config=config)

    assert [a["url_hash"] for a in model.groups["Healthcare"]] == ["v0", "v1"]
    assert [a["url_hash"] for a in model.additional_articles] == ["v2", "w0"]


def test_appendix_overflow_does_not_alter_ledger_counts():
    """Capped-out rows are displayed, not suppressed: they never enter
    weak_relevance, and below_impact_threshold still counts only
    suppression-surviving below-visible rows."""
    articles = [
        _make_new_article("v0", 10, commercial_segment="Healthcare",
                          headline="Hospital network merger squeezes specialty polymer volumes"),
        _make_new_article("v1", 7, commercial_segment="Healthcare",
                          headline="FDA clears new implantable-grade compound for cardiac devices"),
        _make_new_article("w0", 4, commercial_segment="Packaging",
                          headline="Beverage brands trial mono-material caps in European pilot"),
    ]
    config = {"reporting": {"visible_impact_threshold": 6,
                            "max_visible_articles_per_segment": 1}}
    model = assemble_report(articles, config=config)

    # w0 is the only below-visible survivor; v1 (visible-band, capped) is not counted.
    assert model.ledger.breakdown["below_impact_threshold"] == 1
    # w0 is shown in the appendix, so it is not "shown nowhere".
    assert model.ledger.breakdown.get("weak_relevance", 0) == 0
    assert model.surfaced_count == 1
    assert [a["url_hash"] for a in model.additional_articles] == ["v1", "w0"]


_APPENDIX_ACCT_HEADLINES = [
    "Recycled content mandate reshapes flexible film sourcing",
    "Feedstock naphtha spread widens across Gulf Coast crackers",
    "Nonwoven wipes producer books capacity through winter",
    "Colorant supplier flags titanium dioxide allocation risk",
    "Automotive interior program shifts to bio-based softeners",
    "Carpet tile demand rebounds on office refurbishment cycle",
    "Barrier resin qualification opens new pouch applications",
    "Compounder adds twin-screw line for engineered grades",
    "Pigment dispersion lead times ease after port backlog clears",
    "Agricultural mulch film season starts with firmer pricing",
    "Wire jacketing compound tightens on copper build-out",
    "Medical tubing extruder wins implantable device contract",
]


def test_appendix_displayed_rows_not_counted_weak_relevance():
    """A score-5 row shown in the appendix is not counted as weak_relevance."""
    row = _make_new_article("shown", 5, commercial_segment="Packaging",
                            headline="Near-threshold packaging note shown in appendix")
    model = assemble_report([row], config=_APPENDIX_CFG)
    assert _appendix_hashes(model) == ["shown"]
    # record_count is a no-op at 0, so the key is simply absent.
    assert model.ledger.breakdown.get("weak_relevance", 0) == 0


def test_appendix_capped_out_rows_counted_weak_relevance():
    """Eligible score-5 rows pushed out by the appendix cap are counted as
    weak_relevance (in neither the main groups nor the appendix)."""
    rows = [
        _make_new_article(f"w{i}", 5, commercial_segment="Packaging", headline=h)
        for i, h in enumerate(_APPENDIX_ACCT_HEADLINES)  # 12 rows
    ]
    config = {"reporting": {"visible_impact_threshold": 6, "max_additional_articles": 10}}
    model = assemble_report(rows, config=config)
    assert len(model.additional_articles) == 10
    assert model.ledger.breakdown["weak_relevance"] == 2


def test_below_impact_threshold_unchanged_by_appendix():
    """below_impact_threshold still counts every suppression-surviving row below
    the visible threshold, including rows the appendix now displays."""
    rows = [
        _make_new_article("s5", 5, commercial_segment="Packaging",
                          headline="Five-band signal that lands in the appendix"),
        _make_new_article("s4", 4, commercial_segment="Packaging",
                          headline="Four-band signal that also lands in appendix"),
        _make_new_article("s3", 3, commercial_segment="Packaging",
                          headline="Three-band signal below the supporting floor"),
    ]
    model = assemble_report(rows, config=_APPENDIX_CFG)
    # All three are below the visible threshold (6) and survive suppression.
    assert model.ledger.breakdown["below_impact_threshold"] == 3
    # Two of them are surfaced in the appendix — that overlap is intentional.
    assert len(model.additional_articles) == 2


# ===========================================================================
# Appendix-only category exclusion (macro-group rows, issue #73)
# ===========================================================================


_EXCLUDE_CFG = {"reporting": {"visible_impact_threshold": 6,
                              "supporting_impact_threshold": 3,
                              "appendix_exclude_categories": ["macro_inflation_rates",
                                                              "macro_energy_freight"]}}


def _macro_row(url_hash, score, *, category="macro_inflation_rates", headline,
               segment="Enterprise / Cross-Segment"):
    return {**_make_new_article(url_hash, score, commercial_segment=segment, headline=headline),
            "category": category,
            "americhem_impact": f"Limited direct exposure — {headline.lower()} commentary only."}


def test_appendix_excludes_configured_categories():
    """Issue #73: rows discovered by a configured category (the macro_* groups)
    never reach Additional Articles, whatever their score in the band — they
    exist to feed the Macroeconomic Outlook, not to be headline rows."""
    macro = _macro_row("macro", 4, headline="Lower energy prices fail to fix inflation pressures")
    keep = _make_new_article("keep", 3, commercial_segment="Packaging",
                             headline="Barrier film converter adds a line")
    model = assemble_report([macro, keep], config=_EXCLUDE_CFG)
    assert _appendix_hashes(model) == ["keep"]


def test_appendix_exclusion_matches_category_exactly_not_by_prefix():
    """The list is exact group keys: an unlisted macro_* group (or a
    look-alike) is not excluded — the config↔targets parity test is what
    keeps the list complete."""
    listed = _macro_row("listed", 4, category="macro_energy_freight",
                        headline="Russian tanker freight rates surge")
    unlisted = _macro_row("unlisted", 4, category="macro_automotive",
                          headline="Light-vehicle sales pace slows in July")
    model = assemble_report([listed, unlisted], config=_EXCLUDE_CFG)
    assert _appendix_hashes(model) == ["unlisted"]


def test_appendix_exclusion_is_recorded_in_the_ledger_with_samples():
    """The drop is accounted for: one count per excluded appendix-band row,
    with samples, under the delivery-owned reason — so the
    daily_summaries.suppression_breakdown write-back shows it."""
    m1 = _macro_row("m1", 4, headline="Lower energy prices fail to fix inflation pressures")
    m2 = _macro_row("m2", 3, category="macro_energy_freight",
                    headline="Russian tanker freight rates surge")
    keep = _make_new_article("keep", 5, commercial_segment="Packaging",
                             headline="Packaging-specific near-threshold signal kept")
    model = assemble_report([m1, m2, keep], config=_EXCLUDE_CFG)
    assert dict(model.ledger.breakdown).get("appendix_excluded_category") == 2
    sampled = {s.url for s in model.ledger.samples if s.reason == "appendix_excluded_category"}
    assert sampled == {m1["source_url"]}  # both rows share the fixture URL; deduped by (reason, url, title)
    titles = {s.title for s in model.ledger.samples if s.reason == "appendix_excluded_category"}
    assert titles == {m1["headline"], m2["headline"]}


def test_appendix_exclusion_never_touches_visible_cards():
    """Appendix-only: a macro-group row scored at/above the visible threshold
    is still a visible card and is not counted under the new reason."""
    macro6 = {**_macro_row("macro6", 7, headline="Fed cuts rates; resin buyers expect cheaper credit",
                           segment="Building & Construction"),
              "americhem_impact": "Cheaper credit lifts housing starts and construction resin pull-through."}
    model = assemble_report([macro6], config=_EXCLUDE_CFG)
    group_hashes = {a["url_hash"] for arts in model.groups.values() for a in arts}
    assert group_hashes == {"macro6"}
    assert "appendix_excluded_category" not in dict(model.ledger.breakdown)


def test_appendix_exclusion_only_counts_rows_that_would_have_been_appendix_rows():
    """A macro-group row already hidden for another reason (below the
    supporting band, or dropped by delivery suppression) is not double-counted
    under the new reason — the reason names the appendix decision only."""
    below_band = _macro_row("low", 2, headline="Crypto index drifts lower on thin volume")
    non_template_cross = {**_macro_row("cross", 4, headline="Bitcoin futures top eighty thousand"),
                          "americhem_impact": "Ordinary So-What, so rule 1 drops it first."}
    model = assemble_report([below_band, non_template_cross], config=_EXCLUDE_CFG)
    assert "appendix_excluded_category" not in dict(model.ledger.breakdown)
    assert dict(model.ledger.breakdown).get("enterprise_cross_segment_low_impact") == 1


def test_appendix_exclusion_absent_or_malformed_config_excludes_nothing():
    """Config-only rollback: no key, an empty list, or a malformed value means
    no exclusion (a bad config must not silently shrink the appendix)."""
    macro = _macro_row("macro", 4, headline="Lower energy prices fail to fix inflation pressures")
    for reporting in ({}, {"appendix_exclude_categories": None},
                      {"appendix_exclude_categories": []},
                      {"appendix_exclude_categories": "macro_inflation_rates"},
                      {"appendix_exclude_categories": 42}):
        cfg = {"reporting": {"visible_impact_threshold": 6, "supporting_impact_threshold": 3,
                             **reporting}}
        model = assemble_report([macro], config=cfg)
        assert _appendix_hashes(model) == ["macro"], reporting
        assert "appendix_excluded_category" not in dict(model.ledger.breakdown)


def test_appendix_excluded_rows_still_count_as_weak_relevance():
    """An excluded weak-band row is shown nowhere, so it stays inside the
    broader weak_relevance / below_impact_threshold counts — the new reason
    explains the hiding, it does not replace the existing accounting."""
    macro = _macro_row("macro", 4, headline="Lower energy prices fail to fix inflation pressures")
    model = assemble_report([macro], config=_EXCLUDE_CFG)
    breakdown = dict(model.ledger.breakdown)
    assert breakdown.get("weak_relevance") == 1
    assert breakdown.get("below_impact_threshold") == 1
    assert breakdown.get("appendix_excluded_category") == 1


def test_report_model_carries_macro_outlook():
    row = _make_new_article("v", 8, commercial_segment="Packaging",
                            headline="Visible packaging card to make a daily model")
    macro = {"dominant_condition": "Demand Softness", "macro_outlook": _VALID_MACRO_OUTLOOK}
    model = assemble_report([row], macro_summary=macro, config=_APPENDIX_CFG)
    assert model.macro_outlook == _VALID_MACRO_OUTLOOK


def test_report_model_macro_outlook_none_when_absent():
    row = _make_new_article("v", 8, commercial_segment="Packaging",
                            headline="Visible packaging card with no macro outlook")
    model = assemble_report([row], macro_summary={"dominant_condition": "Mixed / Watch"},
                            config=_APPENDIX_CFG)
    assert model.macro_outlook is None


def test_report_model_macro_outlook_none_when_malformed():
    row = _make_new_article("v", 8, commercial_segment="Packaging",
                            headline="Visible packaging card with malformed outlook")
    for bad in ({}, {"current_condition": "x", "signals": []},
                {"current_condition": "  ", "signals": [{"indicator": "PMI"}]},
                {"signals": [{"indicator": "PMI"}]}, "nope", None):
        model = assemble_report([row], macro_summary={"macro_outlook": bad},
                                config=_APPENDIX_CFG)
        assert model.macro_outlook is None, bad


def test_report_model_no_news_macro_outlook_none():
    model = assemble_report([], macro_summary={"macro_outlook": _VALID_MACRO_OUTLOOK},
                            config=_APPENDIX_CFG)
    assert model.variant == "no_news"
    assert model.macro_outlook is None


# ===========================================================================
# Uncapped-by-default report: caps are optional knobs (null / absent = no cap)
# ===========================================================================


_UNCAPPED_HC_HEADLINES = [
    "Hospital network merger squeezes specialty polymer volumes",
    "FDA clears new implantable-grade compound for cardiac devices",
    "Aging population drives record demand for medical-grade resins",
    "Generic drug expansion pressures premium plastics pricing",
    "Supply disruption at key resin plant delays surgical kit output",
]


def test_assemble_report_uncapped_per_segment_when_null():
    """With max_visible_articles_per_segment: null, every visible article in a
    segment survives — no per-segment drop."""
    articles = [
        _make_new_article(
            f"h{i}", americhem_impact_score=10 - i,
            commercial_segment="Healthcare",
            headline=_UNCAPPED_HC_HEADLINES[i],
        )
        for i in range(5)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": None,
            "max_total_visible_articles": None,
        }
    }
    model = assemble_report(articles, config=config)
    assert len(model.groups["Healthcare"]) == 5
    assert model.surfaced_count == 5


def test_assemble_report_uncapped_total_when_null():
    """With max_total_visible_articles: null, all visible articles across
    segments survive — no total drop."""
    # 14 genuinely distinct headlines (semantic-duplicate suppression fires at
    # token_sort_ratio >= 88, so near-identical headlines would collapse).
    specs = [
        ("Healthcare", "Hospital merger reshapes specialty polymer procurement"),
        ("Healthcare", "FDA clears implantable-grade compound for cardiac devices"),
        ("Fibers", "Nonwoven hygiene demand lifts polypropylene fiber orders"),
        ("Fibers", "Carpet mill restart tightens solution-dyed yarn supply"),
        ("Packaging", "Brand owners accelerate recyclable flexible film pledges"),
        ("Packaging", "Food-contact resin shortage delays beverage closures"),
        ("Industrial", "Wire-and-cable buildout drives jacketing compound volumes"),
        ("Industrial", "Agricultural film season opens with firmer additive pricing"),
        ("Transportation - Automotive", "EV interior programs shift to flame-retardant grades"),
        ("Transportation - Automotive", "Tier-one supplier books record under-hood resin demand"),
        ("Transportation - Aerospace", "Rotorcraft OEM qualifies new flame-rated cabin polymer"),
        ("Transportation - Aerospace", "Defense procurement lifts high-temperature composite orders"),
        ("Engineered Resins", "PEEK capacity expansion eases medical-device lead times"),
        ("Engineered Resins", "Glass-filled nylon pricing climbs on feedstock tightness"),
    ]
    articles = [
        _make_new_article(
            f"u{i}", americhem_impact_score=8,
            commercial_segment=seg, headline=headline,
        )
        for i, (seg, headline) in enumerate(specs)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": None,
            "max_total_visible_articles": None,
        }
    }
    model = assemble_report(articles, config=config)
    assert model.surfaced_count == 14


def test_assemble_report_uncapped_by_default():
    """Built-in defaults (config=None) impose no caps: all 5 visible articles
    in one segment survive."""
    articles = [
        _make_new_article(
            f"h{i}", americhem_impact_score=8,
            commercial_segment="Healthcare",
            headline=_UNCAPPED_HC_HEADLINES[i],
        )
        for i in range(5)
    ]
    model = assemble_report(articles, config=None)
    assert len(model.groups["Healthcare"]) == 5


def test_assemble_report_integer_cap_still_enforced():
    """An integer cap in config still caps — the knob is retained for rollback."""
    articles = [
        _make_new_article(
            f"h{i}", americhem_impact_score=10 - i,
            commercial_segment="Healthcare",
            headline=_UNCAPPED_HC_HEADLINES[i],
        )
        for i in range(5)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 3,
            "max_total_visible_articles": 12,
        }
    }
    model = assemble_report(articles, config=config)
    assert [a["url_hash"] for a in model.groups["Healthcare"]] == ["h0", "h1", "h2"]
    assert model.surfaced_count == 3


# ===========================================================================
# _config_int coercion
# ===========================================================================


def test_config_int_returns_int_for_numeric_value():
    cfg = {"visible_impact_threshold": 7}
    assert _config_int(cfg, "visible_impact_threshold", 6) == 7


def test_config_int_coerces_string_to_int():
    """YAML authors may quote numbers; ensure we still get an int."""
    cfg = {"visible_impact_threshold": "8"}
    assert _config_int(cfg, "visible_impact_threshold", 6) == 8


def test_config_int_returns_default_for_missing_key():
    assert _config_int({}, "visible_impact_threshold", 6) == 6


def test_config_int_returns_default_and_warns_for_bad_value(caplog):
    import logging
    cfg = {"visible_impact_threshold": "high"}
    with caplog.at_level(logging.WARNING, logger="delivery_engine"):
        result = _config_int(cfg, "visible_impact_threshold", 6)
    assert result == 6
    assert "visible_impact_threshold" in caplog.text


# ===========================================================================
# _apply_delivery_suppression()
# ===========================================================================


def _supp_config(**overrides) -> dict:
    """Default delivery_suppression config for tests; overrides applied on top."""
    base = {
        "enable_duplicate_headline": True,
        "enable_semantic_duplicate_headline": True,
        "headline_duplicate_threshold": 90,
        "enable_product_listing": True,
        "enable_job_posting": True,
        "job_posting_override_action": "Escalate to leadership",
        "enable_generic_market_report": True,
        "enable_unrelated_color_result": True,
        "enable_enterprise_low_impact": True,
        "enterprise_min_impact": 7,
        "url_patterns_product_listing": ["/product/", "amazon.com"],
        "url_patterns_job_posting": ["linkedin.com/jobs", "/careers/"],
        "title_patterns_generic_market_report": ["market size", "market report"],
        "color_terms": ["color", "colour"],
        "plastics_relevance_terms": ["plastic", "polymer", "masterbatch", "colorant"],
    }
    base.update(overrides)
    return {"delivery_suppression": base}


def _row(**overrides) -> dict:
    base = {
        "url_hash": overrides.get("url_hash", "abc"),
        "source_url": "https://example.com/article",
        "headline": "Default Headline",
        "americhem_impact": "Effect.",
        "americhem_impact_score": 8,
        "sentiment_tag": "Neutral",
        "commercial_segment": "Healthcare",
        "signal_type": "Customer",
        "entities_mentioned": ["Acme"],
        "recommended_action": "Monitor",
    }
    base.update(overrides)
    return base


def test_apply_delivery_suppression_drops_enterprise_low_impact():
    from report import _apply_delivery_suppression
    rows = [_row(commercial_segment="Enterprise / Cross-Segment", americhem_impact_score=5)]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"enterprise_cross_segment_low_impact": 1}
    assert ledger.samples[0].to_dict()["reason"] == "enterprise_cross_segment_low_impact"


def test_apply_delivery_suppression_keeps_enterprise_high_impact():
    from report import _apply_delivery_suppression
    rows = [_row(commercial_segment="Enterprise / Cross-Segment", americhem_impact_score=8)]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {}


_ENTERPRISE = "Enterprise / Cross-Segment"


def test_rule1_exempts_low_exposure_template_rows_below_visible():
    """Issue #65: both RULE 6 templates skip rule 1 while below the visible
    threshold, so the 3–4 band the prompt promises actually reaches the appendix."""
    from report import _apply_delivery_suppression
    rows = [
        _row(url_hash="a", commercial_segment=_ENTERPRISE, americhem_impact_score=3,
             headline="Recycled PET bottle demand climbs in Europe",
             americhem_impact="Adjacent market — no direct Americhem participation indicated."),
        _row(url_hash="b", commercial_segment=_ENTERPRISE, americhem_impact_score=4,
             headline="Bank raises passive stake in Huntsman",
             americhem_impact="Limited direct exposure — passive shareholding change only."),
    ]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert [r["url_hash"] for r in kept] == ["a", "b"]
    assert dict(ledger.breakdown) == {}


def test_rule1_still_drops_template_rows_at_or_above_visible():
    """The exemption can add appendix rows but must never mint a visible card:
    an over-scored template row (the model ignored 'never above 4') is still
    rule-1 noise."""
    from report import _apply_delivery_suppression
    rows = [_row(commercial_segment=_ENTERPRISE, americhem_impact_score=6,
                 americhem_impact="Limited direct exposure — one plant, one grade.")]
    config = {**_supp_config(), "reporting": {"visible_impact_threshold": 6}}
    kept, ledger = _apply_delivery_suppression(rows, config)
    assert kept == []
    assert dict(ledger.breakdown) == {"enterprise_cross_segment_low_impact": 1}


def test_rule1_exemption_follows_the_configured_visible_threshold():
    """Raise visible_impact_threshold to 7 and the score-6 template row is
    below visible again, so it is exempt (it lands in the appendix)."""
    from report import _apply_delivery_suppression
    rows = [_row(commercial_segment=_ENTERPRISE, americhem_impact_score=6,
                 americhem_impact="Limited direct exposure — one plant, one grade.")]
    config = {**_supp_config(), "reporting": {"visible_impact_threshold": 7}}
    kept, _ = _apply_delivery_suppression(rows, config)
    assert len(kept) == 1


def test_rule1_exemption_matches_prefix_case_insensitively_past_leading_quote():
    from report import _apply_delivery_suppression
    rows = [_row(commercial_segment=_ENTERPRISE, americhem_impact_score=3,
                 americhem_impact=' "adjacent market: outside every served segment."')]
    kept, _ = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1


def test_rule1_exemption_ignores_non_template_so_whats():
    """A So-What that merely *contains* template words, or starts with a
    near-miss, is not a template — rule 1 still applies."""
    from report import _apply_delivery_suppression
    rows = [
        _row(url_hash="a", commercial_segment=_ENTERPRISE, americhem_impact_score=3,
             headline="Naphtha cracker margins narrow in Asia",
             americhem_impact="Adjacent to Americhem's feedstock chain — no direct effect."),
        _row(url_hash="b", commercial_segment=_ENTERPRISE, americhem_impact_score=4,
             headline="Chemical distributor reports flat quarter",
             americhem_impact="Margin pressure; limited direct exposure otherwise."),
        _row(url_hash="c", commercial_segment=_ENTERPRISE, americhem_impact_score=4,
             headline="Regional resin trader opens new warehouse",
             americhem_impact=""),
    ]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"enterprise_cross_segment_low_impact": 3}


def test_duplicate_headline_contest_never_goes_to_a_template_row():
    """Codex review on PR #71: rows arrive impact-desc, so a score-4 template
    row precedes a score-3 segment-specific duplicate and would win rules 6/7,
    displacing it before the appendix ranks templates last. Dedup must prefer
    the non-template row whatever the input order."""
    from report import _apply_delivery_suppression
    template = _row(url_hash="tpl", commercial_segment=_ENTERPRISE, americhem_impact_score=4,
                    source_url="https://a.example/tpl",
                    headline="Recycled PET bottle demand climbs in Europe",
                    americhem_impact="Adjacent market — no direct Americhem participation indicated.")
    real = _row(url_hash="pkg", commercial_segment="Packaging", americhem_impact_score=3,
                source_url="https://b.example/pkg",
                headline="Recycled PET bottle demand climbs in Europe",
                americhem_impact="Bottle-grade rPET pull supports Packaging colorant volume.")
    kept, ledger = _apply_delivery_suppression([template, real], _supp_config())
    assert [r["url_hash"] for r in kept] == ["pkg"]
    assert dict(ledger.breakdown) == {"duplicate_headline": 1}
    assert ledger.samples[0].to_dict()["url"] == "https://a.example/tpl"


def test_semantic_duplicate_contest_never_goes_to_a_template_row():
    from report import _apply_delivery_suppression
    template = _row(url_hash="tpl", commercial_segment="Packaging", americhem_impact_score=5,
                    headline="Recycled PET bottle demand climbs across Europe in 2026",
                    americhem_impact="Limited direct exposure — bottle-grade rPET is not a served grade.")
    real = _row(url_hash="pkg", commercial_segment="Packaging", americhem_impact_score=3,
                headline="Recycled PET bottle demand climbs across Europe in 2025",
                americhem_impact="Bottle-grade rPET pull supports Packaging colorant volume.")
    kept, ledger = _apply_delivery_suppression([template, real], _supp_config())
    assert [r["url_hash"] for r in kept] == ["pkg"]
    assert dict(ledger.breakdown) == {"semantic_duplicate_headline": 1}


def test_apply_delivery_suppression_preserves_input_order_of_kept_rows():
    """Template-aware dedup must not reorder the survivors: section order
    downstream is first-seen, and rows arrive impact-desc."""
    from report import _apply_delivery_suppression
    rows = [
        _row(url_hash="h8", commercial_segment="Healthcare", americhem_impact_score=8,
             headline="Catheter maker qualifies a new radiopaque compound"),
        _row(url_hash="t4", commercial_segment=_ENTERPRISE, americhem_impact_score=4,
             headline="Bank raises passive stake in Huntsman",
             americhem_impact="Limited direct exposure — passive shareholding change only."),
        _row(url_hash="p3", commercial_segment="Packaging", americhem_impact_score=3,
             headline="Barrier film converter adds a line"),
    ]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert [r["url_hash"] for r in kept] == ["h8", "t4", "p3"]
    assert dict(ledger.breakdown) == {}


def test_appendix_keeps_the_segment_row_when_a_template_duplicates_it():
    """End to end: with the exemption on, a template duplicate never costs the
    appendix its segment-specific row."""
    template = {**_make_new_article("tpl", 4, commercial_segment=_ENTERPRISE,
                                    headline="Recycled PET bottle demand climbs in Europe"),
                "source_url": "https://a.example/tpl",
                "americhem_impact": "Adjacent market — no direct Americhem participation indicated."}
    real = {**_make_new_article("pkg", 3, commercial_segment="Packaging",
                                headline="Recycled PET bottle demand climbs in Europe"),
            "source_url": "https://b.example/pkg"}
    config = {"reporting": {"visible_impact_threshold": 6, "supporting_impact_threshold": 3}}
    model = assemble_report([template, real], config=config)
    assert _appendix_hashes(model) == ["pkg"]


def test_rule1_exemption_config_switch_off_restores_the_drop():
    from report import _apply_delivery_suppression
    rows = [_row(commercial_segment=_ENTERPRISE, americhem_impact_score=3,
                 americhem_impact="Adjacent market — no direct Americhem participation indicated.")]
    kept, ledger = _apply_delivery_suppression(
        rows, _supp_config(enable_low_exposure_template_exemption=False))
    assert kept == []
    assert dict(ledger.breakdown) == {"enterprise_cross_segment_low_impact": 1}


def test_apply_delivery_suppression_drops_product_listing():
    from report import _apply_delivery_suppression
    rows = [_row(source_url="https://example.com/product/widget")]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"product_listing": 1}


def test_apply_delivery_suppression_drops_job_posting():
    from report import _apply_delivery_suppression
    rows = [_row(source_url="https://www.linkedin.com/jobs/12345")]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"job_posting": 1}


def test_apply_delivery_suppression_job_posting_escalate_override():
    """A job-posting URL with recommended_action='Escalate to leadership' is kept."""
    from report import _apply_delivery_suppression
    rows = [_row(source_url="https://www.linkedin.com/jobs/ceo-move",
                 recommended_action="Escalate to leadership")]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {}


def test_apply_delivery_suppression_drops_generic_market_report_no_entities():
    from report import _apply_delivery_suppression
    rows = [_row(headline="Global Polypropylene Market Size 2026-2032",
                 entities_mentioned=[])]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"generic_market_report": 1}


def test_apply_delivery_suppression_keeps_generic_market_report_with_entities():
    from report import _apply_delivery_suppression
    rows = [_row(headline="Global Polypropylene Market 2026 Report",
                 entities_mentioned=["Avient"])]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {}


def test_apply_delivery_suppression_drops_unrelated_color_result():
    from report import _apply_delivery_suppression
    rows = [_row(headline="What extension cord colors mean",
                 americhem_impact="No plastics relevance.",
                 entities_mentioned=["DIY Network"])]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"unrelated_color_result": 1}


def test_apply_delivery_suppression_keeps_color_result_with_plastics_term():
    from report import _apply_delivery_suppression
    rows = [_row(headline="New masterbatch colors for automotive interiors",
                 americhem_impact="Drives masterbatch demand.",
                 entities_mentioned=["BASF"])]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {}


def test_apply_delivery_suppression_drops_exact_duplicate_headline():
    from report import _apply_delivery_suppression
    rows = [_row(url_hash="a", headline="Plant fire halts production"),
            _row(url_hash="b", headline="Plant fire halts production")]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert kept[0]["url_hash"] == "a"
    assert dict(ledger.breakdown) == {"duplicate_headline": 1}


def test_apply_delivery_suppression_drops_semantic_duplicate_headline():
    from report import _apply_delivery_suppression
    rows = [
        _row(url_hash="a", headline="Plant fire halts production at BASF site"),
        _row(url_hash="b", headline="BASF plant fire halts production at site"),
    ]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {"semantic_duplicate_headline": 1}


def test_apply_delivery_suppression_first_match_wins():
    """A row matching both product_listing and generic_market_report is counted once,
    under product_listing (which is checked first in the rule order)."""
    from report import _apply_delivery_suppression
    rows = [_row(source_url="https://amazon.com/product/123",
                 headline="Plastic Market Report 2026",
                 entities_mentioned=[])]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"product_listing": 1}  # NOT generic_market_report


def test_apply_delivery_suppression_disabled_rule_allows_through():
    from report import _apply_delivery_suppression
    cfg = _supp_config(enable_product_listing=False)
    rows = [_row(source_url="https://example.com/product/widget")]
    kept, ledger = _apply_delivery_suppression(rows, cfg)
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {}


def test_apply_delivery_suppression_samples_capped_at_10():
    from report import _apply_delivery_suppression
    rows = [
        _row(url_hash=f"h{i}", source_url=f"https://amazon.com/product/{i}",
             headline=f"Product {i}")
        for i in range(15)
    ]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert ledger.breakdown["product_listing"] == 15
    assert len(ledger.samples) == 10


# ===========================================================================
# _group_by_commercial_segment
# ===========================================================================


def test_group_by_commercial_segment_keys_off_new_field():
    from report import _group_by_commercial_segment
    rows = [
        {"url_hash": "a", "commercial_segment": "Healthcare",
         "americhem_impact_score": 8, "headline": "A"},
        {"url_hash": "b", "commercial_segment": "Healthcare",
         "americhem_impact_score": 7, "headline": "B"},
        {"url_hash": "c", "commercial_segment": "Packaging",
         "americhem_impact_score": 6, "headline": "C"},
    ]
    groups = _group_by_commercial_segment(rows)
    assert set(groups.keys()) == {"Healthcare", "Packaging"}
    assert len(groups["Healthcare"]) == 2


def test_group_by_commercial_segment_defaults_when_field_missing():
    from report import _group_by_commercial_segment
    rows = [
        {"url_hash": "a", "americhem_impact_score": 8, "headline": "A"},
        {"url_hash": "b", "commercial_segment": "Packaging",
         "americhem_impact_score": 7, "headline": "B"},
    ]
    groups = _group_by_commercial_segment(rows)
    assert "Enterprise / Cross-Segment" in groups
    assert "Packaging" in groups


# ===========================================================================
# _citation_display_map
# ===========================================================================


def test_citation_display_map_renumbers_by_first_appearance():
    bullets = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": [5, 8]},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": [8, 2]},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    sources = [_src(5), _src(8), _src(2)]
    assert _citation_display_map(bullets, sources) == {5: 1, 8: 2, 2: 3}


def test_citation_display_map_ignores_ids_without_a_source():
    bullets = [{"label": "Market pressure", "body": "A.", "citation_source_ids": [5, 99]}]
    assert _citation_display_map(bullets, [_src(5)]) == {5: 1}


# ===========================================================================
# Display-only segment merge: ground transportation -> "Transportation — Vehicles"
# ===========================================================================


_MERGE_CFG = {
    "reporting": {
        "visible_impact_threshold": 6,
        "max_visible_articles_per_segment": 5,
        "segment_display_groups": {
            "Transportation — Vehicles": [
                "Transportation - Automotive",
                "Transportation - Non-Automotive",
            ],
        },
    },
}


def test_ground_transportation_merges_aerospace_separate():
    """Automotive + Non-Automotive rows collapse into one display group;
    Aerospace keeps its own section."""
    auto = _make_new_article("auto", 8, commercial_segment="Transportation - Automotive",
                             headline="EV platform retooling lifts under-hood compound demand")
    non_auto = _make_new_article("nonauto", 7, commercial_segment="Transportation - Non-Automotive",
                                 headline="Rail freight operators standardize on flame-retardant interiors")
    aero = _make_new_article("aero", 9, commercial_segment="Transportation - Aerospace",
                             headline="Aircraft interior supplier qualifies new lightweight composite")

    model = assemble_report([auto, non_auto, aero], config=_MERGE_CFG)

    assert set(model.groups) == {"Transportation — Vehicles", "Transportation - Aerospace"}
    merged = {a["url_hash"] for a in model.groups["Transportation — Vehicles"]}
    assert merged == {"auto", "nonauto"}
    assert [a["url_hash"] for a in model.groups["Transportation - Aerospace"]] == ["aero"]


def test_ground_transportation_cap_applies_post_merge():
    """The per-segment cap governs the merged group: 4 automotive + 3
    non-automotive visible rows with cap 5 yield 5 cards in the merged group
    (top-5 by impact), and the 2 overflow rows land in the appendix — the
    combined section must not balloon to all 7."""
    auto_headlines = [
        "EV platform retooling lifts under-hood compound demand",
        "Automaker qualifies bio-based interior trim resin",
        "Tier-1 supplier expands lightweight bumper compounding",
        "Battery-tray molders shift to flame-retardant grades",
    ]
    non_auto_headlines = [
        "Rail freight operators standardize on flame-retardant interiors",
        "Heavy-truck OEM adopts new thermal-management polymer",
        "Off-highway equipment maker requalifies hydraulic seals",
    ]
    autos = [
        _make_new_article(f"auto{i}", americhem_impact_score=10 - i,
                          commercial_segment="Transportation - Automotive",
                          headline=auto_headlines[i])
        for i in range(4)
    ]
    non_autos = [
        _make_new_article(f"non{i}", americhem_impact_score=6,
                          commercial_segment="Transportation - Non-Automotive",
                          headline=non_auto_headlines[i])
        for i in range(3)
    ]
    model = assemble_report(autos + non_autos, config=_MERGE_CFG)

    merged = model.groups["Transportation — Vehicles"]
    assert len(merged) == 5
    assert model.surfaced_count == 5
    # Top-5 by impact: auto0(10), auto1(9), auto2(8), auto3(7), then one of the
    # score-6 non-autos (tie broken by the stable sort). The two other score-6
    # non-autos overflow into the appendix.
    overflow = {a["url_hash"] for a in model.additional_articles}
    assert len(overflow) == 2
    assert overflow.issubset({"non0", "non1", "non2"})


def test_absent_segment_display_groups_no_merge():
    """With no segment_display_groups the two ground-transport segments stay
    separate — a config-only rollback restores today's behavior exactly."""
    auto = _make_new_article("auto", 8, commercial_segment="Transportation - Automotive",
                             headline="EV platform retooling lifts under-hood compound demand")
    non_auto = _make_new_article("nonauto", 7, commercial_segment="Transportation - Non-Automotive",
                                 headline="Rail freight operators standardize on flame-retardant interiors")

    no_key = assemble_report([auto, non_auto],
                             config={"reporting": {"visible_impact_threshold": 6}})
    empty_map = assemble_report([auto, non_auto],
                                config={"reporting": {"visible_impact_threshold": 6,
                                                      "segment_display_groups": {}}})

    for model in (no_key, empty_map):
        assert set(model.groups) == {"Transportation - Automotive",
                                     "Transportation - Non-Automotive"}


def test_unknown_segment_label_in_mapping_ignored():
    """A mapping entry naming a segment label that is not a real canonical
    segment is ignored — no crash, no phantom group."""
    cfg = {
        "reporting": {
            "visible_impact_threshold": 6,
            "segment_display_groups": {
                "Bogus Display": ["Not A Real Segment", "Also Fake"],
                "Transportation — Vehicles": ["Transportation - Automotive",
                                              "Transportation - Non-Automotive"],
            },
        },
    }
    auto = _make_new_article("auto", 8, commercial_segment="Transportation - Automotive",
                             headline="EV platform retooling lifts under-hood compound demand")
    model = assemble_report([auto], config=cfg)

    assert set(model.groups) == {"Transportation — Vehicles"}
    assert "Bogus Display" not in model.groups


def test_merged_group_is_single_synthesis_candidate():
    """A merged group with 2+ articles appears once, under the display label, in
    synthesis_candidates — so thematic synthesis produces one paragraph for the
    combined section (not one per canonical segment)."""
    auto = _make_new_article("auto", 8, commercial_segment="Transportation - Automotive",
                             headline="EV platform retooling lifts under-hood compound demand")
    non_auto = _make_new_article("nonauto", 7, commercial_segment="Transportation - Non-Automotive",
                                 headline="Rail freight operators standardize on flame-retardant interiors")

    model = assemble_report([auto, non_auto], config=_MERGE_CFG)
    candidates = model.synthesis_candidates()

    assert list(candidates) == ["Transportation — Vehicles"]
    assert {a["url_hash"] for a in candidates["Transportation — Vehicles"]} == {"auto", "nonauto"}


def test_surfaced_count_equals_sum_of_final_group_sizes_through_merge():
    """The surfaced_count invariant (== sum of final group sizes) holds through
    the merge, even with mixed segments and a cap forcing overflow."""
    auto = [
        _make_new_article(f"auto{i}", americhem_impact_score=9 - i,
                          commercial_segment="Transportation - Automotive",
                          headline=f"Automotive compound development milestone number {i}")
        for i in range(4)
    ]
    non_auto = [
        _make_new_article(f"non{i}", americhem_impact_score=6,
                          commercial_segment="Transportation - Non-Automotive",
                          headline=f"Rail and heavy-truck polymer qualification update {i}")
        for i in range(3)
    ]
    aero = _make_new_article("aero", 9, commercial_segment="Transportation - Aerospace",
                             headline="Aircraft interior supplier qualifies new lightweight composite")

    model = assemble_report(auto + non_auto + [aero], config=_MERGE_CFG)

    assert model.surfaced_count == sum(len(arts) for arts in model.groups.values())


def test_appendix_rows_carry_display_label():
    """Cap-overflow rows from a merged group show the merged display label in
    the appendix segment column — the header consistency the reader sees on the
    cards also applies to the appendix (mapping done at model-assembly time)."""
    _auto_headlines = [
        "EV platform retooling lifts under-hood compound demand",
        "Automaker qualifies bio-based interior trim resin",
        "Tier-1 supplier expands lightweight bumper compounding",
        "Battery-tray molders shift to flame-retardant grades",
        "Under-the-hood sensor housings move to high-heat nylon",
        "Fuel-system component maker requalifies a barrier polymer",
    ]
    autos = [
        _make_new_article(f"auto{i}", americhem_impact_score=10 - i,
                          commercial_segment="Transportation - Automotive",
                          headline=_auto_headlines[i])
        for i in range(6)  # 6 visible-band (score 10..8..6..5) but distinct headlines
    ]
    # Force all six into the visible band so exactly one overflows the cap of 5.
    for a in autos:
        a["americhem_impact_score"] = 10 - autos.index(a) if autos.index(a) < 4 else 6
    model = assemble_report(autos, config=_MERGE_CFG)

    assert len(model.groups["Transportation — Vehicles"]) == 5
    assert len(model.additional_articles) == 1
    assert model.additional_articles[0]["commercial_segment"] == "Transportation — Vehicles"
    # Purity: the caller's input rows are untouched (copy-on-write).
    assert all(a["commercial_segment"] == "Transportation - Automotive" for a in autos)


def test_macro_outlook_affected_segments_show_display_label():
    """Macro-outlook signal affected_segments are remapped to display labels for
    render consistency; validation (canonical, at ingestion) is untouched.
    Collisions between two merged canonical labels dedupe to one chip."""
    macro_summary = {
        "macro_outlook": {
            "current_condition": "Ground-transport demand firming.",
            "signals": [{
                "indicator": "Light-vehicle build rate",
                "direction": "Improving",
                "americhem_implication": "Upside for under-hood compound volumes.",
                "affected_segments": ["Transportation - Automotive",
                                      "Transportation - Non-Automotive"],
                "citation_source_ids": [1],
            }],
        },
    }
    rows = [_make_new_article("v", 8, commercial_segment="Transportation - Automotive",
                              headline="EV platform retooling lifts under-hood compound demand")]
    model = assemble_report(rows, macro_summary=macro_summary, config=_MERGE_CFG)

    assert model.macro_outlook["signals"][0]["affected_segments"] == ["Transportation — Vehicles"]
    # Stored row untouched.
    assert macro_summary["macro_outlook"]["signals"][0]["affected_segments"] == [
        "Transportation - Automotive", "Transportation - Non-Automotive",
    ]
