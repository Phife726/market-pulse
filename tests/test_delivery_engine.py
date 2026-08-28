"""delivery_engine.py — Fetch → Format → Send.

Rendering (cards, sections, the macro outlook, the appendix, citations, the
test-mode markings), the fetches and their run-mode isolation, the summary
write-back, `synthesize_thematic_paragraphs`, `send_email` on the mailer seam, and
`execute_pipeline`. Tests that assemble a `ReportModel` only to render it belong
here: the HTML is the observable.
"""

from dataclasses import replace
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

import pytest

from llm import FakeLLM
from tests.conftest import (
    RUN_INSTANT as _RUN,
    TEST_RUN_INSTANT as _TEST_RUN,
    _APPENDIX_CFG,
    _VALID_MACRO_OUTLOOK,
    _appendix_hashes,
    _make_new_article,
    _src,
)
from delivery_engine import (
    _render_card,
    synthesize_thematic_paragraphs,
    assemble_report,
    prepare_report,
    render_report,
    send_email as _send_email,
    _safe_http_url,
    _render_executive_bullets,
    _render_sources_footer,
    _render_exec_summary,
    _render_sources_section,
)
from daily_intelligence_repo import InMemoryIntelligenceRepo
from report import _citation_display_map


# ===========================================================================
# _render_card() article_summary rendering
# ===========================================================================


_TODAY_STR = _RUN.header_date


def test_render_card_omits_article_summary():
    """article_summary must never appear in card HTML regardless of content."""
    item = {
        "headline": "Test Headline",
        "source_url": "https://news.com/article",
        "americhem_impact": "Some impact.",
        "category": "competitors",
        "sentiment_score": 5,
        "source_publication": "Reuters",
        "sentiment_rationale": "Neutral article.",
        "recommended_action": "Monitor",
        "article_summary": "BASF announced a new plant in Germany.",
    }
    html = _render_card(item)
    assert "BASF announced a new plant in Germany." not in html


# ===========================================================================
# send_email() — the consumer side of the mailer seam
# ===========================================================================


def test_send_email_hands_one_composed_message_to_the_mailer(fake_mailer):
    """send_email composes the digest (sender, recipients, subject, html) and
    crosses the seam exactly once; transport and retries are the adapter's."""
    _send_email("<html>test</html>", run=_RUN)

    assert len(fake_mailer.sent) == 1
    message = fake_mailer.sent[0]
    assert message.sender == "noreply@harness.test"
    assert message.recipients == ("qa@harness.test",)
    assert message.html == "<html>test</html>"


def test_send_email_refuses_an_empty_recipient_list(fake_mailer, monkeypatch):
    """RECIPIENT_EMAILS set to separators only is a configuration error the
    consumer catches before the seam — never a request with `to: []`."""
    monkeypatch.setenv("RECIPIENT_EMAILS", " , ,")

    with pytest.raises(ValueError, match="RECIPIENT_EMAILS"):
        _send_email("<html>x</html>", run=_RUN)

    assert fake_mailer.sent == []


# ===========================================================================
# _render_card() — no ACTION line
# ===========================================================================


@pytest.mark.parametrize("action", ["Monitor", "Escalate to leadership"])
def test_render_card_never_renders_action_line(action):
    """The shipped card shows no ACTION line for any recommended_action."""
    item = {
        "headline": "Plant fire halts BASF production",
        "source_url": "https://news.com/article",
        "americhem_impact": "Direct feedstock disruption risk.",
        "category": "suppliers",
        "sentiment_score": 2,
        "recommended_action": action,
    }
    html = _render_card(item)
    assert "ACTION:" not in html


# ===========================================================================
# Macro outlook flows through fetch_macro_summary and the renderer
# ===========================================================================


def test_macro_outlook_cites_card_suppressed_article():
    """A macro article suppressed as a card (generic-market-report title, no
    entities) can still be cited by the outlook — the outlook renders from the
    summary row, independent of card visibility."""
    suppressed = _make_new_article("supp", 8, commercial_segment="Industrial",
                                   headline="Global polymer market outlook to reach $50 billion")
    suppressed["entities_mentioned"] = []
    visible = _make_new_article("vis", 8, commercial_segment="Packaging",
                                headline="Packaging supply disruption raises converter costs")
    macro = {
        "macro_outlook": {
            "current_condition": "Industrial demand softening.",
            "signals": [
                {"indicator": "Manufacturing PMI", "direction": "Declining",
                 "americhem_implication": "Downside risk for industrial compound demand.",
                 "affected_segments": ["Industrial"], "citation_source_ids": [7]},
            ],
        },
        "executive_sources": [
            {"id": 7, "headline": "Global polymer market outlook report",
             "url": "https://s/7", "domain": "m.com"},
        ],
    }
    config = {
        "reporting": {"visible_impact_threshold": 6},
        "delivery_suppression": {"title_patterns_generic_market_report": ["market outlook", "to reach $"]},
    }
    model = assemble_report([suppressed, visible], macro_summary=macro, config=config)
    card_hashes = {a["url_hash"] for arts in model.groups.values() for a in arts}
    assert "supp" not in card_hashes                      # suppressed as a card
    html = render_report(model, today_str=_TODAY_STR)
    assert _MACRO_TITLE in html                           # outlook still renders
    assert "Global polymer market outlook report" in html  # cited in Sources footer


def test_fetch_macro_summary_passes_macro_outlook_through(monkeypatch):
    """Delivery's fetch_macro_summary returns the row verbatim, so macro_outlook
    (incl. the test-mode production-row fallback) is carried along for free."""
    import delivery_engine
    fake_repo = InMemoryIntelligenceRepo()
    fake_repo.upsert_summary({
        "run_date": _RUN.run_date,
        "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "Mixed / Watch",
        "macro_outlook": _VALID_MACRO_OUTLOOK,
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake_repo)
    summary = delivery_engine.fetch_macro_summary(_RUN)
    assert summary["macro_outlook"] == _VALID_MACRO_OUTLOOK


# ===========================================================================
# _render_card() — article_summary must not appear in rendered HTML
# ===========================================================================


def test_render_card_excludes_article_summary():
    """article_summary must not appear in rendered card HTML."""
    item = {
        "headline": "Test headline",
        "source_url": "https://example.com",
        "americhem_impact": "Some impact.",
        "category": "markets",
        "sentiment_score": 5,
        "article_summary": "This is the article summary text.",
    }
    html = _render_card(item)
    assert "This is the article summary text." not in html


# ===========================================================================
# Thematic synthesis helpers — shared fixture
# ===========================================================================


def _make_article(
    url_hash: str,
    score: int,
    category: str | None,
    headline: str = "Test Headline",
) -> dict:
    return {
        "url_hash": url_hash,
        "sentiment_score": score,
        "category": category,
        "headline": headline,
        "americhem_impact": "Some impact.",
        "entities_mentioned": ["TestCorp"],
        "source_url": "https://news.com/article",
    }


# ===========================================================================
# synthesize_thematic_paragraphs()
# ===========================================================================


def _make_synthesis_mock(paragraphs: dict) -> FakeLLM:
    return FakeLLM(returns=paragraphs)


def test_synthesize_thematic_paragraphs_returns_paragraphs():
    """Returns dict of {category: paragraph} on success."""
    groups = {
        "competitors": [
            _make_article("a", 8, "competitors"),
            _make_article("b", 7, "competitors"),
        ]
    }
    expected = {"competitors": "Avient and Techmer raised prices."}
    mock_client = _make_synthesis_mock(expected)

    with patch("delivery_engine._llm", return_value=mock_client):
        result = synthesize_thematic_paragraphs(groups)

    assert result == expected


def test_synthesize_thematic_paragraphs_passes_grouped_text_to_seam():
    """The caller sends one request carrying the grouped category text."""
    groups = {
        "suppliers": [
            _make_article("a", 4, "suppliers"),
            _make_article("b", 5, "suppliers"),
        ]
    }
    fake = _make_synthesis_mock({"suppliers": "Supply chain tightening."})

    with patch("delivery_engine._llm", return_value=fake):
        synthesize_thematic_paragraphs(groups)

    assert len(fake.calls) == 1
    assert "CATEGORY: suppliers" in fake.calls[-1]["user"]


def test_synthesize_thematic_paragraphs_empty_groups():
    """Returns {} immediately without touching the seam when groups is empty."""
    fake = FakeLLM()

    with patch("delivery_engine._llm", return_value=fake):
        result = synthesize_thematic_paragraphs({})

    assert fake.calls == []
    assert result == {}


def test_synthesize_thematic_paragraphs_graceful_degradation():
    """Returns {} when the seam yields no usable response — does not re-raise."""
    groups = {
        "competitors": [
            _make_article("a", 7, "competitors"),
            _make_article("b", 8, "competitors"),
        ]
    }
    # The seam swallows transport/parse failures and returns None.
    with patch("delivery_engine._llm", return_value=FakeLLM(returns=None)):
        result = synthesize_thematic_paragraphs(groups)

    assert result == {}


# ===========================================================================
# Report assembly + rendering integration
# ===========================================================================


def test_report_legacy_critical_old_sections_gone():
    """Legacy sentiment_score<=3 rows fall below the visible threshold (6) via the
    sentiment_score fallback, and the pre-redesign section labels never render."""
    data = [
        {"url_hash": "c0", "sentiment_score": 2, "category": "suppliers",
         "headline": "Legacy critical headline about plant fire",
         "americhem_impact": "Disruption.",
         "entities_mentioned": ["BASF"], "source_url": "https://x/0",
         "commercial_segment": "Enterprise / Cross-Segment"},
    ]
    # Note: this legacy row has no americhem_impact_score, so the visibility filter
    # uses sentiment_score=2 -> effective_impact <= 3, which is BELOW the visible
    # threshold (6). So the row will not surface in the segment watch under the
    # current threshold filter. What we DO assert: the old section labels are gone
    # and Peripheral Signals is hidden in production. The CRITICAL badge behaviour
    # is unit-tested directly via test_render_segment_watch_section_critical_badge_for_legacy_low_score.
    model = assemble_report(data, config={"reporting": {"visible_impact_threshold": 6}})
    html = render_report(model, today_str=_TODAY_STR)
    assert "PERIPHERAL SIGNALS" not in html
    assert "CRITICAL DISRUPTIONS" not in html
    assert "THEMATIC INTELLIGENCE" not in html


def test_report_routes_two_plus_to_segment_watch():
    """Two articles in the same commercial_segment produce a Commercial Segment
    Watch block with a synthesis paragraph."""
    data = [
        {"url_hash": "a", "commercial_segment": "Healthcare",
         "americhem_impact_score": 7, "sentiment_tag": "Positive",
         "signal_type": "Customer", "headline": "Avient expands healthcare polymer line",
         "americhem_impact": "Effect.", "source_url": "https://x/a",
         "entities_mentioned": ["Avient"]},
        {"url_hash": "b", "commercial_segment": "Healthcare",
         "americhem_impact_score": 8, "sentiment_tag": "Positive",
         "signal_type": "Customer", "headline": "Techmer launches sterilizable compound line",
         "americhem_impact": "Effect.", "source_url": "https://x/b",
         "entities_mentioned": ["Techmer"]},
    ]
    model = assemble_report(data, config={"reporting": {"visible_impact_threshold": 6}})
    assert model.synthesis_candidates() == {"Healthcare": model.groups["Healthcare"]}
    html = render_report(
        model.with_synthesis({"Healthcare": "Synthesis paragraph here."}),
        today_str=_TODAY_STR,
    )
    assert "COMMERCIAL SEGMENT WATCH" in html
    assert "Healthcare" in html
    assert "Synthesis paragraph here." in html
    assert "THEMATIC INTELLIGENCE" not in html


def test_report_single_low_relevance_not_a_visible_card():
    """An impact-5 article is never a visible card (no Peripheral Signals
    section), but IS surfaced in the optional-discovery appendix — so it is
    not counted as weak_relevance."""
    data = [{"url_hash": "x", "commercial_segment": "Packaging",
             "americhem_impact_score": 5, "sentiment_tag": "Neutral",
             "signal_type": "Customer",
             "headline": "Low relevance packaging signal",
             "americhem_impact": ".", "source_url": "https://x/p",
             "entities_mentioned": ["Acme"]}]
    model = assemble_report(data, config={"reporting": {"visible_impact_threshold": 6}})
    assert model.groups == {}
    assert [a["url_hash"] for a in model.additional_articles] == ["x"]
    assert model.ledger.breakdown.get("weak_relevance", 0) == 0
    html = render_report(model, today_str=_TODAY_STR)
    assert "PERIPHERAL SIGNALS" not in html


def test_assemble_report_filters_below_impact_threshold():
    """Articles with americhem_impact_score below the threshold must not appear in the report.
    Use a non-Enterprise segment so the Enterprise-low-impact suppression rule
    doesn't claim the row first — this exercises the visibility filter itself."""
    low_impact = _make_new_article("low", americhem_impact_score=3, headline="Low Impact Headline",
                                   commercial_segment="Packaging")
    high_impact = _make_new_article("high", americhem_impact_score=8, headline="High Impact Headline",
                                    commercial_segment="Packaging")

    model = assemble_report([low_impact, high_impact],
                            config={"reporting": {"visible_impact_threshold": 6}})

    kept = {a["url_hash"] for arts in model.groups.values() for a in arts}
    assert kept == {"high"}
    assert model.ledger.breakdown["below_impact_threshold"] == 1
    html = render_report(model, today_str=_TODAY_STR)
    assert "High Impact Headline" in html
    assert "Low Impact Headline" not in html


def test_legacy_outlook_render_lists_no_orphan_sources():
    """A daily_summaries row stored before the cap reduction may hold 6 signals
    citing 6 distinct sources. The rendered outlook body shows only the sliced
    3 signals, so the exec-summary citation numbering and the bottom Sources
    footer must list ONLY those 3 cited sources — no orphan [4][5][6] footer
    entries with no inline marker anywhere in the visible email (QA
    run_ingestion=false re-render scenario)."""
    from prompts import MAX_MACRO_OUTLOOK_SIGNALS

    signals = [
        {
            "indicator": f"Indicator {i + 1}",
            "direction": "Declining",
            "americhem_implication": f"Downside risk number {i + 1} for resin demand.",
            "affected_segments": ["Industrial"],
            "citation_source_ids": [i + 1],
        }
        for i in range(6)
    ]
    sources = [
        {"id": i + 1, "headline": f"Macro source {i + 1}",
         "url": f"https://s/{i + 1}", "domain": f"src{i + 1}.com"}
        for i in range(6)
    ]
    macro_summary = {
        "dominant_condition": "Demand Softness",
        # Bullets cite nothing, so citation numbering starts with the signals.
        "executive_bullets": [
            {"label": "Market pressure", "body": "Industrial demand cooling.", "citation_source_ids": []},
            {"label": "Supply chain watch", "body": "Feedstock steady.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Engage key accounts.", "citation_source_ids": []},
        ],
        "macro_outlook": {"current_condition": "Manufacturing demand mixed.",
                          "signals": signals},
        "executive_sources": sources,
    }
    rows = [_make_new_article("v", 8, commercial_segment="Packaging",
                              headline="Packaging demand firms on brand-owner restocking")]
    model = assemble_report(rows, macro_summary=macro_summary, config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)

    # The kept (sliced) sources appear in the Sources footer.
    for i in range(MAX_MACRO_OUTLOOK_SIGNALS):
        assert f"Macro source {i + 1}" in html
        assert f"src{i + 1}.com" in html
    # The sliced-off sources have no inline marker, so they must NOT appear as
    # orphan Sources footer entries.
    for i in range(MAX_MACRO_OUTLOOK_SIGNALS, 6):
        assert f"Macro source {i + 1}" not in html
        assert f"src{i + 1}.com" not in html


def test_assemble_report_groups_by_commercial_segment():
    """Two new-style articles with the same commercial_segment are grouped under that label."""
    # Use genuinely distinct headlines so delivery suppression doesn't flag them
    # as semantic duplicates (token_sort_ratio threshold is 88).
    art_a = _make_new_article("a", 8, commercial_segment="Healthcare",
                              headline="Hospital network consolidation squeezes specialty polymer demand")
    art_b = _make_new_article("b", 7, commercial_segment="Healthcare",
                              headline="FDA clears new medical-grade compound for implantable devices")

    model = assemble_report([art_a, art_b],
                            config={"reporting": {"visible_impact_threshold": 6}})
    assert [a["url_hash"] for a in model.groups["Healthcare"]] == ["a", "b"]

    html = render_report(
        model.with_synthesis({"Healthcare": "Healthcare synthesis paragraph."}),
        today_str=_TODAY_STR,
    )
    assert "HEALTHCARE" in html.upper()
    assert "Healthcare synthesis paragraph." in html


# ===========================================================================
# _render_card() — sentiment_tag and americhem_impact_score display
# ===========================================================================


def test_render_card_shows_impact_score_and_sentiment_tag():
    """When americhem_impact_score and sentiment_tag are present, the card shows
    'Impact: X/10' and the tag label, NOT the old 'Score: X/10' format."""
    item = {
        "headline": "Plant closure disrupts supply",
        "source_url": "https://news.com/article",
        "americhem_impact": "Feedstock shortfall for masterbatch lines.",
        "americhem_impact_score": 8,
        "sentiment_tag": "Negative",
        "impact_rationale": "Direct feedstock cost increase.",
        "commercial_segment": "Raw Materials / Supply Chain",
        "source_publication": "Chemical Week",
        "recommended_action": "Flag to procurement",
        "category": "markets",
    }
    html = _render_card(item)
    assert "Impact: 8/10" in html
    assert "Negative" in html
    assert "Score:" not in html


def test_render_card_falls_back_to_sentiment_score_for_old_rows():
    """Old-style rows without new fields must render the legacy 'Score: X/10' display."""
    item = {
        "headline": "Old Article",
        "source_url": "https://news.com/article",
        "americhem_impact": "Some impact.",
        "sentiment_score": 6,
        "source_publication": "Reuters",
        "recommended_action": "Monitor",
        "category": "markets",
    }
    html = _render_card(item)
    assert "Score: 6/10" in html
    assert "Impact:" not in html


def test_render_card_prefixes_so_what_with_direction_glyph():
    """The So-What label carries a colored direction glyph keyed by sentiment_tag,
    so a mis-spun sentence is visibly contradicted where the reader's eye is."""
    base = {
        "headline": "Plant closure disrupts supply",
        "source_url": "https://news.com/article",
        "americhem_impact": "Feedstock shortfall for masterbatch lines.",
        "americhem_impact_score": 8,
        "commercial_segment": "Raw Materials / Supply Chain",
        "source_publication": "Chemical Week",
        "recommended_action": "Monitor",
        "category": "markets",
    }
    cases = [
        ("Negative", "&#9660;", "#DC2626"),
        ("Neutral", "&#9679;", "#6B7280"),
        ("Positive", "&#9650;", "#16A34A"),
    ]
    for tag, glyph, color in cases:
        html = _render_card({**base, "sentiment_tag": tag})
        assert (
            f'<span style="color:{color};font-family:Arial,sans-serif;">{glyph}</span> <strong'
            in html
        )


def test_render_card_without_tag_renders_so_what_label_unchanged():
    """Rows with no sentiment_tag, or an unrecognised one (e.g. a value outside the
    Negative/Neutral/Positive vocabulary), get no glyph — the label starts the line."""
    item = {
        "headline": "Old Article",
        "source_url": "https://news.com/article",
        "americhem_impact": "Some impact.",
        "sentiment_score": 6,
        "source_publication": "Reuters",
        "recommended_action": "Monitor",
        "category": "markets",
    }
    html = _render_card(item)
    assert "&#9660;" not in html
    assert "&#9650;" not in html
    assert 'line-height:1.55;"><strong' in html

    html_bad_tag = _render_card({**item, "sentiment_tag": "Bullish"})
    assert "&#9660;" not in html_bad_tag
    assert "&#9650;" not in html_bad_tag
    assert 'line-height:1.55;"><strong' in html_bad_tag


def test_render_card_escapes_headline_and_so_what():
    """Scraped text is untrusted: a stray tag or ampersand in the headline or
    So-What must render as text, like every neighbouring renderer (issue #65)."""
    item = {
        "headline": "Resin <script>alert(1)</script> & co",
        "source_url": "https://news.com/article",
        "americhem_impact": "Margin <b>hit</b> on TiO2 & carbon black",
        "americhem_impact_score": 8,
        "sentiment_tag": "Negative",
        "signal_type": "Supply Chain",
    }
    html = _render_card(item)
    assert "<script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt; &amp; co" in html
    assert "<b>hit</b>" not in html
    assert "Margin &lt;b&gt;hit&lt;/b&gt; on TiO2 &amp; carbon black" in html


def test_render_card_drops_unsafe_href_but_keeps_the_headline():
    item = {
        "headline": "Plant closure disrupts supply",
        "source_url": "javascript:alert(1)",
        "americhem_impact": "Effect.",
        "americhem_impact_score": 8,
        "sentiment_tag": "Negative",
    }
    html = _render_card(item)
    assert "href=" not in html
    assert "javascript:" not in html
    assert "Plant closure disrupts supply" in html


def test_render_card_links_a_well_formed_url_unchanged():
    item = {
        "headline": "Plant closure disrupts supply",
        "source_url": "https://news.com/article?id=1&x=2",
        "americhem_impact": "Effect.",
        "americhem_impact_score": 8,
        "sentiment_tag": "Negative",
    }
    html = _render_card(item)
    assert 'href="https://news.com/article?id=1&amp;x=2"' in html


def test_sentiment_tag_maps_cover_exactly_the_schema_vocabulary():
    """The glyph map, the color map, and the Insight schema's tag vocabulary are
    three definitions of one list — a fourth tag must not silently lose its cue."""
    import delivery_engine
    import insight
    assert set(delivery_engine._SENTIMENT_TAG_GLYPHS) == set(delivery_engine._SENTIMENT_TAG_COLORS)
    assert set(delivery_engine._SENTIMENT_TAG_GLYPHS) == set(insight.VALID_SENTIMENT_TAGS)


# ===========================================================================
# Capped-out rows render in the appendix
# ===========================================================================


def test_report_capped_articles_flow_into_appendix():
    """Articles dropped by the per-segment cap reappear in the Additional
    Articles appendix — never as visible cards. (Flipped 2026-07-17: the cap
    previously dropped overflow entirely.)"""
    # Genuinely distinct headlines — token_sort_ratio >= 88 would otherwise
    # suppress them as semantic duplicates before the cap runs.
    _hc_headlines = [
        "Hospital network merger squeezes specialty polymer volumes",
        "FDA clears new implantable-grade compound for cardiac devices",
        "Aging population drives record demand for medical-grade resins",
        "Generic drug expansion pressures premium plastics pricing",
    ]
    articles = [
        _make_new_article(
            f"h{i}", americhem_impact_score=10 - i,
            commercial_segment="Healthcare",
            headline=_hc_headlines[i],
        )
        for i in range(4)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 3,
            "max_total_visible_articles": 12,
        }
    }
    model = assemble_report(articles, config=config)

    # Top 3 by impact are cards; h3 (impact=7) is capped out but not lost.
    assert [a["url_hash"] for a in model.groups["Healthcare"]] == ["h0", "h1", "h2"]
    assert [a["url_hash"] for a in model.additional_articles] == ["h3"]
    assert model.surfaced_count == 3

    html = render_report(model, today_str=_TODAY_STR)
    assert _hc_headlines[3] in html


# ===========================================================================
# Macroeconomic Outlook — rendering
# ===========================================================================


_MACRO_TITLE = "MACROECONOMIC OUTLOOK"


def _macro_summary_with_outlook() -> dict:
    return {
        "dominant_condition": "Demand Softness",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Industrial demand cooling.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Feedstock steady.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Engage key accounts.", "citation_source_ids": []},
        ],
        "macro_outlook": {
            "current_condition": "Industrial and construction demand both softening.",
            "signals": [
                {"indicator": "Housing starts", "direction": "Declining",
                 "americhem_implication": "Weakness in building-products volumes.",
                 "affected_segments": ["Industrial"], "citation_source_ids": [2]},
            ],
        },
        "executive_sources": [
            {"id": 1, "headline": "Industrial PMI slips", "url": "https://s/1", "domain": "s.com"},
            {"id": 2, "headline": "Housing starts fall", "url": "https://s/2", "domain": "t.com"},
        ],
    }


def test_macro_section_renders_between_exec_and_segment_watch():
    visible = _make_new_article("v", 8, commercial_segment="Packaging",
                                headline="High-impact packaging supply disruption card")
    model = assemble_report([visible], macro_summary=_macro_summary_with_outlook(),
                            config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    assert _MACRO_TITLE in html
    assert "Industrial and construction demand both softening." in html
    assert "Housing starts" in html
    assert "Declining" in html
    assert "Weakness in building-products volumes." in html
    assert "Industrial" in html
    i_exec = html.find("Executive Summary")
    i_macro = html.find(_MACRO_TITLE)
    i_watch = html.find("COMMERCIAL SEGMENT WATCH")
    assert i_exec != -1 and i_macro != -1 and i_watch != -1
    assert i_exec < i_macro < i_watch


def test_macro_section_absent_when_none():
    visible = _make_new_article("v", 8, commercial_segment="Packaging",
                                headline="High-impact packaging card with no outlook")
    model = assemble_report([visible], macro_summary={"dominant_condition": "Mixed / Watch"},
                            config=_APPENDIX_CFG)
    assert model.macro_outlook is None
    html = render_report(model, today_str=_TODAY_STR)
    assert _MACRO_TITLE not in html


def test_macro_section_current_condition_rendered_once():
    model = assemble_report(
        [_make_new_article("v", 8, commercial_segment="Packaging",
                           headline="Packaging card to accompany the macro outlook")],
        macro_summary=_macro_summary_with_outlook(), config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    assert html.count("Industrial and construction demand both softening.") == 1


def test_macro_section_shares_one_citation_numbering_space():
    """Bullets cite source 1, the macro signal cites source 2: the exec summary
    shows [1], the macro section shows [2], and the single Sources footer lists
    both — one numbering space (bullets enumerated, then signals)."""
    model = assemble_report(
        [_make_new_article("v", 8, commercial_segment="Packaging",
                           headline="Packaging card next to the macro outlook here")],
        macro_summary=_macro_summary_with_outlook(), config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    # Both cited sources resolve in the bottom Sources list.
    assert "Industrial PMI slips" in html
    assert "Housing starts fall" in html
    # Footer numbering covers both ids.
    assert "[1]" in html and "[2]" in html
    # The macro signal's marker links to source 2's url.
    assert 'href="https://s/2"' in html


def test_section_header_is_full_width_not_squeezed():
    """Section headers span the full width with an underline — no `nowrap`
    title cell that a narrow client squeezes into a 3-line wrap."""
    from delivery_engine import _section_header_row
    hdr = _section_header_row("Additional Articles to Explore",
                              title_color="#5a6678", rule_color="#E5E7EB")
    assert "Additional Articles to Explore" in hdr
    assert "white-space:nowrap" not in hdr
    assert "border-bottom:1px solid #E5E7EB" in hdr


def test_section_headers_render_without_nowrap():
    """The rendered email's section titles are not placed in nowrap cells."""
    macro = _macro_summary_with_outlook()
    model = assemble_report(
        [_make_new_article("v", 8, commercial_segment="Packaging",
                           headline="High-impact packaging card for header test"),
         _make_new_article("w", 5, commercial_segment="Industrial",
                           headline="Near-threshold industrial reading for appendix here")],
        macro_summary=macro, config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    for title in ("MACROECONOMIC OUTLOOK", "COMMERCIAL SEGMENT WATCH",
                  "Additional Articles to Explore"):
        # Each title sits in a full-width underlined header cell, not a
        # nowrap+padding-right two-cell layout.
        i = html.find(title)
        assert i != -1
        header_open = html.rfind("<td", 0, i)
        assert "white-space:nowrap" not in html[header_open:i]


def test_macro_section_direction_styling_is_valence_neutral():
    """Direction must not be risk-colored: 'Rising' is adverse for cost-side
    indicators (inflation, energy, freight) but favorable for demand-side ones,
    and the signal carries no good/bad field — so green/red would invert the
    risk on cost rows. Valence lives in the implication text, not the color."""
    macro = {
        "macro_outlook": {
            "current_condition": "Input costs climbing while demand holds.",
            "signals": [
                {"indicator": "Producer prices", "direction": "Rising",
                 "americhem_implication": "Margin pressure through resin, energy, and freight costs.",
                 "affected_segments": ["Industrial"], "citation_source_ids": [1]},
                {"indicator": "Housing starts", "direction": "Declining",
                 "americhem_implication": "Weakness in building-products volumes.",
                 "affected_segments": ["Industrial"], "citation_source_ids": [1]},
            ],
        },
        "executive_sources": [
            {"id": 1, "headline": "PPI climbs", "url": "https://s/1", "domain": "s.com"},
        ],
    }
    model = assemble_report(
        [_make_new_article("v", 8, commercial_segment="Packaging", sentiment_tag="Neutral",
                           headline="Neutral-tag packaging card beside the outlook")],
        macro_summary=macro, config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    macro_section = html[html.find(_MACRO_TITLE):html.find("COMMERCIAL SEGMENT WATCH")]
    # No sentiment green/red inside the macro section — direction is neutral.
    assert "#16A34A" not in macro_section    # green (would imply Rising = good)
    assert "#DC2626" not in macro_section    # red (would imply Declining = bad)
    assert "Rising" in macro_section and "Declining" in macro_section


def test_macro_section_escapes_untrusted_text():
    macro = _macro_summary_with_outlook()
    macro["macro_outlook"]["signals"][0]["americhem_implication"] = "<script>alert('x')</script> risk"
    model = assemble_report(
        [_make_new_article("v", 8, commercial_segment="Packaging",
                           headline="Packaging card with an XSS-y macro outlook")],
        macro_summary=macro, config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    assert "<script>alert" not in html
    assert "&lt;script&gt;" in html


# ===========================================================================
# Additional Articles appendix — rendering
# ===========================================================================


_APPENDIX_TITLE = "Additional Articles to Explore"


def test_appendix_renders_when_items_present():
    """The appendix section shows title, linked headline, segment, impact, and
    source for each row."""
    row = _make_new_article("a", 5, commercial_segment="Packaging",
                            headline="Near-threshold packaging demand firms up")
    row["source_publication"] = "Plastics News"
    model = assemble_report([row], config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    assert _APPENDIX_TITLE in html
    assert "Near-threshold packaging demand firms up" in html
    assert "Packaging" in html
    assert "Impact: 5/10" in html
    assert "Plastics News" in html
    assert 'href="https://news.com/article"' in html


def test_appendix_absent_when_empty():
    """No appendix section renders when there are no additional articles."""
    row = _make_new_article("v", 8, commercial_segment="Packaging",
                            headline="Visible high-impact packaging card only")
    model = assemble_report([row], config=_APPENDIX_CFG)
    assert model.additional_articles == ()
    html = render_report(model, today_str=_TODAY_STR)
    assert _APPENDIX_TITLE not in html


def test_appendix_shows_date_only_when_published_at():
    """Publication date renders only from published_at, never a scrape timestamp."""
    dated = _make_new_article("d", 5, commercial_segment="Packaging",
                              headline="Dated near-threshold packaging signal here")
    dated["published_at"] = "2026-07-15T09:00:00+00:00"
    dated["created_at"] = "2026-07-16T23:59:00+00:00"  # scrape time — must NOT show
    undated = _make_new_article("u", 5, commercial_segment="Industrial",
                                headline="Undated near-threshold industrial signal")
    undated["created_at"] = "2026-07-16T23:59:00+00:00"
    model = assemble_report([dated, undated], config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    assert "Jul 15, 2026" in html          # published_at of the dated row
    assert "Jul 16, 2026" not in html      # scrape timestamp never displayed


def test_appendix_omits_so_what_narrative():
    """The appendix does not render the americhem_impact 'So what' narrative."""
    row = _make_new_article("a", 5, commercial_segment="Packaging",
                            headline="Near-threshold packaging note for appendix")
    row["americhem_impact"] = "UNIQUE_SO_WHAT_NARRATIVE_TOKEN"
    model = assemble_report([row], config=_APPENDIX_CFG)
    assert _appendix_hashes(model) == ["a"]
    html = render_report(model, today_str=_TODAY_STR)
    assert _APPENDIX_TITLE in html
    assert "UNIQUE_SO_WHAT_NARRATIVE_TOKEN" not in html


def test_appendix_escapes_untrusted_and_guards_href():
    """Headline/source are HTML-escaped and a non-http(s) URL is neutralized."""
    row = _make_new_article("a", 5, commercial_segment="Packaging",
                            headline="<script>alert('x')</script> resin note")
    row["source_url"] = "javascript:alert(1)"
    model = assemble_report([row], config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    assert "<script>alert" not in html
    assert "&lt;script&gt;" in html
    assert 'href="javascript:' not in html


def test_appendix_renders_below_segment_watch_above_sources():
    """Section order: Commercial Segment Watch -> Additional Articles -> Sources."""
    visible = _make_new_article("v", 8, commercial_segment="Packaging",
                                headline="High-impact packaging supply disruption card")
    weak = _make_new_article("w", 5, commercial_segment="Industrial",
                             headline="Near-threshold industrial reading for appendix")
    macro = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Pressure body.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Supply body.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Action body.", "citation_source_ids": []},
        ],
        "executive_sources": [
            {"id": 1, "headline": "Source one", "url": "https://s/1", "domain": "s.com"},
        ],
    }
    model = assemble_report([visible, weak], macro_summary=macro, config=_APPENDIX_CFG)
    html = render_report(model, today_str=_TODAY_STR)
    i_watch = html.find("COMMERCIAL SEGMENT WATCH")
    i_appendix = html.find(_APPENDIX_TITLE)
    i_sources = html.find(">Sources<")
    assert i_watch != -1 and i_appendix != -1 and i_sources != -1
    assert i_watch < i_appendix < i_sources


# ===========================================================================
# Negative moderate-impact: impact score drives filtering, not sentiment tone
# ===========================================================================


def test_assemble_report_excludes_negative_low_impact_new_style():
    """A Negative-sentiment article with low americhem_impact_score must be excluded.
    Filtering is by impact score, not tone — this validates the invariant."""
    neg_low = _make_new_article(
        "neg_low", americhem_impact_score=4,
        sentiment_tag="Negative",
        headline="Negative Low Impact Headline",
    )
    pos_high = _make_new_article(
        "pos_high", americhem_impact_score=8,
        sentiment_tag="Positive",
        headline="Positive High Impact Headline",
    )
    model = assemble_report([neg_low, pos_high],
                            config={"reporting": {"visible_impact_threshold": 6}})
    html = render_report(model, today_str=_TODAY_STR)

    assert "Positive High Impact Headline" in html
    assert "Negative Low Impact Headline" not in html


def test_report_shows_negative_high_impact():
    """A Negative-sentiment article with high americhem_impact_score MUST appear.
    A high-impact supply disruption (Negative) is more important than a positive routine signal."""
    neg_high = _make_new_article(
        "neg_high", americhem_impact_score=9,
        sentiment_tag="Negative",
        commercial_segment="Raw Materials / Supply Chain",
        headline="Negative High Impact Supply Disruption",
    )
    model = assemble_report([neg_high],
                            config={"reporting": {"visible_impact_threshold": 6}})
    html = render_report(model, today_str=_TODAY_STR)

    assert "Negative High Impact Supply Disruption" in html
    assert "Negative" in html


# ===========================================================================
# MARKET_PULSE_RUN_MODE — test-mode markings
# ===========================================================================


def test_send_email_test_mode_prefixes_subject(fake_mailer):
    """A test-mode run instant marks the subject and dates it."""
    _send_email("<html>x</html>", run=_TEST_RUN)

    assert fake_mailer.sent[0].subject == "[TEST] Americhem Market-Pulse \u2014 August 27, 2026"


def test_send_email_production_mode_subject_unchanged(fake_mailer):
    """A production run instant dates the subject and adds no [TEST] prefix."""
    _send_email("<html>x</html>", run=_RUN)

    assert fake_mailer.sent[0].subject == "Americhem Market-Pulse \u2014 August 27, 2026"


def test_send_email_recipient_list_is_only_recipient_emails_env(fake_mailer, monkeypatch):
    """Recipient invariant: send_email() builds the Resend 'to' list strictly from the
    RECIPIENT_EMAILS env var and never falls back to any hardcoded address. This is
    the safety guarantee that lets the workflow swap recipient pools by env var alone.
    """
    monkeypatch.setenv("RECIPIENT_EMAILS", " jphifer@americhem.com , qa@americhem.com,, ")

    _send_email("<html>x</html>", run=_RUN)

    assert fake_mailer.sent[0].recipients == ("jphifer@americhem.com", "qa@americhem.com")


def test_render_report_test_mode_prefixes_header():
    """With test_mode=True, render_report() must include [TEST] in the title and
    a visible TEST RUN banner in the rendered HTML."""
    model = assemble_report([_make_new_article("h", 8, headline="Some Headline")])
    html = render_report(model, today_str=_TODAY_STR, test_mode=True)
    assert "[TEST]" in html
    assert "TEST RUN" in html
    assert "Jason-only QA output" in html


def test_render_report_production_mode_unchanged():
    """With test_mode=False (the default), the rendered HTML must contain
    no [TEST] markers or TEST RUN banner."""
    model = assemble_report([_make_new_article("h", 8, headline="Some Headline")])
    html = render_report(model, today_str=_TODAY_STR)
    assert "[TEST]" not in html
    assert "TEST RUN" not in html


def test_no_news_email_test_mode_marks_header():
    """The no-news variant HTML must carry [TEST] and the TEST RUN banner in test mode."""
    model = assemble_report([])
    assert model.variant == "no_news"
    html = render_report(model, today_str=_TODAY_STR, test_mode=True)
    assert "[TEST]" in html
    assert "TEST RUN" in html
    assert "No significant market events" in html


# ===========================================================================
# fetch_macro_summary() — run-mode isolation
# ===========================================================================


def test_fetch_macro_summary_filters_by_run_mode_production(monkeypatch):
    """A production run instant fetches the production row even when a test
    row exists — and even under a stray MARKET_PULSE_RUN_MODE=test: the value
    governs, not the environment."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    from delivery_engine import fetch_macro_summary
    today = _RUN.run_date

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "Prod summary", "macro_sentiment": "Stable",
    })
    fake.upsert_summary({
        "run_date": today, "run_mode": "test",
        "executive_summary": "Test summary", "macro_sentiment": "Stable",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    result = fetch_macro_summary(_RUN)
    assert result is not None
    assert result["run_mode"] == "production"
    assert result["executive_summary"] == "Prod summary"


def test_fetch_macro_summary_filters_by_run_mode_test(monkeypatch):
    """Test delivery must fetch the test row, not the production row."""
    from delivery_engine import fetch_macro_summary
    today = _RUN.run_date

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "Prod summary", "macro_sentiment": "Stable",
    })
    fake.upsert_summary({
        "run_date": today, "run_mode": "test",
        "executive_summary": "Test summary", "macro_sentiment": "Stable",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    result = fetch_macro_summary(_TEST_RUN)
    assert result is not None
    assert result["run_mode"] == "test"
    assert result["executive_summary"] == "Test summary"


def test_fetch_macro_summary_test_mode_falls_back_to_production_row(monkeypatch):
    """A delivery-only test run (run_ingestion=false) has no test-mode macro
    row — it must fall back to the production row read-only, so the QA
    re-render carries the executive summary and citation sources."""
    from delivery_engine import fetch_macro_summary
    today = _RUN.run_date

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "Prod summary", "macro_sentiment": "Stable",
        "executive_sources": [{"id": 1, "headline": "H", "url": "https://s/1",
                               "domain": "s.com", "segment": "Packaging", "score": 9}],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    result = fetch_macro_summary(_TEST_RUN)
    assert result is not None
    assert result["run_mode"] == "production"
    assert result["executive_sources"]


def test_fetch_macro_summary_test_mode_prefers_newer_production_over_stale_test_row(monkeypatch):
    """A test row from YESTERDAY (run_ingestion=true QA run the day before)
    must not shadow TODAY's production row — the re-render would pair today's
    articles with stale executive bullets/citations."""
    from delivery_engine import fetch_macro_summary
    today = _RUN.run_date
    yesterday = _RUN.min_summary_date

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": yesterday, "run_mode": "test",
        "executive_summary": "Stale test summary", "macro_sentiment": "Stable",
    })
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "Fresh prod summary", "macro_sentiment": "Stable",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    result = fetch_macro_summary(_TEST_RUN)
    assert result is not None
    assert result["executive_summary"] == "Fresh prod summary"


def test_fetch_macro_summary_test_mode_keeps_test_row_on_run_date_tie(monkeypatch):
    """Recency ties prefer the test row — covers the date-rollover grace
    (test ingestion writes at 23:59, delivery reads at 00:01: both candidate
    rows carry yesterday's run_date and the minutes-old test row must win)."""
    from delivery_engine import fetch_macro_summary
    yesterday = _RUN.min_summary_date

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": yesterday, "run_mode": "test",
        "executive_summary": "Rollover test summary", "macro_sentiment": "Stable",
    })
    fake.upsert_summary({
        "run_date": yesterday, "run_mode": "production",
        "executive_summary": "Prod summary", "macro_sentiment": "Stable",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    result = fetch_macro_summary(_TEST_RUN)
    assert result is not None
    assert result["executive_summary"] == "Rollover test summary"


def test_fetch_macro_summary_test_mode_accounting_only_test_row_does_not_shadow_production(monkeypatch):
    """A zero-yield test ingestion run persists an accounting-only test row
    (issue #43). On a run-date tie it must NOT shadow a content-full production
    row — content-fullness is compared before recency, so the QA re-render
    keeps the executive summary."""
    from delivery_engine import fetch_macro_summary
    today = _RUN.run_date

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": today, "run_mode": "test",
        "screened_count": 9, "suppression_breakdown": {"duplicate_url": 9},
        "suppression_samples": [],
    })
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "Prod summary", "macro_sentiment": "Stable",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    result = fetch_macro_summary(_TEST_RUN)
    assert result is not None
    assert result["executive_summary"] == "Prod summary"


def test_fetch_macro_summary_test_mode_accounting_only_production_row_does_not_shadow_test(monkeypatch):
    """The mirror direction: a strictly-newer accounting-only production row
    (zero-yield production run today) must not shadow yesterday's content-full
    test row — pre-#43 no production row would have existed at all."""
    from delivery_engine import fetch_macro_summary
    today = _RUN.run_date
    yesterday = _RUN.min_summary_date

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": yesterday, "run_mode": "test",
        "executive_summary": "Rollover test summary", "macro_sentiment": "Stable",
    })
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "screened_count": 14, "suppression_breakdown": {"scrape_failed": 14},
        "suppression_samples": [],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    result = fetch_macro_summary(_TEST_RUN)
    assert result is not None
    assert result["executive_summary"] == "Rollover test summary"


def test_fetch_macro_summary_test_mode_returns_accounting_only_row_when_no_content_anywhere(monkeypatch):
    """When the only candidate is an accounting-only test row, return it — the
    QA debug section still renders that day's suppression accounting."""
    from delivery_engine import fetch_macro_summary
    today = _RUN.run_date

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": today, "run_mode": "test",
        "screened_count": 6, "suppression_breakdown": {"unscrapable_domain": 6},
        "suppression_samples": [],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    result = fetch_macro_summary(_TEST_RUN)
    assert result is not None
    assert result["screened_count"] == 6


def test_render_report_tolerates_accounting_only_macro_summary():
    """A summary-less row (zero-yield ingestion day, issue #43) renders without
    crashing: no Executive Summary block, no Macroeconomic Outlook, but the
    QA suppression summary and the screened count in the subtitle still come
    from the row's accounting."""
    summary = {
        "run_date": "2026-07-17", "run_mode": "test",
        "screened_count": 21,
        "suppression_breakdown": {"duplicate_url": 5},
        "suppression_samples": [{"reason": "duplicate_url", "url": "https://x/d", "title": "Dup"}],
    }
    rows = [
        {"url_hash": "v1", "commercial_segment": "Packaging",
         "americhem_impact_score": 7, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": "Visible packaging signal",
         "americhem_impact": "Effect.", "source_url": "https://x/v1",
         "entities_mentioned": ["Acme"]},
    ]
    model = assemble_report(rows, summary, config={"reporting": {"visible_impact_threshold": 6}})
    html = render_report(model, today_str=_TODAY_STR, test_mode=True)
    assert "Executive Summary" not in html
    assert "MACROECONOMIC OUTLOOK" not in html
    assert "Suppression Summary" in html
    assert "21 screened items" in html


def test_fetch_macro_summary_production_never_reads_test_rows(monkeypatch):
    """The fallback is one-directional: production delivery with only a test
    row available must return None, not leak QA data into production mail."""
    from delivery_engine import fetch_macro_summary
    today = _RUN.run_date

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": today, "run_mode": "test",
        "executive_summary": "Test summary", "macro_sentiment": "Stable",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    assert fetch_macro_summary(_RUN) is None


def test_fetch_macro_summary_lookback_floor_is_the_run_instants_yesterday(monkeypatch):
    """The >= yesterday window is relative to the run instant, not the process
    clock: a row on min_summary_date is found, one a day earlier is not."""
    from delivery_engine import fetch_macro_summary

    fake = InMemoryIntelligenceRepo()
    fake.upsert_summary({
        "run_date": "2026-08-25", "run_mode": "production",
        "executive_summary": "Too old", "macro_sentiment": "Stable",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    assert fetch_macro_summary(_RUN) is None

    fake.upsert_summary({
        "run_date": _RUN.min_summary_date, "run_mode": "production",
        "executive_summary": "Yesterday", "macro_sentiment": "Stable",
    })
    assert fetch_macro_summary(_RUN)["executive_summary"] == "Yesterday"


# ===========================================================================
# _render_segment_watch_section
# ===========================================================================


def test_render_segment_watch_section_displays_meta_strip_with_signal():
    from delivery_engine import _render_segment_watch_section
    groups = {
        "Healthcare": [{
            "url_hash": "a",
            "headline": "Test Card Headline",
            "source_url": "https://news.com/a",
            "americhem_impact": "Direct demand effect.",
            "americhem_impact_score": 8,
            "sentiment_tag": "Positive",
            "signal_type": "Customer",
            "commercial_segment": "Healthcare",
            "recommended_action": "Monitor",
        }],
    }
    html = _render_segment_watch_section(groups, synthesis={})
    assert "HEALTHCARE" in html.upper()
    assert "Test Card Headline" in html
    assert "Impact: 8/10" in html
    assert "Positive" in html
    assert "Signal: Customer" in html
    assert "Direct demand effect." in html


def test_render_segment_watch_section_omits_signal_for_legacy_row():
    from delivery_engine import _render_segment_watch_section
    groups = {
        "Healthcare": [{
            "url_hash": "a",
            "headline": "Legacy Row Headline",
            "source_url": "https://news.com/a",
            "americhem_impact": "Effect.",
            "americhem_impact_score": 7,
            "sentiment_tag": "Neutral",
            "commercial_segment": "Healthcare",
            # no signal_type
        }],
    }
    html = _render_segment_watch_section(groups, synthesis={})
    assert "Impact: 7/10" in html
    assert "Signal:" not in html


def test_render_segment_watch_section_critical_badge_for_legacy_low_score():
    from delivery_engine import _render_segment_watch_section
    groups = {
        "Enterprise / Cross-Segment": [{
            "url_hash": "a",
            "headline": "Critical legacy headline",
            "source_url": "https://news.com/a",
            "americhem_impact": "Effect.",
            "sentiment_score": 2,
            "commercial_segment": "Enterprise / Cross-Segment",
        }],
    }
    html = _render_segment_watch_section(groups, synthesis={})
    assert "CRITICAL" in html


def test_render_segment_watch_section_renders_synthesis_paragraph():
    from delivery_engine import _render_segment_watch_section
    groups = {
        "Packaging": [
            {"url_hash": "a", "headline": "A", "source_url": "https://x/a",
             "americhem_impact": "X.", "americhem_impact_score": 7,
             "sentiment_tag": "Neutral", "signal_type": "Sustainability",
             "commercial_segment": "Packaging"},
            {"url_hash": "b", "headline": "B", "source_url": "https://x/b",
             "americhem_impact": "Y.", "americhem_impact_score": 6,
             "sentiment_tag": "Neutral", "signal_type": "Sustainability",
             "commercial_segment": "Packaging"},
        ]
    }
    synth = {"Packaging": "Brand-owners are shifting toward recycled content."}
    html = _render_segment_watch_section(groups, synth)
    assert "Brand-owners are shifting toward recycled content." in html


# ===========================================================================
# prepare_report / execute_pipeline integration
# ===========================================================================


def test_prepare_report_surfaced_count_is_post_cap(monkeypatch):
    """The written-back surfaced_count must reflect the final visible-card list AFTER per-segment caps."""
    from daily_intelligence_repo import InMemoryIntelligenceRepo

    rows = [
        {"url_hash": f"h{i}", "commercial_segment": "Healthcare",
         "americhem_impact_score": 8, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": f"HC {i}",
         "americhem_impact": "Effect.", "source_url": f"https://x/{i}",
         "entities_mentioned": ["Acme"]}
        for i in range(5)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 2,
            "max_total_visible_articles": 12,
        }
    }

    fake = InMemoryIntelligenceRepo()
    today = _RUN.run_date
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": {}, "suppression_samples": [],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    with patch("delivery_engine._llm", return_value=FakeLLM()):
        prepare_report(rows, None, run=_RUN, report_config=config)

    stored = fake.get_delivery_state(run_date=today, run_mode="production")
    assert stored is not None, "Expected an update() call to daily_summaries"
    assert stored["surfaced_count"] == 2


def test_prepare_report_writes_delivery_suppression_counts_back(monkeypatch):
    """Delivery must write below_impact_threshold into suppression_breakdown via update()."""
    from daily_intelligence_repo import InMemoryIntelligenceRepo

    rows = [
        {"url_hash": "low", "commercial_segment": "Healthcare",
         "americhem_impact_score": 4, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": "Below threshold",
         "americhem_impact": ".", "source_url": "https://x/1",
         "entities_mentioned": ["Acme"]},
        {"url_hash": "high", "commercial_segment": "Packaging",
         "americhem_impact_score": 8, "sentiment_tag": "Positive",
         "signal_type": "Customer", "headline": "Surfaced",
         "americhem_impact": ".", "source_url": "https://x/2",
         "entities_mentioned": ["Acme"]},
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 3,
            "max_total_visible_articles": 12,
        }
    }
    fake = InMemoryIntelligenceRepo()
    today = _RUN.run_date
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": {}, "suppression_samples": [],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    with patch("delivery_engine._llm", return_value=FakeLLM()):
        prepare_report(rows, None, run=_RUN, report_config=config)

    stored = fake.get_delivery_state(run_date=today, run_mode="production")
    breakdown = stored["suppression_breakdown"]
    assert breakdown["below_impact_threshold"] == 1
    assert stored["surfaced_count"] == 1


def test_prepare_report_update_filtered_by_run_date_and_run_mode(monkeypatch):
    """The update() call must be filtered by run_date AND run_mode."""
    from daily_intelligence_repo import InMemoryIntelligenceRepo

    rows = [{
        "url_hash": "a", "commercial_segment": "Healthcare",
        "americhem_impact_score": 8, "sentiment_tag": "Neutral",
        "signal_type": "Customer", "headline": "H", "americhem_impact": ".",
        "source_url": "https://x/a", "entities_mentioned": ["Acme"],
    }]

    fake = InMemoryIntelligenceRepo()
    today = _RUN.run_date
    fake.upsert_summary({
        "run_date": today, "run_mode": "test",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": {}, "suppression_samples": [],
    })
    update_calls = []
    real_update = fake.update_delivery_counts

    def spy_update(*, run_date, run_mode, surfaced_count, ledger_row):
        update_calls.append({"run_date": run_date, "run_mode": run_mode})
        return real_update(
            run_date=run_date, run_mode=run_mode,
            surfaced_count=surfaced_count, ledger_row=ledger_row,
        )
    fake.update_delivery_counts = spy_update
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    with patch("delivery_engine._llm", return_value=FakeLLM()):
        prepare_report(rows, None, run=_TEST_RUN, report_config={"reporting": {"visible_impact_threshold": 6}})

    assert update_calls, f"Expected update_delivery_counts call. calls={update_calls}"
    keys = set()
    for call in update_calls:
        keys.update(call.keys())
    assert "run_date" in keys, f"calls: {update_calls}"
    assert "run_mode" in keys, f"calls: {update_calls}"
    rm_values = [c["run_mode"] for c in update_calls]
    assert any(v == "test" for v in rm_values), f"Expected run_mode='test' in {rm_values}"


def test_prepare_report_synthesis_sees_only_final_capped_groups(monkeypatch):
    """Thematic synthesis must receive ONLY the final capped groups with 2+
    articles — capped-out rows and single-article segments never reach the LLM."""
    from daily_intelligence_repo import InMemoryIntelligenceRepo

    hc_headlines = [
        "Hospital network merger squeezes specialty polymer volumes",
        "FDA clears new implantable-grade compound for cardiac devices",
        "Aging population drives record demand for medical-grade resins",
    ]
    rows = [
        {"url_hash": f"h{i}", "commercial_segment": "Healthcare",
         "americhem_impact_score": 9 - i, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": hc_headlines[i],
         "americhem_impact": f"Healthcare impact {i}.",
         "source_url": f"https://x/h{i}", "entities_mentioned": ["Acme"]}
        for i in range(3)
    ] + [
        {"url_hash": "p0", "commercial_segment": "Packaging",
         "americhem_impact_score": 8, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": "Single packaging signal",
         "americhem_impact": "Packaging impact.",
         "source_url": "https://x/p0", "entities_mentioned": ["Acme"]},
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 2,
            "max_total_visible_articles": 12,
        }
    }
    fake_llm = FakeLLM(returns={"Healthcare": "Healthcare synthesis."})
    monkeypatch.setattr("delivery_engine._repo", lambda: InMemoryIntelligenceRepo())

    with patch("delivery_engine._llm", return_value=fake_llm):
        model = prepare_report(rows, None, run=_RUN, report_config=config)

    assert len(fake_llm.calls) == 1
    user = fake_llm.calls[-1]["user"]
    assert "CATEGORY: Healthcare" in user
    assert "Healthcare impact 0." in user and "Healthcare impact 1." in user
    assert "Healthcare impact 2." not in user      # capped out by max_per_segment=2
    assert "CATEGORY: Packaging" not in user       # single-article group, no synthesis
    assert model.synthesis == {"Healthcare": "Healthcare synthesis."}


def test_prepare_report_no_news_skips_write_back_and_llm(monkeypatch):
    """The no_news variant performs neither side effect: no daily_summaries
    write-back and no LLM call (the no-news path never wrote back)."""
    repo_touched: list[bool] = []

    def spy_repo():
        repo_touched.append(True)
        from daily_intelligence_repo import InMemoryIntelligenceRepo
        return InMemoryIntelligenceRepo()

    monkeypatch.setattr("delivery_engine._repo", spy_repo)
    fake_llm = FakeLLM()

    with patch("delivery_engine._llm", return_value=fake_llm):
        model = prepare_report([], None, run=_RUN, report_config={})

    assert model.variant == "no_news"
    assert repo_touched == []
    assert fake_llm.calls == []


def _seed_delivery_repo(run_mode: str) -> "InMemoryIntelligenceRepo":
    """InMemory repo with two visible Healthcare rows and the run's summary row."""
    fake = InMemoryIntelligenceRepo(now=lambda: _RUN.now)   # one clock per test
    headlines = [
        "Hospital network merger squeezes specialty polymer volumes",
        "FDA clears new implantable-grade compound for cardiac devices",
    ]
    for i, headline in enumerate(headlines):
        fake.upsert_insight({
            "url_hash": f"wire{i}", "headline": headline,
            "americhem_impact_score": 8, "sentiment_tag": "Neutral",
            "signal_type": "Customer", "commercial_segment": "Healthcare",
            "americhem_impact": "Wiring effect.", "source_url": f"https://x/wire{i}",
            "entities_mentioned": ["Acme"],
        })
    fake.upsert_summary({
        "run_date": _RUN.run_date, "run_mode": run_mode,
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": {}, "suppression_samples": [],
    })
    return fake


def test_delivery_execute_pipeline_wires_prepare_render_and_send(run_delivery_pipeline):
    """End-to-end wiring of delivery's entrypoint: fetch → prepare_report
    (write-back + synthesis, exactly once) → render_report with test_mode from
    the run instant → send_email. Pins the composition itself: swapping
    prepare_report for assemble_report (write-back silently lost) or dropping
    the run→test_mode wiring must fail this test."""
    fake = _seed_delivery_repo("test")

    result = run_delivery_pipeline(
        fake, run=_TEST_RUN,
        llm_returns={"Healthcare": "Wired synthesis paragraph."},
    )

    assert len(result.sent) == 1   # exactly one email per run
    message = result.sent[-1]
    html = message.html
    # run → render wiring: a test-mode run instant marks the HTML body and
    # dates the header; the SAME instant reaches the mailer (the subject).
    assert "[TEST]" in html
    assert "TEST RUN" in html
    assert _TEST_RUN.header_date in html
    assert message.subject.startswith("[TEST] ") and _TEST_RUN.subject_date in message.subject
    # prepare_report ran: its synthesis reached the rendered email...
    assert "Wired synthesis paragraph." in html
    # ...and its write-back landed on the run's daily_summaries row.
    stored = fake.get_delivery_state(run_date=_TEST_RUN.run_date, run_mode="test")
    assert stored is not None and stored["surfaced_count"] == 2


def test_delivery_only_test_run_renders_exec_summary_without_touching_prod_row(run_delivery_pipeline):
    """The run_ingestion=false QA scenario: only a PRODUCTION macro row exists
    (test-mode ingestion never ran). The test-mode delivery must still render
    the executive summary + sources from it, and its write-back must be a
    silent no-op on the production row."""
    import copy

    fake = _seed_delivery_repo("production")   # production row only — no test row
    fake.upsert_summary({
        "run_date": _RUN.run_date, "run_mode": "production",
        "dominant_condition": "Supply Volatility",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
            {"label": "Commercial action",  "body": "C.", "citation_source_ids": []},
        ],
        "executive_sources": [{"id": 1, "headline": "Resin prices climb",
                               "url": "https://s/1", "domain": "s.com",
                               "segment": "Packaging", "score": 9}],
        "suppression_breakdown": {"duplicate_url": 3}, "suppression_samples": [],
    })
    prod_row_before = copy.deepcopy(
        fake.get_delivery_state(run_date=_RUN.run_date, run_mode="production"))

    result = run_delivery_pipeline(fake, run=_TEST_RUN)

    assert len(result.sent) == 1
    html = result.sent[-1].html
    assert "Executive Summary" in html
    assert "Market pressure" in html
    assert "Resin prices climb" in html                 # cited source in the footer
    assert "[TEST]" in html and "TEST RUN" in html      # still marked as QA output
    # Production accounting untouched: write-back keyed run_mode='test' matched
    # no row (silent no-op), and no test row was created.
    assert fake.get_delivery_state(run_date=_RUN.run_date, run_mode="production") == prod_row_before
    assert fake.get_delivery_state(run_date=_TEST_RUN.run_date, run_mode="test") is None


def test_delivery_execute_pipeline_production_run_ships_unmarked_html(run_delivery_pipeline):
    """The inverse wiring check: with a production run instant, the sent
    HTML carries no test markers (a hardcoded test_mode=True must fail here)."""
    fake = _seed_delivery_repo("production")

    result = run_delivery_pipeline(fake, run=_RUN)

    assert len(result.sent) == 1
    assert "[TEST]" not in result.sent[-1].html
    assert "TEST RUN" not in result.sent[-1].html
    assert "[TEST]" not in result.sent[-1].subject


def test_render_executive_bullets_renders_three_labeled_bullets():
    from delivery_engine import _render_executive_bullets
    bullets = [
        {"label": "Market pressure",    "body": "Techmer raised prices."},
        {"label": "Supply chain watch", "body": "Mitsubishi restructuring."},
        {"label": "Commercial action",  "body": "Prioritize additives."},
    ]
    html = _render_executive_bullets(bullets)
    assert "Market pressure" in html
    assert "Supply chain watch" in html
    assert "Commercial action" in html
    assert "Techmer raised prices." in html
    assert "Mitsubishi restructuring." in html
    assert "Prioritize additives." in html


def test_render_exec_summary_uses_structured_bullets_when_present():
    from delivery_engine import _render_exec_summary
    macro = {
        "dominant_condition": "Competitive Pressure",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
        "executive_summary": "Should not be used.",
    }
    html = _render_exec_summary(macro)
    assert "Market pressure" in html
    assert "A." in html
    assert "Should not be used." not in html
    assert "Competitive Pressure" in html  # condition badge


def test_render_exec_summary_falls_back_to_legacy_when_bullets_null():
    from delivery_engine import _render_exec_summary
    macro = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": None,
        "executive_summary": "Legacy prose summary used.",
    }
    html = _render_exec_summary(macro)
    assert "Legacy prose summary used." in html
    assert "Market pressure" not in html


def test_render_exec_summary_no_summary_returns_empty():
    from delivery_engine import _render_exec_summary
    assert _render_exec_summary(None) == ""
    assert _render_exec_summary({}) == ""


# ===========================================================================
# Null-safe header fallbacks (screened_count, dominant_condition)
# ===========================================================================


def test_header_falls_back_to_len_data_when_screened_null():
    """When screened_count is NULL, header uses len(data)."""
    rows = [
        {"url_hash": f"h{i}", "commercial_segment": "Healthcare",
         "americhem_impact_score": 8, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": f"Distinct Healthcare News {i}",
         "americhem_impact": ".", "source_url": f"https://x/{i}",
         "entities_mentioned": ["Acme"]}
        for i in range(7)
    ]
    macro = {"executive_bullets": [
        {"label": "Market pressure",    "body": "A."},
        {"label": "Supply chain watch", "body": "B."},
        {"label": "Commercial action",  "body": "C."},
    ], "dominant_condition": "Competitive Pressure",
       "screened_count": None, "surfaced_count": None}

    model = assemble_report(rows, macro, config={"reporting": {"visible_impact_threshold": 6}})
    html = render_report(model, today_str=_TODAY_STR)

    assert "from 7 screened items" in html
    assert "from None screened items" not in html


def test_header_omits_dominant_condition_clause_when_null():
    """When dominant_condition is NULL, the badge clause is omitted (no literal 'None')."""
    rows = [{"url_hash": "a", "commercial_segment": "Healthcare",
             "americhem_impact_score": 8, "sentiment_tag": "Neutral",
             "signal_type": "Customer", "headline": "Some Distinct Headline",
             "americhem_impact": ".", "source_url": "https://x/a",
             "entities_mentioned": ["Acme"]}]
    macro = {"executive_bullets": None, "executive_summary": "Fallback prose.",
             "dominant_condition": None, "macro_sentiment": None,
             "screened_count": 5, "surfaced_count": 1}

    model = assemble_report(rows, macro, config={"reporting": {"visible_impact_threshold": 6}})
    html = render_report(model, today_str=_TODAY_STR)

    # The literal string 'None' must not appear anywhere as a rendered value.
    assert ">None<" not in html
    assert "Dominant condition: None" not in html


# ===========================================================================
# QA suppression-summary section
# ===========================================================================


def test_qa_debug_section_appears_in_test_mode():
    rows = [{"url_hash": "a", "commercial_segment": "Healthcare",
             "americhem_impact_score": 8, "sentiment_tag": "Neutral",
             "signal_type": "Customer", "headline": "Some Distinct QA Headline",
             "americhem_impact": ".", "source_url": "https://x/a",
             "entities_mentioned": ["Acme"]}]
    macro = {
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
        "dominant_condition": "Competitive Pressure",
        "screened_count": 87,
        "surfaced_count": 1,
        "suppression_breakdown": {
            "duplicate_url": 23,
            "llm_discard": 12,
            "product_listing": 5,
            "job_posting": 3,
        },
        "suppression_samples": [
            {"reason": "product_listing", "url": "https://amazon.com/product/1",
             "title": "Pretty plastic tote"},
            {"reason": "llm_discard", "url": "https://news.com/extension-cord",
             "title": "Best extension cord colors"},
        ],
    }

    model = assemble_report(rows, macro, config={"reporting": {"visible_impact_threshold": 6}})
    html = render_report(model, today_str=_TODAY_STR, test_mode=True)

    assert "QA" in html
    assert "Suppression Summary" in html
    # Friendly labels expected (Task 14 spec uses friendly forms in the email).
    assert "duplicate URL" in html
    assert "product listing" in html
    assert "Pretty plastic tote" in html
    assert "Best extension cord colors" in html


def test_qa_debug_section_absent_in_production():
    rows = [{"url_hash": "a", "commercial_segment": "Healthcare",
             "americhem_impact_score": 8, "sentiment_tag": "Neutral",
             "signal_type": "Customer", "headline": "Production Distinct Headline",
             "americhem_impact": ".", "source_url": "https://x/a",
             "entities_mentioned": ["Acme"]}]
    macro = {
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
        "dominant_condition": "Competitive Pressure",
        "screened_count": 87,
        "surfaced_count": 1,
        "suppression_breakdown": {"duplicate_url": 23, "product_listing": 5},
        "suppression_samples": [{"reason": "product_listing",
                                 "url": "https://amazon.com/product/1",
                                 "title": "Pretty plastic tote"}],
    }

    model = assemble_report(rows, macro, config={"reporting": {"visible_impact_threshold": 6}})
    html = render_report(model, today_str=_TODAY_STR, test_mode=False)

    assert "Suppression Summary" not in html
    assert "Pretty plastic tote" not in html


def test_render_qa_debug_section_uses_friendly_labels():
    from delivery_engine import _render_qa_debug_section
    macro = {
        "screened_count": 87,
        "surfaced_count": 6,
        "suppression_breakdown": {
            "duplicate_url": 23,
            "semantic_duplicate": 4,
            "llm_discard": 12,
            "enterprise_cross_segment_low_impact": 3,
        },
        "suppression_samples": [
            {"reason": "duplicate_url", "url": "https://x/1", "title": "Dup"},
        ],
    }
    html = _render_qa_debug_section(macro)
    assert "duplicate URL" in html
    assert "semantic duplicate" in html
    assert "LLM discard" in html
    assert "Enterprise / Cross-Segment" in html


def test_render_qa_debug_section_includes_relevance_gate_drops():
    """The ZoomInfo relevance-gate code must get a labeled breakdown row so its
    count is visible during the test-pipeline validation run (not just folded
    into the suppressed total)."""
    from delivery_engine import _render_qa_debug_section
    macro = {
        "screened_count": 40,
        "surfaced_count": 5,
        "suppression_breakdown": {"zoominfo_company_mismatch": 3},
        "suppression_samples": [],
    }
    html = _render_qa_debug_section(macro)
    assert "ZoomInfo company mismatch" in html
    assert ">3</td>" in html


# ===========================================================================
# idempotent suppression breakdown on same-day retries
# ===========================================================================


def test_update_delivery_summary_counts_overwrites_delivery_keys(monkeypatch):
    """Delivery-owned keys must be REPLACED, not added, on retry. Ingestion-owned
    keys must be preserved unchanged."""
    from suppression_ledger import SuppressionLedger
    from delivery_engine import _update_delivery_summary_counts
    from daily_intelligence_repo import InMemoryIntelligenceRepo

    prior = {
        "duplicate_url": 10,            # ingestion-owned
        "semantic_duplicate": 2,        # ingestion-owned
        "below_impact_threshold": 22,   # delivery-owned (must be replaced)
        "weak_relevance": 7,            # delivery-owned (must be replaced)
    }

    fake = InMemoryIntelligenceRepo()
    today = _RUN.run_date
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": prior,
        "suppression_samples": [],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    ledger = (SuppressionLedger.for_delivery()
              .record_count("below_impact_threshold", 5)
              .record_count("weak_relevance", 2))

    _update_delivery_summary_counts(run=_RUN, surfaced_count=6, ledger=ledger)

    stored = fake.get_delivery_state(run_date=today, run_mode="production")
    merged = stored["suppression_breakdown"]
    # Ingestion-owned keys preserved unchanged:
    assert merged["duplicate_url"] == 10
    assert merged["semantic_duplicate"] == 2
    # Delivery-owned keys REPLACED (not added):
    assert merged["below_impact_threshold"] == 5, "delivery-owned count must be overwritten, not added"
    assert merged["weak_relevance"] == 2


def test_update_delivery_summary_counts_idempotent_on_retry(monkeypatch):
    """Two consecutive calls with the same ledger must produce the same
    final breakdown — no doubling."""
    from suppression_ledger import SuppressionLedger
    from delivery_engine import _update_delivery_summary_counts
    from daily_intelligence_repo import InMemoryIntelligenceRepo

    fake = InMemoryIntelligenceRepo()
    today = _RUN.run_date
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": {"duplicate_url": 10},
        "suppression_samples": [],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    ledger = (SuppressionLedger.for_delivery()
              .record_count("below_impact_threshold", 22)
              .record("product_listing", url="https://amazon.com/p/1", title="Plastic tote")
              .record_count("product_listing", 4))  # total product_listing = 5

    _update_delivery_summary_counts(run=_RUN, surfaced_count=6, ledger=ledger)
    stored = fake.get_delivery_state(run_date=today, run_mode="production")
    first_breakdown = dict(stored["suppression_breakdown"])
    first_samples = list(stored["suppression_samples"])

    _update_delivery_summary_counts(run=_RUN, surfaced_count=6, ledger=ledger)
    stored = fake.get_delivery_state(run_date=today, run_mode="production")
    second_breakdown = dict(stored["suppression_breakdown"])
    second_samples = list(stored["suppression_samples"])

    assert first_breakdown == second_breakdown, \
        f"Retry must be idempotent. First={first_breakdown} Second={second_breakdown}"
    assert second_breakdown["below_impact_threshold"] == 22, "must not double"
    assert second_breakdown["product_listing"] == 5, "must not double"
    assert second_breakdown["duplicate_url"] == 10, "ingestion-owned key preserved"
    assert first_samples == second_samples, \
        f"Retry must not duplicate samples. First={first_samples} Second={second_samples}"


def test_update_delivery_summary_counts_preserves_unknown_prior_keys(monkeypatch):
    """Unknown keys in the existing breakdown (e.g., future codes) must be preserved."""
    from suppression_ledger import SuppressionLedger
    from delivery_engine import _update_delivery_summary_counts
    from daily_intelligence_repo import InMemoryIntelligenceRepo

    prior = {"some_future_reason": 99, "duplicate_url": 5}
    fake = InMemoryIntelligenceRepo()
    today = _RUN.run_date
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": prior,
        "suppression_samples": [],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    ledger = SuppressionLedger.for_delivery().record_count("below_impact_threshold", 2)

    _update_delivery_summary_counts(run=_RUN, surfaced_count=1, ledger=ledger)

    stored = fake.get_delivery_state(run_date=today, run_mode="production")
    merged = stored["suppression_breakdown"]
    assert merged["some_future_reason"] == 99
    assert merged["duplicate_url"] == 5
    assert merged["below_impact_threshold"] == 2


def test_delivery_suppression_idempotent_on_same_day_retry(monkeypatch):
    """Running delivery twice in the same day with the same inputs must
    produce identical persisted breakdown and samples."""
    from suppression_ledger import SuppressionLedger
    from delivery_engine import _update_delivery_summary_counts
    from daily_intelligence_repo import InMemoryIntelligenceRepo

    fake = InMemoryIntelligenceRepo()
    today = _RUN.run_date
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": {},
        "suppression_samples": [],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    ledger = (SuppressionLedger.for_delivery()
              .record("duplicate_headline", url="u", title="t")
              .record_count("below_impact_threshold", 3))

    _update_delivery_summary_counts(run=_RUN, surfaced_count=5, ledger=ledger)
    stored = fake.get_delivery_state(run_date=today, run_mode="production")
    first_breakdown = dict(stored["suppression_breakdown"])
    first_samples = list(stored["suppression_samples"])

    _update_delivery_summary_counts(run=_RUN, surfaced_count=5, ledger=ledger)
    stored = fake.get_delivery_state(run_date=today, run_mode="production")
    second_breakdown = dict(stored["suppression_breakdown"])
    second_samples = list(stored["suppression_samples"])

    assert first_breakdown == second_breakdown, \
        f"Retry must be idempotent. First={first_breakdown} Second={second_breakdown}"
    assert first_samples == second_samples, \
        f"Retry must not duplicate samples. First={first_samples} Second={second_samples}"


# ===========================================================================
# Prompt text crosses the LLM seam unchanged
# ===========================================================================


def test_synthesize_thematic_ships_prompts_module_text_across_seam():
    """Seam-crossing check: the thematic system/user text crossing the LLM seam
    is exactly what prompts.thematic_prompt assembles."""
    import prompts

    groups = {"Healthcare": [
        _make_article("a", 8, "competitors"),
        _make_article("b", 7, "competitors"),
    ]}
    fake = FakeLLM(returns=None)
    with patch("delivery_engine._llm", return_value=fake):
        synthesize_thematic_paragraphs(groups)

    spec = prompts.thematic_prompt(groups)
    assert fake.calls[-1]["system"] == spec.system
    assert fake.calls[-1]["user"] == spec.user


# ===========================================================================
# Repository wiring — delivery paths route through _repo()
# ===========================================================================


def test_fetch_todays_intelligence_routes_through_repo(monkeypatch):
    """fetch_todays_intelligence returns repo.fetch_since rows verbatim
    (alert_tier decoration is no longer this function's job)."""
    from delivery_engine import fetch_todays_intelligence
    fake = InMemoryIntelligenceRepo()
    fake.upsert_insight({
        "url_hash": "a", "headline": "Alpha",
        "americhem_impact_score": 8, "sentiment_score": 7,
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    rows = fetch_todays_intelligence(_RUN)
    assert len(rows) == 1
    assert rows[0]["headline"] == "Alpha"
    assert "alert_tier" not in rows[0]   # decoration moved to caller


def test_fetch_todays_intelligence_uses_72h_on_monday(monkeypatch):
    """Monday detection still drives the wall-clock fallback when no prior
    production delivery is recorded (the anchored path is covered in
    tests/test_delivery_window.py)."""
    import delivery_engine
    fake = MagicMock(spec=InMemoryIntelligenceRepo)
    fake.fetch_last_delivery.return_value = None
    fake.fetch_since.return_value = []
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    fixed_monday = datetime(2026, 5, 25, 9, 0, 0)  # Monday
    delivery_engine.fetch_todays_intelligence(replace(_RUN, now=fixed_monday))
    fake.fetch_since.assert_called_once_with(fixed_monday - timedelta(hours=72))


def test_fetch_macro_summary_routes_through_repo(monkeypatch):
    """fetch_macro_summary returns repo.fetch_latest_summary verbatim."""
    from delivery_engine import fetch_macro_summary
    fake = InMemoryIntelligenceRepo()
    today = _RUN.run_date
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "today's summary", "macro_sentiment": "x",
        "dominant_condition": "Mixed / Watch",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    got = fetch_macro_summary(_RUN)
    assert got is not None
    assert got["executive_summary"] == "today's summary"


def test_fetch_macro_summary_returns_none_when_missing(monkeypatch):
    from delivery_engine import fetch_macro_summary
    fake = InMemoryIntelligenceRepo()
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)
    assert fetch_macro_summary(_RUN) is None


def test_update_delivery_summary_counts_merges_with_prior(monkeypatch):
    """The same-day-retry merge: prior delivery counts are preserved through
    ingestion-owned codes; new delivery-owned codes overwrite."""
    from delivery_engine import _update_delivery_summary_counts
    from suppression_ledger import SuppressionLedger

    fake = InMemoryIntelligenceRepo()
    today = _RUN.run_date
    # Seed a prior row mimicking ingestion having already written.
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
        "suppression_breakdown": {"duplicate_url": 5, "below_impact_threshold": 9},
        "suppression_samples": [{"reason": "duplicate_url", "url": "u", "title": "t"}],
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    new_ledger = (
        SuppressionLedger.for_delivery()
        .record_count("below_impact_threshold", 3)
        .record_count("product_listing", 1)
    )
    _update_delivery_summary_counts(run=_RUN, surfaced_count=4, ledger=new_ledger)

    got = fake.get_delivery_state(run_date=today, run_mode="production")
    assert got["surfaced_count"] == 4
    # Ingestion-owned code preserved from prior.
    assert got["suppression_breakdown"]["duplicate_url"] == 5
    # Delivery-owned code overwritten by this run.
    assert got["suppression_breakdown"]["below_impact_threshold"] == 3
    assert got["suppression_breakdown"]["product_listing"] == 1


def test_update_delivery_summary_counts_swallows_write_failure(monkeypatch, caplog):
    """A failed metadata write must not block the email — preserves the
    existing 'Non-critical' operational decision."""
    from delivery_engine import _update_delivery_summary_counts
    from suppression_ledger import SuppressionLedger

    failing = MagicMock()
    failing.get_delivery_state.return_value = None
    failing.update_delivery_counts.side_effect = RuntimeError("DB down")
    monkeypatch.setattr("delivery_engine._repo", lambda: failing)

    # Should not raise.
    _update_delivery_summary_counts(
        run=_RUN,
        surfaced_count=0,
        ledger=SuppressionLedger.for_delivery(),
    )
    assert "Failed to update delivery counts" in caplog.text


def test_update_delivery_summary_counts_aborts_write_on_prior_read_failure(monkeypatch, caplog):
    """If require_delivery_state raises, the caller must NOT call
    update_delivery_counts — otherwise the write would overwrite prior
    ingestion-owned suppression state with an empty ledger."""
    from delivery_engine import _update_delivery_summary_counts
    from suppression_ledger import SuppressionLedger

    class _ReadFailingRepo:
        def require_delivery_state(self, *, run_date, run_mode):
            raise RuntimeError("read failed")

        def update_delivery_counts(self, *args, **kwargs):
            raise AssertionError(
                "update_delivery_counts must NOT be called after prior-state "
                "read failure — otherwise prior suppression state is overwritten"
            )

    monkeypatch.setattr("delivery_engine._repo", lambda: _ReadFailingRepo())

    # Must not raise.
    _update_delivery_summary_counts(
        run=_RUN,
        surfaced_count=4,
        ledger=SuppressionLedger.for_delivery(),
    )
    assert "Failed to update delivery counts" in caplog.text


def test_update_delivery_summary_counts_writes_when_no_prior_row(monkeypatch):
    """When require_delivery_state returns None (no prior row), the write
    must still proceed — that's the fresh-row path, not a failure."""
    from delivery_engine import _update_delivery_summary_counts
    from suppression_ledger import SuppressionLedger

    fake = InMemoryIntelligenceRepo()
    today = _RUN.run_date
    # Seed a row so update_delivery_counts has somewhere to write
    # (the in-memory fake's update is silent no-op without a row, mimicking
    # Supabase UPDATE-WHERE-no-match). For the fresh-row case in production,
    # daily_summaries already has an ingestion-written row before delivery
    # rendering — we mimic that here without seeding any suppression state.
    fake.upsert_summary({
        "run_date": today, "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "x",
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake)

    ledger = SuppressionLedger.for_delivery().record_count("below_impact_threshold", 2)
    _update_delivery_summary_counts(run=_RUN, surfaced_count=3, ledger=ledger)

    got = fake.get_delivery_state(run_date=today, run_mode="production")
    assert got["surfaced_count"] == 3
    assert got["suppression_breakdown"] == {"below_impact_threshold": 2}


# ===========================================================================
# Citation rendering and the Sources footer
# ===========================================================================

def test_safe_http_url_allows_http_and_https():
    assert _safe_http_url("https://x.com/a") == "https://x.com/a"
    assert _safe_http_url("http://x.com/a") == "http://x.com/a"


def test_safe_http_url_rejects_other_schemes():
    assert _safe_http_url("javascript:alert(1)") == ""
    assert _safe_http_url("data:text/html,x") == ""
    assert _safe_http_url("") == ""
    assert _safe_http_url(None) == ""


def test_render_bullets_inline_citation_is_grouped_and_linked():
    bullets = [
        {"label": "Market pressure", "body": "Pricing firm.", "citation_source_ids": [5, 8]},
        {"label": "Supply chain watch", "body": "Freight up.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "Watch.", "citation_source_ids": []},
    ]
    sources = [_src(5, url="https://a.com/x"), _src(8, url="https://b.com/y")]
    dmap = _citation_display_map(bullets, sources)
    html_out = _render_executive_bullets(bullets, sources, dmap)
    assert "Pricing firm." in html_out
    assert 'href="https://a.com/x"' in html_out
    assert 'title="https://a.com/x"' in html_out
    assert ">1</a>" in html_out and ">2</a>" in html_out
    # Grouped: a comma separates the two numbers, enclosed in brackets.
    assert "[" in html_out and ", " in html_out and "]" in html_out


def test_render_bullets_no_citation_when_empty():
    bullets = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": []},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    html_out = _render_executive_bullets(bullets, [], {})
    assert "<a" not in html_out


def test_render_bullets_escapes_malicious_url_and_headline():
    bullets = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": [1]},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    # javascript: scheme must be dropped -> number rendered as plain text, no href.
    sources = [_src(1, url="javascript:alert(1)")]
    dmap = _citation_display_map(bullets, sources)
    html_out = _render_executive_bullets(bullets, sources, dmap)
    assert "javascript:alert(1)" not in html_out
    assert "href=" not in html_out
    assert ">1<" in html_out or "[1]" in html_out  # number still shown, just unlinked


def test_render_sources_footer_orders_and_escapes():
    bullets = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": [8, 5]},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    sources = [
        _src(5, headline="Resin <b>up</b>", url="https://a.com/x", domain="a.com"),
        _src(8, headline="Freight", url="https://b.com/y", domain="b.com"),
    ]
    dmap = _citation_display_map(bullets, sources)
    footer = _render_sources_footer(sources, dmap)
    # Display order follows first appearance: 8 -> [1], 5 -> [2].
    assert footer.index("Freight") < footer.index("Resin")
    assert "b.com" in footer and "a.com" in footer
    assert "<b>up</b>" not in footer        # escaped
    assert "&lt;b&gt;up&lt;/b&gt;" in footer


def test_render_sources_footer_empty_when_no_citations():
    assert _render_sources_footer([], {}) == ""


def test_render_sources_footer_handles_missing_url_gracefully():
    bullets = [{"label": "Market pressure", "body": "A.", "citation_source_ids": [1]}]
    sources = [_src(1, headline="", url="", domain="")]
    dmap = _citation_display_map(bullets, sources)
    footer = _render_sources_footer(sources, dmap)
    assert footer != ""               # does not crash, still renders a row
    assert "href=" not in footer      # no valid URL -> unlinked


def test_render_bullets_escapes_html_metacharacters_in_body():
    bullets = [
        {"label": "Market pressure", "body": "Margins fell <5% as AT&T cut orders.",
         "citation_source_ids": []},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    html_out = _render_executive_bullets(bullets, [], {})
    assert "<5%" not in html_out
    assert "&lt;5%" in html_out
    assert "AT&amp;T" in html_out


def test_render_marker_mixes_linked_and_unlinked_by_url_safety():
    bullets = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": [1, 2]},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    sources = [
        _src(1, url="javascript:alert(1)"),   # unsafe -> plain text [1]
        _src(2, url="https://safe.com/y"),     # safe -> linked [2]
    ]
    dmap = _citation_display_map(bullets, sources)
    html_out = _render_executive_bullets(bullets, sources, dmap)
    assert 'href="https://safe.com/y"' in html_out   # id 2 linked
    assert ">2</a>" in html_out
    assert "javascript:alert(1)" not in html_out      # id 1 not linked
    # id 1's display number 1 appears as plain text inside the marker, not as a link
    assert ">1</a>" not in html_out


def test_exec_summary_renders_inline_citations_and_footer():
    macro = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Pricing firm.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Freight up.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Watch.", "citation_source_ids": []},
        ],
        "executive_sources": [
            {"id": 1, "headline": "Resin prices climb", "url": "https://reuters.com/x",
             "domain": "reuters.com", "segment": "Auto", "score": 8},
        ],
    }
    html_out = _render_exec_summary(macro)
    # Inline citation marker + link stay in the executive summary block...
    assert "Pricing firm." in html_out
    assert 'href="https://reuters.com/x"' in html_out
    assert ">1</a>" in html_out
    # ...but the Sources list itself now lives in its own bottom-of-email section,
    # NOT inside the executive summary block.
    assert "Sources" not in html_out
    assert "Resin prices climb" not in html_out


def test_exec_summary_legacy_row_renders_without_footer():
    # Old row: bullets without citation_source_ids, no executive_sources.
    macro = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action", "body": "C."},
        ],
    }
    html_out = _render_exec_summary(macro)
    assert "A." in html_out
    assert "Sources" not in html_out
    assert "<a" not in html_out


def test_exec_summary_prose_fallback_unchanged():
    macro = {"executive_summary": "Prose summary.", "dominant_condition": "Low Signal"}
    html_out = _render_exec_summary(macro)
    assert "Prose summary." in html_out
    assert "Sources" not in html_out


def test_exec_summary_legacy_string_bullets_fall_back_to_prose():
    # Legacy/malformed row: executive_bullets is a truthy list of strings (not
    # dicts) AND prose is present. The structured citation path would render
    # blank "• :" rows, so we must fall through to the legacy prose instead.
    macro = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": ["Market pressure: pricing firm.", "Freight up.", "Watch."],
        "executive_summary": "Prose summary stands in.",
    }
    html_out = _render_exec_summary(macro)
    assert "Prose summary stands in." in html_out
    assert "Sources" not in html_out
    assert "<a" not in html_out


def _macro_with_citations():
    return {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Pricing firm.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Freight up.", "citation_source_ids": [2]},
            {"label": "Commercial action", "body": "Watch.", "citation_source_ids": []},
        ],
        "executive_sources": [
            {"id": 1, "headline": "Resin prices climb", "url": "https://reuters.com/x",
             "domain": "reuters.com", "segment": "Auto", "score": 8},
            {"id": 2, "headline": "Freight rates spike", "url": "https://icis.com/y",
             "domain": "icis.com", "segment": "Auto", "score": 7},
        ],
    }


def test_render_sources_section_renders_footer_when_cited():
    html_out = _render_sources_section(_macro_with_citations())
    assert "Sources" in html_out
    assert "Resin prices climb" in html_out
    assert "reuters.com" in html_out
    assert 'href="https://reuters.com/x"' in html_out
    # Wrapped as a full-width email row so it sits in the outer email table.
    assert "<tr>" in html_out and "<td" in html_out


def test_render_sources_section_empty_for_legacy_and_uncited():
    # No structured bullets / no executive_sources -> no bottom Sources section.
    assert _render_sources_section(None) == ""
    assert _render_sources_section({"executive_summary": "Prose."}) == ""
    assert _render_sources_section({
        "executive_bullets": ["string bullet"],
        "executive_summary": "Prose.",
    }) == ""
    assert _render_sources_section({
        "executive_bullets": [
            {"label": "Market pressure", "body": "A.", "citation_source_ids": []},
            {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
        ],
        "executive_sources": [
            {"id": 1, "headline": "Unused", "url": "https://x.com/a", "domain": "x.com"},
        ],
    }) == ""


def test_sources_section_numbering_matches_inline_markers():
    macro = _macro_with_citations()
    exec_html = _render_exec_summary(macro)
    sources_html = _render_sources_section(macro)
    # Inline markers show [1] and [2]; the footer lists [1] and [2] for the same
    # sources (shared deterministic display map).
    assert ">1</a>" in exec_html and ">2</a>" in exec_html
    assert "[1]" in sources_html and "[2]" in sources_html
    assert sources_html.index("Resin prices climb") < sources_html.index("Freight rates spike")


def test_citation_numbering_is_one_space_across_bullets_signals_and_footer():
    """The invariant the CitationSet exists to make structural: the executive
    bullets, the Macroeconomic Outlook and the Sources footer share ONE display
    numbering space.

    Both sides are derived from the bytes the renderer emitted — the inline
    markers are parsed out of the superscript spans, the footer numbers out of
    the '[n] headline &mdash; domain' rows. Nothing is hand-copied, so deleting
    a section or renumbering either side fails this test rather than passing on
    a stale constant.
    """
    import re

    macro = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Pricing firm.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Freight up.", "citation_source_ids": [2]},
            {"label": "Commercial action", "body": "Watch.", "citation_source_ids": []},
        ],
        "macro_outlook": {
            "current_condition": "Demand soft, costs firm.",
            "signals": [{
                "indicator": "Producer prices",
                "direction": "Rising",
                "americhem_implication": "Cost-side pressure into Q3.",
                "affected_segments": ["Packaging"],
                # cites a source NO bullet cites, plus one a bullet does — so the
                # numbering must continue across the two sections, not restart.
                "citation_source_ids": [3, 1],
            }],
        },
        "executive_sources": [
            {"id": 1, "headline": "Resin prices climb", "url": "https://reuters.com/x",
             "domain": "reuters.com", "segment": "Packaging", "score": 8},
            {"id": 2, "headline": "Freight rates spike", "url": "https://icis.com/y",
             "domain": "icis.com", "segment": "Packaging", "score": 7},
            {"id": 3, "headline": "PPI ticks up", "url": "https://bls.example/z",
             "domain": "bls.example", "segment": "Packaging", "score": 6},
        ],
    }
    data = [{
        "headline": "Packaging market update",
        "source_url": "https://example.com/card",
        "americhem_impact": "Pricing pressure on packaging.",
        "americhem_impact_score": 8,
        "sentiment_tag": "Negative",
        "commercial_segment": "Packaging",
        "signal_type": "Pricing",
    }]
    html = render_report(assemble_report(data, macro), today_str=_TODAY_STR)

    def _inline_numbers(fragment: str) -> set[int]:
        """Display numbers inside the superscript citation markers of a fragment
        (tags stripped, so a linked '1' and a plain '1' read the same)."""
        blocks = re.findall(r'vertical-align:super;">\[(.*?)\]</span>', fragment, re.S)
        return {int(n) for blk in blocks
                for n in re.findall(r"\d+", re.sub(r"<[^>]*>", "", blk))}

    inline = _inline_numbers(html)
    footer = [int(n) for n in re.findall(r"\[(\d+)\] [^<]*?&mdash;", html)]

    # Guard against a vacuous pass: both sections and the footer must be present.
    assert "MACROECONOMIC OUTLOOK" in html
    assert inline, "no inline citation markers rendered"
    assert footer, "no Sources footer rendered"

    # The footer is exactly the inline numbering, listed 1..N in order.
    assert inline == set(footer)
    assert footer == list(range(1, len(footer) + 1))

    # ...and it is genuinely ONE space: the outlook's signal contributes a number
    # above every bullet number rather than restarting at 1.
    outlook_html = html[html.index("MACROECONOMIC OUTLOOK"):html.index("COMMERCIAL SEGMENT WATCH")]
    exec_html = html[:html.index("MACROECONOMIC OUTLOOK")]
    assert max(_inline_numbers(outlook_html)) > max(_inline_numbers(exec_html))


def test_report_places_sources_at_bottom():
    macro = _macro_with_citations()
    data = [{
        "headline": "Packaging market update",  # distinct from the source headline
        "source_url": "https://example.com/card",
        "americhem_impact": "Pricing pressure on packaging.",
        "americhem_impact_score": 8,
        "sentiment_tag": "Negative",
        "commercial_segment": "Packaging",
        "signal_type": "Pricing",
    }]
    html = render_report(assemble_report(data, macro), today_str=_TODAY_STR)

    # The cited-source headline now appears only in the bottom Sources section
    # (it was removed from the executive summary block), exactly once.
    assert html.count("Resin prices climb") == 1
    assert "Sources" in html
    # Sources block sits AFTER the executive summary block (moved to the bottom).
    assert html.index("Executive Summary") < html.index("Resin prices climb")
    assert html.index("Pricing firm.") < html.index("Resin prices climb")


def test_exec_summary_sources_present_but_none_cited_renders_no_footer():
    # executive_sources is non-empty, but no bullet cites any id -> empty display
    # map -> no inline markers and no orphan Sources footer.
    macro = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "A.", "citation_source_ids": []},
            {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
        ],
        "executive_sources": [
            {"id": 1, "headline": "Unused", "url": "https://x.com/a",
             "domain": "x.com", "segment": "Auto", "score": 7},
        ],
    }
    html_out = _render_exec_summary(macro)
    assert "A." in html_out
    assert "Sources" not in html_out
    assert "<a" not in html_out
    assert "Unused" not in html_out   # uncited source never leaks into output


# ===========================================================================
# QA debug section — unscrapable-domain row
# ===========================================================================


def test_render_qa_debug_section_includes_unscrapable_domain():
    """The unscrapable_domain code must get a labeled breakdown row in the QA
    debug section (not just fold into the suppressed total)."""
    from delivery_engine import _render_qa_debug_section
    macro = {
        "screened_count": 40,
        "surfaced_count": 5,
        "suppression_breakdown": {"unscrapable_domain": 4},
        "suppression_samples": [],
    }
    html = _render_qa_debug_section(macro)
    assert "unscrapable domain" in html
    assert ">4</td>" in html


def test_render_qa_debug_section_lists_every_ledger_code():
    """The QA breakdown's display order is derived from the ledger taxonomy,
    so a code added to suppression_ledger (issue #73's
    appendix_excluded_category, and synthesis_failed before it) gets a labeled
    row automatically instead of silently folding into the suppressed total.
    Both sides of the comparison are derived — nothing hand-listed."""
    from delivery_engine import _render_qa_debug_section
    from suppression_ledger import DELIVERY_CODES, INGESTION_CODES, label_for
    codes = sorted(INGESTION_CODES | DELIVERY_CODES)
    macro = {
        "screened_count": 200,
        "surfaced_count": 5,
        "suppression_breakdown": {code: i + 1 for i, code in enumerate(codes)},
        "suppression_samples": [],
    }
    html = _render_qa_debug_section(macro)
    for i, code in enumerate(codes):
        assert f"{label_for(code)}</td>" in html, code
        assert f">{i + 1}</td>" in html, code


def test_render_qa_debug_section_orders_ingestion_codes_before_delivery_codes():
    """Stable reading order for the QA strip: ingestion-side rows first, then
    delivery-side, each in taxonomy order."""
    from delivery_engine import _render_qa_debug_section
    from suppression_ledger import label_for
    macro = {
        "screened_count": 10,
        "surfaced_count": 1,
        "suppression_breakdown": {
            "appendix_excluded_category": 2,   # delivery (last in taxonomy)
            "below_impact_threshold": 3,       # delivery (first in taxonomy)
            "synthesis_failed": 1,             # ingestion
        },
        "suppression_samples": [],
    }
    html = _render_qa_debug_section(macro)
    positions = [html.index(label_for(c)) for c in
                 ("synthesis_failed", "below_impact_threshold", "appendix_excluded_category")]
    assert positions == sorted(positions)
