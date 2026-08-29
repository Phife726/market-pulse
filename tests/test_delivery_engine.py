"""delivery_engine.py — Fetch → Format → Send.

The fetches and their run-mode isolation, the summary write-back,
`synthesize_thematic_paragraphs`, `send_email` on the mailer seam, and
`execute_pipeline`. Rendering lives in `renderer.py` and its tests in
`tests/test_renderer.py`; a test here that reads HTML reads what crossed the
mailer.
"""

from dataclasses import replace
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

import pytest

from llm import FakeLLM
from tests.conftest import (
    RUN_INSTANT as _RUN,
    TEST_RUN_INSTANT as _TEST_RUN,
    VALID_MACRO_OUTLOOK,
    VISIBLE_6_CFG,
    stub_row,
)
from delivery_engine import (
    synthesize_thematic_paragraphs,
    prepare_report,
    send_email as _send_email,
)
from daily_intelligence_repo import InMemoryIntelligenceRepo


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


def test_fetch_macro_summary_passes_macro_outlook_through(monkeypatch):
    """Delivery's fetch_macro_summary returns the row verbatim, so macro_outlook
    (incl. the test-mode production-row fallback) is carried along for free."""
    import delivery_engine
    fake_repo = InMemoryIntelligenceRepo()
    fake_repo.upsert_summary({
        "run_date": _RUN.run_date,
        "run_mode": "production",
        "executive_summary": "x", "macro_sentiment": "Mixed / Watch",
        "macro_outlook": VALID_MACRO_OUTLOOK,
    })
    monkeypatch.setattr("delivery_engine._repo", lambda: fake_repo)
    summary = delivery_engine.fetch_macro_summary(_RUN)
    assert summary["macro_outlook"] == VALID_MACRO_OUTLOOK


# ===========================================================================
# synthesize_thematic_paragraphs()
# ===========================================================================


def test_synthesize_thematic_paragraphs_returns_paragraphs():
    """Returns dict of {category: paragraph} on success."""
    groups = {
        "competitors": [
            stub_row("a", 8, category="competitors"),
            stub_row("b", 7, category="competitors"),
        ]
    }
    expected = {"competitors": "Avient and Techmer raised prices."}
    mock_client = FakeLLM(returns=expected)

    with patch("delivery_engine._llm", return_value=mock_client):
        result = synthesize_thematic_paragraphs(groups)

    assert result == expected


def test_synthesize_thematic_paragraphs_passes_grouped_text_to_seam():
    """The caller sends one request carrying the grouped category text."""
    groups = {
        "suppliers": [
            stub_row("a", 4, category="suppliers"),
            stub_row("b", 5, category="suppliers"),
        ]
    }
    fake = FakeLLM(returns={"suppliers": "Supply chain tightening."})

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
            stub_row("a", 7, category="competitors"),
            stub_row("b", 8, category="competitors"),
        ]
    }
    # The seam swallows transport/parse failures and returns None.
    with patch("delivery_engine._llm", return_value=FakeLLM(returns=None)):
        result = synthesize_thematic_paragraphs(groups)

    assert result == {}


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
        prepare_report(rows, None, run=_TEST_RUN, report_config=VISIBLE_6_CFG)

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
        stub_row("a", 8, category="competitors"),
        stub_row("b", 7, category="competitors"),
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
