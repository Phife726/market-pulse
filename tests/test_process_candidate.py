# tests/test_process_candidate.py
"""The ingestion candidate gauntlet as a testable unit.

process_candidate(candidate, target, ctx) runs one candidate through the
per-candidate gates and returns Stored | Suppressed(reason) | Error. All
run-state mutation happens on the RunContext; the record+bump pairing is
centralized in ctx.suppress so no gate can record without bumping (the
pre-extraction LLM-None gate forgot both).
"""
from unittest.mock import MagicMock

import pytest

import ingestion_engine
from tests.conftest import stub_insight, stub_target
from ingestion_engine import (
    RunContext,
    Stored,
    Suppressed,
    Error,
    process_candidate,
)


TARGET = {"name": "TestCorp", "category": "competitors", "min_article_length": 500}


def make_candidate(**overrides) -> dict:
    base = {
        "url": "https://example.com/article?utm_source=x",
        "title": "Headline A",
        "provider": "serper",
    }
    base.update(overrides)
    return base


def make_ctx(providers_by_name: dict | None = None, *,
             blocked_domains: frozenset[str] = frozenset(),
             unsafe_urls: set[str] | None = None) -> RunContext:
    return RunContext(providers_by_name=providers_by_name or {}, blocked_domains=blocked_domains,
                      unsafe_urls=set(unsafe_urls or ()))


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(ingestion_engine.time, "sleep", lambda s: None)


def test_duplicate_url_suppresses_and_bumps(monkeypatch):
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h, modes: True)
    ctx = make_ctx()
    out = process_candidate(make_candidate(), TARGET, ctx)
    assert out == Suppressed("duplicate_url")
    assert ctx.ledger.breakdown == {"duplicate_url": 1}
    assert ctx.provider_yield["serper"]["duplicates"] == 1
    assert ctx.scrapes_attempted == 0


def test_semantic_duplicate_suppresses_and_bumps(monkeypatch):
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h, modes: False)
    monkeypatch.setattr(
        ingestion_engine, "is_semantic_duplicate",
        lambda title, seen: (True, "Seen Headline", 92),
    )
    ctx = make_ctx()
    out = process_candidate(make_candidate(), TARGET, ctx)
    assert out == Suppressed("semantic_duplicate")
    assert ctx.ledger.breakdown == {"semantic_duplicate": 1}
    assert ctx.provider_yield["serper"]["duplicates"] == 1


def test_unscrapable_domain_suppresses_pre_scrape(monkeypatch):
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h, modes: False)
    monkeypatch.setattr(
        ingestion_engine, "is_semantic_duplicate", lambda title, seen: (False, "", 0))
    scraper = MagicMock()
    monkeypatch.setattr(ingestion_engine, "scrape_article", scraper)
    ctx = make_ctx()
    out = process_candidate(
        make_candidate(url="https://www.linkedin.com/posts/x"), TARGET, ctx)
    assert out == Suppressed("unscrapable_domain")
    assert ctx.ledger.breakdown == {"unscrapable_domain": 1}
    assert ctx.provider_yield["serper"]["unscrapable"] == 1
    scraper.assert_not_called()


def test_blocked_domain_suppresses_before_any_lookup(monkeypatch):
    """The security block is the first gate: a compromised domain is dropped
    before the duplicate lookup, so it costs no DB read and is ledgered as
    blocked_domain even when the URL was stored on an earlier run."""
    monkeypatch.setattr(
        ingestion_engine, "url_already_processed",
        lambda h, modes: pytest.fail("no DB lookup for a blocked domain"))
    monkeypatch.setattr(
        ingestion_engine, "is_semantic_duplicate", lambda title, seen: (False, "", 0))
    scraper = MagicMock()
    monkeypatch.setattr(ingestion_engine, "scrape_article", scraper)
    ctx = make_ctx(blocked_domains=frozenset({"chargedevs.com"}))
    out = process_candidate(
        make_candidate(url="https://chargedevs.com/newswire/lanxess-battery-lab/"), TARGET, ctx)
    assert out == Suppressed("blocked_domain")
    assert ctx.ledger.breakdown == {"blocked_domain": 1}
    assert ctx.provider_yield["serper"]["blocked"] == 1
    scraper.assert_not_called()


def test_unsafe_url_suppresses_before_any_lookup_or_scrape(monkeypatch):
    """The link-reputation gate sits right after the security block: a URL
    Safe Browsing flagged (the loop batches the lookup per target onto
    ctx.unsafe_urls) costs no DB read, no scrape, and is ledgered
    unsafe_url even when an earlier run stored it."""
    monkeypatch.setattr(
        ingestion_engine, "url_already_processed",
        lambda h, modes: pytest.fail("no DB lookup for an unsafe URL"))
    scraper = MagicMock()
    monkeypatch.setattr(ingestion_engine, "scrape_article", scraper)
    url = "https://compromised.example/story"
    ctx = make_ctx(unsafe_urls={url})
    out = process_candidate(make_candidate(url=url), TARGET, ctx)
    assert out == Suppressed("unsafe_url")
    assert ctx.ledger.breakdown == {"unsafe_url": 1}
    assert ctx.provider_yield["serper"]["unsafe"] == 1
    scraper.assert_not_called()


def test_security_block_wins_over_the_link_reputation_verdict(monkeypatch):
    """A URL that is both on a blocked domain and flagged unsafe is ledgered
    once, as blocked_domain — the security block is the first gate."""
    url = "https://chargedevs.com/newswire/x/"
    ctx = make_ctx(blocked_domains=frozenset({"chargedevs.com"}), unsafe_urls={url})
    out = process_candidate(make_candidate(url=url), TARGET, ctx)
    assert out == Suppressed("blocked_domain")
    assert ctx.ledger.breakdown == {"blocked_domain": 1}


def test_link_reputation_gate_reads_the_run_context_not_the_seam(monkeypatch):
    """The gauntlet never calls the seam itself (the loop batches per target):
    with nothing on ctx.unsafe_urls a candidate flows on to the duplicate lookup."""
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h, modes: True)
    monkeypatch.setattr(
        ingestion_engine, "_link_reputation",
        lambda: pytest.fail("process_candidate must not consult the seam"))
    ctx = make_ctx()
    out = process_candidate(make_candidate(url="https://compromised.example/story"), TARGET, ctx)
    assert out == Suppressed("duplicate_url")


def test_blocked_domain_gate_reads_the_run_context_not_a_built_in_list(monkeypatch):
    """The list is config-driven (security.blocked_domains, threaded onto the
    RunContext by execute_pipeline): with nothing blocked, a chargedevs.com
    candidate flows on to the duplicate lookup like any other."""
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h, modes: True)
    ctx = make_ctx(blocked_domains=frozenset())
    out = process_candidate(
        make_candidate(url="https://chargedevs.com/newswire/lanxess-battery-lab/"), TARGET, ctx)
    assert out == Suppressed("duplicate_url")
    assert "blocked_domain" not in ctx.ledger.breakdown


def test_provider_gate_drop_suppresses_with_gate_reason(monkeypatch):
    from relevance_gate import GateDecision

    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h, modes: False)
    monkeypatch.setattr(
        ingestion_engine, "is_semantic_duplicate", lambda title, seen: (False, "", 0))
    scraper = MagicMock()
    monkeypatch.setattr(ingestion_engine, "scrape_article", scraper)

    class _GatingProvider:
        name = "zoominfo"

        def gate(self, candidate, target):
            return GateDecision(drop=True, reason="zoominfo_company_mismatch",
                                matched_exclude="hospitality")

    ctx = make_ctx(providers_by_name={"zoominfo": _GatingProvider()})
    out = process_candidate(make_candidate(provider="zoominfo"), TARGET, ctx)
    assert out == Suppressed("zoominfo_company_mismatch")
    assert ctx.ledger.breakdown == {"zoominfo_company_mismatch": 1}
    assert ctx.provider_yield["zoominfo"]["relevance_dropped"] == 1
    scraper.assert_not_called()


def test_unknown_provider_never_gates(monkeypatch):
    """A candidate whose provider is absent from providers_by_name skips the
    gate (gate_decision None) and proceeds to the scrape step."""
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h, modes: False)
    monkeypatch.setattr(
        ingestion_engine, "is_semantic_duplicate", lambda title, seen: (False, "", 0))
    monkeypatch.setattr(ingestion_engine, "scrape_article", lambda url, m: None)
    ctx = make_ctx(providers_by_name={})
    out = process_candidate(make_candidate(provider="mystery"), TARGET, ctx)
    assert out == Suppressed("scrape_failed")


def test_scrape_failed_counts_the_attempt(monkeypatch):
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h, modes: False)
    monkeypatch.setattr(
        ingestion_engine, "is_semantic_duplicate", lambda title, seen: (False, "", 0))
    monkeypatch.setattr(ingestion_engine, "scrape_article", lambda url, m: None)
    ctx = make_ctx()
    out = process_candidate(make_candidate(), TARGET, ctx)
    assert out == Suppressed("scrape_failed")
    assert ctx.scrapes_attempted == 1
    assert ctx.stats["scrapes_attempted"] == 1
    assert ctx.provider_yield["serper"]["scraped"] == 1
    assert ctx.provider_yield["serper"]["scrape_failed"] == 1
    assert ctx.stats["errors"] == 0


def test_synthesis_failure_is_a_suppression_not_a_silent_error(monkeypatch):
    """The LLM-None fix: a failed synthesis records to the ledger and bumps the
    provider yield, mirroring scrape_failed — stats['errors'] is NOT touched
    (it now means store-failures only)."""
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h, modes: False)
    monkeypatch.setattr(
        ingestion_engine, "is_semantic_duplicate", lambda title, seen: (False, "", 0))
    monkeypatch.setattr(ingestion_engine, "scrape_article", lambda url, m: "text " * 200)
    monkeypatch.setattr(
        ingestion_engine, "synthesize_insight", lambda text, url, entity, cat: None)
    sleeps: list[float] = []
    monkeypatch.setattr(ingestion_engine.time, "sleep", lambda s: sleeps.append(s))
    ctx = make_ctx()
    out = process_candidate(make_candidate(), TARGET, ctx)
    assert out == Suppressed("synthesis_failed")
    assert ctx.ledger.breakdown == {"synthesis_failed": 1}
    assert ctx.provider_yield["serper"]["synthesis_failed"] == 1
    assert ctx.stats["errors"] == 0
    assert sleeps == [1.5]  # sleep iff an LLM call was spent


def _happy_path_until_synthesis(monkeypatch):
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h, modes: False)
    monkeypatch.setattr(
        ingestion_engine, "is_semantic_duplicate", lambda title, seen: (False, "", 0))
    monkeypatch.setattr(ingestion_engine, "scrape_article", lambda url, m: "text " * 200)


def test_llm_discard_suppresses(monkeypatch):
    _happy_path_until_synthesis(monkeypatch)
    monkeypatch.setattr(
        ingestion_engine, "synthesize_insight",
        lambda text, url, entity, cat: {"americhem_impact": "DISCARD"},
    )
    ctx = make_ctx()
    out = process_candidate(make_candidate(), TARGET, ctx)
    assert out == Suppressed("llm_discard")
    assert ctx.ledger.breakdown == {"llm_discard": 1}
    assert ctx.provider_yield["serper"]["discards"] == 1


def test_stored_persists_and_updates_run_state(monkeypatch):
    _happy_path_until_synthesis(monkeypatch)
    monkeypatch.setattr(
        ingestion_engine, "synthesize_insight",
        lambda text, url, entity, cat: stub_insight(
            url, headline="TestCorp expands compounding capacity",
            entities_mentioned=["TestCorp"]),
    )
    stored_payloads: list[dict] = []
    monkeypatch.setattr(
        ingestion_engine, "store_insight", lambda p, run_mode: (stored_payloads.append(p), True)[1])
    ctx = make_ctx()
    out = process_candidate(make_candidate(), TARGET, ctx)
    assert out == Stored()
    assert len(stored_payloads) == 1
    payload = stored_payloads[0]
    # url_hash comes from the NORMALIZED url (query params stripped)
    assert payload["url_hash"] == ingestion_engine.compute_url_hash(
        "https://example.com/article")
    assert payload["trigger_entity"] == "TestCorp"
    assert payload["category"] == "competitors"
    assert ctx.stats["insights_stored"] == 1
    assert ctx.provider_yield["serper"]["stored"] == 1
    assert ctx.stored_articles_buffer == [payload]
    assert "TestCorp expands compounding capacity" in ctx.seen_headlines
    assert ctx.ledger.breakdown == {}
    assert ctx.stats["errors"] == 0


def test_store_failure_is_an_error_not_a_suppression(monkeypatch):
    _happy_path_until_synthesis(monkeypatch)
    monkeypatch.setattr(
        ingestion_engine, "synthesize_insight",
        lambda text, url, entity, cat: stub_insight(
            url, headline="TestCorp expands compounding capacity",
            entities_mentioned=["TestCorp"]),
    )

    def _boom(payload, run_mode):
        raise RuntimeError("supabase down")

    monkeypatch.setattr(ingestion_engine, "store_insight", _boom)
    ctx = make_ctx()
    out = process_candidate(make_candidate(), TARGET, ctx)
    assert out == Error()
    assert ctx.stats["errors"] == 1
    assert ctx.ledger.breakdown == {}
    assert ctx.stored_articles_buffer == []
    assert ctx.seen_headlines == set()


def test_pipeline_persists_synthesis_failed_in_breakdown(run_ingestion_pipeline):
    """End-to-end: a run where every synthesis fails must carry
    synthesis_failed into the accounting handed to generate_macro_summary —
    the pre-extraction pipeline dropped these on the floor (issue: LLM-None
    gate recorded neither ledger nor yield)."""
    run = run_ingestion_pipeline(
        targets=[stub_target("TestCorp", category="competitors")],
        candidates=[make_candidate()],
        insight=None,          # unusable LLM response -> synthesis_failed
    )

    run.macro.assert_called_once()
    kwargs = run.macro.call_args.kwargs
    assert kwargs["suppression_breakdown"] == {"synthesis_failed": 1}
    assert kwargs["screened_count"] == 1


def test_yield_table_covers_every_ingestion_reason():
    """Every ingestion taxonomy code must map to a provider-yield counter, and
    every mapped counter must exist in the yield dict — a new reason code
    without a yield key would let a gate record without bumping again."""
    from suppression_ledger import INGESTION_CODES

    table = ingestion_engine._YIELD_KEY_FOR_REASON
    assert set(table.keys()) == set(INGESTION_CODES)
    yield_keys = set(ingestion_engine._new_provider_yield().keys())
    assert set(table.values()) <= yield_keys


# ---------------------------------------------------------------------------
# Market-report publisher gate (2026-09-08): a market-research report is
# dropped BEFORE the scrape, so it can never reach the scorer whatever RULE 3
# says — by publisher domain, or by the "market forecast to 20XX" headline
# pattern on a wire domain.
# ---------------------------------------------------------------------------

def _past_dedup(monkeypatch) -> MagicMock:
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h, modes: False)
    monkeypatch.setattr(ingestion_engine, "is_semantic_duplicate", lambda t, s: (False, "", 0))
    scrape = MagicMock(return_value=None)
    monkeypatch.setattr(ingestion_engine, "scrape_article", scrape)
    return scrape


def test_market_report_publisher_domain_suppresses_pre_scrape(monkeypatch):
    scrape = _past_dedup(monkeypatch)
    ctx = make_ctx()
    out = process_candidate(make_candidate(
        url="https://www.indexbox.io/blog/vinyl-siding-panel-market-forecast/",
        title="Vinyl Siding Panel Market Forecast Points Higher Toward 2035 | IndexBox"), TARGET, ctx)
    assert out == Suppressed("market_report_publisher")
    assert ctx.ledger.breakdown == {"market_report_publisher": 1}
    assert ctx.provider_yield["serper"]["market_reports"] == 1
    scrape.assert_not_called()
    assert ctx.scrapes_attempted == 0


def test_market_report_headline_on_a_wire_domain_suppresses_pre_scrape(monkeypatch):
    scrape = _past_dedup(monkeypatch)
    ctx = make_ctx()
    out = process_candidate(make_candidate(
        url="https://www.globenewswire.com/news-release/2026/07/20/agri-films.html",
        title="Agricultural Films Market to Reach USD 29.08 Billion by 2033"), TARGET, ctx)
    assert out == Suppressed("market_report_publisher")
    scrape.assert_not_called()


def test_ordinary_wire_headline_reaches_the_scrape(monkeypatch):
    scrape = _past_dedup(monkeypatch)
    ctx = make_ctx()
    out = process_candidate(make_candidate(
        url="https://www.globenewswire.com/news-release/2026/07/28/univar-royal.html",
        title="Univar Solutions Acquires H.M. Royal"), TARGET, ctx)
    assert out == Suppressed("scrape_failed")       # the stub scrape returns None
    scrape.assert_called_once()
    assert "market_report_publisher" not in ctx.ledger.breakdown


# ---------------------------------------------------------------------------
# Run mode (issue #100, ADR 0001): the gauntlet's dedup reads the modes the
# run may see, and the store writes as the run's mode. Real InMemory repo —
# the point is the seam's semantics, not a stubbed answer.
# ---------------------------------------------------------------------------
from datetime import datetime

from daily_intelligence_repo import InMemoryIntelligenceRepo

_URL_HASH = ingestion_engine.compute_url_hash("https://example.com/article")


def _real_repo_happy_path(monkeypatch) -> InMemoryIntelligenceRepo:
    fake = InMemoryIntelligenceRepo(now=lambda: datetime(2026, 9, 16, 10, 30))
    monkeypatch.setattr("ingestion_engine._repo", lambda: fake)
    monkeypatch.setattr(ingestion_engine, "scrape_article", lambda url, m: "x" * 600)
    monkeypatch.setattr(
        ingestion_engine, "synthesize_insight",
        lambda text, url, entity, cat: stub_insight(url, headline="Fresh headline"),
    )
    return fake


def test_run_context_visible_modes_follows_its_run_mode():
    assert make_ctx().run_mode == "production"
    assert make_ctx().visible_modes == frozenset({"production"})
    ctx = RunContext(providers_by_name={}, run_mode="test")
    assert ctx.visible_modes == frozenset({"production", "test"})


def test_production_run_stores_over_a_test_row(monkeypatch):
    """A test row is disposable: production does not see it as a duplicate,
    scrapes and scores the URL, and its write replaces the row as production."""
    fake = _real_repo_happy_path(monkeypatch)
    fake.upsert_insight({"url_hash": _URL_HASH, "headline": "QA scored"}, run_mode="test")
    ctx = make_ctx()
    out = process_candidate(make_candidate(), TARGET, ctx)
    assert out == Stored()
    (row,) = fake.fetch_since(datetime(2000, 1, 1), modes=frozenset({"production", "test"}))
    assert (row["run_mode"], row["headline"]) == ("production", "Fresh headline")
    assert ctx.stats["insights_stored"] == 1


def test_test_run_treats_a_production_row_as_a_duplicate(monkeypatch):
    fake = _real_repo_happy_path(monkeypatch)
    fake.upsert_insight({"url_hash": _URL_HASH, "headline": "Prod scored"}, run_mode="production")
    ctx = RunContext(providers_by_name={}, run_mode="test")
    out = process_candidate(make_candidate(), TARGET, ctx)
    assert out == Suppressed("duplicate_url")
    assert ctx.scrapes_attempted == 0
    (row,) = fake.fetch_since(datetime(2000, 1, 1), modes=frozenset({"production", "test"}))
    assert (row["run_mode"], row["headline"]) == ("production", "Prod scored")


def test_ignored_test_write_is_a_duplicate_not_a_store(monkeypatch, caplog):
    """The overlap race: both runs saw the URL as new; production wrote first.
    The test write is ON CONFLICT DO NOTHING — ledgered duplicate_url, one
    WARNING, and never counted as stored."""
    fake = _real_repo_happy_path(monkeypatch)
    fake.upsert_insight({"url_hash": _URL_HASH, "headline": "Prod scored"}, run_mode="production")
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h, modes: False)
    ctx = RunContext(providers_by_name={}, run_mode="test")
    with caplog.at_level("WARNING"):
        out = process_candidate(make_candidate(), TARGET, ctx)
    assert out == Suppressed("duplicate_url")
    assert ctx.ledger.breakdown == {"duplicate_url": 1}
    assert ctx.stats["insights_stored"] == 0
    assert ctx.stored_articles_buffer == []
    assert ctx.provider_yield["serper"]["duplicates"] == 1
    assert any("not stored" in r.message and r.levelname == "WARNING" for r in caplog.records)
    (row,) = fake.fetch_since(datetime(2000, 1, 1), modes=frozenset({"production", "test"}))
    assert row["headline"] == "Prod scored"
