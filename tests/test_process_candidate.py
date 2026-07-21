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


def make_ctx(providers_by_name: dict | None = None) -> RunContext:
    return RunContext(providers_by_name=providers_by_name or {})


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(ingestion_engine.time, "sleep", lambda s: None)


def test_duplicate_url_suppresses_and_bumps(monkeypatch):
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h: True)
    ctx = make_ctx()
    out = process_candidate(make_candidate(), TARGET, ctx)
    assert out == Suppressed("duplicate_url")
    assert ctx.ledger.breakdown == {"duplicate_url": 1}
    assert ctx.provider_yield["serper"]["duplicates"] == 1
    assert ctx.scrapes_attempted == 0


def test_semantic_duplicate_suppresses_and_bumps(monkeypatch):
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h: False)
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
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h: False)
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


def test_provider_gate_drop_suppresses_with_gate_reason(monkeypatch):
    from relevance_gate import GateDecision

    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h: False)
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
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h: False)
    monkeypatch.setattr(
        ingestion_engine, "is_semantic_duplicate", lambda title, seen: (False, "", 0))
    monkeypatch.setattr(ingestion_engine, "scrape_article", lambda url, m: None)
    ctx = make_ctx(providers_by_name={})
    out = process_candidate(make_candidate(provider="mystery"), TARGET, ctx)
    assert out == Suppressed("scrape_failed")


def test_scrape_failed_counts_the_attempt(monkeypatch):
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h: False)
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
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h: False)
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
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h: False)
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


def _valid_insight(url: str) -> dict:
    return {
        "headline": "TestCorp expands compounding capacity",
        "americhem_impact": "Capacity shift affects resin supply.",
        "sentiment_score": 5,
        "source_url": url,
        "entities_mentioned": ["TestCorp"],
    }


def test_stored_persists_and_updates_run_state(monkeypatch):
    _happy_path_until_synthesis(monkeypatch)
    monkeypatch.setattr(
        ingestion_engine, "synthesize_insight",
        lambda text, url, entity, cat: _valid_insight(url),
    )
    stored_payloads: list[dict] = []
    monkeypatch.setattr(
        ingestion_engine, "store_insight", lambda p: stored_payloads.append(p))
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
        lambda text, url, entity, cat: _valid_insight(url),
    )

    def _boom(payload):
        raise RuntimeError("supabase down")

    monkeypatch.setattr(ingestion_engine, "store_insight", _boom)
    ctx = make_ctx()
    out = process_candidate(make_candidate(), TARGET, ctx)
    assert out == Error()
    assert ctx.stats["errors"] == 1
    assert ctx.ledger.breakdown == {}
    assert ctx.stored_articles_buffer == []
    assert ctx.seen_headlines == set()


def test_pipeline_persists_synthesis_failed_in_breakdown(monkeypatch):
    """End-to-end: a run where every synthesis fails must carry
    synthesis_failed into the accounting handed to generate_macro_summary —
    the pre-extraction pipeline dropped these on the floor (issue: LLM-None
    gate recorded neither ledger nor yield)."""
    monkeypatch.setattr(
        ingestion_engine, "load_targets",
        lambda path: [{
            "name": "TestCorp", "category": "competitors",
            "query": '"TestCorp"', "results_per_entity": 2,
            "lookback_hours": 24, "min_article_length": 500,
            "search_mode": "entity",
        }],
    )
    monkeypatch.setattr(
        ingestion_engine, "discover_candidates",
        lambda target, providers: [make_candidate()],
    )
    monkeypatch.setattr(ingestion_engine, "_hydrate_seen_headlines", lambda: set())
    monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h: False)
    monkeypatch.setattr(
        ingestion_engine, "is_semantic_duplicate", lambda title, seen: (False, "", 0))
    monkeypatch.setattr(ingestion_engine, "scrape_article", lambda url, m: "text " * 200)
    monkeypatch.setattr(
        ingestion_engine, "synthesize_insight", lambda text, url, entity, cat: None)
    macro = MagicMock(return_value=True)
    monkeypatch.setattr(ingestion_engine, "generate_macro_summary", macro)

    ingestion_engine.execute_pipeline()

    macro.assert_called_once()
    kwargs = macro.call_args.kwargs
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
