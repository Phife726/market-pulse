import os
import sys
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Callable, Optional, Union
from unittest.mock import MagicMock

# Make scripts/ importable as top-level modules in tests (e.g. `enrich_targets`).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import pytest  # noqa: E402

import discovery  # noqa: E402
from mailer import FakeMailer  # noqa: E402
from run_budget import RunBudget  # noqa: E402
from run_instant import RunInstant  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_discovery_providers():
    """Drop the cached discovery-provider singletons (and their lazily-loaded
    gate metadata) before and after every test, so provider state never leaks."""
    discovery._reset_discovery_providers()
    yield
    discovery._reset_discovery_providers()


# ===========================================================================
# The run instant every clock-sensitive test hands the engines
# ===========================================================================
# Thursday 2026-08-27 14:01 UTC — the late start from issue #64. Tests pass
# this instead of reading the process clock or flipping MARKET_PULSE_RUN_MODE;
# only tests/test_run_instant.py spells the literal itself, since it tests
# the value.

RUN_INSTANT = RunInstant(now=datetime(2026, 8, 27, 14, 1, 0), run_mode="production")
TEST_RUN_INSTANT = replace(RUN_INSTANT, run_mode="test")


# ===========================================================================
# Shared execute_pipeline harness
# ===========================================================================
# One stub stack for every seam execute_pipeline reaches, so a seam added to
# the pipeline is wired in ONE place instead of three hand-maintained copies.
# tests/test_pipeline_harness.py reads the setattr calls below straight out of
# this file and diffs them against the pipeline's real call graph, in both
# directions — so a missed seam fails the suite instead of the 10:00 UTC cron.

def stub_insight(url: str, **overrides) -> dict:
    """A minimal valid Insight dict for a stubbed `synthesize_insight`.

    Carries exactly the keys `process_candidate` reads without a default;
    every other stored-payload field is `.get(..., default)`."""
    return {
        "headline": "Stub headline",
        "americhem_impact": "Impact.",
        "sentiment_score": 5,
        "source_url": url,
        "entities_mentioned": [],
        **overrides,
    }


def stub_target(
    name: str, *, search_mode: str = "entity", results_per_entity: int = 2, **overrides,
) -> dict:
    """A target dict as `load_targets` would map it — the companion factory to
    `stub_insight` for tests that hand `execute_pipeline` targets directly."""
    target = {
        "name": name,
        "category": name,
        "query": f'"{name}"',
        "results_per_entity": results_per_entity,
        "lookback_hours": 24,
        "min_article_length": 500,
        "search_mode": search_mode,
    }
    target.update(overrides)
    return target


@dataclass(frozen=True)
class PipelineRun:
    """The observable result of one stubbed execute_pipeline run."""

    #: Every payload handed to `store_insight`, in order.
    stored: list
    #: The `generate_macro_summary` mock, for asserting the run's accounting.
    macro: MagicMock


@pytest.fixture
def run_ingestion_pipeline(monkeypatch, tmp_path):
    """Run `ingestion_engine.execute_pipeline()` over a fully stubbed seam stack.

    Pass exactly one target source:
      * `targets=[{...}]`      — patches `load_targets` with the dicts verbatim.
      * `targets_yaml="..."`   — writes tmp_path/targets.yaml and chdirs there,
        leaving the REAL `load_targets` to map it (the only way a test exercises
        YAML-derived fields such as `zoominfo_company_id`).

    Other knobs, all with defaults that make one candidate flow through to a
    successful store:
      * `candidates` — a list, or a callable(target) -> list, of candidate dicts.
      * `insight`    — overrides merged into `stub_insight`, a callable
        (text, url, entity, category) -> dict, or `None` to return an unusable
        LLM response (what the real `synthesize_insight` returns on failure,
        driving process_candidate's synthesis_failed gate).
      * `scrape`     — article text, or a callable(url, min_length) -> str|None.
      * `run`        — the run instant handed to execute_pipeline (RUN_INSTANT).
      * `limits`     — keyword overrides for `RunBudget.for_targets`
        (`max_scrapes` / `deadline_seconds` / `tail_reserve_seconds`); the
        harness builds the budget from the targets it resolves, so it can
        never mis-index. `None` lets the engine build the default (the
        production path).
    """

    def _run(
        *,
        targets: Optional[list] = None,
        targets_yaml: Optional[str] = None,
        candidates: Union[list, Callable] = (),
        insight: Union[dict, Callable, None] = {},
        scrape: Union[str, Callable] = "text " * 200,
        run: RunInstant = RUN_INSTANT,
        limits: Optional[dict] = None,
    ) -> PipelineRun:
        import ingestion_engine  # local: keeps supabase/openai imports off narrow runs

        if (targets is None) == (targets_yaml is None):
            raise TypeError("pass exactly one of targets= / targets_yaml=")

        if targets is not None:
            monkeypatch.setattr(ingestion_engine, "load_targets", lambda path: targets)
            resolved_targets = targets
        else:
            (tmp_path / "targets.yaml").write_text(targets_yaml)
            monkeypatch.chdir(tmp_path)
            resolved_targets = ingestion_engine.load_targets("targets.yaml")
        budget = None if limits is None else RunBudget.for_targets(resolved_targets, **limits)

        discover = candidates if callable(candidates) else (lambda target: list(candidates))
        monkeypatch.setattr(
            ingestion_engine, "discover_candidates",
            lambda target, providers: discover(target),
        )
        monkeypatch.setattr(ingestion_engine, "_hydrate_seen_headlines", lambda: set())
        monkeypatch.setattr(ingestion_engine, "url_already_processed", lambda h: False)
        monkeypatch.setattr(
            ingestion_engine, "is_semantic_duplicate", lambda title, seen: (False, "", 0))

        scraper = scrape if callable(scrape) else (lambda url, min_length: scrape)
        monkeypatch.setattr(ingestion_engine, "scrape_article", scraper)

        if callable(insight):
            synthesize = insight
        else:
            synthesize = lambda text, url, entity, category: (  # noqa: E731
                None if insight is None else stub_insight(url, **insight)
            )
        monkeypatch.setattr(ingestion_engine, "synthesize_insight", synthesize)

        stored: list = []
        monkeypatch.setattr(ingestion_engine, "store_insight", stored.append)

        macro = MagicMock(return_value=True)
        monkeypatch.setattr(ingestion_engine, "generate_macro_summary", macro)
        monkeypatch.setattr(ingestion_engine.time, "sleep", lambda s: None)

        ingestion_engine.execute_pipeline(run, budget=budget)
        return PipelineRun(stored=stored, macro=macro)

    return _run


# ===========================================================================
# Shared delivery harness — the ingestion harness's twin
# ===========================================================================

@pytest.fixture(autouse=True)
def _inert_mailer(monkeypatch) -> FakeMailer:
    """Install a recording FakeMailer as the process-wide mailer for EVERY
    test, so no test can reach the real Resend transport by omission — the
    production workflow's pytest step runs with the live SMTP_PASS and the
    production RECIPIENT_EMAILS in its environment. Consumer tests that
    assert on what was sent take `fake_mailer` (below), which builds on this."""
    fake = FakeMailer()
    monkeypatch.setattr("mailer._mailer_singleton", fake)
    return fake


@pytest.fixture
def fake_mailer(monkeypatch, _inert_mailer) -> FakeMailer:
    """The process-wide FakeMailer plus the addressing env the real
    `send_email` reads. `sent` is every EmailMessage that crossed the seam;
    set `fail_with` to simulate a failed send."""
    monkeypatch.setenv("SENDER_EMAIL", "noreply@harness.test")
    monkeypatch.setenv("RECIPIENT_EMAILS", "qa@harness.test")
    return _inert_mailer


@pytest.fixture
def run_delivery_pipeline(monkeypatch, fake_mailer):
    """Run `delivery_engine.execute_pipeline(run)` over an injected repo with
    the LLM and the mailer seams faked — the real `send_email` composes the
    message, so the subject/recipient wiring is exercised end to end.

      * `fake_repo`     — the InMemoryIntelligenceRepo the run reads and writes.
      * `run`           — the run instant (RUN_INSTANT).
      * `llm_returns`   — what the FakeLLM answers (None = unusable response,
        i.e. bullets-only synthesis).
      * `report_config` — the mp_config dict (default: visible threshold 6).
    Returns the `fake_mailer` fixture's FakeMailer (`sent` = what went out).
    """

    def _run(
        fake_repo,
        *,
        run: RunInstant = RUN_INSTANT,
        llm_returns=None,
        report_config: Optional[dict] = None,
    ) -> FakeMailer:
        import delivery_engine
        from llm import FakeLLM

        cfg = report_config if report_config is not None else {
            "reporting": {"visible_impact_threshold": 6}
        }
        monkeypatch.setattr("delivery_engine._repo", lambda: fake_repo)
        monkeypatch.setattr("delivery_engine._llm", lambda: FakeLLM(returns=llm_returns))
        monkeypatch.setattr("config.mp_config", lambda: cfg)
        delivery_engine.execute_pipeline(run)
        return fake_mailer

    return _run
