import os
import sys
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Optional, Union
from unittest.mock import MagicMock

# Make scripts/ importable as top-level modules in tests (e.g. `enrich_targets`).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import pytest  # noqa: E402

import requests  # noqa: E402

import discovery  # noqa: E402
from mailer import FakeMailer  # noqa: E402

if TYPE_CHECKING:  # annotations only — keep openai/report off narrow runs
    from llm import FakeLLM
    from report import ReportModel
from run_budget import RunBudget  # noqa: E402
from targets import ENTITY_OPTIONAL_KEYS  # noqa: E402
from run_instant import RunInstant  # noqa: E402
from prompts import EXEC_BULLET_LABELS  # noqa: E402

#: The repository root — the structural guards read module sources by path.
REPO_ROOT = Path(__file__).resolve().parents[1]
#: The two shipped control files. A test that pins repo policy reads these;
#: one that exercises the parser writes its own file under tmp_path.
TARGETS_PATH = REPO_ROOT / "targets.yaml"
CONFIG_PATH = REPO_ROOT / "market_pulse_config.yaml"


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
    if search_mode == "entity":
        target.update(ENTITY_OPTIONAL_KEYS)  # the ZoomInfo fields + resolution hints, at their defaults
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

        cfg = report_config if report_config is not None else VISIBLE_6_CFG
        monkeypatch.setattr("delivery_engine._repo", lambda: fake_repo)
        monkeypatch.setattr("delivery_engine._llm", lambda: FakeLLM(returns=llm_returns))
        # The same fake the autouse _inert_mailer installs at the seam module —
        # injected here too so the harness guard derives the mailer as stubbed.
        monkeypatch.setattr("delivery_engine._mailer", lambda: fake_mailer)
        monkeypatch.setattr("config.mp_config", lambda: cfg)
        delivery_engine.execute_pipeline(run)
        return fake_mailer

    return _run


# ===========================================================================
# Shared builders — the one spelling of each shape. Files that still carry a
# hand-built literal are swept as they are touched; never add a second builder.
# ===========================================================================

def stub_row(url_hash: str = "abc", americhem_impact_score: int = 8, **overrides) -> dict:
    """A stored `daily_intelligence` row as delivery reads it — new-style
    (relevance fields, no `sentiment_score`); any key can be overridden or added."""
    row = {
        "url_hash": url_hash,
        "americhem_impact_score": americhem_impact_score,
        "sentiment_tag": "Neutral",
        "impact_rationale": "Direct feedstock cost effect.",
        "commercial_segment": "Enterprise / Cross-Segment",
        "headline": "Test Headline",
        "americhem_impact": "Some impact.",
        "entities_mentioned": ["TestCorp"],
        "source_url": "https://news.com/article",
        "category": "markets",
    }
    row.update(overrides)
    return row


def stub_source(source_id: int, headline: str = "H", url: str = "https://x.com/a",
                domain: str = "x.com") -> dict:
    """One `executive_sources` entry (a cited source), keyed by its pack id."""
    # segment/score are pack-shape only: prompts.py writes them, no renderer reads them.
    return {"id": source_id, "headline": headline, "url": url, "domain": domain,
            "segment": "Auto", "score": 7}


def appendix_hashes(model: "ReportModel") -> list[str]:
    """The appendix rows' hashes in display order — what a ranking test asserts on."""
    return [a["url_hash"] for a in model.additional_articles]


#: The suite's baseline report config: visible at 6, every other lever at its
#: code default. Extend it with `{**VISIBLE_6_CFG["reporting"], ...}`; never mutate.
VISIBLE_6_CFG = {"reporting": {"visible_impact_threshold": 6}}

def stub_macro_signal(**overrides) -> dict:
    """One valid Macroeconomic Outlook signal (a citable `direction` +
    canonical `affected_segments` + one citation)."""
    signal = {
        "indicator": "Manufacturing PMI",
        "direction": "Declining",
        "americhem_implication": "Downside risk for engineered-resin demand.",
        "affected_segments": ["Industrial"],
        "citation_source_ids": [1],
    }
    signal.update(overrides)
    return signal


VALID_MACRO_OUTLOOK = {
    "current_condition": "Industrial demand softening as construction cools.",
    "signals": [stub_macro_signal()],
}


def stub_macro_llm(**overrides) -> dict:
    """The raw macro-summary dict as the LLM returns it — what
    `macro_summary.assemble_macro_content` consumes. Content only: the stored
    row's `executive_sources` and accounting columns are added downstream
    (see `stub_summary_row`). The bullet labels come from the prompt's own
    constant, so a relabelling is an import-time change here, not a payload
    that silently stops validating."""
    parsed = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [{"label": label, "body": "Body."} for label in EXEC_BULLET_LABELS],
        "macro_outlook": {**VALID_MACRO_OUTLOOK, "signals": [stub_macro_signal()]},
    }
    parsed.update(overrides)
    return parsed


def stub_summary_row(*, run_date: str, run_mode: str = "production", **overrides) -> dict:
    """A `daily_summaries` seed row: the accounting columns every run writes,
    keyed on the run instant's `run_date` + `run_mode`. For scaffolding — a
    test whose subject is the row's *content* (the run-mode fallback ranking,
    the accounting-only shape) should keep its own literal."""
    row = {
        "run_date": run_date,
        "run_mode": run_mode,
        "executive_summary": "x",
        "macro_sentiment": "x",
        "suppression_breakdown": {},
        "suppression_samples": [],
    }
    row.update(overrides)
    return row


def stub_summary(row: Optional[dict] = None, **overrides) -> "MacroSummary":
    """A stored `daily_summaries` row as `fetch_macro_summary` hands it on —
    the typed read face, not the raw row. Tests keep building raw rows (that is
    what the database holds) and pass them through here, which is the seam the
    delivery engine crosses. For a test whose subject is the row's *content*,
    build the literal and wrap it; `stub_summary_row` remains the raw-row
    builder for tests that assert on what was written."""
    from macro_summary import MacroSummary

    return MacroSummary.from_row({**(row or {}), **overrides})


def stub_llm_insight(**overrides) -> "FakeLLM":
    """A FakeLLM answering one per-article insight: `stub_insight`'s minimal
    payload plus any relevance fields the test adds."""
    from llm import FakeLLM  # local: the one deliberate lazy import — keeps openai off narrow runs

    return FakeLLM(returns=stub_insight("https://news.com/article", **overrides))


def stub_http_response(status: int, *, json: Optional[dict] = None, text: Optional[str] = None) -> MagicMock:
    """A `requests.Response` stand-in: `status_code`, `ok`, `text`, `json()`,
    and a `raise_for_status` that raises `HTTPError(response=resp)` at >= 400."""
    resp = MagicMock(status_code=status, ok=status < 400,
                     text=f"body {status}" if text is None else text)
    resp.json.return_value = json if json is not None else {}
    if status >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status}", response=resp)
    return resp
