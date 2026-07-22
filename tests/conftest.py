import os
import sys
from dataclasses import dataclass
from typing import Callable, Optional, Union
from unittest.mock import MagicMock

# Make scripts/ importable as top-level modules in tests (e.g. `enrich_targets`).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import pytest  # noqa: E402

import discovery  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_discovery_providers():
    """Drop the cached discovery-provider singletons (and their lazily-loaded
    gate metadata) before and after every test, so provider state never leaks."""
    discovery._reset_discovery_providers()
    yield
    discovery._reset_discovery_providers()


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
    """

    def _run(
        *,
        targets: Optional[list] = None,
        targets_yaml: Optional[str] = None,
        candidates: Union[list, Callable] = (),
        insight: Union[dict, Callable, None] = {},
        scrape: Union[str, Callable] = "text " * 200,
    ) -> PipelineRun:
        import ingestion_engine  # local: keeps supabase/openai imports off narrow runs

        if (targets is None) == (targets_yaml is None):
            raise TypeError("pass exactly one of targets= / targets_yaml=")

        if targets is not None:
            monkeypatch.setattr(ingestion_engine, "load_targets", lambda path: targets)
        else:
            (tmp_path / "targets.yaml").write_text(targets_yaml)
            monkeypatch.chdir(tmp_path)

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

        ingestion_engine.execute_pipeline()
        return PipelineRun(stored=stored, macro=macro)

    return _run
