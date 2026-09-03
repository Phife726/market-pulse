# tests/test_pipeline_harness.py
"""The shared execute_pipeline harnesses must stay in sync with the real pipelines.

`run_ingestion_pipeline` and `run_delivery_pipeline` (tests/conftest.py) stub
every seam their engine reaches. A seam added to a pipeline but missed by its
harness used to surface only as a runtime KeyError/AttributeError in the 10:00
UTC cron — never as a test failure, because the stub stacks were hand-maintained.

These guards walk both sides of each harness and compare them:

  * the pipeline's real call graph, from the engine module's AST, and
  * what the fixture actually monkeypatches, from conftest's AST — both the
    attribute form `setattr(ingestion_engine, "x", …)` and the string form
    `setattr("delivery_engine._repo", …)`.

Both sides are DERIVED. Nothing here is a hand-copied list of stub names, so a
guard cannot quietly drift away from the harness it is guarding.
"""
import ast
import inspect
import logging
import os
from dataclasses import dataclass
from types import ModuleType

import pytest

import delivery_engine
import ingestion_engine

_CONFTEST = os.path.join(os.path.dirname(__file__), "conftest.py")


@dataclass(frozen=True)
class Harness:
    """One engine's harness contract: the fixture whose body is the harness,
    the functions that make up a run, and the calls it deliberately leaves real."""
    engine: ModuleType
    #: The conftest fixture whose body is the harness — only its own
    #: `monkeypatch.setattr` calls count as harness coverage.
    fixture: str
    #: The functions that make up a run, walked for seam calls. Calls between
    #: them are the code under test, never stubs.
    functions: tuple
    #: Calls the harness deliberately leaves real. Dotted entries are attribute
    #: calls on one of the engine's module-level imports.
    deliberately_real: frozenset


INGESTION = Harness(
    engine=ingestion_engine,
    fixture="run_ingestion_pipeline",
    # The loop, the per-target body, the candidate gauntlet, and the shared teardown.
    functions=("execute_pipeline", "_run_target", "process_candidate", "_finalize_run"),
    deliberately_real=frozenset({
        # Pure transforms and predicates
        "normalize_url", "compute_url_hash", "_is_unscrapable_domain",
        "RunBudget.for_targets", "_new_provider_yield", "_discovery_metadata",
        "insight.is_discard", "is_synthesis_outage",
        # Value types / outcome variants
        "RunContext", "Stored", "Error",
        # The end-of-run guard: stubbing it would disarm the very failure the
        # harness tests exercise (a run where every synthesis call fails).
        "SynthesisOutageError",
        # Log-only sinks
        "_log_stats", "_log_provider_yield",
        # Registry: reset by the autouse _reset_discovery_providers fixture
        "_discovery_providers",
        # Read at use time; tests drive it with monkeypatch.setenv instead
        "config.store_discovery_metadata",
        # The clock: the tests that care fake it themselves (tail reserve, deadline)
        "time.monotonic",
    }),
)

DELIVERY = Harness(
    engine=delivery_engine,
    fixture="run_delivery_pipeline",
    # The run, its two fetches, report preparation (assembly + write-back +
    # thematic synthesis), composition/send, and the post-send stamp.
    functions=(
        "execute_pipeline", "fetch_todays_intelligence", "resolve_summary_row",
        "prepare_report", "_update_delivery_summary_counts",
        "synthesize_thematic_paragraphs", "send_email", "_record_delivery",
    ),
    deliberately_real=frozenset({
        # Pure: the window rule, report assembly and rendering, the summary
        # preference, scoring, prompt assembly, the message value
        "delivery_window", "assemble_report", "render_report",
        "_prefer_production_summary", "_alert_tier", "prompts.thematic_prompt",
        "EmailMessage", "SuppressionLedger.from_row", "_as_candidate",
        "SummaryKey", "_run_day",
    }),
)


def _qualified(node: ast.Call) -> str:
    """The dotted name a call targets, or "" if it isn't a module-level seam.

    Bare `foo()` -> "foo"; `time.sleep()` -> "time.sleep". Calls through a
    local (`ctx.suppress()`, `repo.fetch_since()`) return "" — they are reached
    via an object, not an attribute monkeypatch could replace.
    """
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
        return f"{func.value.id}.{func.attr}"
    return ""


def _pipeline_seam_calls(h: Harness) -> set:
    """Every call in the pipeline functions that resolves into the engine's
    module namespace — i.e. something monkeypatch could replace on the module,
    which is exactly what a seam is."""
    tree = ast.parse(inspect.getsource(h.engine))
    functions = {
        node.name: node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
    }
    missing = set(h.functions) - set(functions)
    assert not missing, (
        f"{h.engine.__name__} no longer defines {sorted(missing)} — this guard walks "
        "the real pipeline functions, so update the Harness.functions tuple."
    )
    namespace = vars(h.engine)
    calls = set()
    for func_name in h.functions:
        for node in ast.walk(functions[func_name]):
            if not isinstance(node, ast.Call):
                continue
            name = _qualified(node)
            base = name.split(".")[0]
            if not name or base not in namespace:
                continue
            if isinstance(namespace[base], logging.Logger):
                continue  # a log sink is never a seam the harness needs
            calls.add(name)
    return calls


def _setattr_target(node: ast.Call) -> tuple:
    """(module, attribute) a `monkeypatch.setattr` call replaces, in either
    spelling: `setattr(ingestion_engine.time, "sleep", …)` or
    `setattr("delivery_engine._repo", …)`. ("", "") if it is neither."""
    target = node.args[0]
    if isinstance(target, ast.Constant) and isinstance(target.value, str):
        mod, _, attr = target.value.rpartition(".")
        return mod, attr
    if not (len(node.args) >= 2 and isinstance(node.args[1], ast.Constant)):
        return "", ""
    if isinstance(target, ast.Name):
        return target.id, node.args[1].value
    if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name):
        return f"{target.value.id}.{target.attr}", node.args[1].value
    return "", ""


def _harness_stubbed_names(h: Harness) -> set:
    """What the fixture actually replaces, read from its own body in conftest —
    so these guards check the HARNESS, not a copy of it. A target on the engine
    module is recorded bare (`_repo`, `time.sleep`); one on another module the
    engine imports is recorded dotted (`config.mp_config`)."""
    with open(_CONFTEST) as fh:
        tree = ast.parse(fh.read())
    fixture = next(n for n in ast.walk(tree)
                   if isinstance(n, ast.FunctionDef) and n.name == h.fixture)
    engine_name = h.engine.__name__
    stubbed = set()
    for node in ast.walk(fixture):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "setattr" and node.args):
            continue
        mod, attr = _setattr_target(node)
        if not mod:
            continue
        if mod == engine_name:
            stubbed.add(attr)
        elif mod.startswith(engine_name + "."):
            stubbed.add(f"{mod[len(engine_name) + 1:]}.{attr}")
        else:
            stubbed.add(f"{mod}.{attr}")
    assert stubbed, f"{h.fixture} in {_CONFTEST} stubs nothing"
    return stubbed


@pytest.mark.parametrize("h", [INGESTION, DELIVERY], ids=lambda h: h.fixture)
def test_harness_accounts_for_every_pipeline_seam(h: Harness):
    """A new call in the pipeline must be a conscious decision: stub it in the
    shared harness, or record it here as deliberately real."""
    unaccounted = (_pipeline_seam_calls(h) - _harness_stubbed_names(h)
                   - h.deliberately_real - set(h.functions))
    assert not unaccounted, (
        f"{h.engine.__name__} gained call(s) {sorted(unaccounted)} in "
        f"{list(h.functions)} — either stub them in the {h.fixture} fixture "
        "(tests/conftest.py), or add them to that Harness's deliberately_real here."
    )


@pytest.mark.parametrize("h", [INGESTION, DELIVERY], ids=lambda h: h.fixture)
def test_harness_stubs_nothing_the_pipeline_no_longer_calls(h: Harness):
    """The mirror check: a stub for a seam the pipeline dropped is dead
    scaffolding, and monkeypatch would keep it alive silently."""
    stale = _harness_stubbed_names(h) - _pipeline_seam_calls(h)
    assert not stale, (
        f"{h.fixture} still stubs {sorted(stale)}, which "
        f"{list(h.functions)} no longer call."
    )


@pytest.mark.parametrize("h", [INGESTION, DELIVERY], ids=lambda h: h.fixture)
def test_deliberately_real_names_only_calls_the_pipeline_makes(h: Harness):
    """The allowlist is the guard's one escape hatch: an entry the pipeline no
    longer calls is a stale wave-through, and must go the way a stale stub does."""
    stale = h.deliberately_real - _pipeline_seam_calls(h)
    assert not stale, f"{h.engine.__name__} no longer calls {sorted(stale)} — drop them from deliberately_real."
