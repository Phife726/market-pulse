# tests/test_pipeline_harness.py
"""The shared execute_pipeline harness must stay in sync with the real pipeline.

`run_ingestion_pipeline` (tests/conftest.py) stubs every seam the ingestion
pipeline reaches. A seam added to the pipeline but missed by the harness used to
surface only as a runtime KeyError/AttributeError in the 10:00 UTC cron — never
as a test failure, because the stub stacks were hand-maintained in three places.

These guards walk both sides and compare them:

  * the pipeline's real call graph, from ingestion_engine's AST, and
  * what the fixture actually monkeypatches, from conftest's AST.

Both sides are DERIVED. Nothing here is a hand-copied list of stub names, so
the guard cannot quietly drift away from the harness it is guarding.
"""
import ast
import inspect
import os

import ingestion_engine

#: The functions that make up a run: the loop, the candidate gauntlet it
#: delegates to, and the shared teardown every exit path takes.
PIPELINE_FUNCTIONS = ("execute_pipeline", "process_candidate", "_finalize_run")

#: Calls the harness deliberately leaves real — pure helpers, value types,
#: logging, and the discovery-provider registry (reset per-test by the autouse
#: fixture, and exercised for real by the ZoomInfo gate tests). Dotted entries
#: are attribute calls on one of ingestion_engine's module-level imports.
DELIBERATELY_REAL = {
    # Pure transforms and predicates
    "normalize_url",
    "compute_url_hash",
    "_is_unscrapable_domain",
    "_concept_demand_ahead",
    "_new_provider_yield",
    "_discovery_metadata",
    "insight.is_discard",
    # Value types / outcome variants
    "RunContext",
    "Stored",
    "Error",
    # Log-only sinks
    "_log_stats",
    "_log_provider_yield",
    "logger.info",
    "logger.warning",
    "logger.error",
    # Under test, not stubbed
    "process_candidate",
    "_finalize_run",
    # Registry: reset by the autouse _reset_discovery_providers fixture
    "_discovery_providers",
    # Read at use time; tests drive it with monkeypatch.setenv instead
    "config.store_discovery_metadata",
    # The clock: the tests that care fake it themselves (tail reserve, deadline)
    "time.monotonic",
}

_CONFTEST = os.path.join(os.path.dirname(__file__), "conftest.py")


def _qualified(node: ast.Call) -> str:
    """The dotted name a call targets, or "" if it isn't a module-level seam.

    Bare `foo()` -> "foo"; `time.sleep()` -> "time.sleep". Calls through a
    local (`ctx.suppress()`, `provider_obj.gate()`) return "" — they are
    reached via an object, not an attribute monkeypatch could replace.
    """
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
        return f"{func.value.id}.{func.attr}"
    return ""


def _pipeline_seam_calls() -> set:
    """Every call in the pipeline functions that resolves into ingestion_engine's
    module namespace — i.e. something monkeypatch could replace on the module,
    which is exactly what a seam is."""
    tree = ast.parse(inspect.getsource(ingestion_engine))
    functions = {
        node.name: node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
    }
    missing = set(PIPELINE_FUNCTIONS) - set(functions)
    assert not missing, (
        f"ingestion_engine no longer defines {sorted(missing)} — this guard walks "
        "the real pipeline functions, so update PIPELINE_FUNCTIONS."
    )
    namespace = set(vars(ingestion_engine))
    calls = set()
    for func_name in PIPELINE_FUNCTIONS:
        for node in ast.walk(functions[func_name]):
            if not isinstance(node, ast.Call):
                continue
            name = _qualified(node)
            if name and name.split(".")[0] in namespace:
                calls.add(name)
    return calls


def _harness_stubbed_names() -> set:
    """What run_ingestion_pipeline actually replaces, read from conftest's own
    source — so these guards check the HARNESS, not a copy of it.

    Picks up both `monkeypatch.setattr(ingestion_engine, "x", ...)` and
    `monkeypatch.setattr(ingestion_engine.time, "sleep", ...)`.
    """
    with open(_CONFTEST) as fh:
        tree = ast.parse(fh.read())
    stubbed = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "setattr"
                and len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)):
            continue
        target, attr = node.args[0], node.args[1].value
        if isinstance(target, ast.Name) and target.id == "ingestion_engine":
            stubbed.add(attr)
        elif (isinstance(target, ast.Attribute)
              and isinstance(target.value, ast.Name)
              and target.value.id == "ingestion_engine"):
            stubbed.add(f"{target.attr}.{attr}")
    assert stubbed, f"found no ingestion_engine stubs in {_CONFTEST}"
    return stubbed


def test_harness_accounts_for_every_pipeline_seam():
    """A new call in the pipeline must be a conscious decision: stub it in the
    shared harness, or record it here as deliberately real."""
    unaccounted = _pipeline_seam_calls() - _harness_stubbed_names() - DELIBERATELY_REAL
    assert not unaccounted, (
        f"ingestion_engine gained call(s) {sorted(unaccounted)} in "
        f"{list(PIPELINE_FUNCTIONS)} — either stub them in the "
        "run_ingestion_pipeline fixture (tests/conftest.py), or add them to "
        "DELIBERATELY_REAL here."
    )


def test_harness_stubs_nothing_the_pipeline_no_longer_calls():
    """The mirror check: a stub for a seam the pipeline dropped is dead
    scaffolding, and monkeypatch would keep it alive silently."""
    stale = _harness_stubbed_names() - _pipeline_seam_calls()
    assert not stale, (
        f"run_ingestion_pipeline still stubs {sorted(stale)}, which "
        f"{list(PIPELINE_FUNCTIONS)} no longer call."
    )
