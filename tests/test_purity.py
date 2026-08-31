"""test_purity.py — the pure modules, pinned structurally (CONTEXT.md: Pure module).

One table, `PURE_MODULES`, is the list of pure modules and the source of
truth for it. Each row's source is parsed and walked (function-local
imports included) for the three things a pure module never does: import
anything outside the closed set of harmless libraries and the other rows
(a zero-I/O row may not import a file-backed one — the read is inherited);
read the clock; open a file beyond the count its row declares. A new pure
module is one row (and a module whose docstring calls itself pure must have
one); a new library, harmless or not, must be classified before a pure module
may import it.
"""

import ast
import re
from dataclasses import dataclass

import pytest

from tests.conftest import REPO_ROOT

#: The closed set of libraries a pure module may import — the stdlib that
#: reaches no clock, environment, filesystem, network or random source, plus
#: the two third-party parsers the rows use. Dotted names as written, so
#: `urllib.parse` is harmless while `urllib.request` is not on the list.
HARMLESS_LIBRARIES = frozenset({
    "__future__", "typing", "dataclasses", "collections", "itertools", "re",
    "hashlib", "html", "datetime", "urllib.parse", "logging",
    "yaml", "rapidfuzz.fuzz",
})

#: Method names that read the clock whatever the receiver is called
#: (`datetime.now()`, `date.today()`, `_dt.utcnow()` on an aliased import).
CLOCK_METHODS = frozenset({"now", "today", "utcnow"})


@dataclass(frozen=True)
class Purity:
    #: Harmless libraries this module nonetheless forgoes.
    forbids: frozenset = frozenset()
    #: How many `open(` calls the module makes — its declared I/O.
    reads_files: int = 0


PURE_MODULES = {
    "report": Purity(),
    "prompts": Purity(),
    "scoring": Purity(),
    "insight": Purity(),
    "macro_summary": Purity(),
    "run_budget": Purity(),
    "suppression_ledger": Purity(),
    "target_enricher": Purity(),
    # The two file-backed parsers (targets.yaml, target_metadata.yaml): one read each.
    "targets": Purity(reads_files=1),
    "relevance_gate": Purity(reads_files=1),
    # The renderer forgoes a logger too: same model, same bytes, no side channel.
    "renderer": Purity(forbids=frozenset({"logging"})),
}


def _tree(name: str) -> ast.Module:
    """The root module's AST, read by path — no import, so a module that
    cannot import still reports its violation, and a same-named script on
    sys.path cannot be scanned in its place."""
    return ast.parse((REPO_ROOT / f"{name}.py").read_text(encoding="utf-8"))


def _imports(tree: ast.Module) -> set:
    """Every module imported anywhere in the tree, as the dotted name written
    (`urllib.parse`, not `urllib`). A relative import is reported as `.` so it
    fails as unclassifiable rather than vanishing."""
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            found.add(("." * node.level) + (node.module or ""))
    return found


def _calls(tree: ast.Module):
    return (node for node in ast.walk(tree) if isinstance(node, ast.Call))


@pytest.mark.parametrize("name", sorted(PURE_MODULES))
def test_pure_module_imports_only_harmless_libraries_and_other_pure_modules(name):
    imported = _imports(_tree(name))
    extra = imported - HARMLESS_LIBRARIES - set(PURE_MODULES)
    assert not extra, (
        f"{name} imports {sorted(extra)} — take the value as a parameter; if the import is "
        f"harmless, add it to HARMLESS_LIBRARIES, and if it is a pure repo module, give it a row")
    forbidden = imported & PURE_MODULES[name].forbids
    assert not forbidden, f"{name} imports {sorted(forbidden)}, which its row forgoes"
    if not PURE_MODULES[name].reads_files:
        # A file read is inherited through an import: a zero-I/O row may not
        # import a file-backed row (Codex review on #84).
        file_backed = imported & {n for n, p in PURE_MODULES.items() if p.reads_files}
        assert not file_backed, (
            f"{name} imports {sorted(file_backed)}, which read files — "
            f"take the parsed value as a parameter")


@pytest.mark.parametrize("name", sorted(PURE_MODULES))
def test_pure_module_reads_no_clock_and_opens_exactly_its_declared_files(name):
    tree = _tree(name)
    clock_calls = sorted({
        f"{ast.unparse(c.func)}()" for c in _calls(tree)
        if isinstance(c.func, ast.Attribute) and c.func.attr in CLOCK_METHODS
    })
    assert not clock_calls, f"{name} calls {clock_calls} — the caller derives that field"
    opens = sum(
        1 for c in _calls(tree)
        if (isinstance(c.func, ast.Name) and c.func.id == "open")
        or (isinstance(c.func, ast.Attribute) and c.func.attr == "open")
    )
    assert opens == PURE_MODULES[name].reads_files, (
        f"{name} opens {opens} file(s); its row declares {PURE_MODULES[name].reads_files}")


def test_every_module_that_calls_itself_pure_is_a_row():
    """The docstrings are the other copy of the list; a module that opens with
    'pure' and is not in the table has escaped the scan."""
    self_described = set()
    for path in REPO_ROOT.glob("*.py"):
        doc = ast.get_docstring(ast.parse(path.read_text(encoding="utf-8"))) or ""
        if re.search(r"\bpure\b", doc.splitlines()[0] if doc else "", re.I):
            self_described.add(path.stem)
    missing = self_described - set(PURE_MODULES)
    assert not missing, f"{sorted(missing)} call themselves pure — add a PURE_MODULES row or reword the docstring"
    assert self_described, "no module calls itself pure — the parity pin has nothing to pin"
