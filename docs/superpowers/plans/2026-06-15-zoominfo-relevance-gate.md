# ZoomInfo Metadata-Backed Relevance Gate — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drop obvious ZoomInfo company-mismatch news candidates before the paid scrape/synthesis, using the reviewed `target_metadata.yaml`, behind a default-off flag.

**Architecture:** A new pure module `relevance_gate.py` (exclude-hit + identity-rescue rule, word-boundary matching) is wired into `ingestion_engine.execute_pipeline()` immediately before scrape, for `provider == "zoominfo"` candidates only, guarded by `ZOOMINFO_RELEVANCE_GATE_ENABLED` (default off). Drops are accounted via a new ingestion-owned suppression reason code and a new `provider_yield` counter.

**Tech Stack:** Python 3.10, PyYAML, pytest, `unittest.mock`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-06-15-zoominfo-relevance-gate-design.md`

---

## File Structure

- **Create** `relevance_gate.py` — pure module: `GateDecision`, `evaluate()`, `_term_matches()`, `_identity_terms()`, `load_target_metadata()`. Zero network; the loader is the only I/O and it swallows read errors.
- **Create** `tests/test_relevance_gate.py` — exhaustive unit tests for the rule, matching, and loader.
- **Modify** `suppression_ledger.py` — add one ingestion-owned reason code.
- **Modify** `tests/test_suppression_ledger.py` — assert the new code is ingestion-owned.
- **Modify** `ingestion_engine.py` — flag reader, metadata load, `provider_yield` counter + log line, a small `_gate_zoominfo_candidate()` helper, and the pre-scrape wiring.
- **Modify** `tests/test_zoominfo.py` — update the yield-line assertion; add helper + integration wiring tests.
- **Modify** `CLAUDE.md` — bump the ingestion-owned reason-code count from 4 to 5.

### Invariants this plan must preserve (from the spec)

- Flag-on **plus missing/unparseable `target_metadata.yaml`** → no-op (gate disabled, no crash).
- A **non-active** metadata record (`metadata_record_status != "active"`) is ignored.
- **Serper candidates are never gated** — only `provider == "zoominfo"`.
- The `provider_yield` **log format is updated safely** — the one existing test that asserts the exact string is updated in the same task.
- `suppression_ledger.py` owns the **new ingestion reason only** — a delivery ledger using it must still raise.
- **No broad ingestion/delivery refactor** — edits are additive and localized.

---

## Task 1: Add the `zoominfo_company_mismatch` ingestion reason code

**Files:**
- Modify: `suppression_ledger.py:10-15` (the `_INGESTION_REASONS` tuple)
- Test: `tests/test_suppression_ledger.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_suppression_ledger.py`:

```python
def test_zoominfo_company_mismatch_is_ingestion_owned():
    from suppression_ledger import side_of, label_for
    assert side_of("zoominfo_company_mismatch") == "ingestion"
    assert label_for("zoominfo_company_mismatch") == "ZoomInfo company mismatch"
    led = SuppressionLedger.for_ingestion().record(
        "zoominfo_company_mismatch", url="https://x/1", title="T1",
    )
    assert led.breakdown == {"zoominfo_company_mismatch": 1}


def test_zoominfo_company_mismatch_rejected_on_delivery_ledger():
    led = SuppressionLedger.for_delivery()
    with pytest.raises(ValueError, match="not owned by delivery"):
        led.record("zoominfo_company_mismatch", url="u", title="t")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_suppression_ledger.py::test_zoominfo_company_mismatch_is_ingestion_owned tests/test_suppression_ledger.py::test_zoominfo_company_mismatch_rejected_on_delivery_ledger -v`
Expected: FAIL — `side_of` raises `KeyError` (reason unknown).

- [ ] **Step 3: Add the reason code**

In `suppression_ledger.py`, extend `_INGESTION_REASONS` (keep existing four, append the fifth):

```python
_INGESTION_REASONS: tuple[tuple[str, str], ...] = (
    ("duplicate_url",            "duplicate URL"),
    ("semantic_duplicate",       "semantic duplicate"),
    ("llm_discard",              "LLM discard"),
    ("scrape_failed",            "scrape failed"),
    ("zoominfo_company_mismatch", "ZoomInfo company mismatch"),
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_suppression_ledger.py -v`
Expected: PASS (all, including the two new tests).

- [ ] **Step 5: Commit**

```bash
git add suppression_ledger.py tests/test_suppression_ledger.py
git commit -m "feat(suppression): add zoominfo_company_mismatch ingestion reason code"
```

---

## Task 2: Create `relevance_gate.py` — the pure evaluate rule

**Files:**
- Create: `relevance_gate.py`
- Test: `tests/test_relevance_gate.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_relevance_gate.py`:

```python
"""Unit tests for the pure ZoomInfo relevance gate. No network, no files
(loader tests use tmp_path)."""
import pytest

from relevance_gate import GateDecision, evaluate


RTP_RECORD = {
    "metadata_record_status": "active",
    "canonical_name": "RTP Co",
    "company_identity_terms": ["RTP Co", "RTP Company"],
    "manual_aliases": ["RTP Co", "RTP Company"],
    "exclude_terms": [
        "real-time payments", "RTP Network", "return to player", "casino",
        "slots", "Research Triangle Park", "RTP Global", "Rain Tree Photonics",
    ],
}

AVIENT_RECORD = {
    "metadata_record_status": "active",
    "canonical_name": "Avient",
    "company_identity_terms": ["Avient"],
    "manual_aliases": [],
    "exclude_terms": [],
}


@pytest.mark.parametrize("term", [
    "real-time payments", "RTP Network", "return to player", "casino",
    "slots", "Research Triangle Park", "RTP Global", "Rain Tree Photonics",
])
def test_exclude_term_drops_without_identity_rescue(term):
    d = evaluate(title=f"Breaking: {term} expands", description="", record=RTP_RECORD)
    assert d.drop is True
    assert d.reason == "zoominfo_company_mismatch"
    assert d.matched_exclude == term


def test_identity_rescue_keeps_even_with_exclude_present():
    # "RTP Company" rescues even though "casino" (an exclude term) appears.
    d = evaluate(
        title="RTP Company opens plant near a casino district",
        description="", record=RTP_RECORD,
    )
    assert d.drop is False
    assert d.matched_identity == "RTP Company"


def test_canonical_name_rescues():
    d = evaluate(title="RTP Co wins slots contract", description="", record=RTP_RECORD)
    assert d.drop is False


def test_no_exclude_term_keeps_even_without_identity_text():
    d = evaluate(title="Quarterly polymer market update", description="", record=RTP_RECORD)
    assert d.drop is False
    assert d.matched_exclude is None
    assert d.matched_identity is None


def test_empty_exclude_terms_never_drops():
    d = evaluate(title="Avient casino slots real-time payments", description="",
                 record=AVIENT_RECORD)
    # "Avient" is an identity term, so this is a rescue anyway; assert keep.
    assert d.drop is False


def test_empty_exclude_terms_keeps_with_no_identity_either():
    d = evaluate(title="Totally unrelated casino headline", description="",
                 record=AVIENT_RECORD)
    assert d.drop is False  # no exclude_terms => nothing to drop on


def test_case_insensitive_exclude_match():
    d = evaluate(title="CASINO night downtown", description="", record=RTP_RECORD)
    assert d.drop is True
    assert d.matched_exclude == "casino"


def test_word_boundary_no_partial_false_match():
    # "slots" must not match inside "slotsmachineco"; "casinos" must not match "casino"?
    # We require whole-word: "casinos" should NOT match the term "casino".
    d = evaluate(title="The slotsmachineco product launch", description="",
                 record=RTP_RECORD)
    assert d.drop is False


def test_word_boundary_phrase_matches_as_phrase():
    d = evaluate(title="News from Research Triangle Park today", description="",
                 record=RTP_RECORD)
    assert d.drop is True
    assert d.matched_exclude == "Research Triangle Park"


def test_description_text_is_searched():
    d = evaluate(title="Neutral headline", description="hosted at a casino",
                 record=RTP_RECORD)
    assert d.drop is True
    assert d.matched_exclude == "casino"


def test_gate_decision_is_frozen():
    d = GateDecision(drop=False)
    with pytest.raises(Exception):
        d.drop = True  # frozen dataclass
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_relevance_gate.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'relevance_gate'`.

- [ ] **Step 3: Write the module**

Create `relevance_gate.py`:

```python
"""Pure ZoomInfo relevance gate.

A targeted false-positive suppressor (NOT a second entity resolver). ZoomInfo
News candidates are already linked to a company by id; this gate drops a
candidate only when a curated `exclude_term` appears AND no identity term
(canonical name / identity terms / manual aliases) rescues it. Absence of
identity text alone never drops.

`evaluate` is pure (no I/O). `load_target_metadata` is the only I/O and swallows
read errors so a missing/bad companion file silently disables the gate rather
than crashing ingestion.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

GATE_REASON = "zoominfo_company_mismatch"


@dataclass(frozen=True)
class GateDecision:
    """Outcome of evaluating one candidate. `drop=True` means suppress it."""
    drop: bool
    reason: Optional[str] = None
    matched_exclude: Optional[str] = None
    matched_identity: Optional[str] = None


def _term_matches(text: str, term: str) -> bool:
    """True if `term` appears in `text` as a whole word/phrase, case-insensitive.

    Internal whitespace runs in `term` match one-or-more whitespace in `text`,
    so multi-word phrases survive irregular spacing. Word boundaries prevent
    partial matches (e.g. 'casino' does not match 'casinos')."""
    term = (term or "").strip()
    if not term:
        return False
    parts = [re.escape(p) for p in term.split()]
    pattern = r"\b" + r"\s+".join(parts) + r"\b"
    return re.search(pattern, text, re.IGNORECASE) is not None


def _identity_terms(record: dict) -> list[str]:
    """canonical_name + company_identity_terms + manual_aliases, non-empty."""
    terms: list[str] = []
    canonical = (record.get("canonical_name") or "").strip()
    if canonical:
        terms.append(canonical)
    for key in ("company_identity_terms", "manual_aliases"):
        for term in (record.get(key) or []):
            if isinstance(term, str) and term.strip():
                terms.append(term)
    return terms


def evaluate(*, title: str, description: str, record: dict) -> GateDecision:
    """Decide whether a ZoomInfo candidate is an obvious company mismatch.

    Rule: identity rescue first (keep), then exclude hit (drop), else keep.
    """
    text = f"{title or ''} {description or ''}"

    for term in _identity_terms(record):
        if _term_matches(text, term):
            return GateDecision(drop=False, matched_identity=term)

    for term in (record.get("exclude_terms") or []):
        if isinstance(term, str) and _term_matches(text, term):
            return GateDecision(drop=True, reason=GATE_REASON, matched_exclude=term)

    return GateDecision(drop=False)


def load_target_metadata(path: str = "target_metadata.yaml") -> dict:
    """Return {target_key: record} from the companion file.

    Reads swallow exceptions and return {} — a missing or unparseable file
    silently disables the gate; ingestion never crashes on it."""
    try:
        with open(path, "r") as fh:
            data = yaml.safe_load(fh) or {}
    except (OSError, yaml.YAMLError) as exc:
        logger.warning(
            "relevance gate: could not load %s (%s) — gate disabled", path, exc
        )
        return {}
    targets = data.get("targets")
    return targets if isinstance(targets, dict) else {}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_relevance_gate.py -v`
Expected: PASS (all evaluate/matching tests). Loader tests come in Task 3.

- [ ] **Step 5: Commit**

```bash
git add relevance_gate.py tests/test_relevance_gate.py
git commit -m "feat(relevance-gate): add pure evaluate rule and matching"
```

---

## Task 3: `load_target_metadata` loader tests (graceful failure)

**Files:**
- Modify: `tests/test_relevance_gate.py`
- (No production change — `load_target_metadata` already written in Task 2.)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_relevance_gate.py`:

```python
from relevance_gate import load_target_metadata


def test_load_target_metadata_reads_targets(tmp_path):
    p = tmp_path / "target_metadata.yaml"
    p.write_text(
        "version: 1\n"
        "targets:\n"
        "  RTP Company:\n"
        "    metadata_record_status: active\n"
        "    canonical_name: RTP Co\n"
    )
    data = load_target_metadata(str(p))
    assert "RTP Company" in data
    assert data["RTP Company"]["canonical_name"] == "RTP Co"


def test_load_target_metadata_missing_file_returns_empty():
    assert load_target_metadata("/nonexistent/target_metadata.yaml") == {}


def test_load_target_metadata_bad_yaml_returns_empty(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("targets: [unbalanced\n")
    assert load_target_metadata(str(p)) == {}


def test_load_target_metadata_no_targets_key_returns_empty(tmp_path):
    p = tmp_path / "empty.yaml"
    p.write_text("version: 1\n")
    assert load_target_metadata(str(p)) == {}
```

- [ ] **Step 2: Run tests to verify they pass immediately**

Run: `python -m pytest tests/test_relevance_gate.py -k load_target_metadata -v`
Expected: PASS (the loader already exists from Task 2; these lock in the swallow-on-error contract).

- [ ] **Step 3: Commit**

```bash
git add tests/test_relevance_gate.py
git commit -m "test(relevance-gate): cover load_target_metadata graceful failure"
```

---

## Task 4: Wire the gate into `ingestion_engine.execute_pipeline()`

**Files:**
- Modify: `ingestion_engine.py` — import, flag reader (near line 309), `_new_provider_yield` (353-357), `_log_provider_yield` (360-369), new `_gate_zoominfo_candidate` helper, metadata load in `execute_pipeline` (after 913), gate block in the loop (after 1004, before 1006).
- Modify: `tests/test_zoominfo.py` — update the yield-line assertion (1106-1108); add helper + integration tests.

- [ ] **Step 1: Write the failing helper unit tests**

Append to `tests/test_zoominfo.py` (import is already `import ingestion_engine`):

```python
# ---------------------------------------------------------------------------
# Relevance gate wiring
# ---------------------------------------------------------------------------

_GATE_RTP_META = {
    "Magna International": {
        "metadata_record_status": "active",
        "canonical_name": "Magna",
        "company_identity_terms": ["Magna"],
        "manual_aliases": [],
        "exclude_terms": ["casino", "real-time payments"],
    }
}


def _serper_like(title):
    return {"url": "https://x/s", "title": title, "provider": "serper",
            "description": "", "zoominfo_company_id": None, "raw": {}}


def _zi_like(title, description=""):
    return {"url": "https://x/z", "title": title, "provider": "zoominfo",
            "description": description, "zoominfo_company_id": 12345678, "raw": {}}


def test_gate_helper_ignores_serper_candidate():
    d = ingestion_engine._gate_zoominfo_candidate(
        _serper_like("Casino night"), "Magna International", _GATE_RTP_META)
    assert d is None


def test_gate_helper_noop_when_metadata_empty():
    d = ingestion_engine._gate_zoominfo_candidate(
        _zi_like("Casino night"), "Magna International", {})
    assert d is None


def test_gate_helper_noop_when_no_record_for_target():
    d = ingestion_engine._gate_zoominfo_candidate(
        _zi_like("Casino night"), "Unknown Co", _GATE_RTP_META)
    assert d is None


def test_gate_helper_ignores_non_active_record():
    meta = {"Magna International": dict(_GATE_RTP_META["Magna International"],
                                       metadata_record_status="orphaned")}
    d = ingestion_engine._gate_zoominfo_candidate(
        _zi_like("Casino night"), "Magna International", meta)
    assert d is None


def test_gate_helper_drops_exclude_without_rescue():
    d = ingestion_engine._gate_zoominfo_candidate(
        _zi_like("Casino night downtown"), "Magna International", _GATE_RTP_META)
    assert d is not None and d.drop is True
    assert d.matched_exclude == "casino"


def test_gate_helper_keeps_with_identity_rescue():
    d = ingestion_engine._gate_zoominfo_candidate(
        _zi_like("Magna opens near a casino"), "Magna International", _GATE_RTP_META)
    assert d is not None and d.drop is False
```

- [ ] **Step 2: Run to verify they fail**

Run: `python -m pytest tests/test_zoominfo.py -k gate_helper -v`
Expected: FAIL — `AttributeError: module 'ingestion_engine' has no attribute '_gate_zoominfo_candidate'`.

- [ ] **Step 3: Add the import, flag reader, helper, and counter**

In `ingestion_engine.py`:

(a) Near the other top-level imports (e.g. after `import zoominfo_client`):

```python
import relevance_gate
```

(b) Near `_zoominfo_news_enabled()` (after line 311), add:

```python
def _relevance_gate_enabled() -> bool:
    """True when ZOOMINFO_RELEVANCE_GATE_ENABLED is a recognised truthy value.
    Default off — production behavior is unchanged until explicitly enabled."""
    return os.environ.get("ZOOMINFO_RELEVANCE_GATE_ENABLED", "").strip().lower() in _TRUTHY_ENV_VALUES


def _gate_zoominfo_candidate(candidate: dict, entity_name: str,
                             target_metadata: dict) -> Optional[relevance_gate.GateDecision]:
    """Evaluate the relevance gate for one candidate, or return None when the
    gate does not apply (non-ZoomInfo provider, gate disabled / empty metadata,
    no record for the target, or a non-active record). Never raises."""
    if candidate.get("provider") != "zoominfo" or not target_metadata:
        return None
    record = target_metadata.get(entity_name)
    if not record or record.get("metadata_record_status") != "active":
        return None
    return relevance_gate.evaluate(
        title=candidate.get("title", ""),
        description=candidate.get("description", ""),
        record=record,
    )
```

(Confirm `Optional` is already imported from `typing` at the top of `ingestion_engine.py`; it is used in existing signatures, so no new import is needed.)

(c) In `_new_provider_yield()` (353-357), add the `relevance_dropped` key:

```python
def _new_provider_yield() -> dict:
    return {
        "discovered": 0, "scraped": 0, "stored": 0,
        "discards": 0, "relevance_dropped": 0, "scrape_failed": 0, "duplicates": 0,
    }
```

(d) In `_log_provider_yield()` (360-369), add `relevance_dropped` to the format string and args:

```python
        logger.info(
            "Provider yield — %s discovered=%d scraped=%d stored=%d "
            "discards=%d relevance_dropped=%d scrape_failed=%d duplicates=%d",
            provider, y["discovered"], y["scraped"], y["stored"],
            y["discards"], y["relevance_dropped"], y["scrape_failed"], y["duplicates"],
        )
```

- [ ] **Step 4: Update the existing yield-line assertion**

In `tests/test_zoominfo.py`, the test `test_zoominfo_yield_line_logged_with_zero_candidates` (around line 1106) asserts the exact yield string. Update it to include the new counter:

```python
    assert (
        "Provider yield — zoominfo discovered=0 scraped=0 stored=0 "
        "discards=0 relevance_dropped=0 scrape_failed=0 duplicates=0" in caplog.text
    )
```

- [ ] **Step 5: Run helper tests + the yield-line test**

Run: `python -m pytest tests/test_zoominfo.py -k "gate_helper or yield_line" -v`
Expected: PASS — helper tests pass, and the updated yield-line assertion matches.

- [ ] **Step 6: Add the metadata load and the in-loop gate block**

In `execute_pipeline()`:

(a) After `targets = load_targets("targets.yaml")` (line 913), add:

```python
    target_metadata = (
        relevance_gate.load_target_metadata("target_metadata.yaml")
        if _relevance_gate_enabled() else {}
    )
```

(b) In the candidate loop, immediately AFTER the semantic-duplicate `continue` block (ends line 1004) and BEFORE `scrapes_attempted += 1` (line 1006), insert:

```python
            gate_decision = _gate_zoominfo_candidate(candidate, entity_name, target_metadata)
            if gate_decision is not None and gate_decision.drop:
                logger.info(
                    "RELEVANCE_GATE drop (%s): exclude=%r no identity rescue | %s",
                    provider, gate_decision.matched_exclude, normalized,
                )
                _bump(provider, "relevance_dropped")
                suppression_ledger = suppression_ledger.record(
                    "zoominfo_company_mismatch", url=raw_url, title=candidate_title,
                )
                continue
```

- [ ] **Step 7: Write the integration wiring tests**

Append to `tests/test_zoominfo.py` (reuses `_stub_pipeline_internals` from this file; it writes `targets.yaml` with the single active entity "Magna International" and chdirs to tmp_path):

```python
def _write_gate_metadata(tmp_path):
    (tmp_path / "target_metadata.yaml").write_text(textwrap.dedent(
        """\
        version: 1
        targets:
          Magna International:
            metadata_record_status: active
            canonical_name: Magna
            company_identity_terms:
            - Magna
            manual_aliases: []
            exclude_terms:
            - casino
            - real-time payments
        """
    ))


def test_pipeline_gate_drops_false_positive_before_scrape(monkeypatch, tmp_path, caplog):
    monkeypatch.setenv("ZOOMINFO_RELEVANCE_GATE_ENABLED", "true")
    captured: list[dict] = []
    scraped: list[str] = []
    # A ZoomInfo candidate with an exclude term and NO identity term -> drop.
    candidate = _zi_like("Casino jackpot hits record", "real-time payments rollout")
    _stub_pipeline_internals(monkeypatch, tmp_path, candidate, captured)
    monkeypatch.setattr(ingestion_engine, "scrape_article",
                        lambda url, mn: scraped.append(url) or "body")
    _write_gate_metadata(tmp_path)

    with caplog.at_level("INFO"):
        ingestion_engine.execute_pipeline()

    assert captured == []          # nothing stored
    assert scraped == []           # dropped BEFORE scrape
    assert "RELEVANCE_GATE drop" in caplog.text


def test_pipeline_gate_keeps_identity_rescue(monkeypatch, tmp_path):
    monkeypatch.setenv("ZOOMINFO_RELEVANCE_GATE_ENABLED", "true")
    captured: list[dict] = []
    candidate = _zi_like("Magna expansion near casino district", "")
    _stub_pipeline_internals(monkeypatch, tmp_path, candidate, captured)
    _write_gate_metadata(tmp_path)

    ingestion_engine.execute_pipeline()

    assert len(captured) == 1      # identity rescue -> stored despite "casino"


def test_pipeline_gate_off_is_noop(monkeypatch, tmp_path):
    monkeypatch.delenv("ZOOMINFO_RELEVANCE_GATE_ENABLED", raising=False)
    captured: list[dict] = []
    candidate = _zi_like("Casino jackpot hits record", "real-time payments rollout")
    _stub_pipeline_internals(monkeypatch, tmp_path, candidate, captured)
    _write_gate_metadata(tmp_path)

    ingestion_engine.execute_pipeline()

    assert len(captured) == 1      # gate disabled -> false positive still stored


def test_pipeline_gate_never_gates_serper(monkeypatch, tmp_path):
    monkeypatch.setenv("ZOOMINFO_RELEVANCE_GATE_ENABLED", "true")
    captured: list[dict] = []
    candidate = _serper_like("Casino jackpot hits record")
    _stub_pipeline_internals(monkeypatch, tmp_path, candidate, captured)
    _write_gate_metadata(tmp_path)

    ingestion_engine.execute_pipeline()

    assert len(captured) == 1      # serper is never gated
```

- [ ] **Step 8: Run the full ZoomInfo + relevance suites**

Run: `python -m pytest tests/test_zoominfo.py tests/test_relevance_gate.py -v`
Expected: PASS (all, including the four new integration tests).

- [ ] **Step 9: Run the entire suite (no regressions)**

Run: `python -m pytest tests/ -q`
Expected: PASS — previously 421 passed; now 421 + new tests, 0 failures.

- [ ] **Step 10: Commit**

```bash
git add ingestion_engine.py tests/test_zoominfo.py
git commit -m "feat(ingestion): wire ZoomInfo relevance gate before scrape behind flag"
```

---

## Task 5: Update CLAUDE.md reason-code count

**Files:**
- Modify: `CLAUDE.md` (the `suppression_ledger.py` description line)

- [ ] **Step 1: Update the count**

In `CLAUDE.md`, find the `suppression_ledger.py` description:

> ... owning the suppression reason taxonomy (4 ingestion-owned + 9 delivery-owned codes) ...

Change `4 ingestion-owned` to `5 ingestion-owned`:

```
**`suppression_ledger.py`** — Pure in-process module owning the suppression reason taxonomy (5 ingestion-owned + 9 delivery-owned codes), `SAMPLES_CAP = 10`, and the same-day-retry merge semantics.
```

- [ ] **Step 2: Verify the count matches code**

Run: `python -c "from suppression_ledger import INGESTION_CODES, DELIVERY_CODES; print(len(INGESTION_CODES), len(DELIVERY_CODES))"`
Expected: `5 9`

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(claude): bump ingestion reason-code count to 5"
```

---

## Final verification

- [ ] **Run the full suite:** `python -m pytest tests/ -q` → all pass, 0 failures.
- [ ] **Confirm flag-off is a true no-op:** with `ZOOMINFO_RELEVANCE_GATE_ENABLED` unset, `target_metadata` is `{}` and `_gate_zoominfo_candidate` returns `None` for every candidate (covered by `test_pipeline_gate_off_is_noop`).
- [ ] **Confirm no delivery/ZoomInfo-request changes:** `git diff main --stat` shows only `relevance_gate.py`, `tests/test_relevance_gate.py`, `suppression_ledger.py`, `tests/test_suppression_ledger.py`, `ingestion_engine.py`, `tests/test_zoominfo.py`, `CLAUDE.md`, and the spec/plan docs. No `delivery_engine.py`, no `zoominfo_client.py`.

## Out of scope (do NOT do in this PR)

- Flipping the flag on in the production scheduled workflow (separate small config PR).
- Removing the metadata dry-run workflow (deferred until after validation).
- Any shadow/log-only mode.
- Adding `exclude_terms` to non-RTP targets.
