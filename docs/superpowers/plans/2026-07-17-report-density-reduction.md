# Report Density Reduction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cap the daily email at 5 visible cards per segment (overflow flows into the Additional Articles appendix), cap the Macroeconomic Outlook at 3 signals, and shrink each segment synthesis to one sentence.

**Architecture:** All behavior changes land in the two pure modules (`report.py` for the appendix band + macro-outlook slice, `prompts.py` for the signal-cap constant and thematic wording) plus one config value in `market_pulse_config.yaml`. No I/O, schema, or migration changes. Spec: `docs/superpowers/specs/2026-07-17-report-density-reduction-design.md`.

**Tech Stack:** Python 3, pytest, YAML config. Tests use dict literals and zero patches (existing pattern in `tests/test_pipeline.py`).

**Conventions that matter here:**
- Run tests with `pytest tests/ -q` (all) or `pytest tests/test_pipeline.py::<name> -v` (one).
- `model.ledger.breakdown` is a plain dict property — read counts with `["key"]` / `.get("key", 0)`.
- Semantic headline dedup fires at `token_sort_ratio >= 88` — test articles in the same run need genuinely distinct headlines or they get suppressed before the code under test runs. Reuse the realistic headline strings shown in each test below; do not substitute `"Headline 1"`-style strings.
- Structured logging with `%s` placeholders; type hints on all signatures (no changes below add logging, but keep this if you deviate).

---

### Task 0: Branch

- [ ] **Step 1: Create the working branch**

```bash
cd /workspaces/market-pulse
git checkout -b report-density-reduction
```

---

### Task 1: Macro Outlook signal cap 6 → 3 (constant + validator truncation test)

The single constant `MAX_MACRO_OUTLOOK_SIGNALS` in `prompts.py` drives both the macro prompt's promised signal count (f-string at `prompts.py:407`) and the validator truncation in `ingestion_engine._validate_macro_outlook` (`ingestion_engine.py:674`). No existing test pins the number.

**Files:**
- Modify: `prompts.py:61`
- Test: `tests/test_pipeline.py` (macro-outlook validation section — the `_macro_signal` / `_macro_outlook` helpers live near line 676)
- Test: `tests/test_prompts.py` (macro prompt-contract section — uses the module-level `_article` helper at line 14)

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_prompts.py` (after the existing macro-prompt tests):

```python
def test_macro_prompt_promises_signal_cap():
    """The macro system prompt promises the same signal cap the validator
    enforces, and the product cap is 3 (reduced from 6 on 2026-07-17)."""
    mp = prompts.macro_prompt([_article("Manufacturing PMI slips again", score=8)])
    assert f"up to {prompts.MAX_MACRO_OUTLOOK_SIGNALS}," in mp.system
    assert "up to 3," in mp.system
```

Add after the existing `_validate_macro_outlook` tests (search for `def _macro_outlook(` and place the new test after the last test in that section):

```python
def test_validate_macro_outlook_truncates_at_cap():
    """The validator keeps at most MAX_MACRO_OUTLOOK_SIGNALS signals, and the
    product cap is 3 (reduced from 6 on 2026-07-17 for report density)."""
    from prompts import MAX_MACRO_OUTLOOK_SIGNALS

    assert MAX_MACRO_OUTLOOK_SIGNALS == 3
    signals = [_macro_signal(indicator=f"Indicator {i}") for i in range(5)]
    result = _validate_macro_outlook(_macro_outlook(signals=signals), _MACRO_VALID_IDS)
    assert [s["indicator"] for s in result["signals"]] == [
        "Indicator 0", "Indicator 1", "Indicator 2",
    ]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_pipeline.py::test_validate_macro_outlook_truncates_at_cap tests/test_prompts.py::test_macro_prompt_promises_signal_cap -v`
Expected: both FAIL — the constant is currently 6, so the `== 3` assertion and the `"up to 3,"` literal both miss.

- [ ] **Step 3: Change the constant**

In `prompts.py` line 61:

```python
# Before
MAX_MACRO_OUTLOOK_SIGNALS = 6
# After
MAX_MACRO_OUTLOOK_SIGNALS = 3
```

- [ ] **Step 4: Run tests to verify they pass, then the full suite**

Run: `pytest tests/test_pipeline.py::test_validate_macro_outlook_truncates_at_cap tests/test_prompts.py::test_macro_prompt_promises_signal_cap -v`
Expected: both PASS
Run: `pytest tests/ -q`
Expected: all pass (no existing test pins the old value; if one fails on a "6", update it to reference `prompts.MAX_MACRO_OUTLOOK_SIGNALS` instead of a literal).

- [ ] **Step 5: Commit**

```bash
git add prompts.py tests/test_pipeline.py tests/test_prompts.py
git commit -m "feat(report): cap Macroeconomic Outlook at 3 signals"
```

---

### Task 2: Render-side slice — stored rows with >3 signals render at most 3

Rows written to `daily_summaries` before Task 1 deploys can hold up to 6 signals; the QA workflow re-renders old rows with `run_ingestion=false`. Slice at report assembly so the cap holds immediately.

**Files:**
- Modify: `report.py` (imports near line 27; `_extract_macro_outlook` at lines 286–301)
- Test: `tests/test_pipeline.py` (place next to the other `assemble_report` tests, e.g. after the appendix section)

- [ ] **Step 1: Write the failing test**

```python
def test_report_macro_outlook_sliced_to_cap():
    """daily_summaries rows stored before the cap reduction may hold up to 6
    signals; assemble_report slices to MAX_MACRO_OUTLOOK_SIGNALS so QA
    re-renders (run_ingestion=false) comply immediately."""
    from prompts import MAX_MACRO_OUTLOOK_SIGNALS

    signals = [
        {
            "indicator": f"Indicator {i}",
            "direction": "Declining",
            "americhem_implication": "Downside risk for resin demand.",
            "affected_segments": ["Industrial"],
            "citation_source_ids": [1],
        }
        for i in range(6)
    ]
    macro_summary = {
        "macro_outlook": {"current_condition": "Manufacturing demand mixed.",
                          "signals": signals},
    }
    rows = [_make_new_article("a", 8, commercial_segment="Packaging",
                              headline="Packaging demand firms on brand-owner restocking")]
    model = assemble_report(rows, macro_summary=macro_summary)

    assert [s["indicator"] for s in model.macro_outlook["signals"]] == [
        "Indicator 0", "Indicator 1", "Indicator 2",
    ]
    assert len(model.macro_outlook["signals"]) == MAX_MACRO_OUTLOOK_SIGNALS
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_pipeline.py::test_report_macro_outlook_sliced_to_cap -v`
Expected: FAIL — 6 signals come back (no slice yet).

- [ ] **Step 3: Implement the slice**

In `report.py`, add the import after the existing pure-module imports (below `from scoring import Scoring`):

```python
from prompts import MAX_MACRO_OUTLOOK_SIGNALS
```

(`prompts.py` is a pure module — no I/O/clock/env — so the module's purity-by-import-graph contract holds.)

Replace the body of `_extract_macro_outlook` (currently returns `outlook` unchanged):

```python
def _extract_macro_outlook(macro_summary: Optional[dict]) -> Optional[dict]:
    """Pull a renderable macro_outlook out of the macro-summary row: a dict with
    a non-empty current_condition and at least one signal. Anything else
    (missing, None, malformed, empty signals) becomes None, so the renderer
    shows no section. Signal *contents* were validated at ingestion
    (_validate_macro_outlook); this is the defensive read of a stored row.
    Signals are sliced to MAX_MACRO_OUTLOOK_SIGNALS so rows stored before a
    cap reduction render at most the current cap (returns a new dict — never
    mutates the stored row)."""
    outlook = (macro_summary or {}).get("macro_outlook")
    if not isinstance(outlook, dict):
        return None
    current = outlook.get("current_condition")
    signals = outlook.get("signals")
    if not isinstance(current, str) or not current.strip():
        return None
    if not isinstance(signals, list) or not signals:
        return None
    return {"current_condition": current,
            "signals": signals[:MAX_MACRO_OUTLOOK_SIGNALS]}
```

- [ ] **Step 4: Run test to verify it passes, then the full suite**

Run: `pytest tests/test_pipeline.py::test_report_macro_outlook_sliced_to_cap -v`
Expected: PASS
Run: `pytest tests/ -q`
Expected: all pass (existing macro-outlook render tests use ≤3 signals or don't count them; if one constructs >3 signals and counts them, cut its fixture to 3).

- [ ] **Step 5: Commit**

```bash
git add report.py tests/test_pipeline.py
git commit -m "feat(report): slice stored macro_outlook signals to the cap at assembly"
```

---

### Task 3: Cap overflow flows into the Additional Articles appendix

Widen the appendix eligibility band from "weak-relevance only (4–5)" to "supporting threshold and above (≥ 4), not shown as a visible card". The caller already excludes visible-card hashes, so capped-out score-6+ rows become appendix candidates; the existing impact-desc sort ranks them ahead of the 4–5 band automatically. Ledger semantics do not change.

**Files:**
- Modify: `report.py` (`_is_usable_additional_article` at lines 210–221; docstring of `_select_additional_articles` at lines 249–260; step-6 comment at lines 367–368)
- Test: `tests/test_pipeline.py` (rewrite `test_report_capped_articles_do_not_reappear` at line 1496; add two tests in the appendix section)

- [ ] **Step 1: Rewrite the expectation-flip test (currently pins the old behavior)**

Replace `test_report_capped_articles_do_not_reappear` (tests/test_pipeline.py:1496–1517) entirely with:

```python
def test_report_capped_articles_flow_into_appendix():
    """Articles dropped by the per-segment cap reappear in the Additional
    Articles appendix — never as visible cards. (Flipped 2026-07-17: the cap
    previously dropped overflow entirely.)"""
    # Genuinely distinct headlines — token_sort_ratio >= 88 would otherwise
    # suppress them as semantic duplicates before the cap runs.
    _hc_headlines = [
        "Hospital network merger squeezes specialty polymer volumes",
        "FDA clears new implantable-grade compound for cardiac devices",
        "Aging population drives record demand for medical-grade resins",
        "Generic drug expansion pressures premium plastics pricing",
    ]
    articles = [
        _make_new_article(
            f"h{i}", americhem_impact_score=10 - i,
            commercial_segment="Healthcare",
            headline=_hc_headlines[i],
        )
        for i in range(4)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 3,
            "max_total_visible_articles": 12,
        }
    }
    model = assemble_report(articles, config=config)

    # Top 3 by impact are cards; h3 (impact=7) is capped out but not lost.
    assert [a["url_hash"] for a in model.groups["Healthcare"]] == ["h0", "h1", "h2"]
    assert [a["url_hash"] for a in model.additional_articles] == ["h3"]
    assert model.surfaced_count == 3

    html = render_report(model, today_str=_TODAY_STR)
    assert _hc_headlines[3] in html
```

- [ ] **Step 2: Add the ordering test (overflow ranks ahead of the 4–5 band)**

Add in the Additional Articles appendix section of `tests/test_pipeline.py`:

```python
def test_appendix_ranks_cap_overflow_ahead_of_weak_relevance():
    """Capped-out visible-band rows (impact >= 6) precede weak-relevance
    (4-5) rows in the appendix — the existing impact-desc sort, wider band."""
    articles = [
        _make_new_article("v0", 10, commercial_segment="Healthcare",
                          headline="Hospital network merger squeezes specialty polymer volumes"),
        _make_new_article("v1", 9, commercial_segment="Healthcare",
                          headline="FDA clears new implantable-grade compound for cardiac devices"),
        _make_new_article("v2", 7, commercial_segment="Healthcare",
                          headline="Aging population drives record demand for medical-grade resins"),
        _make_new_article("w0", 5, commercial_segment="Packaging",
                          headline="Beverage brands trial mono-material caps in European pilot"),
    ]
    config = {"reporting": {"visible_impact_threshold": 6,
                            "max_visible_articles_per_segment": 2}}
    model = assemble_report(articles, config=config)

    assert [a["url_hash"] for a in model.groups["Healthcare"]] == ["v0", "v1"]
    assert [a["url_hash"] for a in model.additional_articles] == ["v2", "w0"]
```

- [ ] **Step 3: Add the ledger-accounting test**

```python
def test_appendix_overflow_does_not_alter_ledger_counts():
    """Capped-out rows are displayed, not suppressed: they never enter
    weak_relevance, and below_impact_threshold still counts only
    suppression-surviving below-visible rows."""
    articles = [
        _make_new_article("v0", 10, commercial_segment="Healthcare",
                          headline="Hospital network merger squeezes specialty polymer volumes"),
        _make_new_article("v1", 7, commercial_segment="Healthcare",
                          headline="FDA clears new implantable-grade compound for cardiac devices"),
        _make_new_article("w0", 4, commercial_segment="Packaging",
                          headline="Beverage brands trial mono-material caps in European pilot"),
    ]
    config = {"reporting": {"visible_impact_threshold": 6,
                            "max_visible_articles_per_segment": 1}}
    model = assemble_report(articles, config=config)

    # w0 is the only below-visible survivor; v1 (visible-band, capped) is not counted.
    assert model.ledger.breakdown["below_impact_threshold"] == 1
    # w0 is shown in the appendix, so it is not "shown nowhere".
    assert model.ledger.breakdown.get("weak_relevance", 0) == 0
    assert model.surfaced_count == 1
    assert [a["url_hash"] for a in model.additional_articles] == ["v1", "w0"]
```

- [ ] **Step 4: Run the three tests to verify they fail**

Run: `pytest tests/test_pipeline.py::test_report_capped_articles_flow_into_appendix tests/test_pipeline.py::test_appendix_ranks_cap_overflow_ahead_of_weak_relevance tests/test_pipeline.py::test_appendix_overflow_does_not_alter_ledger_counts -v`
Expected: all three FAIL — capped/visible-band rows are absent from `additional_articles` (old band is weak-relevance only).

- [ ] **Step 5: Widen the eligibility band**

Replace `_is_usable_additional_article` (report.py:210–221):

```python
def _is_usable_additional_article(row: dict, scorer: Scoring) -> bool:
    """True when a row qualifies for the optional-discovery appendix: it scores
    at or above the supporting threshold — the weak-relevance band plus
    visible-band rows that lost their card slot to a cap — and carries a
    non-blank headline and source URL.

    Delivery-suppression survival and 'not already a visible card' are enforced
    by the caller (it selects from `kept` minus the final-group hashes)."""
    return (
        (scorer.is_weak_relevance(row) or scorer.is_visible(row))
        and bool((row.get("headline") or "").strip())
        and bool((row.get("source_url") or "").strip())
    )
```

Update the `_select_additional_articles` docstring ordering sentence (report.py:255–260) — replace:

```
    Deterministic order (applied as stable sorts, least-significant first):
    url_hash asc -> normalized headline asc -> recency desc -> effective impact
    desc. So every score-5 precedes every score-4, ties break by recency then
    headline then hash. Capped at `cap`."""
```

with:

```
    Deterministic order (applied as stable sorts, least-significant first):
    url_hash asc -> normalized headline asc -> recency desc -> effective impact
    desc. So cap overflow (score 6+) precedes every score-5, which precedes
    every score-4; ties break by recency then headline then hash. Capped at
    `cap`."""
```

Update the step-6 comment in `assemble_report` (report.py:367–368) — replace:

```python
    # 6. Optional-discovery appendix: suppression survivors in the weak-relevance
    #    band not shown as cards. Does NOT alter surfaced_count.
```

with:

```python
    # 6. Optional-discovery appendix: suppression survivors at/above the
    #    supporting threshold not shown as cards — the weak-relevance band plus
    #    rows capped out of their segment. Does NOT alter surfaced_count.
```

- [ ] **Step 6: Run the three tests, then the full suite**

Run: `pytest tests/test_pipeline.py::test_report_capped_articles_flow_into_appendix tests/test_pipeline.py::test_appendix_ranks_cap_overflow_ahead_of_weak_relevance tests/test_pipeline.py::test_appendix_overflow_does_not_alter_ledger_counts -v`
Expected: PASS.
Run: `pytest tests/ -q`
Expected: all pass. Watch the existing appendix block (tests/test_pipeline.py:1520 onward) and `test_assemble_report_total_articles_cap`: if any test asserts a capped-out or total-cap-dropped visible row appears nowhere, flip that assertion to expect it in `additional_articles` (same rationale as Step 1 — the spec makes this expectation flip explicit).

- [ ] **Step 7: Commit**

```bash
git add report.py tests/test_pipeline.py
git commit -m "feat(report): flow cap overflow into the Additional Articles appendix"
```

---

### Task 4: Production config — per-segment cap of 5

**Files:**
- Modify: `market_pulse_config.yaml:16-20`

- [ ] **Step 1: Set the cap and update the comment**

Replace lines 16–20 of `market_pulse_config.yaml`:

```yaml
  # Optional caps on visible cards. null (the default) means show EVERY article
  # at or above visible_impact_threshold. Set either to an integer to re-impose
  # a cap if the report gets noisy — a config-only change, no code edit.
  max_visible_articles_per_segment: null   # e.g. 3 to cap per-segment
  max_total_visible_articles: null         # e.g. 12 to cap the total
```

with:

```yaml
  # Optional caps on visible cards. null means show EVERY article at or above
  # visible_impact_threshold. Per-segment cap re-imposed at 5 on 2026-07-17
  # (full-target coverage made the uncapped report too long); capped-out
  # articles flow into the Additional Articles appendix, they are not dropped.
  max_visible_articles_per_segment: 5      # top 5 by impact score per segment
  max_total_visible_articles: null         # e.g. 12 to cap the total
```

- [ ] **Step 2: Verify the file parses and the value reads back**

Run: `python -c "import yaml; print(yaml.safe_load(open('market_pulse_config.yaml'))['reporting']['max_visible_articles_per_segment'])"`
Expected output: `5`

- [ ] **Step 3: Run the full suite (tests use dict-literal configs, so nothing should move)**

Run: `pytest tests/ -q`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add market_pulse_config.yaml
git commit -m "feat(report): cap visible cards at 5 per segment"
```

---

### Task 5: One-sentence segment summaries

Prompt-only change. `synthesize_thematic_paragraphs` validation (free-form string per category) and the renderer stay untouched — an occasionally long sentence degrades gracefully instead of being rejected.

**Files:**
- Modify: `prompts.py:464-475` (`thematic_prompt` system text)
- Test: `tests/test_prompts.py:299-310` (`test_thematic_user_contains_category_blocks_and_impact_lines`)

- [ ] **Step 1: Update the test to pin the new wording**

In `tests/test_prompts.py`, replace the last assertion of `test_thematic_user_contains_category_blocks_and_impact_lines` (line 310):

```python
    assert "exactly one synthesis paragraph" in spec.system
```

with:

```python
    assert "exactly one synthesis sentence" in spec.system
    assert "maximum 30 words" in spec.system
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_prompts.py::test_thematic_user_contains_category_blocks_and_impact_lines -v`
Expected: FAIL — system text still says "exactly one synthesis paragraph (2–3 sentences)".

- [ ] **Step 3: Rewrite the thematic system prompt**

In `prompts.py` (`thematic_prompt`, lines 464–475), replace:

```python
    system = (
        f"OUTPUT LANGUAGE:\n{ENGLISH_OUTPUT_RULE}\n\n"
        "You are a market intelligence analyst for Americhem, a specialty plastics compounder.\n\n"
        "For each CATEGORY block below, write exactly one synthesis paragraph (2–3 sentences).\n"
        "The paragraph must:\n"
        "- Identify the shared trend or structural driver across the listed signals\n"
        "- Explicitly state the implication for Americhem's supply chain, demand pipeline, or margin\n"
        "- Be written for a senior executive who will act on it — no hedging, no filler\n\n"
        "Return valid JSON with category names as keys and synthesis paragraphs as values.\n"
        "Use the exact category names provided. Do not invent categories.\n"
        "Only include categories that appear in the input."
    )
```

with:

```python
    system = (
        f"OUTPUT LANGUAGE:\n{ENGLISH_OUTPUT_RULE}\n\n"
        "You are a market intelligence analyst for Americhem, a specialty plastics compounder.\n\n"
        "For each CATEGORY block below, write exactly one synthesis sentence (maximum 30 words).\n"
        "The sentence must:\n"
        "- Fuse the shared trend or structural driver across the listed signals with "
        "its implication for Americhem's supply chain, demand pipeline, or margin\n"
        "- Be written for a senior executive who will act on it — no hedging, no filler\n\n"
        "Return valid JSON with category names as keys and synthesis sentences as values.\n"
        "Use the exact category names provided. Do not invent categories.\n"
        "Only include categories that appear in the input."
    )
```

- [ ] **Step 4: Run test to verify it passes, then the full suite**

Run: `pytest tests/test_prompts.py::test_thematic_user_contains_category_blocks_and_impact_lines -v`
Expected: PASS
Run: `pytest tests/ -q`
Expected: all pass.

- [ ] **Step 5 (optional sanity, zero API spend): eyeball the assembled prompt**

Run: `python scripts/show_prompts.py | grep -A4 "synthesis sentence"`
Expected: the new one-sentence instruction appears in the thematic prompt dump.

- [ ] **Step 6: Commit**

```bash
git add prompts.py tests/test_prompts.py
git commit -m "feat(prompts): one-sentence segment synthesis (max 30 words)"
```

---

### Task 6: Documentation sync (CLAUDE.md, CONTEXT.md) + final verification

**Files:**
- Modify: `CLAUDE.md:86-87` and `CLAUDE.md:127-128`
- Modify: `CONTEXT.md` ("Additional Articles to Explore" entry, lines 68–75)

- [ ] **Step 1: Update CLAUDE.md report-pipeline steps 4 and 5**

Line 86 — replace the tail:

```
An integer re-imposes the cap (a config-only rollback); capped-out rows are dropped, there is no fallback section.
```

with:

```
An integer re-imposes the cap (a config-only rollback); capped-out rows flow into the Additional Articles appendix (step 5), never a visible card. Production sets `max_visible_articles_per_segment: 5` (since 2026-07-17).
```

Line 87 — replace:

```
suppression-surviving rows in the weak-relevance band (`supporting ≤ effective_impact < visible`, i.e. score 4–5 today) that are not visible cards
```

with:

```
suppression-surviving rows scoring at or above the supporting threshold (≥ 4) that are not visible cards — the weak-relevance band plus visible-band rows capped out of their segment
```

- [ ] **Step 2: Update CLAUDE.md config-file section (lines 127–128)**

Line 127 — replace the parenthetical:

```
(the eligibility band is `Scoring.is_weak_relevance`)
```

with:

```
(the appendix band is "supporting threshold and above, not shown as a card" — weak-relevance rows plus cap overflow)
```

Line 128 — replace:

```
- `reporting.max_visible_articles_per_segment` and `reporting.max_total_visible_articles` (both default `null` = uncapped) — set either to an integer to cap per-segment / total visible cards if the report gets noisy. Config-only, no code change.
```

with:

```
- `reporting.max_visible_articles_per_segment` (default `null` = uncapped; production sets **5**) and `reporting.max_total_visible_articles` (default `null`) — integer caps on per-segment / total visible cards. Capped-out rows flow into the Additional Articles appendix. Config-only, no code change.
```

- [ ] **Step 3: Update CONTEXT.md's appendix entry**

In the "Additional Articles to Explore" glossary entry (lines 68–75), replace:

```
  (`ReportModel.additional_articles`): suppression-surviving weak-relevance
  (score 4–5) rows that are not visible cards, ranked deterministically and
```

with:

```
  (`ReportModel.additional_articles`): suppression-surviving rows scoring at
  or above the supporting threshold (≥ 4) that are not visible cards — the
  weak-relevance band plus cap overflow — ranked deterministically and
```

Also scan CONTEXT.md's Macroeconomic Outlook entry for a hardcoded signal count ("6" / "six"); update to 3 if present (earlier inspection found none, but verify).

- [ ] **Step 4: Full suite + working-tree check**

Run: `pytest tests/ -q`
Expected: all pass.
Run: `git status --short`
Expected: only the two doc files modified (everything else committed in Tasks 1–5).

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md CONTEXT.md
git commit -m "docs: sync CLAUDE.md/CONTEXT.md with density caps and widened appendix band"
```

---

### Completion

After Task 6, all spec requirements are implemented:

| Spec section | Task |
|---|---|
| Per-segment cap = 5 (config) | 4 |
| Overflow → appendix | 3 |
| Macro cap = 3 (prompt + validator) | 1 |
| Macro cap on stored rows (render-side slice) | 2 |
| One-sentence summaries | 5 |
| Docs (CLAUDE.md / CONTEXT.md) | 6 |

Use the superpowers:finishing-a-development-branch skill to integrate (`report-density-reduction` → PR to `main`). Verification beyond tests: a QA re-render via the test workflow (`run_ingestion=false`, `send_email=true` to the Jason-only pool) will show the capped layout against yesterday's real rows — the macro slice from Task 2 applies immediately; segment overflow only shows once a segment actually exceeds 5.
