# So-What Honesty, Appendix Breadth, and Direction Cue — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the LLM's "So what" line from spinning direction (invented upside on Americhem-negative news, upside claimed in adjacent markets), widen the Additional Articles appendix for quick-scanning, and add a colored direction glyph to the So-What line.

**Architecture:** Three independent changes per the approved spec (`docs/superpowers/specs/2026-08-03-so-what-honesty-and-breadth-design.md`): (1) text-only edits to `_SYSTEM_PROMPT_BASE` in `prompts.py` — a RULE 2 competitor-default line and a RULE 6 rewrite; (2) two value changes in `market_pulse_config.yaml` (production config only, code defaults untouched); (3) a sentiment-keyed glyph prepended to the So-What label in `delivery_engine._render_card`, reusing the existing `_SENTIMENT_TAG_COLORS` map.

**Tech Stack:** Python 3, pytest. Prompt tests are pure (no fakes, no patching) in `tests/test_prompts.py`; card render tests use dict literals in `tests/test_pipeline.py`.

**Conventions that apply here (from CLAUDE.md):** type hints on signatures; prompt assembly is `str.replace()`, never `.format()` — the literal JSON braces in `_SYSTEM_PROMPT_BASE` are load-bearing and pinned by an existing test. Do not touch the schema block at the bottom of the prompt.

---

### Task 1: RULE 2 competitor-default line (prompts.py)

**Files:**
- Modify: `prompts.py` (RULE 2 block inside `_SYSTEM_PROMPT_BASE`, currently lines 184–194)
- Test: `tests/test_prompts.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_prompts.py`, after `test_insight_rule4_injects_labels_and_descriptions` (a new section comment plus test). `_insight_spec()` is the existing helper at the top of the file:

```python
# ---------------------------------------------------------------------------
# Insight prompt — So-What honesty rules (RULE 2 competitor default, RULE 6)
# ---------------------------------------------------------------------------

def test_rule2_carries_the_competitor_default():
    """A competitor's success must default to Negative/Neutral for Americhem —
    the tag is fixed at the source so RULE 6's consistency check inherits it."""
    system = _insight_spec().system
    assert "COMPETITOR DEFAULT" in system
    assert "competitive threat, not an Americhem opportunity" in system
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_prompts.py::test_rule2_carries_the_competitor_default -v`
Expected: FAIL with `AssertionError` (anchor not in prompt)

- [ ] **Step 3: Edit RULE 2 in `prompts.py`**

In `_SYSTEM_PROMPT_BASE`, the RULE 2 block currently ends:

```
IMPORTANT: sentiment_tag is direction only. A barely-relevant article can be Negative.
A neutral article can have a high impact score. Do NOT conflate tone with importance.
```

Append a new paragraph directly after those two lines (before the `Also assign sentiment_score` paragraph):

```
COMPETITOR DEFAULT: A competitor's product launch, contract win, capacity expansion, or
other success is "Negative" or "Neutral" for Americhem by default — it is a competitive
threat, not an Americhem opportunity. Tag it "Positive" only if the article demonstrates
growth in a market Americhem serves that outweighs the competitive threat.
```

- [ ] **Step 4: Run the test and the existing prompt suite**

Run: `pytest tests/test_prompts.py -v`
Expected: all PASS (including the new test and the existing JSON-brace guard)

- [ ] **Step 5: Commit**

```bash
git add prompts.py tests/test_prompts.py
git commit -m "feat(prompts): competitor success defaults to Negative/Neutral sentiment"
```

---

### Task 2: RULE 6 rewrite — directional honesty (prompts.py)

**Files:**
- Modify: `prompts.py` (RULE 6 block inside `_SYSTEM_PROMPT_BASE`, currently lines 217–222)
- Test: `tests/test_prompts.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_prompts.py`, in the section created in Task 1:

```python
def test_rule6_requires_direction_consistency_and_taxonomy_routed_upside():
    """The So-What may not contradict sentiment_tag, and upside claims must
    run through a RULE 4 segment — adjacent markets get an explicit callout."""
    system = _insight_spec().system
    assert "must agree with sentiment_tag" in system
    assert "Adjacent market — no direct Americhem participation indicated." in system


def test_rule6_permits_honest_low_exposure_and_keeps_the_ban():
    """The lazy phrase stays banned, but an honest low-exposure template is
    explicitly legal so the model has an exit besides inventing impact."""
    system = _insight_spec().system
    assert "Limited direct exposure — [specific reason]" in system
    assert '"No direct impact. Monitoring required."' in system
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_prompts.py -k rule6 -v`
Expected: both FAIL with `AssertionError`

- [ ] **Step 3: Replace RULE 6 in `prompts.py`**

The current block reads:

```
RULE 6 — RIGOROUS IMPACT STATEMENT:
Always write a specific So-What for Americhem even for routine items.
Identify which business unit or cost line could be affected and in what direction.
If truly no commercial connection exists, write: "Indirect exposure only — monitor for [specific reason]."
Do NOT write "No direct impact. Monitoring required." — this phrase is banned.
Do NOT write phrases like "may increase demand" or "could affect" without citing specific data.
```

Replace it entirely with:

```
RULE 6 — RIGOROUS, HONEST IMPACT STATEMENT:
Write a specific So-What for Americhem, but NEVER invent impact the article does not support.
- DIRECTION CONSISTENCY: the So-What's direction must agree with sentiment_tag. Never
  describe upside for Americhem under a "Negative" tag, or downside under a "Positive" tag.
- UPSIDE ROUTES THROUGH RULE 4: claim demand or sales upside ONLY when the mechanism runs
  through one of the RULE 4 commercial segments. If the market is adjacent to but outside
  those segments, write: "Adjacent market — no direct Americhem participation indicated."
- HONEST LOW EXPOSURE IS LEGAL: when true impact is limited, write
  "Limited direct exposure — [specific reason]" instead of inventing a commercial effect.
Identify which business unit or cost line could be affected and in what direction.
Do NOT write "No direct impact. Monitoring required." — this exact phrase is banned.
Do NOT write phrases like "may increase demand" or "could affect" without citing specific data.
```

(Note: this deliberately replaces the old "Indirect exposure only — monitor for [specific reason]" template with the spec's "Limited direct exposure" template; no code parses either phrase.)

- [ ] **Step 4: Run the full prompt suite**

Run: `pytest tests/test_prompts.py -v`
Expected: all PASS (brace guard included — the schema block below RULE 7 was not touched)

- [ ] **Step 5: Sanity-check the assembled prompt offline**

Run: `python scripts/show_prompts.py | head -80`
Expected: RULE 2 shows the COMPETITOR DEFAULT paragraph; RULE 6 shows the three-bullet rewrite; note the changed `system_fingerprint` for the QA log.

- [ ] **Step 6: Commit**

```bash
git add prompts.py tests/test_prompts.py
git commit -m "feat(prompts): RULE 6 directional honesty — consistency, taxonomy-routed upside, honest low-exposure exit"
```

---

### Task 3: Appendix widening (market_pulse_config.yaml)

**Files:**
- Modify: `market_pulse_config.yaml` (`reporting:` block, lines 14 and 26)

Config-only; code defaults in `scoring.py`/`report.py` stay untouched. No new test — the parity tests pin taxonomy labels, not these values; the full-suite run in Step 2 proves nothing pins them.

- [ ] **Step 1: Edit the two values**

Change line 14:

```yaml
  supporting_impact_threshold: 4
```

to:

```yaml
  # Lowered 4 → 3 on 2026-08-03 to widen the Additional Articles appendix
  # (recipient feedback: wants more quick-scan headlines). Raise back to 4
  # if the appendix gets noisy.
  supporting_impact_threshold: 3
```

Change line 26:

```yaml
  max_additional_articles: 10
```

to:

```yaml
  # Raised 10 → 20 on 2026-08-03 alongside the threshold change above.
  max_additional_articles: 20
```

- [ ] **Step 2: Run the full suite to confirm nothing pins the old values**

Run: `pytest tests/`
Expected: all PASS

- [ ] **Step 3: Commit**

```bash
git add market_pulse_config.yaml
git commit -m "config: widen Additional Articles appendix (threshold 3, cap 20)"
```

---

### Task 4: Direction glyph on the So-What line (delivery_engine.py)

**Files:**
- Modify: `delivery_engine.py` — `_render_card` (currently lines 131–154) and the module-level color map area (currently line 838)
- Test: `tests/test_pipeline.py` (append to the `_render_card()` sentiment display section, currently around line 1826)

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py` directly after `test_render_card_falls_back_to_sentiment_score_for_old_rows`:

```python
def test_render_card_prefixes_so_what_with_direction_glyph():
    """The So-What label carries a colored direction glyph keyed by sentiment_tag,
    so a mis-spun sentence is visibly contradicted where the reader's eye is."""
    base = {
        "headline": "Plant closure disrupts supply",
        "source_url": "https://news.com/article",
        "americhem_impact": "Feedstock shortfall for masterbatch lines.",
        "americhem_impact_score": 8,
        "commercial_segment": "Raw Materials / Supply Chain",
        "source_publication": "Chemical Week",
        "recommended_action": "Monitor",
        "category": "markets",
    }
    cases = [
        ("Negative", "&#9660;", "#DC2626"),
        ("Neutral", "&#9679;", "#6B7280"),
        ("Positive", "&#9650;", "#16A34A"),
    ]
    for tag, glyph, color in cases:
        html = _render_card({**base, "sentiment_tag": tag})
        assert f'<span style="color:{color};">{glyph}</span> <strong' in html


def test_render_card_without_tag_renders_so_what_label_unchanged():
    """Legacy rows (no sentiment_tag) get no glyph — the label starts the line."""
    item = {
        "headline": "Old Article",
        "source_url": "https://news.com/article",
        "americhem_impact": "Some impact.",
        "sentiment_score": 6,
        "source_publication": "Reuters",
        "recommended_action": "Monitor",
        "category": "markets",
    }
    html = _render_card(item)
    assert "&#9660;" not in html
    assert "&#9650;" not in html
    assert 'line-height:1.55;"><strong' in html
```

(Why the adjacency assertions: `&#9679;` also appears as the meta-strip separator dot, so the tests match the exact `<span …>glyph</span> <strong` composition, not the bare entity.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_pipeline.py -k direction_glyph -v` and `pytest tests/test_pipeline.py::test_render_card_without_tag_renders_so_what_label_unchanged -v`
Expected: the glyph test FAILS (`AssertionError`); the no-tag test may pass its entity asserts but FAIL on the adjacency assert only if rendering already differs — treat any pass as "run again after Step 3".

- [ ] **Step 3: Add the glyph map next to the color map**

In `delivery_engine.py`, directly below `_SENTIMENT_TAG_COLORS` (line 838):

```python
_SENTIMENT_TAG_GLYPHS: dict[str, str] = {
    "Negative": "&#9660;",  # ▼
    "Neutral":  "&#9679;",  # ●
    "Positive": "&#9650;",  # ▲
}
```

- [ ] **Step 4: Prepend the glyph in `_render_card`**

The current So-What block in `_render_card`:

```python
    americhem_impact = item.get("americhem_impact", "")
    so_what_html = (
        f'<p style="margin:4px 0 0 0;font-size:13px;color:#374151;'
        f"font-family:Georgia,'Times New Roman',serif;line-height:1.55;\">"
        f'<strong style="color:{_BRAND_NAVY};">So what:</strong> {americhem_impact}</p>'
        if americhem_impact else ""
    )
```

Replace with:

```python
    americhem_impact = item.get("americhem_impact", "")
    tag = item.get("sentiment_tag") or ""
    glyph = _SENTIMENT_TAG_GLYPHS.get(tag)
    glyph_html = (
        f'<span style="color:{_SENTIMENT_TAG_COLORS.get(tag, "#6B7280")};">{glyph}</span> '
        if glyph else ""
    )
    so_what_html = (
        f'<p style="margin:4px 0 0 0;font-size:13px;color:#374151;'
        f"font-family:Georgia,'Times New Roman',serif;line-height:1.55;\">"
        f'{glyph_html}<strong style="color:{_BRAND_NAVY};">So what:</strong> {americhem_impact}</p>'
        if americhem_impact else ""
    )
```

- [ ] **Step 5: Run the new tests, then the whole pipeline suite**

Run: `pytest tests/test_pipeline.py -k "render_card" -v`
Expected: all PASS (new tests plus the four pre-existing `_render_card` tests)

Run: `pytest tests/`
Expected: all PASS

- [ ] **Step 6: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat(delivery): colored sentiment-direction glyph on the So-What line"
```

---

### Task 5: Full verification + manual QA handoff

**Files:** none (verification only)

- [ ] **Step 1: Full suite**

Run: `pytest tests/`
Expected: all PASS

- [ ] **Step 2: Assembled-prompt check for the QA record**

Run: `python scripts/show_prompts.py | sed -n '/RULE 2/,/RULE 7/p'`
Expected: the COMPETITOR DEFAULT paragraph appears in RULE 2 and the three-bullet rewrite in RULE 6; the source diff itself is already visible via `git log -p -- prompts.py` (Tasks 1–2 committed it in two focused commits).

- [ ] **Step 3: Manual QA (human step — not automatable here)**

Dispatch `.github/workflows/market_pulse_test.yml` with `run_ingestion=true` and `send_email=true` (a re-render-only run would not exercise the new prompt). Review the Jason-only QA email for: no upside claims on Negative-tagged rows, competitor items framed as threats, glyphs rendering in the mail client, appendix showing up to 20 rows. This bills the APIs for one run — it is the point of the QA pool.
