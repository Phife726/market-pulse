# Design: Thematic Synthesis Email Redesign

**Date:** 2026-04-14  
**Status:** Pending approval  
**Scope:** `delivery_engine.py` only — no schema changes, no ingestion changes

---

## Problem Statement

The current email delivers a linear list of individual cards grouped only by alert tier (CRITICAL / STRATEGIC / ROUTINE). At full capacity (150 items), this creates alert fatigue: readers skim or ignore the digest because no cognitive work has been done to surface the trend across signals. Raw news is a commodity; the proprietary value is the contextual mapping to Americhem's specific systems.

Two structural flaws in the current format:
1. **No thematic aggregation.** Ten articles about competitor pricing pressure appear as ten separate cards. The reader must infer the pattern.
2. **Low information density.** A 4/10 article about a routine CEO transition gets the same visual weight as an 8/10 strategic opportunity.

---

## Proposed Solution

Rebuild the email body around three zones:

1. **Critical Disruptions** — full cards for scores 1–3 (unchanged behavior)
2. **Thematic Intelligence** — LLM-synthesized paragraphs grouped by structural category, with supporting bullets
3. **Peripheral Signals** — compact bullet list for scores 4–6 that don't reach synthesis threshold

---

## Routing Rules

Every fetched article is classified into exactly one zone using these ordered rules:

| Priority | Condition | Zone |
|---|---|---|
| 1 | Score 1–3 | Critical Disruptions (full card) |
| 2 | Score 7–10, category has 2+ qualifying articles | Thematic Intelligence (synthesis paragraph + bullets) |
| 3 | Score 7–10, category has 1 qualifying article | Thematic Intelligence (thin: header + single bullet, no paragraph) |
| 4 | Score 4–6, category has 2+ qualifying articles | Thematic Intelligence (synthesis paragraph + bullets, lower visual weight) |
| 5 | Score 4–6, category has 1 qualifying article | Peripheral Signals (compact bullet) |

**"Qualifying articles" for category grouping:** scores 4–10 only. Score 1–3 articles are always pulled to Critical before grouping runs.

**Missing `category` field:** If an article's `category` field is `None`, empty string, or missing, it is treated as `"Uncategorized"` for grouping purposes. This prevents a KeyError in grouping and allows uncategorized articles to still reach the Peripheral Signals section.

---

## New Email Structure

```
┌──────────────────────────────────────────────────┐
│  Header (unchanged)                              │
│  Date bar (unchanged)                            │
│  Executive Summary block (unchanged)             │
├──────────────────────────────────────────────────┤
│  CRITICAL DISRUPTIONS                            │
│  └─ Full card per item (existing _render_card)   │
├──────────────────────────────────────────────────┤
│  THEMATIC INTELLIGENCE                           │
│  └─ Per category group (ordered by min score):  │
│      [CATEGORY NAME]                             │
│      LLM synthesis paragraph (if 2+ articles)   │
│      · [Entity: score/10] one-line summary       │
│      · [Entity: score/10] one-line summary       │
├──────────────────────────────────────────────────┤
│  PERIPHERAL SIGNALS                              │
│  Monitoring only — lower probability of impact  │
│  · [Entity: score/10] Headline text              │
│  · [Entity: score/10] Headline text              │
└──────────────────────────────────────────────────┘
```

Category ordering within Thematic Intelligence: ascending by minimum score in the group (lowest/most urgent group appears first).

---

## LLM Synthesis Call

### When it fires
Once per delivery run, after fetching articles and applying routing rules. Only fires if at least one category group has 2+ articles. Single-article groups render directly from existing `americhem_impact` text — no LLM call needed.

### Input
All category groups with 2+ articles, formatted as:

```
CATEGORY: Raw Material Supply Chain
- [ExxonMobil Chemical | 3/10] Force majeure declared on LLDPE through May. Americhem's PE-based lines face spot availability risk.
- [Formosa Plastics | 4/10] PE production curtailment. Secondary supplier exposure confirmed.
- [BASF | 8/10] Ohio capacity expansion on schedule for Q3. Partial offset to near-term tightness.

CATEGORY: Competitor Pricing Pressure
- [Avient | 8/10] Q2 surcharge announced on PA6/PA66 compounds. Americhem pricing window opens.
- [Techmer PM | 7/10] Nylon compound price increase. Customers may seek alternatives.
```

### Prompt
```
You are a market intelligence analyst for Americhem, a specialty plastics compounder.

For each CATEGORY block below, write exactly one synthesis paragraph (2–3 sentences).
The paragraph must:
- Identify the shared trend or structural driver across the listed signals
- Explicitly state the implication for Americhem's supply chain, demand pipeline, or margin
- Be written for a senior executive who will act on it — no hedging, no filler

Return valid JSON with category names as keys and synthesis paragraphs as values.
Use the exact category names provided. Do not invent categories.
Only include categories that appear in the input.

{grouped_articles_text}
```

### OpenAI call pattern
Uses `response_format={"type": "json_object"}` — matching the pattern in `ingestion_engine.py:synthesize_insight()`. The model is instructed to return JSON in the prompt; the response_format enforces it. Parse with `json.loads(completion.choices[0].message.content)`.

### Output
```json
{
  "Raw Material Supply Chain": "ExxonMobil and Formosa's concurrent PE curtailments signal a tightening spot market through at least May, putting Americhem's PE-intensive compound lines at sourcing risk. BASF's Q3 Ohio expansion provides a medium-term offset but offers no near-term relief. Procurement should qualify backup resin positions this week.",
  "Competitor Pricing Pressure": "Avient and Techmer's Q2 surcharges create a pricing window for Americhem to hold or modestly increase compound prices without risk of appearing uncompetitive. Customers facing surcharges from both competitors are likely to be receptive to value-based pricing conversations."
}
```

### Model
Same `OPENAI_MODEL` constant already used by ingestion (`gpt-5.4-nano`). No new env vars required.

### Cost
~14K input tokens + ~1.5K output tokens per run ≈ **$0.002/day** at nano pricing.

---

## Code Changes — `delivery_engine.py`

### New functions

**`_group_for_thematic(items: list[dict]) -> dict[str, list[dict]]`**  
Takes the non-critical articles (scores 4–10). Returns a dict of `{category: [articles]}` for groups with 2+ articles. Single-article groups are excluded (handled separately as thin entries or peripheral bullets).

**`_collect_thin_entries(items: list[dict], groups: dict[str, list[dict]]) -> list[dict]`**  
Returns single-article items scoring 7–10 that were not captured in a 2+ group.

**`_collect_peripheral(items: list[dict], groups: dict[str, list[dict]]) -> list[dict]`**  
Returns single-article items scoring 4–6 that were not captured in a 2+ group.

**`synthesize_thematic_paragraphs(groups: dict[str, list[dict]]) -> dict[str, str]`**  
Calls OpenAI once. Input: the 2+ article groups. Output: `{category: synthesis_paragraph}`. Returns `{}` on any error (graceful degradation — rendering falls back to bullets-only).

**`_render_thematic_section(groups, thin_entries, synthesis) -> str`**  
Renders the full Thematic Intelligence section. Iterates groups ordered by min score ascending (most urgent first). For each group: category header → synthesis paragraph (if present in `synthesis` dict) → supporting bullets ordered by score ascending. Thin entries appended after all groups, ordered by score ascending.

**`_render_peripheral_section(items: list[dict]) -> str`**  
Renders the Peripheral Signals section as a flat bullet list. Each bullet: `[Entity: score/10] Headline text`.

### Modified functions

**`generate_html_email()`**  
Replace the current three-section `_render_section()` calls with the new routing + rendering pipeline:
1. Partition articles into critical / thematic-eligible / peripheral
2. Build category groups
3. Call `synthesize_thematic_paragraphs()` (if groups exist)
4. Render: critical cards → thematic section → peripheral bullets

### Unchanged functions
- `_render_card()` — used only for Critical Disruptions
- `_render_section()` — used only for the Critical Disruptions wrapper
- `_render_exec_summary()` — unchanged
- All fetch, send, retry logic — unchanged

---

## Graceful Degradation

If `synthesize_thematic_paragraphs()` raises or returns `{}` (OpenAI timeout, missing API key in test, etc.), the Thematic Intelligence section renders with category headers and bullets only — no synthesis paragraphs. The email still sends. This means the OpenAI call failure does not block delivery.

---

## Test Coverage

New unit tests in `tests/test_pipeline.py`:

1. **`test_routing_critical_always_card`** — score 1–3 article never enters thematic grouping
2. **`test_routing_single_article_high_score`** — score 7–10 single-article goes to thin entries, not peripheral
3. **`test_routing_single_article_low_score`** — score 4–6 single-article goes to peripheral
4. **`test_routing_two_plus_any_score`** — 2+ articles in same category → synthesis group regardless of score
5. **`test_synthesize_graceful_degradation`** — synthesis call returns `{}` on OpenAI error; rendering still produces valid HTML
6. **`test_peripheral_render`** — peripheral section renders correct bullet count

7. **`test_routing_all_critical`** — all articles score 1–3; verify email renders with only Critical Disruptions section, Thematic Intelligence and Peripheral Signals sections are absent from HTML output.
8. **`test_synthesize_bullets_only_fallback`** — synthesis returns `{}`; verify `_render_thematic_section` produces valid HTML with category headers and bullets but no synthesis paragraph text.

All OpenAI calls mocked. No live API calls in test suite (consistent with existing convention).

---

## Out of Scope

- Schema changes — none required
- `ingestion_engine.py` changes — none required
- New environment variables — none required
- Mobile/responsive email changes — out of scope
- AlphaSense-style CAGR/regulatory macro data ingestion — separate initiative
