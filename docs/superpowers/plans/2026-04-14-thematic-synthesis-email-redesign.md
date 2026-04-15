# Plan: Thematic Synthesis Email Redesign

**Goal:** Replace the linear card layout in the daily digest with a three-zone email structure: Critical Disruptions (full cards), Thematic Intelligence (LLM-synthesized paragraphs by category), and Peripheral Signals (compact bullet list).

**Design spec:** `docs/superpowers/specs/2026-04-14-thematic-synthesis-email-redesign.md`

**Architecture overview:**
All changes are confined to `delivery_engine.py`. No schema changes. No ingestion changes. New functions are added, `generate_html_email()` is modified to use them. All new code follows TDD: test → fail → implement → pass → commit.

**Tech stack:** Python 3.11, OpenAI Python SDK, Supabase, pytest

---

## File Map

| File | Change type | Responsibility |
|---|---|---|
| `delivery_engine.py` | Modify | Add OpenAI client, 6 new functions, modify `generate_html_email()` |
| `tests/test_pipeline.py` | Modify | Add 8 new test cases for routing, synthesis, and rendering |

---

## Tasks

---

### Task 1 — Add OpenAI client infrastructure to `delivery_engine.py`

**Why:** `synthesize_thematic_paragraphs()` (Task 4) calls OpenAI. The client pattern is identical to `ingestion_engine.py` — add the same imports, constant, and helper to delivery_engine.

**Files:** `delivery_engine.py`

**Step 1 — Write failing test**

Add to `tests/test_pipeline.py` (after the existing imports block):

```python
from delivery_engine import _get_openai as _delivery_get_openai, OPENAI_MODEL as _DELIVERY_MODEL
```

Run:

```bash
pytest tests/test_pipeline.py -k "not test_" --collect-only 2>&1 | head -5
```

Expected: ImportError — `_get_openai` and `OPENAI_MODEL` don't exist in delivery_engine yet.

**Step 2 — Implement**

At the top of `delivery_engine.py`, add after the existing imports:

```python
import json

from openai import OpenAI
```

After the `_TRANSIENT_HTTP_CODES` constant, add:

```python
# ---------------------------------------------------------------------------
# OpenAI client
# ---------------------------------------------------------------------------

OPENAI_MODEL = "gpt-5.4-nano"


def _get_openai() -> OpenAI:
    """Return an authenticated OpenAI client using env credentials."""
    return OpenAI(api_key=os.environ["OPENAI_API_KEY"])
```

**Step 3 — Verify**

```bash
pytest tests/test_pipeline.py -k "not test_" --collect-only 2>&1 | head -5
```

Expected: clean collection, no ImportError.

**Step 4 — Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat: add OpenAI client infrastructure to delivery_engine"
```

---

### Task 2 — Implement `_group_for_thematic()`

**Why:** This is the core routing function. It takes all non-critical articles (scores 4–10) and returns dict of `{category: [articles]}` for groups with 2+ articles only. Underpins all downstream routing.

**Files:** `delivery_engine.py`, `tests/test_pipeline.py`

**Step 1 — Write failing tests**

Add to `tests/test_pipeline.py`:

```python
# ---------------------------------------------------------------------------
# Thematic routing: _group_for_thematic
# ---------------------------------------------------------------------------

from delivery_engine import _group_for_thematic


def _make_article(
    url_hash: str,
    score: int,
    category: str | None,
    headline: str = "Test Headline",
) -> dict:
    return {
        "url_hash": url_hash,
        "sentiment_score": score,
        "category": category,
        "headline": headline,
        "americhem_impact": "Some impact.",
        "entities_mentioned": ["TestCorp"],
        "source_url": "https://news.com/article",
    }


def test_group_for_thematic_requires_two_plus():
    """Categories with only one article must NOT appear in groups."""
    items = [_make_article("a", 7, "competitors")]
    groups = _group_for_thematic(items)
    assert groups == {}


def test_group_for_thematic_two_same_category():
    """Two articles in the same category produce one group."""
    items = [
        _make_article("a", 7, "competitors"),
        _make_article("b", 8, "competitors"),
    ]
    groups = _group_for_thematic(items)
    assert "competitors" in groups
    assert len(groups["competitors"]) == 2


def test_group_for_thematic_excludes_critical():
    """Score 1–3 articles must never appear in groups even if passed in."""
    items = [
        _make_article("a", 2, "suppliers"),
        _make_article("b", 2, "suppliers"),
    ]
    groups = _group_for_thematic(items)
    assert groups == {}


def test_group_for_thematic_none_category_becomes_uncategorized():
    """Articles with None or empty category must group under 'Uncategorized'."""
    items = [
        _make_article("a", 6, None),
        _make_article("b", 5, ""),
    ]
    groups = _group_for_thematic(items)
    assert "Uncategorized" in groups
    assert len(groups["Uncategorized"]) == 2
```

Run:

```bash
pytest tests/test_pipeline.py -k "test_group_for_thematic" -v
```

Expected: `ImportError` or `4 failed` — function doesn't exist yet.

**Step 2 — Implement**

Add after `_render_section()` in `delivery_engine.py`:

```python
# ---------------------------------------------------------------------------
# Thematic routing helpers
# ---------------------------------------------------------------------------

def _group_for_thematic(items: list[dict]) -> dict[str, list[dict]]:
    """Group qualifying articles (score 4–10) by category for thematic synthesis.

    Args:
        items: Articles pre-filtered to scores 4–10. Score 1–3 articles are
            silently skipped as a safety guard.

    Returns:
        Dict of {category: [articles]} containing only groups with 2+ articles.
        Articles with a missing or null category are grouped under 'Uncategorized'.
    """
    from collections import defaultdict
    buckets: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        score = item.get("sentiment_score") or 0
        if score <= 3:
            continue
        category = item.get("category") or "Uncategorized"
        buckets[category].append(item)
    return {cat: arts for cat, arts in buckets.items() if len(arts) >= 2}
```

**Step 3 — Run tests**

```bash
pytest tests/test_pipeline.py -k "test_group_for_thematic" -v
```

Expected: `4 passed`

**Step 4 — Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat: add _group_for_thematic routing function"
```

---

### Task 3 — Implement `_collect_thin_entries()` and `_collect_peripheral()`

**Why:** After grouping, ungrouped articles must be routed to either the thin Thematic entry list (score 7–10) or Peripheral Signals (score 4–6).

**Files:** `delivery_engine.py`, `tests/test_pipeline.py`

**Step 1 — Write failing tests**

```python
# ---------------------------------------------------------------------------
# Thematic routing: _collect_thin_entries, _collect_peripheral
# ---------------------------------------------------------------------------

from delivery_engine import _collect_thin_entries, _collect_peripheral


def test_collect_thin_entries_single_high_score():
    """Single-article score 7–10 not in any group goes to thin entries."""
    items = [_make_article("solo", 8, "customers")]
    groups = {}  # no groups
    thin = _collect_thin_entries(items, groups)
    assert len(thin) == 1
    assert thin[0]["url_hash"] == "solo"


def test_collect_thin_entries_excludes_grouped():
    """Articles already in a synthesis group must not appear in thin entries."""
    art_a = _make_article("a", 8, "customers")
    art_b = _make_article("b", 9, "customers")
    groups = {"customers": [art_a, art_b]}
    thin = _collect_thin_entries([art_a, art_b], groups)
    assert thin == []


def test_collect_thin_entries_excludes_low_score():
    """Score 4–6 articles must not appear in thin entries even if ungrouped."""
    items = [_make_article("low", 5, "markets")]
    thin = _collect_thin_entries(items, {})
    assert thin == []


def test_collect_peripheral_single_low_score():
    """Single-article score 4–6 not in any group goes to peripheral."""
    items = [_make_article("p", 5, "markets")]
    peripheral = _collect_peripheral(items, {})
    assert len(peripheral) == 1
    assert peripheral[0]["url_hash"] == "p"


def test_collect_peripheral_excludes_grouped():
    """Articles in a synthesis group must not appear in peripheral."""
    art_a = _make_article("a", 5, "markets")
    art_b = _make_article("b", 6, "markets")
    groups = {"markets": [art_a, art_b]}
    peripheral = _collect_peripheral([art_a, art_b], groups)
    assert peripheral == []


def test_collect_peripheral_excludes_high_score():
    """Score 7–10 articles must not appear in peripheral even if ungrouped."""
    items = [_make_article("high", 8, "markets")]
    peripheral = _collect_peripheral(items, {})
    assert peripheral == []
```

Run:

```bash
pytest tests/test_pipeline.py -k "test_collect_thin or test_collect_peripheral" -v
```

Expected: `6 failed` — functions not implemented yet.

**Step 2 — Implement**

Add after `_group_for_thematic()` in `delivery_engine.py`:

```python
def _collect_thin_entries(
    items: list[dict],
    groups: dict[str, list[dict]],
) -> list[dict]:
    """Return ungrouped score 7–10 articles for thin thematic rendering.

    Args:
        items: All non-critical articles (scores 4–10).
        groups: The 2+ article groups from _group_for_thematic().

    Returns:
        Single-article items scoring 7–10 not captured in any group,
        ordered by sentiment_score ascending.
    """
    grouped_hashes = {
        art.get("url_hash") for arts in groups.values() for art in arts
    }
    thin = [
        item for item in items
        if item.get("url_hash") not in grouped_hashes
        and (item.get("sentiment_score") or 0) >= 7
    ]
    return sorted(thin, key=lambda x: x.get("sentiment_score") or 0)


def _collect_peripheral(
    items: list[dict],
    groups: dict[str, list[dict]],
) -> list[dict]:
    """Return ungrouped score 4–6 articles for the Peripheral Signals section.

    Args:
        items: All non-critical articles (scores 4–10).
        groups: The 2+ article groups from _group_for_thematic().

    Returns:
        Single-article items scoring 4–6 not captured in any group,
        ordered by sentiment_score ascending.
    """
    grouped_hashes = {
        art.get("url_hash") for arts in groups.values() for art in arts
    }
    peripheral = [
        item for item in items
        if item.get("url_hash") not in grouped_hashes
        and (item.get("sentiment_score") or 0) <= 6
    ]
    return sorted(peripheral, key=lambda x: x.get("sentiment_score") or 0)
```

**Step 3 — Run tests**

```bash
pytest tests/test_pipeline.py -k "test_collect_thin or test_collect_peripheral" -v
```

Expected: `6 passed`

**Step 4 — Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat: add _collect_thin_entries and _collect_peripheral routing helpers"
```

---

### Task 4 — Implement `synthesize_thematic_paragraphs()`

**Why:** The one OpenAI call per delivery run. Takes 2+ article groups, returns `{category: synthesis_paragraph}`. Must degrade gracefully on any failure.

**Files:** `delivery_engine.py`, `tests/test_pipeline.py`

**Step 1 — Write failing tests**

```python
# ---------------------------------------------------------------------------
# synthesize_thematic_paragraphs
# ---------------------------------------------------------------------------

from unittest.mock import MagicMock, patch
from delivery_engine import synthesize_thematic_paragraphs


def _make_synthesis_mock(paragraphs: dict) -> MagicMock:
    mock_message = MagicMock()
    mock_message.content = json.dumps(paragraphs)
    mock_choice = MagicMock()
    mock_choice.message = mock_message
    mock_completion = MagicMock()
    mock_completion.choices = [mock_choice]
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_completion
    return mock_client


def test_synthesize_thematic_paragraphs_returns_paragraphs():
    """Returns dict of {category: paragraph} on success."""
    groups = {
        "competitors": [
            _make_article("a", 8, "competitors"),
            _make_article("b", 7, "competitors"),
        ]
    }
    expected = {"competitors": "Avient and Techmer raised prices."}
    mock_client = _make_synthesis_mock(expected)

    with patch("delivery_engine._get_openai", return_value=mock_client):
        result = synthesize_thematic_paragraphs(groups)

    assert result == expected


def test_synthesize_thematic_paragraphs_uses_json_response_format():
    """Must call OpenAI with response_format={'type': 'json_object'}."""
    groups = {
        "suppliers": [
            _make_article("a", 4, "suppliers"),
            _make_article("b", 5, "suppliers"),
        ]
    }
    mock_client = _make_synthesis_mock({"suppliers": "Supply chain tightening."})

    with patch("delivery_engine._get_openai", return_value=mock_client):
        synthesize_thematic_paragraphs(groups)

    _, kwargs = mock_client.chat.completions.create.call_args
    assert kwargs.get("response_format") == {"type": "json_object"}


def test_synthesize_thematic_paragraphs_uses_openai_model():
    """Must use OPENAI_MODEL constant, not a hardcoded string."""
    from delivery_engine import OPENAI_MODEL
    groups = {
        "markets": [
            _make_article("a", 6, "markets"),
            _make_article("b", 6, "markets"),
        ]
    }
    mock_client = _make_synthesis_mock({"markets": "Markets paragraph."})

    with patch("delivery_engine._get_openai", return_value=mock_client):
        synthesize_thematic_paragraphs(groups)

    _, kwargs = mock_client.chat.completions.create.call_args
    assert kwargs.get("model") == OPENAI_MODEL


def test_synthesize_thematic_paragraphs_empty_groups():
    """Returns {} immediately without calling OpenAI when groups is empty."""
    mock_client = MagicMock()

    with patch("delivery_engine._get_openai", return_value=mock_client):
        result = synthesize_thematic_paragraphs({})

    mock_client.chat.completions.create.assert_not_called()
    assert result == {}


def test_synthesize_thematic_paragraphs_graceful_degradation():
    """Returns {} and logs error when OpenAI raises — does not re-raise."""
    groups = {
        "competitors": [
            _make_article("a", 7, "competitors"),
            _make_article("b", 8, "competitors"),
        ]
    }
    mock_client = MagicMock()
    mock_client.chat.completions.create.side_effect = Exception("API timeout")

    with patch("delivery_engine._get_openai", return_value=mock_client):
        result = synthesize_thematic_paragraphs(groups)

    assert result == {}
```

Run:

```bash
pytest tests/test_pipeline.py -k "test_synthesize_thematic" -v
```

Expected: `5 failed` — function not implemented yet.

**Step 2 — Implement**

Add after `_collect_peripheral()` in `delivery_engine.py`:

```python
def synthesize_thematic_paragraphs(
    groups: dict[str, list[dict]],
) -> dict[str, str]:
    """Generate one synthesis paragraph per category group via OpenAI.

    Args:
        groups: Dict of {category: [articles]} — only groups with 2+ articles.

    Returns:
        Dict of {category: synthesis_paragraph}. Returns {} on any error so the
        caller can fall back to bullets-only rendering without blocking delivery.
    """
    if not groups:
        return {}

    lines: list[str] = []
    for category, articles in groups.items():
        lines.append(f"CATEGORY: {category}")
        for art in articles:
            score = art.get("sentiment_score", 5)
            entities = art.get("entities_mentioned") or []
            entity = entities[0] if entities else art.get("category", "Unknown")
            impact = art.get("americhem_impact", "")
            lines.append(f"- [{entity} | {score}/10] {impact}")
        lines.append("")

    grouped_text = "\n".join(lines).strip()

    system_prompt = (
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

    try:
        client = _get_openai()
        completion = client.chat.completions.create(
            model=OPENAI_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": grouped_text},
            ],
        )
        result: dict[str, str] = json.loads(completion.choices[0].message.content)
        logger.info("Thematic synthesis complete — %d categories.", len(result))
        return result
    except Exception as exc:
        logger.error(
            "Thematic synthesis failed — falling back to bullets-only: %s", exc
        )
        return {}
```

**Step 3 — Run tests**

```bash
pytest tests/test_pipeline.py -k "test_synthesize_thematic" -v
```

Expected: `5 passed`

**Step 4 — Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat: add synthesize_thematic_paragraphs with graceful degradation"
```

---

### Task 5 — Implement `_render_peripheral_section()`

**Why:** Renders the compact bullet list for score 4–6 ungrouped articles. Replaces the full `_render_section()` + `_render_card()` path for these items.

**Files:** `delivery_engine.py`, `tests/test_pipeline.py`

**Step 1 — Write failing tests**

```python
# ---------------------------------------------------------------------------
# _render_peripheral_section
# ---------------------------------------------------------------------------

from delivery_engine import _render_peripheral_section


def test_render_peripheral_section_empty_returns_empty_string():
    """Empty items list must return empty string (no section rendered)."""
    assert _render_peripheral_section([]) == ""


def test_render_peripheral_section_correct_bullet_count():
    """Each item produces exactly one bullet in the HTML output."""
    items = [
        _make_article("a", 5, "markets", "Headline A"),
        _make_article("b", 4, "economic", "Headline B"),
        _make_article("c", 6, "customers", "Headline C"),
    ]
    html = _render_peripheral_section(items)
    assert html.count("Headline A") == 1
    assert html.count("Headline B") == 1
    assert html.count("Headline C") == 1


def test_render_peripheral_section_includes_score():
    """Each bullet must display the sentiment score."""
    items = [_make_article("a", 5, "markets", "Some Headline")]
    html = _render_peripheral_section(items)
    assert "5/10" in html


def test_render_peripheral_section_headline_is_linked():
    """Each headline must be an anchor tag pointing to source_url."""
    items = [_make_article("a", 5, "markets", "Linked Headline")]
    html = _render_peripheral_section(items)
    assert "https://news.com/article" in html
    assert "Linked Headline" in html
```

Run:

```bash
pytest tests/test_pipeline.py -k "test_render_peripheral" -v
```

Expected: `4 failed`

**Step 2 — Implement**

Add after `synthesize_thematic_paragraphs()` in `delivery_engine.py`:

```python
def _render_peripheral_section(items: list[dict]) -> str:
    """Render the Peripheral Signals compact bullet list.

    Args:
        items: Score 4–6 articles not captured in any synthesis group.

    Returns:
        HTML string for the Peripheral Signals section, or empty string if
        items is empty.
    """
    if not items:
        return ""

    bullets_html = ""
    for item in items:
        entities = item.get("entities_mentioned") or []
        entity = entities[0] if entities else (item.get("category") or "Unknown")
        score = item.get("sentiment_score", "")
        headline = item.get("headline", "")
        source_url = item.get("source_url", "#")
        bullets_html += f"""
              <tr>
                <td style="padding:2px 0;">
                  <span style="font-size:12px;font-family:Arial,sans-serif;color:#6B7280;">
                    &bull;&nbsp;<strong style="color:#374151;">[{entity}: {score}/10]</strong>
                    &nbsp;<a href="{source_url}"
                             style="color:#374151;text-decoration:none;">{headline}</a>
                  </span>
                </td>
              </tr>"""

    return f"""
      <tr>
        <td style="padding:24px 32px 4px 32px;">
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="padding-bottom:8px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="font-size:11px;font-weight:700;letter-spacing:1.5px;
                                text-transform:uppercase;color:#9CA3AF;
                                font-family:Arial,sans-serif;white-space:nowrap;
                                padding-right:12px;">
                      PERIPHERAL SIGNALS
                    </td>
                    <td style="border-bottom:1px solid #E5E7EB;width:100%;"></td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td>
                <p style="margin:0 0 8px 0;font-size:11px;color:#9CA3AF;
                           font-family:Arial,sans-serif;font-style:italic;">
                  Monitoring only &mdash; lower probability of direct impact
                </p>
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  {bullets_html}
                </table>
              </td>
            </tr>
          </table>
        </td>
      </tr>"""
```

**Step 3 — Run tests**

```bash
pytest tests/test_pipeline.py -k "test_render_peripheral" -v
```

Expected: `4 passed`

**Step 4 — Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat: add _render_peripheral_section HTML renderer"
```

---

### Task 6 — Implement `_render_thematic_section()`

**Why:** The main body renderer. Iterates category groups ordered by min score, renders synthesis paragraph (if present) + supporting bullets. Appends thin entries (single-article, score 7–10) after all groups.

**Files:** `delivery_engine.py`, `tests/test_pipeline.py`

**Step 1 — Write failing tests**

```python
# ---------------------------------------------------------------------------
# _render_thematic_section
# ---------------------------------------------------------------------------

from delivery_engine import _render_thematic_section


def test_render_thematic_section_empty_returns_empty_string():
    """Empty groups and thin_entries must return empty string."""
    assert _render_thematic_section({}, [], {}) == ""


def test_render_thematic_section_synthesis_paragraph_appears():
    """Synthesis paragraph must appear in HTML when provided for a 2+ group."""
    groups = {
        "competitors": [
            _make_article("a", 8, "competitors", "Avient Raises Prices"),
            _make_article("b", 7, "competitors", "Techmer Price Hike"),
        ]
    }
    synthesis = {"competitors": "Both competitors raised prices this quarter."}
    html = _render_thematic_section(groups, [], synthesis)
    assert "Both competitors raised prices this quarter." in html


def test_render_thematic_section_bullets_only_when_no_synthesis():
    """Category group renders with bullets only when synthesis dict is empty."""
    groups = {
        "competitors": [
            _make_article("a", 8, "competitors", "Avient Headline"),
            _make_article("b", 7, "competitors", "Techmer Headline"),
        ]
    }
    html = _render_thematic_section(groups, [], {})
    assert "Avient Headline" in html
    assert "Techmer Headline" in html


def test_render_thematic_section_thin_entry_appears():
    """Thin entries (single-article 7–10) appear without a synthesis paragraph."""
    thin = [_make_article("solo", 9, "customers", "Solo High Score Headline")]
    html = _render_thematic_section({}, thin, {})
    assert "Solo High Score Headline" in html


def test_render_thematic_section_category_header_uppercase():
    """Category name must appear as a section header in the HTML."""
    groups = {
        "Raw Material Supply Chain": [
            _make_article("a", 4, "Raw Material Supply Chain"),
            _make_article("b", 5, "Raw Material Supply Chain"),
        ]
    }
    html = _render_thematic_section(groups, [], {})
    assert "RAW MATERIAL SUPPLY CHAIN" in html.upper()
```

Run:

```bash
pytest tests/test_pipeline.py -k "test_render_thematic_section" -v
```

Expected: `5 failed`

**Step 2 — Implement**

Add after `_render_peripheral_section()` in `delivery_engine.py`:

```python
def _render_thematic_section(
    groups: dict[str, list[dict]],
    thin_entries: list[dict],
    synthesis: dict[str, str],
) -> str:
    """Render the Thematic Intelligence section.

    Args:
        groups: 2+ article groups from _group_for_thematic(), keyed by category.
        thin_entries: Single-article items scoring 7–10 from _collect_thin_entries().
        synthesis: LLM paragraphs from synthesize_thematic_paragraphs(). May be
            empty dict — sections render bullets-only in that case.

    Returns:
        HTML string for the Thematic Intelligence section, or empty string if
        both groups and thin_entries are empty.
    """
    if not groups and not thin_entries:
        return ""

    ordered_groups = sorted(
        groups.items(),
        key=lambda kv: min(
            (a.get("sentiment_score") or 5) for a in kv[1]
        ),
    )

    def _bullet(item: dict) -> str:
        entities = item.get("entities_mentioned") or []
        entity = entities[0] if entities else (item.get("category") or "Unknown")
        score = item.get("sentiment_score", "")
        headline = item.get("headline", "")
        source_url = item.get("source_url", "#")
        return (
            f'<tr><td style="padding:2px 0;">'
            f'<span style="font-size:12px;font-family:Arial,sans-serif;">'
            f'&bull;&nbsp;'
            f'<a href="{source_url}" style="color:{_BRAND_NAVY};text-decoration:none;'
            f'font-weight:600;">[{entity}: {score}/10]</a>'
            f'&nbsp;<span style="color:#374151;">{headline}</span>'
            f'</span></td></tr>'
        )

    def _category_block(category: str, articles: list[dict], para: str) -> str:
        para_html = (
            f'<p style="margin:0 0 10px 0;font-size:13px;color:#1a2a45;'
            f'font-family:Georgia,\'Times New Roman\',serif;line-height:1.65;">'
            f'{para}</p>'
        ) if para else ""
        sorted_articles = sorted(
            articles, key=lambda x: x.get("sentiment_score") or 0
        )
        bullets = "".join(_bullet(a) for a in sorted_articles)
        return (
            f'<tr><td style="padding:0 0 18px 0;">'
            f'<p style="margin:0 0 6px 0;font-size:11px;font-weight:700;'
            f'letter-spacing:1px;text-transform:uppercase;color:{_BRAND_NAVY};'
            f'font-family:Arial,sans-serif;">{category}</p>'
            f'{para_html}'
            f'<table width="100%" cellpadding="0" cellspacing="0" border="0">'
            f'{bullets}</table>'
            f'</td></tr>'
        )

    categories_html = "".join(
        _category_block(cat, arts, synthesis.get(cat, ""))
        for cat, arts in ordered_groups
    )

    thin_sorted = sorted(
        thin_entries, key=lambda x: x.get("sentiment_score") or 0
    )
    for item in thin_sorted:
        category = item.get("category") or "Uncategorized"
        categories_html += _category_block(category, [item], "")

    return f"""
      <tr>
        <td style="padding:24px 32px 4px 32px;">
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="padding-bottom:10px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="font-size:11px;font-weight:700;letter-spacing:1.5px;
                                text-transform:uppercase;color:{_BRAND_NAVY};
                                font-family:Arial,sans-serif;white-space:nowrap;
                                padding-right:12px;">
                      THEMATIC INTELLIGENCE
                    </td>
                    <td style="border-bottom:1px solid {_BRAND_NAVY};width:100%;"></td>
                  </tr>
                </table>
              </td>
            </tr>
            {categories_html}
          </table>
        </td>
      </tr>"""
```

**Step 3 — Run tests**

```bash
pytest tests/test_pipeline.py -k "test_render_thematic_section" -v
```

Expected: `5 passed`

**Step 4 — Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat: add _render_thematic_section HTML renderer"
```

---

### Task 7 — Modify `generate_html_email()` and add integration tests

**Why:** Wire all new functions into the email pipeline. Replace the three `_render_section()` calls for STRATEGIC and ROUTINE tiers with the new routing + synthesis + rendering pipeline. CRITICAL cards remain unchanged.

**Files:** `delivery_engine.py`, `tests/test_pipeline.py`

**Step 1 — Write failing tests**

```python
# ---------------------------------------------------------------------------
# generate_html_email — routing integration
# ---------------------------------------------------------------------------

from delivery_engine import generate_html_email


def test_generate_html_email_all_critical_no_thematic_section(monkeypatch):
    """When all articles score 1–3, Thematic Intelligence must not appear in output."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    data = [
        {**_make_article(f"h{i}", 2, "suppliers", f"Critical Headline {i}"),
         "alert_tier": "CRITICAL"}
        for i in range(3)
    ]
    with patch("delivery_engine._get_openai", return_value=MagicMock()):
        html = generate_html_email(data)
    assert "THEMATIC INTELLIGENCE" not in html
    assert "PERIPHERAL SIGNALS" not in html
    assert "Critical Headline 0" in html


def test_generate_html_email_routes_to_thematic_with_two_plus(monkeypatch):
    """Two articles in same category produce a Thematic Intelligence section."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    mock_client = _make_synthesis_mock({"competitors": "Synthesis paragraph here."})
    data = [
        {**_make_article("a", 7, "competitors", "Avient Headline"), "alert_tier": "ROUTINE"},
        {**_make_article("b", 8, "competitors", "Techmer Headline"), "alert_tier": "ROUTINE"},
    ]
    with patch("delivery_engine._get_openai", return_value=mock_client):
        html = generate_html_email(data)
    assert "THEMATIC INTELLIGENCE" in html
    assert "Synthesis paragraph here." in html


def test_generate_html_email_routes_single_low_to_peripheral(monkeypatch):
    """Single score 4–6 article goes to Peripheral Signals, not Thematic."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    data = [
        {**_make_article("x", 5, "markets", "Peripheral Headline"), "alert_tier": "ROUTINE"},
    ]
    with patch("delivery_engine._get_openai", return_value=MagicMock()):
        html = generate_html_email(data)
    assert "PERIPHERAL SIGNALS" in html
    assert "Peripheral Headline" in html
    assert "THEMATIC INTELLIGENCE" not in html
```

Run:

```bash
pytest tests/test_pipeline.py -k "test_generate_html_email_routes or test_generate_html_email_all" -v
```

Expected: `3 failed`

**Step 2 — Implement**

Replace the body of `generate_html_email()` in `delivery_engine.py`. Find the current implementation starting at line 299 and replace the sections_html block:

```python
def generate_html_email(
    data: list[dict],
    macro_summary: dict | None = None,
) -> str:
    # --- Zone 1: Critical (score 1–3) — always full cards ---
    critical = [r for r in data if (r.get("sentiment_score") or 0) <= 3]

    # --- Zones 2 & 3: Thematic + Peripheral (scores 4–10) ---
    non_critical = [r for r in data if (r.get("sentiment_score") or 0) >= 4]
    groups       = _group_for_thematic(non_critical)
    thin_entries = _collect_thin_entries(non_critical, groups)
    peripheral   = _collect_peripheral(non_critical, groups)
    synthesis    = synthesize_thematic_paragraphs(groups)

    sections_html = (
        _render_section(
            "CRITICAL", "Critical Disruptions",
            "#EF4444", "#FEF2F2", "#B91C1C", critical,
        )
        + _render_thematic_section(groups, thin_entries, synthesis)
        + _render_peripheral_section(peripheral)
    )

    exec_html = _render_exec_summary(macro_summary)

    today_str = datetime.now().strftime("%A, %B %d, %Y")
    total     = len(data)
    item_word = "item" if total == 1 else "items"

    macro_badge_html = ""
    if macro_summary:
        sentiment = macro_summary.get("macro_sentiment", "")
        macro_badge_html = (
            f'<span style="background-color:rgba(127,176,105,0.2);'
            f'color:{_BRAND_GREEN};border:1px solid rgba(127,176,105,0.4);'
            f'padding:3px 12px;border-radius:20px;font-size:11px;font-weight:600;'
            f'font-family:Arial,sans-serif;letter-spacing:0.5px;">'
            f'{sentiment}</span>'
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta name="color-scheme" content="light">
  <title>Americhem Market-Pulse: Daily Intelligence</title>
</head>
<body style="margin:0;padding:0;background-color:#F3F4F6;
             font-family:Arial,sans-serif;-webkit-text-size-adjust:100%;">
  <table width="100%" cellpadding="0" cellspacing="0" border="0"
         style="background-color:#F3F4F6;padding:24px 0;">
    <tr><td align="center">
      <table width="640" cellpadding="0" cellspacing="0" border="0"
             style="max-width:640px;background-color:#ffffff;
                    border:0.5px solid #E5E7EB;border-radius:8px;overflow:hidden;">
        <tr><td>
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="background-color:{_BRAND_NAVY};padding:20px 32px 0 32px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="width:1%;white-space:nowrap;padding-right:16px;">
                      <img src="{_LOGO_URL}" alt="Americhem" width="140"
                           style="display:block;height:auto;max-height:40px;background-color:#ffffff;padding:3px 8px;border-radius:3px;">
                    </td>
                    <td style="width:1%;white-space:nowrap;padding-right:16px;">
                      <div style="width:1px;height:32px;background-color:rgba(255,255,255,0.25);"></div>
                    </td>
                    <td>
                      <p style="margin:0;font-size:11px;font-weight:700;letter-spacing:1.5px;color:{_BRAND_GREEN};font-family:Arial,sans-serif;text-transform:uppercase;">Market Intelligence</p>
                      <p style="margin:2px 0 0 0;font-size:18px;font-weight:700;color:#ffffff;font-family:Arial,sans-serif;line-height:1.2;">Market-Pulse: Daily Intelligence</p>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr><td style="background-color:{_BRAND_GREEN};height:3px;font-size:0;line-height:0;">&nbsp;</td></tr>
            <tr>
              <td style="background-color:{_BRAND_NAVY_DARK};padding:10px 32px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="font-size:12px;color:rgba(255,255,255,0.65);font-family:Arial,sans-serif;">{today_str} &nbsp;&middot;&nbsp; {total} {item_word} today</td>
                    <td align="right">{macro_badge_html}</td>
                  </tr>
                </table>
              </td>
            </tr>
          </table>
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            {exec_html}
            {sections_html}
            <tr><td style="height:24px;"></td></tr>
          </table>
          <table width="100%" cellpadding="0" cellspacing="0" border="0">
            <tr>
              <td style="border-top:0.5px solid #E5E7EB;background-color:#FAFAFA;padding:16px 32px;">
                <table width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="font-size:11px;color:#9CA3AF;font-family:Arial,sans-serif;">
                      Generated by <strong style="color:{_BRAND_NAVY};">Americhem Market-Pulse</strong> &nbsp;&middot;&nbsp; Powered by OpenAI &amp; Supabase
                    </td>
                    <td align="right">
                      <img src="{_LOGO_URL}" alt="Americhem" width="80" style="display:block;height:auto;opacity:0.4;">
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
          </table>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""
```

**Step 3 — Run tests**

```bash
pytest tests/test_pipeline.py -k "test_generate_html_email_routes or test_generate_html_email_all" -v
```

Expected: `3 passed`

**Step 4 — Run full test suite**

```bash
pytest tests/test_pipeline.py -v
```

Expected: all existing tests still pass + 3 new tests pass.

**Step 5 — Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat: wire thematic synthesis pipeline into generate_html_email"
```

---

### Task 8 — Remove dead code and final verification

**Why:** `_render_section()` is now only used for Critical Disruptions. The STRATEGIC and ROUTINE labels (`"Strategic Opportunities"`, `"Routine Monitoring"`) in `generate_html_email()` are removed with the old call sites. Verify no stale references remain.

**Files:** `delivery_engine.py`

**Step 1 — Check for stale references**

```bash
grep -n "STRATEGIC\|ROUTINE\|Strategic Opportunities\|Routine Monitoring" delivery_engine.py
```

Expected: none — these strings lived only in the old `generate_html_email()` call sites now replaced in Task 7.

**Step 2 — Run full test suite one final time**

```bash
pytest tests/test_pipeline.py -v
```

Expected: all tests pass, zero failures.

**Step 3 — Commit**

```bash
git add delivery_engine.py
git commit -m "chore: remove stale STRATEGIC/ROUTINE section references after thematic redesign"
```

---

## Completion Checklist

- [ ] Task 1 — OpenAI infrastructure added, import verified
- [ ] Task 2 — `_group_for_thematic()` passes 4 tests
- [ ] Task 3 — `_collect_thin_entries()` + `_collect_peripheral()` pass 6 tests
- [ ] Task 4 — `synthesize_thematic_paragraphs()` passes 5 tests including graceful degradation
- [ ] Task 5 — `_render_peripheral_section()` passes 4 tests
- [ ] Task 6 — `_render_thematic_section()` passes 5 tests
- [ ] Task 7 — `generate_html_email()` passes 3 integration tests; full suite green
- [ ] Task 8 — No stale references; all tests green
