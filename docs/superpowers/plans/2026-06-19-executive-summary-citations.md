# Executive Summary Source Citations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add validated, persistent, reader-facing source citations to the executive-summary bullets — each bullet ends with a grouped `[1, 2, 3]` of links to supporting articles, plus a "Sources" footer listing `[n] headline — domain`.

**Architecture:** The summary is generated in `ingestion_engine.generate_macro_summary` (which builds a deterministic, capped, numbered *source pack*, has the LLM cite pack IDs per bullet, validates those IDs in code, and persists both the cleaned bullets and an `executive_sources` map) and rendered in `delivery_engine` (which renumbers cited IDs sequentially by first appearance, renders inline citation links + a Sources footer, with http(s)-only URL validation and HTML escaping). Citations survive the DB round-trip via a new `executive_sources` jsonb column on `daily_summaries`.

**Tech Stack:** Python 3, pytest, Supabase (Postgres jsonb), existing `llm.py` / `daily_intelligence_repo.py` / `insight.py` seams.

**Reference spec:** `docs/superpowers/specs/2026-06-19-executive-summary-citations-design.md`

**Branch:** `feat/exec-summary-citations` (already checked out; spec already committed there).

---

## Rollout note (read before deploying — not a code task)

Migration `004` adds a **required** column. The repo write contract raises on unknown columns, and `fetch_latest_summary` swallows a bad SELECT and reports "no summary". So — exactly like migrations `001`/`002` — **apply migration `004` in Supabase BEFORE the code ships.** No feature flag (unlike `003`, which gated an optional feature); citations are core. The InMemory test fake round-trips arbitrary columns, so the whole test suite passes without the migration applied.

---

## File Structure

- `migrations/004_add_executive_sources.sql` — **Create.** Adds `executive_sources jsonb` to `daily_summaries`.
- `schema.sql` — **Modify.** Same column, for fresh-DB initialization.
- `ingestion_engine.py` — **Modify.** Constants, source-pack builder, citation-ID cleaning, extended bullet validation, wired `generate_macro_summary` (numbered digest, prompt, persist `executive_sources`).
- `daily_intelligence_repo.py` — **Modify.** Add `executive_sources` to the prod `fetch_latest_summary` SELECT.
- `delivery_engine.py` — **Modify.** URL-scheme guard, display-number map, inline citation rendering, Sources footer, wired `_render_exec_summary`.
- `tests/test_pipeline.py` — **Modify.** Ingestion validation/pack + delivery rendering tests.
- `tests/test_intelligence_repo.py` — **Modify.** Assert the prod SELECT requests `executive_sources`.

---

## Task 1: Add the `executive_sources` column (migration + schema)

**Files:**
- Create: `migrations/004_add_executive_sources.sql`
- Modify: `schema.sql:51-65` (the `daily_summaries` table)

- [ ] **Step 1: Create the migration file**

Create `migrations/004_add_executive_sources.sql`:

```sql
-- Migration 004: Add executive_sources to daily_summaries.
-- Backs reader-facing source citations on the executive-summary bullets.
-- Apply via Supabase SQL editor or psql. Safe to run multiple times.
--
-- IMPORTANT ROLLOUT ORDER:
--   Apply this migration BEFORE deploying the citation code. The column is
--   REQUIRED (not flag-gated): generate_macro_summary writes executive_sources
--   on every run, and delivery's fetch_latest_summary SELECTs it. Deploying the
--   code first would crash ingestion upserts and blank out delivery's summary.
--
-- Column:
--   executive_sources — JSON array of cited source objects:
--     [{ "id": int, "headline": text, "url": text,
--        "domain": text, "segment": text, "score": int }]
--   Holds only sources cited by at least one executive bullet. Empty array when
--   the summary has no valid citations.

alter table daily_summaries
  add column if not exists executive_sources jsonb;
```

- [ ] **Step 2: Add the column to `schema.sql`**

In `schema.sql`, the `daily_summaries` table currently ends (lines 60-64):

```sql
    dominant_condition text,
    executive_bullets jsonb,
    screened_count integer,
    surfaced_count integer,
    suppression_breakdown jsonb,
    suppression_samples jsonb
);
```

Add `executive_sources jsonb` after `executive_bullets jsonb,` so the block reads:

```sql
    dominant_condition text,
    executive_bullets jsonb,
    executive_sources jsonb,
    screened_count integer,
    surfaced_count integer,
    suppression_breakdown jsonb,
    suppression_samples jsonb
);
```

- [ ] **Step 3: Sanity-check the SQL parses (no DB needed)**

Run: `python -c "import pathlib; print(pathlib.Path('migrations/004_add_executive_sources.sql').read_text().count('executive_sources'))"`
Expected: prints `2` (one in the comment, one in the DDL).

- [ ] **Step 4: Commit**

```bash
git add migrations/004_add_executive_sources.sql schema.sql
git commit -m "feat(schema): add executive_sources column for summary citations"
```

---

## Task 2: Source-pack builder + constants (ingestion)

Deterministic, capped, numbered citable-source pack. The invariant: **the same article set always yields the same pack IDs.**

**Files:**
- Modify: `ingestion_engine.py` (add constants + helpers near `_EXEC_BULLET_LABELS`, line ~659)
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py` (near the other ingestion imports at the top, `_build_macro_source_pack` and the constants are imported from `ingestion_engine`):

```python
from ingestion_engine import (
    _build_macro_source_pack,
    _rank_macro_articles,
    MAX_MACRO_SUMMARY_SOURCE_PACK_ARTICLES,
)


def _article(headline, *, score=5, url="https://example.com/a", url_hash="h", segment="Healthcare"):
    return {
        "headline": headline,
        "americhem_impact_score": score,
        "source_url": url,
        "url_hash": url_hash,
        "commercial_segment": segment,
    }


def test_source_pack_orders_by_materiality_then_headline_then_hash():
    articles = [
        _article("Bravo", score=5, url_hash="h2"),
        _article("Alpha", score=9, url_hash="h1"),
        _article("Charlie", score=5, url_hash="h0"),
    ]
    pack = _build_macro_source_pack(_rank_macro_articles(articles))
    # Materiality 9 first; remaining two (score 5) tie-break by headline asc.
    assert [s["headline"] for s in pack] == ["Alpha", "Bravo", "Charlie"]
    assert [s["id"] for s in pack] == [1, 2, 3]


def test_source_pack_is_deterministic_for_same_set():
    articles = [_article(f"H{i}", score=i % 4, url_hash=f"h{i}") for i in range(10)]
    a = _build_macro_source_pack(_rank_macro_articles(list(articles)))
    b = _build_macro_source_pack(_rank_macro_articles(list(reversed(articles))))
    assert [(s["id"], s["headline"]) for s in a] == [(s["id"], s["headline"]) for s in b]


def test_source_pack_caps_at_max():
    articles = [_article(f"H{i:02d}", score=5, url_hash=f"h{i:02d}") for i in range(60)]
    pack = _build_macro_source_pack(_rank_macro_articles(articles))
    assert len(pack) == MAX_MACRO_SUMMARY_SOURCE_PACK_ARTICLES
    assert pack[-1]["id"] == MAX_MACRO_SUMMARY_SOURCE_PACK_ARTICLES


def test_source_pack_entry_shape_and_domain():
    pack = _build_macro_source_pack(_rank_macro_articles(
        [_article("Alpha", url="https://www.Reuters.com/x", segment="Auto")]
    ))
    s = pack[0]
    assert s == {
        "id": 1,
        "headline": "Alpha",
        "url": "https://www.Reuters.com/x",
        "domain": "reuters.com",
        "segment": "Auto",
        "score": 5,
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -k "source_pack" -v`
Expected: FAIL with `ImportError: cannot import name '_build_macro_source_pack'`.

- [ ] **Step 3: Implement constants + helpers**

In `ingestion_engine.py`, immediately after the `_EXEC_BULLET_LABELS` definition (ends line 659), add:

```python
MAX_MACRO_SUMMARY_SOURCE_PACK_ARTICLES = 40
MAX_EXECUTIVE_BULLET_CITATIONS = 3


def _source_domain(url: str) -> str:
    """Lowercased host minus a leading 'www.'; '' when unparseable/empty."""
    from urllib.parse import urlparse
    try:
        host = urlparse(url or "").netloc.lower()
    except (ValueError, TypeError):
        return ""
    return host[4:] if host.startswith("www.") else host


def _rank_macro_articles(articles: list[dict]) -> list[dict]:
    """Deterministic, capped ordering of citable articles.

    Sort key: materiality desc, headline asc, url_hash asc. created_at is NOT
    used — the in-memory stored-articles buffer does not carry it — but the key
    is still fully deterministic, so the same article set always ranks the same.
    """
    ordered = sorted(
        articles,
        key=lambda a: (
            -insight.effective_impact(a),
            a.get("headline", "") or "",
            a.get("url_hash", "") or "",
        ),
    )
    return ordered[:MAX_MACRO_SUMMARY_SOURCE_PACK_ARTICLES]


def _build_macro_source_pack(ranked_articles: list[dict]) -> list[dict]:
    """Number the already-ranked articles 1..N as the citable source pack.

    Pass the output of _rank_macro_articles. Each entry:
    {id, headline, url, domain, segment, score}.
    """
    pack: list[dict] = []
    for i, a in enumerate(ranked_articles, start=1):
        url = a.get("source_url", "") or ""
        pack.append({
            "id": i,
            "headline": a.get("headline", "") or "",
            "url": url,
            "domain": _source_domain(url),
            "segment": insight.commercial_segment(a),
            "score": insight.effective_impact(a),
        })
    return pack
```

(`insight` is already imported in `ingestion_engine.py`.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/test_pipeline.py -k "source_pack" -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add ingestion_engine.py tests/test_pipeline.py
git commit -m "feat(ingestion): deterministic capped macro source pack"
```

---

## Task 3: Citation-ID cleaning + extended bullet validation (ingestion)

**Files:**
- Modify: `ingestion_engine.py:707-726` (`_validate_executive_bullets`)
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py`:

```python
from ingestion_engine import _validate_executive_bullets


def _raw_bullets(a_ids, b_ids, c_ids):
    return [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": a_ids},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": b_ids},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": c_ids},
    ]


def test_validate_bullets_keeps_only_in_pack_ids():
    out = _validate_executive_bullets(_raw_bullets([1, 99], [2], []), frozenset({1, 2}))
    assert out[0]["citation_source_ids"] == [1]   # 99 not in pack -> dropped
    assert out[1]["citation_source_ids"] == [2]
    assert out[2]["citation_source_ids"] == []


def test_validate_bullets_dedupes_preserving_order():
    out = _validate_executive_bullets(_raw_bullets([2, 1, 2, 1], [], []), frozenset({1, 2}))
    assert out[0]["citation_source_ids"] == [2, 1]


def test_validate_bullets_caps_citations_per_bullet():
    out = _validate_executive_bullets(_raw_bullets([1, 2, 3, 4], [], []), frozenset({1, 2, 3, 4}))
    assert out[0]["citation_source_ids"] == [1, 2, 3]   # MAX_EXECUTIVE_BULLET_CITATIONS


def test_validate_bullets_garbage_citations_become_empty():
    raw = _raw_bullets("nope", [None, "x", True, 1.5], [1], frozenset())  # noqa: intentional
    # Rebuild explicitly to control each field type:
    raw = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": "nope"},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": [None, "x", True, 1.5]},
        {"label": "Commercial action", "body": "C."},  # key missing entirely
    ]
    out = _validate_executive_bullets(raw, frozenset({1, 2}))
    assert out[0]["citation_source_ids"] == []
    assert out[1]["citation_source_ids"] == []   # bool True excluded, non-ints excluded
    assert out[2]["citation_source_ids"] == []


def test_validate_bullets_rejects_wrong_label_order():
    raw = [
        {"label": "Supply chain watch", "body": "A.", "citation_source_ids": []},
        {"label": "Market pressure", "body": "B.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    assert _validate_executive_bullets(raw, frozenset()) is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -k "validate_bullets" -v`
Expected: FAIL — current `_validate_executive_bullets` takes one arg and returns bullets without `citation_source_ids`.

- [ ] **Step 3: Replace `_validate_executive_bullets` and add `_clean_citation_ids`**

In `ingestion_engine.py`, replace the whole function at lines 707-726 with:

```python
def _clean_citation_ids(raw, valid_source_ids: frozenset[int]) -> list[int]:
    """Keep only int ids present in valid_source_ids: dedupe (order preserved),
    cap at MAX_EXECUTIVE_BULLET_CITATIONS. bool is excluded (it subclasses int).
    Any non-list / garbage input yields []."""
    if not isinstance(raw, list):
        return []
    out: list[int] = []
    for v in raw:
        if isinstance(v, bool) or not isinstance(v, int):
            continue
        if v not in valid_source_ids or v in out:
            continue
        out.append(v)
        if len(out) >= MAX_EXECUTIVE_BULLET_CITATIONS:
            break
    return out


def _validate_executive_bullets(raw, valid_source_ids: frozenset[int] = frozenset()) -> Optional[list[dict]]:
    """Return the cleaned bullets list if valid; None otherwise (delivery falls
    back to prose).

    Valid shape: exactly 3 objects, with labels matching _EXEC_BULLET_LABELS in
    order, and non-empty string body fields. Each returned bullet carries a
    cleaned citation_source_ids list (only ids in valid_source_ids survive;
    invalid ids are never stored).
    """
    if not isinstance(raw, list) or len(raw) != 3:
        return None
    cleaned: list[dict] = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            return None
        label = item.get("label")
        body = item.get("body")
        if label != _EXEC_BULLET_LABELS[i]:
            return None
        if not isinstance(body, str) or not body.strip():
            return None
        cleaned.append({
            "label": label,
            "body": body.strip(),
            "citation_source_ids": _clean_citation_ids(item.get("citation_source_ids"), valid_source_ids),
        })
    return cleaned
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/test_pipeline.py -k "validate_bullets" -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add ingestion_engine.py tests/test_pipeline.py
git commit -m "feat(ingestion): validate and clean per-bullet citation ids"
```

---

## Task 4: Wire `generate_macro_summary` (numbered digest, prompt, persist sources)

**Files:**
- Modify: `ingestion_engine.py:751-827` (inside `generate_macro_summary`)
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_pipeline.py`:

```python
def test_generate_macro_summary_persists_validated_citations():
    fake = FakeLLM(returns={
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Pricing firm.", "citation_source_ids": [1, 99]},
            {"label": "Supply chain watch", "body": "Freight rising.", "citation_source_ids": [2, 2]},
            {"label": "Commercial action", "body": "Watch packaging.", "citation_source_ids": []},
        ],
    })
    fake_repo = InMemoryIntelligenceRepo()
    articles = [
        {"category": "competitors", "headline": "Alpha", "americhem_impact_score": 9,
         "americhem_impact": "x", "source_url": "https://a.com/1", "url_hash": "h1",
         "commercial_segment": "Healthcare"},
        {"category": "competitors", "headline": "Bravo", "americhem_impact_score": 7,
         "americhem_impact": "y", "source_url": "https://b.com/2", "url_hash": "h2",
         "commercial_segment": "Auto"},
    ]

    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        assert generate_macro_summary(articles) is True

    stored = fake_repo.fetch_latest_summary(run_mode="production", min_date="2000-01-01")
    bullets = stored["executive_bullets"]
    assert bullets[0]["citation_source_ids"] == [1]   # 99 dropped (not in pack)
    assert bullets[1]["citation_source_ids"] == [2]   # deduped
    assert bullets[2]["citation_source_ids"] == []
    # executive_sources holds only cited ids (1 and 2), with full metadata.
    src_ids = sorted(s["id"] for s in stored["executive_sources"])
    assert src_ids == [1, 2]
    assert {s["domain"] for s in stored["executive_sources"]} == {"a.com", "b.com"}


def test_generate_macro_summary_numbers_the_digest():
    fake = FakeLLM(returns={
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "A.", "citation_source_ids": []},
            {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
        ],
    })
    fake_repo = InMemoryIntelligenceRepo()
    articles = [
        {"category": "competitors", "headline": "TopMateriality", "americhem_impact_score": 9,
         "americhem_impact": "x", "source_url": "https://a.com/1", "url_hash": "h1"},
    ]
    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        generate_macro_summary(articles)

    user_prompt = fake.calls[-1]["user"]
    assert "[1]" in user_prompt and "TopMateriality" in user_prompt
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -k "persists_validated_citations or numbers_the_digest" -v`
Expected: FAIL — digest has no `[1]` prefix and the stored row has no `executive_sources`.

- [ ] **Step 3: Rewrite the body of `generate_macro_summary`**

In `ingestion_engine.py`, replace the digest construction at lines 751-756:

```python
    article_digest = "\n".join(
        f"- [{a.get('category', '').upper()}] {a.get('headline', '')} "
        f"(Impact {a.get('americhem_impact_score', a.get('sentiment_score', ''))}/10): "
        f"{a.get('americhem_impact', '')}"
        for a in articles
    )
```

with a ranked, numbered digest and source pack:

```python
    ranked = _rank_macro_articles(articles)
    source_pack = _build_macro_source_pack(ranked)
    valid_source_ids = frozenset(s["id"] for s in source_pack)

    article_digest = "\n".join(
        f"[{i}] [{a.get('category', '').upper()}] {a.get('headline', '')} "
        f"(Impact {insight.effective_impact(a)}/10): "
        f"{a.get('americhem_impact', '')}"
        for i, a in enumerate(ranked, start=1)
    )
```

- [ ] **Step 4: Update the system prompt to request citations**

Replace the `system_prompt` assignment at lines 761-779 with:

```python
    system_prompt = (
        f"OUTPUT LANGUAGE:\n{_ENGLISH_OUTPUT_RULE}\n\n"
        "You are a senior Americhem commercial intelligence analyst writing the morning brief\n"
        "for GMMs and Sales leaders. Output ONLY a JSON object with two keys.\n\n"
        "1. dominant_condition — pick exactly one value from this list that best describes\n"
        "   today's overall commercial weather across the digest:\n"
        f"     {macro_conditions_text}\n\n"
        "2. executive_bullets — exactly three objects, in this order, with these exact labels:\n"
        f'     {{"label": "{label_a}",    "body": "<one sentence, <=30 words>", "citation_source_ids": [<source numbers>]}}\n'
        f'     {{"label": "{label_b}", "body": "<one sentence, <=30 words>", "citation_source_ids": [<source numbers>]}}\n'
        f'     {{"label": "{label_c}",  "body": "<one sentence, <=30 words>", "citation_source_ids": [<source numbers>]}}\n\n'
        '   Each body must reference specific named entities or segments from the digest.\n'
        '   citation_source_ids: the bracketed [n] source numbers from the digest that\n'
        f'   directly support that body. Cite 1 to {MAX_EXECUTIVE_BULLET_CITATIONS} of the most relevant\n'
        '   sources, most relevant first. Use ONLY source numbers that appear in the digest.\n'
        '   If a bullet is not supported by any specific source, use an empty list [].\n'
        '   Do NOT hedge ("may", "could", "potentially") without a specific data point.\n'
        '   Do NOT write generic statements ("monitor closely", "remain vigilant").\n\n'
        '   Low-signal special case:\n'
        '   If dominant_condition is "Low Signal", the Commercial action body MUST be the\n'
        '   literal string "No action required." with citation_source_ids []. The other two\n'
        '   bullets MUST describe the absence of meaningful signal.'
    )
```

- [ ] **Step 5: Pass the pack to validation, force-bullet shape, and persist sources**

Replace lines 804-808:

```python
    # Validate executive_bullets.
    bullets = _validate_executive_bullets(parsed.get("executive_bullets"))

    # Low Signal: force the third bullet body.
    if bullets is not None and cond == "Low Signal":
        bullets[2] = {"label": _EXEC_BULLET_LABELS[2], "body": "No action required."}
```

with:

```python
    # Validate executive_bullets (cleans per-bullet citation_source_ids against the pack).
    bullets = _validate_executive_bullets(parsed.get("executive_bullets"), valid_source_ids)

    # Low Signal: force the third bullet body.
    if bullets is not None and cond == "Low Signal":
        bullets[2] = {
            "label": _EXEC_BULLET_LABELS[2],
            "body": "No action required.",
            "citation_source_ids": [],
        }

    # executive_sources: pack entries cited by at least one surviving bullet.
    cited_ids: set[int] = set()
    if bullets is not None:
        for b in bullets:
            cited_ids.update(b["citation_source_ids"])
    executive_sources = [s for s in source_pack if s["id"] in cited_ids]
```

Then add `executive_sources` to the upsert dict (lines 817-827). After the `"executive_bullets": bullets,` line, add `"executive_sources": executive_sources,`:

```python
    _repo().upsert_summary({
        "run_date": date.today().isoformat(),
        "run_mode": _run_mode(),
        "dominant_condition": cond,
        "executive_bullets": bullets,
        "executive_sources": executive_sources,
        "executive_summary": executive_summary,
        "macro_sentiment": cond,
        "screened_count": screened_count,
        "suppression_breakdown": suppression_breakdown or {},
        "suppression_samples": suppression_samples or [],
    })
```

- [ ] **Step 6: Run the new + existing macro tests**

Run: `pytest tests/test_pipeline.py -k "macro_summary or validate_bullets or source_pack" -v`
Expected: PASS (including the pre-existing `test_generate_macro_summary_uses_macro_temperature` and `test_generate_macro_summary_empty_articles`).

- [ ] **Step 7: Commit**

```bash
git add ingestion_engine.py tests/test_pipeline.py
git commit -m "feat(ingestion): cite numbered sources in macro summary"
```

---

## Task 5: Fetch `executive_sources` in the prod repo

**Files:**
- Modify: `daily_intelligence_repo.py:119-123` (the `fetch_latest_summary` SELECT)
- Test: `tests/test_intelligence_repo.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_intelligence_repo.py`:

```python
from daily_intelligence_repo import SupabaseIntelligenceRepo


class _RecordingTable:
    def __init__(self):
        self.select_arg = None

    def select(self, arg):
        self.select_arg = arg
        return self

    def eq(self, *a, **k):
        return self

    def gte(self, *a, **k):
        return self

    def order(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def execute(self):
        class _R:
            data = []
        return _R()


class _RecordingSupabase:
    def __init__(self):
        self.table_obj = _RecordingTable()

    def table(self, name):
        return self.table_obj


def test_fetch_latest_summary_selects_executive_sources(monkeypatch):
    repo = SupabaseIntelligenceRepo()
    rec = _RecordingSupabase()
    monkeypatch.setattr(repo, "_supabase", lambda: rec)
    repo.fetch_latest_summary(run_mode="production", min_date="2026-01-01")
    assert "executive_sources" in rec.table_obj.select_arg
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/test_intelligence_repo.py -k "selects_executive_sources" -v`
Expected: FAIL — `executive_sources` is not in the SELECT string.

- [ ] **Step 3: Add the column to the SELECT**

In `daily_intelligence_repo.py`, the SELECT at lines 119-123 reads:

```python
                .select(
                    "run_date, run_mode, executive_summary, macro_sentiment, "
                    "dominant_condition, executive_bullets, screened_count, "
                    "surfaced_count, suppression_breakdown, suppression_samples"
                )
```

Change it to include `executive_sources`:

```python
                .select(
                    "run_date, run_mode, executive_summary, macro_sentiment, "
                    "dominant_condition, executive_bullets, executive_sources, "
                    "screened_count, surfaced_count, suppression_breakdown, "
                    "suppression_samples"
                )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/test_intelligence_repo.py -k "selects_executive_sources" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add daily_intelligence_repo.py tests/test_intelligence_repo.py
git commit -m "feat(repo): fetch executive_sources in latest-summary query"
```

---

## Task 6: Delivery rendering helpers (display map, URL guard, inline + footer)

**Files:**
- Modify: `delivery_engine.py` (top-of-file imports; helpers near `_render_executive_bullets`, line ~605)
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py`:

```python
from delivery_engine import (
    _safe_http_url,
    _citation_display_map,
    _render_executive_bullets,
    _render_sources_footer,
)


def _src(id, headline="H", url="https://x.com/a", domain="x.com"):
    return {"id": id, "headline": headline, "url": url, "domain": domain,
            "segment": "Auto", "score": 7}


def test_safe_http_url_allows_http_and_https():
    assert _safe_http_url("https://x.com/a") == "https://x.com/a"
    assert _safe_http_url("http://x.com/a") == "http://x.com/a"


def test_safe_http_url_rejects_other_schemes():
    assert _safe_http_url("javascript:alert(1)") == ""
    assert _safe_http_url("data:text/html,x") == ""
    assert _safe_http_url("") == ""
    assert _safe_http_url(None) == ""


def test_citation_display_map_renumbers_by_first_appearance():
    bullets = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": [5, 8]},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": [8, 2]},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    sources = [_src(5), _src(8), _src(2)]
    assert _citation_display_map(bullets, sources) == {5: 1, 8: 2, 2: 3}


def test_citation_display_map_ignores_ids_without_a_source():
    bullets = [{"label": "Market pressure", "body": "A.", "citation_source_ids": [5, 99]}]
    assert _citation_display_map(bullets, [_src(5)]) == {5: 1}


def test_render_bullets_inline_citation_is_grouped_and_linked():
    bullets = [
        {"label": "Market pressure", "body": "Pricing firm.", "citation_source_ids": [5, 8]},
        {"label": "Supply chain watch", "body": "Freight up.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "Watch.", "citation_source_ids": []},
    ]
    sources = [_src(5, url="https://a.com/x"), _src(8, url="https://b.com/y")]
    dmap = _citation_display_map(bullets, sources)
    html_out = _render_executive_bullets(bullets, sources, dmap)
    assert "Pricing firm." in html_out
    assert 'href="https://a.com/x"' in html_out
    assert 'title="https://a.com/x"' in html_out
    assert ">1</a>" in html_out and ">2</a>" in html_out
    # Grouped: a comma separates the two numbers, enclosed in brackets.
    assert "[" in html_out and ", " in html_out and "]" in html_out


def test_render_bullets_no_citation_when_empty():
    bullets = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": []},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    html_out = _render_executive_bullets(bullets, [], {})
    assert "<a" not in html_out


def test_render_bullets_escapes_malicious_url_and_headline():
    bullets = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": [1]},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    # javascript: scheme must be dropped -> number rendered as plain text, no href.
    sources = [_src(1, url="javascript:alert(1)")]
    dmap = _citation_display_map(bullets, sources)
    html_out = _render_executive_bullets(bullets, sources, dmap)
    assert "javascript:alert(1)" not in html_out
    assert "href=" not in html_out
    assert ">1<" in html_out or "[1]" in html_out  # number still shown, just unlinked


def test_render_sources_footer_orders_and_escapes():
    bullets = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": [8, 5]},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    sources = [
        _src(5, headline="Resin <b>up</b>", url="https://a.com/x", domain="a.com"),
        _src(8, headline="Freight", url="https://b.com/y", domain="b.com"),
    ]
    dmap = _citation_display_map(bullets, sources)
    footer = _render_sources_footer(sources, dmap)
    # Display order follows first appearance: 8 -> [1], 5 -> [2].
    assert footer.index("Freight") < footer.index("Resin")
    assert "b.com" in footer and "a.com" in footer
    assert "<b>up</b>" not in footer        # escaped
    assert "&lt;b&gt;up&lt;/b&gt;" in footer


def test_render_sources_footer_empty_when_no_citations():
    assert _render_sources_footer([], {}) == ""


def test_render_sources_footer_handles_missing_url_gracefully():
    bullets = [{"label": "Market pressure", "body": "A.", "citation_source_ids": [1]}]
    sources = [_src(1, headline="", url="", domain="")]
    dmap = _citation_display_map(bullets, sources)
    footer = _render_sources_footer(sources, dmap)
    assert footer != ""               # does not crash, still renders a row
    assert "href=" not in footer      # no valid URL -> unlinked
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -k "safe_http_url or citation_display_map or render_bullets or sources_footer" -v`
Expected: FAIL with `ImportError: cannot import name '_safe_http_url'`.

- [ ] **Step 3: Ensure imports exist**

At the top of `delivery_engine.py`, confirm `import html` and `from urllib.parse import urlparse` are present. If either is missing, add it with the other top-level imports.

- [ ] **Step 4: Add the helpers and replace `_render_executive_bullets`**

In `delivery_engine.py`, the current `_render_executive_bullets` is at lines 605-625. Replace that function with the four functions below (new helpers + the new two-extra-arg signature):

```python
def _safe_http_url(url) -> str:
    """Return url only when its scheme is http/https; otherwise ''. Guards
    against javascript:/data: and malformed values being placed into href."""
    if not isinstance(url, str) or not url:
        return ""
    try:
        scheme = urlparse(url).scheme.lower()
    except (ValueError, TypeError):
        return ""
    return url if scheme in ("http", "https") else ""


def _citation_display_map(bullets, sources) -> dict:
    """Map raw cited source id -> sequential display number (1..N), ordered by
    first appearance across bullets. Only ids that have a matching source entry
    are numbered, so legacy rows (no executive_sources) yield an empty map."""
    src_ids = {s["id"] for s in (sources or []) if isinstance(s, dict) and "id" in s}
    order: list = []
    for b in bullets or []:
        if not isinstance(b, dict):
            continue
        for cid in b.get("citation_source_ids") or []:
            if cid in src_ids and cid not in order:
                order.append(cid)
    return {cid: n for n, cid in enumerate(order, start=1)}


def _render_citation_marker(cited_ids, src_by_id, display_map) -> str:
    """Grouped inline citation, e.g. [1, 2]. Each number links to its source URL
    (http/https only; otherwise plain text). Returns '' when nothing to show."""
    parts: list[str] = []
    for cid in cited_ids or []:
        if cid not in display_map:
            continue
        n = display_map[cid]
        url = _safe_http_url((src_by_id.get(cid) or {}).get("url"))
        if url:
            safe = html.escape(url, quote=True)
            parts.append(
                f'<a href="{safe}" title="{safe}" '
                f'style="color:{_BRAND_NAVY};text-decoration:none;">{n}</a>'
            )
        else:
            parts.append(str(n))
    if not parts:
        return ""
    inner = ", ".join(parts)
    return (
        f'&nbsp;<span style="font-size:10px;color:{_BRAND_NAVY};'
        f'vertical-align:super;">[{inner}]</span>'
    )


def _render_executive_bullets(bullets: list[dict], sources=None, display_map=None) -> str:
    """Render the 3-bullet executive summary body, each bullet followed by its
    grouped inline citation marker when it has resolvable cited sources.

    sources/display_map default to empty so legacy callers (and legacy rows with
    no citations) render exactly as before, with no markers.
    """
    sources = sources or []
    display_map = display_map or {}
    src_by_id = {s["id"]: s for s in sources if isinstance(s, dict) and "id" in s}
    items_html = ""
    for b in bullets:
        label = b.get("label", "") if isinstance(b, dict) else ""
        body = b.get("body", "") if isinstance(b, dict) else ""
        cited = b.get("citation_source_ids", []) if isinstance(b, dict) else []
        marker = _render_citation_marker(cited, src_by_id, display_map)
        items_html += (
            f'<tr><td style="padding:2px 0;font-size:13px;color:#1a2a45;'
            f"font-family:Georgia,'Times New Roman',serif;line-height:1.55;\">"
            f'&bull;&nbsp;<strong>{label}:</strong> {body}{marker}'
            f'</td></tr>'
        )
    return (
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0">'
        f'{items_html}</table>'
    )


def _render_sources_footer(sources, display_map) -> str:
    """Render the 'Sources' footer: one row per cited source, ordered by display
    number, as '[n] headline — domain' linked to the source URL. Empty string
    when there are no cited sources."""
    if not display_map:
        return ""
    src_by_id = {s["id"]: s for s in (sources or []) if isinstance(s, dict) and "id" in s}
    rows = ""
    for cid, n in sorted(display_map.items(), key=lambda kv: kv[1]):
        src = src_by_id.get(cid) or {}
        headline = html.escape(src.get("headline") or "Headline unavailable")
        domain = html.escape(src.get("domain") or "source link")
        label = f"[{n}] {headline} &mdash; {domain}"
        url = _safe_http_url(src.get("url"))
        if url:
            safe = html.escape(url, quote=True)
            entry = (
                f'<a href="{safe}" style="color:{_BRAND_NAVY};text-decoration:none;">{label}</a>'
            )
        else:
            entry = label
        rows += (
            f'<tr><td style="padding:1px 0;font-size:11px;color:#5a6678;'
            f"font-family:Arial,sans-serif;line-height:1.5;\">{entry}</td></tr>"
        )
    return (
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0" '
        f'style="margin-top:10px;border-top:1px solid #d8deec;padding-top:6px;">'
        f'<tr><td style="padding:4px 0 2px 0;font-size:9px;font-weight:700;'
        f'letter-spacing:1px;color:#5a6678;font-family:Arial,sans-serif;'
        f'text-transform:uppercase;">Sources</td></tr>{rows}</table>'
    )
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pytest tests/test_pipeline.py -k "safe_http_url or citation_display_map or render_bullets or sources_footer" -v`
Expected: PASS (11 tests).

- [ ] **Step 6: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat(delivery): citation markers and sources footer renderers"
```

---

## Task 7: Wire `_render_exec_summary` (pass sources, append footer, backward compat)

**Files:**
- Modify: `delivery_engine.py:628-681` (`_render_exec_summary`)
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_pipeline.py`:

```python
from delivery_engine import _render_exec_summary


def test_exec_summary_renders_inline_citations_and_footer():
    macro = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Pricing firm.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Freight up.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Watch.", "citation_source_ids": []},
        ],
        "executive_sources": [
            {"id": 1, "headline": "Resin prices climb", "url": "https://reuters.com/x",
             "domain": "reuters.com", "segment": "Auto", "score": 8},
        ],
    }
    html_out = _render_exec_summary(macro)
    assert "Pricing firm." in html_out
    assert 'href="https://reuters.com/x"' in html_out
    assert "Sources" in html_out
    assert "Resin prices climb" in html_out
    assert "reuters.com" in html_out


def test_exec_summary_legacy_row_renders_without_footer():
    # Old row: bullets without citation_source_ids, no executive_sources.
    macro = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action", "body": "C."},
        ],
    }
    html_out = _render_exec_summary(macro)
    assert "A." in html_out
    assert "Sources" not in html_out
    assert "<a" not in html_out


def test_exec_summary_prose_fallback_unchanged():
    macro = {"executive_summary": "Prose summary.", "dominant_condition": "Low Signal"}
    html_out = _render_exec_summary(macro)
    assert "Prose summary." in html_out
    assert "Sources" not in html_out
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/test_pipeline.py -k "exec_summary_renders_inline or legacy_row_renders or prose_fallback_unchanged" -v`
Expected: FAIL — footer is never rendered and bullets are called without sources.

- [ ] **Step 3: Wire sources + footer into `_render_exec_summary`**

In `delivery_engine.py`, the relevant slice of `_render_exec_summary` (lines 637-654) is:

```python
    bullets = macro_summary.get("executive_bullets")
    legacy_text = macro_summary.get("executive_summary") or ""
    condition = (
        macro_summary.get("dominant_condition")
        or macro_summary.get("macro_sentiment")
        or ""
    )

    if bullets:
        body_html = _render_executive_bullets(bullets)
    elif legacy_text:
        body_html = (
            f'<p style="margin:0;font-size:14px;color:#1a2a45;'
            f"font-family:Georgia,'Times New Roman',serif;line-height:1.65;\">"
            f'{legacy_text}</p>'
        )
    else:
        return ""
```

Replace it with (adds the sources lookup, display map, and footer):

```python
    bullets = macro_summary.get("executive_bullets")
    sources = macro_summary.get("executive_sources") or []
    legacy_text = macro_summary.get("executive_summary") or ""
    condition = (
        macro_summary.get("dominant_condition")
        or macro_summary.get("macro_sentiment")
        or ""
    )

    footer_html = ""
    if bullets:
        display_map = _citation_display_map(bullets, sources)
        body_html = _render_executive_bullets(bullets, sources, display_map)
        footer_html = _render_sources_footer(sources, display_map)
    elif legacy_text:
        body_html = (
            f'<p style="margin:0;font-size:14px;color:#1a2a45;'
            f"font-family:Georgia,'Times New Roman',serif;line-height:1.65;\">"
            f'{legacy_text}</p>'
        )
    else:
        return ""
```

- [ ] **Step 4: Render the footer inside the box**

In the same function, the returned HTML (lines 671-681) currently ends the box cell with:

```python
                  Executive Summary{badge_html}
                </p>
                {body_html}
              </td>
```

Add `{footer_html}` immediately after `{body_html}`:

```python
                  Executive Summary{badge_html}
                </p>
                {body_html}
                {footer_html}
              </td>
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pytest tests/test_pipeline.py -k "exec_summary" -v`
Expected: PASS.

- [ ] **Step 6: Run the full suite**

Run: `pytest tests/ -q`
Expected: PASS — all tests, including the pre-existing delivery integration tests that assert the removed section headers do not appear.

- [ ] **Step 7: Commit**

```bash
git add delivery_engine.py tests/test_pipeline.py
git commit -m "feat(delivery): wire citations and sources footer into exec summary"
```

---

## Self-Review (completed by plan author)

**Spec coverage** — every spec section maps to a task:
- Validated LLM citations / source pack → Tasks 2, 3, 4.
- `citation_source_ids` field name → Task 3.
- Deterministic capped pack (40, materiality→headline→url_hash; created_at dropped because the in-memory buffer lacks it — fully deterministic regardless) → Task 2.
- Sequential renumber by first appearance → Task 6 (`_citation_display_map`), tested.
- Grouped `[1, 2, 3]` inline, each number linked, `title` hover → Task 6 (`_render_citation_marker`).
- Footer `[n] headline — domain` linked, display order → Task 6 (`_render_sources_footer`).
- Zero-citation → omit all UI → Task 6/7 (empty display map ⇒ no markers, footer returns "").
- http(s)-only URL guard + `html.escape` → Task 6 (`_safe_http_url`, escaping in both renderers).
- Domain via `urlparse().netloc` minus `www.`, graceful empties → Task 2 (`_source_domain`), Task 6 footer fallback.
- New `executive_sources` jsonb column + migration `004` + `schema.sql` → Task 1.
- Repo persistence (upsert pass-through; fake needs no change) + fetch SELECT → Task 4 (write) / Task 5 (read).
- Backward compatibility (missing/null/[] sources, bullets without `citation_source_ids`, legacy prose) → Task 6/7, tested in Task 7.
- Migration-first rollout (required column, no flag) → Rollout note + Task 1 migration header.
- Tests enumerated in spec → covered across Tasks 2–7.

**Deferred (not in any task, by design):** factual claim verification, heuristic citation matching, confidence scoring, per-sentence prose citations, `executive_source_policy` column, LLM-module refactor, dead card-path cleanup.

**Placeholder scan:** none — every code/test step shows complete code.

**Type consistency:** `_rank_macro_articles` → `_build_macro_source_pack` chain; pack entry keys `{id, headline, url, domain, segment, score}` identical across ingestion (Task 2/4) and delivery (Task 6/7); `_validate_executive_bullets(raw, valid_source_ids)` signature consistent between Task 3 (definition) and Task 4 (call); `citation_source_ids` key spelled identically everywhere; `_citation_display_map(bullets, sources)`, `_render_executive_bullets(bullets, sources, display_map)`, `_render_sources_footer(sources, display_map)` signatures consistent between Task 6 and Task 7.
