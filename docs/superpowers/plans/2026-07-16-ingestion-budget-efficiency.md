# Ingestion Budget Efficiency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reclaim wasted ingestion wall-clock so far more of the 126 targets fit inside the 600-second pipeline deadline, without any architecture change.

**Architecture:** Four surgical fixes inside the existing two-script pipeline: (1) enforce `results_per_entity` client-side — Serper's news endpoint returns pages of 10 and ignores small `num` values, so the pipeline currently processes ~5× the configured candidate volume; (2) suppress known-unscrapable domains (login-walled platforms, retail product pages) *before* the Firecrawl call, with honest ledger accounting via a new ingestion-owned suppression reason; (3) drop the Firecrawl wall-clock ceiling from 45s to 20s; (4) remove the 1.5s sleep on the scrape-failed path (no LLM call happened, so there is nothing to rate-limit). Plus a per-target elapsed-time log line to support the follow-up measurement run.

**Tech Stack:** Python 3, pytest (all external APIs mocked — no live calls), existing `SuppressionLedger` taxonomy.

**Out of scope (deliberate):** raising `PIPELINE_DEADLINE_SECONDS` / the workflow timeout, reordering macro groups (reverses a documented invariant — separate product decision), and any target-rotation architecture. Those are gated on the measurement run this PR enables.

---

## File Structure

- Modify: `ingestion_engine.py` — Serper truncation, `UNSCRAPABLE_DOMAINS` + `_is_unscrapable_domain()`, loop integration, timeout constant, sleep removal, per-target timing log
- Modify: `suppression_ledger.py` — add `unscrapable_domain` to the ingestion-owned reason taxonomy
- Modify: `tests/test_pipeline.py` — new tests for truncation, the domain helper, and the pre-scrape suppression path
- Modify: `tests/test_suppression_ledger.py` — new taxonomy test + updated count pin
- Modify: `CLAUDE.md` — taxonomy count, ingestion step description, timeout value

---

### Task 0: Branch setup

- [ ] **Step 1: Create a work branch off main**

```bash
cd /workspaces/market-pulse
git checkout main && git pull
git checkout -b perf/ingestion-budget-efficiency
```

Expected: new branch `perf/ingestion-budget-efficiency`, clean status.

- [ ] **Step 2: Confirm the suite is green before touching anything**

Run: `pytest tests/ -q`
Expected: all tests pass (285 at last CI run).

---

### Task 1: Enforce `results_per_entity` client-side (the 5× fix)

Serper's `/news` endpoint returns results in pages of 10 regardless of small `num` values. `discover_urls()` passes `num: 2` but then consumes **everything** in `data["news"]`. Slice the parsed list.

**Files:**
- Modify: `ingestion_engine.py` (in `discover_urls`, the `results = [...]` list comprehension, ~line 243)
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing test**

In `tests/test_pipeline.py`, add `discover_urls` to the existing `from ingestion_engine import (...)` block (keep the list alphabetized):

```python
from ingestion_engine import (
    _TextExtractor,
    _scrape_fallback,
    _validate_executive_bullets,
    build_query,
    compute_url_hash,
    discover_urls,
    generate_macro_summary,
    load_targets,
    normalize_url,
    scrape_article,
    synthesize_insight,
)
```

Then add this test (a new numbered section at the end of the file, following the file's `# --- N. description ---` comment convention):

```python
# ---------------------------------------------------------------------------
# 22. discover_urls — client-side truncation to results_per_entity
# ---------------------------------------------------------------------------

def test_discover_urls_truncates_to_results_per_entity(monkeypatch):
    """Serper's news endpoint returns pages of 10 regardless of the `num`
    param — the client must enforce results_per_entity itself."""
    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"news": [
                {"link": f"https://example.com/article-{i}", "title": f"Headline {i}"}
                for i in range(10)
            ]}

    monkeypatch.setenv("SERPER_API_KEY", "test_key")
    monkeypatch.setattr("ingestion_engine.requests.post", lambda *a, **k: FakeResponse())

    results = discover_urls("test query", 24, 2)

    assert len(results) == 2
    assert results[0] == ("https://example.com/article-0", "Headline 0")
    assert results[1] == ("https://example.com/article-1", "Headline 1")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_pipeline.py::test_discover_urls_truncates_to_results_per_entity -v`
Expected: FAIL — `assert 10 == 2` (all 10 items come through today).

- [ ] **Step 3: Write minimal implementation**

In `ingestion_engine.py`, `discover_urls()` — slice the comprehension:

```python
    data = response.json()
    # Serper's news endpoint returns pages of 10 and ignores small `num`
    # values, so results_per_entity must be enforced client-side.
    results = [
        (item["link"], item.get("title", ""))
        for item in data.get("news", [])
        if "link" in item
    ][:results_per_entity]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_pipeline.py::test_discover_urls_truncates_to_results_per_entity -v`
Expected: PASS. Then run the whole file: `pytest tests/test_pipeline.py -q` — all pass.

- [ ] **Step 5: Commit**

```bash
git add tests/test_pipeline.py ingestion_engine.py
git commit -m "fix(ingestion): enforce results_per_entity client-side — Serper ignores num<10

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Add `unscrapable_domain` to the suppression taxonomy

The pre-scrape filter (Task 3) needs an ingestion-owned reason code so suppression accounting stays honest. `SuppressionLedger.record()` raises `ValueError` on unknown codes, so the taxonomy entry must land first.

**Files:**
- Modify: `suppression_ledger.py` (`_INGESTION_REASONS`, ~line 10)
- Test: `tests/test_suppression_ledger.py`

- [ ] **Step 1: Write the failing test**

In `tests/test_suppression_ledger.py`, add at the end of the file (mirrors the existing `test_zoominfo_company_mismatch_is_ingestion_owned` pattern):

```python
def test_unscrapable_domain_is_ingestion_owned():
    assert side_of("unscrapable_domain") == "ingestion"
    assert label_for("unscrapable_domain") == "unscrapable domain"
    led = SuppressionLedger.for_ingestion().record(
        "unscrapable_domain", url="https://www.linkedin.com/posts/x", title="T1",
    )
    assert led.breakdown == {"unscrapable_domain": 1}
```

Also update the taxonomy count pin near the top of the file: change `assert len(INGESTION_CODES) == 5` to `assert len(INGESTION_CODES) == 6` (this makes the pin test fail first, which is the point — the taxonomy is deliberately pinned).

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_suppression_ledger.py -v`
Expected: `test_unscrapable_domain_is_ingestion_owned` FAILS with `KeyError: 'unscrapable_domain'` (from `side_of`), and the count-pin test FAILS with `assert 5 == 6`.

- [ ] **Step 3: Write minimal implementation**

In `suppression_ledger.py`, add one entry to `_INGESTION_REASONS`:

```python
_INGESTION_REASONS: tuple[tuple[str, str], ...] = (
    ("duplicate_url",            "duplicate URL"),
    ("semantic_duplicate",       "semantic duplicate"),
    ("llm_discard",              "LLM discard"),
    ("scrape_failed",            "scrape failed"),
    ("unscrapable_domain",       "unscrapable domain"),
    ("zoominfo_company_mismatch", "ZoomInfo company mismatch"),
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_suppression_ledger.py -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add tests/test_suppression_ledger.py suppression_ledger.py
git commit -m "feat(suppression): add unscrapable_domain ingestion reason

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Pre-scrape unscrapable-domain filter

Skip Firecrawl entirely for domains that are login-walled (LinkedIn returns 0 chars after burning the full wall-clock timeout) or retail product pages (Amazon/Home Depot — never articles; today they get scraped, synthesized, and only suppressed post-hoc at delivery). Match by hostname suffix. Record via the ledger; count in provider yield. Also surface the new reason in the QA debug email section (found in Task 2 review: `delivery_engine._render_qa_debug_section` has a hardcoded `display_order` list; codes not in it silently vanish from the "By reason" table while still counting toward the suppressed total).

**Files:**
- Modify: `ingestion_engine.py` — new constant + helper near `normalize_url` (~line 419); loop integration in `execute_pipeline` after the semantic-duplicate check (~line 837); `_new_provider_yield` + `_log_provider_yield` (~lines 325–341)
- Modify: `delivery_engine.py` — add `"unscrapable_domain"` to `display_order` in `_render_qa_debug_section` (~line 363), after `"scrape_failed"`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Add `_is_unscrapable_domain` and `execute_pipeline` to the `from ingestion_engine import (...)` block in `tests/test_pipeline.py`:

```python
from ingestion_engine import (
    _TextExtractor,
    _is_unscrapable_domain,
    _scrape_fallback,
    _validate_executive_bullets,
    build_query,
    compute_url_hash,
    discover_urls,
    execute_pipeline,
    generate_macro_summary,
    load_targets,
    normalize_url,
    scrape_article,
    synthesize_insight,
)
```

Add the tests (new numbered section at the end of the file):

```python
# ---------------------------------------------------------------------------
# 23. Pre-scrape unscrapable-domain filter
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("url,expected", [
    ("https://www.linkedin.com/posts/somebody-123", True),
    ("https://linkedin.com/pulse/x", True),
    ("https://uk.linkedin.com/jobs/view/1", True),     # country subdomain
    ("https://www.amazon.com/dp/B0ABC123", True),
    ("https://www.homedepot.com/p/product/12345", True),
    ("https://www.reuters.com/markets/some-article/", False),
    ("https://notlinkedin.com/article", False),        # suffix must be dot-anchored
    ("not a url", False),                              # malformed → let the scraper decide
])
def test_is_unscrapable_domain(url, expected):
    assert _is_unscrapable_domain(url) is expected


def test_execute_pipeline_skips_unscrapable_domain_before_scraping(monkeypatch):
    """An unscrapable-domain candidate must be suppressed pre-scrape: no
    Firecrawl attempt, and the ledger records unscrapable_domain."""
    import ingestion_engine as ie

    target = {
        "name": "Acme", "category": "competitor", "query": '"Acme"',
        "lookback_hours": 24, "results_per_entity": 2, "min_article_length": 500,
    }
    candidate = {
        "url": "https://www.linkedin.com/posts/acme-update",
        "title": "Acme update", "provider": "serper",
    }
    summary_kwargs = {}

    monkeypatch.setattr(ie, "load_targets", lambda path: [target])
    monkeypatch.setattr(ie, "discover_candidates", lambda t: [candidate])
    monkeypatch.setattr(ie, "_hydrate_seen_headlines", lambda: set())
    monkeypatch.setattr(ie, "url_already_processed", lambda h: False)
    monkeypatch.setattr(
        ie, "scrape_article",
        lambda *a, **k: pytest.fail("scrape_article must not be called for an unscrapable domain"),
    )
    monkeypatch.setattr(
        ie, "generate_macro_summary",
        lambda buffer, screened_count, **kwargs: summary_kwargs.update(kwargs),
    )

    execute_pipeline()

    assert summary_kwargs["suppression_breakdown"] == {"unscrapable_domain": 1}
    assert summary_kwargs["suppression_samples"] == [{
        "reason": "unscrapable_domain",
        "url": "https://www.linkedin.com/posts/acme-update",
        "title": "Acme update",
    }]


def test_render_qa_debug_section_includes_unscrapable_domain():
    """The unscrapable_domain code must get a labeled breakdown row in the QA
    debug section (not just fold into the suppressed total)."""
    from delivery_engine import _render_qa_debug_section
    macro = {
        "screened_count": 40,
        "surfaced_count": 5,
        "suppression_breakdown": {"unscrapable_domain": 4},
        "suppression_samples": [],
    }
    html = _render_qa_debug_section(macro)
    assert "unscrapable domain" in html
    assert ">4</td>" in html
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_pipeline.py -k "unscrapable" -v`
Expected: import-time FAIL — `ImportError: cannot import name '_is_unscrapable_domain'`.

- [ ] **Step 3: Write the implementation**

In `ingestion_engine.py`, add the constant and helper directly above `normalize_url()`:

```python
UNSCRAPABLE_DOMAINS: frozenset[str] = frozenset({
    # Login-walled or bot-blocked platforms — Firecrawl returns 0 chars or
    # burns the full wall-clock timeout on these.
    "linkedin.com", "facebook.com", "instagram.com", "x.com", "twitter.com",
    "youtube.com", "tiktok.com", "reddit.com",
    # Retail product pages — never articles, frequent Serper false positives.
    "amazon.com", "ebay.com", "walmart.com", "homedepot.com", "lowes.com",
})


def _is_unscrapable_domain(url: str) -> bool:
    """True when the URL's host is (or is a subdomain of) a domain we never
    scrape — login-walled platforms and retail product pages that waste the
    Firecrawl budget. Malformed URLs return False (let the scraper decide)."""
    host = (urlparse(url).hostname or "").lower()
    return any(host == d or host.endswith("." + d) for d in UNSCRAPABLE_DOMAINS)
```

In `execute_pipeline()`, insert the check **after** the semantic-duplicate `continue` block and **before** the `_gate_zoominfo_candidate` call:

```python
            if _is_unscrapable_domain(raw_url):
                logger.info("UNSCRAPABLE_DOMAIN — skipped pre-scrape (%s): %s", provider, normalized)
                _bump(provider, "unscrapable")
                suppression_ledger = suppression_ledger.record(
                    "unscrapable_domain", url=raw_url, title=candidate_title,
                )
                continue
```

Update `_new_provider_yield()` to include the new counter:

```python
def _new_provider_yield() -> dict:
    return {
        "discovered": 0, "scraped": 0, "stored": 0,
        "discards": 0, "relevance_dropped": 0, "scrape_failed": 0,
        "unscrapable": 0, "duplicates": 0,
    }
```

In `delivery_engine.py`, `_render_qa_debug_section()` — add the new code to the hardcoded `display_order` list, after `"scrape_failed"` (ingestion-side group):

```python
    display_order = [
        "duplicate_url", "semantic_duplicate", "llm_discard", "scrape_failed",
        "unscrapable_domain", "zoominfo_company_mismatch",
        ...
    ]
```

And extend `_log_provider_yield()` to print it:

```python
def _log_provider_yield(provider_yield: dict[str, dict]) -> None:
    """Emit one yield line per discovery provider seen this run."""
    for provider in sorted(provider_yield):
        y = provider_yield[provider]
        logger.info(
            "Provider yield — %s discovered=%d scraped=%d stored=%d "
            "discards=%d relevance_dropped=%d scrape_failed=%d unscrapable=%d duplicates=%d",
            provider, y["discovered"], y["scraped"], y["stored"],
            y["discards"], y["relevance_dropped"], y["scrape_failed"],
            y["unscrapable"], y["duplicates"],
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_pipeline.py -k "unscrapable" -v`
Expected: all 9 PASS (8 parametrized + 1 pipeline test).
Then the full suite: `pytest tests/ -q` — all pass (catches any provider-yield key mismatch elsewhere, e.g. in `tests/test_zoominfo.py`). If a test elsewhere pins the `_new_provider_yield` key set or the yield log format, update it to include `unscrapable`.

- [ ] **Step 5: Commit**

```bash
git add tests/test_pipeline.py ingestion_engine.py
git commit -m "feat(ingestion): suppress unscrapable domains before the Firecrawl call

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Tighten the Firecrawl ceiling and drop the dead sleep

Two timing-only changes, no new tests (no observable contract beyond the constant's value; behavior is covered by the existing scrape tests and the full suite):

- `FIRECRAWL_WALL_CLOCK_TIMEOUT` 45 → 20. Real articles return in single-digit seconds; 45s only ever protects hangs, and 20s still does.
- Remove `time.sleep(1.5)` in the scrape-failed branch. The sleep paces the OpenAI API between synthesis calls; on a failed scrape no LLM call happened, so it is pure wasted wall-clock. The sleeps on the synthesize-error / discard / stored paths **stay**.

**Files:**
- Modify: `ingestion_engine.py` (~line 42 and the scrape-failed branch, ~line 861)

- [ ] **Step 1: Change the timeout constant**

```python
FIRECRAWL_WALL_CLOCK_TIMEOUT = 20  # hard per-request ceiling; prevents keepalive-induced hangs
```

- [ ] **Step 2: Remove the scrape-failed sleep**

The branch becomes (the `time.sleep(1.5)` line is deleted; nothing else changes):

```python
            article_text = scrape_article(raw_url, min_article_length)
            if article_text is None:
                _bump(provider, "scrape_failed")
                suppression_ledger = suppression_ledger.record(
                    "scrape_failed", url=raw_url, title=candidate_title,
                )
                continue
```

- [ ] **Step 3: Run the full suite**

Run: `pytest tests/ -q`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add ingestion_engine.py
git commit -m "perf(ingestion): 20s Firecrawl ceiling; no sleep on scrape-failed path

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Per-target elapsed-time log

Supports the follow-up measurement run — one log line per target with elapsed seconds and candidate count. Logging only; no test.

**Files:**
- Modify: `ingestion_engine.py` (`execute_pipeline`, the `for target in targets:` body)

- [ ] **Step 1: Add the timing instrumentation**

At the top of the target body (right after the deadline check's `break`), capture the start time:

```python
        entity_name = target["name"]
        category = target["category"]
        min_article_length = target["min_article_length"]
        target_start = time.monotonic()
```

At the very end of the target body (after the inner candidate loop, still inside `for target in targets:`):

```python
        logger.info(
            "Target '%s' processed in %.1fs (%d candidates)",
            entity_name, time.monotonic() - target_start, len(candidates),
        )
```

(The mid-batch deadline / scrape-cap paths `return` before this line — acceptable; those runs end with the stats summary instead.)

- [ ] **Step 2: Run the full suite**

Run: `pytest tests/ -q`
Expected: all pass (the Task 3 pipeline test exercises this code path; `%.1fs` with a monotonic delta cannot raise).

- [ ] **Step 3: Commit**

```bash
git add ingestion_engine.py
git commit -m "chore(ingestion): log per-target elapsed time for budget measurement

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Documentation updates

**Files:**
- Modify: `CLAUDE.md`
- Check: `CONTEXT.md` (update only if it enumerates the suppression codes or the 45s timeout)

- [ ] **Step 1: Update CLAUDE.md**

Three edits:

1. In the `suppression_ledger.py` section, change "(5 ingestion-owned + 9 delivery-owned codes)" to "(6 ingestion-owned + 9 delivery-owned codes)".
2. In the `ingestion_engine.py` numbered flow, step 2: after the semantic-deduplication sentence, append: "Serper results are truncated client-side to `results_per_entity` (the API returns pages of 10 regardless of `num`), and candidates on known-unscrapable domains (login-walled platforms, retail product pages — `UNSCRAPABLE_DOMAINS`) are suppressed before any scrape with reason `unscrapable_domain`."
3. In the `ingestion_engine.py` numbered flow, step 4: note the Firecrawl per-request wall-clock ceiling is 20s.

- [ ] **Step 2: Check CONTEXT.md**

Run: `grep -n -i "suppression\|firecrawl\|45" CONTEXT.md`
If the suppression-code count or the timeout appears, apply the same corrections; otherwise no change.

- [ ] **Step 3: Run the full suite one final time**

Run: `pytest tests/ -q`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md CONTEXT.md
git commit -m "docs: record client-side Serper truncation, unscrapable-domain suppression, 20s Firecrawl ceiling

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## After the plan: measurement (not part of this PR's code)

Once merged, dispatch `.github/workflows/market_pulse_test.yml` with `run_ingestion=true, send_email=false` and read the new per-target timing lines plus the provider-yield `unscrapable` counts. That run decides the follow-ups (deadline raise, macro-group ordering, rotation) — per the standing rule, those gate on measured runtime data, not guesses.
