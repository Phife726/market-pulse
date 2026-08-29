"""ingestion_engine.py — Scrape → Synthesize → Store.

URL normalisation and hashing, the scraper and its fallback, `synthesize_insight`
validation, `generate_macro_summary`, the run loop under the run budget, the synthesis-outage guard, and the ingestion-side repo wiring. Every
external client is a fake or an in-memory adapter — no live calls.
"""

from functools import partial
from unittest.mock import MagicMock, patch
from datetime import datetime

import pytest

import ingestion_engine
from llm import FakeLLM
from run_instant import RunInstant
from run_budget import RunBudget
from tests.conftest import (
    RUN_INSTANT as _RUN,
    stub_http_response,
    stub_insight,
    stub_llm_insight,
    stub_row,
    stub_target,
)
from ingestion_engine import (
    _TextExtractor,
    _is_unscrapable_domain,
    _scrape_fallback,
    compute_url_hash,
    execute_pipeline,
    generate_macro_summary,
    normalize_url,
    scrape_article,
    synthesize_insight,
)
from daily_intelligence_repo import InMemoryIntelligenceRepo


# ===========================================================================
# URL normalisation
# ===========================================================================


def test_url_normalization():
    result = normalize_url("https://news.com/a?utm=1#sec")
    assert result == "https://news.com/a"


def test_url_normalization_preserves_path():
    result = normalize_url("https://news.com/section/article-slug")
    assert result == "https://news.com/section/article-slug"


# ===========================================================================
# Hash collision
# ===========================================================================


def test_compute_url_hash_collision():
    clean = "https://news.com/article"
    polluted = "https://news.com/article?utm_source=newsletter&utm_medium=email&utm_campaign=weekly"
    assert compute_url_hash(normalize_url(clean)) == compute_url_hash(normalize_url(polluted))


# ===========================================================================
# Sentiment score clamping
# ===========================================================================


@pytest.mark.parametrize(
    "raw_score, expected",
    [
        (0,  1),
        (15, 10),
    ],
)
def test_sentiment_clamp(raw_score: int, expected: int):
    with patch("ingestion_engine._llm", return_value=stub_llm_insight(sentiment_score=raw_score)):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["sentiment_score"] == expected


# ===========================================================================
# recommended_action soft default
# ===========================================================================


@pytest.mark.parametrize("payload", [{}, {"recommended_action": "Do something weird"}],
                         ids=["absent", "invalid"])
def test_recommended_action_default(payload):
    with patch("ingestion_engine._llm", return_value=stub_llm_insight(sentiment_score=5, **payload)):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["recommended_action"] == "Monitor"


# ===========================================================================
# article_summary soft default
# ===========================================================================


def test_article_summary_default():
    with patch("ingestion_engine._llm", return_value=stub_llm_insight()):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["article_summary"] == ""


def test_synthesize_insight_uses_low_temperature():
    # Model + json-format are the adapter's contract (see test_llm.py); the caller
    # owns the temperature it requests across the seam.
    fake = stub_llm_insight()

    with patch("ingestion_engine._llm", return_value=fake):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )

    assert result is not None
    assert fake.calls[-1]["temperature"] == 0.2


def test_generate_macro_summary_uses_macro_temperature():
    fake = FakeLLM(returns={
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Body."},
            {"label": "Supply chain watch", "body": "Body."},
            {"label": "Commercial action", "body": "Body."},
        ],
    })
    fake_repo = InMemoryIntelligenceRepo()

    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        result = generate_macro_summary(
            [
                {
                    "category": "competitors",
                    "headline": "Headline",
                    "sentiment_score": 5,
                    "americhem_impact": "Impact.",
                }
            ],
            run=_RUN,
        )

    assert result is True
    assert fake.calls[-1]["temperature"] == 0.3


# ===========================================================================
# _TextExtractor — visible text extraction
# ===========================================================================


def test_text_extractor_strips_tags():
    """_TextExtractor must return visible text with HTML tags removed."""
    html = "<html><body><p>Hello <b>World</b></p></body></html>"
    extractor = _TextExtractor()
    extractor.feed(html)
    text = extractor.get_text()
    assert "Hello" in text
    assert "World" in text
    assert "<" not in text


def test_text_extractor_skips_script_and_style():
    """_TextExtractor must ignore script/style/noscript/nav/footer/header/aside/form content."""
    html = (
        "<html><head><style>body{color:red}</style></head>"
        "<body><script>alert(1)</script><p>Article text here.</p>"
        "<footer>Copyright 2026</footer>"
        "<aside>Subscribe now</aside>"
        "<form>Enter email</form></body></html>"
    )
    extractor = _TextExtractor()
    extractor.feed(html)
    text = extractor.get_text()
    assert "Article text here." in text
    assert "alert" not in text
    assert "body{color:red}" not in text
    assert "Copyright 2026" not in text
    assert "Subscribe now" not in text
    assert "Enter email" not in text


# ===========================================================================
# _scrape_fallback — direct-HTTP fallback
# ===========================================================================


def test_scrape_fallback_returns_text_on_success():
    """_scrape_fallback must return extracted text when the HTTP request succeeds."""
    mock_resp = MagicMock()
    mock_resp.text = "<html><body><p>Chemical plant update with details.</p></body></html>"
    mock_resp.raise_for_status = MagicMock()

    with patch("ingestion_engine.requests.get", return_value=mock_resp):
        result = _scrape_fallback("https://example.com/article")

    assert result is not None
    assert "Chemical plant update" in result


def test_scrape_fallback_returns_none_on_request_error():
    """_scrape_fallback must return None when the HTTP request fails."""
    import requests as _req
    with patch("ingestion_engine.requests.get", side_effect=_req.exceptions.ConnectionError("refused")):
        result = _scrape_fallback("https://example.com/article")
    assert result is None


# ===========================================================================
# scrape_article — 402 triggers fallback
# ===========================================================================


def test_scrape_article_uses_fallback_on_402(monkeypatch):
    """scrape_article must invoke the fallback when Firecrawl returns HTTP 402."""

    # Firecrawl returns 402
    firecrawl_resp = stub_http_response(402)

    # Fallback returns long enough text
    fallback_text = "A" * 600

    monkeypatch.setenv("FIRECRAWL_API_KEY", "test_key")

    with patch("ingestion_engine.requests.post", return_value=firecrawl_resp), \
         patch("ingestion_engine._scrape_fallback", return_value=fallback_text) as mock_fallback:
        result = scrape_article("https://example.com/article", min_length=500)

    mock_fallback.assert_called_once_with("https://example.com/article")
    assert result == fallback_text


def test_scrape_article_returns_none_when_fallback_content_too_short(monkeypatch):
    """scrape_article must return None when fallback text is below min_length."""

    firecrawl_resp = stub_http_response(402)

    monkeypatch.setenv("FIRECRAWL_API_KEY", "test_key")

    with patch("ingestion_engine.requests.post", return_value=firecrawl_resp), \
         patch("ingestion_engine._scrape_fallback", return_value="too short"):
        result = scrape_article("https://example.com/article", min_length=500)

    assert result is None


def test_scrape_article_returns_none_when_fallback_fails(monkeypatch):
    """scrape_article must return None when both Firecrawl (402) and fallback fail."""

    firecrawl_resp = stub_http_response(402)

    monkeypatch.setenv("FIRECRAWL_API_KEY", "test_key")

    with patch("ingestion_engine.requests.post", return_value=firecrawl_resp), \
         patch("ingestion_engine._scrape_fallback", return_value=None):
        result = scrape_article("https://example.com/article", min_length=500)

    assert result is None


def test_scrape_article_no_fallback_on_non_402_error(monkeypatch):
    """scrape_article must NOT invoke the fallback for non-402 Firecrawl errors."""

    firecrawl_resp = stub_http_response(500)

    monkeypatch.setenv("FIRECRAWL_API_KEY", "test_key")

    with patch("ingestion_engine.requests.post", return_value=firecrawl_resp), \
         patch("ingestion_engine._scrape_fallback") as mock_fallback:
        result = scrape_article("https://example.com/article", min_length=500)

    mock_fallback.assert_not_called()
    assert result is None


# ===========================================================================
# generate_macro_summary()
# ===========================================================================


def test_generate_macro_summary_empty_articles_persists_accounting_only_row():
    """Zero stored articles must still persist the run's ingestion accounting
    (issue #43): an accounting-only daily_summaries row carrying screened_count
    and the suppression breakdown/samples, returning False (no summary was
    generated). Content columns are OMITTED from the payload — not written as
    null — so a same-day retry can never wipe an earlier full summary."""
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._repo", lambda: fake_repo):
        result = generate_macro_summary(
            [],
            run=_RUN,
            screened_count=17,
            suppression_breakdown={"duplicate_url": 9, "unscrapable_domain": 2},
            suppression_samples=[{"reason": "duplicate_url", "url": "u", "title": "t"}],
        )
    assert result is False
    stored = fake_repo.fetch_latest_summary(run_mode="production", min_date="2000-01-01")
    assert stored is not None
    assert stored["screened_count"] == 17
    assert stored["suppression_breakdown"] == {"duplicate_url": 9, "unscrapable_domain": 2}
    assert stored["suppression_samples"] == [
        {"reason": "duplicate_url", "url": "u", "title": "t"}
    ]
    for content_key in ("executive_summary", "macro_sentiment", "dominant_condition",
                        "executive_bullets", "macro_outlook", "executive_sources"):
        assert content_key not in stored, f"{content_key} must be omitted, not written"


def _macro_articles() -> list[dict]:
    return [
        stub_row("m1", 9, category="macro_manufacturing", signal_type="Macro", source_url="https://x/1",
                 headline="Manufacturing PMI slips into contraction", americhem_impact="Industrial demand softening."),
        stub_row("m2", 8, category="macro_construction", signal_type="Macro", source_url="https://x/2",
                 headline="Housing starts fall for third month", americhem_impact="Building products demand risk."),
        stub_row("c1", 7, category="competitors", signal_type="Competitive", source_url="https://x/3",
                 headline="Competitor opens new compounding line", americhem_impact="Capacity pressure."),
    ]


def test_generate_macro_summary_persists_macro_outlook_and_union_sources():
    """generate_macro_summary validates + persists macro_outlook and packs
    executive_sources as the UNION of bullet-cited and signal-cited sources."""
    fake = FakeLLM(returns={
        "dominant_condition": "Demand Softness",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Industrial demand cooling.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Feedstock steady.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Engage key accounts.", "citation_source_ids": []},
        ],
        "macro_outlook": {
            "current_condition": "Industrial and construction demand both softening.",
            "signals": [
                {"indicator": "Housing starts", "direction": "Declining",
                 "americhem_implication": "Weakness in Building & Construction-adjacent volumes.",
                 "affected_segments": ["Industrial"], "citation_source_ids": [2]},
            ],
        },
    })
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        result = generate_macro_summary(_macro_articles(), run=_RUN)

    assert result is True
    stored = fake_repo.fetch_latest_summary(run_mode="production", min_date="2000-01-01")
    assert stored["macro_outlook"] is not None
    assert [s["indicator"] for s in stored["macro_outlook"]["signals"]] == ["Housing starts"]
    # executive_sources is the union of bullet-cited (1) and signal-cited (2).
    assert {s["id"] for s in stored["executive_sources"]} == {1, 2}


def test_generate_macro_summary_llm_none_persists_accounting_only_row():
    """An LLM transport failure (None) yields False and no summary content —
    but the run's ingestion accounting must still be persisted (issue #43),
    exactly as on a zero-yield run."""
    fake = FakeLLM(returns=None)
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        result = generate_macro_summary(
            _macro_articles(),
            run=_RUN,
            screened_count=5,
            suppression_breakdown={"scrape_failed": 1},
        )
    assert result is False
    stored = fake_repo.fetch_latest_summary(run_mode="production", min_date="2000-01-01")
    assert stored is not None
    assert stored["screened_count"] == 5
    assert stored["suppression_breakdown"] == {"scrape_failed": 1}
    assert "executive_summary" not in stored
    assert "executive_bullets" not in stored


def test_generate_macro_summary_zero_yield_retry_keeps_earlier_content():
    """Same-day retry: a morning run wrote a full summary; an afternoon retry
    that stores zero articles refreshes the accounting columns WITHOUT wiping
    the morning's summary content (column-subset upsert)."""
    fake = FakeLLM(returns={
        "dominant_condition": "Demand Softness",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Industrial demand cooling.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Feedstock steady.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Engage key accounts.", "citation_source_ids": []},
        ],
    })
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        assert generate_macro_summary(
            _macro_articles(), run=_RUN, screened_count=40,
            suppression_breakdown={"duplicate_url": 3},
        ) is True
    with patch("ingestion_engine._repo", lambda: fake_repo):
        assert generate_macro_summary(
            [], run=_RUN, screened_count=12,
            suppression_breakdown={"duplicate_url": 12},
        ) is False
    stored = fake_repo.fetch_latest_summary(run_mode="production", min_date="2000-01-01")
    assert stored["dominant_condition"] == "Demand Softness"
    assert len(stored["executive_bullets"]) == 3
    assert stored["screened_count"] == 12
    assert stored["suppression_breakdown"] == {"duplicate_url": 12}


def test_generate_macro_summary_malformed_outlook_keeps_bullets():
    """A malformed macro_outlook key degrades to None while the executive
    bullets survive — per-key validation, one call, independent failure."""
    fake = FakeLLM(returns={
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Industrial steady.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Feedstock steady.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Engage.", "citation_source_ids": []},
        ],
        "macro_outlook": "totally not an object",
    })
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        result = generate_macro_summary(_macro_articles(), run=_RUN)
    assert result is True
    stored = fake_repo.fetch_latest_summary(run_mode="production", min_date="2000-01-01")
    assert stored["macro_outlook"] is None
    assert stored["executive_bullets"] is not None
    assert len(stored["executive_bullets"]) == 3


def test_generate_macro_summary_persists_none_when_no_material_signal():
    """When macro_outlook has no material signal, None is persisted (no section)."""
    fake = FakeLLM(returns={
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Steady.", "citation_source_ids": [1]},
            {"label": "Supply chain watch", "body": "Steady.", "citation_source_ids": []},
            {"label": "Commercial action", "body": "Steady.", "citation_source_ids": []},
        ],
        "macro_outlook": {"current_condition": "Quiet.", "signals": []},
    })
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        generate_macro_summary(_macro_articles(), run=_RUN)
    stored = fake_repo.fetch_latest_summary(run_mode="production", min_date="2000-01-01")
    assert stored["macro_outlook"] is None


# Legacy-shaped rows on purpose (`sentiment_score` only): `generate_macro_summary`
# must accept pre-relevance-upgrade rows, which `stub_row` deliberately never builds.
def _make_articles(n: int) -> list[dict]:
    return [
        {"category": "competitors", "headline": f"H{i}",
         "sentiment_score": 5, "americhem_impact": f"Impact {i}."}
        for i in range(n)
    ]


def _capture_summary(fake_repo) -> dict:
    """Return the most recent summary row stored in the fake repo."""
    row = fake_repo.get_delivery_state(run_date=_RUN.run_date, run_mode=_RUN.run_mode)
    assert row is not None, "No summary row was upserted"
    return row


def test_generate_macro_summary_writes_dominant_condition_when_valid():
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    payload = {
        "dominant_condition": "Competitive Pressure",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "Body A."},
            {"label": "Supply chain watch", "body": "Body B."},
            {"label": "Commercial action",  "body": "Body C."},
        ],
    }
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=FakeLLM(returns=payload)), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        assert generate_macro_summary(_make_articles(5), run=_RUN) is True
    row = _capture_summary(fake_repo)
    assert row["dominant_condition"] == "Competitive Pressure"
    # Each bullet gains citation_source_ids (empty when LLM returns none)
    expected_bullets = [
        {"label": "Market pressure",    "body": "Body A.", "citation_source_ids": []},
        {"label": "Supply chain watch", "body": "Body B.", "citation_source_ids": []},
        {"label": "Commercial action",  "body": "Body C.", "citation_source_ids": []},
    ]
    assert row["executive_bullets"] == expected_bullets
    # Legacy fields still populated for backward compat:
    assert row["macro_sentiment"] == "Competitive Pressure"
    assert row["executive_summary"]  # joined paragraph


def test_generate_macro_summary_coerces_invalid_dominant_condition():
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    payload = {
        "dominant_condition": "NonExistentCondition",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
    }
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=FakeLLM(returns=payload)), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        generate_macro_summary(_make_articles(5), run=_RUN)
    row = _capture_summary(fake_repo)
    assert row["dominant_condition"] == "Mixed / Watch"


def test_generate_macro_summary_defaults_low_signal_when_few_articles():
    """When fewer than 3 articles are passed in and the LLM omits a valid condition,
    default to Low Signal."""
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    payload = {"executive_bullets": [
        {"label": "Market pressure",    "body": "Quiet day."},
        {"label": "Supply chain watch", "body": "Quiet day."},
        {"label": "Commercial action",  "body": "Anything."},
    ]}
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=FakeLLM(returns=payload)), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        generate_macro_summary(_make_articles(2), run=_RUN)
    row = _capture_summary(fake_repo)
    assert row["dominant_condition"] == "Low Signal"


def test_generate_macro_summary_low_signal_coerces_action_body():
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    payload = {
        "dominant_condition": "Low Signal",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "Quiet day."},
            {"label": "Supply chain watch", "body": "Quiet day."},
            {"label": "Commercial action",  "body": "Sales should call every customer."},
        ],
    }
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=FakeLLM(returns=payload)), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        generate_macro_summary(_make_articles(2), run=_RUN)
    row = _capture_summary(fake_repo)
    assert row["executive_bullets"][2]["body"] == "No action required."


@pytest.mark.parametrize("bad_bullets", [
    None,                                              # missing key
    [],                                                # wrong count
    [{"label": "Market pressure", "body": "A."}],      # wrong count
    [{"label": "X", "body": "A."},                     # wrong labels
     {"label": "Supply chain watch", "body": "B."},
     {"label": "Commercial action", "body": "C."}],
    [{"label": "Market pressure", "body": "A."},       # wrong order
     {"label": "Commercial action", "body": "B."},
     {"label": "Supply chain watch", "body": "C."}],
    [{"body": "A."},                                   # missing label key
     {"label": "Supply chain watch", "body": "B."},
     {"label": "Commercial action", "body": "C."}],
    "not a list",                                      # wrong type
])
def test_generate_macro_summary_invalid_bullets_set_null(bad_bullets):
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    payload = {"dominant_condition": "Mixed / Watch", "executive_bullets": bad_bullets}
    fake_repo = InMemoryIntelligenceRepo()
    with patch("ingestion_engine._llm", return_value=FakeLLM(returns=payload)), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        generate_macro_summary(_make_articles(5), run=_RUN)
    row = _capture_summary(fake_repo)
    assert row["executive_bullets"] is None
    # Legacy executive_summary still populated so delivery has a fallback:
    assert row["executive_summary"]


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
        assert generate_macro_summary(articles, run=_RUN) is True

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
        generate_macro_summary(articles, run=_RUN)

    user_prompt = fake.calls[-1]["user"]
    assert "[1]" in user_prompt and "TopMateriality" in user_prompt


# ===========================================================================
# Pipeline deadline early-exit
# ===========================================================================


def test_execute_pipeline_deadline_calls_log_stats_and_macro_summary(monkeypatch, tmp_path):
    """When the pipeline deadline is exceeded (here at the first target
    checkpoint — the clock is already past it), _log_stats and
    generate_macro_summary must still be called before the function returns."""
    import textwrap
    import ingestion_engine
    import run_budget

    # Write a minimal targets.yaml with one active entity
    config_yaml = textwrap.dedent(
        """\
        competitors:
          search_mode: entity
          include_all: []
          exclude_any: []
          entities:
            - name: TestCorp
              active: true
        discovery:
          results_per_entity: 2
          lookback_hours: 24
          min_article_length: 500
        """
    )
    cfg_file = tmp_path / "targets.yaml"
    cfg_file.write_text(config_yaml)

    call_count = {"n": 0}

    def fake_monotonic():
        call_count["n"] += 1
        # First call (pipeline_start assignment) returns 0; subsequent calls
        # return a value past the deadline so the first checkpoint stops the run.
        if call_count["n"] == 1:
            return 0.0
        return float(run_budget.PIPELINE_DEADLINE_SECONDS + 1)

    monkeypatch.setattr(ingestion_engine.time, "monotonic", fake_monotonic)

    # Provide one discovered candidate so the inner loop is entered
    monkeypatch.setattr(
        ingestion_engine,
        "discover_candidates",
        lambda *a, **kw: [{
            "url": "https://example.com/article",
            "title": "Test Title",
            "provider": "serper",
        }],
    )

    mock_log_stats = MagicMock()
    mock_macro = MagicMock(return_value=True)
    monkeypatch.setattr(ingestion_engine, "_log_stats", mock_log_stats)
    monkeypatch.setattr(ingestion_engine, "generate_macro_summary", mock_macro)
    monkeypatch.setattr(ingestion_engine, "_hydrate_seen_headlines", lambda: set())

    # Run from the tmp targets file
    monkeypatch.chdir(tmp_path)

    execute_pipeline(_RUN)

    mock_log_stats.assert_called_once()
    mock_macro.assert_called_once()


# ===========================================================================
# Run budget — tail reserve, scrape cap, and the injected-budget guard
# ===========================================================================


def _reserve_candidate(target: dict) -> dict:
    return {
        "url": f"https://example.com/{target['name']}",
        "title": f"News about {target['name']}",
        "provider": "serper",
    }


def _run_reserve_pipeline(
    run_ingestion_pipeline, targets: list[dict], **limits: int,
) -> list[str]:
    """Run execute_pipeline over fake targets (one candidate each, every scrape
    succeeds and stores) under a budget with `limits`, and return the trigger
    entities stored, in order."""
    run = run_ingestion_pipeline(
        targets=targets,
        limits=limits,
        candidates=lambda target: [_reserve_candidate(target)],
        insight=lambda text, url, entity, category: stub_insight(
            url, headline=f"Headline {entity}"),
    )
    return [payload["trigger_entity"] for payload in run.stored]


def test_tail_reserve_skips_entity_targets_when_scrape_budget_low(monkeypatch, run_ingestion_pipeline):
    """When remaining scrape slots fall to the reserve, remaining ENTITY targets
    are skipped but concept targets still run — concept/macro coverage is
    protected from entity-tail starvation."""
    # One concept target × results_per_entity=2 → derived reserve of 2.
    targets = [
        stub_target("EntityA"),
        stub_target("EntityB"),
        stub_target("concept_group", search_mode="concept"),
    ]
    stored = _run_reserve_pipeline(run_ingestion_pipeline, targets, max_scrapes=3)
    # EntityA consumes the single unreserved slot; EntityB is skipped by the
    # reserve; the concept group spends the reserved budget.
    assert stored == ["EntityA", "concept_group"]


def test_tail_reserve_covers_the_full_concept_demand_ahead(monkeypatch, run_ingestion_pipeline):
    """The slot reserve is DERIVED from the concept demand still ahead
    (sum of results_per_entity over the concept targets not yet run), not a fixed constant —
    so every concept/macro group gets a discovery pass even when each earlier
    concept target consumes its full candidate budget."""
    # Two concept targets × results_per_entity=2 → demand 4; cap 5 leaves
    # exactly one unreserved slot for the entity tier.
    targets = [
        stub_target("EntityA"),
        stub_target("EntityB"),
        stub_target("concept_one", search_mode="concept"),
        stub_target("concept_two", search_mode="concept"),
    ]
    stored = _run_reserve_pipeline(run_ingestion_pipeline, targets, max_scrapes=5)
    assert stored == ["EntityA", "concept_one", "concept_two"]


def test_tail_reserve_excludes_front_loaded_concepts(monkeypatch, run_ingestion_pipeline):
    """A concept group positioned BEFORE the entity tier (Tier 1 priority
    segments) must NOT be counted in the entity gate's reserve — it has already
    run, so counting it over-reserves and skips entity targets that the budget
    could still afford. The reserve protects only the concept demand still
    AHEAD of the current target."""
    # concept_front runs first (1 scrape), then two entities, then concept_tail.
    # Static all-concepts reserve = 4 (both concepts) → entity threshold MAX-4=0
    # → both entities wrongly skipped. Position-aware reserve at the entities =
    # only concept_tail (2) → threshold MAX-2=2 → EntityA survives.
    targets = [
        stub_target("concept_front", search_mode="concept"),
        stub_target("EntityA"),
        stub_target("EntityB"),
        stub_target("concept_tail", search_mode="concept"),
    ]
    stored = _run_reserve_pipeline(run_ingestion_pipeline, targets, max_scrapes=4)
    assert stored == ["concept_front", "EntityA", "concept_tail"]


def test_tail_reserve_skips_entity_targets_when_wall_clock_low(monkeypatch, run_ingestion_pipeline):
    """When remaining wall-clock falls to the time reserve, remaining ENTITY
    targets are skipped but concept targets still run."""
    import ingestion_engine

    # First call anchors pipeline_start at 0; everything after runs at t=60 —
    # past the entity cutoff (100-50=50) but inside the hard deadline (100).
    call_count = {"n": 0}

    def fake_monotonic():
        call_count["n"] += 1
        return 0.0 if call_count["n"] == 1 else 60.0

    monkeypatch.setattr(ingestion_engine.time, "monotonic", fake_monotonic)
    targets = [
        stub_target("EntityA"),
        stub_target("concept_group", search_mode="concept"),
    ]
    stored = _run_reserve_pipeline(
        run_ingestion_pipeline, targets, deadline_seconds=100, tail_reserve_seconds=50)
    assert stored == ["concept_group"]


def test_scrape_cap_stops_the_run_before_the_next_target_discovers(run_ingestion_pipeline, caplog):
    """A target reached at the cap never spends discovery it cannot scrape:
    the cap fires at the target checkpoint, before `discover_candidates`,
    not only per candidate."""
    discovered: list[str] = []

    def candidates(target: dict) -> list[dict]:
        discovered.append(target["name"])
        return [_reserve_candidate(target)]

    targets = [
        stub_target("concept_one", search_mode="concept"),
        stub_target("concept_two", search_mode="concept"),
    ]
    run = run_ingestion_pipeline(
        targets=targets, candidates=candidates, limits={"max_scrapes": 1})
    assert [p["trigger_entity"] for p in run.stored] == ["concept_one"]
    assert discovered == ["concept_one"]
    run.macro.assert_called_once()
    assert "Run budget exhausted (scrape_cap) before target 'concept_two'" in caplog.text


def test_scrape_cap_stops_the_run_mid_batch(run_ingestion_pipeline, caplog):
    """A hard limit crossed inside a target's batch ends the run there — the
    remaining candidates are not scraped, the next target is never discovered,
    and the single teardown still runs. (The tail reserve never cuts a started
    target; a hard limit does.)"""
    discovered: list[str] = []

    def candidates(target: dict) -> list[dict]:
        discovered.append(target["name"])
        return [
            {**_reserve_candidate(target), "url": f"https://example.com/{target['name']}/{n}"}
            for n in range(3)
        ]

    targets = [stub_target("concept_one", search_mode="concept"),
               stub_target("concept_two", search_mode="concept")]
    run = run_ingestion_pipeline(
        targets=targets, candidates=candidates, limits={"max_scrapes": 2})
    assert [p["source_url"] for p in run.stored] == [
        "https://example.com/concept_one/0", "https://example.com/concept_one/1"]
    assert discovered == ["concept_one"]
    run.macro.assert_called_once()
    # The operator can tell a cut batch (discovery spent) from a target never
    # started: the next target's checkpoint would stop the run either way.
    assert "Run budget exhausted (scrape_cap) mid-batch at target 'concept_one'" in caplog.text


def test_execute_pipeline_rejects_a_budget_built_from_other_targets(monkeypatch):
    """The budget is indexed by target position, so an injected one must have
    been built from the list the engine loads — a mismatch fails loudly
    before any seam is touched, not with an IndexError mid-run."""
    import ingestion_engine

    monkeypatch.setattr(
        ingestion_engine, "load_targets", lambda path: [stub_target("EntityA")])
    with pytest.raises(ValueError, match="built from a different targets list"):
        ingestion_engine.execute_pipeline(_RUN, budget=RunBudget.for_targets([]))


# ===========================================================================
# synthesize_insight() — relevance-field validation
# ===========================================================================


# The relevance-upgrade payload, legacy `strategic_segment` spelling.
_relevance_llm_legacy_segment = partial(
    stub_llm_insight,
    americhem_impact="Direct impact on compounding margins.",
    sentiment_tag="Neutral",
    americhem_impact_score=7,
    impact_rationale="Directly affects masterbatch feedstock cost.",
    strategic_segment="Raw Materials / Supply Chain",
)


# --- Sentiment tag validation
@pytest.mark.parametrize("bad_tag", ["NEGATIVE", "negative", "Bad", "", None, 42])
def test_synthesize_insight_defaults_invalid_sentiment_tag(bad_tag):
    """Any invalid sentiment_tag must be replaced with 'Neutral'."""
    mock_client = _relevance_llm_legacy_segment(sentiment_tag=bad_tag)
    with patch("ingestion_engine._llm", return_value=mock_client):
        result = synthesize_insight(
            article_text="Article text.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["sentiment_tag"] == "Neutral"


@pytest.mark.parametrize("valid_tag", ["Negative", "Neutral", "Positive"])
def test_synthesize_insight_preserves_valid_sentiment_tag(valid_tag):
    """Valid sentiment_tag values must be preserved unchanged."""
    mock_client = _relevance_llm_legacy_segment(sentiment_tag=valid_tag)
    with patch("ingestion_engine._llm", return_value=mock_client):
        result = synthesize_insight(
            article_text="Article text.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["sentiment_tag"] == valid_tag


# --- americhem_impact_score clamping
@pytest.mark.parametrize(
    "raw_impact, expected",
    [
        (0,   1),
        (-5,  1),
        (11,  10),
        (100, 10),
    ],
)
def test_impact_score_clamped(raw_impact, expected):
    """americhem_impact_score must be clamped to the 1–10 range."""
    mock_client = _relevance_llm_legacy_segment(americhem_impact_score=raw_impact)
    with patch("ingestion_engine._llm", return_value=mock_client):
        result = synthesize_insight(
            article_text="Article text.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["americhem_impact_score"] == expected


@pytest.mark.parametrize("bad_value", [None, "high"])
def test_impact_score_defaults_on_bad_value(bad_value):
    """Non-convertible or missing americhem_impact_score defaults to 5."""
    mock_client = _relevance_llm_legacy_segment(americhem_impact_score=bad_value)
    with patch("ingestion_engine._llm", return_value=mock_client):
        result = synthesize_insight(
            article_text="Article text.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["americhem_impact_score"] == 5


# ===========================================================================
# synthesize_insight() — segment / signal-type validation
# ===========================================================================


# The new-style per-article payload (commercial_segment + signal_type).
_relevance_llm = partial(
    stub_llm_insight,
    americhem_impact="Direct effect on compounding margin.",
    sentiment_tag="Neutral",
    americhem_impact_score=7,
    impact_rationale="Direct feedstock cost effect.",
    commercial_segment="Healthcare",
    signal_type="Technology",
)


@pytest.mark.parametrize(
    "valid_segment",
    [
        "Healthcare", "Fibers",
        "Transportation - Automotive", "Transportation - Non-Automotive",
        "Transportation - Aerospace",
        "Industrial", "Packaging", "Engineered Resins",
        "Enterprise / Cross-Segment",
    ],
)
def test_synthesize_insight_preserves_valid_commercial_segment(valid_segment):
    mock = _relevance_llm(commercial_segment=valid_segment)
    with patch("ingestion_engine._llm", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert result["commercial_segment"] == valid_segment


@pytest.mark.parametrize("bad_segment", [None, "", "  ", "NotASegment", 42])
def test_synthesize_insight_defaults_invalid_commercial_segment(bad_segment):
    mock = _relevance_llm(commercial_segment=bad_segment)
    with patch("ingestion_engine._llm", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert result["commercial_segment"] == "Enterprise / Cross-Segment"


@pytest.mark.parametrize(
    "valid_signal",
    ["Competitive", "Customer", "Regulatory", "Sustainability",
     "Supply Chain", "Technology", "Macro", "Other"],
)
def test_synthesize_insight_preserves_valid_signal_type(valid_signal):
    mock = _relevance_llm(signal_type=valid_signal)
    with patch("ingestion_engine._llm", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert result["signal_type"] == valid_signal


@pytest.mark.parametrize("bad_signal", [None, "", "BAD", 42])
def test_synthesize_insight_defaults_invalid_signal_type(bad_signal):
    mock = _relevance_llm(signal_type=bad_signal)
    with patch("ingestion_engine._llm", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert result["signal_type"] == "Other"


def test_synthesize_insight_drops_strategic_segment_field():
    """If the LLM still returns strategic_segment, it must not appear in the result."""
    mock = _relevance_llm(strategic_segment="LegacyValue")
    with patch("ingestion_engine._llm", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert "strategic_segment" not in result


# ===========================================================================
# Ingestion-side suppression accounting
# ===========================================================================


def test_generate_macro_summary_persists_suppression_breakdown_and_samples():
    """generate_macro_summary must accept counts and samples and persist them."""
    from daily_intelligence_repo import InMemoryIntelligenceRepo
    counts = {"duplicate_url": 3, "llm_discard": 2}
    samples = [
        {"reason": "llm_discard", "url": "https://x.com/1", "title": "Bad article"},
    ]
    fake_repo = InMemoryIntelligenceRepo()
    payload = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
    }
    with patch("ingestion_engine._llm", return_value=FakeLLM(returns=payload)), \
         patch("ingestion_engine._repo", lambda: fake_repo):
        generate_macro_summary(
            _make_articles(5),
            run=_RUN,
            screened_count=87,
            suppression_breakdown=counts,
            suppression_samples=samples,
        )
    row = _capture_summary(fake_repo)
    assert row["screened_count"] == 87
    assert row["suppression_breakdown"] == counts
    assert row["suppression_samples"] == samples


# ===========================================================================
# Prompt text crosses the LLM seam unchanged
# ===========================================================================


def test_synthesize_insight_non_english_body_keeps_english_directive():
    """Regression: a Chinese article body must reach synthesize_insight with the
    English-output directive intact in the system prompt, and the source-language
    body must be forwarded verbatim in the user prompt (no client-side translation)."""
    chinese_body = "中文测试文本 — Teknor Apex 推出含 70% PCR 的 Crealen R PP 汽车内饰再生材料。"

    fake = stub_llm_insight()
    with patch("ingestion_engine._llm", return_value=fake):
        result = synthesize_insight(
            article_text=chinese_body,
            source_url="https://example.cn/article",
            trigger_entity="Teknor Apex",
            category="competitors",
        )

    assert result is not None
    system_message = fake.calls[-1]["system"]
    user_message = fake.calls[-1]["user"]
    from prompts import ENGLISH_OUTPUT_RULE
    assert ENGLISH_OUTPUT_RULE in system_message
    assert chinese_body in user_message, (
        "Source-language article body should be forwarded verbatim to the LLM; "
        "no client-side translation should occur."
    )


def test_generate_macro_summary_ships_prompts_module_text_across_seam():
    """Seam-crossing check: the system/user text generate_macro_summary sends
    through the LLM seam is exactly what prompts.macro_prompt assembles — an
    engine-side prompt override cannot pass unnoticed."""
    import prompts
    from ingestion_engine import generate_macro_summary
    from daily_intelligence_repo import InMemoryIntelligenceRepo as _Repo

    articles = [{"category": "competitors", "headline": "Stub headline",
                 "sentiment_score": 5, "americhem_impact": "Stub impact."}]
    fake = FakeLLM(returns=None)
    with patch("ingestion_engine._llm", return_value=fake), \
         patch("ingestion_engine._repo", lambda: _Repo()):
        generate_macro_summary(articles, run=_RUN)

    mp = prompts.macro_prompt(articles)
    assert fake.calls[-1]["system"] == mp.system
    assert fake.calls[-1]["user"] == mp.user


# ===========================================================================
# Repository wiring — ingestion paths route through _repo()
# ===========================================================================


def test_url_already_processed_routes_through_repo(monkeypatch):
    """url_already_processed returns True iff the InMemory fake reports a hit."""
    from ingestion_engine import url_already_processed
    fake = InMemoryIntelligenceRepo()
    fake.upsert_insight({"url_hash": "abc123", "headline": "Test"})
    monkeypatch.setattr("ingestion_engine._repo", lambda: fake)
    assert url_already_processed("abc123") is True
    assert url_already_processed("never_seen") is False


def test_hydrate_seen_headlines_routes_through_repo(monkeypatch):
    """_hydrate_seen_headlines returns the fake's recent headlines."""
    from ingestion_engine import _hydrate_seen_headlines
    fake = InMemoryIntelligenceRepo()
    fake.upsert_insight({"url_hash": "a", "headline": "Alpha"})
    fake.upsert_insight({"url_hash": "b", "headline": "Beta"})
    monkeypatch.setattr("ingestion_engine._repo", lambda: fake)
    assert _hydrate_seen_headlines() == {"Alpha", "Beta"}


def test_store_insight_routes_through_repo(monkeypatch):
    """store_insight upserts via the repo and returns the fake's stored row."""
    from ingestion_engine import store_insight
    fake = InMemoryIntelligenceRepo()
    monkeypatch.setattr("ingestion_engine._repo", lambda: fake)
    store_insight({"url_hash": "abc", "headline": "Stored"})
    rows = fake.fetch_since(datetime(2000, 1, 1))  # any past cutoff
    assert rows[0]["headline"] == "Stored"


def test_store_insight_raises_on_repo_write_failure(monkeypatch):
    """The repo's write methods raise; store_insight propagates."""
    from ingestion_engine import store_insight
    failing = MagicMock()
    failing.upsert_insight.side_effect = RuntimeError("write blew up")
    monkeypatch.setattr("ingestion_engine._repo", lambda: failing)
    with pytest.raises(RuntimeError, match="write blew up"):
        store_insight({"url_hash": "abc", "headline": "x"})


def test_generate_macro_summary_routes_through_repo(monkeypatch):
    """The summary upsert hits repo.upsert_summary, not Supabase directly."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    fake = InMemoryIntelligenceRepo()
    monkeypatch.setattr("ingestion_engine._repo", lambda: fake)

    # Inject a FakeLLM returning a valid macro summary.
    fake_llm = FakeLLM(returns={
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "Some pressure body text."},
            {"label": "Supply chain watch", "body": "Some supply watch text."},
            {"label": "Commercial action", "body": "Some commercial text."},
        ],
    })

    with patch("ingestion_engine._llm", return_value=fake_llm):
        result = generate_macro_summary([
            {"category": "competitors", "headline": "x",
             "sentiment_score": 5, "americhem_impact": "y"}
        ], run=_RUN)

    assert result is True
    stored = fake.get_delivery_state(run_date=_RUN.run_date, run_mode="production")
    assert stored is not None
    assert stored["dominant_condition"] == "Mixed / Watch"


def test_generate_macro_summary_propagates_repo_write_failure(monkeypatch):
    """If repo.upsert_summary raises, the function raises."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    failing = MagicMock()
    failing.upsert_summary.side_effect = RuntimeError("DB down")
    monkeypatch.setattr("ingestion_engine._repo", lambda: failing)

    fake_llm = FakeLLM(returns={
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure", "body": "x"},
            {"label": "Supply chain watch", "body": "y"},
            {"label": "Commercial action", "body": "z"},
        ],
    })

    with patch("ingestion_engine._llm", return_value=fake_llm):
        with pytest.raises(RuntimeError, match="DB down"):
            generate_macro_summary([
                {"category": "competitors", "headline": "x",
                 "sentiment_score": 5, "americhem_impact": "y"}
            ], run=_RUN)


# ===========================================================================
# Pre-scrape unscrapable-domain filter
# ===========================================================================


@pytest.mark.parametrize("url,expected", [
    ("https://www.linkedin.com/posts/somebody-123", True),
    ("https://linkedin.com/pulse/x", True),
    ("https://uk.linkedin.com/jobs/view/1", True),     # country subdomain
    ("https://www.amazon.com/dp/B0ABC123", True),
    ("https://www.homedepot.com/p/product/12345", True),
    ("https://www.reuters.com/markets/some-article/", False),
    ("https://notlinkedin.com/article", False),        # suffix must be dot-anchored
    ("not a url", False),                              # malformed → let the scraper decide
    ("https://corporate.walmart.com/news/2026/earnings", False),   # retail newsroom subdomain
    ("https://corporate.homedepot.com/newsroom/some-story", False),
    ("https://m.facebook.com/story.php?id=1", True),               # social subdomains still suffix-matched
])
def test_is_unscrapable_domain(url, expected):
    assert _is_unscrapable_domain(url) is expected


def test_execute_pipeline_skips_unscrapable_domain_before_scraping(run_ingestion_pipeline):
    """An unscrapable-domain candidate must be suppressed pre-scrape: no
    Firecrawl attempt, and the ledger records unscrapable_domain."""
    run = run_ingestion_pipeline(
        targets=[{
            "name": "Acme", "category": "competitor", "query": '"Acme"',
            "lookback_hours": 24, "results_per_entity": 2, "min_article_length": 500,
        }],
        candidates=[{
            "url": "https://www.linkedin.com/posts/acme-update",
            "title": "Acme update", "provider": "serper",
        }],
        scrape=lambda *a, **k: pytest.fail(
            "scrape_article must not be called for an unscrapable domain"),
    )

    summary_kwargs = run.macro.call_args.kwargs
    assert summary_kwargs["suppression_breakdown"] == {"unscrapable_domain": 1}
    assert summary_kwargs["suppression_samples"] == [{
        "reason": "unscrapable_domain",
        "url": "https://www.linkedin.com/posts/acme-update",
        "title": "Acme update",
    }]


# ===========================================================================
# scrape_article — wall-clock ceiling actually bounds wall-clock
# ===========================================================================


def test_scrape_article_returns_promptly_after_wall_clock_timeout(monkeypatch):
    """After the wall-clock timeout fires, scrape_article must return without
    waiting for the hung request thread (the old `with ThreadPoolExecutor`
    pattern blocked in shutdown(wait=True) until the thread finished)."""
    import time as _time
    import threading
    import ingestion_engine as ie

    def hanging_post(*args, **kwargs):
        # Use threading.Event().wait instead of time.sleep in case tests
        # globally monkeypatch time.sleep to a no-op.
        threading.Event().wait(2.0)
        raise AssertionError("hung request should have been abandoned")

    monkeypatch.setenv("FIRECRAWL_API_KEY", "test_key")
    monkeypatch.setattr(ie, "FIRECRAWL_WALL_CLOCK_TIMEOUT", 0.2)
    monkeypatch.setattr("ingestion_engine.requests.post", hanging_post)

    start = _time.monotonic()
    result = ie.scrape_article("https://example.com/article", min_length=500)
    elapsed = _time.monotonic() - start

    assert result is None
    assert elapsed < 1.0  # must not wait out the 2s hung thread


# ===========================================================================
# Synthesis-outage guard — a dead LLM must not look like a quiet news day
# ===========================================================================


def _outage_candidate(n: int) -> dict:
    return {
        "url": f"https://news.com/article-{n}",
        "title": f"Headline {n}",
        "provider": "serper",
    }


_OUTAGE_TARGET = {
    "name": "TestCorp", "category": "competitors",
    "query": '"TestCorp"', "results_per_entity": 10,
    "lookback_hours": 24, "min_article_length": 500,
    "search_mode": "entity",
}


def test_synthesis_outage_detected_when_every_attempt_failed():
    """The 2026-08-03 signature: articles scraped, zero stored, every LLM call
    rejected (expired credits). Not a quiet day — an outage."""
    assert ingestion_engine.is_synthesis_outage(
        {"insights_stored": 0}, {"synthesis_failed": 98},
    )


def test_quiet_news_day_is_not_an_outage():
    """Nothing scraped and nothing stored is the legitimate no-news run: the
    LLM was never asked, so there is nothing to alarm about."""
    assert not ingestion_engine.is_synthesis_outage(
        {"insights_stored": 0}, {"duplicate_url": 205, "semantic_duplicate": 3},
    )


def test_discards_prove_the_llm_answered():
    """A discard is a *successful* LLM call that judged the article irrelevant.
    Zero stored alongside discards is a real quiet day, not an outage."""
    assert not ingestion_engine.is_synthesis_outage(
        {"insights_stored": 0}, {"synthesis_failed": 4, "llm_discard": 12},
    )


def test_partial_synthesis_failure_is_not_an_outage():
    """Some articles stored means the LLM is up; scattered failures are the
    ordinary flakiness the seam already swallows."""
    assert not ingestion_engine.is_synthesis_outage(
        {"insights_stored": 7}, {"synthesis_failed": 40},
    )


@pytest.mark.parametrize("failures", [1, 2])
def test_below_minimum_attempts_is_not_an_outage(failures: int):
    """One or two failed calls on an otherwise empty day is too thin a sample
    to suppress delivery over — the guard needs a real run behind it."""
    assert not ingestion_engine.is_synthesis_outage(
        {"insights_stored": 0}, {"synthesis_failed": failures},
    )


def test_outage_minimum_is_the_declared_constant():
    """The threshold is a named constant, not a literal buried in the predicate."""
    n = ingestion_engine.SYNTHESIS_OUTAGE_MIN_ATTEMPTS
    assert not ingestion_engine.is_synthesis_outage(
        {"insights_stored": 0}, {"synthesis_failed": n - 1},
    )
    assert ingestion_engine.is_synthesis_outage(
        {"insights_stored": 0}, {"synthesis_failed": n},
    )


def test_pipeline_raises_on_synthesis_outage_after_persisting_accounting(
    run_ingestion_pipeline,
):
    """The run must still record what it screened *before* it fails: the
    accounting-only summary row is written, then the outage is raised."""
    n = ingestion_engine.SYNTHESIS_OUTAGE_MIN_ATTEMPTS
    with pytest.raises(ingestion_engine.SynthesisOutageError) as excinfo:
        run_ingestion_pipeline(
            targets=[_OUTAGE_TARGET],
            candidates=[_outage_candidate(i) for i in range(n)],
            insight=None,  # unusable LLM response -> synthesis_failed
        )

    # The operator-facing message must name the counts, not just "failed".
    assert str(n) in str(excinfo.value)


def test_execute_pipeline_hands_the_run_instant_to_the_macro_summary(run_ingestion_pipeline):
    """The row ingestion writes is keyed on the instant main() read: the
    harness's fixed instant must reach generate_macro_summary verbatim."""
    run = run_ingestion_pipeline(
        targets=[{"name": "Acme", "category": "competitors", "search_mode": "entity",
                  "results_per_entity": 2, "min_article_length": 500}],
        candidates=[],
    )
    assert run.macro.call_args.kwargs["run"] is _RUN


def test_finalize_persists_accounting_before_raising(monkeypatch):
    """Ordering guard: the accounting-only row is written BEFORE the outage is
    raised, so a failed run still records what it screened."""
    import ingestion_engine as ie

    calls: list = []
    monkeypatch.setattr(
        ie, "generate_macro_summary",
        lambda articles, **kwargs: calls.append(kwargs) or True,
    )

    ctx = ie.RunContext(providers_by_name={})
    ctx.stats["urls_discovered"] = 40
    for i in range(ie.SYNTHESIS_OUTAGE_MIN_ATTEMPTS):
        ctx.suppress("synthesis_failed", "serper",
                     url=f"https://news.com/a-{i}", title=f"H{i}")

    with pytest.raises(ie.SynthesisOutageError):
        ie._finalize_run(ctx, _RUN)

    assert len(calls) == 1
    assert calls[0]["run"] is _RUN   # the summary row is keyed on the run instant
    assert calls[0]["screened_count"] == 40
    assert calls[0]["suppression_breakdown"]["synthesis_failed"] == (
        ie.SYNTHESIS_OUTAGE_MIN_ATTEMPTS
    )


def test_successful_run_does_not_raise(run_ingestion_pipeline):
    """The guard is silent on any run that stored something."""
    run = run_ingestion_pipeline(
        targets=[_OUTAGE_TARGET],
        candidates=[_outage_candidate(1)],
    )
    assert len(run.stored) == 1


def test_main_exits_nonzero_on_synthesis_outage(monkeypatch):
    """The cron contract: an outage exits non-zero so the GitHub Actions job
    goes red AND the delivery step never runs — no misleading no-news email."""
    import ingestion_engine as ie

    monkeypatch.setattr(ie.config, "validate_environment", lambda engine: None)

    def _boom(run) -> None:
        raise ie.SynthesisOutageError("every synthesis call failed")

    monkeypatch.setattr(ie, "execute_pipeline", _boom)

    with pytest.raises(SystemExit) as excinfo:
        ie.main()
    assert excinfo.value.code == 1


def test_main_returns_normally_on_a_healthy_run(monkeypatch):
    import ingestion_engine as ie

    monkeypatch.setattr(ie.config, "validate_environment", lambda engine: None)
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    seen: list = []
    monkeypatch.setattr(ie, "execute_pipeline", seen.append)

    ie.main()  # no SystemExit
    # main() is where the run instant is read — once — and handed down; the
    # env's run mode rides on it, which is how a QA run keys its own row.
    assert len(seen) == 1 and isinstance(seen[0], RunInstant)
    assert seen[0].test_mode
