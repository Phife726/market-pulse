"""
Smoke tests for ingestion_engine.py and delivery_engine.py.
No live API calls — all external clients are mocked.
"""
import json
import textwrap
from unittest.mock import MagicMock, patch

import pytest

from ingestion_engine import (
    _TextExtractor,
    _scrape_fallback,
    build_query,
    compute_url_hash,
    generate_macro_summary,
    load_targets,
    normalize_url,
    scrape_article,
    synthesize_insight,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_openai_mock(sentiment_score: int | float) -> MagicMock:
    content = json.dumps(
        {
            "headline": "Test Headline",
            "americhem_impact": "Test impact on Americhem.",
            "sentiment_score": sentiment_score,
            "source_url": "https://news.com/article",
            "entities_mentioned": ["Avient"],
        }
    )
    mock_message = MagicMock()
    mock_message.content = content
    mock_choice = MagicMock()
    mock_choice.message = mock_message
    mock_completion = MagicMock()
    mock_completion.choices = [mock_choice]
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_completion
    return mock_client


# ---------------------------------------------------------------------------
# 1. URL normalisation
# ---------------------------------------------------------------------------

def test_url_normalization():
    result = normalize_url("https://news.com/a?utm=1#sec")
    assert result == "https://news.com/a"


def test_url_normalization_preserves_path():
    result = normalize_url("https://news.com/section/article-slug")
    assert result == "https://news.com/section/article-slug"


# ---------------------------------------------------------------------------
# 2. Hash collision
# ---------------------------------------------------------------------------

def test_compute_url_hash_collision():
    clean = "https://news.com/article"
    polluted = "https://news.com/article?utm_source=newsletter&utm_medium=email&utm_campaign=weekly"
    assert compute_url_hash(normalize_url(clean)) == compute_url_hash(normalize_url(polluted))


# ---------------------------------------------------------------------------
# 3. Sentiment score clamping
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw_score, expected",
    [
        (0,  1),
        (15, 10),
    ],
)
def test_sentiment_clamp(raw_score: int, expected: int):
    with patch("ingestion_engine._get_openai", return_value=_make_openai_mock(raw_score)):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["sentiment_score"] == expected


# ---------------------------------------------------------------------------
# 4. load_targets
# ---------------------------------------------------------------------------

def test_load_targets_filters_inactive(tmp_path):
    """Inactive entities in entity-mode groups must not appear in results."""
    config_yaml = textwrap.dedent(
        """\
        competitors:
          search_mode: entity
          include_all: []
          exclude_any: []
          entities:
            - name: ActiveCorp
              active: true
            - name: InactiveCorp
              active: false
        discovery:
          results_per_entity: 2
          lookback_hours: 24
          min_article_length: 500
        """
    )
    config_file = tmp_path / "targets.yaml"
    config_file.write_text(config_yaml)
    targets = load_targets(str(config_file))
    names = [t["name"] for t in targets]
    assert "ActiveCorp" in names
    assert "InactiveCorp" not in names


def test_load_targets_returns_expected_fields(tmp_path):
    """Entity-mode target dicts must contain name, category, query, and discovery fields."""
    config_yaml = textwrap.dedent(
        """\
        competitors:
          search_mode: entity
          include_all: []
          exclude_any: []
          entities:
            - name: Avient
              active: true
        discovery:
          results_per_entity: 3
          lookback_hours: 48
          min_article_length: 300
        """
    )
    config_file = tmp_path / "targets.yaml"
    config_file.write_text(config_yaml)
    targets = load_targets(str(config_file))
    assert len(targets) == 1
    t = targets[0]
    assert t["name"] == "Avient"
    assert t["category"] == "competitors"
    assert t["query"] == '"Avient"'
    assert t["results_per_entity"] == 3
    assert t["lookback_hours"] == 48
    assert t["min_article_length"] == 300


def test_load_targets_concept_group(tmp_path):
    """Active concept-mode groups produce a single target with an OR query."""
    config_yaml = textwrap.dedent(
        """\
        industry:
          search_mode: concept
          active: true
          include_any:
            - "plastics industry"
            - "chemical industry"
          include_all: []
          exclude_any:
            - tenders
        discovery:
          results_per_entity: 2
          lookback_hours: 24
          min_article_length: 500
        """
    )
    config_file = tmp_path / "targets.yaml"
    config_file.write_text(config_yaml)
    targets = load_targets(str(config_file))
    assert len(targets) == 1
    t = targets[0]
    assert t["name"] == "industry"
    assert t["category"] == "industry"
    assert '("plastics industry" OR "chemical industry")' in t["query"]
    assert '-"tenders"' in t["query"]


def test_load_targets_inactive_concept_group(tmp_path):
    """Concept-mode groups with active: false must not appear in results."""
    config_yaml = textwrap.dedent(
        """\
        industry:
          search_mode: concept
          active: false
          include_any:
            - "plastics industry"
          include_all: []
          exclude_any: []
        discovery:
          results_per_entity: 2
          lookback_hours: 24
          min_article_length: 500
        """
    )
    config_file = tmp_path / "targets.yaml"
    config_file.write_text(config_yaml)
    targets = load_targets(str(config_file))
    assert targets == []


def test_load_targets_entity_excludes_applied_to_query(tmp_path):
    """Group-level exclude_any must appear as -\"term\" in every entity query."""
    config_yaml = textwrap.dedent(
        """\
        customers:
          search_mode: entity
          include_all: []
          exclude_any:
            - patents
            - "securities analyst reports"
          entities:
            - name: Shaw Industries
              active: true
        discovery:
          results_per_entity: 2
          lookback_hours: 24
          min_article_length: 500
        """
    )
    config_file = tmp_path / "targets.yaml"
    config_file.write_text(config_yaml)
    targets = load_targets(str(config_file))
    assert len(targets) == 1
    q = targets[0]["query"]
    assert '"Shaw Industries"' in q
    assert '-"patents"' in q
    assert '-"securities analyst reports"' in q


# ---------------------------------------------------------------------------
# 5. DISCARD signal
# ---------------------------------------------------------------------------

def test_discard_signal_detected():
    insight = {"americhem_impact": "DISCARD"}
    assert insight.get("americhem_impact") == "DISCARD"


# ---------------------------------------------------------------------------
# 6. recommended_action soft default
# ---------------------------------------------------------------------------

def _make_openai_mock_no_action(sentiment_score: int) -> MagicMock:
    content = json.dumps(
        {
            "headline": "Test Headline",
            "americhem_impact": "Test impact on Americhem.",
            "sentiment_score": sentiment_score,
            "source_url": "https://news.com/article",
            "entities_mentioned": ["Avient"],
        }
    )
    mock_message = MagicMock()
    mock_message.content = content
    mock_choice = MagicMock()
    mock_choice.message = mock_message
    mock_completion = MagicMock()
    mock_completion.choices = [mock_choice]
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_completion
    return mock_client


def _make_openai_mock_invalid_action(sentiment_score: int) -> MagicMock:
    content = json.dumps(
        {
            "headline": "Test Headline",
            "americhem_impact": "Test impact on Americhem.",
            "sentiment_score": sentiment_score,
            "recommended_action": "Do something weird",
            "source_url": "https://news.com/article",
            "entities_mentioned": ["Avient"],
        }
    )
    mock_message = MagicMock()
    mock_message.content = content
    mock_choice = MagicMock()
    mock_choice.message = mock_message
    mock_completion = MagicMock()
    mock_completion.choices = [mock_choice]
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_completion
    return mock_client


@pytest.mark.parametrize("mock_fn", [_make_openai_mock_no_action, _make_openai_mock_invalid_action])
def test_recommended_action_default(mock_fn):
    with patch("ingestion_engine._get_openai", return_value=mock_fn(5)):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["recommended_action"] == "Monitor"


# ---------------------------------------------------------------------------
# 8. article_summary soft default
# ---------------------------------------------------------------------------

def test_article_summary_default():
    with patch("ingestion_engine._get_openai", return_value=_make_openai_mock(5)):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["article_summary"] == ""


def test_synthesize_insight_uses_gpt_5_4_nano():
    mock_client = _make_openai_mock(5)

    with patch("ingestion_engine._get_openai", return_value=mock_client):
        result = synthesize_insight(
            article_text="Some article text about the market.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )

    assert result is not None
    _, kwargs = mock_client.chat.completions.create.call_args
    assert kwargs["model"] == "gpt-5.4-nano"


def test_generate_macro_summary_uses_gpt_5_4_nano():
    mock_message = MagicMock()
    mock_message.content = json.dumps(
        {
            "executive_summary": "Summary text.",
            "macro_sentiment": "Stable",
        }
    )
    mock_choice = MagicMock()
    mock_choice.message = mock_message
    mock_completion = MagicMock()
    mock_completion.choices = [mock_choice]
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_completion
    mock_supabase = MagicMock()
    mock_supabase.table.return_value.upsert.return_value.execute.return_value = MagicMock()

    with patch("ingestion_engine._get_openai", return_value=mock_client), patch(
        "ingestion_engine._get_supabase", return_value=mock_supabase
    ):
        result = generate_macro_summary(
            [
                {
                    "category": "competitors",
                    "headline": "Headline",
                    "sentiment_score": 5,
                    "americhem_impact": "Impact.",
                }
            ]
        )

    assert result is True
    _, kwargs = mock_client.chat.completions.create.call_args
    assert kwargs["model"] == "gpt-5.4-nano"


# ---------------------------------------------------------------------------
# 9. _render_card() article_summary rendering
# ---------------------------------------------------------------------------

from delivery_engine import (
    _render_card,
    _get_openai as _delivery_get_openai,
    OPENAI_MODEL as _DELIVERY_MODEL,
    _group_for_thematic,
    _collect_thin_entries,
    _collect_peripheral,
    synthesize_thematic_paragraphs,
    _render_peripheral_section,
    _render_thematic_section,
    generate_html_email,
    _config_int,
)


def test_render_card_omits_article_summary():
    """article_summary must never appear in card HTML regardless of content."""
    item = {
        "headline": "Test Headline",
        "source_url": "https://news.com/article",
        "americhem_impact": "Some impact.",
        "category": "competitors",
        "sentiment_score": 5,
        "source_publication": "Reuters",
        "sentiment_rationale": "Neutral article.",
        "recommended_action": "Monitor",
        "article_summary": "BASF announced a new plant in Germany.",
    }
    html = _render_card(item, accent="#1B3A6B", bg="#E8EDF5", text="#1B3A6B")
    assert "BASF announced a new plant in Germany." not in html


# ---------------------------------------------------------------------------
# 10. send_email() HTTP retry behaviour
# ---------------------------------------------------------------------------

import time as _time

import requests as _requests

from delivery_engine import send_email as _send_email


def _email_env(monkeypatch) -> None:
    monkeypatch.setenv("SMTP_PASS", "re_test_key")
    monkeypatch.setenv("SENDER_EMAIL", "noreply@test.com")
    monkeypatch.setenv("RECIPIENT_EMAILS", "user@test.com")


def test_send_email_retries_on_429_then_succeeds(monkeypatch):
    _email_env(monkeypatch)
    monkeypatch.setattr(_time, "sleep", lambda s: None)

    attempt = {"count": 0}

    def fake_post(*args, **kwargs):
        attempt["count"] += 1
        resp = MagicMock()
        if attempt["count"] == 1:
            resp.status_code = 429
            resp.raise_for_status = MagicMock()
        else:
            resp.status_code = 200
            resp.raise_for_status = MagicMock()
        return resp

    monkeypatch.setattr(_requests, "post", fake_post)
    _send_email("<html>test</html>")
    assert attempt["count"] == 2


def test_send_email_raises_immediately_on_auth_failure(monkeypatch):
    _email_env(monkeypatch)
    monkeypatch.setattr(_time, "sleep", lambda s: None)

    attempt = {"count": 0}

    def fake_post(*args, **kwargs):
        attempt["count"] += 1
        resp = MagicMock()
        resp.status_code = 403
        resp.ok = False
        http_err = _requests.HTTPError()
        http_err.response = resp
        resp.raise_for_status = MagicMock(side_effect=http_err)
        return resp

    monkeypatch.setattr(_requests, "post", fake_post)

    with pytest.raises(_requests.HTTPError):
        _send_email("<html>test</html>")

    assert attempt["count"] == 1  # must NOT have retried


# ---------------------------------------------------------------------------
# 11. _TextExtractor — visible text extraction
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# 12. _scrape_fallback — direct-HTTP fallback
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# 13. scrape_article — 402 triggers fallback
# ---------------------------------------------------------------------------

def _make_http_error(status_code: int) -> MagicMock:
    """Return a requests.HTTPError mock with the given status code."""
    import requests as _req
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    err = _req.exceptions.HTTPError(response=mock_resp)
    return err


def test_scrape_article_uses_fallback_on_402(monkeypatch):
    """scrape_article must invoke the fallback when Firecrawl returns HTTP 402."""
    import requests as _req

    # Firecrawl returns 402
    firecrawl_resp = MagicMock()
    firecrawl_resp.raise_for_status.side_effect = _make_http_error(402)

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
    import requests as _req

    firecrawl_resp = MagicMock()
    firecrawl_resp.raise_for_status.side_effect = _make_http_error(402)

    monkeypatch.setenv("FIRECRAWL_API_KEY", "test_key")

    with patch("ingestion_engine.requests.post", return_value=firecrawl_resp), \
         patch("ingestion_engine._scrape_fallback", return_value="too short"):
        result = scrape_article("https://example.com/article", min_length=500)

    assert result is None


def test_scrape_article_returns_none_when_fallback_fails(monkeypatch):
    """scrape_article must return None when both Firecrawl (402) and fallback fail."""
    import requests as _req

    firecrawl_resp = MagicMock()
    firecrawl_resp.raise_for_status.side_effect = _make_http_error(402)

    monkeypatch.setenv("FIRECRAWL_API_KEY", "test_key")

    with patch("ingestion_engine.requests.post", return_value=firecrawl_resp), \
         patch("ingestion_engine._scrape_fallback", return_value=None):
        result = scrape_article("https://example.com/article", min_length=500)

    assert result is None


def test_scrape_article_no_fallback_on_non_402_error(monkeypatch):
    """scrape_article must NOT invoke the fallback for non-402 Firecrawl errors."""
    import requests as _req

    firecrawl_resp = MagicMock()
    firecrawl_resp.raise_for_status.side_effect = _make_http_error(500)

    monkeypatch.setenv("FIRECRAWL_API_KEY", "test_key")

    with patch("ingestion_engine.requests.post", return_value=firecrawl_resp), \
         patch("ingestion_engine._scrape_fallback") as mock_fallback:
        result = scrape_article("https://example.com/article", min_length=500)

    mock_fallback.assert_not_called()
    assert result is None


# ---------------------------------------------------------------------------
# 14. _render_card() — "Monitor" action suppression
# ---------------------------------------------------------------------------

def test_render_card_suppresses_monitor_action():
    """Cards with recommended_action='Monitor' must NOT render the ACTION line."""
    item = {
        "headline": "Test Headline",
        "source_url": "https://news.com/article",
        "americhem_impact": "Some impact.",
        "category": "competitors",
        "sentiment_score": 5,
        "source_publication": "Reuters",
        "sentiment_rationale": "Neutral article.",
        "recommended_action": "Monitor",
        "article_summary": "",
    }
    html = _render_card(item, accent="#1B3A6B", bg="#E8EDF5", text="#1B3A6B")
    assert "&#9654; ACTION:" not in html


def test_render_card_shows_escalation_action():
    """Cards with recommended_action='Escalate to leadership' MUST render the ACTION line."""
    item = {
        "headline": "Plant fire halts BASF production",
        "source_url": "https://news.com/article",
        "americhem_impact": "Direct feedstock disruption risk.",
        "category": "suppliers",
        "sentiment_score": 2,
        "source_publication": "Reuters",
        "sentiment_rationale": "Severe supply disruption.",
        "recommended_action": "Escalate to leadership",
        "article_summary": "",
    }
    html = _render_card(item, accent="#EF4444", bg="#FEF2F2", text="#B91C1C")
    assert "&#9654; ACTION:" in html
    assert "Escalate to leadership" in html


# ---------------------------------------------------------------------------
# 15. generate_macro_summary()
# ---------------------------------------------------------------------------

from ingestion_engine import generate_macro_summary


def test_generate_macro_summary_empty_articles():
    """Should return False immediately when no articles are provided."""
    result = generate_macro_summary([])
    assert result is False


def test_generate_macro_summary_success():
    """Should call OpenAI, parse response, upsert to daily_summaries, return True."""
    articles = [
        {
            "headline": "Polymer prices surge",
            "category": "markets",
            "sentiment_score": 2,
            "americhem_impact": "Cost pressure on compounding margins.",
        }
    ]

    mock_completion = MagicMock()
    mock_completion.choices[0].message.content = json.dumps({
        "executive_summary": "Polymer prices are surging.",
        "macro_sentiment": "Bearish",
    })

    mock_openai = MagicMock()
    mock_openai.chat.completions.create.return_value = mock_completion

    mock_supabase = MagicMock()
    mock_supabase.table.return_value.upsert.return_value.execute.return_value = MagicMock()

    with patch("ingestion_engine._get_openai", return_value=mock_openai), \
         patch("ingestion_engine._get_supabase", return_value=mock_supabase):
        result = generate_macro_summary(articles)

    assert result is True
    mock_supabase.table.assert_called_with("daily_summaries")
    call_kwargs = mock_supabase.table.return_value.upsert.call_args[0][0]
    assert call_kwargs["executive_summary"] == "Polymer prices are surging."
    assert call_kwargs["macro_sentiment"] == "Bearish"
    assert "run_date" in call_kwargs


# ---------------------------------------------------------------------------
# 16. _render_card() — article_summary must not appear in rendered HTML
# ---------------------------------------------------------------------------

def test_render_card_excludes_article_summary():
    """article_summary must not appear in rendered card HTML."""
    item = {
        "headline": "Test headline",
        "source_url": "https://example.com",
        "americhem_impact": "Some impact.",
        "category": "markets",
        "sentiment_score": 5,
        "article_summary": "This is the article summary text.",
    }
    html = _render_card(item, "#000000", "#ffffff", "#000000")
    assert "This is the article summary text." not in html


# ---------------------------------------------------------------------------
# 17. build_query()
# ---------------------------------------------------------------------------

def test_build_query_entity_mode_bare():
    """Entity mode with no include_all or exclude_any produces a quoted name."""
    result = build_query("entity", name="Shaw Industries")
    assert result == '"Shaw Industries"'


def test_build_query_entity_mode_with_excludes():
    """Entity mode exclude_any terms become -\"term\" operators."""
    result = build_query(
        "entity",
        name="Shaw Industries",
        include_all=[],
        exclude_any=["patents", "securities analyst reports"],
    )
    assert '"Shaw Industries"' in result
    assert '-"patents"' in result
    assert '-"securities analyst reports"' in result


def test_build_query_concept_mode():
    """Concept mode ORs all include_any terms and ANDs include_all."""
    result = build_query(
        "concept",
        include_any=["plastics industry", "chemical industry", "compounding"],
        include_all=["business"],
        exclude_any=[],
    )
    assert '("plastics industry" OR "chemical industry" OR "compounding")' in result
    assert '"business"' in result


def test_build_query_filters_moody_internal_excludes():
    """Moody's platform identifiers in exclude_any must be silently dropped."""
    result = build_query(
        "concept",
        include_any=["plastics industry"],
        include_all=[],
        exclude_any=["source set 238658", "PR wires", "Targeted News Search", "tenders"],
    )
    assert "source set 238658" not in result
    assert "PR wires" not in result
    assert "Targeted News Search" not in result
    assert '-"tenders"' in result   # real term must survive


def test_build_query_concept_mode_no_include_all():
    """Concept mode with empty include_all produces no spurious quoted terms."""
    result = build_query(
        "concept",
        include_any=["automotive industry"],
        include_all=[],
        exclude_any=[],
    )
    assert result == '("automotive industry")'


# ===========================================================================
# Thematic synthesis helpers — shared fixture
# ===========================================================================

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


# ---------------------------------------------------------------------------
# Task 2 — _group_for_thematic
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Task 3 — _collect_thin_entries, _collect_peripheral
# ---------------------------------------------------------------------------

def test_collect_thin_entries_single_high_score():
    """Single-article score 7–10 not in any group goes to thin entries."""
    items = [_make_article("solo", 8, "customers")]
    thin = _collect_thin_entries(items, {})
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


# ---------------------------------------------------------------------------
# Task 4 — synthesize_thematic_paragraphs
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Task 5 — _render_peripheral_section
# ---------------------------------------------------------------------------

def test_render_peripheral_section_empty_returns_empty_string():
    """Empty items list must return empty string (no section rendered)."""
    assert _render_peripheral_section([]) == ""


def test_render_peripheral_section_correct_bullet_count():
    """Each item produces exactly one headline in the HTML output."""
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
    """Each headline must reference the source_url."""
    items = [_make_article("a", 5, "markets", "Linked Headline")]
    html = _render_peripheral_section(items)
    assert "https://news.com/article" in html
    assert "Linked Headline" in html


# ---------------------------------------------------------------------------
# Task 6 — _render_thematic_section
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Task 7 — generate_html_email integration
# ---------------------------------------------------------------------------

def test_generate_html_email_all_critical_no_thematic_section(monkeypatch):
    """When all articles score 1–3, Thematic Intelligence must not appear."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    data = [
        {**_make_article(f"h{i}", 2, "suppliers", f"Critical Headline {i}")}
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
        _make_article("a", 7, "competitors", "Avient Headline"),
        _make_article("b", 8, "competitors", "Techmer Headline"),
    ]
    with patch("delivery_engine._get_openai", return_value=mock_client):
        html = generate_html_email(data)
    assert "THEMATIC INTELLIGENCE" in html
    assert "Synthesis paragraph here." in html


def test_generate_html_email_routes_single_low_to_peripheral(monkeypatch):
    """Single score 4–6 article goes to Peripheral Signals, not Thematic."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    data = [_make_article("x", 5, "markets", "Peripheral Headline")]
    with patch("delivery_engine._get_openai", return_value=MagicMock()):
        html = generate_html_email(data)
    assert "PERIPHERAL SIGNALS" in html
    assert "Peripheral Headline" in html
    assert "THEMATIC INTELLIGENCE" not in html


# ---------------------------------------------------------------------------
# Pipeline deadline early-exit
# ---------------------------------------------------------------------------

from ingestion_engine import execute_pipeline


def test_execute_pipeline_deadline_calls_log_stats_and_macro_summary(monkeypatch, tmp_path):
    """When the pipeline deadline is exceeded mid-batch, _log_stats and
    generate_macro_summary must be called before the function returns."""
    import textwrap
    import ingestion_engine

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
        # return a value past the deadline so the mid-batch check fires.
        if call_count["n"] == 1:
            return 0.0
        return float(ingestion_engine.PIPELINE_DEADLINE_SECONDS + 1)

    monkeypatch.setattr(ingestion_engine.time, "monotonic", fake_monotonic)

    # Provide one discovered URL so the inner loop is entered
    monkeypatch.setattr(
        ingestion_engine,
        "discover_urls",
        lambda *a, **kw: [("https://example.com/article", "Test Title")],
    )

    mock_log_stats = MagicMock()
    mock_macro = MagicMock(return_value=True)
    monkeypatch.setattr(ingestion_engine, "_log_stats", mock_log_stats)
    monkeypatch.setattr(ingestion_engine, "generate_macro_summary", mock_macro)
    monkeypatch.setattr(ingestion_engine, "_hydrate_seen_headlines", lambda: set())

    # Run from the tmp targets file
    monkeypatch.chdir(tmp_path)

    execute_pipeline()

    mock_log_stats.assert_called_once()
    mock_macro.assert_called_once()


# ===========================================================================
# Relevance upgrade — new field validation in synthesize_insight()
# ===========================================================================

def _make_openai_mock_with_fields(**overrides) -> MagicMock:
    """Return an OpenAI mock that outputs a minimal valid insight plus overrides."""
    base = {
        "headline": "Test Headline",
        "americhem_impact": "Direct impact on compounding margins.",
        "sentiment_score": 5,
        "sentiment_tag": "Neutral",
        "americhem_impact_score": 7,
        "impact_rationale": "Directly affects masterbatch feedstock cost.",
        "strategic_segment": "Raw Materials / Supply Chain",
        "source_url": "https://news.com/article",
        "entities_mentioned": ["Avient"],
    }
    base.update(overrides)
    content = json.dumps(base)
    mock_message = MagicMock()
    mock_message.content = content
    mock_choice = MagicMock()
    mock_choice.message = mock_message
    mock_completion = MagicMock()
    mock_completion.choices = [mock_choice]
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = mock_completion
    return mock_client


# ---------------------------------------------------------------------------
# Sentiment tag validation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_tag", ["NEGATIVE", "negative", "Bad", "", None, 42])
def test_synthesize_insight_defaults_invalid_sentiment_tag(bad_tag):
    """Any invalid sentiment_tag must be replaced with 'Neutral'."""
    mock_client = _make_openai_mock_with_fields(sentiment_tag=bad_tag)
    with patch("ingestion_engine._get_openai", return_value=mock_client):
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
    mock_client = _make_openai_mock_with_fields(sentiment_tag=valid_tag)
    with patch("ingestion_engine._get_openai", return_value=mock_client):
        result = synthesize_insight(
            article_text="Article text.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["sentiment_tag"] == valid_tag


# ---------------------------------------------------------------------------
# americhem_impact_score clamping
# ---------------------------------------------------------------------------

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
    mock_client = _make_openai_mock_with_fields(americhem_impact_score=raw_impact)
    with patch("ingestion_engine._get_openai", return_value=mock_client):
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
    mock_client = _make_openai_mock_with_fields(americhem_impact_score=bad_value)
    with patch("ingestion_engine._get_openai", return_value=mock_client):
        result = synthesize_insight(
            article_text="Article text.",
            source_url="https://news.com/article",
            trigger_entity="Avient",
            category="competitors",
        )
    assert result is not None
    assert result["americhem_impact_score"] == 5



# ---------------------------------------------------------------------------
# Threshold filtering in generate_html_email()
# ---------------------------------------------------------------------------

def _make_new_article(
    url_hash: str,
    americhem_impact_score: int,
    strategic_segment: str = "Raw Materials / Supply Chain",
    sentiment_tag: str = "Neutral",
    headline: str = "Test Headline",
) -> dict:
    """Build a fully-populated new-style article with all relevance fields."""
    return {
        "url_hash": url_hash,
        "americhem_impact_score": americhem_impact_score,
        "sentiment_tag": sentiment_tag,
        "impact_rationale": "Direct feedstock cost effect.",
        "strategic_segment": strategic_segment,
        "headline": headline,
        "americhem_impact": "Some impact.",
        "entities_mentioned": ["TestCorp"],
        "source_url": "https://news.com/article",
        "category": "markets",
        # No sentinel_score — new-style row
    }


def test_generate_html_email_filters_below_impact_threshold(monkeypatch):
    """Articles with americhem_impact_score below the threshold must not appear in the email."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    low_impact = _make_new_article("low", americhem_impact_score=3, headline="Low Impact Headline")
    high_impact = _make_new_article("high", americhem_impact_score=8, headline="High Impact Headline")

    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email([low_impact, high_impact])

    assert "High Impact Headline" in html
    assert "Low Impact Headline" not in html


def test_generate_html_email_groups_by_strategic_segment(monkeypatch):
    """Two new-style articles with the same strategic_segment are grouped under that label."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    art_a = _make_new_article("a", 8, strategic_segment="Healthcare", headline="Healthcare Headline A")
    art_b = _make_new_article("b", 7, strategic_segment="Healthcare", headline="Healthcare Headline B")

    mock_client = _make_synthesis_mock({"Healthcare": "Healthcare synthesis paragraph."})
    with patch("delivery_engine._get_openai", return_value=mock_client), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email([art_a, art_b])

    assert "HEALTHCARE" in html.upper()
    assert "Healthcare synthesis paragraph." in html


# ---------------------------------------------------------------------------
# _render_card() — sentiment_tag and americhem_impact_score display
# ---------------------------------------------------------------------------

def test_render_card_shows_impact_score_and_sentiment_tag():
    """When americhem_impact_score and sentiment_tag are present, the card shows
    'Impact: X/10' and the tag label, NOT the old 'Score: X/10' format."""
    item = {
        "headline": "Plant closure disrupts supply",
        "source_url": "https://news.com/article",
        "americhem_impact": "Feedstock shortfall for masterbatch lines.",
        "americhem_impact_score": 8,
        "sentiment_tag": "Negative",
        "impact_rationale": "Direct feedstock cost increase.",
        "strategic_segment": "Raw Materials / Supply Chain",
        "source_publication": "Chemical Week",
        "recommended_action": "Flag to procurement",
        "category": "markets",
    }
    html = _render_card(item, accent="#EF4444", bg="#FEF2F2", text="#B91C1C")
    assert "Impact: 8/10" in html
    assert "Negative" in html
    assert "Score:" not in html


def test_render_card_falls_back_to_sentiment_score_for_old_rows():
    """Old-style rows without new fields must render the legacy 'Score: X/10' display."""
    item = {
        "headline": "Old Article",
        "source_url": "https://news.com/article",
        "americhem_impact": "Some impact.",
        "sentiment_score": 6,
        "source_publication": "Reuters",
        "recommended_action": "Monitor",
        "category": "markets",
    }
    html = _render_card(item, accent="#1B3A6B", bg="#E8EDF5", text="#1B3A6B")
    assert "Score: 6/10" in html
    assert "Impact:" not in html


# ===========================================================================
# Article cap enforcement
# ===========================================================================

def test_generate_html_email_per_segment_cap(monkeypatch):
    """No more than max_per_segment articles from the same segment appear in the email."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    # 5 Healthcare articles, impacts 10 down to 6 — all above threshold
    articles = [
        _make_new_article(
            f"h{i}", americhem_impact_score=10 - i,
            strategic_segment="Healthcare",
            headline=f"HC Headline {i}",
        )
        for i in range(5)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 3,
            "max_total_visible_articles": 12,
        }
    }
    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value=config):
        html = generate_html_email(articles)

    # Top 3 by impact score (h0=10, h1=9, h2=8) must appear
    assert "HC Headline 0" in html
    assert "HC Headline 1" in html
    assert "HC Headline 2" in html
    # h3 (impact 7) and h4 (impact 6) must be excluded
    assert "HC Headline 3" not in html
    assert "HC Headline 4" not in html


def test_generate_html_email_total_articles_cap(monkeypatch):
    """Total visible articles must not exceed max_total_visible_articles."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    # 7 segments × 2 articles = 14 articles, all impact=8
    segments = [
        "Healthcare", "Fibers", "Packaging", "Industrial",
        "Raw Materials / Supply Chain", "Regulatory / Sustainability",
        "Competitive / Customer Signal",
    ]
    articles = [
        _make_new_article(
            f"s{si}_{ai}", americhem_impact_score=8,
            strategic_segment=seg,
            headline=f"Seg{si} Art{ai}",
        )
        for si, seg in enumerate(segments)
        for ai in range(2)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 3,
            "max_total_visible_articles": 10,
        }
    }
    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value=config):
        html = generate_html_email(articles)

    visible_count = sum(1 for art in articles if art["headline"] in html)
    assert visible_count <= 10


def test_generate_html_email_capped_articles_do_not_reappear(monkeypatch):
    """Articles dropped by the per-segment cap must not reappear in thin entries or peripheral."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    # 4 Healthcare articles, impacts 10, 9, 8, 7 — max_per_segment=3 drops impact-7
    articles = [
        _make_new_article(
            f"h{i}", americhem_impact_score=10 - i,
            strategic_segment="Healthcare",
            headline=f"HC Headline {i}",
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
    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value=config):
        html = generate_html_email(articles)

    # h3 (impact=7) was in the group but capped — must not reappear anywhere
    assert "HC Headline 3" not in html


# ===========================================================================
# Negative moderate-impact: impact score drives filtering, not sentiment tone
# ===========================================================================

def test_generate_html_email_excludes_negative_low_impact_new_style(monkeypatch):
    """A Negative-sentiment article with low americhem_impact_score must be excluded.
    Filtering is by impact score, not tone — this validates the invariant."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    neg_low = _make_new_article(
        "neg_low", americhem_impact_score=4,
        sentiment_tag="Negative",
        headline="Negative Low Impact Headline",
    )
    pos_high = _make_new_article(
        "pos_high", americhem_impact_score=8,
        sentiment_tag="Positive",
        headline="Positive High Impact Headline",
    )
    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email([neg_low, pos_high])

    assert "Positive High Impact Headline" in html
    assert "Negative Low Impact Headline" not in html


def test_generate_html_email_shows_negative_high_impact(monkeypatch):
    """A Negative-sentiment article with high americhem_impact_score MUST appear.
    A high-impact supply disruption (Negative) is more important than a positive routine signal."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    neg_high = _make_new_article(
        "neg_high", americhem_impact_score=9,
        sentiment_tag="Negative",
        strategic_segment="Raw Materials / Supply Chain",
        headline="Negative High Impact Supply Disruption",
    )
    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email([neg_high])

    assert "Negative High Impact Supply Disruption" in html
    assert "Negative" in html


# ---------------------------------------------------------------------------
# _config_int coercion
# ---------------------------------------------------------------------------

def test_config_int_returns_int_for_numeric_value():
    cfg = {"visible_impact_threshold": 7}
    assert _config_int(cfg, "visible_impact_threshold", 6) == 7


def test_config_int_coerces_string_to_int():
    """YAML authors may quote numbers; ensure we still get an int."""
    cfg = {"visible_impact_threshold": "8"}
    assert _config_int(cfg, "visible_impact_threshold", 6) == 8


def test_config_int_returns_default_for_missing_key():
    assert _config_int({}, "visible_impact_threshold", 6) == 6


def test_config_int_returns_default_and_warns_for_bad_value(caplog):
    import logging
    cfg = {"visible_impact_threshold": "high"}
    with caplog.at_level(logging.WARNING, logger="delivery_engine"):
        result = _config_int(cfg, "visible_impact_threshold", 6)
    assert result == 6
    assert "visible_impact_threshold" in caplog.text


# ===========================================================================
# MARKET_PULSE_RUN_MODE — test-mode markings
# ===========================================================================

def test_send_email_test_mode_prefixes_subject(monkeypatch):
    """In test mode, the Resend payload subject must start with '[TEST] '."""
    _email_env(monkeypatch)
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    monkeypatch.setattr(_time, "sleep", lambda s: None)

    captured = {}
    def fake_post(*args, **kwargs):
        captured["payload"] = kwargs["json"]
        resp = MagicMock(); resp.status_code = 200; resp.ok = True
        resp.raise_for_status = MagicMock()
        return resp

    monkeypatch.setattr(_requests, "post", fake_post)
    _send_email("<html>x</html>")
    assert captured["payload"]["subject"].startswith("[TEST] ")


def test_send_email_production_mode_subject_unchanged(monkeypatch):
    """When MARKET_PULSE_RUN_MODE is unset, the subject must have no [TEST] prefix."""
    _email_env(monkeypatch)
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    monkeypatch.setattr(_time, "sleep", lambda s: None)

    captured = {}
    def fake_post(*args, **kwargs):
        captured["payload"] = kwargs["json"]
        resp = MagicMock(); resp.status_code = 200; resp.ok = True
        resp.raise_for_status = MagicMock()
        return resp

    monkeypatch.setattr(_requests, "post", fake_post)
    _send_email("<html>x</html>")
    assert "[TEST]" not in captured["payload"]["subject"]


def test_send_email_recipient_list_is_only_recipient_emails_env(monkeypatch):
    """Recipient invariant: send_email() builds the Resend 'to' list strictly from the
    RECIPIENT_EMAILS env var and never falls back to any hardcoded address. This is
    the safety guarantee that lets the workflow swap recipient pools by env var alone.
    """
    monkeypatch.setenv("SMTP_PASS", "re_test_key")
    monkeypatch.setenv("SENDER_EMAIL", "noreply@test.com")
    monkeypatch.setenv("RECIPIENT_EMAILS", "jphifer@americhem.com")
    monkeypatch.setattr(_time, "sleep", lambda s: None)

    captured = {}
    def fake_post(*args, **kwargs):
        captured["payload"] = kwargs["json"]
        resp = MagicMock(); resp.status_code = 200; resp.ok = True
        resp.raise_for_status = MagicMock()
        return resp

    monkeypatch.setattr(_requests, "post", fake_post)
    _send_email("<html>x</html>")
    assert captured["payload"]["to"] == ["jphifer@americhem.com"]


def test_generate_html_email_test_mode_prefixes_header(monkeypatch):
    """In test mode, generate_html_email() must include [TEST] in the title and
    a visible TEST RUN banner in the rendered HTML."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    data = [_make_new_article("h", 8, headline="Some Headline")]
    with patch("delivery_engine._get_openai", return_value=MagicMock()):
        html = generate_html_email(data)
    assert "[TEST]" in html
    assert "TEST RUN" in html
    assert "Jason-only QA output" in html


def test_generate_html_email_production_mode_unchanged(monkeypatch):
    """When MARKET_PULSE_RUN_MODE is unset, the rendered HTML must contain
    no [TEST] markers or TEST RUN banner."""
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    data = [_make_new_article("h", 8, headline="Some Headline")]
    with patch("delivery_engine._get_openai", return_value=MagicMock()):
        html = generate_html_email(data)
    assert "[TEST]" not in html
    assert "TEST RUN" not in html


def test_no_news_email_test_mode_marks_header(monkeypatch):
    """The no-news fallback HTML must carry [TEST] and the TEST RUN banner in test mode."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    from delivery_engine import _generate_no_news_email
    html = _generate_no_news_email()
    assert "[TEST]" in html
    assert "TEST RUN" in html


def _make_openai_mock_with_new_fields(**overrides) -> MagicMock:
    """OpenAI mock that returns the new-style per-article payload."""
    base = {
        "headline": "Test Headline",
        "americhem_impact": "Direct effect on compounding margin.",
        "sentiment_score": 5,
        "sentiment_tag": "Neutral",
        "americhem_impact_score": 7,
        "impact_rationale": "Direct feedstock cost effect.",
        "commercial_segment": "Healthcare",
        "signal_type": "Technology",
        "source_url": "https://news.com/article",
        "entities_mentioned": ["Avient"],
    }
    base.update(overrides)
    msg = MagicMock(); msg.content = json.dumps(base)
    choice = MagicMock(); choice.message = msg
    completion = MagicMock(); completion.choices = [choice]
    client = MagicMock()
    client.chat.completions.create.return_value = completion
    return client


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
    mock = _make_openai_mock_with_new_fields(commercial_segment=valid_segment)
    with patch("ingestion_engine._get_openai", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert result["commercial_segment"] == valid_segment


@pytest.mark.parametrize("bad_segment", [None, "", "  ", "NotASegment", 42])
def test_synthesize_insight_defaults_invalid_commercial_segment(bad_segment):
    mock = _make_openai_mock_with_new_fields(commercial_segment=bad_segment)
    with patch("ingestion_engine._get_openai", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert result["commercial_segment"] == "Enterprise / Cross-Segment"


@pytest.mark.parametrize(
    "valid_signal",
    ["Competitive", "Customer", "Regulatory", "Sustainability",
     "Supply Chain", "Technology", "Macro", "Other"],
)
def test_synthesize_insight_preserves_valid_signal_type(valid_signal):
    mock = _make_openai_mock_with_new_fields(signal_type=valid_signal)
    with patch("ingestion_engine._get_openai", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert result["signal_type"] == valid_signal


@pytest.mark.parametrize("bad_signal", [None, "", "BAD", 42])
def test_synthesize_insight_defaults_invalid_signal_type(bad_signal):
    mock = _make_openai_mock_with_new_fields(signal_type=bad_signal)
    with patch("ingestion_engine._get_openai", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert result["signal_type"] == "Other"


def test_synthesize_insight_drops_strategic_segment_field():
    """If the LLM still returns strategic_segment, it must not appear in the result."""
    mock = _make_openai_mock_with_new_fields(strategic_segment="LegacyValue")
    with patch("ingestion_engine._get_openai", return_value=mock):
        result = synthesize_insight("text", "https://news.com/a", "Avient", "competitors")
    assert result is not None
    assert "strategic_segment" not in result


def test_build_commercial_segment_rule_injects_labels_and_descriptions():
    """_build_commercial_segment_rule must include all 9 labels and their full
    descriptions from config."""
    from ingestion_engine import _build_commercial_segment_rule
    cfg = {
        "commercial_segments": {
            "healthcare": {"label": "Healthcare", "description": "Med devices."},
            "fibers": {"label": "Fibers", "description": "Synthetic fiber chains."},
        }
    }
    rule_text = _build_commercial_segment_rule(cfg)
    assert "RULE 4 — COMMERCIAL SEGMENT" in rule_text
    assert "Healthcare" in rule_text
    assert "Med devices." in rule_text
    assert "Fibers" in rule_text
    assert "Synthetic fiber chains." in rule_text


def test_build_signal_type_rule_injects_labels_and_descriptions():
    """_build_signal_type_rule must include all 8 labels and descriptions."""
    from ingestion_engine import _build_signal_type_rule
    cfg = {
        "signal_types": {
            "competitive": {"label": "Competitive", "description": "Comp moves."},
            "regulatory": {"label": "Regulatory", "description": "Gov actions."},
        }
    }
    rule_text = _build_signal_type_rule(cfg)
    assert "RULE 5 — SIGNAL TYPE" in rule_text
    assert "Competitive" in rule_text
    assert "Comp moves." in rule_text
    assert "Regulatory" in rule_text
    assert "Gov actions." in rule_text


def test_system_prompt_includes_both_segment_and_signal_rules():
    """The assembled system prompt must contain both new rules with their
    descriptions, not just the labels."""
    from ingestion_engine import _build_system_prompt
    cfg = {
        "commercial_segments": {
            "engineered_resins": {
                "label": "Engineered Resins",
                "description": "High-performance compounds.",
            },
        },
        "signal_types": {
            "supply_chain": {
                "label": "Supply Chain",
                "description": "Resin pricing, force majeure.",
            },
        },
    }
    prompt = _build_system_prompt(cfg)
    assert "RULE 4 — COMMERCIAL SEGMENT" in prompt
    assert "RULE 5 — SIGNAL TYPE" in prompt
    assert "Engineered Resins" in prompt
    assert "High-performance compounds." in prompt
    assert "Supply Chain" in prompt
    assert "Resin pricing, force majeure." in prompt
    assert "seven rules" in prompt


def test_config_has_commercial_segments_and_signal_types():
    """market_pulse_config.yaml must expose the new commercial_segments,
    signal_types, macro_conditions, executive_bullet_labels, and
    delivery_suppression blocks with the expected labels."""
    import yaml
    with open("market_pulse_config.yaml") as fh:
        cfg = yaml.safe_load(fh)

    segments = {s["label"] for s in cfg["commercial_segments"].values()}
    assert segments == {
        "Healthcare", "Fibers",
        "Transportation - Automotive", "Transportation - Non-Automotive",
        "Transportation - Aerospace",
        "Industrial", "Packaging", "Engineered Resins",
        "Enterprise / Cross-Segment",
    }

    signals = {s["label"] for s in cfg["signal_types"].values()}
    assert signals == {
        "Competitive", "Customer", "Regulatory", "Sustainability",
        "Supply Chain", "Technology", "Macro", "Other",
    }

    assert cfg["macro_conditions"] == [
        "Competitive Pressure", "Supply Volatility", "Demand Expansion",
        "Demand Softness", "Regulatory Pressure", "Sustainability Pull",
        "Commercial Opportunity", "Mixed / Watch", "Low Signal",
    ]

    assert cfg["executive_bullet_labels"] == [
        "Market pressure", "Supply chain watch", "Commercial action",
    ]

    sup = cfg["delivery_suppression"]
    assert sup["enable_duplicate_headline"] is True
    assert sup["headline_duplicate_threshold"] == 90
    assert sup["enterprise_min_impact"] == 7
    assert "linkedin.com/jobs" in sup["url_patterns_job_posting"]
    assert "market size" in sup["title_patterns_generic_market_report"]
    assert "masterbatch" in sup["plastics_relevance_terms"]
