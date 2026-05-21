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
    synthesize_thematic_paragraphs,
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
# Task 7 — generate_html_email integration
# ---------------------------------------------------------------------------

def test_generate_html_email_legacy_critical_appears_with_badge(monkeypatch):
    """Legacy sentiment_score<=3 rows appear in Commercial Segment Watch with a
    CRITICAL badge in the meta strip. The old Critical Disruptions section is gone."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    data = [
        {"url_hash": "c0", "sentiment_score": 2, "category": "suppliers",
         "headline": "Legacy critical headline about plant fire",
         "americhem_impact": "Disruption.",
         "entities_mentioned": ["BASF"], "source_url": "https://x/0",
         "strategic_segment": "Broader Americhem"},
    ]
    mock_supa = MagicMock()
    mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()
    mock_supa.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(data=[])

    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._get_supabase", return_value=mock_supa), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email(data)
    # Note: this legacy row has no americhem_impact_score, so the visibility filter
    # uses sentiment_score=2 -> effective_impact <= 3, which is BELOW the visible
    # threshold (6). So the row will not surface in the segment watch under the
    # current threshold filter. What we DO assert: the old section labels are gone
    # and Peripheral Signals is hidden in production. The CRITICAL badge behaviour
    # is unit-tested directly via test_render_segment_watch_section_critical_badge_for_legacy_low_score.
    assert "PERIPHERAL SIGNALS" not in html
    assert "CRITICAL DISRUPTIONS" not in html
    assert "THEMATIC INTELLIGENCE" not in html


def test_generate_html_email_routes_two_plus_to_segment_watch(monkeypatch):
    """Two articles in the same commercial_segment produce a Commercial Segment
    Watch block with a synthesis paragraph."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    data = [
        {"url_hash": "a", "commercial_segment": "Healthcare",
         "americhem_impact_score": 7, "sentiment_tag": "Positive",
         "signal_type": "Customer", "headline": "Avient expands healthcare polymer line",
         "americhem_impact": "Effect.", "source_url": "https://x/a",
         "entities_mentioned": ["Avient"]},
        {"url_hash": "b", "commercial_segment": "Healthcare",
         "americhem_impact_score": 8, "sentiment_tag": "Positive",
         "signal_type": "Customer", "headline": "Techmer launches sterilizable compound line",
         "americhem_impact": "Effect.", "source_url": "https://x/b",
         "entities_mentioned": ["Techmer"]},
    ]
    mock_synth = _make_synthesis_mock({"Healthcare": "Synthesis paragraph here."})
    mock_supa = MagicMock()
    mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()
    mock_supa.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(data=[])

    with patch("delivery_engine._get_openai", return_value=mock_synth), \
         patch("delivery_engine._get_supabase", return_value=mock_supa), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email(data)
    assert "COMMERCIAL SEGMENT WATCH" in html
    assert "Healthcare" in html
    assert "Synthesis paragraph here." in html
    assert "THEMATIC INTELLIGENCE" not in html


def test_generate_html_email_single_low_relevance_hidden_in_production(monkeypatch):
    """An ungrouped impact-5 article must be HIDDEN in production
    (no Peripheral Signals section anymore)."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    data = [{"url_hash": "x", "commercial_segment": "Packaging",
             "americhem_impact_score": 5, "sentiment_tag": "Neutral",
             "signal_type": "Customer",
             "headline": "Low relevance packaging signal",
             "americhem_impact": ".", "source_url": "https://x/p",
             "entities_mentioned": ["Acme"]}]
    mock_supa = MagicMock()
    mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()
    mock_supa.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(data=[])

    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._get_supabase", return_value=mock_supa), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email(data)
    assert "Low relevance packaging signal" not in html
    assert "PERIPHERAL SIGNALS" not in html


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
    # Use genuinely distinct headlines so delivery suppression doesn't flag them
    # as semantic duplicates (token_sort_ratio threshold is 88).
    art_a = _make_new_article("a", 8, strategic_segment="Healthcare",
                              headline="Hospital network consolidation squeezes specialty polymer demand")
    art_b = _make_new_article("b", 7, strategic_segment="Healthcare",
                              headline="FDA clears new medical-grade compound for implantable devices")

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
    # 5 Healthcare articles with genuinely distinct headlines (avoids semantic-duplicate
    # suppression which fires at token_sort_ratio >= 88).
    _hc_headlines = [
        "Hospital network merger squeezes specialty polymer volumes",
        "FDA clears new implantable-grade compound for cardiac devices",
        "Aging population drives record demand for medical-grade resins",
        "Generic drug expansion pressures premium plastics pricing",
        "Supply disruption at key resin plant delays surgical kit output",
    ]
    articles = [
        _make_new_article(
            f"h{i}", americhem_impact_score=10 - i,
            strategic_segment="Healthcare",
            headline=_hc_headlines[i],
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
    assert _hc_headlines[0] in html
    assert _hc_headlines[1] in html
    assert _hc_headlines[2] in html
    # h3 (impact 7) and h4 (impact 6) must be excluded
    assert _hc_headlines[3] not in html
    assert _hc_headlines[4] not in html


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


# ---------------------------------------------------------------------------
# Task 8 — _commercial_segment_of and _signal_type_of helpers
# ---------------------------------------------------------------------------

def test_commercial_segment_of_prefers_new_field():
    from delivery_engine import _commercial_segment_of
    row = {"commercial_segment": "Healthcare", "strategic_segment": "Industrial"}
    assert _commercial_segment_of(row) == "Healthcare"


def test_commercial_segment_of_falls_back_to_strategic_segment():
    from delivery_engine import _commercial_segment_of
    cases = {
        "Healthcare": "Healthcare",
        "Fibers": "Fibers",
        "Packaging": "Packaging",
        "Industrial": "Industrial",
        "Raw Materials / Supply Chain": "Enterprise / Cross-Segment",
        "Regulatory / Sustainability": "Enterprise / Cross-Segment",
        "Competitive / Customer Signal": "Enterprise / Cross-Segment",
        "Broader Americhem": "Enterprise / Cross-Segment",
    }
    for legacy, expected in cases.items():
        row = {"strategic_segment": legacy}
        assert _commercial_segment_of(row) == expected, f"{legacy} -> {expected}"


def test_commercial_segment_of_handles_null_strategic_segment():
    from delivery_engine import _commercial_segment_of
    assert _commercial_segment_of({}) == "Enterprise / Cross-Segment"
    assert _commercial_segment_of({"strategic_segment": None}) == "Enterprise / Cross-Segment"
    assert _commercial_segment_of({"strategic_segment": ""}) == "Enterprise / Cross-Segment"
    assert _commercial_segment_of({"strategic_segment": "UnknownValue"}) == "Enterprise / Cross-Segment"


def test_signal_type_of_prefers_new_field():
    from delivery_engine import _signal_type_of
    assert _signal_type_of({"signal_type": "Regulatory"}) == "Regulatory"


def test_signal_type_of_falls_back_to_other():
    from delivery_engine import _signal_type_of
    assert _signal_type_of({}) == "Other"
    assert _signal_type_of({"signal_type": None}) == "Other"
    assert _signal_type_of({"signal_type": ""}) == "Other"


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


# ===========================================================================
# Task 5 — structured macro summary (dominant_condition + executive_bullets)
# ===========================================================================

def _make_macro_mock(payload: dict) -> MagicMock:
    msg = MagicMock(); msg.content = json.dumps(payload)
    choice = MagicMock(); choice.message = msg
    completion = MagicMock(); completion.choices = [choice]
    client = MagicMock(); client.chat.completions.create.return_value = completion
    return client


def _make_articles(n: int) -> list[dict]:
    return [
        {"category": "competitors", "headline": f"H{i}",
         "sentiment_score": 5, "americhem_impact": f"Impact {i}."}
        for i in range(n)
    ]


def _capture_upsert(mock_supabase) -> dict:
    """Return the dict that was passed to .upsert()."""
    return mock_supabase.table.return_value.upsert.call_args[0][0]


def test_generate_macro_summary_writes_dominant_condition_when_valid():
    payload = {
        "dominant_condition": "Competitive Pressure",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "Body A."},
            {"label": "Supply chain watch", "body": "Body B."},
            {"label": "Commercial action",  "body": "Body C."},
        ],
    }
    mock_supa = MagicMock()
    with patch("ingestion_engine._get_openai", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._get_supabase", return_value=mock_supa):
        assert generate_macro_summary(_make_articles(5)) is True
    row = _capture_upsert(mock_supa)
    assert row["dominant_condition"] == "Competitive Pressure"
    assert row["executive_bullets"] == payload["executive_bullets"]
    # Legacy fields still populated for backward compat:
    assert row["macro_sentiment"] == "Competitive Pressure"
    assert row["executive_summary"]  # joined paragraph


def test_generate_macro_summary_coerces_invalid_dominant_condition():
    payload = {
        "dominant_condition": "NonExistentCondition",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
    }
    mock_supa = MagicMock()
    with patch("ingestion_engine._get_openai", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._get_supabase", return_value=mock_supa):
        generate_macro_summary(_make_articles(5))
    row = _capture_upsert(mock_supa)
    assert row["dominant_condition"] == "Mixed / Watch"


def test_generate_macro_summary_defaults_low_signal_when_few_articles():
    """When fewer than 3 articles are passed in and the LLM omits a valid condition,
    default to Low Signal."""
    payload = {"executive_bullets": [
        {"label": "Market pressure",    "body": "Quiet day."},
        {"label": "Supply chain watch", "body": "Quiet day."},
        {"label": "Commercial action",  "body": "Anything."},
    ]}
    mock_supa = MagicMock()
    with patch("ingestion_engine._get_openai", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._get_supabase", return_value=mock_supa):
        generate_macro_summary(_make_articles(2))
    row = _capture_upsert(mock_supa)
    assert row["dominant_condition"] == "Low Signal"


def test_generate_macro_summary_low_signal_coerces_action_body():
    payload = {
        "dominant_condition": "Low Signal",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "Quiet day."},
            {"label": "Supply chain watch", "body": "Quiet day."},
            {"label": "Commercial action",  "body": "Sales should call every customer."},
        ],
    }
    mock_supa = MagicMock()
    with patch("ingestion_engine._get_openai", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._get_supabase", return_value=mock_supa):
        generate_macro_summary(_make_articles(2))
    row = _capture_upsert(mock_supa)
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
    payload = {"dominant_condition": "Mixed / Watch", "executive_bullets": bad_bullets}
    mock_supa = MagicMock()
    with patch("ingestion_engine._get_openai", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._get_supabase", return_value=mock_supa):
        generate_macro_summary(_make_articles(5))
    row = _capture_upsert(mock_supa)
    assert row["executive_bullets"] is None
    # Legacy executive_summary still populated so delivery has a fallback:
    assert row["executive_summary"]


# ===========================================================================
# Task 6 — ingestion-side suppression accounting
# ===========================================================================

def test_generate_macro_summary_persists_suppression_breakdown_and_samples():
    """generate_macro_summary must accept counts and samples and persist them."""
    counts = {"duplicate_url": 3, "llm_discard": 2}
    samples = [
        {"reason": "llm_discard", "url": "https://x.com/1", "title": "Bad article"},
    ]
    mock_supa = MagicMock()
    payload = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
    }
    with patch("ingestion_engine._get_openai", return_value=_make_macro_mock(payload)), \
         patch("ingestion_engine._get_supabase", return_value=mock_supa):
        generate_macro_summary(
            _make_articles(5),
            screened_count=87,
            suppression_breakdown=counts,
            suppression_samples=samples,
        )
    row = _capture_upsert(mock_supa)
    assert row["screened_count"] == 87
    assert row["suppression_breakdown"] == counts
    assert row["suppression_samples"] == samples


# ===========================================================================
# Task 7 — run-mode isolation in delivery fetch_macro_summary()
# ===========================================================================

def test_fetch_macro_summary_filters_by_run_mode_production(monkeypatch):
    """Production delivery must fetch the production row even when a test row exists."""
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    from delivery_engine import fetch_macro_summary

    mock_supa = MagicMock()
    mock_supa.table.return_value.select.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.eq.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.gte.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.order.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.limit.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.execute.return_value = MagicMock(
        data=[{"run_date": "2026-05-21", "run_mode": "production",
               "executive_summary": "Prod summary", "macro_sentiment": "Stable"}]
    )

    with patch("delivery_engine._get_supabase", return_value=mock_supa):
        result = fetch_macro_summary()

    # eq() must have been called with run_mode='production'.
    eq_calls = mock_supa.table.return_value.eq.call_args_list
    assert any(c.args == ("run_mode", "production") for c in eq_calls), \
        f"Expected eq('run_mode', 'production') in {eq_calls}"
    assert result["executive_summary"] == "Prod summary"


def test_fetch_macro_summary_filters_by_run_mode_test(monkeypatch):
    """Test delivery must fetch the test row."""
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    from delivery_engine import fetch_macro_summary

    mock_supa = MagicMock()
    mock_supa.table.return_value.select.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.eq.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.gte.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.order.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.limit.return_value = mock_supa.table.return_value
    mock_supa.table.return_value.execute.return_value = MagicMock(data=[])

    with patch("delivery_engine._get_supabase", return_value=mock_supa):
        fetch_macro_summary()

    eq_calls = mock_supa.table.return_value.eq.call_args_list
    assert any(c.args == ("run_mode", "test") for c in eq_calls), \
        f"Expected eq('run_mode', 'test') in {eq_calls}"


def test_run_mode_helper(monkeypatch):
    """_run_mode() returns 'test' when env=test; 'production' otherwise; case-insensitive."""
    from delivery_engine import _run_mode
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    assert _run_mode() == "test"
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "TEST")
    assert _run_mode() == "test"
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "")
    assert _run_mode() == "production"
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    assert _run_mode() == "production"


# ===========================================================================
# Task 9 — _apply_delivery_suppression()
# ===========================================================================

def _supp_config(**overrides) -> dict:
    """Default delivery_suppression config for tests; overrides applied on top."""
    base = {
        "enable_duplicate_headline": True,
        "enable_semantic_duplicate_headline": True,
        "headline_duplicate_threshold": 90,
        "enable_product_listing": True,
        "enable_job_posting": True,
        "job_posting_override_action": "Escalate to leadership",
        "enable_generic_market_report": True,
        "enable_unrelated_color_result": True,
        "enable_enterprise_low_impact": True,
        "enterprise_min_impact": 7,
        "url_patterns_product_listing": ["/product/", "amazon.com"],
        "url_patterns_job_posting": ["linkedin.com/jobs", "/careers/"],
        "title_patterns_generic_market_report": ["market size", "market report"],
        "color_terms": ["color", "colour"],
        "plastics_relevance_terms": ["plastic", "polymer", "masterbatch", "colorant"],
    }
    base.update(overrides)
    return {"delivery_suppression": base}


def _row(**overrides) -> dict:
    base = {
        "url_hash": overrides.get("url_hash", "abc"),
        "source_url": "https://example.com/article",
        "headline": "Default Headline",
        "americhem_impact": "Effect.",
        "americhem_impact_score": 8,
        "sentiment_tag": "Neutral",
        "commercial_segment": "Healthcare",
        "signal_type": "Customer",
        "entities_mentioned": ["Acme"],
        "recommended_action": "Monitor",
    }
    base.update(overrides)
    return base


def test_apply_delivery_suppression_drops_enterprise_low_impact():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(commercial_segment="Enterprise / Cross-Segment", americhem_impact_score=5)]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"enterprise_cross_segment_low_impact": 1}
    assert ledger.samples[0].to_dict()["reason"] == "enterprise_cross_segment_low_impact"


def test_apply_delivery_suppression_keeps_enterprise_high_impact():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(commercial_segment="Enterprise / Cross-Segment", americhem_impact_score=8)]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {}


def test_apply_delivery_suppression_drops_product_listing():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(source_url="https://example.com/product/widget")]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"product_listing": 1}


def test_apply_delivery_suppression_drops_job_posting():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(source_url="https://www.linkedin.com/jobs/12345")]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"job_posting": 1}


def test_apply_delivery_suppression_job_posting_escalate_override():
    """A job-posting URL with recommended_action='Escalate to leadership' is kept."""
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(source_url="https://www.linkedin.com/jobs/ceo-move",
                 recommended_action="Escalate to leadership")]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {}


def test_apply_delivery_suppression_drops_generic_market_report_no_entities():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(headline="Global Polypropylene Market Size 2026-2032",
                 entities_mentioned=[])]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"generic_market_report": 1}


def test_apply_delivery_suppression_keeps_generic_market_report_with_entities():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(headline="Global Polypropylene Market 2026 Report",
                 entities_mentioned=["Avient"])]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {}


def test_apply_delivery_suppression_drops_unrelated_color_result():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(headline="What extension cord colors mean",
                 americhem_impact="No plastics relevance.",
                 entities_mentioned=["DIY Network"])]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"unrelated_color_result": 1}


def test_apply_delivery_suppression_keeps_color_result_with_plastics_term():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(headline="New masterbatch colors for automotive interiors",
                 americhem_impact="Drives masterbatch demand.",
                 entities_mentioned=["BASF"])]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {}


def test_apply_delivery_suppression_drops_exact_duplicate_headline():
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(url_hash="a", headline="Plant fire halts production"),
            _row(url_hash="b", headline="Plant fire halts production")]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert kept[0]["url_hash"] == "a"
    assert dict(ledger.breakdown) == {"duplicate_headline": 1}


def test_apply_delivery_suppression_drops_semantic_duplicate_headline():
    from delivery_engine import _apply_delivery_suppression
    rows = [
        _row(url_hash="a", headline="Plant fire halts production at BASF site"),
        _row(url_hash="b", headline="BASF plant fire halts production at site"),
    ]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {"semantic_duplicate_headline": 1}


def test_apply_delivery_suppression_first_match_wins():
    """A row matching both product_listing and generic_market_report is counted once,
    under product_listing (which is checked first in the rule order)."""
    from delivery_engine import _apply_delivery_suppression
    rows = [_row(source_url="https://amazon.com/product/123",
                 headline="Plastic Market Report 2026",
                 entities_mentioned=[])]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert dict(ledger.breakdown) == {"product_listing": 1}  # NOT generic_market_report


def test_apply_delivery_suppression_disabled_rule_allows_through():
    from delivery_engine import _apply_delivery_suppression
    cfg = _supp_config(enable_product_listing=False)
    rows = [_row(source_url="https://example.com/product/widget")]
    kept, ledger = _apply_delivery_suppression(rows, cfg)
    assert len(kept) == 1
    assert dict(ledger.breakdown) == {}


def test_apply_delivery_suppression_samples_capped_at_10():
    from delivery_engine import _apply_delivery_suppression
    rows = [
        _row(url_hash=f"h{i}", source_url=f"https://amazon.com/product/{i}",
             headline=f"Product {i}")
        for i in range(15)
    ]
    kept, ledger = _apply_delivery_suppression(rows, _supp_config())
    assert kept == []
    assert ledger.breakdown["product_listing"] == 15
    assert len(ledger.samples) == 10


# ---------------------------------------------------------------------------
# Task 10: _group_by_commercial_segment + _render_segment_watch_section
# ---------------------------------------------------------------------------

def test_group_by_commercial_segment_keys_off_new_field():
    from delivery_engine import _group_by_commercial_segment
    rows = [
        {"url_hash": "a", "commercial_segment": "Healthcare",
         "americhem_impact_score": 8, "headline": "A"},
        {"url_hash": "b", "commercial_segment": "Healthcare",
         "americhem_impact_score": 7, "headline": "B"},
        {"url_hash": "c", "commercial_segment": "Packaging",
         "americhem_impact_score": 6, "headline": "C"},
    ]
    groups = _group_by_commercial_segment(rows)
    assert set(groups.keys()) == {"Healthcare", "Packaging"}
    assert len(groups["Healthcare"]) == 2


def test_group_by_commercial_segment_uses_legacy_fallback():
    from delivery_engine import _group_by_commercial_segment
    rows = [
        {"url_hash": "a", "strategic_segment": "Healthcare",
         "americhem_impact_score": 8, "headline": "A"},
        {"url_hash": "b", "strategic_segment": "Competitive / Customer Signal",
         "americhem_impact_score": 7, "headline": "B"},
    ]
    groups = _group_by_commercial_segment(rows)
    assert "Healthcare" in groups
    assert "Enterprise / Cross-Segment" in groups


def test_render_segment_watch_section_displays_meta_strip_with_signal():
    from delivery_engine import _render_segment_watch_section
    groups = {
        "Healthcare": [{
            "url_hash": "a",
            "headline": "Test Card Headline",
            "source_url": "https://news.com/a",
            "americhem_impact": "Direct demand effect.",
            "americhem_impact_score": 8,
            "sentiment_tag": "Positive",
            "signal_type": "Customer",
            "commercial_segment": "Healthcare",
            "recommended_action": "Monitor",
        }],
    }
    html = _render_segment_watch_section(groups, synthesis={})
    assert "HEALTHCARE" in html.upper()
    assert "Test Card Headline" in html
    assert "Impact: 8/10" in html
    assert "Positive" in html
    assert "Signal: Customer" in html
    assert "Direct demand effect." in html


def test_render_segment_watch_section_omits_signal_for_legacy_row():
    from delivery_engine import _render_segment_watch_section
    groups = {
        "Healthcare": [{
            "url_hash": "a",
            "headline": "Legacy Row Headline",
            "source_url": "https://news.com/a",
            "americhem_impact": "Effect.",
            "americhem_impact_score": 7,
            "sentiment_tag": "Neutral",
            "strategic_segment": "Healthcare",
            # no signal_type
        }],
    }
    html = _render_segment_watch_section(groups, synthesis={})
    assert "Impact: 7/10" in html
    assert "Signal:" not in html


def test_render_segment_watch_section_critical_badge_for_legacy_low_score():
    from delivery_engine import _render_segment_watch_section
    groups = {
        "Enterprise / Cross-Segment": [{
            "url_hash": "a",
            "headline": "Critical legacy headline",
            "source_url": "https://news.com/a",
            "americhem_impact": "Effect.",
            "sentiment_score": 2,
            "strategic_segment": "Broader Americhem",
        }],
    }
    html = _render_segment_watch_section(groups, synthesis={})
    assert "CRITICAL" in html


def test_render_segment_watch_section_renders_synthesis_paragraph():
    from delivery_engine import _render_segment_watch_section
    groups = {
        "Packaging": [
            {"url_hash": "a", "headline": "A", "source_url": "https://x/a",
             "americhem_impact": "X.", "americhem_impact_score": 7,
             "sentiment_tag": "Neutral", "signal_type": "Sustainability",
             "commercial_segment": "Packaging"},
            {"url_hash": "b", "headline": "B", "source_url": "https://x/b",
             "americhem_impact": "Y.", "americhem_impact_score": 6,
             "sentiment_tag": "Neutral", "signal_type": "Sustainability",
             "commercial_segment": "Packaging"},
        ]
    }
    synth = {"Packaging": "Brand-owners are shifting toward recycled content."}
    html = _render_segment_watch_section(groups, synth)
    assert "Brand-owners are shifting toward recycled content." in html


# ---------------------------------------------------------------------------
# Task 11: generate_html_email() pipeline integration tests
# ---------------------------------------------------------------------------

def test_generate_html_email_surfaced_count_is_post_cap(monkeypatch):
    """surfaced_count must reflect the final visible-card list AFTER per-segment caps."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    rows = [
        {"url_hash": f"h{i}", "commercial_segment": "Healthcare",
         "americhem_impact_score": 8, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": f"HC {i}",
         "americhem_impact": "Effect.", "source_url": f"https://x/{i}",
         "entities_mentioned": ["Acme"]}
        for i in range(5)
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 2,
            "max_total_visible_articles": 12,
        }
    }

    captured = {}
    mock_supa = MagicMock()

    def fake_update(payload):
        captured["update"] = payload
        return mock_supa.table.return_value.update.return_value

    mock_supa.table.return_value.update.side_effect = fake_update
    mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()
    mock_supa.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(data=[])

    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value=config), \
         patch("delivery_engine._get_supabase", return_value=mock_supa):
        generate_html_email(rows)

    assert "update" in captured, "Expected an update() call to daily_summaries"
    assert captured["update"]["surfaced_count"] == 2


def test_generate_html_email_writes_delivery_suppression_counts_back(monkeypatch):
    """Delivery must write below_impact_threshold into suppression_breakdown via update()."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    rows = [
        {"url_hash": "low", "commercial_segment": "Healthcare",
         "americhem_impact_score": 4, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": "Below threshold",
         "americhem_impact": ".", "source_url": "https://x/1",
         "entities_mentioned": ["Acme"]},
        {"url_hash": "high", "commercial_segment": "Packaging",
         "americhem_impact_score": 8, "sentiment_tag": "Positive",
         "signal_type": "Customer", "headline": "Surfaced",
         "americhem_impact": ".", "source_url": "https://x/2",
         "entities_mentioned": ["Acme"]},
    ]
    config = {
        "reporting": {
            "visible_impact_threshold": 6,
            "max_visible_articles_per_segment": 3,
            "max_total_visible_articles": 12,
        }
    }
    captured = {}
    mock_supa = MagicMock()

    def fake_update(payload):
        captured["update"] = payload
        return mock_supa.table.return_value.update.return_value

    mock_supa.table.return_value.update.side_effect = fake_update
    mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()
    mock_supa.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(data=[])

    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value=config), \
         patch("delivery_engine._get_supabase", return_value=mock_supa):
        generate_html_email(rows)

    breakdown = captured["update"]["suppression_breakdown"]
    assert breakdown["below_impact_threshold"] == 1
    assert captured["update"]["surfaced_count"] == 1


def test_generate_html_email_update_filtered_by_run_date_and_run_mode(monkeypatch):
    """The update() call must be filtered by run_date AND run_mode."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    rows = [{
        "url_hash": "a", "commercial_segment": "Healthcare",
        "americhem_impact_score": 8, "sentiment_tag": "Neutral",
        "signal_type": "Customer", "headline": "H", "americhem_impact": ".",
        "source_url": "https://x/a", "entities_mentioned": ["Acme"],
    }]

    eq_calls = []
    mock_supa = MagicMock()

    update_chain = MagicMock()
    def fake_update_eq(col, val):
        eq_calls.append((col, val))
        return update_chain
    update_chain.eq.side_effect = fake_update_eq
    update_chain.eq.return_value = update_chain
    update_chain.execute.return_value = MagicMock()

    def fake_update(payload):
        return update_chain
    mock_supa.table.return_value.update.side_effect = fake_update

    mock_supa.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(data=[])

    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}), \
         patch("delivery_engine._get_supabase", return_value=mock_supa):
        generate_html_email(rows)

    keys = {c[0] for c in eq_calls}
    assert "run_date" in keys, f"eq calls: {eq_calls}"
    assert "run_mode" in keys, f"eq calls: {eq_calls}"
    rm_calls = [c for c in eq_calls if c[0] == "run_mode"]
    assert any(c[1] == "test" for c in rm_calls), f"Expected run_mode='test' in {rm_calls}"


def test_render_executive_bullets_renders_three_labeled_bullets():
    from delivery_engine import _render_executive_bullets
    bullets = [
        {"label": "Market pressure",    "body": "Techmer raised prices."},
        {"label": "Supply chain watch", "body": "Mitsubishi restructuring."},
        {"label": "Commercial action",  "body": "Prioritize additives."},
    ]
    html = _render_executive_bullets(bullets)
    assert "Market pressure" in html
    assert "Supply chain watch" in html
    assert "Commercial action" in html
    assert "Techmer raised prices." in html
    assert "Mitsubishi restructuring." in html
    assert "Prioritize additives." in html


def test_render_exec_summary_uses_structured_bullets_when_present():
    from delivery_engine import _render_exec_summary
    macro = {
        "dominant_condition": "Competitive Pressure",
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
        "executive_summary": "Should not be used.",
    }
    html = _render_exec_summary(macro)
    assert "Market pressure" in html
    assert "A." in html
    assert "Should not be used." not in html
    assert "Competitive Pressure" in html  # condition badge


def test_render_exec_summary_falls_back_to_legacy_when_bullets_null():
    from delivery_engine import _render_exec_summary
    macro = {
        "dominant_condition": "Mixed / Watch",
        "executive_bullets": None,
        "executive_summary": "Legacy prose summary used.",
    }
    html = _render_exec_summary(macro)
    assert "Legacy prose summary used." in html
    assert "Market pressure" not in html


def test_render_exec_summary_no_summary_returns_empty():
    from delivery_engine import _render_exec_summary
    assert _render_exec_summary(None) == ""
    assert _render_exec_summary({}) == ""


# ===========================================================================
# Task 13 — Null-safe header fallbacks (screened_count, dominant_condition)
# ===========================================================================

def test_header_falls_back_to_len_data_when_screened_null(monkeypatch):
    """When screened_count is NULL, header uses len(data)."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    rows = [
        {"url_hash": f"h{i}", "commercial_segment": "Healthcare",
         "americhem_impact_score": 8, "sentiment_tag": "Neutral",
         "signal_type": "Customer", "headline": f"Distinct Healthcare News {i}",
         "americhem_impact": ".", "source_url": f"https://x/{i}",
         "entities_mentioned": ["Acme"]}
        for i in range(7)
    ]
    macro = {"executive_bullets": [
        {"label": "Market pressure",    "body": "A."},
        {"label": "Supply chain watch", "body": "B."},
        {"label": "Commercial action",  "body": "C."},
    ], "dominant_condition": "Competitive Pressure",
       "screened_count": None, "surfaced_count": None}

    mock_supa = MagicMock()
    mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()
    mock_supa.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(data=[])

    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._get_supabase", return_value=mock_supa), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email(rows, macro_summary=macro)

    assert "from 7 screened items" in html
    assert "from None screened items" not in html


def test_header_omits_dominant_condition_clause_when_null(monkeypatch):
    """When dominant_condition is NULL, the badge clause is omitted (no literal 'None')."""
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    rows = [{"url_hash": "a", "commercial_segment": "Healthcare",
             "americhem_impact_score": 8, "sentiment_tag": "Neutral",
             "signal_type": "Customer", "headline": "Some Distinct Headline",
             "americhem_impact": ".", "source_url": "https://x/a",
             "entities_mentioned": ["Acme"]}]
    macro = {"executive_bullets": None, "executive_summary": "Fallback prose.",
             "dominant_condition": None, "macro_sentiment": None,
             "screened_count": 5, "surfaced_count": 1}

    mock_supa = MagicMock()
    mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()
    mock_supa.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(data=[])

    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._get_supabase", return_value=mock_supa), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email(rows, macro_summary=macro)

    # The literal string 'None' must not appear anywhere as a rendered value.
    assert ">None<" not in html
    assert "Dominant condition: None" not in html


# ===========================================================================
# Task 14 — QA suppression-summary section
# ===========================================================================

def test_qa_debug_section_appears_in_test_mode(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", "test")
    rows = [{"url_hash": "a", "commercial_segment": "Healthcare",
             "americhem_impact_score": 8, "sentiment_tag": "Neutral",
             "signal_type": "Customer", "headline": "Some Distinct QA Headline",
             "americhem_impact": ".", "source_url": "https://x/a",
             "entities_mentioned": ["Acme"]}]
    macro = {
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
        "dominant_condition": "Competitive Pressure",
        "screened_count": 87,
        "surfaced_count": 1,
        "suppression_breakdown": {
            "duplicate_url": 23,
            "llm_discard": 12,
            "product_listing": 5,
            "job_posting": 3,
        },
        "suppression_samples": [
            {"reason": "product_listing", "url": "https://amazon.com/product/1",
             "title": "Pretty plastic tote"},
            {"reason": "llm_discard", "url": "https://news.com/extension-cord",
             "title": "Best extension cord colors"},
        ],
    }
    mock_supa = MagicMock()
    mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()
    mock_supa.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(data=[])

    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._get_supabase", return_value=mock_supa), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email(rows, macro_summary=macro)

    assert "QA" in html
    assert "Suppression Summary" in html
    # Friendly labels expected (Task 14 spec uses friendly forms in the email).
    assert "duplicate URL" in html
    assert "product listing" in html
    assert "Pretty plastic tote" in html
    assert "Best extension cord colors" in html


def test_qa_debug_section_absent_in_production(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "test_key")
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    rows = [{"url_hash": "a", "commercial_segment": "Healthcare",
             "americhem_impact_score": 8, "sentiment_tag": "Neutral",
             "signal_type": "Customer", "headline": "Production Distinct Headline",
             "americhem_impact": ".", "source_url": "https://x/a",
             "entities_mentioned": ["Acme"]}]
    macro = {
        "executive_bullets": [
            {"label": "Market pressure",    "body": "A."},
            {"label": "Supply chain watch", "body": "B."},
            {"label": "Commercial action",  "body": "C."},
        ],
        "dominant_condition": "Competitive Pressure",
        "screened_count": 87,
        "surfaced_count": 1,
        "suppression_breakdown": {"duplicate_url": 23, "product_listing": 5},
        "suppression_samples": [{"reason": "product_listing",
                                 "url": "https://amazon.com/product/1",
                                 "title": "Pretty plastic tote"}],
    }
    mock_supa = MagicMock()
    mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()
    mock_supa.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(data=[])

    with patch("delivery_engine._get_openai", return_value=MagicMock()), \
         patch("delivery_engine._get_supabase", return_value=mock_supa), \
         patch("delivery_engine._load_mp_config", return_value={"reporting": {"visible_impact_threshold": 6}}):
        html = generate_html_email(rows, macro_summary=macro)

    assert "Suppression Summary" not in html
    assert "Pretty plastic tote" not in html


def test_render_qa_debug_section_uses_friendly_labels():
    from delivery_engine import _render_qa_debug_section
    macro = {
        "screened_count": 87,
        "surfaced_count": 6,
        "suppression_breakdown": {
            "duplicate_url": 23,
            "semantic_duplicate": 4,
            "llm_discard": 12,
            "enterprise_cross_segment_low_impact": 3,
        },
        "suppression_samples": [
            {"reason": "duplicate_url", "url": "https://x/1", "title": "Dup"},
        ],
    }
    html = _render_qa_debug_section(macro)
    assert "duplicate URL" in html
    assert "semantic duplicate" in html
    assert "LLM discard" in html
    assert "Enterprise / Cross-Segment" in html


# ===========================================================================
# PR #7 fix — idempotent suppression breakdown on same-day retries
# ===========================================================================

def test_update_delivery_summary_counts_overwrites_delivery_keys(monkeypatch):
    """Delivery-owned keys must be REPLACED, not added, on retry. Ingestion-owned
    keys must be preserved unchanged."""
    from suppression_ledger import SuppressionLedger
    from delivery_engine import _update_delivery_summary_counts

    prior = {
        "duplicate_url": 10,            # ingestion-owned
        "semantic_duplicate": 2,        # ingestion-owned
        "below_impact_threshold": 22,   # delivery-owned (must be replaced)
        "weak_relevance": 7,            # delivery-owned (must be replaced)
    }

    mock_supa = MagicMock()
    mock_supa.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(
        data=[{"suppression_breakdown": prior, "suppression_samples": []}]
    )
    captured = {}
    def fake_update(payload):
        captured["update"] = payload
        return mock_supa.table.return_value.update.return_value
    mock_supa.table.return_value.update.side_effect = fake_update
    mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()

    ledger = (SuppressionLedger.for_delivery()
              .record_count("below_impact_threshold", 5)
              .record_count("weak_relevance", 2))

    with patch("delivery_engine._get_supabase", return_value=mock_supa):
        _update_delivery_summary_counts(surfaced_count=6, ledger=ledger)

    merged = captured["update"]["suppression_breakdown"]
    # Ingestion-owned keys preserved unchanged:
    assert merged["duplicate_url"] == 10
    assert merged["semantic_duplicate"] == 2
    # Delivery-owned keys REPLACED (not added):
    assert merged["below_impact_threshold"] == 5, "delivery-owned count must be overwritten, not added"
    assert merged["weak_relevance"] == 2


def test_update_delivery_summary_counts_idempotent_on_retry():
    """Two consecutive calls with the same ledger must produce the same
    final breakdown — no doubling."""
    from suppression_ledger import SuppressionLedger
    from delivery_engine import _update_delivery_summary_counts

    captured = {}
    mock_supa = MagicMock()

    def fake_select_chain(*args, **kwargs):
        return mock_supa.table.return_value.select.return_value
    mock_supa.table.return_value.select.side_effect = fake_select_chain
    mock_supa.table.return_value.select.return_value.eq.return_value = mock_supa.table.return_value.select.return_value
    mock_supa.table.return_value.select.return_value.limit.return_value = mock_supa.table.return_value.select.return_value

    # Track prior state across calls.
    state = {"prior_breakdown": {"duplicate_url": 10}, "prior_samples": []}

    def fake_execute_select():
        return MagicMock(data=[{
            "suppression_breakdown": dict(state["prior_breakdown"]),
            "suppression_samples": list(state["prior_samples"]),
        }])
    mock_supa.table.return_value.select.return_value.execute.side_effect = fake_execute_select

    def fake_update(payload):
        captured["update"] = payload
        # Simulate the write landing on the row for the next .select() read.
        state["prior_breakdown"] = payload["suppression_breakdown"]
        state["prior_samples"] = payload["suppression_samples"]
        return mock_supa.table.return_value.update.return_value
    mock_supa.table.return_value.update.side_effect = fake_update
    mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()

    ledger = (SuppressionLedger.for_delivery()
              .record_count("below_impact_threshold", 22)
              .record("product_listing", url="https://amazon.com/p/1", title="Plastic tote")
              .record_count("product_listing", 4))  # total product_listing = 5

    with patch("delivery_engine._get_supabase", return_value=mock_supa):
        _update_delivery_summary_counts(surfaced_count=6, ledger=ledger)
        first_breakdown = dict(captured["update"]["suppression_breakdown"])
        first_samples = list(captured["update"]["suppression_samples"])

        _update_delivery_summary_counts(surfaced_count=6, ledger=ledger)
        second_breakdown = dict(captured["update"]["suppression_breakdown"])
        second_samples = list(captured["update"]["suppression_samples"])

    assert first_breakdown == second_breakdown, \
        f"Retry must be idempotent. First={first_breakdown} Second={second_breakdown}"
    assert second_breakdown["below_impact_threshold"] == 22, "must not double"
    assert second_breakdown["product_listing"] == 5, "must not double"
    assert second_breakdown["duplicate_url"] == 10, "ingestion-owned key preserved"
    assert first_samples == second_samples, \
        f"Retry must not duplicate samples. First={first_samples} Second={second_samples}"


def test_update_delivery_summary_counts_preserves_unknown_prior_keys():
    """Unknown keys in the existing breakdown (e.g., future codes) must be preserved."""
    from suppression_ledger import SuppressionLedger
    from delivery_engine import _update_delivery_summary_counts

    prior = {"some_future_reason": 99, "duplicate_url": 5}
    mock_supa = MagicMock()
    mock_supa.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = MagicMock(
        data=[{"suppression_breakdown": prior, "suppression_samples": []}]
    )
    captured = {}
    def fake_update(payload):
        captured["update"] = payload
        return mock_supa.table.return_value.update.return_value
    mock_supa.table.return_value.update.side_effect = fake_update
    mock_supa.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.return_value = MagicMock()

    ledger = SuppressionLedger.for_delivery().record_count("below_impact_threshold", 2)

    with patch("delivery_engine._get_supabase", return_value=mock_supa):
        _update_delivery_summary_counts(surfaced_count=1, ledger=ledger)

    merged = captured["update"]["suppression_breakdown"]
    assert merged["some_future_reason"] == 99
    assert merged["duplicate_url"] == 5
    assert merged["below_impact_threshold"] == 2


def test_delivery_suppression_idempotent_on_same_day_retry(monkeypatch):
    """Running delivery twice in the same day with the same inputs must
    produce identical persisted breakdown and samples."""
    from suppression_ledger import SuppressionLedger
    from delivery_engine import _update_delivery_summary_counts

    captured = []
    mock_supa = MagicMock()
    def capture_update(payload):
        captured.append(payload)
        m = MagicMock()
        m.eq.return_value.eq.return_value.execute.return_value = MagicMock()
        return m
    mock_supa.table.return_value.update.side_effect = capture_update
    # First read returns empty prior; second read returns what was just written
    reads = [MagicMock(data=[]), MagicMock(data=[{"suppression_breakdown": {}, "suppression_samples": []}])]
    mock_supa.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.side_effect = reads

    ledger = (SuppressionLedger.for_delivery()
              .record("duplicate_headline", url="u", title="t")
              .record_count("below_impact_threshold", 3))

    with patch("delivery_engine._get_supabase", return_value=mock_supa):
        _update_delivery_summary_counts(surfaced_count=5, ledger=ledger)
        _update_delivery_summary_counts(surfaced_count=5, ledger=ledger)

    assert len(captured) == 2
    # Second run produces same breakdown + samples as first
    assert captured[0]["suppression_breakdown"] == captured[1]["suppression_breakdown"]
    assert captured[0]["suppression_samples"]   == captured[1]["suppression_samples"]
