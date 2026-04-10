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
    config_yaml = textwrap.dedent(
        """\
        competitors:
          - name: ActiveCorp
            active: true
          - name: InactiveCorp
            active: false
        customers: []
        suppliers: []
        markets: []
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
    config_yaml = textwrap.dedent(
        """\
        competitors:
          - name: Avient
            active: true
        customers: []
        suppliers: []
        markets: []
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
    assert t["results_per_entity"] == 3
    assert t["lookback_hours"] == 48
    assert t["min_article_length"] == 300


# ---------------------------------------------------------------------------
# 5. DISCARD signal
# ---------------------------------------------------------------------------

def test_discard_signal_detected():
    insight = {"americhem_impact": "DISCARD"}
    assert insight.get("americhem_impact") == "DISCARD"


# ---------------------------------------------------------------------------
# 6. raw_materials category
# ---------------------------------------------------------------------------

def test_raw_materials_category_loaded(tmp_path):
    config_yaml = textwrap.dedent(
        """\
        competitors: []
        customers: []
        suppliers: []
        raw_materials:
          - name: "commodity resins"
            active: true
        markets: []
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
    assert "commodity resins" in names
    assert targets[0]["category"] == "raw_materials"


# ---------------------------------------------------------------------------
# 7. recommended_action soft default
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

from delivery_engine import _render_card


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
