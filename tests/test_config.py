"""Tests for the config seam: env coercion, run mode, config load, startup validation."""

import pytest

import config


# ---------------------------------------------------------------------------
# run_mode
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value", ["test", "TEST", " Test ", "test\n"])
def test_run_mode_test(monkeypatch, value):
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", value)
    assert config.run_mode() == "test"


@pytest.mark.parametrize("value", ["production", "", "prod", "0"])
def test_run_mode_production(monkeypatch, value):
    monkeypatch.setenv("MARKET_PULSE_RUN_MODE", value)
    assert config.run_mode() == "production"


def test_run_mode_unset_defaults_production(monkeypatch):
    monkeypatch.delenv("MARKET_PULSE_RUN_MODE", raising=False)
    assert config.run_mode() == "production"


# ---------------------------------------------------------------------------
# env_int
# ---------------------------------------------------------------------------

def test_env_int_uses_default_when_unset(monkeypatch):
    monkeypatch.delenv("SOME_INT", raising=False)
    assert config.env_int("SOME_INT", 5) == 5


def test_env_int_uses_default_when_blank(monkeypatch):
    monkeypatch.setenv("SOME_INT", "   ")
    assert config.env_int("SOME_INT", 5) == 5


def test_env_int_parses_valid(monkeypatch):
    monkeypatch.setenv("SOME_INT", " 7 ")
    assert config.env_int("SOME_INT", 5) == 7


def test_env_int_invalid_falls_back_to_default(monkeypatch, caplog):
    monkeypatch.setenv("SOME_INT", "not-a-number")
    with caplog.at_level("WARNING"):
        assert config.env_int("SOME_INT", 2) == 2
    assert "Invalid integer" in caplog.text


# ---------------------------------------------------------------------------
# feature flags
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value", ["true", "TRUE", "1", "yes", "on", " On "])
def test_flags_truthy(monkeypatch, value):
    monkeypatch.setenv("ZOOMINFO_NEWS_ENABLED", value)
    monkeypatch.setenv("ZOOMINFO_RELEVANCE_GATE_ENABLED", value)
    monkeypatch.setenv("STORE_DISCOVERY_METADATA", value)
    assert config.zoominfo_news_enabled() is True
    assert config.relevance_gate_enabled() is True
    assert config.store_discovery_metadata() is True


@pytest.mark.parametrize("value", ["false", "0", "no", "off", "", "maybe"])
def test_flags_falsy(monkeypatch, value):
    monkeypatch.setenv("ZOOMINFO_NEWS_ENABLED", value)
    monkeypatch.setenv("ZOOMINFO_RELEVANCE_GATE_ENABLED", value)
    monkeypatch.setenv("STORE_DISCOVERY_METADATA", value)
    assert config.zoominfo_news_enabled() is False
    assert config.relevance_gate_enabled() is False
    assert config.store_discovery_metadata() is False


def test_flags_default_off(monkeypatch):
    for name in ("ZOOMINFO_NEWS_ENABLED", "ZOOMINFO_RELEVANCE_GATE_ENABLED", "STORE_DISCOVERY_METADATA"):
        monkeypatch.delenv(name, raising=False)
    assert config.zoominfo_news_enabled() is False
    assert config.relevance_gate_enabled() is False
    assert config.store_discovery_metadata() is False


# ---------------------------------------------------------------------------
# mp_config
# ---------------------------------------------------------------------------

def test_mp_config_is_cached(monkeypatch, tmp_path):
    # Point the loader at a real temp file and count how often it is opened.
    cfg_file = tmp_path / "market_pulse_config.yaml"
    cfg_file.write_text("reporting:\n  visible_impact_threshold: 6\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config, "_MP_CONFIG", None)

    calls = {"n": 0}
    real_open = open

    def counting_open(path, *args, **kwargs):
        if path == "market_pulse_config.yaml":
            calls["n"] += 1
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr("builtins.open", counting_open)

    first = config.mp_config()
    second = config.mp_config()
    assert first == {"reporting": {"visible_impact_threshold": 6}}
    assert first is second       # cached, same object
    assert calls["n"] == 1       # file read exactly once, not per call


def test_mp_config_missing_file_returns_empty(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)  # no market_pulse_config.yaml here
    monkeypatch.setattr(config, "_MP_CONFIG", None)
    assert config.mp_config() == {}


# ---------------------------------------------------------------------------
# validate_environment (the new fail-fast behavior)
# ---------------------------------------------------------------------------

_ALL = ("SERPER_API_KEY", "FIRECRAWL_API_KEY", "OPENAI_API_KEY", "SUPABASE_URL",
        "SUPABASE_KEY", "SMTP_PASS", "SENDER_EMAIL", "RECIPIENT_EMAILS")


def _set_all(monkeypatch):
    for name in _ALL:
        monkeypatch.setenv(name, "x")


def test_validate_ingestion_all_present(monkeypatch):
    _set_all(monkeypatch)
    config.validate_environment("ingestion")  # no raise


def test_validate_delivery_all_present(monkeypatch):
    _set_all(monkeypatch)
    config.validate_environment("delivery")  # no raise


def test_validate_ingestion_missing_secret_raises(monkeypatch):
    _set_all(monkeypatch)
    monkeypatch.delenv("SERPER_API_KEY", raising=False)
    with pytest.raises(config.MissingEnvironmentError) as exc:
        config.validate_environment("ingestion")
    assert "SERPER_API_KEY" in str(exc.value)


def test_validate_treats_blank_as_missing(monkeypatch):
    _set_all(monkeypatch)
    monkeypatch.setenv("RECIPIENT_EMAILS", "   ")
    with pytest.raises(config.MissingEnvironmentError) as exc:
        config.validate_environment("delivery")
    assert "RECIPIENT_EMAILS" in str(exc.value)


def test_validate_lists_every_missing_var(monkeypatch):
    for name in _ALL:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(config.MissingEnvironmentError) as exc:
        config.validate_environment("ingestion")
    msg = str(exc.value)
    for name in config.REQUIRED_SECRETS["ingestion"]:
        assert name in msg


def test_validate_ingestion_ignores_delivery_only_secret(monkeypatch):
    # SMTP_PASS is delivery-only; ingestion must not require it.
    for name in config.REQUIRED_SECRETS["ingestion"]:
        monkeypatch.setenv(name, "x")
    monkeypatch.delenv("SMTP_PASS", raising=False)
    config.validate_environment("ingestion")  # no raise
