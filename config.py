"""Single module owning the pipeline's runtime configuration reads.

Concentrates what the two engines read directly from the process environment and
from ``market_pulse_config.yaml``: the cached config-file load, the run-mode
switch, integer/flag env coercion, and a fail-fast startup check for the secrets
each engine requires. The three Protocol seams (``llm``, ``daily_intelligence_repo``,
``zoominfo_client``) keep reading their own values at use time — their adapters are
the substitution path, so this module only *validates their presence*, it does not
own their values.

The pure report/scoring/prompt modules never import this module: they receive a
plain config dict as a parameter, so their zero-I/O purity is untouched.
"""

import logging
import os
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

_TRUTHY_ENV_VALUES: frozenset[str] = frozenset({"true", "1", "yes", "on"})

# Secrets each engine cannot run without. Validated once at startup by
# validate_environment(); the seam modules still read the values themselves.
REQUIRED_SECRETS: dict[str, tuple[str, ...]] = {
    "ingestion": (
        "SERPER_API_KEY",
        "FIRECRAWL_API_KEY",
        "OPENAI_API_KEY",
        "SUPABASE_URL",
        "SUPABASE_KEY",
    ),
    "delivery": (
        "SMTP_PASS",
        "SENDER_EMAIL",
        "RECIPIENT_EMAILS",
        "SUPABASE_URL",
        "SUPABASE_KEY",
        # Delivery calls OpenAI for thematic synthesis; without a key it would
        # silently ship bullets-only emails every run, so require it up front.
        "OPENAI_API_KEY",
    ),
}


class MissingEnvironmentError(RuntimeError):
    """Raised at startup when a required secret env var is absent or blank."""


_MP_CONFIG: Optional[dict] = None


def mp_config() -> dict:
    """Load market_pulse_config.yaml once; return the cached result on repeat calls."""
    global _MP_CONFIG
    if _MP_CONFIG is None:
        try:
            with open("market_pulse_config.yaml", "r") as fh:
                _MP_CONFIG = yaml.safe_load(fh) or {}
        except Exception as exc:
            logger.warning("Could not load market_pulse_config.yaml — using defaults: %s", exc)
            _MP_CONFIG = {}
    return _MP_CONFIG


def run_mode() -> str:
    """Return 'test' when MARKET_PULSE_RUN_MODE=test (case-insensitive), else 'production'."""
    return "test" if os.environ.get("MARKET_PULSE_RUN_MODE", "").strip().lower() == "test" else "production"


def env_int(name: str, default: int) -> int:
    """Read an integer env var, falling back to *default* (with a warning) on
    missing or non-integer values."""
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw.strip())
    except ValueError:
        logger.warning("Invalid integer for %s=%r — using default %d", name, raw, default)
        return default


def _env_flag(name: str) -> bool:
    """True when *name* holds a recognised truthy value (true/1/yes/on)."""
    return os.environ.get(name, "").strip().lower() in _TRUTHY_ENV_VALUES


def zoominfo_news_enabled() -> bool:
    """True when ZOOMINFO_NEWS_ENABLED is a recognised truthy value. Default off."""
    return _env_flag("ZOOMINFO_NEWS_ENABLED")


def relevance_gate_enabled() -> bool:
    """True when ZOOMINFO_RELEVANCE_GATE_ENABLED is truthy. Default off — production
    behavior is unchanged until explicitly enabled."""
    return _env_flag("ZOOMINFO_RELEVANCE_GATE_ENABLED")


def store_discovery_metadata() -> bool:
    """True when the discovery-metadata columns should be written to Supabase.

    Default off so production upserts keep working until migration 003 (the
    discovery_source / external_company_id / published_at / source_metadata
    columns) has been applied. Flip STORE_DISCOVERY_METADATA on only after the
    migration is live."""
    return _env_flag("STORE_DISCOVERY_METADATA")


def validate_environment(engine: str) -> None:
    """Fail fast at startup if *engine* is missing any required secret.

    *engine* is 'ingestion' or 'delivery'. Raises MissingEnvironmentError listing
    every absent/blank variable, so a misconfigured cron crashes before spending
    on any API instead of part-way through the run.
    """
    required = REQUIRED_SECRETS[engine]
    missing = [name for name in required if not os.environ.get(name, "").strip()]
    if missing:
        raise MissingEnvironmentError(
            f"Missing required environment variable(s) for {engine}: {', '.join(missing)}"
        )
