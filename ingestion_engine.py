import hashlib
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import datetime, timedelta
from html.parser import HTMLParser
from urllib.parse import urlparse, urlunparse
from typing import Optional

from suppression_ledger import SuppressionLedger
from daily_intelligence_repo import _repo
from llm import _llm
import insight
import prompts
# The macro validators enforce exactly what the macro prompt promises — one
# definition, imported from the module that renders it into the prompt text.
from prompts import (
    VALID_MACRO_CONDITIONS as _VALID_MACRO_CONDITIONS,
    VALID_MACRO_DIRECTIONS as _VALID_MACRO_DIRECTIONS,
    EXEC_BULLET_LABELS as _EXEC_BULLET_LABELS,
    MAX_EXECUTIVE_BULLET_CITATIONS,
    MAX_MACRO_OUTLOOK_SIGNALS,
)
import zoominfo_client
import relevance_gate

import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _run_mode() -> str:
    """Return 'test' when MARKET_PULSE_RUN_MODE=test (case-insensitive), else 'production'."""
    return "test" if os.environ.get("MARKET_PULSE_RUN_MODE", "").strip().lower() == "test" else "production"


MAX_DAILY_SCRAPES = 150
PIPELINE_DEADLINE_SECONDS = 600   # stop ingestion after 10 min to stay inside the 15-min CI limit
FIRECRAWL_WALL_CLOCK_TIMEOUT = 20  # hard per-request ceiling; prevents keepalive-induced hangs
_SEMANTIC_DUPLICATE_THRESHOLD: int = 88

_MP_CONFIG: Optional[dict] = None


def _load_mp_config() -> dict:
    """Load market_pulse_config.yaml once; return cached result on repeat calls."""
    global _MP_CONFIG
    if _MP_CONFIG is None:
        try:
            with open("market_pulse_config.yaml", "r") as fh:
                _MP_CONFIG = yaml.safe_load(fh) or {}
        except Exception as exc:
            logger.warning("Could not load market_pulse_config.yaml — using defaults: %s", exc)
            _MP_CONFIG = {}
    return _MP_CONFIG


_MOODY_INTERNAL_EXCLUDES: frozenset[str] = frozenset({
    "source set 238658",
    "PR wires",
    "Targeted News Search",
    "US Federal News",
    "specific Asia PR feed",
    "specific processing feeds",
    "Financial Times feeds",
    "financial markups",
})


def build_query(
    mode: str,
    name: Optional[str] = None,
    include_any: Optional[list[str]] = None,
    include_all: Optional[list[str]] = None,
    exclude_any: Optional[list[str]] = None,
) -> str:
    """Build a Serper.dev search query string from group field semantics.

    Supports two modes:
    - ``entity``: wraps ``name`` in quotes as the primary search term.
    - ``concept``: ORs all ``include_any`` terms into a single combined query.

    ``include_all`` terms are ANDed into every query. ``exclude_any`` terms
    become ``-"term"`` operators; entries in ``_MOODY_INTERNAL_EXCLUDES``
    (Moody's platform-level source identifiers) are silently dropped.

    Returns:
        A query string ready to pass as Serper's ``q`` parameter.
    """
    parts: list[str] = []

    if mode == "entity":
        parts.append(f'"{name}"')
    elif mode == "concept":
        if include_any:
            or_terms = " OR ".join(f'"{t}"' for t in include_any)
            parts.append(f"({or_terms})")

    for term in (include_all or []):
        parts.append(f'"{term}"')

    for term in (exclude_any or []):
        if term not in _MOODY_INTERNAL_EXCLUDES:
            parts.append(f'-"{term}"')

    return " ".join(parts)


class _TextExtractor(HTMLParser):
    _SKIP_TAGS: frozenset[str] = frozenset(
        {"script", "style", "noscript", "nav", "footer", "header", "aside", "form"}
    )
    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []
        self._skip_depth: int = 0
    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag.lower() in self._SKIP_TAGS:
            self._skip_depth += 1
    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in self._SKIP_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)
    def handle_data(self, data: str) -> None:
        if self._skip_depth == 0:
            text = data.strip()
            if text:
                self._parts.append(text)
    def get_text(self) -> str:
        return "\n".join(self._parts)


def _scrape_fallback(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True)
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        logger.warning("Fallback scraper request failed for URL %s: %s", url, exc)
        return None
    extractor = _TextExtractor()
    try:
        extractor.feed(resp.text)
    except Exception as exc:
        logger.warning("Fallback HTML parsing failed for URL %s: %s", url, exc)
        return None
    text = extractor.get_text()
    return text if text else None




def load_targets(config_path: str) -> list[dict]:
    """Load active search targets from a YAML config file.

    Supports two search modes:
    - ``entity``: one Serper query per active company name under ``entities:``.
    - ``concept``: one combined OR query for the whole group (``active: true``
      required at group level).

    Returns:
        List of target dicts, each containing ``name``, ``category``,
        ``query`` (pre-built Serper query string), and discovery settings.
    """
    with open(config_path, "r") as fh:
        config = yaml.safe_load(fh)
    discovery = config.get("discovery", {})
    results_per_entity: int = discovery.get("results_per_entity", 2)
    lookback_hours: int = discovery.get("lookback_hours", 24)
    min_article_length: int = discovery.get("min_article_length", 500)

    targets: list[dict] = []
    for group_name, group_cfg in config.items():
        if group_name == "discovery" or not isinstance(group_cfg, dict):
            continue
        mode: str = group_cfg.get("search_mode", "entity")
        include_all: list[str] = group_cfg.get("include_all", [])
        exclude_any: list[str] = group_cfg.get("exclude_any", [])

        if mode == "entity":
            for entity in group_cfg.get("entities", []):
                if not entity.get("active", False):
                    continue
                # Optional ZoomInfo enrichment: news defaults on when an id is
                # mapped, off when no id exists. Concept groups never get these.
                zoominfo_company_id = entity.get("zoominfo_company_id")
                targets.append({
                    "name": entity["name"],
                    "category": group_name,
                    "query": build_query(
                        "entity",
                        name=entity["name"],
                        include_all=include_all,
                        exclude_any=exclude_any,
                    ),
                    "results_per_entity": results_per_entity,
                    "lookback_hours": lookback_hours,
                    "min_article_length": min_article_length,
                    "zoominfo_company_id": zoominfo_company_id,
                    "zoominfo_news": entity.get("zoominfo_news", True),
                })

        elif mode == "concept":
            if not group_cfg.get("active", False):
                continue
            targets.append({
                "name": group_name,
                "category": group_name,
                "query": build_query(
                    "concept",
                    include_any=group_cfg.get("include_any", []),
                    include_all=include_all,
                    exclude_any=exclude_any,
                ),
                "results_per_entity": results_per_entity,
                "lookback_hours": lookback_hours,
                "min_article_length": min_article_length,
            })

    logger.info("Loaded %d active targets from %s", len(targets), config_path)
    return targets


def discover_urls(query: str, lookback_hours: int, results_per_entity: int) -> list[tuple[str, str]]:
    api_key = os.environ["SERPER_API_KEY"]
    endpoint = "https://google.serper.dev/news"
    payload = {"q": query, "num": results_per_entity, "tbs": f"qdr:h{lookback_hours}"}
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
    try:
        response = requests.post(endpoint, json=payload, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logger.error("Serper.dev request timed out for query '%s'", query[:80])
        return []
    except requests.exceptions.HTTPError as exc:
        logger.error("Serper.dev HTTP error for query '%s': %s", query[:80], exc.response.status_code)
        return []
    except requests.exceptions.RequestException as exc:
        logger.error("Serper.dev request failed for query '%s': %s", query[:80], exc)
        return []
    data = response.json()
    # Serper's news endpoint returns pages of 10 and ignores small `num`
    # values, so results_per_entity must be enforced client-side.
    results = [
        (item["link"], item.get("title", ""))
        for item in data.get("news", [])
        if "link" in item
    ][:results_per_entity]
    logger.info("Discovered %d URL(s) for query '%s'", len(results), query[:80])
    return results


_TRUTHY_ENV_VALUES: frozenset[str] = frozenset({"true", "1", "yes", "on"})

ZOOMINFO_NEWS_LOOKBACK_DAYS_DEFAULT = 2
ZOOMINFO_NEWS_PER_COMPANY_DEFAULT = 5


def _zoominfo_news_enabled() -> bool:
    """True when ZOOMINFO_NEWS_ENABLED is a recognised truthy value."""
    return os.environ.get("ZOOMINFO_NEWS_ENABLED", "").strip().lower() in _TRUTHY_ENV_VALUES


def _relevance_gate_enabled() -> bool:
    """True when ZOOMINFO_RELEVANCE_GATE_ENABLED is a recognised truthy value.
    Default off — production behavior is unchanged until explicitly enabled."""
    return os.environ.get("ZOOMINFO_RELEVANCE_GATE_ENABLED", "").strip().lower() in _TRUTHY_ENV_VALUES


def _gate_zoominfo_candidate(candidate: dict, entity_name: str,
                             target_metadata: dict) -> Optional[relevance_gate.GateDecision]:
    """Evaluate the relevance gate for one candidate, or return None when the
    gate does not apply (non-ZoomInfo provider, gate disabled / empty metadata,
    no record for the target, or a non-active record). Never raises."""
    if candidate.get("provider") != "zoominfo" or not target_metadata:
        return None
    record = target_metadata.get(entity_name)
    if not record or record.get("metadata_record_status") != "active":
        return None
    return relevance_gate.evaluate(
        title=candidate.get("title", ""),
        description=candidate.get("description", ""),
        record=record,
    )


def _env_int(name: str, default: int) -> int:
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


def _store_discovery_metadata() -> bool:
    """True when discovery-metadata columns should be written to Supabase.

    Default OFF so production upserts keep working until migration 003 (the
    discovery_source / external_company_id / published_at / source_metadata
    columns) has been applied. Flip STORE_DISCOVERY_METADATA on only after the
    migration is live."""
    return os.environ.get("STORE_DISCOVERY_METADATA", "").strip().lower() in _TRUTHY_ENV_VALUES


def _discovery_metadata(candidate: dict) -> dict:
    """Build the optional discovery-provenance fields for a stored row."""
    provider = candidate.get("provider", "unknown")
    return {
        "discovery_source": provider,
        "external_company_id": str(candidate.get("zoominfo_company_id") or ""),
        "published_at": candidate.get("published_at") or None,
        "source_metadata": {
            "provider": provider,
            "source_publication": candidate.get("source_publication", ""),
            "description": candidate.get("description", ""),
            "categories": candidate.get("categories", []),
        },
    }


def _new_provider_yield() -> dict:
    return {
        "discovered": 0, "scraped": 0, "stored": 0,
        "discards": 0, "relevance_dropped": 0, "scrape_failed": 0,
        "unscrapable": 0, "duplicates": 0,
    }


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


def _serper_candidate(raw_url: str, title: str) -> dict:
    """Wrap a Serper (url, title) pair in the provider-neutral candidate shape."""
    return {
        "url": raw_url,
        "title": title,
        "provider": "serper",
        "source_publication": "",
        "published_at": "",
        "description": "",
        "categories": [],
        "zoominfo_company_id": None,
        "raw": {},
    }


def discover_serper_candidates(target: dict) -> list[dict]:
    """Discover Serper news URLs for a target as provider-neutral candidates."""
    raw_results = discover_urls(
        target["query"], target["lookback_hours"], target["results_per_entity"]
    )
    return [_serper_candidate(url, title) for url, title in raw_results]


def _zoominfo_target_eligible(target: dict) -> bool:
    """True when ZoomInfo discovery should be attempted for this target:
    the feature flag is on, a company id is mapped, and zoominfo_news is not
    disabled. Concept-mode targets carry no company id, so they are ineligible.
    """
    return (
        _zoominfo_news_enabled()
        and bool(target.get("zoominfo_company_id"))
        and bool(target.get("zoominfo_news", True))
    )


def discover_zoominfo_candidates(target: dict) -> list[dict]:
    """Discover ZoomInfo company-news candidates for an entity target.

    Returns [] (without touching ZoomInfo) when the feature flag is off, the
    target has no mapped company id, or zoominfo_news is disabled for it.
    Concept-mode targets carry no zoominfo_company_id, so they short-circuit
    here and remain Serper-only.
    """
    if not _zoominfo_target_eligible(target):
        return []
    company_id = target["zoominfo_company_id"]

    lookback_days = _env_int("ZOOMINFO_NEWS_LOOKBACK_DAYS", ZOOMINFO_NEWS_LOOKBACK_DAYS_DEFAULT)
    per_company = _env_int("ZOOMINFO_NEWS_PER_COMPANY", ZOOMINFO_NEWS_PER_COMPANY_DEFAULT)
    start_date = (datetime.utcnow() - timedelta(days=lookback_days)).date().isoformat()

    return zoominfo_client.discover_company_news(
        zoominfo_company_id=company_id,
        publishing_date_start=start_date,
        page_size=per_company,
    )


def discover_candidates(target: dict) -> list[dict]:
    """Merge Serper and ZoomInfo discovery for a target.

    Each provider is isolated: a failure in one never suppresses the other.
    """
    candidates: list[dict] = []
    try:
        candidates.extend(discover_serper_candidates(target))
    except Exception as exc:
        logger.error("Serper discovery failed for target '%s': %s", target.get("name"), exc)
    try:
        candidates.extend(discover_zoominfo_candidates(target))
    except Exception as exc:
        logger.error("ZoomInfo discovery failed for target '%s': %s", target.get("name"), exc)
    return candidates


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


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    clean = parsed._replace(query="", fragment="")
    return urlunparse(clean)


def compute_url_hash(normalized_url: str) -> str:
    return hashlib.sha256(normalized_url.encode("utf-8")).hexdigest()


def url_already_processed(url_hash: str) -> bool:
    return _repo().exists_by_hash(url_hash)


def scrape_article(url: str, min_length: int) -> Optional[str]:
    """Fetch article text, trying Firecrawl first then a direct-HTTP fallback.

    Firecrawl returns clean markdown.  If Firecrawl is unavailable due to quota
    exhaustion (HTTP 402), the function retries with a lightweight direct-GET
    scraper so the pipeline keeps running.  Returns None when the content is
    below *min_length* or when all scraping attempts fail.
    """
    api_key = os.environ["FIRECRAWL_API_KEY"]
    endpoint = "https://api.firecrawl.dev/v1/scrape"
    payload = {"url": url, "formats": ["markdown"]}
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    def _firecrawl_post() -> requests.Response:
        return requests.post(endpoint, json=payload, headers=headers, timeout=30)

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            response = executor.submit(_firecrawl_post).result(timeout=FIRECRAWL_WALL_CLOCK_TIMEOUT)
        response.raise_for_status()
    except FutureTimeoutError:
        logger.error(
            "Firecrawl wall-clock timeout (%ds) for URL: %s",
            FIRECRAWL_WALL_CLOCK_TIMEOUT, url,
        )
        return None
    except requests.exceptions.Timeout:
        logger.error("Firecrawl request timed out for URL: %s", url)
        return None
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code
        if status == 402:
            logger.warning(
                "Firecrawl quota exceeded (402) — attempting fallback scrape: %s", url
            )
            text = _scrape_fallback(url)
            if text is None or len(text) < min_length:
                logger.info(
                    "Fallback content too short or unavailable (%d chars, min %d): %s",
                    len(text) if text else 0,
                    min_length,
                    url,
                )
                return None
            logger.info("Fallback scrape succeeded (%d chars): %s", len(text), url)
            return text
        logger.error("Firecrawl HTTP error for URL %s: %s", url, status)
        return None
    except requests.exceptions.RequestException as exc:
        logger.error("Firecrawl request failed for URL %s: %s", url, exc)
        return None
    data = response.json()
    markdown: str = data.get("data", {}).get("markdown", "") or ""
    if len(markdown) < min_length:
        logger.info("Article too short (%d chars, min %d): %s", len(markdown), min_length, url)
        return None
    return markdown


def synthesize_insight(article_text: str, source_url: str, trigger_entity: str, category: str) -> Optional[dict]:
    spec = prompts.insight_prompt(
        _load_mp_config(),
        article_text=article_text,
        source_url=source_url,
        trigger_entity=trigger_entity,
        category=category,
    )
    raw = _llm().complete_json(**spec.kwargs())
    if raw is None:
        return None
    if insight.is_discard(raw):
        return raw
    return insight.normalize(raw)


def is_semantic_duplicate(candidate: str, seen_headlines: set[str]) -> tuple[bool, str, int]:
    """Check whether candidate is a near-duplicate of any headline in seen_headlines."""
    if not seen_headlines:
        return (False, "", 0)
    from rapidfuzz.process import extractOne
    from rapidfuzz.fuzz import token_sort_ratio
    result = extractOne(candidate, seen_headlines, scorer=token_sort_ratio)
    if result is None:
        return (False, "", 0)
    matched_headline, score, _ = result
    is_dup = score >= _SEMANTIC_DUPLICATE_THRESHOLD
    return (is_dup, matched_headline, int(score))


def _hydrate_seen_headlines() -> set[str]:
    headlines = _repo().recent_headlines(hours=72)
    logger.info("Hydrated seen_headlines buffer with %d entries.", len(headlines))
    return headlines


def store_insight(payload: dict) -> None:
    """Persist an article insight. Raises on Supabase failure — callers in
    execute_pipeline catch and bump stats['errors'] so the batch continues."""
    _repo().upsert_insight(payload)


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


def _validate_macro_outlook(raw, valid_source_ids: frozenset[int]) -> Optional[dict]:
    """Validate the structured macro_outlook. Returns a cleaned
    {current_condition, signals:[...]} dict, or None (delivery renders no
    section) when the shape is invalid or no material signal survives.

    A signal survives only when every field is well-formed AND it cites at
    least one valid source id — the deterministic materiality gate that makes
    'source-grounded, no fabricated implications' a structural guarantee. The
    enums (VALID_MACRO_DIRECTIONS, insight.VALID_COMMERCIAL_SEGMENTS) are the
    same definitions the prompt promises."""
    if not isinstance(raw, dict):
        return None
    current = raw.get("current_condition")
    if not isinstance(current, str) or not current.strip():
        return None
    signals_raw = raw.get("signals")
    if not isinstance(signals_raw, list):
        return None

    cleaned: list[dict] = []
    for sig in signals_raw:
        if not isinstance(sig, dict):
            continue
        indicator = sig.get("indicator")
        if not isinstance(indicator, str) or not indicator.strip():
            continue
        if sig.get("direction") not in _VALID_MACRO_DIRECTIONS:
            continue
        implication = sig.get("americhem_implication")
        if not isinstance(implication, str) or not implication.strip():
            continue
        segments_raw = sig.get("affected_segments")
        if not isinstance(segments_raw, list):
            continue
        segments = [s for s in segments_raw if s in insight.VALID_COMMERCIAL_SEGMENTS]
        if not segments:
            continue
        citations = _clean_citation_ids(sig.get("citation_source_ids"), valid_source_ids)
        if not citations:  # materiality gate — an uncitable signal is dropped
            continue
        cleaned.append({
            "indicator": indicator.strip(),
            "direction": sig["direction"],
            "americhem_implication": implication.strip(),
            "affected_segments": segments,
            "citation_source_ids": citations,
        })
        if len(cleaned) >= MAX_MACRO_OUTLOOK_SIGNALS:
            break

    if not cleaned:
        return None
    return {"current_condition": current.strip(), "signals": cleaned}


def generate_macro_summary(
    articles: list[dict],
    *,
    screened_count: Optional[int] = None,
    suppression_breakdown: Optional[dict] = None,
    suppression_samples: Optional[list] = None,
) -> bool:
    """Generate a structured macro summary from today's stored articles.

    Writes dominant_condition (constrained enum) and executive_bullets (3-bullet
    JSON) to daily_summaries. Also populates legacy executive_summary and
    macro_sentiment columns for backward compatibility.

    The optional keyword args persist ingestion-side suppression accounting:
    - screened_count: total URLs discovered (stats["urls_discovered"])
    - suppression_breakdown: reason-code counters dict
    - suppression_samples: list of up to 10 suppressed-item samples
    """
    if not articles:
        logger.warning("No articles to summarize — skipping macro summary generation.")
        return False

    mp = prompts.macro_prompt(articles)
    source_pack = list(mp.source_pack)
    valid_source_ids = frozenset(s["id"] for s in source_pack)

    parsed = _llm().complete_json(**mp.kwargs())
    if parsed is None:
        logger.error("Macro summary generation failed — no usable LLM response.")
        return False

    # Validate dominant_condition.
    cond_raw = parsed.get("dominant_condition")
    if cond_raw not in _VALID_MACRO_CONDITIONS:
        cond = "Low Signal" if len(articles) < 3 else "Mixed / Watch"
    else:
        cond = cond_raw

    # Validate executive_bullets (cleans per-bullet citation_source_ids against the pack).
    bullets = _validate_executive_bullets(parsed.get("executive_bullets"), valid_source_ids)

    # Low Signal: force the third bullet body.
    if bullets is not None and cond == "Low Signal":
        bullets[2] = {
            "label": _EXEC_BULLET_LABELS[2],
            "body": "No action required.",
            "citation_source_ids": [],
        }

    # Validate the structured macro outlook (None -> delivery renders no section).
    macro_outlook = _validate_macro_outlook(parsed.get("macro_outlook"), valid_source_ids)

    # executive_sources: pack entries cited by at least one surviving bullet OR
    # macro-outlook signal — the union, so every rendered citation id (in either
    # section) resolves against one shared numbering space.
    cited_ids: set[int] = set()
    if bullets is not None:
        for b in bullets:
            cited_ids.update(b["citation_source_ids"])
    if macro_outlook is not None:
        for sig in macro_outlook["signals"]:
            cited_ids.update(sig["citation_source_ids"])
    executive_sources = [s for s in source_pack if s["id"] in cited_ids]

    # Legacy executive_summary string for backward compat.
    if bullets is not None:
        executive_summary = " ".join(f"{b['label']}: {b['body']}" for b in bullets)
    else:
        executive_summary = "Macro summary unavailable today."

    from datetime import date
    _repo().upsert_summary({
        "run_date": date.today().isoformat(),
        "run_mode": _run_mode(),
        "dominant_condition": cond,
        "executive_bullets": bullets,
        "macro_outlook": macro_outlook,
        "executive_sources": executive_sources,
        "executive_summary": executive_summary,
        "macro_sentiment": cond,
        "screened_count": screened_count,
        "suppression_breakdown": suppression_breakdown or {},
        "suppression_samples": suppression_samples or [],
    })
    logger.info("Macro summary upserted — condition: %s", cond)
    return True


def _log_stats(stats: dict, breakdown: dict[str, int]) -> None:
    logger.info(
        "Pipeline complete — discovered: %d | duplicates skipped: %d | "
        "semantic duplicates: %d | scrape failed: %d | discards: %d | "
        "scrapes attempted: %d | stored: %d | errors: %d",
        stats["urls_discovered"],
        breakdown.get("duplicate_url", 0),
        breakdown.get("semantic_duplicate", 0),
        breakdown.get("scrape_failed", 0),
        breakdown.get("llm_discard", 0),
        stats["scrapes_attempted"],
        stats["insights_stored"],
        stats["errors"],
    )


def execute_pipeline() -> None:
    pipeline_start = time.monotonic()
    targets = load_targets("targets.yaml")
    target_metadata = (
        relevance_gate.load_target_metadata("target_metadata.yaml")
        if _relevance_gate_enabled() else {}
    )
    seen_headlines: set[str] = _hydrate_seen_headlines()
    scrapes_attempted = 0
    stats = {
        "urls_discovered": 0,
        "scrapes_attempted": 0,
        "insights_stored": 0,
        "errors": 0,
    }
    stored_articles_buffer: list[dict] = []
    suppression_ledger = SuppressionLedger.for_ingestion()
    provider_yield: dict[str, dict] = {}

    def _bump(provider: str, key: str) -> None:
        provider_yield.setdefault(provider, _new_provider_yield())[key] += 1

    for target in targets:
        if time.monotonic() - pipeline_start >= PIPELINE_DEADLINE_SECONDS:
            logger.warning(
                "Pipeline deadline (%ds) reached before processing target '%s' — stopping early.",
                PIPELINE_DEADLINE_SECONDS, target["name"],
            )
            break

        entity_name = target["name"]
        category = target["category"]
        min_article_length = target["min_article_length"]

        candidates = discover_candidates(target)
        stats["urls_discovered"] += len(candidates)
        for candidate in candidates:
            _bump(candidate.get("provider", "unknown"), "discovered")

        # Surface a yield line for every attempted provider, even at zero
        # discovery, so the smoke clearly shows whether ZoomInfo ran. Serper is
        # always attempted; ZoomInfo only when the target is eligible.
        provider_yield.setdefault("serper", _new_provider_yield())
        if _zoominfo_target_eligible(target):
            provider_yield.setdefault("zoominfo", _new_provider_yield())

        for candidate in candidates:
            raw_url = candidate["url"]
            candidate_title = candidate.get("title", "")
            provider = candidate.get("provider", "unknown")

            if time.monotonic() - pipeline_start >= PIPELINE_DEADLINE_SECONDS:
                logger.warning(
                    "Pipeline deadline (%ds) reached mid-batch — stopping early.",
                    PIPELINE_DEADLINE_SECONDS,
                )
                _log_stats(stats, suppression_ledger.breakdown)
                _log_provider_yield(provider_yield)
                generate_macro_summary(
                    stored_articles_buffer,
                    screened_count=stats["urls_discovered"],
                    **suppression_ledger.to_row(),
                )
                return

            if scrapes_attempted >= MAX_DAILY_SCRAPES:
                logger.warning("MAX_DAILY_SCRAPES (%d) reached — stopping.", MAX_DAILY_SCRAPES)
                _log_stats(stats, suppression_ledger.breakdown)
                _log_provider_yield(provider_yield)
                generate_macro_summary(
                    stored_articles_buffer,
                    screened_count=stats["urls_discovered"],
                    **suppression_ledger.to_row(),
                )
                return

            normalized = normalize_url(raw_url)
            url_hash = compute_url_hash(normalized)

            if url_already_processed(url_hash):
                logger.info("Duplicate — skipping (%s): %s", provider, normalized)
                _bump(provider, "duplicates")
                suppression_ledger = suppression_ledger.record(
                    "duplicate_url", url=raw_url, title=candidate_title,
                )
                continue

            is_dup, matched, score = is_semantic_duplicate(candidate_title, seen_headlines)
            if is_dup:
                logger.warning(
                    "SEMANTIC_DUPLICATE — skipped (%s): '%s' ~ '%s' | score: %d",
                    provider, candidate_title, matched, score,
                )
                _bump(provider, "duplicates")
                suppression_ledger = suppression_ledger.record(
                    "semantic_duplicate", url=raw_url, title=candidate_title,
                )
                continue

            if _is_unscrapable_domain(raw_url):
                logger.info("UNSCRAPABLE_DOMAIN — skipped pre-scrape (%s): %s", provider, normalized)
                _bump(provider, "unscrapable")
                suppression_ledger = suppression_ledger.record(
                    "unscrapable_domain", url=raw_url, title=candidate_title,
                )
                continue

            gate_decision = _gate_zoominfo_candidate(candidate, entity_name, target_metadata)
            if gate_decision is not None and gate_decision.drop:
                logger.info(
                    "RELEVANCE_GATE drop (%s): exclude=%r no identity rescue | %s",
                    provider, gate_decision.matched_exclude, normalized,
                )
                _bump(provider, "relevance_dropped")
                suppression_ledger = suppression_ledger.record(
                    gate_decision.reason, url=raw_url, title=candidate_title,
                )
                continue

            scrapes_attempted += 1
            stats["scrapes_attempted"] += 1
            _bump(provider, "scraped")

            article_text = scrape_article(raw_url, min_article_length)
            if article_text is None:
                _bump(provider, "scrape_failed")
                suppression_ledger = suppression_ledger.record(
                    "scrape_failed", url=raw_url, title=candidate_title,
                )
                continue

            article_insight = synthesize_insight(article_text, normalized, entity_name, category)
            if article_insight is None:
                stats["errors"] += 1
                time.sleep(1.5)
                continue

            if insight.is_discard(article_insight):
                logger.info("DISCARD — false positive (%s): %s", provider, normalized)
                _bump(provider, "discards")
                suppression_ledger = suppression_ledger.record(
                    "llm_discard", url=raw_url, title=candidate_title,
                )
                time.sleep(1.5)
                continue

            payload = {
                "headline": article_insight["headline"],
                "americhem_impact": article_insight["americhem_impact"],
                "sentiment_score": article_insight["sentiment_score"],
                "source_url": article_insight["source_url"],
                "url_hash": url_hash,
                "entities_mentioned": article_insight["entities_mentioned"],
                "category": category,
                "trigger_entity": entity_name,
                "source_publication": article_insight.get("source_publication", ""),
                "sentiment_rationale": article_insight.get("sentiment_rationale", ""),
                "recommended_action": article_insight.get("recommended_action", "Monitor"),
                "article_summary": article_insight.get("article_summary", ""),
                "sentiment_tag": article_insight.get("sentiment_tag", "Neutral"),
                "americhem_impact_score": article_insight.get("americhem_impact_score", 5),
                "impact_rationale": article_insight.get("impact_rationale", ""),
                "commercial_segment": article_insight.get("commercial_segment", "Enterprise / Cross-Segment"),
                "signal_type": article_insight.get("signal_type", "Other"),
            }
            # Discovery provenance is gated behind STORE_DISCOVERY_METADATA so
            # production upserts keep working until migration 003 is applied.
            if _store_discovery_metadata():
                payload.update(_discovery_metadata(candidate))

            try:
                store_insight(payload)
            except Exception as exc:
                logger.error("Failed to store insight for %s: %s", normalized, exc)
                stats["errors"] += 1
            else:
                logger.info(
                    "Stored [provider=%s, impact=%d, sentiment=%s] %s",
                    provider,
                    article_insight.get("americhem_impact_score", 5),
                    article_insight.get("sentiment_tag", "Neutral"),
                    article_insight["headline"],
                )
                stats["insights_stored"] += 1
                _bump(provider, "stored")
                stored_articles_buffer.append(payload)
                seen_headlines.add(article_insight["headline"])

            time.sleep(1.5)

    _log_stats(stats, suppression_ledger.breakdown)
    _log_provider_yield(provider_yield)
    generate_macro_summary(
        stored_articles_buffer,
        screened_count=stats["urls_discovered"],
        **suppression_ledger.to_row(),
    )


if __name__ == "__main__":
    execute_pipeline()
