import hashlib
import logging
import os
import time
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
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
import config
from discovery import _discovery_providers

import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


MAX_DAILY_SCRAPES = 180
PIPELINE_DEADLINE_SECONDS = 1800  # stop ingestion after 30 min to stay inside the 40-min CI limit
# Protected tail budget: once remaining scrape slots or wall-clock fall to the
# reserve floor, remaining ENTITY targets are skipped so the concept/macro groups
# at the bottom of targets.yaml always get discovery. Entity coverage is redundant
# day-over-day (dedup absorbs re-discoveries); concept/macro coverage is not.
# The slot reserve is derived per-run and position-aware: at each entity target
# it reserves only the concept demand still AHEAD in file order (see
# _concept_demand_ahead), so adding concept groups or raising results_per_entity
# cannot silently reopen the starvation gap, and front-loading priority concepts
# (Tier 1) cannot over-reserve and finish under cap. Only the time floor is a
# constant (sized for ~40+ scrape attempts at the observed ~9s each).
TAIL_RESERVE_SECONDS = 360
FIRECRAWL_WALL_CLOCK_TIMEOUT = 20  # hard per-request ceiling; prevents keepalive-induced hangs
_SEMANTIC_DUPLICATE_THRESHOLD: int = 88


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
                    "search_mode": "entity",
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
            # Concept groups may declare their own results_per_entity to raise
            # discovery volume for priority segments; absent one, inherit the
            # global discovery value. Concept-only — a stray override on an
            # entity group is ignored (raising entity volume is not intended,
            # and macro groups stay at the global value so the derived tail
            # reserve — _tail_scrape_demand — is not inflated).
            group_results_per_entity: int = group_cfg.get(
                "results_per_entity", results_per_entity
            )
            targets.append({
                "name": group_name,
                "category": group_name,
                "search_mode": "concept",
                "query": build_query(
                    "concept",
                    include_any=group_cfg.get("include_any", []),
                    include_all=include_all,
                    exclude_any=exclude_any,
                ),
                "results_per_entity": group_results_per_entity,
                "lookback_hours": lookback_hours,
                "min_article_length": min_article_length,
            })

    logger.info("Loaded %d active targets from %s", len(targets), config_path)
    return targets


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


_LIFECYCLE_YIELD_KEYS: tuple[str, ...] = ("discovered", "scraped", "stored")


def _new_provider_yield() -> dict:
    """One counter per lifecycle stage plus one per suppression yield key —
    the latter derived from _YIELD_KEY_FOR_REASON, so a new reason code
    cannot miss its counter."""
    suppression_keys = sorted(set(_YIELD_KEY_FOR_REASON.values()))
    return {k: 0 for k in _LIFECYCLE_YIELD_KEYS + tuple(suppression_keys)}


def _log_provider_yield(provider_yield: dict[str, dict]) -> None:
    """Emit one yield line per discovery provider seen this run."""
    for provider in sorted(provider_yield):
        y = provider_yield[provider]
        logger.info(
            "Provider yield — %s discovered=%d scraped=%d stored=%d "
            "discards=%d relevance_dropped=%d scrape_failed=%d unscrapable=%d duplicates=%d "
            "synthesis_failed=%d",
            provider, y["discovered"], y["scraped"], y["stored"],
            y["discards"], y["relevance_dropped"], y["scrape_failed"],
            y["unscrapable"], y["duplicates"], y["synthesis_failed"],
        )


def discover_candidates(target: dict, providers: list) -> list[dict]:
    """Fan in every eligible discovery provider for a target.

    Each provider is isolated: a failure in one never suppresses the others.
    Providers are consulted in registry order (Serper before ZoomInfo), so a
    Serper hit stores first and ZoomInfo's copy of the same article dedupes.
    """
    candidates: list[dict] = []
    for provider in providers:
        if not provider.eligible(target):
            continue
        try:
            candidates.extend(provider.discover(target))
        except Exception as exc:
            logger.error(
                "%s discovery failed for target '%s': %s",
                provider.name, target.get("name"), exc,
            )
    return candidates


UNSCRAPABLE_DOMAINS: frozenset[str] = frozenset({
    # Login-walled or bot-blocked platforms — suffix match: every subdomain
    # (uk.linkedin.com, m.facebook.com) is equally unscrapable.
    "linkedin.com", "facebook.com", "instagram.com", "x.com", "twitter.com",
    "youtube.com", "tiktok.com", "reddit.com",
})

UNSCRAPABLE_HOSTS: frozenset[str] = frozenset({
    # Retail storefronts — exact host match only: product pages are never
    # articles, but corporate newsroom subdomains (corporate.walmart.com,
    # corporate.homedepot.com) publish legitimate news and must stay scrapable.
    "amazon.com", "www.amazon.com",
    "ebay.com", "www.ebay.com",
    "walmart.com", "www.walmart.com",
    "homedepot.com", "www.homedepot.com",
    "lowes.com", "www.lowes.com",
})


def _is_unscrapable_domain(url: str) -> bool:
    """True when the URL's host is a retail storefront (exact match) or is
    (a subdomain of) a login-walled platform we never scrape — both waste the
    Firecrawl budget. Malformed URLs return False (let the scraper decide)."""
    host = (urlparse(url).hostname or "").lower()
    if host in UNSCRAPABLE_HOSTS:
        return True
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

    executor = ThreadPoolExecutor(max_workers=1)
    try:
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
    finally:
        # wait=False: after a wall-clock timeout the worker thread may still be
        # blocked inside requests.post; waiting for it (the `with` default)
        # would defeat the ceiling. The orphaned thread exits on its own when
        # the underlying request times out.
        # Caveat: concurrent.futures' atexit hook still joins orphaned threads
        # at interpreter shutdown, so a hang on the run's final scrape can delay
        # process exit (not the pipeline loop) by up to the inner 30s timeouts.
        executor.shutdown(wait=False)
    data = response.json()
    markdown: str = data.get("data", {}).get("markdown", "") or ""
    if len(markdown) < min_length:
        logger.info("Article too short (%d chars, min %d): %s", len(markdown), min_length, url)
        return None
    return markdown


def synthesize_insight(article_text: str, source_url: str, trigger_entity: str, category: str) -> Optional[dict]:
    spec = prompts.insight_prompt(
        config.mp_config(),
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

    Accounting survives every run (issue #43): when no summary can be
    generated (zero stored articles, or an unusable LLM response), an
    accounting-only row is upserted instead. Content columns are OMITTED from
    that payload — Supabase upsert only updates provided columns, so a
    same-day retry never wipes an earlier full summary.
    """
    from datetime import date
    accounting_row = {
        "run_date": date.today().isoformat(),
        "run_mode": config.run_mode(),
        "screened_count": screened_count,
        "suppression_breakdown": suppression_breakdown or {},
        "suppression_samples": suppression_samples or [],
    }

    if not articles:
        logger.warning(
            "No articles to summarize — persisting accounting-only summary row."
        )
        _repo().upsert_summary(accounting_row)
        return False

    mp = prompts.macro_prompt(articles)
    source_pack = list(mp.source_pack)
    valid_source_ids = frozenset(s["id"] for s in source_pack)

    parsed = _llm().complete_json(**mp.kwargs())
    if parsed is None:
        logger.error(
            "Macro summary generation failed — no usable LLM response; "
            "persisting accounting-only summary row."
        )
        _repo().upsert_summary(accounting_row)
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

    _repo().upsert_summary({
        **accounting_row,
        "dominant_condition": cond,
        "executive_bullets": bullets,
        "macro_outlook": macro_outlook,
        "executive_sources": executive_sources,
        "executive_summary": executive_summary,
        "macro_sentiment": cond,
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


def _tail_scrape_demand(targets: list[dict]) -> int:
    """Worst-case scrape attempts *all* concept/macro targets can consume: every
    concept target yields its full results_per_entity as new scrapable URLs
    (true on any day their queries surface fresh news — dedup only absorbs
    re-discoveries). This is the total configured concept demand; the entity
    gate reserves only the slice still AHEAD of it (see _concept_demand_ahead)."""
    return sum(
        int(t.get("results_per_entity", 0))
        for t in targets
        if t.get("search_mode") == "concept"
    )


def _concept_demand_ahead(targets: list[dict]) -> list[int]:
    """Suffix sums of concept scrape demand: element ``i`` is the worst-case
    demand of ``targets[i:]``. The entity gate must reserve only the concept
    demand that still lies AHEAD of the current target — front-loaded priority
    concepts (Tier 1) have already run, so counting them (as a single static
    total would) over-reserves and skips entity targets the budget could still
    afford, finishing under cap. Order-independent: reordering targets.yaml can
    never silently reopen the starvation gap nor over-protect a run."""
    suffix = [0] * (len(targets) + 1)
    for i in range(len(targets) - 1, -1, -1):
        demand = (
            int(targets[i].get("results_per_entity", 0))
            if targets[i].get("search_mode") == "concept"
            else 0
        )
        suffix[i] = suffix[i + 1] + demand
    return suffix


@dataclass(frozen=True)
class Stored:
    """Candidate survived every gate and was persisted."""


@dataclass(frozen=True)
class Suppressed:
    """Candidate was dropped by a gate; `reason` is the ledger taxonomy code."""
    reason: str


@dataclass(frozen=True)
class Error:
    """Candidate failed on a technical error that is not a suppression (store)."""


# Which provider_yield counter each ingestion suppression reason bumps. Keyed by
# the full ingestion taxonomy so ctx.suppress cannot record without bumping —
# a new reason code without a yield key fails loudly (pinned by a parity test).
_YIELD_KEY_FOR_REASON: dict[str, str] = {
    "duplicate_url": "duplicates",
    "semantic_duplicate": "duplicates",
    "unscrapable_domain": "unscrapable",
    "zoominfo_company_mismatch": "relevance_dropped",
    "scrape_failed": "scrape_failed",
    "synthesis_failed": "synthesis_failed",
    "llm_discard": "discards",
}


def _new_run_stats() -> dict:
    return {
        "urls_discovered": 0,
        "scrapes_attempted": 0,
        "insights_stored": 0,
        "errors": 0,
    }


@dataclass
class RunContext:
    """Mutable run-state threaded through the candidate gauntlet — plus
    `providers_by_name`, read-only wiring for the gate dispatch (never mutated).

    The immutable SuppressionLedger lives here as `ledger`, reassigned in place
    by suppress() — callers never thread a new ledger back by hand.
    """
    providers_by_name: dict
    seen_headlines: set = field(default_factory=set)
    stats: dict = field(default_factory=_new_run_stats)
    provider_yield: dict = field(default_factory=dict)
    stored_articles_buffer: list = field(default_factory=list)
    ledger: SuppressionLedger = field(default_factory=SuppressionLedger.for_ingestion)

    @property
    def scrapes_attempted(self) -> int:
        """The scrape-budget counter the loop's cap and tail-reserve gates
        read — one definition, stored in stats."""
        return self.stats["scrapes_attempted"]

    def bump(self, provider: str, key: str) -> None:
        self.provider_yield.setdefault(provider, _new_provider_yield())[key] += 1

    def suppress(self, reason: str, provider: str, *, url: str, title: str) -> Suppressed:
        """Record a suppression and bump the paired provider-yield counter —
        one call, so the pairing cannot be forgotten at any gate."""
        self.ledger = self.ledger.record(reason, url=url, title=title)
        self.bump(provider, _YIELD_KEY_FOR_REASON[reason])
        return Suppressed(reason)


def process_candidate(candidate: dict, target: dict, ctx: RunContext) -> "Stored | Suppressed | Error":
    """Run one discovered candidate through the per-candidate gauntlet."""
    raw_url = candidate["url"]
    candidate_title = candidate.get("title", "")
    provider = candidate.get("provider", "unknown")

    normalized = normalize_url(raw_url)
    url_hash = compute_url_hash(normalized)

    if url_already_processed(url_hash):
        logger.info("Duplicate — skipping (%s): %s", provider, normalized)
        return ctx.suppress("duplicate_url", provider, url=raw_url, title=candidate_title)

    is_dup, matched, score = is_semantic_duplicate(candidate_title, ctx.seen_headlines)
    if is_dup:
        logger.warning(
            "SEMANTIC_DUPLICATE — skipped (%s): '%s' ~ '%s' | score: %d",
            provider, candidate_title, matched, score,
        )
        return ctx.suppress("semantic_duplicate", provider, url=raw_url, title=candidate_title)

    if _is_unscrapable_domain(raw_url):
        logger.info("UNSCRAPABLE_DOMAIN — skipped pre-scrape (%s): %s", provider, normalized)
        return ctx.suppress("unscrapable_domain", provider, url=raw_url, title=candidate_title)

    # The provider owns its own false-positive gate (Serper has none);
    # the consumer applies the decision so suppression accounting stays
    # in the ledger. No provider-name literal leaks here.
    provider_obj = ctx.providers_by_name.get(provider)
    gate_decision = provider_obj.gate(candidate, target) if provider_obj else None
    if gate_decision is not None and gate_decision.drop:
        logger.info(
            "RELEVANCE_GATE drop (%s): exclude=%r no identity rescue | %s",
            provider, gate_decision.matched_exclude, normalized,
        )
        return ctx.suppress(gate_decision.reason, provider, url=raw_url, title=candidate_title)

    ctx.stats["scrapes_attempted"] += 1
    ctx.bump(provider, "scraped")

    article_text = scrape_article(raw_url, target["min_article_length"])
    if article_text is None:
        return ctx.suppress("scrape_failed", provider, url=raw_url, title=candidate_title)

    article_insight = synthesize_insight(article_text, normalized, target["name"], target["category"])
    # Every exit below has spent an LLM call; the finally paces successive
    # OpenAI requests — one sleep, instead of one copy per exit path.
    try:
        if article_insight is None:
            return ctx.suppress("synthesis_failed", provider, url=raw_url, title=candidate_title)

        if insight.is_discard(article_insight):
            logger.info("DISCARD — false positive (%s): %s", provider, normalized)
            return ctx.suppress("llm_discard", provider, url=raw_url, title=candidate_title)

        payload = {
            "headline": article_insight["headline"],
            "americhem_impact": article_insight["americhem_impact"],
            "sentiment_score": article_insight["sentiment_score"],
            "source_url": article_insight["source_url"],
            "url_hash": url_hash,
            "entities_mentioned": article_insight["entities_mentioned"],
            "category": target["category"],
            "trigger_entity": target["name"],
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
        if config.store_discovery_metadata():
            payload.update(_discovery_metadata(candidate))

        try:
            store_insight(payload)
        except Exception as exc:
            logger.error("Failed to store insight for %s: %s", normalized, exc)
            ctx.stats["errors"] += 1
            return Error()

        logger.info(
            "Stored [provider=%s, impact=%d, sentiment=%s] %s",
            provider,
            article_insight.get("americhem_impact_score", 5),
            article_insight.get("sentiment_tag", "Neutral"),
            article_insight["headline"],
        )
        ctx.stats["insights_stored"] += 1
        ctx.bump(provider, "stored")
        ctx.stored_articles_buffer.append(payload)
        ctx.seen_headlines.add(article_insight["headline"])
        return Stored()
    finally:
        time.sleep(1.5)


def _finalize_run(ctx: RunContext) -> None:
    """The single end-of-run teardown: flush stats and provider yield, then
    persist the macro summary (or the accounting-only row). Called at every
    pipeline exit — mid-batch deadline, scrape cap, and normal completion."""
    _log_stats(ctx.stats, ctx.ledger.breakdown)
    _log_provider_yield(ctx.provider_yield)
    generate_macro_summary(
        ctx.stored_articles_buffer,
        screened_count=ctx.stats["urls_discovered"],
        **ctx.ledger.to_row(),
    )


def execute_pipeline() -> None:
    pipeline_start = time.monotonic()
    targets = load_targets("targets.yaml")
    concept_demand_ahead = _concept_demand_ahead(targets)
    providers = _discovery_providers()
    ctx = RunContext(
        providers_by_name={p.name: p for p in providers},
        seen_headlines=_hydrate_seen_headlines(),
    )

    tail_reserve_triggered = False
    for target_index, target in enumerate(targets):
        if time.monotonic() - pipeline_start >= PIPELINE_DEADLINE_SECONDS:
            logger.warning(
                "Pipeline deadline (%ds) reached before processing target '%s' — stopping early.",
                PIPELINE_DEADLINE_SECONDS, target["name"],
            )
            break

        # Protected tail budget: once either budget falls to its reserve floor,
        # stop starting ENTITY targets so the concept/macro groups still ahead
        # in targets.yaml get their discovery pass. Concept targets keep running
        # until the hard cap/deadline above actually fires. The slot reserve is
        # position-aware — only the concept demand AHEAD of this target — so
        # front-loaded priority concepts (Tier 1), already run, don't get
        # double-reserved and starve the entity tier below its affordable budget.
        if target.get("search_mode", "entity") == "entity":
            tail_reserve_scrapes = concept_demand_ahead[target_index]
            slots_low = ctx.scrapes_attempted >= MAX_DAILY_SCRAPES - tail_reserve_scrapes
            clock_low = (
                time.monotonic() - pipeline_start
                >= PIPELINE_DEADLINE_SECONDS - TAIL_RESERVE_SECONDS
            )
            if slots_low or clock_low:
                if not tail_reserve_triggered:
                    logger.warning(
                        "Tail reserve reached (%s) at target '%s' — skipping remaining "
                        "entity targets to protect concept/macro coverage.",
                        "scrape slots" if slots_low else "wall clock", target["name"],
                    )
                    tail_reserve_triggered = True
                continue

        target_start = time.monotonic()

        # Surface a yield line for every eligible provider, even at zero
        # discovery, so the smoke clearly shows whether each provider ran. The
        # seeding is provider-list-driven — no hard-coded provider names.
        for provider_obj in providers:
            if provider_obj.eligible(target):
                ctx.provider_yield.setdefault(provider_obj.name, _new_provider_yield())

        candidates = discover_candidates(target, providers)
        ctx.stats["urls_discovered"] += len(candidates)
        for candidate in candidates:
            ctx.bump(candidate.get("provider", "unknown"), "discovered")

        for candidate in candidates:
            if time.monotonic() - pipeline_start >= PIPELINE_DEADLINE_SECONDS:
                logger.warning(
                    "Pipeline deadline (%ds) reached mid-batch — stopping early.",
                    PIPELINE_DEADLINE_SECONDS,
                )
                _finalize_run(ctx)
                return

            if ctx.scrapes_attempted >= MAX_DAILY_SCRAPES:
                logger.warning("MAX_DAILY_SCRAPES (%d) reached — stopping.", MAX_DAILY_SCRAPES)
                _finalize_run(ctx)
                return

            process_candidate(candidate, target, ctx)

        logger.info(
            "Target '%s' processed in %.1fs (%d candidates)",
            target["name"], time.monotonic() - target_start, len(candidates),
        )

    _finalize_run(ctx)


def main() -> None:
    """Cron entrypoint: fail fast on missing secrets, then run the pipeline."""
    config.validate_environment("ingestion")
    execute_pipeline()


if __name__ == "__main__":
    main()
