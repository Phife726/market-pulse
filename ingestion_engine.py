import hashlib
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from html.parser import HTMLParser
from urllib.parse import urlparse, urlunparse
from typing import Callable, Optional

from suppression_ledger import SuppressionLedger
from daily_intelligence_repo import _repo
from llm import _llm
from run_instant import RunInstant
from run_budget import RunBudget, SkipEntity, Stop
from targets import load_targets
import insight
import prompts
# The macro summary's schema/validation + pure assembly live in macro_summary.py
# (the run-level twin of insight.py); generate_macro_summary keeps only the LLM
# call and the daily_summaries upsert.
from macro_summary import assemble_macro_content
import config
from discovery import _discovery_providers

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


FIRECRAWL_WALL_CLOCK_TIMEOUT = 20  # hard per-request ceiling; prevents keepalive-induced hangs
_SEMANTIC_DUPLICATE_THRESHOLD: int = 88


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


def generate_macro_summary(
    articles: list[dict],
    *,
    run: RunInstant,
    screened_count: Optional[int] = None,
    suppression_breakdown: Optional[dict] = None,
    suppression_samples: Optional[list] = None,
) -> bool:
    """Generate a structured macro summary from the run's stored articles.

    Writes dominant_condition (constrained enum) and executive_bullets (3-bullet
    JSON) to daily_summaries. Also populates legacy executive_summary and
    macro_sentiment columns for backward compatibility.

    `run` is the run instant: its run_date + run_mode key the daily_summaries
    row this run writes. The optional keyword args persist ingestion-side
    suppression accounting:
    - screened_count: total URLs discovered (stats["urls_discovered"])
    - suppression_breakdown: reason-code counters dict
    - suppression_samples: list of up to 10 suppressed-item samples

    Accounting survives every run (issue #43): when no summary can be
    generated (zero stored articles, or an unusable LLM response), an
    accounting-only row is upserted instead. Content columns are OMITTED from
    that payload — Supabase upsert only updates provided columns, so a
    same-day retry never wipes an earlier full summary.
    """
    accounting_row = {
        "run_date": run.run_date,
        "run_mode": run.run_mode,
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

    parsed = _llm().complete_json(**mp.kwargs())
    if parsed is None:
        logger.error(
            "Macro summary generation failed — no usable LLM response; "
            "persisting accounting-only summary row."
        )
        _repo().upsert_summary(accounting_row)
        return False

    content = assemble_macro_content(parsed, source_pack=source_pack, article_count=len(articles))
    _repo().upsert_summary({**accounting_row, **content})
    logger.info("Macro summary upserted — condition: %s", content["dominant_condition"])
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


#: Below this many failed synthesis calls a zero-store run is too thin a sample
#: to suppress delivery over — one flaky LLM response on an otherwise quiet day
#: must still send the legitimate no-news email.
SYNTHESIS_OUTAGE_MIN_ATTEMPTS = 3


class SynthesisOutageError(RuntimeError):
    """Every LLM synthesis call this run failed — the run produced no
    intelligence for an upstream reason, not because the news was quiet."""


def is_synthesis_outage(stats: dict, breakdown: dict[str, int]) -> bool:
    """True when this run asked the LLM for synthesis and *every* call failed
    while nothing was stored.

    The failure this catches is silent by construction: `llm.complete_json`
    swallows transport errors to None so a provider blip cannot crash the cron,
    the ledger records `synthesis_failed`, and the run exits green having
    stored nothing — indistinguishable, downstream, from a genuinely quiet news
    day (observed 2026-08-03: expired OpenAI credits, 98/98 calls rejected,
    ~108 scrapes billed, a no-news email delivered).

    A discard is a *successful* call that judged an article irrelevant, so any
    discard proves the LLM answered and rules the outage out.
    """
    failed = breakdown.get("synthesis_failed", 0)
    attempted = stats["insights_stored"] + breakdown.get("llm_discard", 0) + failed
    return (
        stats["insights_stored"] == 0
        and failed >= SYNTHESIS_OUTAGE_MIN_ATTEMPTS
        and failed == attempted
    )


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


def _finalize_run(ctx: RunContext, run: RunInstant) -> None:
    """The single end-of-run teardown: flush stats and provider yield, then
    persist the macro summary (or the accounting-only row) on the row `run`
    keys. The pipeline's single exit: `execute_pipeline` reaches it whether
    the run completed or the run budget stopped it.

    Raises SynthesisOutageError last, so a run whose every synthesis call
    failed still records what it screened before it fails the job.
    """
    _log_stats(ctx.stats, ctx.ledger.breakdown)
    _log_provider_yield(ctx.provider_yield)
    generate_macro_summary(
        ctx.stored_articles_buffer,
        run=run,
        screened_count=ctx.stats["urls_discovered"],
        **ctx.ledger.to_row(),
    )
    if is_synthesis_outage(ctx.stats, ctx.ledger.breakdown):
        raise SynthesisOutageError(
            f"All {ctx.ledger.breakdown.get('synthesis_failed', 0)} LLM synthesis "
            f"call(s) failed and 0 articles were stored across "
            f"{ctx.stats['scrapes_attempted']} scrape attempt(s). This is an "
            "upstream LLM failure (expired credits, revoked key, provider "
            "outage), not a quiet news day — check the [ERROR] 'LLM call failed' "
            "lines above for the provider's reason."
        )


def _run_target(
    target: dict, ctx: RunContext, providers: list, budget: RunBudget, elapsed: Callable[[], float],
) -> Optional[Stop]:
    """Discover one target and run its candidates through the gauntlet, asking
    the budget before each. Returns the `Stop` that cut the batch, else None."""
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
        verdict = budget.before_candidate(
            elapsed=elapsed(), scrapes_attempted=ctx.scrapes_attempted)
        if isinstance(verdict, Stop):
            return verdict
        process_candidate(candidate, target, ctx)

    logger.info(
        "Target '%s' processed in %.1fs (%d candidates)",
        target["name"], time.monotonic() - target_start, len(candidates),
    )
    return None


def execute_pipeline(run: RunInstant, *, budget: Optional[RunBudget] = None) -> None:
    """Run every active target through discovery and the candidate gauntlet
    under `budget` (the default is built from the targets), then tear down
    once through `_finalize_run`. The loop owns the stopwatch; the budget
    decides at its two checkpoints, and a `Stop` from either ends the run at
    the single exit below.
    """
    pipeline_start = time.monotonic()
    targets = load_targets("targets.yaml")
    if budget is None:
        budget = RunBudget.for_targets(targets)
    elif len(budget.entity_at) != len(targets):
        raise ValueError(
            f"run budget was built from a different targets list "
            f"({len(budget.entity_at)} targets) than the one loaded ({len(targets)})"
        )
    providers = _discovery_providers()
    ctx = RunContext(
        providers_by_name={p.name: p for p in providers},
        seen_headlines=_hydrate_seen_headlines(),
    )

    def elapsed() -> float:
        return time.monotonic() - pipeline_start

    tail_reserve_logged = False
    for target_index, target in enumerate(targets):
        verdict = budget.before_target(
            elapsed=elapsed(),
            scrapes_attempted=ctx.scrapes_attempted,
            target_index=target_index,
        )
        if isinstance(verdict, SkipEntity):
            if not tail_reserve_logged:
                logger.warning(
                    "Tail reserve reached (%s) at target '%s' — skipping remaining "
                    "entity targets to protect concept/macro coverage.",
                    verdict.reason, target["name"],
                )
                tail_reserve_logged = True
            continue
        if isinstance(verdict, Stop):
            logger.warning(
                "Run budget exhausted (%s) before target '%s' — %d/%d scrape attempts, "
                "%.0f/%ds elapsed — stopping early.",
                verdict.reason, target["name"], ctx.scrapes_attempted, budget.max_scrapes,
                elapsed(), budget.deadline_seconds,
            )
            break
        stop = _run_target(target, ctx, providers, budget, elapsed)
        if stop is not None:
            logger.warning(
                "Run budget exhausted (%s) mid-batch at target '%s' — %d/%d scrape attempts, "
                "%.0f/%ds elapsed — stopping early.",
                stop.reason, target["name"], ctx.scrapes_attempted, budget.max_scrapes,
                elapsed(), budget.deadline_seconds,
            )
            break

    _finalize_run(ctx, run)


def main() -> None:
    """Cron entrypoint: fail fast on missing secrets, read the run instant
    once, then run the pipeline.

    A synthesis outage exits non-zero rather than raising: the workflow step
    goes red (so the run is visibly broken) and, because the delivery step runs
    only on success, the misleading no-news email is never sent.
    """
    config.validate_environment("ingestion")
    try:
        execute_pipeline(RunInstant.current())
    except SynthesisOutageError as exc:
        logger.critical("SYNTHESIS OUTAGE — delivery suppressed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
