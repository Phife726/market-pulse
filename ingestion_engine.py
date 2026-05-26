import hashlib
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import datetime, timedelta
from html.parser import HTMLParser
from urllib.parse import urlparse, urlunparse
from typing import Optional

from suppression_ledger import SuppressionLedger

import requests
import yaml
from openai import OpenAI
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _run_mode() -> str:
    """Return 'test' when MARKET_PULSE_RUN_MODE=test (case-insensitive), else 'production'."""
    return "test" if os.environ.get("MARKET_PULSE_RUN_MODE", "").strip().lower() == "test" else "production"


MAX_DAILY_SCRAPES = 150
OPENAI_MODEL = "gpt-5.4-nano"
PIPELINE_DEADLINE_SECONDS = 600   # stop ingestion after 10 min to stay inside the 15-min CI limit
FIRECRAWL_WALL_CLOCK_TIMEOUT = 45  # hard per-request ceiling; prevents keepalive-induced hangs
_SEMANTIC_DUPLICATE_THRESHOLD: int = 88

_MP_CONFIG: Optional[dict] = None

_FALLBACK_COMMERCIAL_SEGMENT_LIST = (
    "Healthcare | Fibers | Transportation - Automotive | "
    "Transportation - Non-Automotive | Transportation - Aerospace | "
    "Industrial | Packaging | Engineered Resins | Enterprise / Cross-Segment"
)

_FALLBACK_SIGNAL_TYPE_LIST = (
    "Competitive | Customer | Regulatory | Sustainability | "
    "Supply Chain | Technology | Macro | Other"
)


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


def _build_commercial_segment_rule(config: dict) -> str:
    """Return RULE 4 text with commercial segment labels and descriptions from config."""
    segments = config.get("commercial_segments") or {}
    if not segments:
        block = _FALLBACK_COMMERCIAL_SEGMENT_LIST
    else:
        lines = []
        for seg in segments.values():
            if not isinstance(seg, dict):
                continue
            label = seg.get("label", "")
            desc = (seg.get("description") or "").strip().replace("\n", " ")
            if label:
                lines.append(f"  {label}: {desc}" if desc else f"  {label}")
        block = "\n".join(lines) if lines else _FALLBACK_COMMERCIAL_SEGMENT_LIST

    return f"""RULE 4 — COMMERCIAL SEGMENT:
Assign the single best-fit commercial segment for the affected end-market:

{block}

Choose "Enterprise / Cross-Segment" only when the article spans multiple segments
or addresses Americhem-wide topics with no single end-market dominating."""


def _build_signal_type_rule(config: dict) -> str:
    """Return RULE 5 text with signal type labels and descriptions from config."""
    signals = config.get("signal_types") or {}
    if not signals:
        block = _FALLBACK_SIGNAL_TYPE_LIST
    else:
        lines = []
        for sig in signals.values():
            if not isinstance(sig, dict):
                continue
            label = sig.get("label", "")
            desc = (sig.get("description") or "").strip().replace("\n", " ")
            if label:
                lines.append(f"  {label}: {desc}" if desc else f"  {label}")
        block = "\n".join(lines) if lines else _FALLBACK_SIGNAL_TYPE_LIST

    return f"""RULE 5 — SIGNAL TYPE:
Assign the single kind of signal this article represents:

{block}

Prefer a named type over "Other" whenever possible."""

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



def _get_supabase() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)


def _get_openai() -> OpenAI:
    return OpenAI(api_key=os.environ["OPENAI_API_KEY"])


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
    results = [
        (item["link"], item.get("title", ""))
        for item in data.get("news", [])
        if "link" in item
    ]
    logger.info("Discovered %d URL(s) for query '%s'", len(results), query[:80])
    return results


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    clean = parsed._replace(query="", fragment="")
    return urlunparse(clean)


def compute_url_hash(normalized_url: str) -> str:
    return hashlib.sha256(normalized_url.encode("utf-8")).hexdigest()


def url_already_processed(url_hash: str) -> bool:
    try:
        supabase = _get_supabase()
        result = (
            supabase.table("daily_intelligence")
            .select("url_hash")
            .eq("url_hash", url_hash)
            .limit(1)
            .execute()
        )
        return len(result.data) > 0
    except Exception as exc:
        logger.error("Supabase duplicate-check failed for hash %s: %s", url_hash, exc)
        return False


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


# Cross-reference: an identical-body constant lives in delivery_engine.py.
# Both prompts are gated in CI by tests that assert the same anchor substrings;
# if you reword this, reword the delivery_engine.py copy in lockstep.
_ENGLISH_OUTPUT_RULE = (
    "All human-readable generated strings must be written in clear business English, "
    "regardless of the source article's language. Translate non-English source "
    "content into English. Preserve proper nouns — company names, product names, "
    "brand names, source publications, locations, URLs, and quoted legal or product "
    "identifiers — in their original form when translation would reduce precision. "
    "Enum/taxonomy fields must use the configured English labels exactly."
)

_SYSTEM_PROMPT_BASE = """You are an expert market intelligence analyst for AmI (Americhem Intelligence),
a global manufacturer of custom color masterbatch, functional additives, and engineered compounds
serving automotive, healthcare, packaging, wire and cable, and industrial markets.

Your job is to analyze news articles and extract structured intelligence. You MUST enforce all
eight rules below before generating any output.

RULE 0 — OUTPUT LANGUAGE:
{english_output_rule}

RULE 1 — ENTITY DISAMBIGUATION:
Before scoring, verify that the named entity in this article is the correct one.
- If the article mentions "Dow" verify it refers to Dow Chemical / Dow Inc., not the Dow Jones index.
- If the article mentions "Magna" verify it refers to Magna International, not the Magna Carta.
- If the article mentions "Celanese" verify it is the chemical company, not an unrelated brand.
- If the entity is a false match (wrong Dow, wrong Magna, unrelated brand), output ONLY this JSON:
  {"americhem_impact": "DISCARD"}

RULE 2 — SENTIMENT TAG (directional tone only — NOT importance):
Assign exactly one tag based on the direction of impact for Americhem:
- "Negative": adverse direction — threatens customers, suppliers, demand, margin, operations, or compliance
- "Neutral": informational, mixed, or weakly directional signal
- "Positive": favorable direction — demand growth, margin benefit, competitive advantage, supply opportunity

IMPORTANT: sentiment_tag is direction only. A barely-relevant article can be Negative.
A neutral article can have a high impact score. Do NOT conflate tone with importance.

Also assign sentiment_score (1–10, kept for compatibility) using the same directional logic:
1–3 = Negative range, 4–6 = Neutral range, 7–10 = Positive range.

RULE 3 — AMERICHEM IMPACT SCORE (relevance and materiality, 1–10):
Score how relevant and materially important this article is to Americhem's business,
independent of sentiment direction.

1–2: Barely related. Almost no connection to Americhem's markets or supply chain.
3–4: Indirect exposure only. Weak or speculative connection.
5–6: Moderately relevant. Affects an Americhem segment or supply chain with some certainty.
7–8: Clearly relevant. Direct effect on Americhem's customers, suppliers, costs, or demand.
9–10: High-priority strategic signal. Americhem should act or monitor closely.

Score by weighting these factors:
- Segment fit (30%): directly affects a configured segment below
- Americhem exposure (25%): named customers, end-markets, suppliers, competitors, or geographies
- Business materiality (20%): demand volume, margin, capacity, regulatory risk, or supply risk
- Timeliness/novelty (15%): recent, emerging, disruptive event
- Actionability (10%): Sales or GMM team can take a concrete step

{rule4}

{rule5}

RULE 6 — RIGOROUS IMPACT STATEMENT:
Always write a specific So-What for Americhem even for routine items.
Identify which business unit or cost line could be affected and in what direction.
If truly no commercial connection exists, write: "Indirect exposure only — monitor for [specific reason]."
Do NOT write "No direct impact. Monitoring required." — this phrase is banned.
Do NOT write phrases like "may increase demand" or "could affect" without citing specific data.

RULE 7 — DOMAIN RELEVANCE FIREWALL:
Americhem is a plastics and specialty chemicals manufacturer. Only DISCARD if the article has
absolutely zero connection to plastics, polymers, chemicals, materials, manufacturing,
composites, packaging, or supply chain dynamics.
Examples of noise to DISCARD: sports results, political news, celebrity stories, unrelated
financial instruments (stock tips, crypto), or general HR policy.
When relevance is uncertain, do NOT discard. Set americhem_impact_score to 4 and apply Rule 5.

If the article passes all rules, extract data into this strict JSON schema.
Output ONLY the JSON object — no preamble, no markdown, no explanation.

{
  "headline": "<concise factual summary, max 12 words>",
  "source_publication": "<name of the publisher, e.g. Reuters, Chemical Week, Plastics News>",
  "article_summary": "<2-3 sentences, max 50 words. What happened, who is involved, key numbers. Factual only — no Americhem framing.>",
  "americhem_impact": "<BLUF So What for Americhem. Apply Rule 5. Never generic.>",
  "sentiment_score": <integer 1-10 per Rule 2 directional scale, kept for compatibility>,
  "sentiment_tag": "<exactly one of: Negative | Neutral | Positive per Rule 2>",
  "americhem_impact_score": <integer 1-10 per Rule 3>,
  "impact_rationale": "<max 15 words explaining why this impact score was assigned>",
  "commercial_segment": "<exact label from RULE 4>",
  "signal_type": "<exact label from RULE 5>",
  "sentiment_rationale": "<max 10 words explaining exactly why this sentiment was assigned>",
  "recommended_action": "<one of: No action | Monitor | Flag to procurement | Share with sales | Escalate to leadership>",
  "source_url": "<MUST EXACTLY MATCH the URL provided in the user prompt>",
  "entities_mentioned": ["<companies, chemicals, or regions mentioned>"]
}"""


def _build_system_prompt(config: dict) -> str:
    """Assemble the full system prompt, injecting commercial segment and signal type taxonomies."""
    rule4 = _build_commercial_segment_rule(config)
    rule5 = _build_signal_type_rule(config)
    return (
        _SYSTEM_PROMPT_BASE
        .replace("{english_output_rule}", _ENGLISH_OUTPUT_RULE)
        .replace("{rule4}", rule4)
        .replace("{rule5}", rule5)
    )

_VALID_ACTIONS: frozenset[str] = frozenset({
    "No action", "Monitor", "Flag to procurement",
    "Share with sales", "Escalate to leadership",
})

_VALID_SENTIMENT_TAGS: frozenset[str] = frozenset({"Negative", "Neutral", "Positive"})

_VALID_COMMERCIAL_SEGMENTS: frozenset[str] = frozenset({
    "Healthcare", "Fibers",
    "Transportation - Automotive", "Transportation - Non-Automotive",
    "Transportation - Aerospace",
    "Industrial", "Packaging", "Engineered Resins",
    "Enterprise / Cross-Segment",
})

_VALID_SIGNAL_TYPES: frozenset[str] = frozenset({
    "Competitive", "Customer", "Regulatory", "Sustainability",
    "Supply Chain", "Technology", "Macro", "Other",
})

_VALID_MACRO_CONDITIONS: frozenset[str] = frozenset({
    "Competitive Pressure", "Supply Volatility", "Demand Expansion",
    "Demand Softness", "Regulatory Pressure", "Sustainability Pull",
    "Commercial Opportunity", "Mixed / Watch", "Low Signal",
})

_EXEC_BULLET_LABELS: tuple[str, ...] = (
    "Market pressure", "Supply chain watch", "Commercial action",
)


def synthesize_insight(article_text: str, source_url: str, trigger_entity: str, category: str) -> Optional[dict]:
    client = _get_openai()
    user_prompt = (
        f"Trigger entity: {trigger_entity}\nCategory: {category}\n"
        f"Source URL: {source_url}\n\nArticle text:\n{article_text}"
    )
    system_prompt = _build_system_prompt(_load_mp_config())
    try:
        completion = client.chat.completions.create(
            model=OPENAI_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
        )
    except Exception as exc:
        logger.error("OpenAI API call failed for entity '%s': %s", trigger_entity, exc)
        return None
    raw_content = completion.choices[0].message.content or ""
    try:
        insight = json.loads(raw_content)
    except json.JSONDecodeError as exc:
        logger.error("Failed to parse OpenAI JSON response: %s", exc)
        return None
    if insight.get("americhem_impact") == "DISCARD":
        return insight
    required_keys = {"headline", "americhem_impact", "sentiment_score", "source_url", "entities_mentioned"}
    missing = required_keys - insight.keys()
    if missing:
        logger.error("OpenAI response missing keys %s", missing)
        return None
    try:
        score = int(insight["sentiment_score"])
        insight["sentiment_score"] = max(1, min(10, score))
    except (ValueError, TypeError):
        insight["sentiment_score"] = 5
    # Validate and default new relevance fields.
    if insight.get("sentiment_tag") not in _VALID_SENTIMENT_TAGS:
        insight["sentiment_tag"] = "Neutral"
    try:
        impact = int(insight["americhem_impact_score"])
        insight["americhem_impact_score"] = max(1, min(10, impact))
    except (ValueError, TypeError, KeyError):
        insight["americhem_impact_score"] = 5
    insight.setdefault("impact_rationale", "")
    # commercial_segment validation (RULE 4)
    seg = (insight.get("commercial_segment") or "").strip() if isinstance(insight.get("commercial_segment"), str) else ""
    if seg in _VALID_COMMERCIAL_SEGMENTS:
        insight["commercial_segment"] = seg
    else:
        insight["commercial_segment"] = "Enterprise / Cross-Segment"

    # signal_type validation (RULE 5)
    sig = (insight.get("signal_type") or "").strip() if isinstance(insight.get("signal_type"), str) else ""
    if sig in _VALID_SIGNAL_TYPES:
        insight["signal_type"] = sig
    else:
        insight["signal_type"] = "Other"

    # Drop legacy strategic_segment if the LLM still returns it.
    insight.pop("strategic_segment", None)
    if not isinstance(insight["entities_mentioned"], list):
        insight["entities_mentioned"] = []
    insight.setdefault("source_publication", "")
    insight.setdefault("sentiment_rationale", "")
    insight.setdefault("article_summary", "")
    if insight.get("recommended_action") not in _VALID_ACTIONS:
        insight["recommended_action"] = "Monitor"
    return insight


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
    try:
        supabase = _get_supabase()
        cutoff = (datetime.utcnow() - timedelta(hours=72)).isoformat()
        result = (
            supabase.table("daily_intelligence")
            .select("headline")
            .gte("created_at", cutoff)
            .execute()
        )
        headlines = {str(row["headline"]) for row in result.data or []}
        logger.info("Hydrated seen_headlines buffer with %d entries.", len(headlines))
        return headlines
    except Exception as exc:
        logger.error("Failed to hydrate seen_headlines — semantic dedup disabled: %s", exc)
        return set()


def store_insight(payload: dict) -> bool:
    try:
        supabase = _get_supabase()
        supabase.table("daily_intelligence").upsert(payload, on_conflict="url_hash").execute()
        return True
    except Exception as exc:
        logger.error("Supabase upsert failed: %s", exc)
        return False


def _validate_executive_bullets(raw) -> Optional[list[dict]]:
    """Return the bullets list if valid; None otherwise (delivery falls back to prose).

    Valid shape: exactly 3 objects, with labels matching _EXEC_BULLET_LABELS in order,
    and non-empty string body fields.
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
        cleaned.append({"label": label, "body": body.strip()})
    return cleaned


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

    client = _get_openai()

    article_digest = "\n".join(
        f"- [{a.get('category', '').upper()}] {a.get('headline', '')} "
        f"(Impact {a.get('americhem_impact_score', a.get('sentiment_score', ''))}/10): "
        f"{a.get('americhem_impact', '')}"
        for a in articles
    )

    macro_conditions_text = ", ".join(sorted(_VALID_MACRO_CONDITIONS))
    label_a, label_b, label_c = _EXEC_BULLET_LABELS

    system_prompt = (
        "You are a senior Americhem commercial intelligence analyst writing the morning brief\n"
        "for GMMs and Sales leaders. Output ONLY a JSON object with two keys.\n\n"
        "1. dominant_condition — pick exactly one value from this list that best describes\n"
        "   today's overall commercial weather across the digest:\n"
        f"     {macro_conditions_text}\n\n"
        "2. executive_bullets — exactly three objects, in this order, with these exact labels:\n"
        f'     {{"label": "{label_a}",    "body": "<one sentence, <=30 words>"}}\n'
        f'     {{"label": "{label_b}", "body": "<one sentence, <=30 words>"}}\n'
        f'     {{"label": "{label_c}",  "body": "<one sentence, <=30 words>"}}\n\n'
        '   Each body must reference specific named entities or segments from the digest.\n'
        '   Do NOT hedge ("may", "could", "potentially") without a specific data point.\n'
        '   Do NOT write generic statements ("monitor closely", "remain vigilant").\n\n'
        '   Low-signal special case:\n'
        '   If dominant_condition is "Low Signal", the Commercial action body MUST be the\n'
        '   literal string "No action required." The other two bullets MUST describe the\n'
        '   absence of meaningful signal.'
    )

    user_prompt = (
        f"Today's market intelligence digest for Americhem ({len(articles)} articles):\n\n"
        f"{article_digest}\n\nOutput ONLY the JSON object."
    )

    try:
        completion = client.chat.completions.create(
            model=OPENAI_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
        )
        content = completion.choices[0].message.content
        if content is None:
            raise ValueError("OpenAI returned empty content for macro summary")
        parsed = json.loads(content)
    except Exception as exc:
        logger.error("Failed to generate macro summary from OpenAI: %s", exc)
        return False

    # Validate dominant_condition.
    cond_raw = parsed.get("dominant_condition")
    if cond_raw not in _VALID_MACRO_CONDITIONS:
        cond = "Low Signal" if len(articles) < 3 else "Mixed / Watch"
    else:
        cond = cond_raw

    # Validate executive_bullets.
    bullets = _validate_executive_bullets(parsed.get("executive_bullets"))

    # Low Signal: force the third bullet body.
    if bullets is not None and cond == "Low Signal":
        bullets[2] = {"label": _EXEC_BULLET_LABELS[2], "body": "No action required."}

    # Legacy executive_summary string for backward compat.
    if bullets is not None:
        executive_summary = " ".join(f"{b['label']}: {b['body']}" for b in bullets)
    else:
        executive_summary = "Macro summary unavailable today."

    try:
        from datetime import date
        supabase = _get_supabase()
        supabase.table("daily_summaries").upsert(
            {
                "run_date": date.today().isoformat(),
                "run_mode": _run_mode(),
                "dominant_condition": cond,
                "executive_bullets": bullets,
                "executive_summary": executive_summary,
                "macro_sentiment": cond,
                "screened_count": screened_count,
                "suppression_breakdown": suppression_breakdown or {},
                "suppression_samples": suppression_samples or [],
            },
            on_conflict="run_date,run_mode",
        ).execute()
        logger.info("Macro summary upserted — condition: %s", cond)
        return True
    except Exception as exc:
        logger.error("Failed to upsert macro summary to Supabase: %s", exc)
        return False


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

    for target in targets:
        if time.monotonic() - pipeline_start >= PIPELINE_DEADLINE_SECONDS:
            logger.warning(
                "Pipeline deadline (%ds) reached before processing target '%s' — stopping early.",
                PIPELINE_DEADLINE_SECONDS, target["name"],
            )
            break

        entity_name = target["name"]
        category = target["category"]
        lookback_hours = target["lookback_hours"]
        results_per_entity = target["results_per_entity"]
        min_article_length = target["min_article_length"]

        raw_results = discover_urls(target["query"], lookback_hours, results_per_entity)
        stats["urls_discovered"] += len(raw_results)

        for raw_url, serper_title in raw_results:
            if time.monotonic() - pipeline_start >= PIPELINE_DEADLINE_SECONDS:
                logger.warning(
                    "Pipeline deadline (%ds) reached mid-batch — stopping early.",
                    PIPELINE_DEADLINE_SECONDS,
                )
                _log_stats(stats, suppression_ledger.breakdown)
                generate_macro_summary(
                    stored_articles_buffer,
                    screened_count=stats["urls_discovered"],
                    **suppression_ledger.to_row(),
                )
                return

            if scrapes_attempted >= MAX_DAILY_SCRAPES:
                logger.warning("MAX_DAILY_SCRAPES (%d) reached — stopping.", MAX_DAILY_SCRAPES)
                _log_stats(stats, suppression_ledger.breakdown)
                generate_macro_summary(
                    stored_articles_buffer,
                    screened_count=stats["urls_discovered"],
                    **suppression_ledger.to_row(),
                )
                return

            normalized = normalize_url(raw_url)
            url_hash = compute_url_hash(normalized)

            if url_already_processed(url_hash):
                logger.info("Duplicate — skipping: %s", normalized)
                suppression_ledger = suppression_ledger.record(
                    "duplicate_url", url=raw_url, title=serper_title,
                )
                continue

            is_dup, matched, score = is_semantic_duplicate(serper_title, seen_headlines)
            if is_dup:
                logger.warning(
                    "SEMANTIC_DUPLICATE — skipped: '%s' ~ '%s' | score: %d",
                    serper_title, matched, score,
                )
                suppression_ledger = suppression_ledger.record(
                    "semantic_duplicate", url=raw_url, title=serper_title,
                )
                continue

            scrapes_attempted += 1
            stats["scrapes_attempted"] += 1

            article_text = scrape_article(raw_url, min_article_length)
            if article_text is None:
                suppression_ledger = suppression_ledger.record(
                    "scrape_failed", url=raw_url, title=serper_title,
                )
                time.sleep(1.5)
                continue

            insight = synthesize_insight(article_text, normalized, entity_name, category)
            if insight is None:
                stats["errors"] += 1
                time.sleep(1.5)
                continue

            if insight.get("americhem_impact") == "DISCARD":
                logger.info("DISCARD — false positive: %s", normalized)
                suppression_ledger = suppression_ledger.record(
                    "llm_discard", url=raw_url, title=serper_title,
                )
                time.sleep(1.5)
                continue

            payload = {
                "headline": insight["headline"],
                "americhem_impact": insight["americhem_impact"],
                "sentiment_score": insight["sentiment_score"],
                "source_url": insight["source_url"],
                "url_hash": url_hash,
                "entities_mentioned": insight["entities_mentioned"],
                "category": category,
                "trigger_entity": entity_name,
                "source_publication": insight.get("source_publication", ""),
                "sentiment_rationale": insight.get("sentiment_rationale", ""),
                "recommended_action": insight.get("recommended_action", "Monitor"),
                "article_summary": insight.get("article_summary", ""),
                "sentiment_tag": insight.get("sentiment_tag", "Neutral"),
                "americhem_impact_score": insight.get("americhem_impact_score", 5),
                "impact_rationale": insight.get("impact_rationale", ""),
                "commercial_segment": insight.get("commercial_segment", "Enterprise / Cross-Segment"),
                "signal_type": insight.get("signal_type", "Other"),
            }

            if store_insight(payload):
                logger.info(
                    "Stored [impact=%d, sentiment=%s] %s",
                    insight.get("americhem_impact_score", 5),
                    insight.get("sentiment_tag", "Neutral"),
                    insight["headline"],
                )
                stats["insights_stored"] += 1
                stored_articles_buffer.append(payload)
                seen_headlines.add(insight["headline"])
            else:
                stats["errors"] += 1

            time.sleep(1.5)

    _log_stats(stats, suppression_ledger.breakdown)
    generate_macro_summary(
        stored_articles_buffer,
        screened_count=stats["urls_discovered"],
        **suppression_ledger.to_row(),
    )


if __name__ == "__main__":
    execute_pipeline()
