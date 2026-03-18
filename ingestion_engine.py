import hashlib
import json
import logging
import os
import time
from datetime import datetime
from urllib.parse import urlparse, urlunparse
from typing import Optional

import requests
import yaml
from openai import OpenAI
from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

MAX_DAILY_SCRAPES = 20

# ---------------------------------------------------------------------------
# Clients (initialized once at module level so they can be patched in tests)
# ---------------------------------------------------------------------------

def _get_supabase() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)


def _get_openai() -> OpenAI:
    return OpenAI(api_key=os.environ["OPENAI_API_KEY"])


# ---------------------------------------------------------------------------
# 1. Target loading
# ---------------------------------------------------------------------------

def load_targets(config_path: str) -> list[dict]:
    """Load active entities from targets.yaml and expand them into flat records.

    Returns a list of dicts with keys: name, category, results_per_entity,
    lookback_hours, min_article_length.
    """
    with open(config_path, "r") as fh:
        config = yaml.safe_load(fh)

    discovery = config.get("discovery", {})
    results_per_entity: int = discovery.get("results_per_entity", 2)
    lookback_hours: int = discovery.get("lookback_hours", 24)
    min_article_length: int = discovery.get("min_article_length", 500)

    entity_categories = ("competitors", "customers", "suppliers", "raw_materials", "markets")
    targets: list[dict] = []

    for category in entity_categories:
        for entity in config.get(category, []):
            if entity.get("active", False):
                targets.append(
                    {
                        "name": entity["name"],
                        "category": category,
                        "results_per_entity": results_per_entity,
                        "lookback_hours": lookback_hours,
                        "min_article_length": min_article_length,
                    }
                )

    logger.info("Loaded %d active targets from %s", len(targets), config_path)
    return targets


# ---------------------------------------------------------------------------
# 2. URL discovery via Serper.dev
# ---------------------------------------------------------------------------

def discover_urls(
    entity_name: str,
    lookback_hours: int,
    results_per_entity: int,
) -> list[str]:
    """Query Serper.dev news search and return raw article URLs for an entity."""
    api_key = os.environ["SERPER_API_KEY"]
    endpoint = "https://google.serper.dev/news"

    payload = {
        "q": entity_name,
        "num": results_per_entity,
        "tbs": f"qdr:h{lookback_hours}",
    }
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(endpoint, json=payload, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logger.error("Serper.dev request timed out for entity '%s'", entity_name)
        return []
    except requests.exceptions.HTTPError as exc:
        logger.error(
            "Serper.dev HTTP error for entity '%s': %s", entity_name, exc.response.status_code
        )
        return []
    except requests.exceptions.RequestException as exc:
        logger.error("Serper.dev request failed for entity '%s': %s", entity_name, exc)
        return []

    data = response.json()
    urls = [item["link"] for item in data.get("news", []) if "link" in item]
    logger.info("Discovered %d URL(s) for '%s'", len(urls), entity_name)
    return urls


# ---------------------------------------------------------------------------
# 3. URL normalization
# ---------------------------------------------------------------------------

def normalize_url(url: str) -> str:
    """Strip all query parameters and fragments from a URL.

    e.g. https://example.com/article?utm_source=feed#section
         → https://example.com/article
    """
    parsed = urlparse(url)
    clean = parsed._replace(query="", fragment="")
    return urlunparse(clean)


# ---------------------------------------------------------------------------
# 4. URL hashing
# ---------------------------------------------------------------------------

def compute_url_hash(normalized_url: str) -> str:
    """Return the SHA-256 hex digest of a normalized URL."""
    return hashlib.sha256(normalized_url.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# 5. Duplicate check
# ---------------------------------------------------------------------------

def url_already_processed(url_hash: str) -> bool:
    """Return True if this url_hash already exists in daily_intelligence."""
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
        # Treat as unprocessed to avoid silently skipping articles on DB errors.
        return False


# ---------------------------------------------------------------------------
# 6. Article scraping via Firecrawl
# ---------------------------------------------------------------------------

def scrape_article(url: str, min_length: int) -> Optional[str]:
    """Fetch article markdown from Firecrawl. Returns None if below min_length."""
    api_key = os.environ["FIRECRAWL_API_KEY"]
    endpoint = "https://api.firecrawl.dev/v1/scrape"

    payload = {
        "url": url,
        "formats": ["markdown"],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(endpoint, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logger.error("Firecrawl request timed out for URL: %s", url)
        return None
    except requests.exceptions.HTTPError as exc:
        logger.error(
            "Firecrawl HTTP error for URL %s: %s", url, exc.response.status_code
        )
        return None
    except requests.exceptions.RequestException as exc:
        logger.error("Firecrawl request failed for URL %s: %s", url, exc)
        return None

    data = response.json()
    markdown: str = data.get("data", {}).get("markdown", "") or ""

    if len(markdown) < min_length:
        logger.info(
            "Article too short (%d chars, min %d): %s", len(markdown), min_length, url
        )
        return None

    return markdown


# ---------------------------------------------------------------------------
# 7. LLM synthesis via OpenAI
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """You are an expert market intelligence analyst for AmI (Americhem Intelligence),
a global manufacturer of custom color masterbatch, functional additives, and engineered compounds
serving automotive, healthcare, packaging, wire and cable, and industrial markets.

Your job is to analyze news articles and extract structured intelligence. You MUST enforce all
four rules below before generating any output.

RULE 1 — ENTITY DISAMBIGUATION:
Before scoring, verify that the named entity in this article is the correct one.
- If the article mentions "Dow" verify it refers to Dow Chemical / Dow Inc., not the Dow Jones index.
- If the article mentions "Magna" verify it refers to Magna International, not the Magna Carta.
- If the article mentions "Celanese" verify it is the chemical company, not an unrelated brand.
- If the entity is a false match (wrong Dow, wrong Magna, unrelated brand), output ONLY this JSON:
  {"americhem_impact": "DISCARD"}

RULE 2 — THREAT MATRIX CALIBRATION:
Anchor sentiment_score strictly to supply chain and commercial physics.
Use the full 1–10 scale. Do NOT default to 5 unless the article is genuinely neutral.

- Score 1–2: Immediate physical supply chain threat (plant fire, port strike, supplier bankruptcy, force majeure)
- Score 3:   Significant disruption risk — major price spike, force majeure warning, capacity cut >10%
- Score 4:   Negative trend with indirect Americhem exposure (demand softness, margin pressure signals)
- Score 5:   Genuinely neutral — no discernible positive or negative lean for Americhem
- Score 6:   Mild positive — market growth or innovation in Americhem's end markets
- Score 7:   Moderate positive — competitor weakness, OEM expansion, favorable regulation
- Score 8–9: Clear commercial opportunity — large feedstock price drops, competitor capacity loss
- Score 10:  Transformational opportunity — major OEM win potential or supply disruption benefiting Americhem

Alert tier mapping (read-only context — do NOT include in output):
  CRITICAL  = score 1–3  |  ROUTINE = score 4–7  |  STRATEGIC = score 8–10

RULE 3 — RIGOROUS IMPACT STATEMENT:
Always write a specific So-What for Americhem even for routine items.
Identify which business unit or cost line could be affected and in what direction.
If truly no commercial connection exists, write: "Indirect exposure only — monitor for [specific reason]."
Do NOT write "No direct impact. Monitoring required." — this phrase is banned.
Do NOT write phrases like "may increase demand" or "could affect" without citing specific data.

RULE 4 — DOMAIN RELEVANCE FIREWALL:
Americhem is a plastics and specialty chemicals manufacturer. Only DISCARD if the article has
absolutely zero connection to plastics, polymers, chemicals, materials, manufacturing,
composites, packaging, or supply chain dynamics.
Examples of noise to DISCARD: sports results, political news, celebrity stories, unrelated
financial instruments (stock tips, crypto), or general HR policy.
When relevance is uncertain, do NOT discard. Set sentiment_score to 5 and apply Rule 3.

If the article passes all four rules, extract data into this strict JSON schema.
Output ONLY the JSON object — no preamble, no markdown, no explanation.

{
  "headline": "<concise factual summary, max 12 words>",
  "source_publication": "<name of the publisher, e.g. Reuters, Chemical Week, Plastics News>",
  "article_summary": "<2-3 sentences, max 50 words. What happened, who is involved, key numbers. Factual only — no Americhem framing.>",
  "americhem_impact": "<BLUF So What for Americhem. Apply Rule 3. Never generic.>",
  "sentiment_score": <integer 1-10 per Rule 2>,
  "sentiment_rationale": "<max 10 words explaining exactly why this score was assigned>",
  "recommended_action": "<one of: No action | Monitor | Flag to procurement | Share with sales | Escalate to leadership>",
  "source_url": "<MUST EXACTLY MATCH the URL provided in the user prompt>",
  "entities_mentioned": ["<companies, chemicals, or regions mentioned>"]
}"""

_VALID_ACTIONS: frozenset[str] = frozenset({
    "No action", "Monitor", "Flag to procurement",
    "Share with sales", "Escalate to leadership",
})


def synthesize_insight(
    article_text: str,
    source_url: str,
    trigger_entity: str,
    category: str,
) -> Optional[dict]:
    """Call gpt-4o-mini in JSON mode and return a structured insight dict.

    Returns None if the API call fails or the response cannot be parsed.
    The normalized source_url is injected into the user prompt so the model
    copies it back deterministically.
    """
    client = _get_openai()

    user_prompt = (
        f"Trigger entity: {trigger_entity}\n"
        f"Category: {category}\n"
        f"Source URL (copy this verbatim into source_url): {source_url}\n\n"
        f"Article text:\n{article_text}"
    )

    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
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
        logger.error("Failed to parse OpenAI JSON response: %s — raw: %s", exc, raw_content[:200])
        return None

    # Return DISCARD signal early before required-keys validation
    if insight.get("americhem_impact") == "DISCARD":
        return insight

    # Validate required keys
    required_keys = {"headline", "americhem_impact", "sentiment_score", "source_url", "entities_mentioned"}
    missing = required_keys - insight.keys()
    if missing:
        logger.error("OpenAI response missing keys %s for entity '%s'", missing, trigger_entity)
        return None

    # Clamp sentiment_score to 1-10
    try:
        score = int(insight["sentiment_score"])
        insight["sentiment_score"] = max(1, min(10, score))
    except (ValueError, TypeError):
        logger.error("Invalid sentiment_score value '%s' — defaulting to 5", insight.get("sentiment_score"))
        insight["sentiment_score"] = 5

    # Ensure entities_mentioned is a list
    if not isinstance(insight["entities_mentioned"], list):
        insight["entities_mentioned"] = []

    # Normalize new optional fields to empty string if missing
    insight.setdefault("source_publication", "")
    insight.setdefault("sentiment_rationale", "")
    insight.setdefault("article_summary", "")

    # Validate recommended_action — soft default to "Monitor" if missing or invalid
    if insight.get("recommended_action") not in _VALID_ACTIONS:
        insight["recommended_action"] = "Monitor"

    return insight


# ---------------------------------------------------------------------------
# 8. Storage
# ---------------------------------------------------------------------------

def store_insight(payload: dict) -> bool:
    """Upsert a row into daily_intelligence. Returns True on success."""
    try:
        supabase = _get_supabase()
        supabase.table("daily_intelligence").upsert(
            payload, on_conflict="url_hash"
        ).execute()
        return True
    except Exception as exc:
        logger.error("Supabase upsert failed for hash %s: %s", payload.get("url_hash"), exc)
        return False


# ---------------------------------------------------------------------------
# 9. Macro summary generation
# ---------------------------------------------------------------------------

def generate_macro_summary(articles: list[dict]) -> bool:
    """Generate a macro executive summary from today's stored articles.

    Calls gpt-4o-mini with all article headlines and impacts, then upserts
    a single row into daily_summaries keyed on today's run_date.

    Args:
        articles: List of insight payload dicts stored during this run.

    Returns:
        True on success, False on any failure.
    """
    if not articles:
        logger.warning("No articles to summarize — skipping macro summary generation.")
        return False

    client = _get_openai()

    article_digest = "\n".join(
        f"- [{a.get('category', '').upper()}] {a.get('headline', '')} "
        f"(Score {a.get('sentiment_score', '')}/10): {a.get('americhem_impact', '')}"
        for a in articles
    )

    user_prompt = (
        f"Today's market intelligence digest for Americhem ({len(articles)} articles):\n\n"
        f"{article_digest}\n\n"
        f"Generate a JSON object with exactly two keys:\n"
        f"- executive_summary: A 3-sentence macro summary of today's most important market movements "
        f"and their implications for Americhem's supply chain and commercial position.\n"
        f"- macro_sentiment: One word or short phrase describing overall market tone "
        f"(e.g. Stable, Bearish, Volatile, Cautiously Optimistic, Bullish).\n"
        f"Output ONLY the JSON object."
    )

    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You are a senior market intelligence analyst. Output only valid JSON."},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
        )
    except Exception as exc:
        logger.error("OpenAI macro summary call failed: %s", exc)
        return False

    raw = completion.choices[0].message.content or ""

    try:
        summary = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("Failed to parse macro summary JSON: %s — raw: %s", exc, raw[:200])
        return False

    if "executive_summary" not in summary or "macro_sentiment" not in summary:
        logger.error("Macro summary missing required keys: %s", list(summary.keys()))
        return False

    run_date = datetime.utcnow().date().isoformat()

    try:
        supabase = _get_supabase()
        supabase.table("daily_summaries").upsert(
            {
                "run_date": run_date,
                "executive_summary": summary["executive_summary"],
                "macro_sentiment": summary["macro_sentiment"],
            },
            on_conflict="run_date",
        ).execute()
        logger.info("Macro summary stored — sentiment: %s", summary["macro_sentiment"])
        return True
    except Exception as exc:
        logger.error("Supabase upsert failed for daily_summaries: %s", exc)
        return False


# ---------------------------------------------------------------------------
# 10. Main pipeline
# ---------------------------------------------------------------------------

def execute_pipeline() -> None:
    """Orchestrate the full ingestion loop across all active targets."""
    targets = load_targets("targets.yaml")

    scrapes_attempted = 0
    stats = {
        "urls_discovered": 0,
        "urls_skipped_duplicate": 0,
        "urls_skipped_too_short": 0,
        "urls_skipped_discard": 0,
        "scrapes_attempted": 0,
        "insights_stored": 0,
        "errors": 0,
    }
    stored_articles_buffer: list[dict] = []

    for target in targets:
        entity_name: str = target["name"]
        category: str = target["category"]
        lookback_hours: int = target["lookback_hours"]
        results_per_entity: int = target["results_per_entity"]
        min_article_length: int = target["min_article_length"]

        raw_urls = discover_urls(entity_name, lookback_hours, results_per_entity)
        stats["urls_discovered"] += len(raw_urls)

        for raw_url in raw_urls:
            if scrapes_attempted >= MAX_DAILY_SCRAPES:
                logger.warning(
                    "MAX_DAILY_SCRAPES (%d) reached — stopping pipeline early.", MAX_DAILY_SCRAPES
                )
                _log_stats(stats)
                generate_macro_summary(stored_articles_buffer)
                return

            normalized = normalize_url(raw_url)
            url_hash = compute_url_hash(normalized)

            if url_already_processed(url_hash):
                logger.info("Duplicate — skipping: %s", normalized)
                stats["urls_skipped_duplicate"] += 1
                continue

            scrapes_attempted += 1
            stats["scrapes_attempted"] += 1

            article_text = scrape_article(raw_url, min_article_length)
            if article_text is None:
                stats["urls_skipped_too_short"] += 1
                time.sleep(1.5)
                continue

            insight = synthesize_insight(article_text, normalized, entity_name, category)
            if insight is None:
                stats["errors"] += 1
                time.sleep(1.5)
                continue

            if insight.get("americhem_impact") == "DISCARD":
                logger.info("DISCARD — false positive or domain mismatch: %s", normalized)
                stats["urls_skipped_discard"] += 1
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
            }

            if store_insight(payload):
                logger.info(
                    "Stored [score=%d] %s", insight["sentiment_score"], insight["headline"]
                )
                stats["insights_stored"] += 1
                stored_articles_buffer.append(payload)
            else:
                stats["errors"] += 1

            time.sleep(1.5)

    _log_stats(stats)
    generate_macro_summary(stored_articles_buffer)


def _log_stats(stats: dict) -> None:
    logger.info(
        "Pipeline complete — discovered: %d | duplicates skipped: %d | "
        "too short: %d | discards: %d | scrapes attempted: %d | stored: %d | errors: %d",
        stats["urls_discovered"],
        stats["urls_skipped_duplicate"],
        stats["urls_skipped_too_short"],
        stats.get("urls_skipped_discard", 0),
        stats["scrapes_attempted"],
        stats["insights_stored"],
        stats["errors"],
    )


if __name__ == "__main__":
    execute_pipeline()
