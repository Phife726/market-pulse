import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta
from html.parser import HTMLParser
from urllib.parse import urlparse, urlunparse
from typing import Optional

import requests
import yaml
from openai import OpenAI
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MAX_DAILY_SCRAPES = 20
OPENAI_MODEL = "gpt-5.4-nano"
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




def _get_supabase() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)


def _get_openai() -> OpenAI:
    return OpenAI(api_key=os.environ["OPENAI_API_KEY"])


def load_targets(config_path: str) -> list[dict]:
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
                targets.append({
                    "name": entity["name"],
                    "category": category,
                    "results_per_entity": results_per_entity,
                    "lookback_hours": lookback_hours,
                    "min_article_length": min_article_length,
                })
    logger.info("Loaded %d active targets from %s", len(targets), config_path)
    return targets


def discover_urls(entity_name: str, lookback_hours: int, results_per_entity: int) -> list[tuple[str, str]]:
    api_key = os.environ["SERPER_API_KEY"]
    endpoint = "https://google.serper.dev/news"
    payload = {"q": entity_name, "num": results_per_entity, "tbs": f"qdr:h{lookback_hours}"}
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
    try:
        response = requests.post(endpoint, json=payload, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logger.error("Serper.dev request timed out for entity '%s'", entity_name)
        return []
    except requests.exceptions.HTTPError as exc:
        logger.error("Serper.dev HTTP error for entity '%s': %s", entity_name, exc.response.status_code)
        return []
    except requests.exceptions.RequestException as exc:
        logger.error("Serper.dev request failed for entity '%s': %s", entity_name, exc)
        return []
    data = response.json()
    results = [
        (item["link"], item.get("title", ""))
        for item in data.get("news", [])
        if "link" in item
    ]
    logger.info("Discovered %d URL(s) for '%s'", len(results), entity_name)
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
    try:
        response = requests.post(endpoint, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
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


def synthesize_insight(article_text: str, source_url: str, trigger_entity: str, category: str) -> Optional[dict]:
    client = _get_openai()
    user_prompt = (
        f"Trigger entity: {trigger_entity}\nCategory: {category}\n"
        f"Source URL: {source_url}\n\nArticle text:\n{article_text}"
    )
    try:
        completion = client.chat.completions.create(
            model=OPENAI_MODEL,
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


def generate_macro_summary(articles: list[dict]) -> bool:
    """Generate a macro executive summary from today's stored articles.

    Calls the configured OpenAI model with all article headlines and impacts, then upserts
    a single row into daily_summaries keyed on today's run_date.
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
            model=OPENAI_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "You are a senior market intelligence analyst. Output only valid JSON."},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
        )
        content = completion.choices[0].message.content
        if content is None:
            raise ValueError("OpenAI returned empty content for macro summary")
        parsed = json.loads(content)
        executive_summary = parsed["executive_summary"]
        macro_sentiment = parsed["macro_sentiment"]
    except Exception as exc:
        logger.error("Failed to generate macro summary from OpenAI: %s", exc)
        return False

    try:
        from datetime import date
        supabase = _get_supabase()
        supabase.table("daily_summaries").upsert(
            {
                "run_date": date.today().isoformat(),
                "executive_summary": executive_summary,
                "macro_sentiment": macro_sentiment,
            },
            on_conflict="run_date",
        ).execute()
        logger.info("Macro summary upserted — sentiment: %s", macro_sentiment)
        return True
    except Exception as exc:
        logger.error("Failed to upsert macro summary to Supabase: %s", exc)
        return False


def _log_stats(stats: dict) -> None:
    logger.info(
        "Pipeline complete — discovered: %d | duplicates skipped: %d | "
        "semantic duplicates: %d | too short: %d | discards: %d | "
        "scrapes attempted: %d | stored: %d | errors: %d",
        stats["urls_discovered"],
        stats["urls_skipped_duplicate"],
        stats.get("urls_skipped_semantic_duplicate", 0),
        stats["urls_skipped_too_short"],
        stats.get("urls_skipped_discard", 0),
        stats["scrapes_attempted"],
        stats["insights_stored"],
        stats["errors"],
    )


def execute_pipeline() -> None:
    targets = load_targets("targets.yaml")
    seen_headlines: set[str] = _hydrate_seen_headlines()
    scrapes_attempted = 0
    stats = {
        "urls_discovered": 0,
        "urls_skipped_duplicate": 0,
        "urls_skipped_semantic_duplicate": 0,
        "urls_skipped_too_short": 0,
        "urls_skipped_discard": 0,
        "scrapes_attempted": 0,
        "insights_stored": 0,
        "errors": 0,
    }
    stored_articles_buffer: list[dict] = []

    for target in targets:
        entity_name = target["name"]
        category = target["category"]
        lookback_hours = target["lookback_hours"]
        results_per_entity = target["results_per_entity"]
        min_article_length = target["min_article_length"]

        raw_results = discover_urls(entity_name, lookback_hours, results_per_entity)
        stats["urls_discovered"] += len(raw_results)

        for raw_url, serper_title in raw_results:
            if scrapes_attempted >= MAX_DAILY_SCRAPES:
                logger.warning("MAX_DAILY_SCRAPES (%d) reached — stopping.", MAX_DAILY_SCRAPES)
                _log_stats(stats)
                generate_macro_summary(stored_articles_buffer)
                return

            normalized = normalize_url(raw_url)
            url_hash = compute_url_hash(normalized)

            if url_already_processed(url_hash):
                logger.info("Duplicate — skipping: %s", normalized)
                stats["urls_skipped_duplicate"] += 1
                continue

            is_dup, matched, score = is_semantic_duplicate(serper_title, seen_headlines)
            if is_dup:
                logger.warning(
                    "SEMANTIC_DUPLICATE — skipped: '%s' ~ '%s' | score: %d",
                    serper_title, matched, score,
                )
                stats["urls_skipped_semantic_duplicate"] += 1
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
                logger.info("DISCARD — false positive: %s", normalized)
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
                "article_summary": insight.get("article_summary", ""),
            }

            if store_insight(payload):
                logger.info("Stored [score=%d] %s", insight["sentiment_score"], insight["headline"])
                stats["insights_stored"] += 1
                stored_articles_buffer.append(payload)
                seen_headlines.add(insight["headline"])
            else:
                stats["errors"] += 1

            time.sleep(1.5)

    _log_stats(stats)
    generate_macro_summary(stored_articles_buffer)


if __name__ == "__main__":
    execute_pipeline()
