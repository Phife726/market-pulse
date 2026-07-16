"""Pure module owning every LLM prompt the pipeline assembles.

Text assembly only: callers keep validation, the LLM seam (`llm.py`) keeps
transport. Config enters as a dict (same discipline as `report.py`); no I/O,
no clock, no env reads — purity is enforced by the import graph (this module
imports only `insight` and stdlib).

The unit of exchange is the **prompt spec** (`PromptSpec` / `MacroPrompt`) —
a fully-assembled call as plain frozen data, splatted into the LLM seam via
`spec.kwargs()`. Temperature and the diagnostic context label live in the
spec because they co-vary with the wording: a prompt rewrite and its sampling
change land in one file.

This module also owns the vocabulary the prompts *promise* — the macro
condition enum, the executive-bullet labels, the citation cap — which the
macro validators in `ingestion_engine` import. The model is validated against
exactly what it was told, from one definition; drift is an import error, not
a diff-review discipline.
"""
import hashlib
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import insight


# ---------------------------------------------------------------------------
# Shared constants (single source of truth)
# ---------------------------------------------------------------------------

ENGLISH_OUTPUT_RULE = (
    "All human-readable generated strings must be written in clear business English, "
    "regardless of the source article's language. Translate non-English source "
    "content into English. Preserve proper nouns — company names, product names, "
    "brand names, source publications, locations, URLs, and quoted legal or product "
    "identifiers — in their original form when translation would reduce precision. "
    "Enum/taxonomy fields must use the configured English labels exactly."
)

VALID_MACRO_CONDITIONS: frozenset[str] = frozenset({
    "Competitive Pressure", "Supply Volatility", "Demand Expansion",
    "Demand Softness", "Regulatory Pressure", "Sustainability Pull",
    "Commercial Opportunity", "Mixed / Watch", "Low Signal",
})

EXEC_BULLET_LABELS: tuple[str, ...] = (
    "Market pressure", "Supply chain watch", "Commercial action",
)

# Macro-outlook signal direction — a small validated enum (same one-definition
# discipline as VALID_MACRO_CONDITIONS: the prompt promises exactly these and
# the macro-outlook validator enforces exactly these).
VALID_MACRO_DIRECTIONS: frozenset[str] = frozenset({"Rising", "Stable", "Declining"})

MAX_MACRO_SUMMARY_SOURCE_PACK_ARTICLES = 40
# Of those pack slots, reserve up to this many for signal_type == "Macro" rows,
# which tend to score mid-range on materiality and would otherwise be crowded
# out of the citable pack on heavy news days — starving the macro outlook.
MACRO_OUTLOOK_SOURCE_PACK_QUOTA = 10
MAX_MACRO_OUTLOOK_SIGNALS = 6
MAX_EXECUTIVE_BULLET_CITATIONS = 3


# ---------------------------------------------------------------------------
# The prompt spec
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PromptSpec:
    """One fully-assembled structured-LLM call, as plain frozen data.

    Invariants:
    - system and user are complete, final text — no unresolved placeholders.
    - temperature is the value the pipeline ships with for this call
      (0.2 insight, 0.3 macro, None thematic); None means seam default.
    - context is the diagnostic label the LLM seam logs.
    """
    system: str
    user: str
    temperature: Optional[float] = None
    context: str = ""

    def kwargs(self) -> dict:
        """Exactly the keyword dict LLM.complete_json accepts:
        _llm().complete_json(**spec.kwargs())."""
        return {"system": self.system, "user": self.user,
                "temperature": self.temperature, "context": self.context}

    @property
    def system_fingerprint(self) -> str:
        """First 12 hex chars of sha256(system). Stable identity for 'which
        prompt wording produced this run' in logs and diffs."""
        return hashlib.sha256(self.system.encode("utf-8")).hexdigest()[:12]


@dataclass(frozen=True)
class MacroPrompt(PromptSpec):
    """A PromptSpec plus the citation index the digest text minted.

    source_pack[i]["id"] == the [n] marker on digest line n, by construction
    (one enumeration over one ranked list) — this is the contract the
    citation validator relies on. kwargs() excludes source_pack."""
    source_pack: tuple[dict, ...] = ()


# ---------------------------------------------------------------------------
# Insight synthesis prompt (per-article)
# ---------------------------------------------------------------------------

_FALLBACK_COMMERCIAL_SEGMENT_LIST = (
    "Healthcare | Fibers | Transportation - Automotive | "
    "Transportation - Non-Automotive | Transportation - Aerospace | "
    "Industrial | Packaging | Engineered Resins | Enterprise / Cross-Segment"
)

_FALLBACK_SIGNAL_TYPE_LIST = (
    "Competitive | Customer | Regulatory | Sustainability | "
    "Supply Chain | Technology | Macro | Other"
)


def _taxonomy_block(config_block: dict, fallback: str) -> str:
    """Render taxonomy entries as '  Label: description' lines; fall back to the
    canned list when the block is missing/empty or yields no labeled lines.
    Shared by RULE 4 and RULE 5 so a formatting fix cannot land in one only."""
    if not config_block:
        return fallback
    lines: list[str] = []
    for entry in config_block.values():
        if not isinstance(entry, dict):
            continue
        label = entry.get("label", "")
        desc = (entry.get("description") or "").strip().replace("\n", " ")
        if label:
            lines.append(f"  {label}: {desc}" if desc else f"  {label}")
    return "\n".join(lines) if lines else fallback


def _build_commercial_segment_rule(config: dict) -> str:
    """Return RULE 4 text with commercial segment labels and descriptions from config."""
    block = _taxonomy_block(config.get("commercial_segments") or {},
                            _FALLBACK_COMMERCIAL_SEGMENT_LIST)
    return f"""RULE 4 — COMMERCIAL SEGMENT:
Assign the single best-fit commercial segment for the affected end-market:

{block}

Choose "Enterprise / Cross-Segment" only when the article spans multiple segments
or addresses Americhem-wide topics with no single end-market dominating."""


def _build_signal_type_rule(config: dict) -> str:
    """Return RULE 5 text with signal type labels and descriptions from config."""
    block = _taxonomy_block(config.get("signal_types") or {},
                            _FALLBACK_SIGNAL_TYPE_LIST)
    return f"""RULE 5 — SIGNAL TYPE:
Assign the single kind of signal this article represents:

{block}

Prefer a named type over "Other" whenever possible."""


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


def _insight_system_prompt(config: dict) -> str:
    """Assemble the full system prompt, injecting commercial segment and signal
    type taxonomies. Assembly is str.replace() on named markers, never
    .format() — _SYSTEM_PROMPT_BASE's literal JSON braces are load-bearing."""
    rule4 = _build_commercial_segment_rule(config)
    rule5 = _build_signal_type_rule(config)
    return (
        _SYSTEM_PROMPT_BASE
        .replace("{english_output_rule}", ENGLISH_OUTPUT_RULE)
        .replace("{rule4}", rule4)
        .replace("{rule5}", rule5)
    )


def insight_prompt(
    config: dict,
    *,
    article_text: str,
    source_url: str,
    trigger_entity: str,
    category: str,
) -> PromptSpec:
    """Per-article Insight synthesis call.

    system: the eight-rule prompt with ENGLISH_OUTPUT_RULE as RULE 0 and
    RULEs 4/5 assembled from config["commercial_segments"] / ["signal_types"]
    (labels + descriptions verbatim — the documented control-file behavior),
    falling back to the canned taxonomy lists when config is absent/empty.
    user: source_url is injected verbatim — the key invariant: the model
    echoes the canonical URL deterministically."""
    user = (
        f"Trigger entity: {trigger_entity}\nCategory: {category}\n"
        f"Source URL: {source_url}\n\nArticle text:\n{article_text}"
    )
    return PromptSpec(
        system=_insight_system_prompt(config),
        user=user,
        temperature=0.2,
        context=f"entity '{trigger_entity}'",
    )


# ---------------------------------------------------------------------------
# Macro summary prompt (once per run)
# ---------------------------------------------------------------------------

def _source_domain(url: str) -> str:
    """Registrable host minus a leading 'www.'; '' when unparseable/empty.

    Uses urlparse().hostname so any :port is stripped and the host is lowercased.
    """
    try:
        host = urlparse(url or "").hostname or ""
    except (ValueError, TypeError):
        return ""
    return host[4:] if host.startswith("www.") else host


def _macro_sort_key(a: dict):
    """Deterministic macro ranking key: materiality desc, headline asc, hash asc.
    created_at is NOT used — the in-memory stored-articles buffer does not carry
    it — but the key is still fully deterministic."""
    return (
        -insight.effective_impact(a),
        a.get("headline", "") or "",
        a.get("url_hash", "") or "",
    )


def _rank_macro_articles(articles: list[dict]) -> list[dict]:
    """Deterministic, capped ordering of citable articles.

    Ranks by _macro_sort_key and caps at MAX_MACRO_SUMMARY_SOURCE_PACK_ARTICLES,
    but first RESERVES up to MACRO_OUTLOOK_SOURCE_PACK_QUOTA slots for
    signal_type == "Macro" rows so the macro outlook always has something to
    cite; the remaining slots are filled from the overall ranking. The final
    list is re-sorted by the same key, so the digest and source-pack ids still
    come from one enumeration in materiality order."""
    ordered = sorted(articles, key=_macro_sort_key)

    reserved = [a for a in ordered if insight.signal_type(a) == "Macro"][:MACRO_OUTLOOK_SOURCE_PACK_QUOTA]
    reserved_ids = {id(a) for a in reserved}
    remaining = MAX_MACRO_SUMMARY_SOURCE_PACK_ARTICLES - len(reserved)
    fill = [a for a in ordered if id(a) not in reserved_ids][:remaining]

    return sorted(reserved + fill, key=_macro_sort_key)


def _build_macro_source_pack(ranked_articles: list[dict]) -> list[dict]:
    """Number the already-ranked articles 1..N as the citable source pack.

    Each entry: {id, headline, url, domain, segment, score}."""
    pack: list[dict] = []
    for i, a in enumerate(ranked_articles, start=1):
        url = a.get("source_url", "") or ""
        pack.append({
            "id": i,
            "headline": a.get("headline", "") or "",
            "url": url,
            "domain": _source_domain(url),
            "segment": insight.commercial_segment(a),
            "score": insight.effective_impact(a),
        })
    return pack


def macro_prompt(articles: list[dict]) -> MacroPrompt:
    """Macro-summary call over today's stored articles.

    Ranks deterministically (materiality desc, headline asc, url_hash asc),
    caps at MAX_MACRO_SUMMARY_SOURCE_PACK_ARTICLES, and builds the digest AND
    the source pack from the SAME enumeration — digest [n] markers and
    source_pack ids match by construction, not by convention.

    Total on empty input (empty pack, "0 articles" digest); the caller's
    existing not-articles guard means that path is dead in production."""
    ranked = _rank_macro_articles(articles)
    source_pack = _build_macro_source_pack(ranked)

    article_digest = "\n".join(
        f"[{i}] [{a.get('category', '').upper()}] {a.get('headline', '')} "
        f"(Impact {insight.effective_impact(a)}/10): "
        f"{a.get('americhem_impact', '')}"
        for i, a in enumerate(ranked, start=1)
    )

    macro_conditions_text = ", ".join(sorted(VALID_MACRO_CONDITIONS))
    macro_directions_text = " | ".join(sorted(VALID_MACRO_DIRECTIONS))
    label_a, label_b, label_c = EXEC_BULLET_LABELS

    system = (
        f"OUTPUT LANGUAGE:\n{ENGLISH_OUTPUT_RULE}\n\n"
        "You are a senior Americhem commercial intelligence analyst writing the morning brief\n"
        "for GMMs and Sales leaders. Output ONLY a JSON object with three keys.\n\n"
        "1. dominant_condition — pick exactly one value from this list that best describes\n"
        "   today's overall commercial weather across the digest:\n"
        f"     {macro_conditions_text}\n\n"
        "2. executive_bullets — exactly three objects, in this order, with these exact labels:\n"
        f'     {{"label": "{label_a}",    "body": "<one sentence, <=30 words>", "citation_source_ids": [<source numbers>]}}\n'
        f'     {{"label": "{label_b}", "body": "<one sentence, <=30 words>", "citation_source_ids": [<source numbers>]}}\n'
        f'     {{"label": "{label_c}",  "body": "<one sentence, <=30 words>", "citation_source_ids": [<source numbers>]}}\n\n'
        '   Each body must reference specific named entities or segments from the digest.\n'
        '   citation_source_ids: the bracketed [n] source numbers from the digest that\n'
        f'   directly support that body. Cite 1 to {MAX_EXECUTIVE_BULLET_CITATIONS} of the most relevant\n'
        '   sources, most relevant first. Use ONLY source numbers that appear in the digest.\n'
        '   If a bullet is not supported by any specific source, use an empty list [].\n'
        '   Do NOT hedge ("may", "could", "potentially") without a specific data point.\n'
        '   Do NOT write generic statements ("monitor closely", "remain vigilant").\n\n'
        '   Low-signal special case:\n'
        '   If dominant_condition is "Low Signal", the Commercial action body MUST be the\n'
        '   literal string "No action required." with citation_source_ids []. The other two\n'
        '   bullets MUST describe the absence of meaningful signal.\n\n'
        "3. macro_outlook — a structured read of MATERIAL macro/economic signals and their\n"
        "   Americhem implications. An object with two keys:\n"
        '     "current_condition": "<one concise sentence on overall macro conditions>",\n'
        f'     "signals": [ zero or more objects (use [] when no material signal exists), up to {MAX_MACRO_OUTLOOK_SIGNALS}, each:\n'
        '        {\n'
        '          "indicator": "<the macro indicator, e.g. Manufacturing PMI>",\n'
        f'          "direction": "<exactly one of: {macro_directions_text}>",\n'
        '          "americhem_implication": "<the operational so-what for Americhem: a demand,\n'
        '             cost, capacity, margin, or segment effect — NEVER restate the indicator>",\n'
        f'          "affected_segments": ["<one or more EXACT labels from: {_FALLBACK_COMMERCIAL_SEGMENT_LIST}>"],\n'
        '          "citation_source_ids": [<source numbers from the digest>]\n'
        '        }\n'
        '     ]\n'
        '   A signal is MATERIAL only if it implies a demand inflection, cost/margin pressure,\n'
        '   capacity/investment constraint, credit/liquidity pressure, logistics/feedstock\n'
        '   disruption, or a contradiction to the current commercial outlook. EXCLUDE generic\n'
        '   economic commentary (a GDP or inflation mention with no defensible Americhem\n'
        '   implication).\n'
        '   Every signal MUST cite at least one digest source id that supports it; OMIT any\n'
        '   signal you cannot cite. Use ONLY source numbers that appear in the digest.'
    )

    user = (
        f"Today's market intelligence digest for Americhem ({len(articles)} articles):\n\n"
        f"{article_digest}\n\nOutput ONLY the JSON object."
    )

    return MacroPrompt(
        system=system,
        user=user,
        temperature=0.3,
        context="macro summary",
        source_pack=tuple(source_pack),
    )


# ---------------------------------------------------------------------------
# Thematic synthesis prompt (per delivery run)
# ---------------------------------------------------------------------------

def thematic_prompt(groups: dict[str, list[dict]]) -> PromptSpec:
    """Thematic-synthesis call over the final capped 2+ groups.

    Precondition (caller-enforced, as today): groups is non-empty and each
    group has 2+ Insights (ReportModel.synthesis_candidates())."""
    lines: list[str] = []
    for category, articles in groups.items():
        lines.append(f"CATEGORY: {category}")
        for art in articles:
            impact_score = insight.effective_impact(art)
            tag = art.get("sentiment_tag") or ""
            entities = art.get("entities_mentioned") or []
            entity = entities[0] if entities else (art.get("commercial_segment") or art.get("category") or "Unknown")
            americhem_impact = art.get("americhem_impact", "")
            tag_suffix = f" | {tag}" if tag else ""
            lines.append(f"- [{entity} | impact:{impact_score}/10{tag_suffix}] {americhem_impact}")
        lines.append("")

    grouped_text = "\n".join(lines).strip()

    system = (
        f"OUTPUT LANGUAGE:\n{ENGLISH_OUTPUT_RULE}\n\n"
        "You are a market intelligence analyst for Americhem, a specialty plastics compounder.\n\n"
        "For each CATEGORY block below, write exactly one synthesis paragraph (2–3 sentences).\n"
        "The paragraph must:\n"
        "- Identify the shared trend or structural driver across the listed signals\n"
        "- Explicitly state the implication for Americhem's supply chain, demand pipeline, or margin\n"
        "- Be written for a senior executive who will act on it — no hedging, no filler\n\n"
        "Return valid JSON with category names as keys and synthesis paragraphs as values.\n"
        "Use the exact category names provided. Do not invent categories.\n"
        "Only include categories that appear in the input."
    )

    return PromptSpec(
        system=system,
        user=grouped_text,
        temperature=None,
        context="thematic synthesis",
    )
