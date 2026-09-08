"""Pure module owning every LLM prompt the pipeline assembles.

Text assembly only: callers keep validation, the LLM seam (`llm.py`) keeps
transport. Config enters as a dict (same discipline as `report.py`); no I/O,
no clock, no env reads — purity is pinned structurally in tests/test_purity.py (this module
imports only `insight`, `scoring`, `market_reports`, and stdlib).

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
import textwrap
from dataclasses import dataclass
from typing import Optional

import insight
import market_reports
from scoring import Scoring


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
MAX_MACRO_OUTLOOK_SIGNALS = 3
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
    "Industrial | Building & Construction | Packaging | Engineered Resins | "
    "Enterprise / Cross-Segment"
)

_FALLBACK_SIGNAL_TYPE_LIST = (
    "Competitive | Customer | Regulatory | Sustainability | "
    "Supply Chain | Technology | Macro | Other"
)


# The RULE 6 honest low-exposure So-What openers. One definition: RULE 6 is
# assembled from this tuple, and report._apply_delivery_suppression exempts a
# row whose americhem_impact opens with one of them from rule 1 (the
# Enterprise / Cross-Segment low-impact drop) while it sits below the visible
# threshold — issue #65. Change the wording here and both sides move together.
LOW_EXPOSURE_TEMPLATE_PREFIXES: tuple[str, ...] = ("Adjacent market", "Limited direct exposure")


def low_exposure_score_band(scorer: Scoring) -> tuple[int, int]:
    """The americhem_impact_score band RULE 6 binds the low-exposure templates
    to: (low, high). low is the supporting threshold — the appendix floor, so a
    template row is never stranded below the email; high is one above it but
    always below the visible threshold, so a template row never becomes a card.
    Production (3 / 6) gives (3, 4); the code defaults (4 / 6) give (4, 5)."""
    low = scorer.supporting
    high = max(low, min(low + 1, scorer.visible - 1))
    return low, high


def _build_low_exposure_score_rule(scorer: Scoring) -> str:
    """RULE 6's 'SCORE MUST MATCH THE TEMPLATE' sub-rule, band from Scoring."""
    low, high = low_exposure_score_band(scorer)
    band = f"{low} or {high}" if low != high else f"exactly {low}"
    names = " or ".join(f'"{prefix}"' for prefix in LOW_EXPOSURE_TEMPLATE_PREFIXES)
    return (
        "- SCORE MUST MATCH THE TEMPLATE: either template implies low materiality. Pair\n"
        f"  {names} wording with an americhem_impact_score of {band}\n"
        f"  — never above {high} (that would overstate it), and never below {low} "
        "(the article still passed Rule 7).\n"
        "  A template is only for a FLOOR or 4-band article (RULE 3) that is about the right\n"
        "  entity; it is never a substitute for RULE 1's DISCARD, and never the So-What for a\n"
        "  WATCH-or-above event."
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
- PRECEDENCE: this verdict comes first and is final. A false entity match is DISCARD —
  never a RULE 6 low-exposure template ("Limited direct exposure — Dow here is the stock
  index" is a RULE 1 failure, not low exposure) and never a RULE 7 uncertain-relevance
  score. RULE 7's "do NOT discard when uncertain" applies only to articles that ARE about
  the correct entity.

RULE 2 — SENTIMENT TAG (directional tone only — NOT importance):
Assign exactly one tag based on the direction of impact for Americhem:
- "Negative": adverse direction — threatens customers, suppliers, demand, margin, operations, or compliance
- "Neutral": informational, mixed, or weakly directional signal
- "Positive": favorable direction — demand growth, margin benefit, competitive advantage, supply opportunity

IMPORTANT: sentiment_tag is direction only. A barely-relevant article can be Negative.
A neutral article can have a high impact score. Do NOT conflate tone with importance.

COMPETITOR DEFAULT: A competitor's product launch, contract win, capacity expansion, or
other success is "Negative" or "Neutral" for Americhem by default — it is a
competitive threat, not an Americhem opportunity. Tag it "Positive" only if the article
demonstrates growth in a market Americhem serves that outweighs the competitive threat.

Also assign sentiment_score (1–10, kept for compatibility) using the same directional logic:
1–3 = Negative range, 4–6 = Neutral range, 7–10 = Positive range.

RULE 3 — AMERICHEM IMPACT SCORE (relevance and materiality, 1–10):
Score how materially the EVENT bears on Americhem's business, independent of sentiment
direction. Americhem is almost never named in the news it needs to see: score the event and
the actor's position in Americhem's value chain, never whether the article names Americhem
or states an effect on it. "No explicit Americhem linkage" is the normal condition of a
relevant article and is NEVER a reason to score it low — you supply the implied mechanism.

VALUE-CHAIN ACTOR: the trigger entity, or any other company that makes, sells, or distributes
polymers, resins, compounds, masterbatch, pigments, inks, additives, adhesives, composites,
battery or EV materials, or their feedstocks — Americhem's customers, suppliers,
distributors, and competitors. ABSENT TRIGGER ENTITY: a query on one company often returns
another's news. If the trigger entity is absent or incidental but the article is about
ANOTHER value-chain actor's event (Siegwerk's ink capacity for a Sun Chemical query;
Cambium's aerospace adhesive or a composites developer's hydrogen-line program for an
Advanced Composites query), score that actor's event exactly as if it were the trigger.
RULE 1's DISCARD is for a WRONG entity, never an absent one.

The floor bands (1, 2, 3, 4) are checked first; an article that fits one never scores
higher. Each floor band is a closed list: put the article in the band whose list names it.

1 — NOT ABOUT THE BUSINESS: a 404, empty, or paywalled page; a job posting; a retail product
     listing; local or sports news that only shares a name or a street with the entity.

2 — NO EVENT FOR THE ENTITY: the entity is only mentioned in passing; its action is in an
     unrelated business line (software, diagnostics equipment, semiconductors, a bus-camera
     trial); a consumer product review, launch, or promotion; a plant visit, labor dispute,
     or anniversary story; and every MACRO STATISTIC or policy other than the two prints in
     band 5 — vehicle or EV sales anywhere, building permits, construction spending, credit
     or freight indices, energy prices, another country's output, a province's 2030 vehicle
     ban — whatever demand it "implies". So-What opener for 1–2: "No material signal — …".

3 — BUSINESS CONTENT, NOT AN EVENT: market-research forecasts — any "market to reach / CAGR
     / forecast to 20XX" report ({market_report_publishers},
     and wire syndications of them on EIN Presswire, GlobeNewswire or PRNewswire), even
     about masterbatch, even listing a competitor "among key players"; analyst ratings and
     price targets, stock screens, "better-ranked stock" lists, valuation or resilience
     commentary, fund stakes, dividends, earnings-date scheduling, share-price moves; a law
     firm's shareholder investigation of a deal; trade-show exhibits, awards, sponsorships;
     personnel changes below CEO/CFO/COO; industry rankings.

4 — THIN: correct entity, real but thin. A CEO/CFO/COO change; results or earnings with no
     price, volume, or capacity signal, or from a prior fiscal year (except a head-to-head
     competitor's — Avient, Techmer PM, Teknor Apex, RTP, Penn Color, Ampacet — beat-and-
     raise, which is 6); an end product that merely USES a named polymer, however it is
     cleared or certified (an FDA-cleared tray made of Ultem, a sponge, an analyzer);
     brochure-style marketing of an existing grade (headline verbs promotes / highlights /
     showcases / features); trend or growth commentary and executive outlook quotes with no
     transaction, price, capacity, launch, or figure; a trade body's guides, explainers, or
     roundups that set no new obligation; a settlement, fine, consent decree, or remediation
     order — however "binding", even one mandating plant upgrades — and any lawsuit or
     ruling, with no outage, closure, or allocation; a corporate spin-off or IT project;
     RULE 7's uncertain-relevance exit. Never a WATCH event that merely lacks a stated
     Americhem effect.

5 — DEMAND PRINT, or a generic event. (a) The two macro prints that matter to a compounder:
     the US ISM Manufacturing PMI — a reading, a consensus preview, or a bank lifting its
     forecast of it — and US industrial production. These two only; every other statistic
     is band 2. (b) A WATCH-class event told so generically that it names NO counterparty,
     plant, grade, input, figure, or date.

6 — WATCH (the default for an actor's event): a VALUE-CHAIN ACTOR does something
     operationally material, or regulation binds a RULE 4 end-market. Any one of:
     - a price change, force majeure, allocation, outage, or shortage on a polymer, resin,
       pigment, additive, or feedstock (with a named input or a figure it is DIRECT, below)
     - capacity opened, closed, expanded, idled, moved, or sold — a plant, line, lab, or
       capability build — in any RULE 4 end-market (a supplier's EV battery-materials lab
       counts)
     - a JV, distribution-agreement change, or a deal whose counterparty is not named, in
       Americhem's supply chain or sales channel (a NAMED target or buyer is DIRECT, below)
     - financial distress — bankruptcy, restructuring, going-concern warning, guidance
       cut — or a head-to-head competitor's earnings beat and guidance raise
     - a launch, new grade, certification, volume milestone, partnership, or development /
       qualification program by a MATERIAL MAKER (resin, compound, masterbatch, additive,
       adhesive, ink, or composite) in a RULE 4 segment — development-stage counts when it
       states a performance figure or a target application; a competitor's launch is a
       threat and still scores here
     - an aerospace composites program — hydrogen lines, eVTOL or airframe structures — by
       any composites developer: in Transportation - Aerospace every program is a materials
       decision (thermoplastic composite LH2 lines, 50–60% lighter, is 6)
     - quarterly results that carry a price or volume signal for an input or a RULE 4
       end-market
     - regulation binding a RULE 4 end-market in a market Americhem sells into (federal
       US, a US state, the EU or UK, or a trigger entity's home market): a rule taking
       effect, a deadline set, a fee schedule (EPR, PFAS, recycled content, food contact),
       even when reported through an explainer
     A named counterparty, plant, grade, input, figure, or effective date confirms 6.

7–8 — DIRECT: the event is on an input Americhem buys or a company it trades through.
     - a resin, recyclate, or pigment PRICE REPORT or price-direction call — Plastics News
       monthly moves (PE / PP / PET / PS / PC / ABS, R-PET / R-PP, TiO2), "resin markets
       swing as buyers regain leverage", "prices could go up again in August" — with or
       without a figure: these are input prices, never macro statistics
     - an announced price increase on resin, TiO2 and other pigments, carbon black, nylon /
       caprolactam, or PVC, including a supplier's quarterly results that report one (Dow:
       PE price +20%; Chemours: TiO2 +2%)
     - a feedstock disruption that names the polymers hit (Hormuz: polyester and spandex)
     - M&A, divestiture, or plant sale with a NAMED target or buyer in Americhem's supply
       chain or sales channel (Univar acquires Interpur; Sudarshan buys Clariant's dyes
       plant; Mutares buys SABIC's ETP business; a customer buying out a JV partner)
     - a supplier's bankruptcy, force majeure, or exit; a named customer program; a
       masterbatch / compounding competitor head-to-head with a stated quantity, date, or
       plant

9–10 — STRATEGIC: Americhem must act — a key supplier's exit, a binding regulation with a
     near deadline across a whole end-market, a major competitor acquiring a compounder.

{rule4}

{rule5}

RULE 6 — RIGOROUS, HONEST IMPACT STATEMENT:
Write a specific So-What for Americhem, but NEVER invent impact the article does not support.
Where the article supports a direct effect, identify which business unit or cost line is
affected and in what direction. Where the event is a RULE 3 WATCH-or-above event (score 5+),
the So-What states the mechanism IMPLIED by the actor's value-chain position, framed as an
inference rather than as a fact the article reports — e.g. "As a TiO2 supplier, Chemours'
increase raises Americhem's pigment input cost" or "Univar's added distribution reach can
shift additive channel pricing". That is the required So-What for such articles: a
low-exposure template on a WATCH event understates it and is wrong. Where the article
supports no direct effect and the event is FLOOR or 4-band (RULE 3), take one of the exits
below instead of naming a business unit speculatively.
- DIRECTION CONSISTENCY: the So-What's direction must agree with sentiment_tag. Never
  describe upside for Americhem under a "Negative" tag, or downside under a "Positive" tag.
- UPSIDE ROUTES THROUGH RULE 4: claim demand or sales upside ONLY when the mechanism runs
  through one of the RULE 4 commercial segments. If the market is adjacent to but outside
  those segments, write: "{adjacent_market_template} — no direct Americhem participation indicated."
- HONEST LOW EXPOSURE IS LEGAL: when true impact is limited, write
  "{limited_exposure_template} — [specific reason]" instead of inventing a commercial effect.
- FLOOR CLASS A (RULE 3, score 1–2) takes neither template: write
  "No material signal — [what the article actually is]" and score 1 or 2.
{low_exposure_score_rule}
Do NOT write "No direct impact. Monitoring required." — this exact phrase is banned.
Do NOT claim demand or sales UPSIDE ("may increase demand") without data from the article; a
cost, supply, channel, competitive, or regulatory mechanism implied by the actor's position
needs no such data.

RULE 7 — DOMAIN RELEVANCE FIREWALL:
Americhem is a plastics and specialty chemicals manufacturer. Only DISCARD if the article has
absolutely zero connection to plastics, polymers, chemicals, materials, manufacturing,
composites, packaging, or supply chain dynamics.
Examples of noise to DISCARD: sports results, political news, celebrity stories, unrelated
financial instruments (stock tips, crypto), or general HR policy.
When relevance is uncertain for an article that IS about the correct entity (RULE 1), do NOT
discard. Set americhem_impact_score to {uncertain_relevance_score} and apply Rule 6. This
never rescues a RULE 1 false match — that is DISCARD regardless of domain.

If the article passes all rules, extract data into this strict JSON schema.
Output ONLY the JSON object — no preamble, no markdown, no explanation.

{
  "headline": "<concise factual summary, max 12 words>",
  "source_publication": "<name of the publisher, e.g. Reuters, Chemical Week, Plastics News>",
  "article_summary": "<2-3 sentences, max 50 words. What happened, who is involved, key numbers. Factual only — no Americhem framing.>",
  "americhem_impact": "<BLUF So What for Americhem. Apply Rule 6. Never generic.>",
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


def _market_report_publisher_list() -> str:
    """RULE 3's FLOOR names the market-research publishers the ingestion gate
    drops by domain — one definition (market_reports.PUBLISHERS), wrapped to
    the prompt's hand-wrapped width."""
    return textwrap.fill(", ".join(market_reports.PUBLISHER_NAMES), width=90,
                         subsequent_indent="     ")


def _insight_system_prompt(config: dict) -> str:
    """Assemble the full system prompt, injecting commercial segment and signal
    type taxonomies. Assembly is str.replace() on named markers, never
    .format() — _SYSTEM_PROMPT_BASE's literal JSON braces are load-bearing."""
    rule4 = _build_commercial_segment_rule(config)
    rule5 = _build_signal_type_rule(config)
    scorer = Scoring.from_config(config)
    adjacent, limited = LOW_EXPOSURE_TEMPLATE_PREFIXES
    _, uncertain_score = low_exposure_score_band(scorer)
    return (
        _SYSTEM_PROMPT_BASE
        .replace("{english_output_rule}", ENGLISH_OUTPUT_RULE)
        .replace("{rule4}", rule4)
        .replace("{rule5}", rule5)
        .replace("{adjacent_market_template}", adjacent)
        .replace("{limited_exposure_template}", limited)
        .replace("{low_exposure_score_rule}", _build_low_exposure_score_rule(scorer))
        .replace("{uncertain_relevance_score}", str(uncertain_score))
        .replace("{market_report_publishers}", _market_report_publisher_list())
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
            "domain": insight.source_domain(url),
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
        "For each CATEGORY block below, write exactly one synthesis sentence (maximum 30 words).\n"
        "The sentence must:\n"
        "- Fuse the shared trend or structural driver across the listed signals with "
        "its implication for Americhem's supply chain, demand pipeline, or margin\n"
        "- Be written for a senior executive who will act on it — no hedging, no filler\n\n"
        "Return valid JSON with category names as keys and synthesis sentences as values.\n"
        "Use the exact category names provided. Do not invent categories.\n"
        "Only include categories that appear in the input."
    )

    return PromptSpec(
        system=system,
        user=grouped_text,
        temperature=None,
        context="thematic synthesis",
    )
