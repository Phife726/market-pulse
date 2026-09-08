"""Pure module owning the market-research publisher gate — one definition.

Discovery pulls in market-research press releases — "X Market to Reach USD N
Billion by 20XX (CAGR Y%)" from IndexBox, Fact.MR, Future Market Insights and
the wires that syndicate them — which name a customer or competitor "among
key players" and read as relevant to a scorer. They never are: a forecast of
a market's size is not an event. Three consumers share this definition so
they cannot drift:

- `ingestion_engine.process_candidate` drops a matching candidate BEFORE the
  scrape (reason `market_report_publisher`), so a market report never reaches
  the scorer, whatever RULE 3 says;
- `prompts` names `PUBLISHER_NAMES` in RULE 3's FLOOR, so one that does reach
  the model (a wire the headline pattern missed) still scores <= 3;
- `report` rule 4 drops a stored row whose domain or publication is one of
  these, whatever it scored — the rows stored before the gate existed.

Zero I/O, no clock, no config read.
"""
import re
from typing import Optional
from urllib.parse import urlparse

#: (display name, registrable domain). The name is what the prompt says and
#: what a stored row's `source_publication` is matched against; the domain is
#: what the ingestion gate and delivery rule 4 match a URL's host against
#: (suffix match, so every subdomain counts). openPR is a press-release wire,
#: not a research house, but in this pipeline's discovery it has carried
#: nothing else; it is on the list by name (issue: the 2026-09-08 scorer
#: recalibration).
PUBLISHERS: tuple[tuple[str, str], ...] = (
    ("IndexBox", "indexbox.io"),
    ("Fact.MR", "factmr.com"),
    ("Future Market Insights", "futuremarketinsights.com"),
    ("MarketsandMarkets", "marketsandmarkets.com"),
    ("Grand View Research", "grandviewresearch.com"),
    ("Fortune Business Insights", "fortunebusinessinsights.com"),
    ("Lucintel", "lucintel.com"),
    ("Research and Markets", "researchandmarkets.com"),
    ("The Business Research Company", "thebusinessresearchcompany.com"),
    ("Custom Market Insights", "custommarketinsights.com"),
    ("Precedence Research", "precedenceresearch.com"),
    ("Straits Research", "straitsresearch.com"),
    ("Persistence Market Research", "persistencemarketresearch.com"),
    ("Transparency Market Research", "transparencymarketresearch.com"),
    ("SNS Insider", "snsinsider.com"),
    ("24ChemicalResearch", "24chemicalresearch.com"),
    ("24MarketReports", "24marketreports.com"),
    ("ReportPrime", "reportprime.com"),
    ("Market.us", "market.us"),
    ("openPR", "openpr.com"),
)

PUBLISHER_NAMES: tuple[str, ...] = tuple(name for name, _ in PUBLISHERS)
PUBLISHER_DOMAINS: frozenset[str] = frozenset(domain for _, domain in PUBLISHERS)

_NAMES_FOLDED: tuple[str, ...] = tuple(name.casefold() for name in PUBLISHER_NAMES)

_YEAR = r"20[2-5]\d"
_FORECAST_WORDS = (
    r"(?:forecast(?:s|ed)?|projected|projects|CAGR|worth|"
    r"to\s+(?:reach|surpass|hit|exceed|top|cross))"
)
_TO_YEAR = r"(?:by|through|to|toward|towards|until|till)\s+" + _YEAR + r"\b"

#: The "market forecast to 20XX" headline shapes, on a Serper page title or a
#: stored headline. Each pattern needs the market-research framing, not just
#: a word: "market" AND a forecast word (either order), a report-style noun
#: after "market", a forecast word with a horizon year, "market" with a
#: horizon year, or the "among key players" tell. A forecast without a
#: horizon ("PMI forecast to 53.8", "cuts forecasts") and ordinary news that
#: says "market" ("resin markets swing") do not match.
_TITLE_PATTERNS: tuple[re.Pattern, ...] = (
    re.compile(r"\bmarkets?\b.*?\b" + _FORECAST_WORDS + r"\b", re.I | re.S),
    re.compile(r"\b" + _FORECAST_WORDS + r"\b.*?\bmarkets?\b", re.I | re.S),
    re.compile(r"\bmarket\s+(?:size|report|analysis|outlook|trends|insights|growth|"
               r"research|overview|highlights|study)\b", re.I),
    re.compile(r"\b" + _FORECAST_WORDS + r"\b.*?\b" + _TO_YEAR, re.I | re.S),
    re.compile(r"\bmarkets?\b.*?\b" + _TO_YEAR, re.I | re.S),
    re.compile(r"\b(?:key|notable|leading|major|top|prominent)\s+"
               r"(?:players|participants|vendors|companies|suppliers)\b", re.I),
)


def is_market_report_domain(url: object) -> bool:
    """True when `url`'s host is (a subdomain of) a market-research
    publisher. '' / None / unparseable -> False (the gate lets the scraper
    decide, as the unscrapable-domain gate does)."""
    if not isinstance(url, str) or not url:
        return False
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        return False
    if not host:
        return False
    return any(host == d or host.endswith("." + d) for d in PUBLISHER_DOMAINS)


def is_market_report_title(title: object) -> bool:
    """True when a page title or headline reads as a market-research
    forecast (`_TITLE_PATTERNS`) or names a publisher outright."""
    if not isinstance(title, str) or not title.strip():
        return False
    folded = title.casefold()
    if any(name in folded for name in _NAMES_FOLDED):
        return True
    return any(p.search(title) for p in _TITLE_PATTERNS)


def is_market_report_candidate(url: object, title: object) -> bool:
    """The ingestion gate: a publisher domain, or a market-report headline on
    any domain (the wires syndicate the same releases)."""
    return is_market_report_domain(url) or is_market_report_title(title)


def is_market_report_publication(publication: object) -> bool:
    """True when a stored row's LLM-extracted `source_publication` names a
    publisher — anywhere in the string, case-insensitively ("Research and
    Markets (via GlobeNewswire)" counts)."""
    if not isinstance(publication, str) or not publication.strip():
        return False
    folded = publication.casefold()
    return any(name in folded for name in _NAMES_FOLDED)
