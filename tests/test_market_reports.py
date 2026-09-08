"""market_reports.py — the market-research publisher gate, one definition.

The publisher domains and the "market forecast to 20XX" headline pattern are
what the ingestion gauntlet drops before the scrape (so a market report can
never reach the scorer, whatever RULE 3 says), what the insight prompt's RULE 3
FLOOR names, and what delivery rule 4 checks on a stored row's publication.
The labeled backtest set is the oracle: every `suppress` row whose reason is
market-report boilerplate must match the title pattern, and every `surface`
row must not.
"""
import csv
import pathlib

import pytest

from market_reports import (
    PUBLISHERS,
    PUBLISHER_DOMAINS,
    PUBLISHER_NAMES,
    is_market_report_candidate,
    is_market_report_domain,
    is_market_report_publication,
    is_market_report_title,
)

_BACKTEST = pathlib.Path(__file__).resolve().parents[1] / "backtest" / "market_pulse_scorer_backtest.csv"


def _rows() -> list[dict]:
    with open(_BACKTEST, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _labeled_market_reports() -> list[tuple[str, str]]:
    return [(r["backtest_id"], r["headline"]) for r in _rows()
            if r["expected_tier"] == "suppress" and "market-report" in r["label_reason"].lower()]


def _labeled_surface() -> list[tuple[str, str]]:
    return [(r["backtest_id"], r["headline"]) for r in _rows() if r["expected_tier"] == "surface"]


def test_the_definition_is_consistent():
    assert len(PUBLISHERS) >= 10
    assert PUBLISHER_NAMES == tuple(name for name, _ in PUBLISHERS)
    assert PUBLISHER_DOMAINS == frozenset(domain for _, domain in PUBLISHERS)
    for name, domain in PUBLISHERS:
        assert name.strip() == name and domain == domain.lower() and "/" not in domain


@pytest.mark.parametrize("url", [
    "https://www.indexbox.io/blog/vinyl-siding-panel-market-forecast/",
    "https://www.factmr.com/report/halogen-free-polymer-aids-market",
    "https://www.futuremarketinsights.com/reports/low-warpage-resins-market",
    "https://www.openpr.com/news/4579293/competitive-analysis",
    "https://www.lucintel.com/plastic-pipe-market.aspx",
    "https://www.fortunebusinessinsights.com/green-packaging-market-100256",
    "https://www.marketsandmarkets.com/Market-Reports/color-masterbatch-market.html",
    "https://www.grandviewresearch.com/industry-analysis/automotive-interior-materials-market",
    "https://www.researchandmarkets.com/reports/agricultural-films",
    "https://www.thebusinessresearchcompany.com/report/endoscopy-sterilization",
    "https://reports.custommarketinsights.com/x",     # a subdomain
])
def test_publisher_domains_match_with_subdomains(url):
    assert is_market_report_domain(url) is True


@pytest.mark.parametrize("url", [
    "https://www.plasticsnews.com/resin-prices/sp-polyolefins-price-plunge-july/",
    "https://www.globenewswire.com/news-release/2026/07/28/univar-acquires-interpur.html",
    "https://www.prnewswire.com/news-releases/chemours-q2-2026-results.html",
    "https://notindexbox.io/blog/x",                  # suffix match is on the dot
    "", "not a url", None,
])
def test_non_publisher_urls_do_not_match(url):
    assert is_market_report_domain(url) is False


@pytest.mark.parametrize("bt_id, headline", _labeled_market_reports())
def test_title_pattern_catches_every_labeled_market_report_headline(bt_id, headline):
    assert is_market_report_title(headline) is True, f"{bt_id}: {headline}"


@pytest.mark.parametrize("bt_id, headline", _labeled_surface())
def test_title_pattern_spares_every_labeled_surface_headline(bt_id, headline):
    assert is_market_report_title(headline) is False, f"{bt_id}: {headline}"


@pytest.mark.parametrize("title", [
    # Serper page titles, publisher suffix and all
    "Vinyl Siding Panel Market Forecast Points Higher Toward 2035 Driven by Renovation | IndexBox",
    "Agricultural Films Market to Reach USD 29.08 Billion by 2033 - GlobeNewswire",
    "Thermoplastic Elastomer Market Size to Reach $44.81 Billion by 2030 - openPR",
    "Color Masterbatch Market worth $6.68 billion by 2031 - MarketsandMarkets",
    "Hexamethylenediamine Market Size to Surpass USD 18.59 Billion by 2035 on Strong Nylon 6,6 Demand",
    "Low Warpage Resins Market Highlights RTP Company Among Notable Players",
    "Lucintel Forecasts Plastic Pipe Market Growth to 2035",
])
def test_title_pattern_catches_wire_syndications_and_page_titles(title):
    assert is_market_report_title(title) is True


@pytest.mark.parametrize("title", [
    "North American resin markets swing as buyers regain leverage",
    "Morgan Stanley lifts July ISM Manufacturing PMI forecast to 53.8",
    "Global light-vehicle sales forecast shows sixth monthly demand decline",
    "Kimberly-Clark cuts forecasts amid ongoing China diaper claims",
    "Europe gas crisis keeps energy prices high through 2027",
    "Resin market shifts favor buyers amid inventory surge",
    "Chemours announces additional TiO2 price increase effective June 1",
    "", None,
])
def test_title_pattern_spares_ordinary_news_that_mentions_markets_or_forecasts(title):
    assert is_market_report_title(title) is False


def test_candidate_check_is_domain_or_title():
    assert is_market_report_candidate("https://www.indexbox.io/blog/x", "Anything at all") is True
    assert is_market_report_candidate("https://www.globenewswire.com/x",
                                      "Films Market to Reach USD 29 Billion by 2033") is True
    assert is_market_report_candidate("https://www.globenewswire.com/x",
                                      "Univar Solutions Acquires H.M. Royal") is False


@pytest.mark.parametrize("publication, expected", [
    ("IndexBox", True), ("Fact.MR", True), ("fortune business insights", True),
    ("Research and Markets (via GlobeNewswire)", True),
    ("Plastics News", False), ("Reuters", False), ("", False), (None, False),
])
def test_publication_check_matches_a_publisher_name_anywhere_case_insensitively(publication, expected):
    assert is_market_report_publication(publication) is expected
