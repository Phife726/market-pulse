"""Probe Serper's news endpoint under several time filters — operator tool.

Why this exists: on 2026-08-26 production discovery yield collapsed with no
repo change (zero-result Serper queries jumped from ~60/128 to ~98/128; big
public companies such as Kimberly-Clark and Avient returned no news) while
Serper kept answering HTTP 200. `discovery.discover_urls` sent the
non-standard hour-count window ``tbs=qdr:h24``; the documented "past day"
value is ``qdr:d``. This script issues the *exact* production query shape
(``ingestion_engine.build_query``) for a few entities under each window and
prints a side-by-side table plus a verdict. The 2026-08-27 run CONFIRMED it
(``qdr:h24`` == past-hour ``qdr:h`` for 5/5 entities), so
``discovery.serper_time_filter`` now emits day forms; keep this probe for the
next time yield collapses with no repo change.

Usage (needs SERPER_API_KEY in the environment; ~4 credits per entity):

    python scripts/probe_serper.py
    python scripts/probe_serper.py --entities "Kimberly-Clark" "Avient" --filters qdr:h24 qdr:d

The pure helpers (``count_news``, ``sample_dates``, ``verdict``,
``format_table``) are tested in tests/test_probe_serper.py; only ``probe``
touches the network.
"""
import argparse
import os
import sys
from typing import Optional

import requests

# When run as `python scripts/probe_serper.py`, sys.path[0] is scripts/, not
# the repo root — add the repo root so ingestion_engine imports.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)

from ingestion_engine import build_query  # noqa: E402

ENDPOINT = "https://google.serper.dev/news"

# Entities that returned news on 2026-08-25 and nothing on 2026-08-27 — large
# enough that a genuinely empty 24h news window is implausible.
DEFAULT_ENTITIES = ["Kimberly-Clark", "Avient", "Chemours", "Lanxess", "Magna International"]

# Production window first, the documented past-day value second, past-hour
# to test the "h24 is being read as h" theory, and no filter as the ceiling.
DEFAULT_FILTERS: list[Optional[str]] = ["qdr:h24", "qdr:d", "qdr:h", None]

PRODUCTION_FILTER = "qdr:h24"  # the pre-#63 production window; kept as the comparison baseline
DOCUMENTED_DAY_FILTER = "qdr:d"


def count_news(body: dict) -> int:
    """Number of news items in a Serper response — same key discovery reads."""
    return len(body.get("news", []) or [])


def sample_dates(body: dict, limit: int = 3) -> list[str]:
    """First ``limit`` relative dates ("2 hours ago") for a recency eyeball."""
    dates = [item["date"] for item in body.get("news", []) or [] if item.get("date")]
    return dates[:limit]


def probe(query: str, tbs: Optional[str], api_key: str, timeout: int = 15) -> dict:
    """One live Serper call. Never raises on HTTP failure — records the status."""
    payload: dict = {"q": query, "num": 10}
    if tbs:
        payload["tbs"] = tbs
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
    try:
        response = requests.post(ENDPOINT, json=payload, headers=headers, timeout=timeout)
    except requests.exceptions.RequestException as exc:
        return {"query": query, "tbs": tbs, "status": 0, "count": 0, "sample_dates": [], "error": str(exc)}
    body = response.json() if response.ok else {}
    return {
        "query": query,
        "tbs": tbs,
        "status": response.status_code,
        "count": count_news(body),
        "sample_dates": sample_dates(body),
        "credits": body.get("credits"),
    }


def verdict(rows: list[dict]) -> str:
    """Compare the production window against the documented past-day window.

    CONFIRMED when ``qdr:d`` returns strictly more than ``qdr:h24`` for a
    majority of the entities with a clean (HTTP 200) pair; NOT CONFIRMED when
    the pairs agree; INCONCLUSIVE when no clean pair exists.
    """
    by_query: dict[str, dict[Optional[str], int]] = {}
    for row in rows:
        if row.get("status") != 200:
            continue
        by_query.setdefault(row["query"], {})[row["tbs"]] = row["count"]

    pairs = [
        (q, c[PRODUCTION_FILTER], c[DOCUMENTED_DAY_FILTER])
        for q, c in by_query.items()
        if PRODUCTION_FILTER in c and DOCUMENTED_DAY_FILTER in c
    ]
    if not pairs:
        return "INCONCLUSIVE — no entity has an HTTP-200 result for both qdr:h24 and qdr:d."

    under = [q for q, h24, d in pairs if d > h24]
    ratio = f"{len(under)}/{len(pairs)}"
    if len(under) * 2 > len(pairs):
        return (
            f"CONFIRMED — qdr:d out-returns qdr:h24 for {ratio} entities "
            f"({', '.join(under)}). Switch discovery.py's window to qdr:d for a 24h lookback."
        )
    return (
        f"NOT CONFIRMED — qdr:h24 and qdr:d agree for all but {ratio} entities; "
        "the yield drop is not the time-filter spelling. Look upstream (Serper/Google news volume)."
    )


def format_table(rows: list[dict]) -> str:
    header = f"{'entity':<24} {'tbs':<9} {'status':>6} {'count':>5}  sample dates"
    lines = [header, "-" * len(header)]
    for row in rows:
        tbs = row["tbs"] or "(none)"
        dates = ", ".join(row.get("sample_dates") or [])
        err = f"  [{row['error']}]" if row.get("error") else ""
        lines.append(f"{row['query'][:24]:<24} {tbs:<9} {row['status']:>6} {row['count']:>5}  {dates}{err}")
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--entities", nargs="+", default=DEFAULT_ENTITIES)
    parser.add_argument(
        "--filters", nargs="+", default=[f or "none" for f in DEFAULT_FILTERS],
        help="tbs values to try; the literal 'none' sends no tbs",
    )
    args = parser.parse_args(argv)

    api_key = os.environ.get("SERPER_API_KEY")
    if not api_key:
        print("SERPER_API_KEY is not set — export it and re-run.", file=sys.stderr)
        return 2

    filters = [None if f == "none" else f for f in args.filters]
    rows: list[dict] = []
    for entity in args.entities:
        query = build_query("entity", name=entity)
        for tbs in filters:
            rows.append(probe(query, tbs, api_key))
    # Table keys on the display name, not the quoted query string.
    for row in rows:
        row["query"] = row["query"].strip('"')

    print(format_table(rows))
    print()
    print(verdict(rows))
    return 0


if __name__ == "__main__":
    sys.exit(main())
