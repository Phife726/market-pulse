"""Compact review table for a proposed target_metadata.yaml.

Surfaces target -> resolved id, canonical_name, HQ country, and primary industry
so name-only mismatches are easy to spot across many records. A row is flagged
(⚠) when EITHER:
  - the target name and canonical_name share no token (likely wrong entity), OR
  - the resolved primary_industry is off-domain (clearly not a chemicals /
    plastics / manufacturing company — e.g. Software, Hospitality).
Unresolved rows show as (∅). Flagged/unresolved rows sort to the top.

The HQ COUNTRY column is shown but not auto-flagged: many legitimate targets are
non-US (BASF, SABIC, Radici), so a foreign HQ is a *look*, not a fail. It is the
tell for regional-subsidiary mis-resolutions (a US parent resolving to its
China/Mexico/India entity) — scan it by eye.
"""
from __future__ import annotations

import sys

import yaml

# Primary industries a chemicals/plastics/manufacturing target should never
# resolve to. High precision on purpose — only clearly off-domain categories, so
# a hit is a strong "wrong company" signal rather than taxonomy noise.
OFF_DOMAIN_INDUSTRIES = {
    "business services", "software", "media & internet",
    "law firms & legal services", "hospitality", "finance", "banking",
    "insurance", "real estate", "retail", "restaurants", "consumer services",
    "telecommunications", "education", "government", "healthcare services",
    "transportation & logistics",
}


def _tokens(s: str) -> set[str]:
    """Alphanumeric tokens of length >= 2, lowercased (keeps '3m', drops noise)."""
    return {t for t in "".join(c.lower() if c.isalnum() else " " for c in s).split()
            if len(t) >= 2}


def flag_for(key: str, cid, canon: str, industry: str) -> tuple[str, str]:
    """Return (flag, reason). '∅' unresolved; '⚠' name and/or industry mismatch."""
    if not cid:
        return "∅", "no id"
    reasons = []
    if canon and not (_tokens(key) & _tokens(canon)):
        reasons.append("name")
    if industry.strip().lower() in OFF_DOMAIN_INDUSTRIES:
        reasons.append("industry")
    return ("⚠", "+".join(reasons)) if reasons else ("", "")


def build_rows(recs: dict) -> list[tuple]:
    rows = []
    for key, r in recs.items():
        if not isinstance(r, dict):
            continue
        cid = r.get("zoominfo_company_id")
        canon = r.get("canonical_name") or ""
        country = r.get("hq_country") or ""
        industry = r.get("primary_industry") or ""
        conf = r.get("zoominfo_metadata_confidence") or ""
        status = r.get("zoominfo_metadata_status") or ""
        flag, reason = flag_for(key, cid, canon, industry)
        rows.append((flag, key, str(cid or "—"), canon, country, industry,
                     f"{conf}/{status}", reason))
    order = {"⚠": 0, "∅": 1, "": 2}
    rows.sort(key=lambda x: (order[x[0]], x[1].lower()))
    return rows


def main(path: str) -> int:
    with open(path) as fh:
        data = yaml.safe_load(fh) or {}
    rows = build_rows(data.get("targets") or {})
    if not rows:
        print("(no target records found)")
        return 0

    w_name = max(len(r[1]) for r in rows)
    w_canon = min(max(len(r[3]) for r in rows), 44)
    w_country = max(len(r[4]) for r in rows + [("", "", "", "", "COUNTRY")])
    print(f"{'':1} {'TARGET':<{w_name}}  {'ID':<11}  {'CANONICAL':<{w_canon}}  "
          f"{'COUNTRY':<{w_country}}  {'INDUSTRY':<22}  CONF/STATUS  WHY")
    print("-" * (w_name + w_canon + w_country + 62))
    for flag, key, cid, canon, country, industry, confstat, reason in rows:
        print(f"{flag:1} {key:<{w_name}}  {cid:<11}  {canon[:w_canon]:<{w_canon}}  "
              f"{country:<{w_country}}  {industry[:22]:<22}  {confstat:<11}  {reason}")

    n_flag = sum(1 for r in rows if r[0] == "⚠")
    n_none = sum(1 for r in rows if r[0] == "∅")
    n_ok = sum(1 for r in rows if r[0] == "")
    print(f"\n{len(rows)} records: {n_ok} plausible, {n_flag} ⚠ review "
          f"(name/industry), {n_none} ∅ unresolved")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1] if len(sys.argv) > 1 else "target_metadata.yaml"))
