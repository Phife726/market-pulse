"""Compact review table for a proposed target_metadata.yaml.

Surfaces target -> resolved id + canonical_name so name-only mismatches are
easy to spot across many records. Rows where the target name and canonical_name
share no token are flagged (⚠) as likely mis-resolutions; unresolved rows show
as (∅). Flagged/unresolved rows sort to the top.
"""
from __future__ import annotations

import sys

import yaml


def _tokens(s: str) -> set[str]:
    """Alphanumeric tokens of length >= 2, lowercased (keeps '3m', drops noise)."""
    return {t for t in "".join(c.lower() if c.isalnum() else " " for c in s).split()
            if len(t) >= 2}


def build_rows(recs: dict) -> list[tuple]:
    rows = []
    for key, r in recs.items():
        if not isinstance(r, dict):
            continue
        cid = r.get("zoominfo_company_id")
        canon = r.get("canonical_name") or ""
        conf = r.get("zoominfo_metadata_confidence") or ""
        status = r.get("zoominfo_metadata_status") or ""
        shared = _tokens(key) & _tokens(canon) if (canon and cid) else set()
        flag = "⚠" if (cid and not shared) else ("∅" if not cid else "")
        rows.append((flag, key, str(cid or "—"), canon, conf, status))
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
    w_canon = max(len(r[3]) for r in rows)
    print(f"{'':1} {'TARGET':<{w_name}}  {'ID':<10}  {'CANONICAL':<{w_canon}}  CONF/STATUS")
    print("-" * (w_name + w_canon + 40))
    for flag, key, cid, canon, conf, status in rows:
        print(f"{flag:1} {key:<{w_name}}  {cid:<10}  {canon:<{w_canon}}  {conf}/{status}")

    n_flag = sum(1 for r in rows if r[0] == "⚠")
    n_none = sum(1 for r in rows if r[0] == "∅")
    n_ok = sum(1 for r in rows if r[0] == "")
    print(f"\n{len(rows)} records: {n_ok} plausible, {n_flag} ⚠ likely mismatch, "
          f"{n_none} ∅ unresolved")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1] if len(sys.argv) > 1 else "target_metadata.yaml"))
