"""Target-metadata enrichment CLI.

Reads targets.yaml, resolves/enriches each active entity target against ZoomInfo
(reusing the existing OAuth path), and writes a machine-managed companion file
`target_metadata.yaml` for FUTURE relevance filtering. Daily ingestion never
runs this — it consumes the checked-in, reviewed companion file only.

Dry-run by default: prints a unified diff and writes nothing. `--write` is the
only mutation path. Endpoint failures degrade to error/missing status and never
crash the run.
"""
from __future__ import annotations

import argparse
import difflib
import logging
import os
import sys
from datetime import date
from typing import Optional

import yaml

# When run as `python scripts/enrich_targets.py`, sys.path[0] is scripts/, not
# the repo root — add the repo root so target_enricher/zoominfo_client import.
# (Tests import this module via tests/conftest.py, which adds scripts/ instead.)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import target_enricher as te  # noqa: E402
import zoominfo_client  # noqa: E402

logger = logging.getLogger(__name__)

_FIELD_ORDER = [
    "target_key", "metadata_record_status", "zoominfo_company_id", "canonical_name",
    "hq_revenue_range", "employee_range", "primary_industry", "industries",
    "hq_country", "hq_state", "company_identity_terms", "industry_relevance_terms",
    "industry_unmapped", "zoominfo_metadata_status", "zoominfo_metadata_confidence",
    "zoominfo_metadata_last_refreshed", "manual_aliases", "exclude_terms",
]


class _DefaultClient:
    """Adapter so the CLI depends on an injectable object, not the module."""
    def resolve_company(self, **kwargs):
        return zoominfo_client.resolve_company(**kwargs)

    def enrich_company(self, company_id):
        return zoominfo_client.enrich_company(company_id)


def load_targets_for_enrichment(targets_path: str) -> list[dict]:
    """Return active entity-mode targets only (concept groups are out of scope).

    Each dict carries name, optional zoominfo_company_id, and optional
    domain/hq_country/hq_state resolution hints if present in targets.yaml.
    """
    with open(targets_path, "r") as fh:
        config = yaml.safe_load(fh) or {}
    out: list[dict] = []
    for group_name, group_cfg in config.items():
        if group_name == "discovery" or not isinstance(group_cfg, dict):
            continue
        if group_cfg.get("search_mode", "entity") != "entity":
            continue
        for entity in group_cfg.get("entities", []):
            if not entity.get("active", False):
                continue
            out.append({
                "name": entity["name"],
                "zoominfo_company_id": entity.get("zoominfo_company_id"),
                "domain": entity.get("domain"),
                "hq_country": entity.get("hq_country"),
                "hq_state": entity.get("hq_state"),
            })
    return out


def _resolve(target: dict, client) -> dict:
    """Run the resolution cascade for one target; return a normalized dict."""
    cid = target.get("zoominfo_company_id")
    if cid:
        return {"company_id": cid, "match_basis": "precurated"}

    # 1. website/domain
    if target.get("domain"):
        r = client.resolve_company(domain=target["domain"], name=None,
                                   hq_country=None, hq_state=None)
        if r["status"] == "error":
            return {"error": True}
        if r["status"] == "ok":
            return {"company_id": r["company_id"], "match_basis": "domain"}

    # 2. exact name + HQ hints
    if target.get("hq_country") or target.get("hq_state"):
        r = client.resolve_company(domain=None, name=target["name"],
                                   hq_country=target.get("hq_country"),
                                   hq_state=target.get("hq_state"))
        if r["status"] == "error":
            return {"error": True}
        if r["status"] == "ok":
            return {"company_id": r["company_id"], "match_basis": "name_hq"}

    # 3. name only
    r = client.resolve_company(domain=None, name=target["name"],
                               hq_country=None, hq_state=None)
    if r["status"] == "error":
        return {"error": True}
    if r["status"] == "ok":
        return {"company_id": r["company_id"], "match_basis": "name"}
    return {"match_basis": None}


def _ordered(record: dict) -> dict:
    """Reorder a record's keys for stable, readable YAML output."""
    ordered = {k: record[k] for k in _FIELD_ORDER if k in record}
    for k, v in record.items():  # any unexpected keys preserved at the end
        if k not in ordered:
            ordered[k] = v
    return ordered


def _render(targets: dict) -> str:
    doc = {"version": 1,
           "targets": {k: _ordered(targets[k]) for k in sorted(targets)}}
    header = ("# MACHINE-MANAGED by scripts/enrich_targets.py. Edit ONLY the "
              "human-curated\n# fields (manual_aliases, exclude_terms); the "
              "enricher preserves them.\n")
    return header + yaml.safe_dump(doc, sort_keys=False, allow_unicode=True,
                                   default_flow_style=False)


def _load_existing(out_path: str) -> tuple[str, dict]:
    """Return (raw_text, prior_targets_dict). Missing file -> ("", {})."""
    try:
        with open(out_path, "r") as fh:
            text = fh.read()
    except FileNotFoundError:
        return "", {}
    data = yaml.safe_load(text) or {}
    return text, (data.get("targets") or {})


def run(*, targets_path: str, out_path: str, only: Optional[str],
        write: bool, today: str, client=None) -> int:
    """Enrich targets and either print a diff (dry-run) or write the file."""
    client = client or _DefaultClient()
    targets = load_targets_for_enrichment(targets_path)
    active_keys = {t["name"] for t in targets}
    if only:
        targets = [t for t in targets if t["name"] == only]

    existing_text, prior_targets = _load_existing(out_path)

    proposed: dict = {}
    for target in targets:
        key = target["name"]
        resolution = _resolve(target, client)
        enrichment = None
        if resolution.get("company_id") is not None:
            enrichment = client.enrich_company(resolution["company_id"])
        record = te.build_proposed_metadata(
            target_key=key, target_name=key,
            prior_record=prior_targets.get(key),
            resolution=resolution, enrichment=enrichment,
        )
        record["zoominfo_metadata_last_refreshed"] = today
        # Operator pointer: Enrich succeeded but returned no usable firmographics,
        # so the record degrades to `missing`. This is the live `outputFields`
        # risk — the Company Enrich body likely needs an outputFields list. (An
        # auth/entitlement failure is a distinct `error`, not reported here.)
        if (enrichment or {}).get("status") == "ok" and \
                record.get("zoominfo_metadata_status") == "missing":
            logger.warning(
                "ZoomInfo Enrich returned ok but SPARSE firmographics for %r "
                "(company_id=%s) — recording 'missing'. Confirm outputFields on "
                "an entitled live run.", key, resolution.get("company_id"),
            )
        proposed[key] = record

    merged = te.merge_targets(prior_targets, proposed, active_keys)
    new_text = _render(merged)

    if write:
        with open(out_path, "w") as fh:
            fh.write(new_text)
        logger.info("Wrote %d target record(s) to %s", len(merged), out_path)
        return 0

    diff = difflib.unified_diff(
        existing_text.splitlines(keepends=True),
        new_text.splitlines(keepends=True),
        fromfile=out_path + " (current)", tofile=out_path + " (proposed)",
    )
    sys.stdout.writelines(diff)
    if not existing_text:
        # No prior file: unified_diff against "" still shows additions, but make
        # the proposed content unmistakable for a first run.
        sys.stdout.write(new_text)
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Enrich target metadata from ZoomInfo (dry-run by default).")
    parser.add_argument("--targets", default="targets.yaml",
                        help="Path to targets.yaml (default: targets.yaml)")
    parser.add_argument("--out", default="target_metadata.yaml",
                        help="Path to the companion file (default: target_metadata.yaml)")
    parser.add_argument("--only", default=None,
                        help="Restrict to a single target by name")
    parser.add_argument("--write", action="store_true",
                        help="Apply changes (default: dry-run prints a diff)")
    parser.add_argument("--today", default=None,
                        help="Override the YYYY-MM-DD refresh stamp (testing)")
    args = parser.parse_args(argv)

    today = args.today
    if today is None:
        today = date.today().isoformat()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    return run(targets_path=args.targets, out_path=args.out, only=args.only,
               write=args.write, today=today, client=_DefaultClient())


if __name__ == "__main__":
    raise SystemExit(main())
