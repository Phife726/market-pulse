"""Live Company Enrich request-schema probe (TEMPORARY diagnostic).

The live Avient smoke proved OAuth + Company Enrich are entitled but returned a
400 whose pointer (`/data/attributes/companyId`) said the singular `companyId`
input field is invalid for the GTM Company Enrich endpoint — without naming the
correct one. ZoomInfo's docs imply an array input ("use the Company **IDs**",
"Multiple Inputs") plus a required `outputFields` list, but the exact tokens are
account-specific and not enumerable from the public (JS-rendered) docs.

This probe POSTs an explicit, reviewable set of doc-plausible request bodies to
the SAME Company Enrich endpoint and logs each one's SANITIZED outcome — HTTP
status + the API's own `source.pointer`/`code` for a 4xx, or a keys-only shape
summary + per-field "populated" booleans for a 200. The live API is the oracle:
whichever body the API accepts (and whether `outputFields` is needed) is then
wired into `zoominfo_client.enrich_company` in a tiny follow-up — no token is
committed to code until the API confirms it here.

Read-only and safe: never writes a file, never raises, and never logs the
bearer token, request/response auth headers, the request body values, or raw
firmographic values (only field NAMES and populated yes/no).

Delete this script (and its smoke-workflow step) once the schema is wired.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional

import requests

# Run-as-script path shim (mirrors scripts/enrich_targets.py).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import target_enricher as te  # noqa: E402
import zoominfo_client  # noqa: E402

logger = logging.getLogger("probe_company_enrich")

_REQUEST_TIMEOUT = 15  # seconds

# Doc-plausible OUTPUT field tokens. These are NOT yet verified — the probe sends
# them so the live API reveals (via a `/data/attributes/outputFields/...` pointer)
# which spellings are invalid for this account. Do not treat as confirmed.
_OUTPUT_FIELD_CANDIDATES = [
    "name", "revenue", "employeeCount", "primaryIndustry",
    "industries", "country", "state",
]


def _wrap(attributes: dict) -> dict:
    """Wrap enrichment attributes in the JSON:API CompanyEnrich envelope (the
    `/data/attributes` wrapper the live 400 confirmed is accepted)."""
    return {"data": {"type": "CompanyEnrich", "attributes": attributes}}


def build_candidates(company_id: int) -> list[tuple[str, dict]]:
    """Explicit, ordered list of (label, request-body) candidates to probe.

    Hand-written rather than generated so the matrix stays small and reviewable.
    Ordered baseline-first so the logs reproduce the known 400 pointer before the
    array-shaped inputs the docs imply.
    """
    return [
        # Baseline — the known-bad singular field; confirms the probe reproduces
        # the live pointer `/data/attributes/companyId`.
        ("companyId (baseline, known-bad)", _wrap({"companyId": company_id})),
        # Array-shaped inputs ("use the Company IDs", "Multiple Inputs").
        ("companyIds[]", _wrap({"companyIds": [company_id]})),
        ("matchCompanyInput[]", _wrap({"matchCompanyInput": [{"companyId": company_id}]})),
        # Same array inputs, now also requesting outputFields, to learn whether
        # outputFields is required and whether the candidate tokens are valid.
        ("companyIds[] + outputFields",
         _wrap({"companyIds": [company_id], "outputFields": _OUTPUT_FIELD_CANDIDATES})),
        ("matchCompanyInput[] + outputFields",
         _wrap({"matchCompanyInput": [{"companyId": company_id}],
                "outputFields": _OUTPUT_FIELD_CANDIDATES})),
    ]


def _extract_pointers(payload: object) -> tuple[list[str], list[str]]:
    """Pull `errors[].source.pointer` and `errors[].code` from a 4xx JSON body.

    These are schema pointers/codes (e.g. "/data/attributes/companyId",
    "PFAPI0005") — never firmographic values or secrets."""
    pointers: list[str] = []
    codes: list[str] = []
    if isinstance(payload, dict):
        for err in payload.get("errors", []) or []:
            if not isinstance(err, dict):
                continue
            pointer = (err.get("source") or {}).get("pointer")
            if isinstance(pointer, str):
                pointers.append(pointer)
            code = err.get("code")
            if isinstance(code, str):
                codes.append(code)
    return pointers, codes


def _populated_flags(company: dict) -> dict:
    """Which core firmographics came back non-empty (names/booleans only)."""
    firmo = te.extract_firmographics(company if isinstance(company, dict) else {})
    return {
        "canonical_name": bool(firmo.get("canonical_name")),
        "primary_industry": bool(firmo.get("primary_industry")),
        "industries": bool(firmo.get("industries")),
    }


def _probe_one(label: str, body: dict, headers: dict, endpoint: str) -> dict:
    """POST one candidate body; return a sanitized outcome dict. Never raises."""
    try:
        response = requests.post(
            endpoint, json=body, headers=headers, timeout=_REQUEST_TIMEOUT
        )
    except requests.exceptions.RequestException as exc:
        logger.warning("probe[%s] transport error: %s", label, exc)
        return {"label": label, "status": None, "error": str(exc)}

    status = getattr(response, "status_code", None)
    try:
        data = response.json()
    except ValueError:
        data = None

    if status != 200:
        pointers, codes = _extract_pointers(data)
        snippet = zoominfo_client._response_snippet(response)
        logger.info(
            "probe[%s] status=%s pointers=%s codes=%s snippet=%s",
            label, status, pointers, codes, snippet,
        )
        return {"label": label, "status": status,
                "pointers": pointers, "codes": codes, "snippet": snippet}

    companies = zoominfo_client._extract_company_list(data)
    shape = (f"{zoominfo_client._summarize_response_shape(data)} | "
             f"{zoominfo_client._summarize_first_item_shape(companies)}")
    company = companies[0] if companies else {}
    attrs = company.get("attributes") if isinstance(company.get("attributes"), dict) else company
    populated = _populated_flags(attrs)
    logger.info("probe[%s] status=200 shape=%s populated=%s", label, shape, populated)
    return {"label": label, "status": 200, "shape": shape, "populated": populated}


def run_probe(company_id: int) -> list[dict]:
    """Resolve a token and probe every candidate body. Returns sanitized results
    (possibly empty if no token). Never raises."""
    token = zoominfo_client._resolve_access_token()
    if not token:
        logger.error("No ZoomInfo token resolved — cannot probe Company Enrich.")
        return []
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    endpoint = zoominfo_client._enrich_endpoint()
    logger.info("Probing Company Enrich for company_id=%s (%d candidates)",
                company_id, len(build_candidates(company_id)))
    return [_probe_one(label, body, headers, endpoint)
            for label, body in build_candidates(company_id)]


def _resolve_company_id(target: Optional[str], company_id: Optional[int],
                        targets_path: str) -> Optional[int]:
    """Use an explicit --company-id if given, else look it up from targets.yaml."""
    if company_id:
        return company_id
    import enrich_targets  # local import; scripts/ is on sys.path under tests
    for t in enrich_targets.load_targets_for_enrichment(targets_path):
        if t["name"] == target and t.get("zoominfo_company_id"):
            return int(t["zoominfo_company_id"])
    return None


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Probe the live ZoomInfo Company Enrich request schema (read-only).")
    parser.add_argument("--target", default="Avient",
                        help="Target name in targets.yaml to read a company id from")
    parser.add_argument("--company-id", type=int, default=None,
                        help="Explicit ZoomInfo company id (overrides --target lookup)")
    parser.add_argument("--targets", default="targets.yaml", help="Path to targets.yaml")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    company_id = _resolve_company_id(args.target, args.company_id, args.targets)
    if not company_id:
        logger.error("No company id for target %r (set --company-id or add "
                     "zoominfo_company_id in targets.yaml).", args.target)
        return 1
    results = run_probe(company_id)
    accepted = [r for r in results if r.get("status") == 200]
    logger.info("Probe complete: %d/%d candidate(s) returned 200.",
                len(accepted), len(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
