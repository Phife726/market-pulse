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


def _input_variants(company_id: int) -> list[tuple[str, str, dict]]:
    """Ordered input-only attribute shapes (no outputFields), baseline-first.

    Each entry is (label, top_level_key, attributes). Hand-written rather than
    generated so the matrix stays small and reviewable. The top_level_key lets
    the credit-aware walk tell "this input field is invalid" (pointer names the
    key) from "input accepted, something else needed" (pointer names another).
    """
    return [
        # Baseline — the known-bad singular field; reproduces the live pointer
        # `/data/attributes/companyId` for free (a 400 returns no record).
        ("companyId (baseline, known-bad)", "companyId", {"companyId": company_id}),
        # Array-shaped inputs ("use the Company IDs", "Multiple Inputs").
        ("companyIds[]", "companyIds", {"companyIds": [company_id]}),
        ("matchCompanyInput[]", "matchCompanyInput",
         {"matchCompanyInput": [{"companyId": company_id}]}),
    ]


def _with_output_fields(attrs: dict) -> dict:
    """Copy of *attrs* with the candidate outputFields list added."""
    out = dict(attrs)
    out["outputFields"] = _OUTPUT_FIELD_CANDIDATES
    return out


def build_candidates(company_id: int) -> list[tuple[str, dict]]:
    """Full probe matrix (every input shape, then each non-baseline input also
    with outputFields). Used only by the explicit `--all` opt-in; the default
    walk in run_probe stops early to conserve ZoomInfo enrich credits."""
    full = [(label, _wrap(attrs)) for label, _key, attrs in _input_variants(company_id)]
    for label, _key, attrs in _input_variants(company_id):
        if label.startswith("companyId "):  # don't add outputFields to the known-bad baseline
            continue
        full.append((f"{label} + outputFields", _wrap(_with_output_fields(attrs))))
    return full


def _pointer_mentions(result: dict, token: str) -> bool:
    """True if any captured 400 pointer references *token* (e.g. an input key)."""
    return any(token in p for p in result.get("pointers", []))


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


def run_probe(company_id: int, probe_all: bool = False) -> list[dict]:
    """Resolve a token and probe Company Enrich bodies. Returns sanitized results
    (possibly empty if no token). Never raises.

    Default is a CREDIT-AWARE walk: each Enrich 200 that returns a record charges
    a ZoomInfo credit for a non-managed company, so we stop as soon as the schema
    question is answered. 400s are free (no record), so the known-bad baseline and
    rejected inputs cost nothing. At most two credit-charging calls occur (one
    accepted input, plus one outputFields probe if the accepted input came back
    sparse or the API said outputFields is required). Pass probe_all=True to send
    the entire matrix regardless (explicit opt-in; may spend several credits)."""
    token = zoominfo_client._resolve_access_token()
    if not token:
        logger.error("No ZoomInfo token resolved — cannot probe Company Enrich.")
        return []
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    endpoint = zoominfo_client._enrich_endpoint()

    if probe_all:
        logger.info("Probing Company Enrich for company_id=%s (--all: full matrix)",
                    company_id)
        return [_probe_one(label, body, headers, endpoint)
                for label, body in build_candidates(company_id)]

    logger.info("Probing Company Enrich for company_id=%s (credit-aware walk)",
                company_id)
    results: list[dict] = []
    accepted: Optional[dict] = None   # the accepted input attributes
    needs_output = False
    for label, key, attrs in _input_variants(company_id):
        r = _probe_one(label, _wrap(attrs), headers, endpoint)
        results.append(r)
        if r["status"] == 200:
            accepted = attrs
            if any(r.get("populated", {}).values()):
                logger.info("Accepted input %r returned populated firmographics — "
                            "outputFields not required. Stopping.", label)
                return results
            logger.info("Accepted input %r but firmographics sparse — will probe "
                        "outputFields once.", label)
            needs_output = True
            break
        # 400 that names another field (not this input key) => input accepted but
        # something else (e.g. outputFields) is required. A 400 charges no credit.
        if r["status"] not in (None, 200) and not _pointer_mentions(r, key):
            accepted = attrs
            needs_output = True
            logger.info("Input %r appears accepted (400 does not name %r) — will "
                        "probe outputFields once.", label, key)
            break
        # else: input rejected (pointer names this key) — try the next shape, free.

    if accepted is not None and needs_output:
        results.append(_probe_one("accepted input + outputFields",
                                  _wrap(_with_output_fields(accepted)), headers, endpoint))
    elif accepted is None:
        logger.info("No candidate input shape was accepted — see the pointers above.")
    return results


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
    parser.add_argument("--all", action="store_true",
                        help="Send the full candidate matrix instead of the default "
                             "credit-aware early-stop walk (may spend several credits)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    company_id = _resolve_company_id(args.target, args.company_id, args.targets)
    if not company_id:
        logger.error("No company id for target %r (set --company-id or add "
                     "zoominfo_company_id in targets.yaml).", args.target)
        return 1
    results = run_probe(company_id, probe_all=args.all)
    accepted = [r for r in results if r.get("status") == 200]
    logger.info("Probe complete: %d/%d candidate(s) returned 200.",
                len(accepted), len(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
