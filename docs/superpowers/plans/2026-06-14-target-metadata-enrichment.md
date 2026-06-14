# Target Metadata Enrichment Utility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a safe, reviewable ZoomInfo-backed utility that populates a machine-managed `target_metadata.yaml` companion file for future relevance filtering, without touching daily ingestion.

**Architecture:** Approach A — a thin online layer (two additive functions in `zoominfo_client.py` that reuse existing OAuth) feeds a pure, I/O-free transform module (`target_enricher.py`), orchestrated by a CLI (`scripts/enrich_targets.py`). Dry-run prints a unified diff; `--write` is the only mutation path. Per-endpoint entitlement failures degrade to `error`/`missing` status and never crash the run or overwrite good data.

**Tech Stack:** Python 3, `requests` (mocked in tests), `PyYAML`, `difflib`, `argparse`, `pytest`.

**Spec:** `docs/superpowers/specs/2026-06-14-target-metadata-enrichment-design.md`

---

## File Structure

| File | Responsibility | Action |
|------|----------------|--------|
| `zoominfo_client.py` | Add `resolve_company()` + `enrich_company()` + endpoint helpers. Reuse `_resolve_access_token()`; return status dicts; never raise. | Modify (additive) |
| `target_enricher.py` | Pure transforms: de-suffix guardrail, identity/industry terms, firmographics extraction, `build_proposed_metadata`, `merge_targets`. Zero network/file I/O. | Create |
| `scripts/enrich_targets.py` | CLI orchestration + all I/O: load targets, resolution cascade, enrich, stamp date, merge, diff/write. | Create |
| `tests/test_zoominfo_company.py` | Mocked tests for the two new client functions. | Create |
| `tests/test_target_enricher.py` | Pure-module tests (the bulk of coverage; no mocks). | Create |
| `tests/test_enrich_targets_cli.py` | CLI tests with a fake client (no live calls). | Create |
| `README.md` | Usage + per-endpoint entitlement caveat + "ingestion consumes reviewed metadata only" boundary. | Modify |
| `.env.example` | Document the new endpoint override env vars. | Modify |

### Data contracts (consistent across all tasks)

**`zoominfo_client.resolve_company(*, domain=None, name=None, hq_country=None, hq_state=None) -> dict`** — performs ONE search; returns:
- `{"status": "ok", "company_id": <int>}`
- `{"status": "empty"}` (HTTP 200, zero candidates)
- `{"status": "error"}` (401/403/invalid-scope/429/5xx/transport/malformed)

**`zoominfo_client.enrich_company(company_id: int) -> dict`** — returns:
- `{"status": "ok", "company": <raw firmographics dict>}`
- `{"status": "empty"}`
- `{"status": "error"}`

**CLI-normalized `resolution`** (passed into the pure module):
- pre-curated id: `{"company_id": <int>, "match_basis": "precurated"}`
- resolved: `{"company_id": <int>, "match_basis": "domain"|"name_hq"|"name"}`
- no id found: `{"match_basis": None}`
- endpoint error: `{"error": True}`

**CLI-normalized `enrichment`** = the dict returned by `enrich_company`, or `None` when no id was available to enrich.

### Conventions (apply to every task)

- **YAML:** use `yaml.safe_load` and `yaml.safe_dump` **only** — never `yaml.load`/`yaml.dump`. Dump with `sort_keys=False, allow_unicode=True, default_flow_style=False`.
- **Logging:** `%s` placeholders, never f-strings in `logger.*()` calls; specific exceptions only (matches the repo's Python conventions).
- **Type hints** on all new function signatures; `Optional[T]` for nullable returns.

---

## Endpoint verification (DONE — read before Task 1)

The official ZoomInfo GTM API docs were checked on 2026-06-14:

- **Company Enrich:** `POST https://api.zoominfo.com/gtm/data/v1/companies/enrich`
  (note: **`companies`**, plural) — [docs](https://docs.zoominfo.com/reference/enrichinterface_enrichcompany)
- **Company Search:** `POST https://api.zoominfo.com/gtm/data/v1/companies/search`
  (plural) — [docs](https://docs.zoominfo.com/reference/searchinterface_searchcompany)

**Verified:** method (POST) and full paths. The body is a required JSON:API `data`
object (same family as the existing News Enrich call).

**NOT verified from public docs:** the exact attribute field names under
`data.attributes` (e.g. `companyId`, `companyName`, `companyWebsite`, `outputFields`)
and the response field names. These live in the authenticated "Try It!" schema.

**Implementer obligation (amendment #1 & #3):** the paths/method below are used as
defaults *because they are verified*. Before finalizing the request **body builders**
in Tasks 1–2, confirm the exact `data.attributes` field names (and whether
`outputFields` must be sent to receive revenue/employee/industry/country/state)
against the ZoomInfo API reference or "Try It!" explorer, and adjust the body
builders accordingly. **Do not infer body shape from News Enrich alone.** The tests
in these tasks deliberately assert only *wrapper behavior + response normalization*
(not provider-specific field names), so they remain valid whatever the confirmed
attribute names turn out to be. The defensive response-key lists
(`_COMPANY_ID_KEYS`, the firmographics key tuples in Task 5) should be widened to
include the confirmed names once known. Endpoint paths stay env-overridable
(`ZOOMINFO_ENRICH_ENDPOINT` / `ZOOMINFO_SEARCH_ENDPOINT`) so a doc correction never
needs a code change.

---

## Task 1: Client endpoint helpers + `resolve_company`

**Files:**
- Modify: `zoominfo_client.py` (add constants near line 39; add functions after `_resolve_access_token` at line 172)
- Test: `tests/test_zoominfo_company.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_zoominfo_company.py`:

```python
"""Tests for the ZoomInfo company resolve/enrich functions used by the
target-metadata enrichment utility. No live API calls — requests.post is
always mocked."""
from unittest.mock import MagicMock, patch

import pytest
import requests

import zoominfo_client


def _ok(json_payload: dict) -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = json_payload
    resp.raise_for_status = MagicMock()
    return resp


def _err(status: int) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=resp)
    return resp


@pytest.fixture(autouse=True)
def _bearer(monkeypatch):
    # Static bearer path keeps these tests off the OAuth exchange.
    zoominfo_client._reset_token_cache()
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-bearer")
    monkeypatch.delenv("ZOOMINFO_CLIENT_ID", raising=False)
    monkeypatch.delenv("ZOOMINFO_CLIENT_SECRET", raising=False)
    yield
    zoominfo_client._reset_token_cache()


def test_resolve_company_returns_id_on_match():
    payload = {"data": [{"id": 357374413, "name": "Avient Corporation"}]}
    with patch("zoominfo_client.requests.post", return_value=_ok(payload)):
        result = zoominfo_client.resolve_company(domain="avient.com")
    assert result == {"status": "ok", "company_id": 357374413}


def test_resolve_company_empty_when_no_candidates():
    with patch("zoominfo_client.requests.post", return_value=_ok({"data": []})):
        result = zoominfo_client.resolve_company(name="No Such Co")
    assert result == {"status": "empty"}


def test_resolve_company_error_on_403():
    with patch("zoominfo_client.requests.post", return_value=_err(403)):
        result = zoominfo_client.resolve_company(name="Avient")
    assert result == {"status": "error"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_zoominfo_company.py -v`
Expected: FAIL with `AttributeError: module 'zoominfo_client' has no attribute 'resolve_company'`

- [ ] **Step 3: Add endpoint constants**

In `zoominfo_client.py`, after line 39 (`_DEFAULT_TOKEN_URL = ...`):

```python
# Verified against ZoomInfo GTM API docs 2026-06-14 (POST, "companies" plural).
# Override-able so a doc correction never needs a code change.
_DEFAULT_ENRICH_ENDPOINT = "https://api.zoominfo.com/gtm/data/v1/companies/enrich"
_DEFAULT_SEARCH_ENDPOINT = "https://api.zoominfo.com/gtm/data/v1/companies/search"
```

And after the existing `_token_url()` (line 81):

```python
def _enrich_endpoint() -> str:
    return os.environ.get("ZOOMINFO_ENRICH_ENDPOINT", "").strip() or _DEFAULT_ENRICH_ENDPOINT


def _search_endpoint() -> str:
    return os.environ.get("ZOOMINFO_SEARCH_ENDPOINT", "").strip() or _DEFAULT_SEARCH_ENDPOINT


# Candidate keys ZoomInfo may use for a company id / name in search + enrich bodies.
_COMPANY_ID_KEYS = ("id", "companyId", "zoominfoCompanyId", "company_id")
_COMPANY_NAME_KEYS = ("name", "companyName", "canonicalName")
# Candidate keys the company list may live under in a search response envelope.
_COMPANY_LIST_KEYS = ("data", "results", "result", "companies", "items")
```

- [ ] **Step 4: Implement `resolve_company` + a shared status-classifier**

After `_resolve_access_token()` (line 172) in `zoominfo_client.py`:

```python
def _classify_http_error(exc: requests.exceptions.HTTPError, context: str) -> str:
    """Map an HTTPError to the 'error' sentinel, logging per status. Returns
    the literal "error" so callers can `return {"status": _classify...}`."""
    status = getattr(exc.response, "status_code", None)
    if status in (401, 403):
        logger.error(
            "ZoomInfo auth/scope error (%s) for %s — entitlement is per-endpoint; "
            "this proves nothing about other endpoints", status, context,
        )
    elif status == 429:
        logger.warning("ZoomInfo rate limited (429) for %s — skipping", context)
    elif status is not None and 500 <= status < 600:
        logger.warning("ZoomInfo server error (%s) for %s — skipping", status, context)
    else:
        logger.error("ZoomInfo HTTP error (%s) for %s", status, context)
    return "error"


def _company_search_body(*, domain, name, hq_country, hq_state) -> dict:
    """Assemble a Company Search request body from whichever hints are given.

    NOTE (verification obligation): the JSON:API `data` wrapper + POST method are
    doc-verified, but the attribute names below (companyWebsite/companyName/
    country/state) and the resource `type` are NOT yet confirmed against the
    ZoomInfo API reference. Confirm and adjust before relying on live results.
    """
    criteria: dict = {}
    if domain:
        criteria["companyWebsite"] = domain
    if name:
        criteria["companyName"] = name
    if hq_country:
        criteria["country"] = hq_country
    if hq_state:
        criteria["state"] = hq_state
    return {"data": {"type": "CompanySearch", "attributes": criteria}}


def _extract_company_list(payload: object) -> list:
    """Locate the list of company dicts inside a search response envelope."""
    if isinstance(payload, list):
        return [c for c in payload if isinstance(c, dict)]
    if not isinstance(payload, dict):
        return []
    for key in _COMPANY_LIST_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            return [c for c in value if isinstance(c, dict)]
    return []


def _first_company_id(company: dict) -> Optional[int]:
    """Return the first integer-coercible company id among known keys."""
    attrs = company.get("attributes") if isinstance(company.get("attributes"), dict) else company
    for source in (company, attrs):
        for key in _COMPANY_ID_KEYS:
            value = source.get(key)
            if isinstance(value, bool):
                continue
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.strip().isdigit():
                return int(value.strip())
    return None


def resolve_company(*, domain=None, name=None, hq_country=None, hq_state=None) -> dict:
    """Resolve a ZoomInfo company id from a single set of hints.

    Returns {"status": "ok", "company_id": int} on a match, {"status": "empty"}
    when the API returns no candidate, or {"status": "error"} on any auth/scope/
    transport/malformed failure. Never raises.
    """
    token = _resolve_access_token()
    if not token:
        return {"status": "error"}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = _company_search_body(
        domain=domain, name=name, hq_country=hq_country, hq_state=hq_state
    )
    context = f"resolve(domain={domain!r}, name={name!r})"
    try:
        response = requests.post(
            _search_endpoint(), json=body, headers=headers, timeout=_REQUEST_TIMEOUT
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        return {"status": _classify_http_error(exc, context)}
    except requests.exceptions.RequestException as exc:
        logger.error("ZoomInfo search request failed for %s: %s", context, exc)
        return {"status": "error"}
    try:
        data = response.json()
    except ValueError as exc:
        logger.error("ZoomInfo search non-JSON body for %s: %s", context, exc)
        return {"status": "error"}
    companies = _extract_company_list(data)
    for company in companies:
        company_id = _first_company_id(company)
        if company_id is not None:
            return {"status": "ok", "company_id": company_id}
    return {"status": "empty"}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_zoominfo_company.py -v`
Expected: PASS (3 passed)

- [ ] **Step 6: Commit**

```bash
git add zoominfo_client.py tests/test_zoominfo_company.py
git commit -m "feat(zoominfo): add resolve_company for target enrichment"
```

---

## Task 2: Client `enrich_company`

**Files:**
- Modify: `zoominfo_client.py` (after `resolve_company`)
- Test: `tests/test_zoominfo_company.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_zoominfo_company.py`:

```python
def test_enrich_company_returns_raw_company_on_success():
    payload = {"data": [{"attributes": {"name": "Avient Corporation",
                                        "revenueRange": "$1B - $5B"}}]}
    with patch("zoominfo_client.requests.post", return_value=_ok(payload)):
        result = zoominfo_client.enrich_company(357374413)
    assert result["status"] == "ok"
    assert result["company"]["name"] == "Avient Corporation"
    assert result["company"]["revenueRange"] == "$1B - $5B"


def test_enrich_company_empty_when_no_company():
    with patch("zoominfo_client.requests.post", return_value=_ok({"data": []})):
        result = zoominfo_client.enrich_company(999)
    assert result == {"status": "empty"}


def test_enrich_company_error_on_403():
    with patch("zoominfo_client.requests.post", return_value=_err(403)):
        result = zoominfo_client.enrich_company(357374413)
    assert result == {"status": "error"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_zoominfo_company.py -k enrich_company -v`
Expected: FAIL with `AttributeError: ... has no attribute 'enrich_company'`

- [ ] **Step 3: Implement `enrich_company`**

After `resolve_company` in `zoominfo_client.py`:

```python
def enrich_company(company_id: int) -> dict:
    """Fetch firmographics for one ZoomInfo company id.

    Returns {"status": "ok", "company": <raw dict>} on success, {"status":
    "empty"} when no company is returned, or {"status": "error"} on any failure.
    The raw company dict is handed back unmapped — field mapping is the pure
    target_enricher module's job. Never raises.
    """
    token = _resolve_access_token()
    if not token:
        return {"status": "error"}
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # NOTE (verification obligation): POST + path are doc-verified; the `type`,
    # `companyId` attribute name, and whether an `outputFields` list must be sent
    # to receive revenue/employee/industry/country/state are NOT yet confirmed.
    # Confirm against the ZoomInfo API reference and add outputFields if required,
    # else enrich may return only minimal fields.
    body = {"data": {"type": "CompanyEnrich",
                     "attributes": {"companyId": company_id}}}
    context = f"enrich(company_id={company_id})"
    try:
        response = requests.post(
            _enrich_endpoint(), json=body, headers=headers, timeout=_REQUEST_TIMEOUT
        )
        response.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        return {"status": _classify_http_error(exc, context)}
    except requests.exceptions.RequestException as exc:
        logger.error("ZoomInfo enrich request failed for %s: %s", context, exc)
        return {"status": "error"}
    try:
        data = response.json()
    except ValueError as exc:
        logger.error("ZoomInfo enrich non-JSON body for %s: %s", context, exc)
        return {"status": "error"}
    companies = _extract_company_list(data)
    if not companies:
        return {"status": "empty"}
    company = companies[0]
    attrs = company.get("attributes")
    return {"status": "ok", "company": attrs if isinstance(attrs, dict) else company}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_zoominfo_company.py -v`
Expected: PASS (6 passed)

- [ ] **Step 5: Commit**

```bash
git add zoominfo_client.py tests/test_zoominfo_company.py
git commit -m "feat(zoominfo): add enrich_company for target enrichment"
```

---

## Task 3: Pure module — de-suffix guardrail + identity terms

**Files:**
- Create: `target_enricher.py`
- Test: `tests/test_target_enricher.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_target_enricher.py`:

```python
"""Tests for the pure target_enricher transform module. No network, no files."""
import target_enricher as te


def test_de_suffix_keeps_long_remainder():
    assert te.de_suffix("Avient Corporation") == "Avient"


def test_de_suffix_suppresses_short_acronym_remainder():
    # Stripping "Company" leaves "RTP" (3 chars, 1 token) -> suppressed.
    assert te.de_suffix("RTP Company") is None
    # Stripping "SE" leaves "BASF" (4 chars, 1 token) -> suppressed.
    assert te.de_suffix("BASF SE") is None


def test_de_suffix_none_when_no_legal_suffix():
    assert te.de_suffix("Avient") is None


def test_identity_terms_dedup_and_order():
    terms = te.build_identity_terms("Avient Corporation", "Avient")
    assert terms == ["Avient Corporation", "Avient"]


def test_identity_terms_rtp_has_no_bare_acronym():
    terms = te.build_identity_terms("RTP Company", "RTP Company")
    assert terms == ["RTP Company"]
    assert "RTP" not in terms


def test_identity_terms_keeps_literal_target_name_even_if_short():
    # "BASF" is the curated targets.yaml name, not an auto-derived acronym.
    terms = te.build_identity_terms("BASF SE", "BASF")
    assert terms == ["BASF SE", "BASF"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_target_enricher.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'target_enricher'`

- [ ] **Step 3: Implement de-suffix + identity terms**

Create `target_enricher.py`:

```python
"""Pure transform module for the target-metadata enrichment utility.

Takes raw ZoomInfo responses plus prior metadata and returns proposed metadata
records (status, confidence, conservative helper terms). Performs ZERO network
and ZERO file I/O — every function is a deterministic transform, fully unit
-testable without mocks. The clock lives in the CLI: callers stamp
``zoominfo_metadata_last_refreshed`` after calling this module.
"""
from __future__ import annotations

from typing import Optional

# Legal-entity suffixes stripped to derive a de-suffixed identity term.
LEGAL_SUFFIXES = [
    "Incorporated", "Inc.", "Inc", "Corporation", "Corp.", "Corp",
    "LLC", "L.L.C.", "Ltd.", "Ltd", "Limited", "GmbH", "S.E.", "SE",
    "AG", "Co.", "Co", "Company", "Group", "Holdings", "PLC", "plc",
]
_SUFFIX_SET = {s.strip(".,").lower() for s in LEGAL_SUFFIXES}


def de_suffix(name: str) -> Optional[str]:
    """Strip a single trailing legal suffix, with a guardrail.

    Returns the de-suffixed form ONLY if it retains >=2 word tokens OR >=6
    characters — otherwise None. This prevents reducing a name to a short,
    overloaded acronym (e.g. "RTP Company" -> "RTP" is suppressed).
    """
    tokens = (name or "").split()
    if len(tokens) < 2:
        return None
    if tokens[-1].strip(".,").lower() not in _SUFFIX_SET:
        return None
    candidate = " ".join(tokens[:-1]).strip()
    if len(candidate.split()) >= 2 or len(candidate) >= 6:
        return candidate
    return None


def build_identity_terms(canonical_name: str, target_name: str) -> list[str]:
    """Conservative identity terms: canonical name, target name, and de-suffixed
    forms of each. Case-insensitive dedup, canonical-first order. No acronyms."""
    terms: list[str] = []
    seen: set[str] = set()

    def _add(term: Optional[str]) -> None:
        term = (term or "").strip()
        if term and term.lower() not in seen:
            seen.add(term.lower())
            terms.append(term)

    _add(canonical_name)
    _add(target_name)
    _add(de_suffix(canonical_name or ""))
    _add(de_suffix(target_name or ""))
    return terms
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_target_enricher.py -v`
Expected: PASS (6 passed)

- [ ] **Step 5: Commit**

```bash
git add target_enricher.py tests/test_target_enricher.py
git commit -m "feat(enricher): de-suffix guardrail and conservative identity terms"
```

---

## Task 4: Pure module — industry term mapping

**Files:**
- Modify: `target_enricher.py`
- Test: `tests/test_target_enricher.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_target_enricher.py`:

```python
def test_industry_terms_mapped_primary():
    terms, unmapped = te.build_industry_terms("Plastics & Rubber Manufacturing", [])
    assert terms == ["plastics", "polymer", "resin"]
    assert unmapped is False


def test_industry_terms_merges_primary_and_industries_without_dups():
    terms, unmapped = te.build_industry_terms(
        "Plastics & Rubber Manufacturing",
        ["Chemicals Manufacturing", "Plastics & Rubber Manufacturing"],
    )
    assert terms == ["plastics", "polymer", "resin", "chemicals", "specialty chemicals"]
    assert unmapped is False


def test_industry_terms_unmapped_emits_nothing_and_flags():
    terms, unmapped = te.build_industry_terms("Underwater Basket Weaving", [])
    assert terms == []
    assert unmapped is True


def test_industry_terms_empty_input_not_flagged_unmapped():
    terms, unmapped = te.build_industry_terms("", [])
    assert terms == []
    assert unmapped is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_target_enricher.py -k industry -v`
Expected: FAIL with `AttributeError: module 'target_enricher' has no attribute 'build_industry_terms'`

- [ ] **Step 3: Implement the mapping**

Append to `target_enricher.py`:

```python
# Small, checked-in map from ZoomInfo industry labels to curated relevance
# terms. Only mapped industries emit terms; unmapped ones emit nothing and set
# industry_unmapped=True so a human can extend this map. Add entries at the end.
INDUSTRY_TERM_MAP = {
    "Plastics & Rubber Manufacturing":          ["plastics", "polymer", "resin"],
    "Chemicals Manufacturing":                  ["chemicals", "specialty chemicals"],
    "Plastics Material & Resin Manufacturing":  ["resin", "thermoplastics", "compounding"],
    "Packaging & Containers":                   ["packaging"],
    "Automotive":                               ["automotive", "mobility"],
    "Building Materials":                       ["building materials", "construction"],
    "Paints, Coatings & Adhesives":             ["coatings", "pigments", "masterbatch"],
    "Textiles & Apparel":                       ["fibers", "textiles"],
}


def build_industry_terms(primary_industry: str, industries: list) -> tuple[list[str], bool]:
    """Map ZoomInfo industries to curated relevance terms.

    Returns (terms, unmapped). `unmapped` is True only when there was at least
    one non-empty industry input and NONE of them matched the map.
    """
    sources: list[str] = []
    for value in [primary_industry, *(industries or [])]:
        value = (value or "").strip()
        if value and value not in sources:
            sources.append(value)

    terms: list[str] = []
    matched_any = False
    for source in sources:
        mapped = INDUSTRY_TERM_MAP.get(source)
        if mapped:
            matched_any = True
            for term in mapped:
                if term not in terms:
                    terms.append(term)

    unmapped = bool(sources) and not matched_any
    return terms, unmapped
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_target_enricher.py -k industry -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add target_enricher.py tests/test_target_enricher.py
git commit -m "feat(enricher): industry relevance term mapping"
```

---

## Task 5: Pure module — firmographics extraction

**Files:**
- Modify: `target_enricher.py`
- Test: `tests/test_target_enricher.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_target_enricher.py`:

```python
def test_extract_firmographics_maps_known_keys():
    raw = {
        "name": "Avient Corporation",
        "revenueRange": "$1B - $5B",
        "employeeCount": 9000,
        "primaryIndustry": "Plastics & Rubber Manufacturing",
        "industries": ["Plastics & Rubber Manufacturing", "Chemicals Manufacturing"],
        "country": "United States",
        "state": "Ohio",
    }
    firmo = te.extract_firmographics(raw)
    assert firmo == {
        "canonical_name": "Avient Corporation",
        "hq_revenue_range": "$1B - $5B",
        "employee_range": "9000",
        "primary_industry": "Plastics & Rubber Manufacturing",
        "industries": ["Plastics & Rubber Manufacturing", "Chemicals Manufacturing"],
        "hq_country": "United States",
        "hq_state": "Ohio",
    }


def test_extract_firmographics_missing_keys_default_empty():
    firmo = te.extract_firmographics({})
    assert firmo == {
        "canonical_name": "", "hq_revenue_range": "", "employee_range": "",
        "primary_industry": "", "industries": [], "hq_country": "", "hq_state": "",
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_target_enricher.py -k firmographics -v`
Expected: FAIL with `AttributeError: ... has no attribute 'extract_firmographics'`

- [ ] **Step 3: Implement extraction**

Append to `target_enricher.py`:

```python
_CANONICAL_NAME_KEYS = ("name", "companyName", "canonicalName")
_REVENUE_KEYS = ("revenueRange", "revenue", "annualRevenueRange", "revRange")
_EMPLOYEE_KEYS = ("employeeRange", "employeeCount", "employeesRange", "numberOfEmployees")
_PRIMARY_INDUSTRY_KEYS = ("primaryIndustry", "primaryIndustryName", "industry")
_INDUSTRIES_KEYS = ("industries", "subIndustries", "industryList")
_COUNTRY_KEYS = ("country", "companyCountry", "hqCountry", "countryName")
_STATE_KEYS = ("state", "companyState", "hqState", "stateName")


def _first_value(item: dict, keys: tuple) -> str:
    """First non-empty string/number among keys, coerced to a stripped string."""
    for key in keys:
        value = item.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float)):
            return str(value)
    return ""


def _list_value(item: dict, keys: tuple) -> list[str]:
    """First list/string among keys, normalised to a list of non-empty strings."""
    for key in keys:
        value = item.get(key)
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        if isinstance(value, str) and value.strip():
            return [value.strip()]
    return []


def extract_firmographics(raw: dict) -> dict:
    """Map a raw ZoomInfo company dict to our schema fields. Missing fields
    default to "" (or [] for industries). Pure — defensive about key variants."""
    raw = raw if isinstance(raw, dict) else {}
    return {
        "canonical_name": _first_value(raw, _CANONICAL_NAME_KEYS),
        "hq_revenue_range": _first_value(raw, _REVENUE_KEYS),
        "employee_range": _first_value(raw, _EMPLOYEE_KEYS),
        "primary_industry": _first_value(raw, _PRIMARY_INDUSTRY_KEYS),
        "industries": _list_value(raw, _INDUSTRIES_KEYS),
        "hq_country": _first_value(raw, _COUNTRY_KEYS),
        "hq_state": _first_value(raw, _STATE_KEYS),
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_target_enricher.py -k firmographics -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add target_enricher.py tests/test_target_enricher.py
git commit -m "feat(enricher): firmographics extraction from raw ZoomInfo company"
```

---

## Task 6: Pure module — `build_proposed_metadata` (status/confidence matrix)

**Files:**
- Modify: `target_enricher.py`
- Test: `tests/test_target_enricher.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_target_enricher.py`:

```python
_ENRICH_OK = {"status": "ok", "company": {
    "name": "Avient Corporation", "revenueRange": "$1B - $5B",
    "employeeCount": 9000, "primaryIndustry": "Plastics & Rubber Manufacturing",
    "industries": ["Plastics & Rubber Manufacturing"], "country": "United States",
    "state": "Ohio",
}}


def test_precurated_id_plus_enrich_is_verified_high():
    rec = te.build_proposed_metadata(
        target_key="Avient", target_name="Avient",
        prior_record=None,
        resolution={"company_id": 357374413, "match_basis": "precurated"},
        enrichment=_ENRICH_OK,
    )
    assert rec["zoominfo_metadata_status"] == "verified"
    assert rec["zoominfo_metadata_confidence"] == "high"
    assert rec["zoominfo_company_id"] == 357374413
    assert rec["canonical_name"] == "Avient Corporation"
    assert rec["company_identity_terms"] == ["Avient Corporation", "Avient"]
    assert rec["industry_relevance_terms"] == ["plastics", "polymer", "resin"]
    assert rec["metadata_record_status"] == "active"
    assert rec["target_key"] == "Avient"


def test_domain_resolution_is_verified_high():
    rec = te.build_proposed_metadata(
        target_key="Avient", target_name="Avient", prior_record=None,
        resolution={"company_id": 1, "match_basis": "domain"}, enrichment=_ENRICH_OK,
    )
    assert rec["zoominfo_metadata_status"] == "verified"
    assert rec["zoominfo_metadata_confidence"] == "high"


def test_name_hq_resolution_is_needs_review_medium():
    rec = te.build_proposed_metadata(
        target_key="Avient", target_name="Avient", prior_record=None,
        resolution={"company_id": 1, "match_basis": "name_hq"}, enrichment=_ENRICH_OK,
    )
    assert rec["zoominfo_metadata_status"] == "needs_review"
    assert rec["zoominfo_metadata_confidence"] == "medium"


def test_name_only_resolution_is_needs_review_low():
    rec = te.build_proposed_metadata(
        target_key="Avient", target_name="Avient", prior_record=None,
        resolution={"company_id": 1, "match_basis": "name"}, enrichment=_ENRICH_OK,
    )
    assert rec["zoominfo_metadata_status"] == "needs_review"
    assert rec["zoominfo_metadata_confidence"] == "low"


def test_no_id_found_is_missing():
    rec = te.build_proposed_metadata(
        target_key="Ghost Co", target_name="Ghost Co", prior_record=None,
        resolution={"match_basis": None}, enrichment=None,
    )
    assert rec["zoominfo_metadata_status"] == "missing"
    assert rec["zoominfo_company_id"] is None


def test_enrich_empty_with_id_is_missing():
    rec = te.build_proposed_metadata(
        target_key="Avient", target_name="Avient", prior_record=None,
        resolution={"company_id": 5, "match_basis": "precurated"},
        enrichment={"status": "empty"},
    )
    assert rec["zoominfo_metadata_status"] == "missing"
    assert rec["zoominfo_company_id"] == 5


def test_error_preserves_prior_machine_block():
    prior = {
        "target_key": "Avient", "zoominfo_company_id": 357374413,
        "canonical_name": "Avient Corporation", "hq_revenue_range": "$1B - $5B",
        "zoominfo_metadata_status": "verified", "zoominfo_metadata_confidence": "high",
        "manual_aliases": ["AVNT"], "exclude_terms": ["avient health"],
    }
    rec = te.build_proposed_metadata(
        target_key="Avient", target_name="Avient", prior_record=prior,
        resolution={"error": True}, enrichment=None,
    )
    assert rec["zoominfo_metadata_status"] == "error"
    # Prior good data survives untouched.
    assert rec["canonical_name"] == "Avient Corporation"
    assert rec["zoominfo_company_id"] == 357374413
    assert rec["zoominfo_metadata_confidence"] == "high"
    # Curated fields survive.
    assert rec["manual_aliases"] == ["AVNT"]
    assert rec["exclude_terms"] == ["avient health"]


def test_curated_fields_preserved_on_success():
    prior = {"manual_aliases": ["RTP"], "exclude_terms": ["return to player"]}
    rec = te.build_proposed_metadata(
        target_key="RTP Company", target_name="RTP Company", prior_record=prior,
        resolution={"company_id": 46383930, "match_basis": "precurated"},
        enrichment=_ENRICH_OK,
    )
    assert rec["manual_aliases"] == ["RTP"]
    assert rec["exclude_terms"] == ["return to player"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_target_enricher.py -k "proposed or precurated or resolution or missing or error or curated" -v`
Expected: FAIL with `AttributeError: ... has no attribute 'build_proposed_metadata'`

- [ ] **Step 3: Implement `build_proposed_metadata`**

Append to `target_enricher.py`:

```python
_CONFIDENCE_BY_BASIS = {
    "precurated": "high", "domain": "high", "name_hq": "medium", "name": "low",
}


def build_proposed_metadata(*, target_key: str, target_name: str,
                            prior_record: Optional[dict],
                            resolution: dict, enrichment: Optional[dict]) -> dict:
    """Build the proposed metadata record for one target.

    `resolution` is the CLI-normalized dict ({"company_id","match_basis"} |
    {"match_basis": None} | {"error": True}). `enrichment` is the enrich_company
    result dict or None. The CLI stamps zoominfo_metadata_last_refreshed after.
    """
    prior = prior_record or {}
    manual_aliases = list(prior.get("manual_aliases", []))
    exclude_terms = list(prior.get("exclude_terms", []))

    # ERROR: keep the prior machine block verbatim; only re-stamp status. A
    # transient entitlement blip must never wipe verified data.
    if resolution.get("error") or (enrichment or {}).get("status") == "error":
        record = dict(prior)
        record["target_key"] = target_key
        record["metadata_record_status"] = "active"
        record["zoominfo_metadata_status"] = "error"
        record.setdefault("zoominfo_metadata_confidence", "low")
        record["manual_aliases"] = manual_aliases
        record["exclude_terms"] = exclude_terms
        return record

    company_id = resolution.get("company_id")
    match_basis = resolution.get("match_basis")
    confidence = _CONFIDENCE_BY_BASIS.get(match_basis, "low")

    firmo = {}
    if (enrichment or {}).get("status") == "ok":
        firmo = extract_firmographics(enrichment.get("company", {}))

    if company_id is None or not firmo:
        status = "missing"
    elif match_basis in ("precurated", "domain"):
        status = "verified"
    else:
        status = "needs_review"

    identity_terms = build_identity_terms(firmo.get("canonical_name", ""), target_name)
    industry_terms, unmapped = build_industry_terms(
        firmo.get("primary_industry", ""), firmo.get("industries", [])
    )

    return {
        "target_key": target_key,
        "metadata_record_status": "active",
        "zoominfo_company_id": company_id,
        "canonical_name": firmo.get("canonical_name", ""),
        "hq_revenue_range": firmo.get("hq_revenue_range", ""),
        "employee_range": firmo.get("employee_range", ""),
        "primary_industry": firmo.get("primary_industry", ""),
        "industries": firmo.get("industries", []),
        "hq_country": firmo.get("hq_country", ""),
        "hq_state": firmo.get("hq_state", ""),
        "company_identity_terms": identity_terms,
        "industry_relevance_terms": industry_terms,
        "industry_unmapped": unmapped,
        "zoominfo_metadata_status": status,
        "zoominfo_metadata_confidence": confidence,
        "manual_aliases": manual_aliases,
        "exclude_terms": exclude_terms,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_target_enricher.py -v`
Expected: PASS (all target_enricher tests green)

- [ ] **Step 5: Commit**

```bash
git add target_enricher.py tests/test_target_enricher.py
git commit -m "feat(enricher): build_proposed_metadata status/confidence matrix"
```

---

## Task 7: Pure module — `merge_targets` (orphan flagging)

**Files:**
- Modify: `target_enricher.py`
- Test: `tests/test_target_enricher.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_target_enricher.py`:

```python
def test_merge_marks_removed_record_orphaned():
    prior = {"Old Co": {"target_key": "Old Co", "zoominfo_company_id": 7,
                        "metadata_record_status": "active"}}
    proposed = {"Avient": {"target_key": "Avient", "metadata_record_status": "active"}}
    merged = te.merge_targets(prior, proposed, active_keys={"Avient"})
    assert merged["Avient"]["metadata_record_status"] == "active"
    assert merged["Old Co"]["metadata_record_status"] == "orphaned"
    assert merged["Old Co"]["zoominfo_company_id"] == 7  # kept, not deleted


def test_merge_keeps_unprocessed_active_record_active():
    # In active_keys but not re-processed (e.g. --only) -> stays active, untouched.
    prior = {"SABIC": {"target_key": "SABIC", "metadata_record_status": "active",
                       "zoominfo_company_id": 98664698}}
    proposed = {"Avient": {"target_key": "Avient", "metadata_record_status": "active"}}
    merged = te.merge_targets(prior, proposed, active_keys={"Avient", "SABIC"})
    assert merged["SABIC"]["metadata_record_status"] == "active"
    assert merged["SABIC"]["zoominfo_company_id"] == 98664698


def test_merge_reappearing_target_flips_back_to_active():
    prior = {"Avient": {"target_key": "Avient", "metadata_record_status": "orphaned"}}
    proposed = {"Avient": {"target_key": "Avient", "metadata_record_status": "active"}}
    merged = te.merge_targets(prior, proposed, active_keys={"Avient"})
    assert merged["Avient"]["metadata_record_status"] == "active"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_target_enricher.py -k merge -v`
Expected: FAIL with `AttributeError: ... has no attribute 'merge_targets'`

- [ ] **Step 3: Implement `merge_targets`**

Append to `target_enricher.py`:

```python
def merge_targets(prior_targets: dict, proposed_targets: dict,
                  active_keys: set) -> dict:
    """Merge freshly-built records over prior ones.

    - Freshly processed targets (in `proposed_targets`) win outright.
    - Prior records NOT reprocessed: kept. Marked `orphaned` if their key is no
      longer an active target, else left `active`. Records are never deleted.
    """
    merged = dict(proposed_targets)
    for key, record in (prior_targets or {}).items():
        if key in proposed_targets:
            continue
        record = dict(record)
        record.setdefault("target_key", key)
        record["metadata_record_status"] = "active" if key in active_keys else "orphaned"
        merged[key] = record
    return merged
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_target_enricher.py -k merge -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add target_enricher.py tests/test_target_enricher.py
git commit -m "feat(enricher): merge_targets with orphan flagging"
```

---

## Task 8: CLI scaffold — load, resolve cascade, dry-run diff

**Files:**
- Create: `scripts/enrich_targets.py`
- Test: `tests/test_enrich_targets_cli.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_enrich_targets_cli.py`:

```python
"""Tests for scripts/enrich_targets.py. A fake client replaces ZoomInfo — no
live calls. Exercises load, resolution cascade, dry-run diff, and --write."""
import textwrap

import pytest

import enrich_targets


class _FakeClient:
    """Records calls and returns scripted resolve/enrich results."""
    def __init__(self, resolve_results=None, enrich_result=None):
        self.resolve_results = list(resolve_results or [])
        self.enrich_result = enrich_result or {"status": "ok", "company": {
            "name": "Avient Corporation", "revenueRange": "$1B - $5B",
            "employeeCount": 9000, "primaryIndustry": "Plastics & Rubber Manufacturing",
            "industries": ["Plastics & Rubber Manufacturing"],
            "country": "United States", "state": "Ohio"}}
        self.resolve_calls = []
        self.enrich_calls = []

    def resolve_company(self, **kwargs):
        self.resolve_calls.append(kwargs)
        return self.resolve_results.pop(0) if self.resolve_results else {"status": "empty"}

    def enrich_company(self, company_id):
        self.enrich_calls.append(company_id)
        return self.enrich_result


def _write_targets(tmp_path, body: str):
    p = tmp_path / "targets.yaml"
    p.write_text(textwrap.dedent(body))
    return p


def test_load_targets_returns_active_entities_only(tmp_path):
    targets = _write_targets(tmp_path, """\
        discovery:
          results_per_entity: 2
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
            - name: Paused Co
              active: false
        industry:
          search_mode: concept
          active: true
          include_any: [plastics]
        """)
    loaded = enrich_targets.load_targets_for_enrichment(str(targets))
    assert [t["name"] for t in loaded] == ["Avient"]
    assert loaded[0]["zoominfo_company_id"] == 357374413


def test_dry_run_emits_diff_and_writes_nothing(tmp_path, capsys):
    targets = _write_targets(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
        """)
    out = tmp_path / "target_metadata.yaml"
    client = _FakeClient()
    rc = enrich_targets.run(
        targets_path=str(targets), out_path=str(out),
        only=None, write=False, today="2026-06-14", client=client,
    )
    assert rc == 0
    assert not out.exists()  # dry-run writes nothing
    printed = capsys.readouterr().out
    assert "Avient" in printed
    assert "verified" in printed
    assert client.enrich_calls == [357374413]  # precurated id enriched directly


def test_resolution_cascade_falls_back_to_name_only(tmp_path):
    targets = _write_targets(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
        """)
    out = tmp_path / "target_metadata.yaml"
    # No domain/hq hints in yaml -> single name-only resolve call.
    client = _FakeClient(resolve_results=[{"status": "ok", "company_id": 111}])
    enrich_targets.run(
        targets_path=str(targets), out_path=str(out),
        only=None, write=False, today="2026-06-14", client=client,
    )
    assert client.resolve_calls == [{"domain": None, "name": "Avient",
                                     "hq_country": None, "hq_state": None}]
    assert client.enrich_calls == [111]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_enrich_targets_cli.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'enrich_targets'`

(Note: tests import `enrich_targets`; the file lives at `scripts/enrich_targets.py`. Add a `conftest.py` path entry in Step 3 so `scripts/` is importable.)

- [ ] **Step 3: Make `scripts/` importable in tests**

First check whether the file exists: `ls tests/conftest.py`.

- **If it does NOT exist**, create `tests/conftest.py` with exactly:

```python
import os
import sys

# Make scripts/ importable as top-level modules in tests (e.g. `enrich_targets`).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
```

- **If it ALREADY exists**, do NOT overwrite it. Append only the path shim,
  preserving every existing fixture and import. Add (merging the `import os` /
  `import sys` lines if they are already present, rather than duplicating them):

```python
# Make scripts/ importable as top-level modules in tests (e.g. `enrich_targets`).
import os as _os
import sys as _sys
_sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), "..", "scripts"))
```

- [ ] **Step 4: Implement the CLI core (load + resolve cascade + render + dry-run)**

Create `scripts/enrich_targets.py`:

```python
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_enrich_targets_cli.py -v`
Expected: PASS (3 passed)

- [ ] **Step 6: Commit**

```bash
git add scripts/enrich_targets.py tests/test_enrich_targets_cli.py tests/conftest.py
git commit -m "feat(cli): enrich_targets load + resolve cascade + dry-run diff"
```

---

## Task 9: CLI — `--write`, merge round-trip, `argparse` entrypoint

**Files:**
- Modify: `scripts/enrich_targets.py` (add `main()` + `__main__` guard)
- Test: `tests/test_enrich_targets_cli.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_enrich_targets_cli.py`:

```python
def test_write_creates_file_and_preserves_curated_on_rerun(tmp_path):
    targets = _write_targets(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
        """)
    out = tmp_path / "target_metadata.yaml"

    # First write.
    enrich_targets.run(targets_path=str(targets), out_path=str(out),
                       only=None, write=True, today="2026-06-14", client=_FakeClient())
    assert out.exists()

    # Human curates an alias by hand.
    import yaml
    doc = yaml.safe_load(out.read_text())
    doc["targets"]["Avient"]["manual_aliases"] = ["AVNT"]
    out.write_text(yaml.safe_dump(doc, sort_keys=False))

    # Re-run --write: machine block refreshes, curated alias survives.
    enrich_targets.run(targets_path=str(targets), out_path=str(out),
                       only=None, write=True, today="2026-06-15", client=_FakeClient())
    doc2 = yaml.safe_load(out.read_text())
    rec = doc2["targets"]["Avient"]
    assert rec["manual_aliases"] == ["AVNT"]                 # curated preserved
    assert rec["zoominfo_metadata_last_refreshed"] == "2026-06-15"  # machine refreshed
    assert rec["zoominfo_metadata_status"] == "verified"


def test_orphaned_record_kept_and_flagged_on_write(tmp_path):
    targets_v1 = _write_targets(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
            - name: Old Co
              active: true
              zoominfo_company_id: 222
        """)
    out = tmp_path / "target_metadata.yaml"
    enrich_targets.run(targets_path=str(targets_v1), out_path=str(out),
                       only=None, write=True, today="2026-06-14", client=_FakeClient())

    # Old Co removed from targets.yaml.
    targets_v2 = _write_targets(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
        """)
    enrich_targets.run(targets_path=str(targets_v2), out_path=str(out),
                       only=None, write=True, today="2026-06-15", client=_FakeClient())

    import yaml
    doc = yaml.safe_load(out.read_text())
    assert doc["targets"]["Old Co"]["metadata_record_status"] == "orphaned"
    assert doc["targets"]["Old Co"]["zoominfo_company_id"] == 222  # kept, not deleted
    assert doc["targets"]["Avient"]["metadata_record_status"] == "active"


def test_main_parses_args_and_defaults_to_dry_run(tmp_path, monkeypatch, capsys):
    targets = _write_targets(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
        """)
    out = tmp_path / "target_metadata.yaml"
    monkeypatch.setattr(enrich_targets, "_DefaultClient", _FakeClient)
    rc = enrich_targets.main([
        "--targets", str(targets), "--out", str(out), "--today", "2026-06-14",
    ])
    assert rc == 0
    assert not out.exists()  # default is dry-run
    assert "Avient" in capsys.readouterr().out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_enrich_targets_cli.py -k "write or orphaned or main" -v`
Expected: FAIL — `test_main_*` fails with `AttributeError: module 'enrich_targets' has no attribute 'main'` (the write/orphan tests should already pass from Task 8's `run`; if they fail, fix `run` before proceeding).

- [ ] **Step 3: Add `main()` + entrypoint**

Append to `scripts/enrich_targets.py`:

```python
def main(argv: Optional[list] = None) -> int:
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
        from datetime import date
        today = date.today().isoformat()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    return run(targets_path=args.targets, out_path=args.out, only=args.only,
               write=args.write, today=today, client=_DefaultClient())


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the full suite to verify everything passes**

Run: `pytest tests/test_enrich_targets_cli.py tests/test_target_enricher.py tests/test_zoominfo_company.py -v`
Expected: PASS (all green)

- [ ] **Step 5: Verify the existing suite still passes**

Run: `pytest tests/ -v`
Expected: PASS — no regressions in `test_pipeline.py`, `test_zoominfo.py`, etc.

- [ ] **Step 6: Commit**

```bash
git add scripts/enrich_targets.py tests/test_enrich_targets_cli.py
git commit -m "feat(cli): enrich_targets --write, merge round-trip, argparse entrypoint"
```

---

## Task 10: Documentation — README + `.env.example`

**Files:**
- Modify: `README.md`
- Modify: `.env.example`

- [ ] **Step 1: Add the README section**

Add a new section to `README.md` (place it after the ingestion/delivery overview, before Tests):

```markdown
## Target metadata enrichment (`scripts/enrich_targets.py`)

A standalone, reviewable utility that populates `target_metadata.yaml` — a
machine-managed companion file holding ZoomInfo company-identity metadata
(canonical name, HQ revenue/employee ranges, industries, HQ country/state) plus
conservative helper terms for a future relevance gate.

**Daily ingestion never runs this.** Ingestion consumes the checked-in, reviewed
`target_metadata.yaml` only; enrichment is an offline, human-in-the-loop step.

```bash
# Dry-run (default): prints a unified diff, writes nothing
python scripts/enrich_targets.py

# Apply the proposed changes
python scripts/enrich_targets.py --write

# One target only
python scripts/enrich_targets.py --only "Avient"
```

**Per-endpoint entitlement caveat.** A working OAuth token + News Enrich access
(proved by the ingestion pipeline) does NOT imply access to the Company Enrich
or Company Search endpoints this utility uses — ZoomInfo scopes are granted
per-endpoint. When an endpoint returns 401/403/invalid-scope, the affected target
degrades to `zoominfo_metadata_status: error` (prior good data is preserved) and
the run continues; it never crashes.

**Reviewing output.** Records carry `zoominfo_metadata_status`
(`verified|needs_review|missing|error`) and `zoominfo_metadata_confidence`
(`high|medium|low`). Anything not `verified` warrants a human look before trust.
Edit only the human-curated fields — `manual_aliases` (e.g. risky short acronyms
like `RTP` that the utility deliberately will not auto-generate) and
`exclude_terms`; the enricher preserves them on re-runs. Extend
`INDUSTRY_TERM_MAP` in `target_enricher.py` when a `industry_unmapped: true`
record appears.

Removed targets are kept and flagged `metadata_record_status: orphaned`, never
auto-deleted.
```

- [ ] **Step 2: Add env var docs**

Append to `.env.example`:

```bash
# ZoomInfo endpoint overrides (optional — defaults to published GTM paths).
# Used by scripts/enrich_targets.py; auth reuses ZOOMINFO_CLIENT_ID/SECRET.
ZOOMINFO_ENRICH_ENDPOINT=
ZOOMINFO_SEARCH_ENDPOINT=
```

- [ ] **Step 3: Verify nothing references undefined behavior**

Run: `pytest tests/ -q`
Expected: PASS (full suite green)

- [ ] **Step 4: Commit**

```bash
git add README.md .env.example
git commit -m "docs(enrichment): document enrich_targets usage and entitlement caveat"
```

---

## Final verification

- [ ] **Run the whole suite**

Run: `pytest tests/ -v`
Expected: all green, no regressions.

- [ ] **Smoke the CLI dry-run against the real targets.yaml (no creds required)**

Run: `python scripts/enrich_targets.py --today 2026-06-14`
Expected: with no ZoomInfo creds set, `_resolve_access_token()` returns None, so
both `resolve_company` and `enrich_company` return `{"status": "error"}`. Therefore
**every target degrades to `zoominfo_metadata_status: error`** (logged, not crashed),
and any prior good metadata is preserved. `missing` must **not** appear from absent
credentials — it is reserved for a *successful* lookup/enrich that returned no match
(HTTP 200, empty). The command exits 0, writes nothing, and prints the proposed diff
to stdout.

---

## Self-Review Notes

- **Spec coverage:** client extension reusing OAuth (Tasks 1–2) · pure transform core (Tasks 3–7) · CLI orchestration with dry-run default + explicit `--write` (Tasks 8–9) · companion-file schema with `target_key` + `metadata_record_status` (Tasks 6–9) · status/confidence matrix incl. per-endpoint `error` that preserves prior data (Task 6) · conservative identity terms with de-suffix guardrail, no auto-acronyms (Task 3) · industry mapping with unmapped flag (Task 4) · merge-preserve curated fields + orphan flagging (Tasks 7, 9) · mocked-only tests (all) · README + env docs (Task 10). No ingestion/delivery hot-path changes — confirmed only additive functions touch `zoominfo_client.py`.
- **Type consistency:** `resolve_company`/`enrich_company` return status dicts everywhere; CLI-normalized `resolution`/`enrichment` shapes match `build_proposed_metadata`'s contract; `merge_targets(prior, proposed, active_keys)` signature consistent across Tasks 7 and 9.
- **Single milestone:** one safe, reviewable enrichment utility + companion file. The relevance gate that consumes this is explicitly out of scope.
```
