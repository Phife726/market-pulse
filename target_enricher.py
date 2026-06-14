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


def build_industry_terms(primary_industry: str, industries: Optional[list[str]]) -> tuple[list[str], bool]:
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
