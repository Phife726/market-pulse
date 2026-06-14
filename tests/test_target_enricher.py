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


def test_de_suffix_six_char_single_token_remainder_kept():
    # Exactly 6 chars, single token -> kept (the >=6 char branch).
    assert te.de_suffix("Avient SE") == "Avient"


def test_de_suffix_five_char_single_token_remainder_suppressed():
    # "Xenon" = 5 chars, single token -> suppressed despite a valid suffix.
    assert te.de_suffix("Xenon Co") is None


def test_de_suffix_multiword_remainder_kept_via_token_count():
    # 3-token remainder satisfies the >=2 token branch regardless of length.
    assert te.de_suffix("Avient Specialty Materials Corporation") == "Avient Specialty Materials"


def test_de_suffix_empty_string_returns_none():
    assert te.de_suffix("") is None


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


def test_industry_terms_handles_none_industries():
    terms, unmapped = te.build_industry_terms("Plastics & Rubber Manufacturing", None)
    assert terms == ["plastics", "polymer", "resin"]
    assert unmapped is False


def test_industry_terms_primary_miss_but_list_hit_not_unmapped():
    terms, unmapped = te.build_industry_terms("Underwater Basket Weaving", ["Chemicals Manufacturing"])
    assert terms == ["chemicals", "specialty chemicals"]
    assert unmapped is False


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
