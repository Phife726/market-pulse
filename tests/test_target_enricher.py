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


def test_extract_firmographics_bool_employee_count_not_coerced():
    firmo = te.extract_firmographics({"employeeCount": True})
    assert firmo["employee_range"] == ""


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


def test_error_path_sets_record_status_active():
    # Error path must keep metadata_record_status="active" so the row isn't
    # orphaned, while marking the ZoomInfo fetch as failed.
    prior = {"target_key": "Avient", "zoominfo_company_id": 357374413,
             "canonical_name": "Avient Corporation",
             "zoominfo_metadata_status": "verified", "zoominfo_metadata_confidence": "high"}
    rec = te.build_proposed_metadata(
        target_key="Avient", target_name="Avient", prior_record=prior,
        resolution={"error": True}, enrichment=None,
    )
    assert rec["metadata_record_status"] == "active"
    assert rec["zoominfo_metadata_status"] == "error"


def test_name_hq_with_empty_enrichment_is_missing_but_medium():
    # company_id present but firmo empty → "missing"; match_basis drives
    # confidence independently, so name_hq still yields "medium".
    rec = te.build_proposed_metadata(
        target_key="Avient", target_name="Avient", prior_record=None,
        resolution={"company_id": 1, "match_basis": "name_hq"},
        enrichment={"status": "empty"},
    )
    assert rec["zoominfo_metadata_status"] == "missing"
    assert rec["zoominfo_metadata_confidence"] == "medium"


def test_precurated_with_none_enrichment_is_missing_but_high():
    # None enrichment leaves firmo empty → "missing", but precurated match_basis
    # still sets confidence to "high" regardless of enrichment outcome.
    rec = te.build_proposed_metadata(
        target_key="Avient", target_name="Avient", prior_record=None,
        resolution={"company_id": 5, "match_basis": "precurated"},
        enrichment=None,
    )
    assert rec["zoominfo_metadata_status"] == "missing"
    assert rec["zoominfo_metadata_confidence"] == "high"


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
