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
