"""Unit tests for the pure ZoomInfo relevance gate. No network, no files
(loader tests use tmp_path)."""
import pytest

from relevance_gate import GateDecision, evaluate, load_target_metadata


RTP_RECORD = {
    "metadata_record_status": "active",
    "canonical_name": "RTP Co",
    "company_identity_terms": ["RTP Co", "RTP Company"],
    "manual_aliases": ["RTP Co", "RTP Company"],
    "exclude_terms": [
        "real-time payments", "RTP Network", "return to player", "casino",
        "slots", "Research Triangle Park", "RTP Global", "Rain Tree Photonics",
    ],
}

AVIENT_RECORD = {
    "metadata_record_status": "active",
    "canonical_name": "Avient",
    "company_identity_terms": ["Avient"],
    "manual_aliases": [],
    "exclude_terms": [],
}


@pytest.mark.parametrize("term", [
    "real-time payments", "RTP Network", "return to player", "casino",
    "slots", "Research Triangle Park", "RTP Global", "Rain Tree Photonics",
])
def test_exclude_term_drops_without_identity_rescue(term):
    d = evaluate(title=f"Breaking: {term} expands", description="", record=RTP_RECORD)
    assert d.drop is True
    assert d.reason == "zoominfo_company_mismatch"
    assert d.matched_exclude == term


def test_identity_rescue_keeps_even_with_exclude_present():
    # "RTP Company" rescues even though "casino" (an exclude term) appears.
    d = evaluate(
        title="RTP Company opens plant near a casino district",
        description="", record=RTP_RECORD,
    )
    assert d.drop is False
    assert d.matched_identity == "RTP Company"


def test_canonical_name_rescues():
    d = evaluate(title="RTP Co wins slots contract", description="", record=RTP_RECORD)
    assert d.drop is False


def test_no_exclude_term_keeps_even_without_identity_text():
    d = evaluate(title="Quarterly polymer market update", description="", record=RTP_RECORD)
    assert d.drop is False
    assert d.matched_exclude is None
    assert d.matched_identity is None


def test_empty_exclude_terms_never_drops():
    d = evaluate(title="Avient casino slots real-time payments", description="",
                 record=AVIENT_RECORD)
    # "Avient" is an identity term, so this is a rescue anyway; assert keep.
    assert d.drop is False


def test_empty_exclude_terms_keeps_with_no_identity_either():
    d = evaluate(title="Totally unrelated casino headline", description="",
                 record=AVIENT_RECORD)
    assert d.drop is False  # no exclude_terms => nothing to drop on


def test_case_insensitive_exclude_match():
    d = evaluate(title="CASINO night downtown", description="", record=RTP_RECORD)
    assert d.drop is True
    assert d.matched_exclude == "casino"


def test_word_boundary_no_partial_false_match():
    # "slots" must not match inside "slotsmachineco"; whole-word required.
    d = evaluate(title="The slotsmachineco product launch", description="",
                 record=RTP_RECORD)
    assert d.drop is False


def test_word_boundary_phrase_matches_as_phrase():
    d = evaluate(title="News from Research Triangle Park today", description="",
                 record=RTP_RECORD)
    assert d.drop is True
    assert d.matched_exclude == "Research Triangle Park"


def test_description_text_is_searched():
    d = evaluate(title="Neutral headline", description="hosted at a casino",
                 record=RTP_RECORD)
    assert d.drop is True
    assert d.matched_exclude == "casino"


def test_gate_decision_is_frozen():
    import dataclasses
    d = GateDecision(drop=False)
    with pytest.raises(dataclasses.FrozenInstanceError):
        d.drop = True  # frozen dataclass


def test_load_target_metadata_reads_targets(tmp_path):
    p = tmp_path / "target_metadata.yaml"
    p.write_text(
        "version: 1\n"
        "targets:\n"
        "  RTP Company:\n"
        "    metadata_record_status: active\n"
        "    canonical_name: RTP Co\n"
    )
    data = load_target_metadata(str(p))
    assert "RTP Company" in data
    assert data["RTP Company"]["canonical_name"] == "RTP Co"


def test_load_target_metadata_missing_file_returns_empty():
    assert load_target_metadata("/nonexistent/target_metadata.yaml") == {}


def test_load_target_metadata_bad_yaml_returns_empty(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("targets: [unbalanced\n")
    assert load_target_metadata(str(p)) == {}


def test_load_target_metadata_no_targets_key_returns_empty(tmp_path):
    p = tmp_path / "empty.yaml"
    p.write_text("version: 1\n")
    assert load_target_metadata(str(p)) == {}


def test_load_target_metadata_list_root_returns_empty(tmp_path):
    # Valid YAML, non-mapping root: must disable the gate, not crash on .get().
    p = tmp_path / "list.yaml"
    p.write_text("- a\n- b\n")
    assert load_target_metadata(str(p)) == {}


def test_load_target_metadata_scalar_root_returns_empty(tmp_path):
    p = tmp_path / "scalar.yaml"
    p.write_text("just a string\n")
    assert load_target_metadata(str(p)) == {}
