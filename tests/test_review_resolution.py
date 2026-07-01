"""Tests for scripts/review_resolution.py flag logic. Pure dicts, no I/O."""
import review_resolution as rr


def _rec(cid, canon, industry="Manufacturing", country="United States"):
    return {
        "zoominfo_company_id": cid,
        "canonical_name": canon,
        "primary_industry": industry,
        "hq_country": country,
        "zoominfo_metadata_confidence": "low",
        "zoominfo_metadata_status": "needs_review",
    }


def test_exact_domain_match_is_not_flagged():
    flag, reason = rr.flag_for("Teknor Apex", 1, "Teknor Apex", "Manufacturing")
    assert flag == "" and reason == ""


def test_short_name_subset_is_not_flagged():
    # '3m' is a length-2 token and must survive tokenisation.
    assert rr.flag_for("3M", 1, "3M Company", "Manufacturing") == ("", "")


def test_name_mismatch_is_flagged():
    flag, reason = rr.flag_for("Cocona", 1, "Coconas", "Media & Internet")
    assert flag == "⚠"
    # both signals fire here: no shared token AND off-domain industry
    assert "name" in reason and "industry" in reason


def test_off_domain_industry_flags_even_when_name_matches():
    # Name shares a token, but a plastics target in Hospitality is wrong.
    flag, reason = rr.flag_for("Huntsman Corporation", 1,
                               "Huntsman Corporation Hungary Zrt", "Hospitality")
    assert flag == "⚠" and reason == "industry"


def test_foreign_country_alone_is_not_flagged():
    # Legit non-US target with a matching name + on-domain industry must not be
    # flagged just for being foreign (country is a look, not an auto-fail).
    assert rr.flag_for("Mitsubishi Chemical", 1, "Mitsubishi Chemical Group",
                       "Manufacturing") == ("", "")


def test_missing_id_is_unresolved():
    assert rr.flag_for("Ferro Pigments", None, "", "") == ("∅", "no id")


def test_resolved_id_with_blank_canonical_is_flagged():
    # An id with no reviewable company name must not read as plausible.
    assert rr.flag_for("Sparse Co", 12345, "", "Manufacturing") == ("⚠", "canonical")


def test_build_rows_sorts_flagged_and_unresolved_first():
    recs = {
        "Good Co": _rec(1, "Good Co"),
        "Unresolved Co": _rec(None, ""),
        "Wrong Co": _rec(2, "Totally Different", industry="Software"),
    }
    order = [r[1] for r in rr.build_rows(recs)]
    # ⚠ first, then ∅, then plausible
    assert order == ["Wrong Co", "Unresolved Co", "Good Co"]
