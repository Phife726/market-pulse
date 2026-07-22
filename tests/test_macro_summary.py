"""Unit tests for macro_summary.py — the pure schema/validation + assembly for
the once-per-run Macro summary (the run-level twin of insight.py).

No I/O, no fakes, no patching: these exercise the pure transforms directly.
The generate_macro_summary orchestration (LLM call + upsert) is tested in
tests/test_pipeline.py.
"""
from macro_summary import (
    assemble_macro_content,
    validate_executive_bullets,
    validate_macro_outlook,
)
from prompts import EXEC_BULLET_LABELS


# ---------------------------------------------------------------------------
# validate_macro_outlook
# ---------------------------------------------------------------------------

_MACRO_VALID_IDS = frozenset({1, 2, 3})


def _macro_signal(**over) -> dict:
    sig = {
        "indicator": "Manufacturing PMI",
        "direction": "Declining",
        "americhem_implication": "Downside risk for industrial resin demand.",
        "affected_segments": ["Industrial"],
        "citation_source_ids": [1],
    }
    sig.update(over)
    return sig


def _macro_outlook(**over) -> dict:
    out = {"current_condition": "Manufacturing demand mixed.", "signals": [_macro_signal()]}
    out.update(over)
    return out


def test_validate_macro_outlook_accepts_material_signal():
    result = validate_macro_outlook(_macro_outlook(), _MACRO_VALID_IDS)
    assert result is not None
    assert result["current_condition"] == "Manufacturing demand mixed."
    assert len(result["signals"]) == 1
    assert result["signals"][0]["direction"] == "Declining"
    assert result["signals"][0]["affected_segments"] == ["Industrial"]
    assert result["signals"][0]["citation_source_ids"] == [1]


def test_validate_macro_outlook_empty_signals_is_none():
    assert validate_macro_outlook(_macro_outlook(signals=[]), _MACRO_VALID_IDS) is None


def test_validate_macro_outlook_non_dict_is_none():
    assert validate_macro_outlook(None, _MACRO_VALID_IDS) is None
    assert validate_macro_outlook("nope", _MACRO_VALID_IDS) is None


def test_validate_macro_outlook_blank_current_condition_is_none():
    assert validate_macro_outlook(_macro_outlook(current_condition="  "), _MACRO_VALID_IDS) is None


def test_validate_macro_outlook_drops_signal_without_citation():
    """Materiality gate: an uncitable signal is dropped; a lone uncitable signal
    yields no section."""
    out = _macro_outlook(signals=[_macro_signal(citation_source_ids=[])])
    assert validate_macro_outlook(out, _MACRO_VALID_IDS) is None


def test_validate_macro_outlook_drops_signal_with_only_invalid_citations():
    out = _macro_outlook(signals=[_macro_signal(citation_source_ids=[99])])
    assert validate_macro_outlook(out, _MACRO_VALID_IDS) is None


def test_validate_macro_outlook_rejects_invalid_direction():
    out = _macro_outlook(signals=[_macro_signal(direction="Sideways")])
    assert validate_macro_outlook(out, _MACRO_VALID_IDS) is None


def test_validate_macro_outlook_rejects_invalid_segment():
    out = _macro_outlook(signals=[_macro_signal(affected_segments=["Consumer Goods"])])
    assert validate_macro_outlook(out, _MACRO_VALID_IDS) is None


def test_validate_macro_outlook_accepts_building_construction_segment():
    out = _macro_outlook(signals=[_macro_signal(affected_segments=["Building & Construction"])])
    assert validate_macro_outlook(out, _MACRO_VALID_IDS) is not None


def test_validate_macro_outlook_rejects_blank_fields():
    assert validate_macro_outlook(
        _macro_outlook(signals=[_macro_signal(indicator="  ")]), _MACRO_VALID_IDS) is None
    assert validate_macro_outlook(
        _macro_outlook(signals=[_macro_signal(americhem_implication="")]), _MACRO_VALID_IDS) is None


def test_validate_macro_outlook_keeps_only_valid_signals():
    """A mix of valid + invalid signals keeps only the valid ones."""
    out = _macro_outlook(signals=[
        _macro_signal(indicator="Manufacturing PMI"),
        _macro_signal(direction="Sideways"),                       # bad direction
        _macro_signal(indicator="Construction starts", citation_source_ids=[2]),
    ])
    result = validate_macro_outlook(out, _MACRO_VALID_IDS)
    assert [s["indicator"] for s in result["signals"]] == ["Manufacturing PMI", "Construction starts"]


def test_validate_macro_outlook_truncates_at_cap():
    """The validator keeps at most MAX_MACRO_OUTLOOK_SIGNALS signals, and the
    product cap is 3 (reduced from 6 on 2026-07-17 for report density)."""
    from prompts import MAX_MACRO_OUTLOOK_SIGNALS

    assert MAX_MACRO_OUTLOOK_SIGNALS == 3
    signals = [_macro_signal(indicator=f"Indicator {i}") for i in range(5)]
    result = validate_macro_outlook(_macro_outlook(signals=signals), _MACRO_VALID_IDS)
    assert [s["indicator"] for s in result["signals"]] == [
        "Indicator 0", "Indicator 1", "Indicator 2",
    ]


# ---------------------------------------------------------------------------
# validate_executive_bullets — citation_source_ids cleaning
# ---------------------------------------------------------------------------

def _raw_bullets(a_ids, b_ids, c_ids):
    return [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": a_ids},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": b_ids},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": c_ids},
    ]


def test_validate_bullets_keeps_only_in_pack_ids():
    out = validate_executive_bullets(_raw_bullets([1, 99], [2], []), frozenset({1, 2}))
    assert out[0]["citation_source_ids"] == [1]   # 99 not in pack -> dropped
    assert out[1]["citation_source_ids"] == [2]
    assert out[2]["citation_source_ids"] == []


def test_validate_bullets_dedupes_preserving_order():
    out = validate_executive_bullets(_raw_bullets([2, 1, 2, 1], [], []), frozenset({1, 2}))
    assert out[0]["citation_source_ids"] == [2, 1]


def test_validate_bullets_caps_citations_per_bullet():
    out = validate_executive_bullets(_raw_bullets([1, 2, 3, 4], [], []), frozenset({1, 2, 3, 4}))
    assert out[0]["citation_source_ids"] == [1, 2, 3]   # MAX_EXECUTIVE_BULLET_CITATIONS


def test_validate_bullets_garbage_citations_become_empty():
    raw = [
        {"label": "Market pressure", "body": "A.", "citation_source_ids": "nope"},
        {"label": "Supply chain watch", "body": "B.", "citation_source_ids": [None, "x", True, 1.5]},
        {"label": "Commercial action", "body": "C."},  # key missing entirely
    ]
    out = validate_executive_bullets(raw, frozenset({1, 2}))
    assert out[0]["citation_source_ids"] == []
    assert out[1]["citation_source_ids"] == []   # bool True excluded, non-ints excluded
    assert out[2]["citation_source_ids"] == []


def test_validate_bullets_rejects_wrong_label_order():
    raw = [
        {"label": "Supply chain watch", "body": "A.", "citation_source_ids": []},
        {"label": "Market pressure", "body": "B.", "citation_source_ids": []},
        {"label": "Commercial action", "body": "C.", "citation_source_ids": []},
    ]
    assert validate_executive_bullets(raw, frozenset()) is None


# ---------------------------------------------------------------------------
# assemble_macro_content — the pure per-run transform (raw LLM dict -> the
# storable macro-summary content fields). The LLM call + upsert stay in
# ingestion_engine.generate_macro_summary.
# ---------------------------------------------------------------------------

def _pack(*ids):
    return [{"id": i, "headline": f"H{i}", "url": f"http://e/{i}",
             "domain": "e.com", "segment": "Industrial", "score": 7} for i in ids]


def _bullets(a=(1,), b=(2,), c=()):
    return [
        {"label": EXEC_BULLET_LABELS[0], "body": "Alpha.", "citation_source_ids": list(a)},
        {"label": EXEC_BULLET_LABELS[1], "body": "Beta.", "citation_source_ids": list(b)},
        {"label": EXEC_BULLET_LABELS[2], "body": "Gamma.", "citation_source_ids": list(c)},
    ]


def _outlook(cids=(3,)):
    return {"current_condition": "Mixed.",
            "signals": [_macro_signal(citation_source_ids=list(cids))]}


def _parsed(**over):
    p = {"dominant_condition": "Demand Softness",
         "executive_bullets": _bullets(),
         "macro_outlook": _outlook()}
    p.update(over)
    return p


def test_assemble_happy_path_returns_all_content_fields():
    content = assemble_macro_content(_parsed(), source_pack=_pack(1, 2, 3), article_count=5)
    assert content["dominant_condition"] == "Demand Softness"
    assert content["macro_sentiment"] == "Demand Softness"   # mirrors dominant_condition
    assert [b["label"] for b in content["executive_bullets"]] == list(EXEC_BULLET_LABELS)
    assert content["macro_outlook"]["signals"][0]["indicator"] == "Manufacturing PMI"
    # executive_sources = union of bullet ids (1,2) + signal id (3), in pack order
    assert [s["id"] for s in content["executive_sources"]] == [1, 2, 3]
    assert content["executive_summary"] == (
        "Market pressure: Alpha. Supply chain watch: Beta. Commercial action: Gamma."
    )


def test_assemble_invalid_condition_many_articles_is_mixed_watch():
    content = assemble_macro_content(_parsed(dominant_condition="garbage"),
                                     source_pack=_pack(1, 2, 3), article_count=5)
    assert content["dominant_condition"] == "Mixed / Watch"
    assert content["macro_sentiment"] == "Mixed / Watch"


def test_assemble_invalid_condition_few_articles_is_low_signal_and_overrides_third_bullet():
    content = assemble_macro_content(_parsed(dominant_condition="garbage"),
                                     source_pack=_pack(1, 2, 3), article_count=2)
    assert content["dominant_condition"] == "Low Signal"
    assert content["executive_bullets"][2] == {
        "label": EXEC_BULLET_LABELS[2],
        "body": "No action required.",
        "citation_source_ids": [],
    }


def test_assemble_passthrough_low_signal_overrides_third_bullet():
    """A legitimately-returned 'Low Signal' condition (a valid enum member) also
    forces the third bullet, regardless of article count."""
    content = assemble_macro_content(_parsed(dominant_condition="Low Signal"),
                                     source_pack=_pack(1, 2, 3), article_count=10)
    assert content["executive_bullets"][2]["body"] == "No action required."


def test_assemble_low_signal_with_invalid_bullets_does_not_crash():
    content = assemble_macro_content(
        _parsed(dominant_condition="Low Signal", executive_bullets=["bad"]),
        source_pack=_pack(1, 2, 3), article_count=1)
    assert content["executive_bullets"] is None
    assert content["executive_summary"] == "Macro summary unavailable today."


def test_assemble_executive_sources_is_union_in_pack_order():
    parsed = _parsed(executive_bullets=_bullets(a=(3,), b=(1,), c=()),
                     macro_outlook=_outlook(cids=(1,)))   # bullets cite 3,1; signal cites 1
    content = assemble_macro_content(parsed, source_pack=_pack(1, 2, 3), article_count=5)
    # cited = {1, 3}; pack order 1,2,3 -> [1, 3]; id 2 uncited, id 1 deduped
    assert [s["id"] for s in content["executive_sources"]] == [1, 3]


def test_assemble_derives_valid_ids_from_pack():
    parsed = _parsed(executive_bullets=_bullets(a=(1, 99), b=(), c=()),
                     macro_outlook=None)   # invalid outlook -> None
    content = assemble_macro_content(parsed, source_pack=_pack(1, 2), article_count=5)
    assert content["executive_bullets"][0]["citation_source_ids"] == [1]   # 99 not in pack
    assert content["macro_outlook"] is None
    assert [s["id"] for s in content["executive_sources"]] == [1]


def test_assemble_macro_outlook_none_when_no_material_signal():
    content = assemble_macro_content(
        _parsed(macro_outlook={"current_condition": "x", "signals": []}),
        source_pack=_pack(1, 2, 3), article_count=5)
    assert content["macro_outlook"] is None
