"""Unit tests for macro_summary.py — the pure schema/validation + assembly for
the once-per-run Macro summary (the run-level twin of insight.py).

No I/O, no fakes, no patching: these exercise the pure transforms directly.
The generate_macro_summary orchestration (LLM call + upsert) is tested in
tests/test_ingestion_engine.py.
"""
from functools import partial

from tests.conftest import stub_macro_signal, stub_source
from macro_summary import (
    MacroSummary,
    assemble_macro_content,
    validate_executive_bullets,
    validate_macro_outlook,
)
from prompts import EXEC_BULLET_LABELS


# ---------------------------------------------------------------------------
# validate_macro_outlook
# ---------------------------------------------------------------------------

_MACRO_VALID_IDS = frozenset({1, 2, 3})


# The shared valid signal, with this file's implication wording.
_macro_signal = partial(stub_macro_signal, americhem_implication="Downside risk for industrial resin demand.")


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
    return [stub_source(i, f"H{i}", f"http://e/{i}", "e.com") for i in ids]


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


# ---------------------------------------------------------------------------
# MacroSummary — the read face of a stored daily_summaries row
# ---------------------------------------------------------------------------

def _stored_row(**over) -> dict:
    """A content-full stored summary row, as the delivery fetch returns it."""
    row = {
        "executive_summary": "Legacy prose.",
        "macro_sentiment": "Mixed / Watch",
        "dominant_condition": "Softening",
        "executive_bullets": [{"label": "Signal", "text": "t", "citation_source_ids": [1]}],
        "macro_outlook": {"current_condition": "Softening",
                          "signals": [stub_macro_signal(citation_source_ids=[1])]},
        "executive_sources": [stub_source(1)],
        "screened_count": 87,
        "surfaced_count": 6,
        "suppression_breakdown": {"duplicate_url": 3},
        "suppression_samples": [{"reason": "duplicate_url", "url": "u", "title": "t"}],
    }
    row.update(over)
    return row


def test_macro_summary_reads_a_stored_row():
    s = MacroSummary.from_row(_stored_row())
    assert s.legacy_text == "Legacy prose."
    assert s.condition == "Softening"
    assert s.screened_count == 87
    assert s.surfaced_count == 6
    assert s.bullets is not None and len(s.bullets) == 1
    assert s.outlook is not None and len(s.outlook["signals"]) == 1
    assert [src["id"] for src in s.sources] == [1]
    assert s.suppression.breakdown == {"duplicate_url": 3}
    assert len(s.suppression.samples) == 1


def test_macro_summary_condition_prefers_structured_then_falls_back_to_legacy():
    """One rule, one place: dominant_condition, else macro_sentiment, else ''.
    It was spelled twice in renderer.py, 130 lines apart."""
    assert MacroSummary.from_row(_stored_row()).condition == "Softening"
    assert MacroSummary.from_row(
        _stored_row(dominant_condition=None)).condition == "Mixed / Watch"
    assert MacroSummary.from_row(
        _stored_row(dominant_condition=None, macro_sentiment=None)).condition == ""


def test_macro_summary_screened_count_stays_optional():
    """The value types the read; it does not pick a fallback. The report wants a
    derived number (len(rows)); the QA block wants to say the row records none."""
    assert MacroSummary.from_row(_stored_row(screened_count=None)).screened_count is None
    assert MacroSummary.from_row({}).screened_count is None
    assert MacroSummary.from_row({}).surfaced_count is None


def test_macro_summary_has_content_matches_the_accounting_only_row():
    """Content-fullness is the test-mode fallback's first ranking key. An
    accounting-only row (zero-yield run) carries counts but no content."""
    assert MacroSummary.from_row(_stored_row()).has_content is True
    accounting_only = {"screened_count": 40, "suppression_breakdown": {"scrape_failed": 9}}
    assert MacroSummary.from_row(accounting_only).has_content is False
    for field in ("executive_bullets", "executive_summary", "macro_outlook", "dominant_condition"):
        assert MacroSummary.from_row({field: _stored_row()[field]}).has_content is True


def test_macro_summary_legacy_sentiment_alone_is_not_content():
    """macro_sentiment is a tone label, not a brief. `condition` composes the
    two columns for display, but has_content must ask only the structured one —
    otherwise a sentiment-only row outranks a real one in the QA fallback."""
    sentiment_only = MacroSummary.from_row({"macro_sentiment": "Mixed / Watch"})
    assert sentiment_only.condition == "Mixed / Watch"   # still displays
    assert sentiment_only.has_content is False           # but is not a brief


def test_macro_summary_condition_columns_are_kept_apart():
    s = MacroSummary.from_row(_stored_row())
    assert (s.dominant_condition, s.legacy_condition) == ("Softening", "Mixed / Watch")


def test_macro_summary_defends_against_a_ragged_row():
    """Legacy and malformed rows reach the renderer; every reader stays defensive."""
    s = MacroSummary.from_row({
        "executive_bullets": ["not", "dicts"],
        "macro_outlook": {"current_condition": "", "signals": []},
        "executive_sources": None,
        "executive_summary": None,
    })
    assert s.bullets is None
    assert s.outlook is None
    assert s.sources == ()
    assert s.legacy_text == ""
    assert s.condition == ""


def test_macro_summary_from_row_of_none_is_empty_but_constructible():
    s = MacroSummary.from_row(None)
    assert s.has_content is False
    assert s.bullets is None and s.outlook is None and s.sources == ()
    assert s.screened_count is None and s.suppression.breakdown == {}
