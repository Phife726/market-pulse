"""scripts/backtest_scorer.py — the pure grading pieces and the labeled set's
shape. No LLM: the scoring path itself is the `Scorer Backtest` workflow's job.

Inventory:
- `_grade_band`: the band criterion's formula (issue #109) — `direct` passes
  at >= BAND_DIRECT_MIN, `watch` passes at <= BAND_WATCH_MAX, blank otherwise.
- `majority_result`: the majority-of-N protocol — the run whose score is the
  median wins, failed runs excluded.
- The labeled set: `expected_band` only on `surface` rows, only known values,
  and the input / output headers agree.
"""
import csv
import pathlib

import pytest

import backtest_scorer as bs

_BACKTEST_DIR = pathlib.Path(__file__).resolve().parents[1] / "backtest"
_CSV_IN = _BACKTEST_DIR / "market_pulse_scorer_backtest.csv"
_CSV_OUT = _BACKTEST_DIR / "market_pulse_scorer_backtest_v2.csv"


def _read(path: pathlib.Path) -> tuple[list[str], list[dict]]:
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return list(reader.fieldnames or []), list(reader)


# ---------------------------------------------------------------------------
# _grade_band
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("band, score, expected", [
    ("direct", bs.BAND_DIRECT_MIN, "True"),
    ("direct", bs.BAND_DIRECT_MIN - 1, "False"),
    ("direct", 10, "True"),
    ("watch", bs.BAND_WATCH_MAX, "True"),
    ("watch", bs.BAND_WATCH_MAX + 1, "False"),
    ("watch", 5, "True"),
    ("", 6, ""),
    ("direct", None, ""),
    ("watch", None, ""),
])
def test_grade_band_formula(band, score, expected):
    assert bs._grade_band(band, score) == expected


def test_band_edges_match_the_rubric():
    """DIRECT is 7–8 and WATCH is 6 in RULE 3; the grader's edges are those."""
    assert bs.BAND_DIRECT_MIN == 7
    assert bs.BAND_WATCH_MAX == 6
    assert bs.BAND_DIRECT_MIN == bs.BAND_WATCH_MAX + 1


# ---------------------------------------------------------------------------
# majority_result
# ---------------------------------------------------------------------------

def _res(score, rationale="r"):
    return {"score": score, "rationale": rationale, "so_what": "", "signal_type": "", "discard": False}


def test_majority_result_picks_the_median_run_of_three():
    """Two runs agree at 6, one says 8: the majority (6) wins, and the
    rationale returned is the one that came with that score."""
    picked = bs.majority_result([_res(8, "high"), _res(6, "a"), _res(6, "b")])
    assert picked["score"] == 6
    assert picked["rationale"] == "a"


def test_majority_result_takes_the_middle_when_all_three_differ():
    picked = bs.majority_result([_res(5, "lo"), _res(8, "hi"), _res(7, "mid")])
    assert picked["score"] == 7 and picked["rationale"] == "mid"


def test_majority_result_excludes_failed_runs():
    """A run the seam failed on (score None) does not vote."""
    picked = bs.majority_result([_res(None, "LLM_FAILED"), _res(7, "a"), _res(6, "b")])
    assert picked["score"] == 6  # lower median of [6, 7]: the conservative pick


def test_majority_result_single_run_is_that_run():
    picked = bs.majority_result([_res(9, "only")])
    assert picked["score"] == 9 and picked["rationale"] == "only"


def test_majority_result_all_failed_is_a_failed_result():
    picked = bs.majority_result([_res(None, "LLM_FAILED"), _res(None, "LLM_FAILED")])
    assert picked["score"] is None


# ---------------------------------------------------------------------------
# The labeled set
# ---------------------------------------------------------------------------

def test_labeled_set_band_labels_are_known_values_on_surface_rows_only():
    """`expected_band` refines a `surface` label (a band above the 5 line);
    a `suppress` or `unlabeled` row carries none."""
    _, rows = _read(_CSV_IN)
    assert all(r["expected_band"] in bs.EXPECTED_BANDS for r in rows)
    for r in rows:
        if r["expected_band"]:
            assert r["expected_tier"] == "surface", r["backtest_id"]


def test_labeled_set_has_enough_direct_rows_to_gate_on():
    """The rows RULE 3's DIRECT list names by class (named-target M&A, priced
    input moves, supplier distress, feedstock disruption) are labeled
    `direct`; the issue-#109 gate is a recall over them."""
    _, rows = _read(_CSV_IN)
    direct = {r["backtest_id"] for r in rows if r["expected_band"] == "direct"}
    watch = {r["backtest_id"] for r in rows if r["expected_band"] == "watch"}
    for named in ("BT007", "BT041", "BT099", "BT106", "BT116", "BT147"):
        assert named in direct, named
    assert len(direct) >= 10
    assert len(watch) >= 5
    assert not direct & watch


def test_labeled_set_input_and_output_headers_agree():
    """The runner writes the output with the input's fieldnames; the committed
    _v2 must carry the same columns so a diff of the two is a diff of scores."""
    fields_in, _ = _read(_CSV_IN)
    fields_out, _ = _read(_CSV_OUT)
    assert fields_in == fields_out
    assert "expected_band" in fields_in and "band_pass" in fields_in
