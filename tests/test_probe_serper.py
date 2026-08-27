"""Tests for scripts/probe_serper.py pure helpers. Dict literals, no HTTP."""
import probe_serper as ps


def _row(query, tbs, count, status=200):
    return {"query": query, "tbs": tbs, "status": status, "count": count, "sample_dates": []}


def test_count_news_reads_serper_news_key():
    body = {"news": [{"link": "a", "date": "2 hours ago"}, {"link": "b"}], "credits": 1}
    assert ps.count_news(body) == 2
    assert ps.count_news({}) == 0
    assert ps.count_news({"organic": [{"link": "x"}]}) == 0


def test_sample_dates_takes_first_three_dates_only():
    body = {"news": [{"date": "1 hour ago"}, {"date": "3 hours ago"}, {"date": "5 hours ago"}, {"date": "1 day ago"}]}
    assert ps.sample_dates(body) == ["1 hour ago", "3 hours ago", "5 hours ago"]
    assert ps.sample_dates({"news": [{"link": "no-date"}]}) == []


def test_verdict_confirms_when_qdr_d_out_returns_qdr_h24_for_a_majority():
    rows = [
        _row("A", "qdr:h24", 0), _row("A", "qdr:d", 6),
        _row("B", "qdr:h24", 1), _row("B", "qdr:d", 8),
        _row("C", "qdr:h24", 4), _row("C", "qdr:d", 4),
    ]
    text = ps.verdict(rows)
    assert text.startswith("CONFIRMED")
    assert "2/3" in text


def test_verdict_not_confirmed_when_windows_agree():
    rows = [
        _row("A", "qdr:h24", 5), _row("A", "qdr:d", 5),
        _row("B", "qdr:h24", 0), _row("B", "qdr:d", 0),
    ]
    text = ps.verdict(rows)
    assert text.startswith("NOT CONFIRMED")
    assert "0/2" in text


def test_verdict_ignores_non_200_rows_and_handles_missing_pairs():
    rows = [
        _row("A", "qdr:h24", 0, status=429), _row("A", "qdr:d", 6),
        _row("B", "qdr:h24", 0),  # no qdr:d twin
    ]
    text = ps.verdict(rows)
    assert text.startswith("INCONCLUSIVE")


def test_format_table_has_one_line_per_row_and_shows_none_filter():
    rows = [_row("Avient", "qdr:h24", 0), _row("Avient", None, 10)]
    out = ps.format_table(rows)
    lines = [l for l in out.splitlines() if "Avient" in l]
    assert len(lines) == 2
    assert "(none)" in out
