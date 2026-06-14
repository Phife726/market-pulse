"""Tests for scripts/enrich_targets.py. A fake client replaces ZoomInfo — no
live calls. Exercises load, resolution cascade, dry-run diff, and --write."""
import textwrap

import pytest

import enrich_targets


class _FakeClient:
    """Records calls and returns scripted resolve/enrich results."""
    def __init__(self, resolve_results=None, enrich_result=None):
        self.resolve_results = list(resolve_results or [])
        self.enrich_result = enrich_result or {"status": "ok", "company": {
            "name": "Avient Corporation", "revenueRange": "$1B - $5B",
            "employeeCount": 9000, "primaryIndustry": "Plastics & Rubber Manufacturing",
            "industries": ["Plastics & Rubber Manufacturing"],
            "country": "United States", "state": "Ohio"}}
        self.resolve_calls = []
        self.enrich_calls = []

    def resolve_company(self, **kwargs):
        self.resolve_calls.append(kwargs)
        return self.resolve_results.pop(0) if self.resolve_results else {"status": "empty"}

    def enrich_company(self, company_id):
        self.enrich_calls.append(company_id)
        return self.enrich_result


def _write_targets(tmp_path, body: str):
    p = tmp_path / "targets.yaml"
    p.write_text(textwrap.dedent(body))
    return p


def test_load_targets_returns_active_entities_only(tmp_path):
    targets = _write_targets(tmp_path, """\
        discovery:
          results_per_entity: 2
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
            - name: Paused Co
              active: false
        industry:
          search_mode: concept
          active: true
          include_any: [plastics]
        """)
    loaded = enrich_targets.load_targets_for_enrichment(str(targets))
    assert [t["name"] for t in loaded] == ["Avient"]
    assert loaded[0]["zoominfo_company_id"] == 357374413


def test_dry_run_emits_diff_and_writes_nothing(tmp_path, capsys):
    targets = _write_targets(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
        """)
    out = tmp_path / "target_metadata.yaml"
    client = _FakeClient()
    rc = enrich_targets.run(
        targets_path=str(targets), out_path=str(out),
        only=None, write=False, today="2026-06-14", client=client,
    )
    assert rc == 0
    assert not out.exists()  # dry-run writes nothing
    printed = capsys.readouterr().out
    assert "Avient" in printed
    assert "verified" in printed
    assert client.enrich_calls == [357374413]  # precurated id enriched directly


def test_resolution_cascade_falls_back_to_name_only(tmp_path):
    targets = _write_targets(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
        """)
    out = tmp_path / "target_metadata.yaml"
    # No domain/hq hints in yaml -> single name-only resolve call.
    client = _FakeClient(resolve_results=[{"status": "ok", "company_id": 111}])
    enrich_targets.run(
        targets_path=str(targets), out_path=str(out),
        only=None, write=False, today="2026-06-14", client=client,
    )
    assert client.resolve_calls == [{"domain": None, "name": "Avient",
                                     "hq_country": None, "hq_state": None}]
    assert client.enrich_calls == [111]
