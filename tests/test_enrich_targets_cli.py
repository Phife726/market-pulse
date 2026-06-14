"""Tests for scripts/enrich_targets.py. A fake client replaces ZoomInfo — no
live calls. Exercises load, resolution cascade, dry-run diff, and --write."""
import logging
import textwrap

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


def test_write_creates_file_and_preserves_curated_on_rerun(tmp_path):
    targets = _write_targets(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
        """)
    out = tmp_path / "target_metadata.yaml"

    # First write.
    enrich_targets.run(targets_path=str(targets), out_path=str(out),
                       only=None, write=True, today="2026-06-14", client=_FakeClient())
    assert out.exists()

    # Human curates an alias by hand.
    import yaml
    doc = yaml.safe_load(out.read_text())
    doc["targets"]["Avient"]["manual_aliases"] = ["AVNT"]
    out.write_text(yaml.safe_dump(doc, sort_keys=False))

    # Re-run --write: machine block refreshes, curated alias survives.
    enrich_targets.run(targets_path=str(targets), out_path=str(out),
                       only=None, write=True, today="2026-06-15", client=_FakeClient())
    doc2 = yaml.safe_load(out.read_text())
    rec = doc2["targets"]["Avient"]
    assert rec["manual_aliases"] == ["AVNT"]                 # curated preserved
    assert rec["zoominfo_metadata_last_refreshed"] == "2026-06-15"  # machine refreshed
    assert rec["zoominfo_metadata_status"] == "verified"


def test_orphaned_record_kept_and_flagged_on_write(tmp_path):
    targets_v1 = _write_targets(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
            - name: Old Co
              active: true
              zoominfo_company_id: 222
        """)
    out = tmp_path / "target_metadata.yaml"
    enrich_targets.run(targets_path=str(targets_v1), out_path=str(out),
                       only=None, write=True, today="2026-06-14", client=_FakeClient())

    # Old Co removed from targets.yaml.
    targets_v2 = _write_targets(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
        """)
    enrich_targets.run(targets_path=str(targets_v2), out_path=str(out),
                       only=None, write=True, today="2026-06-15", client=_FakeClient())

    import yaml
    doc = yaml.safe_load(out.read_text())
    assert doc["targets"]["Old Co"]["metadata_record_status"] == "orphaned"
    assert doc["targets"]["Old Co"]["zoominfo_company_id"] == 222  # kept, not deleted
    assert doc["targets"]["Avient"]["metadata_record_status"] == "active"


def test_main_parses_args_and_defaults_to_dry_run(tmp_path, monkeypatch, capsys):
    targets = _write_targets(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
        """)
    out = tmp_path / "target_metadata.yaml"
    monkeypatch.setattr(enrich_targets, "_DefaultClient", _FakeClient)
    rc = enrich_targets.main([
        "--targets", str(targets), "--out", str(out), "--today", "2026-06-14",
    ])
    assert rc == 0
    assert not out.exists()  # default is dry-run
    assert "Avient" in capsys.readouterr().out


def test_main_write_flag_creates_file(tmp_path, monkeypatch):
    targets = _write_targets(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
        """)
    out = tmp_path / "target_metadata.yaml"
    monkeypatch.setattr(enrich_targets, "_DefaultClient", _FakeClient)
    rc = enrich_targets.main([
        "--targets", str(targets), "--out", str(out),
        "--today", "2026-06-14", "--write",
    ])
    assert rc == 0
    assert out.exists()
    import yaml
    doc = yaml.safe_load(out.read_text())
    assert doc["targets"]["Avient"]["zoominfo_metadata_status"] == "verified"


def test_target_resolving_to_error_gets_error_status(tmp_path):
    targets = _write_targets(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
        """)
    out = tmp_path / "target_metadata.yaml"
    # resolve returns error -> _resolve returns {"error": True} -> status error, no enrich call
    client = _FakeClient(resolve_results=[{"status": "error"}])
    enrich_targets.run(targets_path=str(targets), out_path=str(out),
                       only=None, write=True, today="2026-06-14", client=client)
    import yaml
    doc = yaml.safe_load(out.read_text())
    assert doc["targets"]["Avient"]["zoominfo_metadata_status"] == "error"
    assert client.enrich_calls == []  # error short-circuits before enrich


def test_sparse_enrich_logs_warning_and_records_missing(tmp_path, caplog):
    # Enrich returns status=ok but no firmographics (the live outputFields risk):
    # the record must become 'missing', and the CLI must warn that an ok Enrich
    # yielded sparse data so an operator knows outputFields likely needs adding.
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
    client.enrich_result = {"status": "ok", "company": {}}  # ok but empty firmographics
    with caplog.at_level(logging.WARNING, logger="enrich_targets"):
        enrich_targets.run(targets_path=str(targets), out_path=str(out),
                           only=None, write=True, today="2026-06-14", client=client)
    import yaml
    doc = yaml.safe_load(out.read_text())
    rec = doc["targets"]["Avient"]
    assert rec["zoominfo_metadata_status"] == "missing"  # behavior preserved
    text = caplog.text.lower()
    assert "avient" in text and "sparse" in text  # operator-facing pointer


def test_error_enrich_does_not_emit_sparse_warning(tmp_path, caplog):
    # An entitlement/auth error must stay 'error' (not 'missing') and must NOT be
    # described as sparse firmographics — those are distinct failure modes.
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
    client.enrich_result = {"status": "error"}
    with caplog.at_level(logging.WARNING, logger="enrich_targets"):
        enrich_targets.run(targets_path=str(targets), out_path=str(out),
                           only=None, write=True, today="2026-06-14", client=client)
    import yaml
    doc = yaml.safe_load(out.read_text())
    assert doc["targets"]["Avient"]["zoominfo_metadata_status"] == "error"
    assert "sparse" not in caplog.text.lower()


def test_only_flag_processes_single_target(tmp_path):
    targets = _write_targets(tmp_path, """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
            - name: SABIC
              active: true
              zoominfo_company_id: 98664698
        """)
    out = tmp_path / "target_metadata.yaml"
    client = _FakeClient()
    enrich_targets.run(targets_path=str(targets), out_path=str(out),
                       only="Avient", write=True, today="2026-06-14", client=client)
    import yaml
    doc = yaml.safe_load(out.read_text())
    assert "Avient" in doc["targets"]
    # SABIC not processed this run, and no prior file -> SABIC absent
    assert "SABIC" not in doc["targets"]
    assert client.enrich_calls == [357374413]
