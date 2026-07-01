"""Tests for scripts/sync_zoominfo_ids.py — the target_metadata -> targets.yaml
id bridge. No live calls; pure text/dict fixtures."""
import textwrap

import yaml

import sync_zoominfo_ids as sync


def _write(tmp_path, name, body):
    p = tmp_path / name
    p.write_text(textwrap.dedent(body))
    return p


def _targets(tmp_path):
    return _write(tmp_path, "targets.yaml", """\
        # keep this comment
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
            - name: Teknor Apex
              active: true
            - name: Needs Review Co
              active: true
            - name: Paused Co
              active: false
            - name: Unresolved Co
              active: true
        industry:
          search_mode: concept
          active: true
          include_any: [plastics]
        """)


def _metadata(tmp_path):
    return _write(tmp_path, "target_metadata.yaml", """\
        version: 1
        targets:
          Teknor Apex:
            metadata_record_status: active
            zoominfo_metadata_status: verified
            zoominfo_company_id: 73040436
          Needs Review Co:
            metadata_record_status: active
            zoominfo_metadata_status: needs_review
            zoominfo_company_id: 44445555
          Paused Co:
            metadata_record_status: active
            zoominfo_metadata_status: approved
            zoominfo_company_id: 11112222
          Dropped Co:
            metadata_record_status: retired
            zoominfo_metadata_status: verified
            zoominfo_company_id: 99998888
        """)


def test_load_resolved_ids_requires_active_and_approved_status(tmp_path):
    ids = sync.load_resolved_ids(str(_metadata(tmp_path)))
    # verified + approved records with ids are eligible.
    assert ids == {"Teknor Apex": 73040436, "Paused Co": 11112222}
    # needs_review is review-only — never eligible for sync, even with an id.
    assert "Needs Review Co" not in ids
    # retired metadata record is ignored regardless of ZoomInfo status.
    assert "Dropped Co" not in ids


def test_needs_review_row_is_not_written_into_targets(tmp_path):
    targets = _targets(tmp_path)
    ids = sync.load_resolved_ids(str(_metadata(tmp_path)))
    _, new, filled = sync.patch_targets(str(targets), ids)

    # 'Needs Review Co' is an active, id-less entity — the ONLY reason it is not
    # filled is its needs_review status. This is the safety contract.
    assert "Needs Review Co" not in filled
    assert "44445555" not in new


def test_patch_fills_only_active_missing_entities(tmp_path):
    targets = _targets(tmp_path)
    ids = sync.load_resolved_ids(str(_metadata(tmp_path)))
    old, new, filled = sync.patch_targets(str(targets), ids)

    # Only the active, id-less, approved/verified entity is filled.
    assert filled == ["Teknor Apex"]
    assert "zoominfo_company_id: 73040436" in new
    # Inactive 'Paused Co' is skipped even though its metadata is approved.
    assert "11112222" not in new
    # Already-curated Avient keeps its single id — no duplicate line.
    assert new.count("357374413") == 1
    # Comment survives the line-based patch.
    assert "# keep this comment" in new


def test_patched_yaml_still_parses_and_id_is_readable(tmp_path):
    targets = _targets(tmp_path)
    ids = sync.load_resolved_ids(str(_metadata(tmp_path)))
    _, new, _ = sync.patch_targets(str(targets), ids)

    parsed = yaml.safe_load(new)
    entities = {e["name"]: e for e in parsed["competitors"]["entities"]}
    assert entities["Teknor Apex"]["zoominfo_company_id"] == 73040436
    assert entities["Teknor Apex"]["active"] is True
    assert "zoominfo_company_id" not in entities["Unresolved Co"]


def test_write_persists_and_is_idempotent(tmp_path):
    targets = _targets(tmp_path)
    ids = sync.load_resolved_ids(str(_metadata(tmp_path)))

    sync.run(targets_path=str(targets), metadata_path=str(_metadata(tmp_path)), write=True)
    after_first = targets.read_text()
    assert "zoominfo_company_id: 73040436" in after_first

    # Re-running is a no-op: the id already exists, nothing new is filled.
    _, new2, filled2 = sync.patch_targets(str(targets), ids)
    assert filled2 == []
    assert new2 == after_first
