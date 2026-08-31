"""Tests for scripts/sync_zoominfo_ids.py — the target_metadata -> targets.yaml
id bridge. No live calls; pure text/dict fixtures.

Resolution gating (active + approved/verified only, int ids only), the
line-level patch (active, id-less entities; idempotent), and the
post-condition through the targets catalogue: an input the loader rejects, a
patch that would not load, a patch that loads to something other than the
current targets plus exactly the fills the catalogue expects (wrong value,
a walker that silently fills nothing, a same-named curated copy in another
group) — each refused with exit 1 and nothing written."""
import textwrap
from typing import Callable

import pytest
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

    assert sync.run(targets_path=str(targets), metadata_path=str(_metadata(tmp_path)), write=True) == 0
    after_first = targets.read_text()
    assert "zoominfo_company_id: 73040436" in after_first

    # Re-running is a no-op: the id already exists, nothing new is filled.
    _, new2, filled2 = sync.patch_targets(str(targets), ids)
    assert filled2 == []
    assert new2 == after_first


# ===========================================================================
# The post-condition: the proposed text must load, and mean what the
# catalogue expects — the patcher's report included
# ===========================================================================


def _bad_patch(old_text: str, id_line: str) -> Callable:
    """A patcher stand-in: `old_text` with `id_line` inserted after the first
    `- name: Teknor Apex` line — mis-indented or misplaced by the caller's
    choice — reporting Teknor Apex as filled."""
    def patch(targets_path: str, ids: dict) -> tuple[str, str, list[str]]:
        lines = old_text.splitlines(keepends=True)
        i = next(k for k, l in enumerate(lines) if "name: Teknor Apex" in l)
        return old_text, "".join(lines[: i + 1] + [id_line] + lines[i + 1:]), ["Teknor Apex"]
    return patch


def test_refuses_an_input_the_loader_already_rejects(tmp_path, capsys):
    targets = _write(tmp_path, "targets.yaml", """\
        industry:
          search_mode: concept
          active: true
        """)   # a concept group without include_any: the cron would fail at t=0
    metadata = _metadata(tmp_path)
    before = targets.read_text()

    rc = sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True)

    assert rc == 1
    assert targets.read_text() == before
    err = capsys.readouterr().err
    assert "already rejected by the loader" in err and "include_any" in err


@pytest.mark.parametrize("write", [True, False], ids=["write", "dry-run"])
def test_refuses_a_patch_the_loader_rejects(tmp_path, capsys, monkeypatch, write):
    """Valid input, invalid output can only be a patcher bug: nothing is
    written, the error says so, exit 1 — and the dry-run still prints the
    diff, which is the evidence."""
    targets, metadata = _targets(tmp_path), _metadata(tmp_path)
    before = targets.read_text()
    monkeypatch.setattr(sync, "patch_targets",
                        _bad_patch(before, "    zoominfo_company_id: 111\n"))   # a sibling of the entity, not a field

    rc = sync.run(targets_path=str(targets), metadata_path=str(metadata), write=write)

    out, err = capsys.readouterr()
    assert rc == 1
    assert targets.read_text() == before
    assert "rejected by the loader" in err and "patcher" in err
    if write:
        assert out == ""                                 # --write prints no diff
    else:
        assert "+    zoominfo_company_id: 111" in out    # dry-run: the evidence, then the refusal


def test_post_condition_pins_meaning_not_just_syntax(tmp_path, capsys, monkeypatch):
    """A patch that loads fine but carries a value the metadata never resolved
    is refused: the loaded targets must equal the current targets plus
    exactly the fills the catalogue expects."""
    targets, metadata = _targets(tmp_path), _metadata(tmp_path)
    before = targets.read_text()
    monkeypatch.setattr(sync, "patch_targets",
                        _bad_patch(before, "      zoominfo_company_id: 999\n"))

    rc = sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True)

    assert rc == 1
    assert targets.read_text() == before
    err = capsys.readouterr().err
    assert "does not mean what the patcher reported" in err and "Teknor Apex" in err


def test_post_condition_refuses_a_patcher_that_silently_fills_nothing(tmp_path, capsys):
    """`active: True` is a boolean the catalogue accepts and the line-walker's
    literal `active: true` does not see: the real patcher fills nothing and
    reports nothing, and the post-condition — planned from the catalogue,
    not from the report — refuses."""
    targets = _write(tmp_path, "targets.yaml", """\
        competitors:
          search_mode: entity
          entities:
            - name: Teknor Apex
              active: True
        """)
    metadata = _metadata(tmp_path)
    before = targets.read_text()

    rc = sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True)

    assert rc == 1
    assert targets.read_text() == before
    err = capsys.readouterr().err
    assert "it filled []" in err and "expected ['Teknor Apex']" in err


def test_same_name_in_two_groups_fills_only_the_id_less_copy(tmp_path):
    """The plan is per target, not per name: a curated copy of an entity in
    another group keeps its id, and the post-condition expects exactly that."""
    targets = _write(tmp_path, "targets.yaml", """\
        competitors:
          search_mode: entity
          entities:
            - name: Teknor Apex
              active: true
              zoominfo_company_id: 111
        customers:
          search_mode: entity
          entities:
            - name: Teknor Apex
              active: true
        """)
    metadata = _metadata(tmp_path)

    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    from targets import load_targets
    ids = [(t["category"], t["zoominfo_company_id"]) for t in load_targets(str(targets))]
    assert ids == [("competitors", 111), ("customers", sync.load_resolved_ids(str(metadata))["Teknor Apex"])]


def test_string_or_boolean_ids_in_metadata_are_not_resolved(tmp_path):
    """An id must be an int: a hand-typed "123" or a stray `true` would be
    written as a different value than the catalogue later reads."""
    metadata = _write(tmp_path, "target_metadata.yaml", """\
        targets:
          Quoted Co:
            zoominfo_company_id: "123"
            zoominfo_metadata_status: approved
          Bool Co:
            zoominfo_company_id: true
            zoominfo_metadata_status: approved
          Real Co:
            zoominfo_company_id: 123
            zoominfo_metadata_status: approved
        """)
    assert sync.load_resolved_ids(str(metadata)) == {"Real Co": 123}
