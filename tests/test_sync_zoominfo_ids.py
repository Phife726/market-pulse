"""Tests for scripts/sync_zoominfo_ids.py — the target_metadata -> targets.yaml
id bridge. No live calls; pure text/dict fixtures.

Resolution gating (active + approved/verified only, int ids only), the
line-level patch (active, id-less entities; idempotent), and the
post-condition through the targets catalogue: an input the loader rejects, a
patch that would not load, a patch that loads to something other than the
current targets plus exactly the fills the catalogue expects (wrong value,
a patcher that silently fills nothing, a same-named curated copy in another
group) — each refused with exit 1 and nothing written; with the ordinal
apparatus gone the oracle is exercised through injected bad patchers.
Decide-in-the-catalogue, locate-by-marks: every catalogue-legal spelling is a
fill (quoted name or key, trailing comment, `active: True`, any key order, a
null placeholder rewritten in place with its note, merged `active:`, a stray
`entities:` list under a concept group, the cancelling drift that defeated
the old count check), the plan is pinned directly, and what a line patcher
cannot edit is refused before patching, by name and line: a planned fill on
a flow-style item, on an alias of an anchored entry, or with no field line
after the name to anchor on — plus the name-parity refusal when the composer
walk and the catalogue disagree. Block-scalar names never swallow the
inserted id (insertion is only ever above a real key line), nested id keys
and column-0 comments are handled, CRLF survives, and an empty plan is a
no-op on any valid file."""
import dataclasses
import textwrap
from typing import Callable

import pytest
import yaml

import sync_zoominfo_ids as sync
from targets import entity_entries, load_targets
from tests.conftest import REPO_ROOT


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
          Placeholder Co:
            metadata_record_status: active
            zoominfo_metadata_status: approved
            zoominfo_company_id: 55556666
          Dropped Co:
            metadata_record_status: retired
            zoominfo_metadata_status: verified
            zoominfo_company_id: 99998888
        """)


def test_load_resolved_ids_requires_active_and_approved_status(tmp_path):
    ids = sync.load_resolved_ids(str(_metadata(tmp_path)))
    # verified + approved records with ids are eligible.
    assert ids == {"Teknor Apex": 73040436, "Paused Co": 11112222, "Placeholder Co": 55556666}
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


@pytest.mark.parametrize("write", [False, True])
def test_refuses_an_input_the_loader_already_rejects(tmp_path, capsys, write):
    targets = _write(tmp_path, "targets.yaml", """\
        industry:
          search_mode: concept
          active: true
        """)   # a concept group without include_any: the cron would fail at t=0
    metadata = _metadata(tmp_path)
    before = targets.read_text()

    rc = sync.run(targets_path=str(targets), metadata_path=str(metadata), write=write)

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


def test_post_condition_refuses_a_patcher_that_silently_fills_nothing(tmp_path, capsys, monkeypatch):
    """The oracle is independent of the patcher: it plans from the catalogue's
    runnable targets, not from the patcher's report or the entries the plan
    came from. A patcher that returns the text untouched and reports nothing
    is refused. (Before the plan moved into the catalogue, `active: True`
    produced this by itself; that spelling is a fill now, so the bug is
    simulated directly.)"""
    targets = _targets(tmp_path)
    metadata = _metadata(tmp_path)
    before = targets.read_text()
    monkeypatch.setattr(sync, "patch_targets", lambda path, ids: (before, before, []))

    rc = sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True)

    assert rc == 1
    assert targets.read_text() == before
    err = capsys.readouterr().err
    assert "it filled []" in err and "expected ['Teknor Apex']" in err
    assert "a bug in this script" in err


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


# ---------------------------------------------------------------------------
# Decide in the catalogue, insert in the text: the spellings that used to be
# all-or-nothing stops are fills, and what the line patcher cannot edit is
# refused by name before anything is patched.
# ---------------------------------------------------------------------------

_SPELLINGS = """\
    # keep this comment
    competitors:
      search_mode: entity
      entities:
        - name: "Teknor Apex"   # quoted, with a trailing comment
          active: True
        - name: Placeholder Co
          active: true
          zoominfo_company_id: null   # pending review
        - name: Paused Co
          active: false
    industry:
      search_mode: concept
      active: true
      include_any: [plastics]
    """


def test_catalogue_legal_spellings_are_fills_not_stops(tmp_path):
    """A quoted name with a trailing comment, `active: True`, and a `null`
    placeholder all load under the catalogue's rules; the patcher judges
    none of them, so the sync fills them instead of refusing the whole run."""
    targets = _write(tmp_path, "targets.yaml", _SPELLINGS)
    metadata = _metadata(tmp_path)

    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0

    ids = {t["name"]: t["zoominfo_company_id"] for t in load_targets(str(targets))
           if t["search_mode"] == "entity"}
    assert ids == {"Teknor Apex": 73040436, "Placeholder Co": 55556666}
    text = targets.read_text()
    assert "# keep this comment" in text
    assert '- name: "Teknor Apex"   # quoted, with a trailing comment' in text
    assert "active: True" in text                       # the operator's spelling survives


def test_null_placeholder_is_replaced_in_place_keeping_its_comment(tmp_path):
    """One key, not two: the value token is rewritten on the existing line and
    the operator's trailing note is kept for the diff to show."""
    targets = _write(tmp_path, "targets.yaml", _SPELLINGS)
    metadata = _metadata(tmp_path)
    sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True)

    lines = targets.read_text().splitlines()
    id_lines = [l for l in lines if "zoominfo_company_id" in l]
    assert "      zoominfo_company_id: 55556666   # pending review" in id_lines
    assert sum("Placeholder Co" in l for l in lines) == 1
    assert len([l for l in id_lines if "55556666" in l]) == 1


def test_spellings_sync_is_idempotent(tmp_path):
    targets = _write(tmp_path, "targets.yaml", _SPELLINGS)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    once = targets.read_text()
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    assert targets.read_text() == once


def test_plan_for_is_exactly_active_idless_and_resolved(tmp_path):
    """The plan pinned directly, not via the patcher's effects: a curated copy
    that already carries an id is not planned even when its name resolves —
    a rewrite would silently no-op on it, which is exactly why this cannot be
    left to the post-condition alone. Whether a fill rewrites (a placeholder
    line exists) or inserts is a fact of the located text."""
    text = textwrap.dedent("""\
        competitors:
          search_mode: entity
          entities:
            - name: Teknor Apex
              active: true
              zoominfo_company_id: 111
            - name: Paused Co
              active: false
            - name: Placeholder Co
              active: true
              zoominfo_company_id: null
        customers:
          search_mode: entity
          entities:
            - name: Teknor Apex
              active: true
            - name: Unresolved Co
              active: true
        """)
    ids = {"Teknor Apex": 73040436, "Paused Co": 11112222, "Placeholder Co": 55556666}
    plan = sync.plan_for(text, ids, source="t")
    assert [(f.name, f.company_id, f.loc.id_line is not None) for f in plan] == [
        ("Placeholder Co", 55556666, True),
        ("Teknor Apex", 73040436, False),
    ]


@pytest.mark.parametrize("placeholder, expected", [
    ("zoominfo_company_id:", "zoominfo_company_id: 55556666"),
    ("zoominfo_company_id: # pending", "zoominfo_company_id: 55556666 # pending"),
    ("zoominfo_company_id: ~   # pending", "zoominfo_company_id: 55556666   # pending"),
    ("zoominfo_company_id: NULL", "zoominfo_company_id: 55556666"),
    ('"zoominfo_company_id": null', '"zoominfo_company_id": 55556666'),
    ("zoominfo_company_id : null  # spaced", "zoominfo_company_id : 55556666  # spaced"),
])
def test_every_placeholder_spelling_is_rewritten_in_place(tmp_path, placeholder, expected):
    """The patcher consults no null vocabulary: the catalogue ruled the value
    null; the rewrite keeps the key and the note and replaces what sat between."""
    targets = _write(tmp_path, "targets.yaml", f"""\
        competitors:
          search_mode: entity
          entities:
            - name: Placeholder Co
              active: true
              {placeholder}
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    assert f"      {expected}\n" in targets.read_text()
    assert load_targets(str(targets))[0]["zoominfo_company_id"] == 55556666


def test_a_comment_at_column_zero_inside_a_block_does_not_hide_the_placeholder(tmp_path):
    targets = _write(tmp_path, "targets.yaml", """\
        competitors:
          search_mode: entity
          entities:
            - name: Placeholder Co
              active: true
        # TODO: fill after review
              zoominfo_company_id: null
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    text = targets.read_text()
    assert "# TODO: fill after review" in text
    assert "zoominfo_company_id: 55556666" in text and "null" not in text


def test_a_wider_dash_keeps_the_inserted_field_aligned(tmp_path):
    targets = _write(tmp_path, "targets.yaml", """\
        competitors:
          search_mode: entity
          entities:
            -   name: Teknor Apex
                active: true
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    assert "        zoominfo_company_id: 73040436\n" in targets.read_text()   # under `name`, column 8


def test_a_nested_name_list_under_an_entity_is_not_an_entry(tmp_path):
    """Composed nodes, not lines: an `aliases:` sub-list with `- name:` items
    is a value inside its entity's mapping, never an entry."""
    targets = _write(tmp_path, "targets.yaml", """\
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
              aliases:
                - name: PolyOne
                - name: Clariant Masterbatches
            - name: Teknor Apex
              active: true
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    ids = {t["name"]: t["zoominfo_company_id"] for t in load_targets(str(targets))}
    assert ids == {"Avient": 357374413, "Teknor Apex": 73040436}
    assert "- name: PolyOne" in targets.read_text()


def test_a_block_scalar_name_keeps_the_id_beside_active_not_inside_the_name(tmp_path):
    targets = _write(tmp_path, "targets.yaml", """\
        competitors:
          search_mode: entity
          entities:
            - name: >-
                Teknor Apex
              active: true
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    loaded = load_targets(str(targets))[0]
    assert (loaded["name"], loaded["zoominfo_company_id"]) == ("Teknor Apex", 73040436)


def test_a_nested_id_key_in_a_sub_mapping_is_not_the_placeholder(tmp_path):
    """Only the entry's own `zoominfo_company_id` key node is its placeholder;
    one nested in a sub-mapping is a different node."""
    targets = _write(tmp_path, "targets.yaml", """\
        competitors:
          search_mode: entity
          entities:
            - name: Placeholder Co
              active: true
              notes:
                zoominfo_company_id: 5
              zoominfo_company_id: null
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    text = targets.read_text()
    assert "    zoominfo_company_id: 5\n" in text          # the nested one, untouched
    assert load_targets(str(targets))[0]["zoominfo_company_id"] == 55556666


def test_a_name_term_under_include_any_and_an_unplanned_flow_item_are_tolerated(tmp_path):
    """A `- name:` mapping under a concept group's `include_any` is never
    visited (the catalogue lists no entries there), and a flow-style item is
    only a refusal when a fill is planned on it — inactive, it is tolerated."""
    targets = _write(tmp_path, "targets.yaml", """\
        industry:
          search_mode: concept
          active: true
          include_any:
            - name: not a term
        competitors:
          search_mode: entity
          entities:
            - name: Teknor Apex
              active: true
            - {name: Paused Co, active: false}
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    ids = {t["name"]: t.get("zoominfo_company_id") for t in load_targets(str(targets))
           if t["search_mode"] == "entity"}
    assert ids == {"Teknor Apex": 73040436}   # the inactive flow entry is untouched
    assert "73040436" not in targets.read_text().split("competitors")[0]


def test_a_merge_key_entry_is_located_and_syncs(tmp_path):
    """A merged `active:` is the catalogue's to read (safe_load flattens the
    merge); the walk identifies the entry by its own `name` and the two
    agree, so it syncs."""
    targets = _write(tmp_path, "targets.yaml", """\
        defaults: &d {active: true}
        competitors:
          search_mode: entity
          entities:
            - name: Teknor Apex
              <<: *d
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    assert load_targets(str(targets))[0]["zoominfo_company_id"] == 73040436


def test_a_valid_file_with_nothing_to_fill_is_a_no_op_even_with_a_stray_list(tmp_path, capsys):
    """The sync doubles as a check on a valid file: with an empty plan there is
    nothing to place, so no pre-condition runs and a stray `- name:` list the
    loader ignores does not turn a no-op into a refusal."""
    targets = _write(tmp_path, "targets.yaml", """\
        industry:
          search_mode: concept
          active: true
          include_any: [plastics]
          entities:
            - name: Left Over Co
        competitors:
          search_mode: entity
          entities:
            - name: Avient
              active: true
              zoominfo_company_id: 357374413
        """)
    metadata = _metadata(tmp_path)
    before = targets.read_text()
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    assert "0 id(s)" in capsys.readouterr().out and targets.read_text() == before


def test_a_crlf_file_stays_crlf_after_an_insert(tmp_path):
    targets = tmp_path / "targets.yaml"
    targets.write_bytes(b"competitors:\r\n  search_mode: entity\r\n  entities:\r\n"
                        b"    - name: Teknor Apex\r\n      active: true\r\n")
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    raw = targets.read_bytes()
    assert b"      zoominfo_company_id: 73040436\r\n" in raw and b"\n" not in raw.replace(b"\r\n", b"")


def test_a_scalar_term_spelled_name_colon_is_not_an_entity(tmp_path):
    """A concept term that happens to read `name: ...` lives under a group the
    catalogue lists no entries for, so the walk never visits it."""
    targets = _write(tmp_path, "targets.yaml", """\
        industry:
          search_mode: concept
          active: true
          include_any:
            - name:brand
        competitors:
          search_mode: entity
          entities:
            - name: Teknor Apex
              active: true
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    assert [t["zoominfo_company_id"] for t in load_targets(str(targets)) if t["search_mode"] == "entity"] == [73040436]


def test_a_merged_null_placeholder_is_filled_by_insert(tmp_path):
    """The catalogue sees a null placeholder that arrived through `<<:`; no
    line carries it, so the fill is an insert — the entry's own key overrides
    the merged null, one effective id."""
    targets = _write(tmp_path, "targets.yaml", """\
        defaults: &d {active: true, zoominfo_company_id: null}
        competitors:
          search_mode: entity
          entities:
            - name: Placeholder Co
              <<: *d
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    assert load_targets(str(targets))[0]["zoominfo_company_id"] == 55556666


@pytest.mark.parametrize("folded, key", [
    # `Acme` is a bare word before the colon, so it IS key-shaped — the case a
    # `Teknor Apex: Inc` fixture silently dodges (the space defeats the key
    # pattern, so such a test passes without exercising the anchor at all).
    ("Acme: Inc", "Acme: Inc"),
    ("Teknor Apex: Inc", "Teknor Apex: Inc"),
    ("note: see below", "note: see below"),
])
def test_a_key_shaped_line_inside_a_folded_name_does_not_anchor_the_edit(tmp_path, folded, key):
    """The entry's field column comes from the `- name:` line, and YAML indents
    block-scalar content deeper than that — so a continuation line that looks
    like a key can never be mistaken for the entry's first field."""
    targets = _write(tmp_path, "targets.yaml", f"""\
        competitors:
          search_mode: entity
          entities:
            - name: >-
                {folded}
              active: true
        """)
    metadata = _write(tmp_path, "target_metadata.yaml", f"""\
        version: 1
        targets:
          "{key}":
            metadata_record_status: active
            zoominfo_metadata_status: verified
            zoominfo_company_id: 73040436
        """)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    loaded = load_targets(str(targets))[0]
    assert (loaded["name"], loaded["zoominfo_company_id"]) == (key, 73040436)
    assert f"        {folded}\n" in targets.read_text()           # name untouched


def test_a_colon_inside_a_folded_name_does_not_anchor_the_edit(tmp_path):
    targets = _write(tmp_path, "targets.yaml", """\
        competitors:
          search_mode: entity
          entities:
            - name: >-
                Teknor Apex: Inc
              active: true
        """)
    metadata = _write(tmp_path, "target_metadata.yaml", """\
        version: 1
        targets:
          "Teknor Apex: Inc":
            metadata_record_status: active
            zoominfo_metadata_status: verified
            zoominfo_company_id: 73040436
        """)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    loaded = load_targets(str(targets))[0]
    assert (loaded["name"], loaded["zoominfo_company_id"]) == ("Teknor Apex: Inc", 73040436)


# ===========================================================================
# Compose-marks walker (the #89 dissent, revisited): location by yaml.compose
# ===========================================================================


def test_a_stray_entities_list_under_a_concept_group_still_syncs(tmp_path):
    """The reversal-of-a-reversal: #89 refused this file because a `- name:`
    list the loader ignores shifted every ordinal. Marks do not count lines,
    so nothing shifts and the file syncs; the loader still ignores the list."""
    targets = _write(tmp_path, "targets.yaml", """\
        competitors:
          search_mode: entity
          entities:
            - name: Teknor Apex
              active: true
        industry:
          search_mode: concept
          active: true
          include_any: ["polymer"]
          entities:
            - name: Leftover Co
              active: true
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    assert load_targets(str(targets))[0]["zoominfo_company_id"] == 73040436


@pytest.mark.parametrize("entry", [
    "    - active: true\n      name: Teknor Apex\n",
    '    - "name": Teknor Apex\n      active: true\n',
], ids=["active-first", "quoted key"])
def test_key_order_and_quoted_keys_are_fills(tmp_path, entry):
    """Marks read the node, not the spelling: key order is YAML-irrelevant and
    a quoted key parses to the same mapping, so both sync."""
    targets = _write(tmp_path, "targets.yaml", "competitors:\n  search_mode: entity\n  entities:\n" + entry)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    assert load_targets(str(targets))[0]["zoominfo_company_id"] == 73040436


def test_a_name_reached_through_a_nested_merge_is_identified(tmp_path):
    """safe_load flattens `<<:` merges recursively, so the composed walk must
    follow a merge target's own `<<:` too — otherwise the catalogue lists a
    name the walk cannot see and a loader-valid file is refused."""
    targets = _write(tmp_path, "targets.yaml", """\
        base: &base {name: Teknor Apex}
        proto: &proto
          <<: *base
          active: true
        competitors:
          search_mode: entity
          entities:
            - <<: *proto
              zoominfo_news: true
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    assert load_targets(str(targets))[0]["zoominfo_company_id"] == 73040436


def test_a_placeholder_on_the_dash_line_is_rewritten(tmp_path):
    """Key order is YAML-irrelevant: an id placeholder that opens the item is
    the entry's own key node, rewritten where it sits."""
    targets = _write(tmp_path, "targets.yaml", """\
        competitors:
          search_mode: entity
          entities:
            - zoominfo_company_id: null
              name: Placeholder Co
              active: true
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    assert "    - zoominfo_company_id: 55556666\n" in targets.read_text()


def test_the_last_duplicate_id_key_is_the_one_rewritten(tmp_path):
    """safe_load keeps the last of two duplicate keys; the rewrite must edit
    that one, or the loaded value stays null and the oracle blames the script."""
    targets = _write(tmp_path, "targets.yaml", """\
        competitors:
          search_mode: entity
          entities:
            - name: Placeholder Co
              active: true
              zoominfo_company_id: 5
              zoominfo_company_id: null
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    assert load_targets(str(targets))[0]["zoominfo_company_id"] == 55556666


def test_a_placeholder_value_on_the_next_line_is_refused_by_name(tmp_path, capsys):
    """A rewrite edits one line; a value that continues below its key would
    fold into the new scalar, so it is refused by name before patching."""
    targets = _write(tmp_path, "targets.yaml", """\
        competitors:
          search_mode: entity
          entities:
            - name: Placeholder Co
              active: true
              zoominfo_company_id:
                null
        """)
    metadata = _metadata(tmp_path)
    before = targets.read_text()
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 1
    err = capsys.readouterr().err
    assert "Placeholder Co" in err and "line 6" in err
    assert targets.read_text() == before


def test_an_entry_aliased_from_an_earlier_uncatalogued_list_syncs(tmp_path):
    """The located line can precede an earlier catalogue entry's (an alias to
    a list anchored in a concept group), so edits must apply by line, not by
    plan order — and the oracle confirms the alias carries the id."""
    targets = _write(tmp_path, "targets.yaml", """\
        industry:
          search_mode: concept
          active: true
          include_any: [x]
          entities: &ents
            - name: Teknor Apex
              active: true
        competitors:
          search_mode: entity
          entities:
            - name: Placeholder Co
              active: true
        customers:
          search_mode: entity
          entities: *ents
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    ids = {t["name"]: t["zoominfo_company_id"] for t in load_targets(str(targets))
           if t["search_mode"] == "entity"}
    assert ids == {"Placeholder Co": 55556666, "Teknor Apex": 73040436}


def test_a_non_string_group_key_is_still_located(tmp_path):
    """The catalogue's group key is the constructed value (2024, not "2024");
    the walk must compare like with like."""
    targets = _write(tmp_path, "targets.yaml", """\
        2024:
          search_mode: entity
          entities:
            - name: Teknor Apex
              active: true
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    assert load_targets(str(targets))[0]["zoominfo_company_id"] == 73040436


def test_a_single_line_merged_entry_has_no_anchor_and_is_refused_by_name(tmp_path, capsys):
    """Identified through its merge, but with no key line after its first
    there is nowhere to insert — refused by name, nothing patched."""
    targets = _write(tmp_path, "targets.yaml", """\
        proto: &proto {name: Teknor Apex, active: true}
        competitors:
          search_mode: entity
          entities:
            - <<: *proto
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 1
    err = capsys.readouterr().err
    assert "Teknor Apex" in err and "line 5" in err and "anchor" in err


def test_a_group_introduced_by_a_root_merge_is_located(tmp_path):
    """safe_load flattens a root-level `<<:` too, so the catalogue lists the
    merged-in group; the walk must resolve root merges the same way (and never
    hand the merge key itself to the constructor)."""
    targets = _write(tmp_path, "targets.yaml", """\
        groups: &groups
          competitors:
            search_mode: entity
            entities:
              - name: Teknor Apex
                active: true
        <<: *groups
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 0
    assert load_targets(str(targets))[0]["zoominfo_company_id"] == 73040436


def test_a_flow_style_planned_entry_is_refused_by_its_name_and_line(tmp_path, capsys):
    """Marks locate a flow-style item, but a line patcher cannot insert a key
    into it — so the refusal names the entry and its line instead of
    inferring a class from a count mismatch."""
    targets = _write(tmp_path, "targets.yaml", """\
        competitors:
          search_mode: entity
          entities:
            - {name: Teknor Apex, active: true}
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 1
    err = capsys.readouterr().err
    assert "Teknor Apex" in err
    assert "line 4" in err
    assert "Nothing written." in err
    assert "zoominfo_company_id: 73040436" not in targets.read_text()


def test_an_aliased_entry_planned_for_fill_is_refused_by_name(tmp_path, capsys):
    """An alias resolves to the anchored node, so its mark points at the
    anchor — one text edit would serve two entries. Refused by name."""
    targets = _write(tmp_path, "targets.yaml", """\
        competitors:
          search_mode: entity
          entities:
            - &proto
              name: Teknor Apex
              active: true
        customers:
          search_mode: entity
          entities:
            - *proto
        """)
    metadata = _metadata(tmp_path)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 1
    err = capsys.readouterr().err
    assert "Teknor Apex" in err
    assert "zoominfo_company_id: 73040436" not in targets.read_text()


def test_a_compose_catalogue_name_divergence_is_refused_with_both_names(tmp_path, capsys, monkeypatch):
    """The one pre-condition: the composer walk's names must match the
    catalogue's entry for entry. A divergence (a drifted mode rule, a case
    neither walk anticipated) is refused naming both sides, nothing patched."""
    targets = _targets(tmp_path)
    metadata = _metadata(tmp_path)

    real = sync._entity_loc

    def drifted(item: yaml.Node) -> sync._EntityLoc:
        loc = real(item)
        return dataclasses.replace(loc, name="Drifted Co") if loc.name == "Avient" else loc

    monkeypatch.setattr(sync, "_entity_loc", drifted)
    assert sync.run(targets_path=str(targets), metadata_path=str(metadata), write=True) == 1
    err = capsys.readouterr().err
    assert "Avient" in err and "Drifted Co" in err   # the divergence, named from both sides
    assert "Nothing written." in err


def test_the_composer_walk_agrees_with_the_catalogue_on_the_shipped_file():
    """The shipped control file is the one input the sync always sees, and its
    plan is empty most days (everything synced), so the dry run alone never
    exercises a fill. Pin the join against the real file directly: every
    entry located, same names in the same order, none flow-style or aliased,
    every one with an insertion boundary."""
    text = (REPO_ROOT / "targets.yaml").read_text(encoding="utf-8")
    entries = entity_entries(text, source="targets.yaml")
    located = sync._locate(text, entries, source="targets.yaml")   # raises on divergence
    locs = [loc for _e, loc in located]
    assert len(locs) == len(entries) > 0
    assert not any(l.flow_style for l in locs)
    assert len({l.line for l in locs}) == len(locs)                # no shared nodes
    assert all(l.insert_before is not None or l.id_line is not None for l in locs)
