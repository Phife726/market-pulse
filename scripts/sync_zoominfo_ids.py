"""Bridge: copy resolved zoominfo_company_id values from target_metadata.yaml
into targets.yaml so ZoomInfo *news discovery* (which reads targets.yaml, not
the metadata companion) turns on for the newly enriched companies.

Decides in the catalogue, inserts in the text: the plan comes from
`targets.entity_entries` (active, id-less, resolved in an approved/verified
metadata record); the patcher locates every entry by `yaml.compose` line
marks and judges no value — a quoted name or key, a trailing comment,
`active: True`, any key order, a `zoominfo_company_id: null` placeholder
(rewritten in place, comment kept) are all fills, not stops. The composer
walk mirrors the catalogue's mode rule (a concept group's leftover
`entities:` list is skipped, exactly as the loader skips it) and follows
`<<:` merges for identification only; the one pre-condition is name parity —
the walk must list the same names, in the same order, as the catalogue, or
the sync refuses at the first divergence, naming both sides. What a line
patcher cannot edit is refused before anything is patched, always by name
and line: a planned fill on a flow-style item, on an alias of an anchored
entry, or on an entry with no key line after its first to anchor on.

Comment-preserving: patches by line, never round-trips the whole YAML.
Dry-run by default (prints a unified diff, writes nothing); --write applies.
Both modes post-condition the result through `targets.parse_targets` as an
independent oracle: the output must load to the input's targets plus exactly
the fills the catalogue expects — otherwise exit 1, nothing written.

Syncing an id into targets.yaml makes that company an active ZoomInfo news
discovery target. So the bridge deliberately refuses to promote `needs_review`
rows: a low-confidence name-only match stays review-only until an operator
records an explicit `approved` status (or the enricher wrote `verified` for a
precurated/domain match). This keeps the helper a review step, not an
auto-approval step.
"""
from __future__ import annotations

import argparse
import difflib
from dataclasses import dataclass
import os
import sys
from itertools import zip_longest
from typing import Optional

import yaml
from yaml.constructor import SafeConstructor

# When run as `python scripts/sync_zoominfo_ids.py`, sys.path[0] is scripts/,
# not the repo root, so the catalogue must be put on the path explicitly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from targets import TargetsError, entity_entries, parse_targets  # noqa: E402

# ZoomInfo statuses that gate a record for sync into targets.yaml. `verified` is
# written by the enricher for precurated/domain (high-confidence) matches;
# `approved` is an operator-set token for a reviewed name-only match. Anything
# still `needs_review` / `missing` / `error` is intentionally NOT synced.
APPROVED_ZOOMINFO_STATUSES = {"verified", "approved"}


def load_resolved_ids(metadata_path: str) -> dict[str, int]:
    """Return {target_key: zoominfo_company_id} for active metadata records that
    carry an id AND an approved/verified ZoomInfo status. Missing file, malformed
    records, or unreviewed (`needs_review`) rows yield no entry."""
    with open(metadata_path) as fh:
        data = yaml.safe_load(fh) or {}
    out: dict[str, int] = {}
    for key, rec in (data.get("targets") or {}).items():
        if not isinstance(rec, dict):
            continue
        cid = rec.get("zoominfo_company_id")
        metadata_status = rec.get("metadata_record_status", "active")
        zoominfo_status = rec.get("zoominfo_metadata_status")
        if (
            isinstance(cid, int) and not isinstance(cid, bool) and cid
            and metadata_status == "active"
            and zoominfo_status in APPROVED_ZOOMINFO_STATUSES
        ):
            out[str(key)] = cid
    return out


_REJECTED = "{source} is already rejected by the loader — fix it before syncing: {exc}"


_MERGE_TAG = "tag:yaml.org,2002:merge"


@dataclass(frozen=True)
class _EntityLoc:
    """Where one entity entry lives in the text, read off `yaml.compose`
    marks (0-based lines, columns): the item's first line; where an insert
    goes — above the first key line after the item's first, at that key's
    column (a key line is never inside a block scalar, so an insert can never
    land in the name); and the entry's own `zoominfo_company_id` placeholder —
    its key line and its value's column span on that line (`None` when the key
    is absent or arrives through a `<<:` merge, so a fill inserts; a span of
    `None` with a key line means the value sits on another line, which a
    one-line rewrite cannot edit)."""
    name: Optional[str]
    line: int
    insert_before: Optional[int] = None
    insert_column: int = 0
    id_line: Optional[int] = None
    id_value: Optional[tuple[int, int]] = None
    flow_style: bool = False


@dataclass(frozen=True)
class Fill:
    """One planned fill: the entry's place in the text and the id to write."""
    company_id: int
    loc: _EntityLoc

    @property
    def name(self) -> str:
        return self.loc.name or ""


class UnplaceableEntity(ValueError):
    """Refused before anything is patched, always by name and line — the
    messages raised in `_locate` and `_refuse_uneditable` are the taxonomy."""


def _fillable(company_id: Optional[int], name: str, ids: dict[str, int]) -> bool:
    """The one fill rule: no id yet, and resolved. Spelled once so the plan and
    the oracle — which apply it to different sources on purpose — cannot
    drift apart."""
    return company_id is None and name in ids


def _planned(entry: dict, ids: dict[str, int]) -> bool:
    """Whether the catalogue's entry gets a fill: active, and the fill rule."""
    return entry["active"] and _fillable(entry["zoominfo_company_id"], entry["name"], ids)


def _merged_value(node: yaml.MappingNode, key: str) -> Optional[yaml.Node]:
    """The value node under `key`, with PyYAML's merge semantics: the node's
    own pair wins, else the first `<<:` target carrying it, each target
    flattened recursively. Identification only — merges are followed to read
    `name` and `entities`, never to place an edit. Compose has already
    resolved aliases into shared nodes."""
    merge_targets: list = []
    for k, v in node.value:
        if k.tag == _MERGE_TAG:
            merge_targets.extend(v.value if isinstance(v, yaml.SequenceNode) else [v])
        elif isinstance(k, yaml.ScalarNode) and k.value == key:
            return v
    for target in merge_targets:
        if isinstance(target, yaml.MappingNode):
            found = _merged_value(target, key)
            if found is not None:
                return found
    return None


def _scalar_key(node: yaml.Node):
    """A mapping key as safe_load constructs it (`2024:` is the int 2024), so
    the walk compares with the catalogue's group names like with like."""
    return SafeConstructor().construct_object(node) if isinstance(node, yaml.ScalarNode) else None


def _entity_loc(item: yaml.Node) -> _EntityLoc:
    line = item.start_mark.line
    if not isinstance(item, yaml.MappingNode):
        return _EntityLoc(name=None, line=line)
    name = _merged_value(item, "name")
    own = [(k, v) for k, v in item.value if isinstance(k, yaml.ScalarNode)]
    anchor = next((k for k, _v in own if k.start_mark.line > line), None)
    # The last duplicate key is the one safe_load keeps, so it is the one to edit.
    id_pair = next(((k, v) for k, v in reversed(own) if k.value == "zoominfo_company_id"), None)
    id_line = id_value = None
    if id_pair is not None:
        id_key, id_val = id_pair
        id_line = id_key.start_mark.line
        if id_val.start_mark.line == id_line:
            id_value = (id_val.start_mark.column, id_val.end_mark.column)
    return _EntityLoc(
        name=name.value if isinstance(name, yaml.ScalarNode) else None,
        line=line,
        insert_before=anchor.start_mark.line if anchor else None,
        insert_column=anchor.start_mark.column if anchor else 0,
        id_line=id_line,
        id_value=id_value,
        flow_style=item.flow_style,
    )


def _locate(text: str, entries: list[dict], *, source: str) -> list[tuple[dict, _EntityLoc]]:
    """Every catalogue entry joined to its place in the text. The composed
    document is walked only where the catalogue reported entries — the groups
    it named, in file order — so no mode rule is re-decided here: a concept
    group's leftover `entities:` list is never visited because the catalogue
    never lists it. The one pre-condition: the walk must yield the same names
    in the same order as the catalogue, or the join is refused at the first
    divergence naming both sides, so a fill can never land on the wrong entry."""
    groups = {e["group"] for e in entries}
    locs: list[_EntityLoc] = []
    for group_key, group_node in yaml.compose(text).value:
        if _scalar_key(group_key) in groups and isinstance(group_node, yaml.MappingNode):
            items = _merged_value(group_node, "entities")
            if isinstance(items, yaml.SequenceNode):
                locs.extend(_entity_loc(item) for item in items.value)
    for i, (entry, loc) in enumerate(zip_longest(entries, locs)):
        want = entry["name"] if entry else None
        got = loc.name if loc else None
        if want != got:
            at = f" (line {loc.line + 1})" if loc else ""
            raise UnplaceableEntity(
                f"{source}: the composed document disagrees with the catalogue at entity "
                f"entry #{i + 1}: the catalogue lists {want!r}, the document walk sees "
                f"{got!r}{at} — {len(entries)} entries vs {len(locs)} located. A fill cannot "
                f"be placed until the two agree."
            )
    return list(zip(entries, locs))


def plan_for(text: str, ids: dict[str, int], *, source: str) -> list[Fill]:
    """The plan for `text`: composed from the catalogue's entries (active, and
    the fill rule), each joined to its place in the text, in document order.
    With nothing to fill there is nothing to locate, so the parity
    pre-condition never runs on a file the sync has nothing to do to."""
    entries = entity_entries(text, source=source)
    if not any(_planned(e, ids) for e in entries):
        return []
    return [Fill(ids[e["name"]], loc)
            for e, loc in _locate(text, entries, source=source) if _planned(e, ids)]


def _refuse_uneditable(plan: list[Fill], *, source: str) -> None:
    """A planned fill the line patcher cannot realize, by name and line: a
    flow-style item (no line to insert a key into); a placeholder whose value
    sits on another line than its key (a one-line rewrite would fold it); an
    entry with no key line after its first to anchor an insert on; two fills
    on one line — an alias resolves to its anchor's node, so one edit would
    serve every occurrence."""
    claimed: set[int] = set()
    for fill in plan:
        loc = fill.loc
        where = f"'{fill.name}' (line {loc.line + 1})"
        if loc.flow_style:
            raise UnplaceableEntity(
                f"{source}: {where} is spelled as a flow-style item — a line patcher "
                f"cannot insert a key into it. Spell it as a block item."
            )
        if loc.id_line is not None and loc.id_value is None:
            raise UnplaceableEntity(
                f"{source}: {where} has its zoominfo_company_id placeholder value on a "
                f"different line than its key (line {loc.id_line + 1}); a one-line rewrite "
                f"cannot edit it. Put the value beside the key."
            )
        if loc.id_line is None and loc.insert_before is None:
            raise UnplaceableEntity(
                f"{source}: {where} has no key line after its first to anchor an insert on."
            )
        if loc.line in claimed:
            raise UnplaceableEntity(
                f"{source}: {where} is an alias of an anchored entry — one text edit would "
                f"serve every occurrence. Spell the entry out."
            )
        claimed.add(loc.line)


def _rewrite_id_line(line: str, value_span: tuple[int, int], company_id: int) -> str:
    """Put `company_id` where the placeholder value sits — the value node's
    own column span, so the key (however spelled) and any trailing note are
    untouched. An empty value (`key:` alone, or before a note) spans nothing
    right after the colon, so the id is spliced in with its own space."""
    start, end = value_span
    filler = f"{company_id}" if end > start else f" {company_id}"
    return line[:start] + filler + line[end:]


def patch_targets(targets_path: str, ids: dict[str, int]) -> tuple[str, str, list[str]]:
    """Fill `zoominfo_company_id` for every entry the plan names. Returns
    (old_text, new_text, filled_names). Raises UnplaceableEntity (nothing
    patched) when the composed document and the catalogue disagree, or for a
    fill a line patcher cannot realize; TargetsError for an input the
    catalogue rejects. With nothing to fill, nothing is located or refused."""
    # newline="" keeps the file's own line endings on both sides of the patch:
    # text mode would rewrite a CRLF file as LF on the way through.
    with open(targets_path, newline="") as fh:
        old = fh.read()
    lines = old.splitlines(keepends=True)
    plan = plan_for(old, ids, source=targets_path)
    if not plan:
        return old, old, []   # nothing to place, so nothing to refuse
    _refuse_uneditable(plan, source=targets_path)

    # Bottom-up by line (an alias can sit textually before an earlier entry),
    # so an edit never shifts a line a fill above it names.
    for fill in sorted(plan, key=lambda f: f.loc.line, reverse=True):
        loc = fill.loc
        if loc.id_line is not None:
            lines[loc.id_line] = _rewrite_id_line(lines[loc.id_line], loc.id_value, fill.company_id)
        else:
            # Insert above the entry's next key line, at its column — YAML fixes
            # every sibling field there. A placeholder the catalogue saw but no
            # line carries (a `<<:` merge default) is overridden by the entry's
            # own key. The new line takes its neighbour's ending, so a CRLF file
            # stays CRLF.
            anchor = lines[loc.insert_before]
            eol = anchor[len(anchor.rstrip("\r\n")):] or "\n"
            lines.insert(loc.insert_before,
                         f"{' ' * loc.insert_column}zoominfo_company_id: {fill.company_id}{eol}")

    return old, "".join(lines), [f.name for f in plan]


def post_condition(current: str, proposed: str, ids: dict[str, int], filled: list[str],
                   *, source: str) -> Optional[str]:
    """Why `proposed` must not be written, or None. Both texts must load under
    the catalogue's rules, and `proposed` must load to the current targets
    with `zoominfo_company_id` set on exactly the entities the catalogue says
    are fillable — active, id-less, resolved in `ids`. The patcher's own
    report (`filled`) is checked against that plan first, so a patcher that
    silently fills nothing is refused with a message that says so: meaning,
    not just syntax."""
    proposed_source = f"{source} (proposed)"
    try:
        before = parse_targets(current, source=source)
    except TargetsError as exc:
        return _REJECTED.format(source=source, exc=exc)
    try:
        # The idempotent re-run proposes the text it was given: one parse.
        after = before if proposed == current else parse_targets(proposed, source=proposed_source)
    except TargetsError as exc:
        return (f"{proposed_source} is rejected by the loader — a bug in this script's "
                f"line patcher, not in the input: {exc}")

    fills: list[str] = []
    expected: list[dict] = []
    for t in before:
        if t["search_mode"] == "entity" and _fillable(t["zoominfo_company_id"], t["name"], ids):
            fills.append(t["name"])
            t = {**t, "zoominfo_company_id": ids[t["name"]]}
        expected.append(t)
    mismatch = f"{proposed_source} does not mean what the patcher reported"
    if set(filled) != set(fills):
        return (f"{mismatch}: it filled {sorted(set(filled))}, the catalogue expected "
                f"{sorted(set(fills))}. A fill went missing or landed on the wrong "
                f"entry — a bug in this script's composer walk or line edits, not in "
                f"the input. Nothing is written until every expected fill lands.")
    for i, (want, got) in enumerate(zip_longest(expected, after)):
        if want != got:
            if want is None or got is None:
                return f"{mismatch}: {len(after)} targets loaded, {len(expected)} expected"
            changed = sorted(k for k in set(want) | set(got) if want.get(k) != got.get(k))
            return (f"{mismatch}: target #{i + 1} {want['name']!r} differs in {changed} — "
                    f"expected {[want.get(k) for k in changed]}, loaded {[got.get(k) for k in changed]}. "
                    f"A line edit changed the file's meaning beyond the planned fills — a bug "
                    f"in this script, not in the input.")
    return None


def _refuse(why: str) -> int:
    """Every refusal, one way: the reason, then the promise, exit 1."""
    print(f"\n{why}\nNothing written.", file=sys.stderr)
    return 1


def run(*, targets_path: str, metadata_path: str, write: bool) -> int:
    ids = load_resolved_ids(metadata_path)
    try:
        old, new, filled = patch_targets(targets_path, ids)
    except TargetsError as exc:
        return _refuse(_REJECTED.format(source=targets_path, exc=exc))
    except UnplaceableEntity as exc:
        return _refuse(str(exc))
    problem = post_condition(old, new, ids, filled, source=targets_path)

    if not write:
        sys.stdout.writelines(difflib.unified_diff(
            old.splitlines(keepends=True), new.splitlines(keepends=True),
            fromfile=targets_path + " (current)", tofile=targets_path + " (proposed)",
        ))
    if problem:
        return _refuse(problem)
    if write and filled:   # the post-condition guarantees `new` is `old` plus the fills
        with open(targets_path, "w", newline="") as fh:
            fh.write(new)
    verb, stream = ("Wrote", sys.stdout) if write else ("\n# would fill", sys.stderr)
    print(f"{verb} {len(filled)} id(s) into {targets_path}: {', '.join(filled) or '(none)'}", file=stream)
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Copy resolved ZoomInfo ids into targets.yaml.")
    p.add_argument("--targets", default="targets.yaml")
    p.add_argument("--metadata", default="target_metadata.yaml")
    p.add_argument("--write", action="store_true",
                   help="Apply changes (default: dry-run prints a diff)")
    args = p.parse_args(argv)
    return run(targets_path=args.targets, metadata_path=args.metadata, write=args.write)


if __name__ == "__main__":
    raise SystemExit(main())
