"""Bridge: copy resolved zoominfo_company_id values from target_metadata.yaml
into targets.yaml so ZoomInfo *news discovery* (which reads targets.yaml, not
the metadata companion) turns on for the newly enriched companies.

Decides in the catalogue, inserts in the text: the plan comes from
`targets.entity_entries` (active, id-less, resolved in an approved/verified
metadata record); the walker keeps two key locators and judges no value,
filling by ordinal — so a quoted name, a trailing comment, `active: True` and
a `zoominfo_company_id: null` placeholder (rewritten in place, comment kept)
are fills, not stops. The walker counts structurally — every list item opens
a block, so a `- name:` nested under an entity is that entity's, not an entry —
and anchors every field edit on the entry's first field line.
What it cannot place is refused before anything is patched: by name when the
catalogue can see the cause (a block not opening with `name:` at or before a
planned fill — after the last fill such a block is tolerated, its ordinals
being past use; a planned fill with no field line to anchor on), by class when only the count reveals it (a flow-style or quoted-key
entry, a stray `- name:` list).

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
import re
import sys
from itertools import zip_longest
from typing import Optional

import yaml

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


# The walker's two locators. Neither judges a value — active/id-less/resolved
# come from the catalogue's entries; these only find the line to edit.
# `    - name: Foo` (entity list item): captures the indent the block hangs off.
_NAME_RE = re.compile(r"^(\s*)-\s+name:(?:\s|$)")
# Any list item at all — the walker opens a block on every one, so whatever
# sits nested under an item it does not recognise (a `- active:`-first entry,
# a concept term) is absorbed into that block instead of surfacing as an entry.
_ITEM_RE = re.compile(r"^(\s*)-(?:\s|$)")
# `      key: ...` — the entry's first field line: a plausible mapping key
# (`active:`, `<<:`) followed by its terminator, at the block's own indent. It
# anchors where the entity's fields sit: the id line is inserted there, and a
# rewrite only touches a key at that indent — never one nested in a
# sub-mapping, nor a continuation line of a block-scalar name (`Foo: Inc`,
# `https://…` — neither is a bare key followed by a space).
_KEY_RE = re.compile(r"^(\s*)(?:<<|[A-Za-z_][\w.-]*):(?:\s|$)")
# `    zoominfo_company_id: <whatever>   # note` — the key, then whatever value
# is written (the catalogue already ruled it null; no null vocabulary here),
# then the trailing note. Matched on a line without its ending.
_ID_KEY_RE = re.compile(r"^(\s*zoominfo_company_id:)[^#]*?(\s*#.*)?$")

_REJECTED = "{source} is already rejected by the loader — fix it before syncing: {exc}"


@dataclass(frozen=True)
class Fill:
    """One planned fill: which entry (by ordinal), which id, and whether the
    entry already carries a placeholder id line to rewrite."""
    ordinal: int
    name: str
    company_id: int
    replace: bool


class UnplaceableEntity(ValueError):
    """Legal YAML the line walker has no locator for, refused before anything
    is patched. By name when the catalogue can see the cause (a block whose
    first key is not `name`, at or before a planned fill); by class when only
    the count reveals it (the entries the walker locates and the catalogue
    lists disagree — a flow-style `- {name: X}`, a quoted or spaced key, a
    stray `- name:` list outside an entity group); and by name again for a
    planned fill whose block has no field line to anchor on."""


def _fillable(company_id: Optional[int], name: str, ids: dict[str, int]) -> bool:
    """The one fill rule: no id yet, and resolved. Spelled once so the plan and
    the oracle — which apply it to different sources on purpose — cannot
    drift on the rule itself."""
    return company_id is None and name in ids


def plan_fills(entries: list[dict], ids: dict[str, int]) -> list[Fill]:
    """The sync plan, composed from the catalogue's entity entries: every
    active entry the fill rule admits. Keyed by ordinal, not name — a curated
    copy of an entity in a second group is a different entry with its own
    verdict."""
    return [
        Fill(e["ordinal"], e["name"], ids[e["name"]], replace=e["has_id_key"])
        for e in entries
        if e["active"] and _fillable(e["zoominfo_company_id"], e["name"], ids)
    ]


def _locate_entries(lines: list[str]) -> list[tuple[int, int, str]]:
    """The entries the walker can see, structurally: every `- name:` item that
    is itself a list item at its own level, as (name_line, block_end, indent).
    Every list item opens a block — until the next item at the same indent, or
    a line that dedents out of it — so a `- name:` nested under any item (an
    `aliases:` sub-list, a `- active:`-first entry) belongs to that block and is
    not an entry. A comment-only line never ends a block: YAML ignores it
    wherever it sits, and it is re-emitted verbatim."""
    found: list[tuple[int, int, str]] = []
    i, n = 0, len(lines)
    while i < n:
        m = _ITEM_RE.match(lines[i])
        if not m:
            i += 1
            continue
        indent = m.group(1)
        j = i + 1
        while j < n:
            nxt = lines[j]
            nm = _ITEM_RE.match(nxt)
            if nm and nm.group(1) == indent:
                break
            if nxt.strip() and not nxt.lstrip().startswith("#") and not nxt.startswith(indent + " "):
                break
            j += 1
        if _NAME_RE.match(lines[i]):
            found.append((i, j, indent))
        i = j
    return found


def _refuse_unplaceable(entries: list[dict], located: int, fills: list["Fill"],
                        *, source: str) -> None:
    """The pre-conditions of ordinal counting, scoped to what the plan needs.
    The count is the ground truth; `opens_with_name` is the catalogue's
    hypothesis about which entries the locator will miss. If every entry was
    located, the hypotheses were false alarms (PyYAML flattens a `<<:` merge
    key's pairs ahead of the entry's own, so such an entry reads as not
    opening with `name:` though its text does) and the ordinals are sound. If
    exactly the suspected entries are missing, a block not opening with
    `name:` shifts every ordinal after it, so it is refused by name only when
    a planned fill sits at or after it — before that point the ordinals are
    sound, and only the blocking entries are named. Otherwise the two disagree
    for a reason the catalogue cannot see, and the refusal names the class."""
    if located == len(entries):
        return
    bad = [e for e in entries if not e["opens_with_name"]]
    if bad and located == len(entries) - len(bad):
        last_fill = max((f.ordinal for f in fills), default=-1)
        blocking = [e for e in bad if e["ordinal"] <= last_fill]
        if blocking:
            listing = ", ".join(f"'{e['name']}' in group '{e['group']}'" for e in blocking)
            raise UnplaceableEntity(
                f"{source}: {listing} — the entity block does not open with `name:`, and the "
                f"line patcher counts `- name:` lines, so it cannot place that entry or any "
                f"after it. Reorder the keys so `name` comes first."
            )
        return
    raise UnplaceableEntity(
        f"{source}: the catalogue lists {len(entries)} entity entries but the line patcher "
        f"locates {located} `- name:` items, so it cannot place fills by position. An entry "
        f"is spelled in a way the locator does not see (flow style `- {{name: X}}`, a quoted "
        f"or spaced key), or a `- name:` line sits outside an entity group (a concept "
        f"group's leftover `entities:`, a block scalar that happens to contain one). Spell "
        f"every entity as a block item opening with `- name:`, and remove stray lists."
    )


def _rewrite_id_line(line: str, company_id: int) -> str:
    """Put `company_id` where the placeholder value was, keeping the key, the
    trailing note and the line ending."""
    body = line.rstrip("\r\n")
    return _ID_KEY_RE.sub(rf"\g<1> {company_id}\g<2>", body, count=1) + line[len(body):]


def _field_anchor(block: list[str]) -> Optional[tuple[int, str]]:
    """(index, indent) of the entry's first field line — the anchor for every
    field edit. Fields precede whatever nests under them, so the first key-shaped
    line sits at the entry's own field indent. None only for a block with no
    field at all, which the plan never names (a fillable entry is active)."""
    for k, fl in enumerate(block):
        m = _KEY_RE.match(fl)
        if m:
            return k, m.group(1)
    return None


def patch_targets(targets_path: str, ids: dict[str, int]) -> tuple[str, str, list[str]]:
    """Fill `zoominfo_company_id` for every entry the plan names. Returns
    (old_text, new_text, filled_names). Raises UnplaceableEntity (nothing
    patched) for an entry the walker has no locator for; TargetsError for an
    input the catalogue rejects."""
    # newline="" keeps the file's own line endings on both sides of the patch:
    # text mode would rewrite a CRLF file as LF on the way through.
    with open(targets_path, newline="") as fh:
        old = fh.read()
    lines = old.splitlines(keepends=True)
    entries = entity_entries(old, source=targets_path)
    plan = plan_fills(entries, ids)
    if not plan:
        return old, old, []   # nothing to place, so nothing to refuse: a valid file is a no-op
    located = _locate_entries(lines)
    _refuse_unplaceable(entries, len(located), plan, source=targets_path)
    fills = {f.ordinal: f for f in plan}

    out: list[str] = []
    filled: list[str] = []
    cursor = 0
    for ordinal, (i, j, indent) in enumerate(located):
        out.extend(lines[cursor:i])
        line, block = lines[i], lines[i + 1:j]
        fill = fills.get(ordinal)
        if fill is not None:
            anchor = _field_anchor(block)
            if anchor is None:
                raise UnplaceableEntity(
                    f"{targets_path}: '{fill.name}' is planned as a fill, but the patcher "
                    f"finds no field line in its block to anchor on. Nothing was patched."
                )
            a, field_indent = anchor
            k = next((k for k, fl in enumerate(block)
                      if fl.startswith(field_indent + "zoominfo_company_id:")), None)
            if fill.replace and k is not None:
                block[k] = _rewrite_id_line(block[k], fill.company_id)
            else:
                # Insert. A placeholder the catalogue saw but no line carries — a
                # `<<:` merge default — is overridden by the entry's own key. The
                # new line takes the anchor line's ending, so a CRLF file stays CRLF.
                eol = block[a][len(block[a].rstrip("\r\n")):] or "\n"
                block.insert(a, f"{field_indent}zoominfo_company_id: {fill.company_id}{eol}")
            filled.append(fill.name)
        out.append(line)
        out.extend(block)
        cursor = j
    out.extend(lines[cursor:])

    return old, "".join(out), filled


def post_condition(current: str, proposed: str, ids: dict[str, int], filled: list[str],
                   *, source: str) -> Optional[str]:
    """Why `proposed` must not be written, or None. Both texts must load under
    the catalogue's rules, and `proposed` must load to the current targets
    with `zoominfo_company_id` set on exactly the entities the catalogue says
    are fillable — active, id-less, resolved in `ids`. The patcher's own
    report (`filled`) is checked against that plan first, so a walker that
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
                f"{sorted(set(fills))}. The walker placed a fill on the wrong block — the "
                f"count pre-condition cannot see one entry it misses offset by one stray "
                f"`- name:` line (a concept group's leftover `entities:`, a flow-style or "
                f"quoted-key entry elsewhere, a colon inside a block-scalar name). Nothing "
                f"is written until every expected fill lands.")
    for i, (want, got) in enumerate(zip_longest(expected, after)):
        if want != got:
            if want is None or got is None:
                return f"{mismatch}: {len(after)} targets loaded, {len(expected)} expected"
            changed = sorted(k for k in set(want) | set(got) if want.get(k) != got.get(k))
            return (f"{mismatch}: target #{i + 1} {want['name']!r} differs in {changed} — "
                    f"expected {[want.get(k) for k in changed]}, loaded {[got.get(k) for k in changed]}. "
                    f"A fill landed on the wrong block: look for a stray `- name:` line outside an "
                    f"entity group, a flow-style or quoted-key entry, or a colon inside a "
                    f"block-scalar name.")
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
