"""Bridge: copy resolved zoominfo_company_id values from target_metadata.yaml
into targets.yaml so ZoomInfo *news discovery* (which reads targets.yaml, not
the metadata companion) turns on for the newly enriched companies.

Comment-preserving: patches by line insertion, never round-trips the whole YAML.
Dry-run by default (prints a unified diff, writes nothing); --write applies.
Both modes post-condition the result through `targets.parse_targets`: the input
must load, and the output must load to the input's targets plus exactly the
fills the catalogue expects — otherwise exit 1, nothing written.
Only fills entities that are (a) active, (b) currently missing an id, and
(c) present with an id in an active target_metadata.yaml record whose ZoomInfo
status is approved/verified.

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
import os
import re
import sys
from itertools import zip_longest
from typing import Optional

import yaml

# When run as `python scripts/sync_zoominfo_ids.py`, sys.path[0] is scripts/,
# not the repo root, so the catalogue must be put on the path explicitly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from targets import TargetsError, parse_targets  # noqa: E402

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


# `    - name: Foo` (entity list item). Captures leading indent + the name.
_NAME_RE = re.compile(r"^(\s*)-\s+name:\s*(.+?)\s*$")


def patch_targets(targets_path: str, ids: dict[str, int]) -> tuple[str, str, list[str]]:
    """Insert `zoominfo_company_id` under each active, id-less entity for which
    *ids* has a value. Returns (old_text, new_text, filled_names)."""
    with open(targets_path) as fh:
        lines = fh.readlines()

    out: list[str] = []
    filled: list[str] = []
    i, n = 0, len(lines)
    while i < n:
        line = lines[i]
        m = _NAME_RE.match(line)
        if not m:
            out.append(line)
            i += 1
            continue

        indent, name = m.group(1), m.group(2)
        field_indent = indent + "  "
        # Walk this entity's field block: until the next `- name:` at the same
        # indent, or a line that dedents out of the block.
        j = i + 1
        has_id = active = False
        while j < n:
            nxt = lines[j]
            nm = _NAME_RE.match(nxt)
            if nm and nm.group(1) == indent:
                break
            if nxt.strip() and not nxt.startswith(field_indent):
                break
            if re.match(rf"^{field_indent}zoominfo_company_id:", nxt):
                has_id = True
            if re.match(rf"^{field_indent}active:\s*true\b", nxt):
                active = True
            j += 1

        out.append(line)  # the `- name:` line itself
        if active and not has_id and name in ids:
            out.append(f"{field_indent}zoominfo_company_id: {ids[name]}\n")
            filled.append(name)
        out.extend(lines[i + 1:j])
        i = j

    return "".join(lines), "".join(out), filled


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
        return f"{source} is already rejected by the loader — fix it before syncing: {exc}"
    try:
        # The idempotent re-run proposes the text it was given: one parse.
        after = before if proposed == current else parse_targets(proposed, source=proposed_source)
    except TargetsError as exc:
        return (f"{proposed_source} is rejected by the loader — a bug in this script's "
                f"line patcher, not in the input: {exc}")

    fills: list[str] = []
    expected: list[dict] = []
    for t in before:
        if t["search_mode"] == "entity" and t["zoominfo_company_id"] is None and t["name"] in ids:
            fills.append(t["name"])
            t = {**t, "zoominfo_company_id": ids[t["name"]]}
        expected.append(t)
    mismatch = f"{proposed_source} does not mean what the patcher reported"
    if set(filled) != set(fills):
        return (f"{mismatch}: it filled {sorted(set(filled))}, the catalogue expected "
                f"{sorted(set(fills))} — an entity the line walker cannot match (quoted name, "
                f"trailing comment, `active: True`, a `zoominfo_company_id: null` placeholder)? "
                f"Nothing is written until every expected fill lands.")
    for i, (want, got) in enumerate(zip_longest(expected, after)):
        if want != got:
            if want is None or got is None:
                return f"{mismatch}: {len(after)} targets loaded, {len(expected)} expected"
            changed = sorted(k for k in set(want) | set(got) if want.get(k) != got.get(k))
            return (f"{mismatch}: target #{i + 1} {want['name']!r} differs in {changed} — "
                    f"expected {[want.get(k) for k in changed]}, loaded {[got.get(k) for k in changed]}")
    return None


def run(*, targets_path: str, metadata_path: str, write: bool) -> int:
    ids = load_resolved_ids(metadata_path)
    old, new, filled = patch_targets(targets_path, ids)
    problem = post_condition(old, new, ids, filled, source=targets_path)

    if not write:
        sys.stdout.writelines(difflib.unified_diff(
            old.splitlines(keepends=True), new.splitlines(keepends=True),
            fromfile=targets_path + " (current)", tofile=targets_path + " (proposed)",
        ))
    if problem:
        print(f"\n{problem}\nNothing written.", file=sys.stderr)
        return 1
    if write and filled:   # the post-condition guarantees `new` is `old` plus the fills
        with open(targets_path, "w") as fh:
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
