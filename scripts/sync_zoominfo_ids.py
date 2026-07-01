"""Bridge: copy resolved zoominfo_company_id values from target_metadata.yaml
into targets.yaml so ZoomInfo *news discovery* (which reads targets.yaml, not
the metadata companion) turns on for the newly enriched companies.

Comment-preserving: patches by line insertion, never round-trips the whole YAML.
Dry-run by default (prints a unified diff, writes nothing); --write applies.
Only fills entities that are (a) active, (b) currently missing an id, and
(c) present with an id in an active target_metadata.yaml record.
"""
from __future__ import annotations

import argparse
import difflib
import re
import sys

import yaml


def load_resolved_ids(metadata_path: str) -> dict[str, int]:
    """Return {target_key: zoominfo_company_id} for active metadata records that
    carry an id. Missing file or malformed records yield an empty mapping."""
    with open(metadata_path) as fh:
        data = yaml.safe_load(fh) or {}
    out: dict[str, int] = {}
    for key, rec in (data.get("targets") or {}).items():
        if not isinstance(rec, dict):
            continue
        cid = rec.get("zoominfo_company_id")
        status = rec.get("metadata_record_status", "active")
        if cid and status == "active":
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


def run(*, targets_path: str, metadata_path: str, write: bool) -> int:
    ids = load_resolved_ids(metadata_path)
    old, new, filled = patch_targets(targets_path, ids)

    if write:
        with open(targets_path, "w") as fh:
            fh.write(new)
        print(f"Wrote {len(filled)} id(s) into {targets_path}: "
              f"{', '.join(filled) or '(none)'}")
        return 0

    sys.stdout.writelines(difflib.unified_diff(
        old.splitlines(keepends=True), new.splitlines(keepends=True),
        fromfile=targets_path + " (current)", tofile=targets_path + " (proposed)",
    ))
    print(f"\n# would fill {len(filled)} id(s): {', '.join(filled) or '(none)'}",
          file=sys.stderr)
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
