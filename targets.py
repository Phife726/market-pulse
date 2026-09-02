"""The targets catalogue — the one parser of `targets.yaml` (CONTEXT.md:
**Target**). Shape errors raise `TargetsError`; policy (tier order, macro
groups last) is pinned in `tests/test_targets.py`, not validated here.
"""
import logging
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

# Moody's platform-level source identifiers that appeared in targets.yaml's
# exclude_any lists (carried over from the News Edge subscription). They are
# not search terms; build_query drops them instead of emitting -"term".
_MOODY_INTERNAL_EXCLUDES: frozenset[str] = frozenset({
    "source set 238658",
    "PR wires",
    "Targeted News Search",
    "US Federal News",
    "specific Asia PR feed",
    "specific processing feeds",
    "Financial Times feeds",
    "financial markups",
})

#: The keys every entity target carries beyond the common ones — the ZoomInfo
#: enrichment fields and the resolution hints — with their defaults when the
#: file has none. `tests/conftest.stub_target` mirrors this (pinned by a test).
ENTITY_OPTIONAL_KEYS: dict = {
    "zoominfo_company_id": None,
    "zoominfo_news": True,
    "domain": None,
    "hq_country": None,
    "hq_state": None,
}


class TargetsError(RuntimeError):
    """The control file has a shape `load_targets` cannot map — the message
    names the group (and entity) so the operator can find the line. Uncaught
    on purpose, like `config.MissingEnvironmentError`: the traceback at t=0
    is the alarm."""


def build_query(
    *,
    name: Optional[str] = None,
    include_any: Optional[list[str]] = None,
    include_all: Optional[list[str]] = None,
    exclude_any: Optional[list[str]] = None,
) -> str:
    """Build a Serper.dev search query string from group field semantics.

    The primary term is either an entity ``name`` (quoted) or a concept
    group's ``include_any`` terms (ORed) — exactly one must be given.
    ``include_all`` terms are ANDed into every query. ``exclude_any`` terms
    become ``-"term"`` operators; entries in ``_MOODY_INTERNAL_EXCLUDES``
    (Moody's platform-level source identifiers) are silently dropped.
    """
    if (name is None) == (not include_any):
        raise ValueError("build_query needs exactly one of name= / include_any=")
    parts: list[str] = []
    if name is not None:
        parts.append(f'"{name}"')
    else:
        parts.append("(" + " OR ".join(f'"{t}"' for t in include_any) + ")")

    for term in (include_all or []):
        parts.append(f'"{term}"')

    for term in (exclude_any or []):
        if term not in _MOODY_INTERNAL_EXCLUDES:
            parts.append(f'-"{term}"')

    return " ".join(parts)


def _list_field(owner: str, cfg: dict, key: str) -> list:
    """`cfg[key]` as a list; an ABSENT key -> []. A scalar where a list
    belongs is a shape error, not a one-element list — and so is a present
    key with no value (a bare `entities:` line is a null, and treating it as
    empty would silently drop the whole group's coverage)."""
    if key not in cfg:
        return []
    value = cfg[key]
    if not isinstance(value, list):
        raise TargetsError(
            f"{owner}: '{key}' must be a list, got {type(value).__name__}"
            + (" (a bare key with no value? give it a list or remove the line)" if value is None else "")
        )
    return value


def _int_setting(owner: str, cfg: dict, key: str, default: int) -> int:
    """A numeric setting the loop slices/compares with — a quoted "2" would
    otherwise fail at the first billed call, not at load."""
    value = cfg.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TargetsError(f"{owner}: '{key}' must be an integer, got {value!r}")
    return value


def _bool_setting(owner: str, cfg: dict, key: str, default: bool) -> bool:
    """A switch that gates spend — a quoted "false" is truthy and would keep
    a paused entity running."""
    value = cfg.get(key, default)
    if not isinstance(value, bool):
        raise TargetsError(f"{owner}: '{key}' must be true or false, got {value!r}")
    return value


def load_targets(config_path: str) -> list[dict]:
    """`parse_targets` over the file at `config_path` — the catalogue's one read."""
    with open(config_path, "r") as fh:
        return parse_targets(fh.read(), source=config_path)


def _load_document(text: str, *, source: str) -> dict:
    """The `targets.yaml` document as a mapping of groups, or TargetsError.
    A document that is not even YAML is the first shape error, not a parser
    traceback."""
    try:
        config = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise TargetsError(f"{source}: not valid YAML — {exc}") from exc
    if not isinstance(config, dict):
        raise TargetsError(f"{source}: expected a mapping of groups, got {type(config).__name__}")
    return config


def parse_targets(text: str, *, source: str) -> list[dict]:
    """Parse `text` — the `targets.yaml` document — into the active search targets.

    Two search modes:
    - ``entity``: one Serper query per active company name under ``entities:``.
    - ``concept``: one combined OR query for the whole group (``active: true``
      required at group level).

    Returns:
        List of target dicts, each carrying ``name``, ``category`` (the group
        key), ``search_mode``, ``query`` (the pre-built Serper query string)
        and the discovery settings; entity targets also carry
        ``ENTITY_OPTIONAL_KEYS`` — the ZoomInfo id/flag and the resolution
        hints ``domain`` / ``hq_country`` / ``hq_state`` (defaults when the
        file has none).

    Raises:
        TargetsError: on a shape the mapping cannot represent — a non-mapping
        document, a group or ``discovery`` block that is not a mapping, an
        entity without a string ``name``, an unknown ``search_mode``, a
        concept group without a non-empty ``include_any``, a scalar where a
        list belongs, a non-integer discovery setting, a non-boolean switch,
        or a file that yields no active target at all. The whole file is
        validated, inactive entries included: a paused group must not rot
        until someone re-enables it. A document that is not even YAML is the
        first shape error, not a parser traceback. `source` names the text in
        errors and the log; `load_targets` is this over a file.
    """
    targets, _entries = _walk(_load_document(text, source=source), source=source)
    logger.info("Loaded %d active targets from %s", len(targets), source)
    return targets


def entity_entries(text: str, *, source: str) -> list[dict]:
    """Every **entity entry** in `text`, in file order, active or not — what the
    control file *lists*, as against a target, which is what the loop *runs*.
    An inactive entity is an entry but never a target.

    Read-only knowledge about the file; decides nothing. Each entry carries
    ``ordinal`` (its position among all entity entries — the key a line-level
    editor can count to), ``group``, ``name``, ``active`` (as the catalogue
    resolves it), ``zoominfo_company_id`` (the value as written: an int, or
    None), ``has_id_key`` (True for a ``null`` placeholder, False when the key
    is absent — a text editor must replace one and insert the other).

    The same single walk as `parse_targets`, so the whole-file verdict comes
    with it: a file the loader rejects is rejected here, with the loader's own
    message, and a listing is only ever of a file the loader accepts.
    """
    _targets, entries = _walk(_load_document(text, source=source), source=source)
    return entries


def _walk(config: dict, *, source: str) -> tuple[list[dict], list[dict]]:
    """The one walk over a loaded document: validates everything and returns
    (active targets, every entity entry). Validation order is the file's:
    group shape, the group's list fields, then its entities or its concept
    terms, then its mode — so the first error a reader sees is the same
    whichever face asked."""
    discovery = config.get("discovery")
    if discovery is not None and not isinstance(discovery, dict):
        raise TargetsError(f"'discovery' must be a mapping, got {type(discovery).__name__}")
    discovery = discovery or {}
    results_per_entity = _int_setting("discovery", discovery, "results_per_entity", 2)
    lookback_hours = _int_setting("discovery", discovery, "lookback_hours", 24)
    min_article_length = _int_setting("discovery", discovery, "min_article_length", 500)

    targets: list[dict] = []
    entries: list[dict] = []
    for group_name, group_cfg in config.items():
        if group_name == "discovery":
            continue
        if isinstance(group_cfg, (str, int, float, bool)):
            continue  # a top-level note or version stamp, not a group
        if not isinstance(group_cfg, dict):
            raise TargetsError(
                f"group '{group_name}': expected a mapping (search_mode, entities/include_any), "
                f"got {type(group_cfg).__name__}"
            )
        owner = f"group '{group_name}'"
        mode: str = group_cfg.get("search_mode", "entity")
        include_all = _list_field(owner, group_cfg, "include_all")
        exclude_any = _list_field(owner, group_cfg, "exclude_any")

        if mode == "entity":
            for position, entity in enumerate(_list_field(owner, group_cfg, "entities")):
                entity_owner = f"{owner}, entity #{position + 1}"
                if not isinstance(entity, dict) or not isinstance(entity.get("name"), str) or not entity["name"]:
                    raise TargetsError(f"{entity_owner}: needs a non-empty string 'name'")
                active = _bool_setting(entity_owner, entity, "active", False)
                zoominfo_news = _bool_setting(entity_owner, entity, "zoominfo_news", True)
                entries.append({
                    "ordinal": len(entries),
                    "group": group_name,
                    "name": entity["name"],
                    "active": active,
                    "zoominfo_company_id": entity.get("zoominfo_company_id"),
                    "has_id_key": "zoominfo_company_id" in entity,
                })
                if not active:
                    continue
                # Optional ZoomInfo enrichment: news defaults on when an id is
                # mapped, off when no id exists. Concept groups never get these.
                targets.append({
                    "name": entity["name"],
                    "category": group_name,
                    "search_mode": "entity",
                    "query": build_query(
                        name=entity["name"], include_all=include_all, exclude_any=exclude_any),
                    "results_per_entity": results_per_entity,
                    "lookback_hours": lookback_hours,
                    "min_article_length": min_article_length,
                    "zoominfo_company_id": entity.get("zoominfo_company_id"),
                    "zoominfo_news": zoominfo_news,
                    # Resolution hints for the enrichment utility (never queried).
                    "domain": entity.get("domain"),
                    "hq_country": entity.get("hq_country"),
                    "hq_state": entity.get("hq_state"),
                })

        elif mode == "concept":
            include_any = _list_field(owner, group_cfg, "include_any")
            if not include_any:
                raise TargetsError(f"{owner}: a concept group needs a non-empty 'include_any'")
            # Concept groups may declare their own results_per_entity to raise
            # discovery volume for priority segments; absent one, inherit the
            # global discovery value. Concept-only — a stray override on an
            # entity group is ignored (raising entity volume is not intended,
            # and macro groups stay at the global value so the tail reserve's
            # concept demand — RunBudget.concept_demand_ahead — is not inflated).
            group_results = _int_setting(owner, group_cfg, "results_per_entity", results_per_entity)
            if not _bool_setting(owner, group_cfg, "active", False):
                continue
            targets.append({
                "name": group_name,
                "category": group_name,
                "search_mode": "concept",
                "query": build_query(
                    include_any=include_any, include_all=include_all, exclude_any=exclude_any),
                "results_per_entity": group_results,
                "lookback_hours": lookback_hours,
                "min_article_length": min_article_length,
            })

        else:
            raise TargetsError(f"{owner}: unknown search_mode '{mode}' (expected 'entity' or 'concept')")

    if not targets:
        raise TargetsError(f"{source}: no active targets — nothing for a run to do")
    return targets, entries
