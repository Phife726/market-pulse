"""Pure module owning the security block — one definition.

Americhem IT flags domains it has found compromised (malicious injection
code, malware). A link to one anywhere in the digest scores the *whole email*
as malware at the recipient gateway (Proofpoint), which quarantined the
2026-09-16 run for every recipient after a visible card linked chargedevs.com
— a green run, "Delivered" at Resend, and nobody received it. The list lives
in `market_pulse_config.yaml` (`security.blocked_domains`), so the next IT
report is a config edit: no deploy, no database access. Two consumers share
this definition so they cannot drift:

- `ingestion_engine.process_candidate` drops a matching candidate at the
  gauntlet's FIRST gate (reason `blocked_domain`): never scraped, stored or
  linked, and ledgered even when an earlier run stored the URL;
- `report` rule 9 drops a stored row whose `source_url` host is blocked
  (reason `blocked_domain_stored`) — the rows stored before the domain was
  reported — so it is never a card, Watch row or appendix row; and
  `report.CitationSet.from_summary` drops a blocked `executive_sources`
  entry before numbering, so its citation marker and footer line vanish
  together (the footer never lists a source no marker references).

`host_of` is the one host spelling every domain gate compares against (the
unscrapable-domain gate reads it too). Zero I/O, no clock, no config read:
the list is handed in from the config dict the caller already holds.
"""
from typing import Iterable, Optional
from urllib.parse import urlparse


class BlockedDomainsError(ValueError):
    """The `security.blocked_domains` list is mis-shaped. Raised, never
    swallowed: a bad security list must crash the run at t=0 (like
    `targets.TargetsError`), not silently unblock a compromised domain."""


def host_of(url: object) -> str:
    """The URL's host as the domain lists spell it: case-folded, with any
    terminal dots stripped — `chargedevs.com.` is the same FQDN as
    `chargedevs.com` to resolvers, browsers and mail scanners, and must not
    slip past a suffix match. Empty for a malformed, empty or non-string URL."""
    if not isinstance(url, str) or not url:
        return ""
    try:
        host = urlparse(url).hostname or ""
    except ValueError:
        return ""
    return host.lower().rstrip(".")


def is_blocked(url: object, domains: Iterable[str]) -> bool:
    """True when `url`'s host is (a subdomain of) a domain in `domains` —
    suffix match, dot-anchored. A malformed or missing URL is never blocked
    (the later gates decide, as the unscrapable-domain gate does)."""
    host = host_of(url)
    if not host:
        return False
    return any(host == d or host.endswith("." + d) for d in domains)


def _normalize_entry(raw: object) -> str:
    if not isinstance(raw, str):
        raise BlockedDomainsError(
            f"security.blocked_domains: every entry must be a domain string, got {raw!r}"
        )
    domain = raw.strip().lower().strip(".")
    if not domain or any(ch in domain for ch in "/:@ \t?#"):
        raise BlockedDomainsError(
            f"security.blocked_domains: {raw!r} is not a bare registrable domain "
            "(spell it like `chargedevs.com` — no scheme, path or whitespace)"
        )
    if "." not in domain:
        # A single label (`com`, `.com`) is a public suffix, not a registrable
        # domain: suffix-matched, it would block every .com source and strip
        # most of the digest. (A multi-label public suffix such as `co.uk`
        # still passes — a full check needs the Public Suffix List.)
        raise BlockedDomainsError(
            f"security.blocked_domains: {raw!r} is a single label, not a registrable "
            "domain — it would block every host under that suffix"
        )
    return domain


def from_config(config: Optional[dict]) -> frozenset[str]:
    """The blocked domains out of the parsed `market_pulse_config.yaml` dict,
    normalized to the spelling `host_of` produces. Absent / null / empty list
    means nothing is blocked; any other shape raises `BlockedDomainsError`."""
    config = config or {}
    if "blocked_domains" in config:
        # The likeliest indentation slip: the key de-dented out of `security:`.
        raise BlockedDomainsError(
            "blocked_domains must sit under `security:` in market_pulse_config.yaml, "
            "not at the top level"
        )
    security = config.get("security")
    if security is None:
        return frozenset()
    if not isinstance(security, dict):
        # A present section that is not a mapping (a list one level too high,
        # a scalar) would otherwise read as "nothing blocked".
        raise BlockedDomainsError(
            f"security must be a mapping, got {type(security).__name__}"
        )
    raw = security.get("blocked_domains")
    if raw is None:
        return frozenset()
    if not isinstance(raw, (list, tuple)):
        raise BlockedDomainsError(
            f"security.blocked_domains must be a list of domains, got {type(raw).__name__}"
        )
    return frozenset(_normalize_entry(entry) for entry in raw)
