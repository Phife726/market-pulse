"""test_blocked_domains.py — the security block, one definition.

`blocked_domains.py` is the one definition of the domains Americhem IT has
flagged as compromised: `from_config` reads the list out of the parsed
`market_pulse_config.yaml` dict (normalizing spellings, failing fast on a
shape that would silently unblock), `is_blocked` is the suffix predicate the
ingestion gauntlet's first gate and delivery rule 9 share, and `host_of` is
the one host spelling every domain gate compares against.
"""
import pytest

from blocked_domains import BlockedDomainsError, from_config, host_of, is_blocked

CHARGEDEVS = frozenset({"chargedevs.com"})


@pytest.mark.parametrize("url,expected", [
    ("https://chargedevs.com/newswire/lanxess-opens-a-battery-laboratory/", True),
    ("https://www.chargedevs.com/some-story", True),          # subdomains too
    ("https://CHARGEDEVS.com/x", True),                        # host is case-folded
    ("https://chargedevs.com./newswire/x", True),              # trailing-dot FQDN resolves the same
    ("https://www.chargedevs.com../x", True),                  # any number of terminal dots
    ("https://notchargedevs.com/article", False),              # suffix must be dot-anchored
    ("https://www.reuters.com/markets/some-article/", False),
    ("not a url", False),                                      # malformed → let the later gates decide
    ("", False),
    (None, False),                                             # a row with no source_url
])
def test_is_blocked(url, expected):
    assert is_blocked(url, CHARGEDEVS) is expected


def test_is_blocked_is_false_when_nothing_is_blocked():
    assert is_blocked("https://chargedevs.com/x", frozenset()) is False


@pytest.mark.parametrize("url,host", [
    ("https://WWW.Example.com./path", "www.example.com"),
    ("https://example.com../", "example.com"),
    ("not a url", ""),
    ("", ""),
])
def test_host_of_folds_case_and_strips_terminal_dots(url, host):
    assert host_of(url) == host


# ---------------------------------------------------------------------------
# from_config — the one reader of security.blocked_domains
# ---------------------------------------------------------------------------


def test_from_config_reads_the_security_list():
    cfg = {"security": {"blocked_domains": ["chargedevs.com", "evil.example"]}}
    assert from_config(cfg) == frozenset({"chargedevs.com", "evil.example"})


@pytest.mark.parametrize("cfg", [
    {},
    {"security": None},
    {"security": {}},
    {"security": {"blocked_domains": None}},
    {"security": {"blocked_domains": []}},
])
def test_from_config_is_empty_when_the_list_is_absent(cfg):
    assert from_config(cfg) == frozenset()


def test_from_config_normalizes_each_entry_to_the_hosts_spelling():
    """Whatever an operator types — capitals, surrounding whitespace, a
    leading or trailing dot — is stored the way `host_of` spells a host, so
    the suffix match cannot miss on spelling."""
    cfg = {"security": {"blocked_domains": ["  ChargedEVs.COM. ", ".evil.example"]}}
    assert from_config(cfg) == frozenset({"chargedevs.com", "evil.example"})


@pytest.mark.parametrize("bad", [
    "chargedevs.com",                       # a scalar where the list belongs
    ["https://chargedevs.com/"],            # a URL pasted instead of a domain
    ["chargedevs.com/newswire"],            # a path
    ["charged evs.com"],                    # whitespace inside
    [""],                                   # an empty entry
    [None],                                 # a null entry
    [42],                                   # a non-string entry
])
def test_from_config_fails_fast_on_a_shape_that_would_silently_unblock(bad):
    """The failure direction is inverted from the report levers (which warn
    and fall back): a mis-shaped security list must crash the run at t=0, not
    quietly let a compromised domain through to the email."""
    with pytest.raises(BlockedDomainsError):
        from_config({"security": {"blocked_domains": bad}})


def test_blocked_domains_error_is_a_value_error():
    assert issubclass(BlockedDomainsError, ValueError)
