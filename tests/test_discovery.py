"""
Tests for the discovery-provider seam (discovery.py).

Discovery is exercised by injecting providers, not by patching requests: the
DiscoveryProvider Protocol lets a FakeDiscoveryProvider stand in for Serper /
ZoomInfo at the consumer, and each production adapter is tested directly for
its own eligibility / discovery / gate behaviour.
"""
from functools import partial

import pytest

import discovery
import relevance_gate
import zoominfo_client
from discovery import (
    SerperProvider,
    ZoomInfoProvider,
    FakeDiscoveryProvider,
)
from tests.conftest import stub_target


#: This section's default target: a ZoomInfo-eligible entity (an id mapped,
#: news on) as `load_targets` maps it.
# `name` is bound as a KEYWORD so a call can override it (`_target(name=...)`).
_target = partial(
    stub_target, name="Magna International", category="customers", zoominfo_company_id=12345678)


# ===========================================================================
# SerperProvider
# ===========================================================================

def test_serper_provider_name_and_always_eligible():
    p = SerperProvider()
    assert p.name == "serper"
    assert p.eligible(_target()) is True
    # Even a concept target with no company id is Serper-eligible.
    assert p.eligible(stub_target("Anything", search_mode="concept")) is True


def test_serper_provider_discover_builds_neutral_shape(monkeypatch):
    monkeypatch.setattr(
        discovery, "discover_urls",
        lambda q, lb, n: [("https://news.com/a", "Serper Title")],
    )
    result = SerperProvider().discover(_target())
    assert len(result) == 1
    c = result[0]
    assert c["url"] == "https://news.com/a"
    assert c["title"] == "Serper Title"
    assert c["provider"] == "serper"
    assert c["zoominfo_company_id"] is None
    assert c["categories"] == []


def test_serper_provider_never_gates():
    assert SerperProvider().gate({"title": "anything"}, _target()) is None


def test_discover_urls_truncates_to_results_per_entity(monkeypatch):
    """Serper's news endpoint returns pages of 10 regardless of the `num`
    param — the client must enforce results_per_entity itself."""
    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"news": [
                {"link": f"https://example.com/article-{i}", "title": f"Headline {i}"}
                for i in range(10)
            ]}

    monkeypatch.setenv("SERPER_API_KEY", "test_key")
    monkeypatch.setattr("discovery.requests.post", lambda *a, **k: FakeResponse())

    results = discovery.discover_urls("test query", 24, 2)

    assert len(results) == 2
    assert results[0] == ("https://example.com/article-0", "Headline 0")
    assert results[1] == ("https://example.com/article-1", "Headline 1")


# ===========================================================================
# ZoomInfoProvider — eligibility
# ===========================================================================

def test_zoominfo_ineligible_when_flag_off(monkeypatch):
    monkeypatch.setenv("ZOOMINFO_NEWS_ENABLED", "false")
    assert ZoomInfoProvider().eligible(_target()) is False


def test_zoominfo_eligible_when_flag_on_with_company_id(monkeypatch):
    monkeypatch.setenv("ZOOMINFO_NEWS_ENABLED", "true")
    assert ZoomInfoProvider().eligible(_target()) is True


@pytest.mark.parametrize("company_id", [None, 0, "357374413"])
def test_zoominfo_ineligible_without_company_id(monkeypatch, company_id):
    """"Mapped" is the catalogue's `is_company_id`, so a hand-built target
    carrying a placeholder or a quoted id is ineligible too — the seam and
    the loader agree on the boundary."""
    monkeypatch.setenv("ZOOMINFO_NEWS_ENABLED", "true")
    assert ZoomInfoProvider().eligible(_target(zoominfo_company_id=company_id)) is False


def test_zoominfo_ineligible_when_news_disabled(monkeypatch):
    monkeypatch.setenv("ZOOMINFO_NEWS_ENABLED", "true")
    assert ZoomInfoProvider().eligible(_target(zoominfo_news=False)) is False


# ===========================================================================
# ZoomInfoProvider — discover (delegates to the client)
# ===========================================================================

def test_zoominfo_discover_delegates_to_client(monkeypatch):
    monkeypatch.setenv("ZOOMINFO_BEARER_TOKEN", "test-token")
    fake = [{"url": "https://news.example.com/x", "provider": "zoominfo",
             "zoominfo_company_id": 12345678}]
    captured = {}

    def _fake_client(*, zoominfo_company_id, publishing_date_start, page_size):
        captured["company_id"] = zoominfo_company_id
        captured["page_size"] = page_size
        return fake

    monkeypatch.setattr(zoominfo_client, "discover_company_news", _fake_client)
    result = ZoomInfoProvider().discover(_target())
    assert result == fake
    assert captured["company_id"] == 12345678
    assert captured["page_size"] == 5  # ZOOMINFO_NEWS_PER_COMPANY default


# ===========================================================================
# ZoomInfoProvider — gate (relevance false-positive suppression)
# ===========================================================================

_GATE_META = {
    "version": 1,
    "targets": {
        "Magna International": {
            "metadata_record_status": "active",
            "canonical_name": "Magna",
            "company_identity_terms": ["Magna"],
            "manual_aliases": [],
            "exclude_terms": ["casino", "real-time payments"],
        }
    },
}


def _write_meta(tmp_path, monkeypatch):
    import yaml
    (tmp_path / "target_metadata.yaml").write_text(yaml.safe_dump(_GATE_META))
    monkeypatch.chdir(tmp_path)


def test_zoominfo_gate_none_when_disabled(tmp_path, monkeypatch):
    monkeypatch.delenv("ZOOMINFO_RELEVANCE_GATE_ENABLED", raising=False)
    _write_meta(tmp_path, monkeypatch)
    d = ZoomInfoProvider().gate({"title": "Casino night"}, _target())
    assert d is None


def test_zoominfo_gate_none_when_no_record(tmp_path, monkeypatch):
    monkeypatch.setenv("ZOOMINFO_RELEVANCE_GATE_ENABLED", "true")
    _write_meta(tmp_path, monkeypatch)
    d = ZoomInfoProvider().gate({"title": "Casino night"}, _target(name="Unknown Co"))
    assert d is None


def test_zoominfo_gate_ignores_non_active_record(tmp_path, monkeypatch):
    import yaml
    meta = {"version": 1, "targets": {"Magna International": dict(
        _GATE_META["targets"]["Magna International"], metadata_record_status="orphaned")}}
    (tmp_path / "target_metadata.yaml").write_text(yaml.safe_dump(meta))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ZOOMINFO_RELEVANCE_GATE_ENABLED", "true")
    d = ZoomInfoProvider().gate({"title": "Casino night"}, _target())
    assert d is None


def test_zoominfo_gate_drops_exclude_without_rescue(tmp_path, monkeypatch):
    monkeypatch.setenv("ZOOMINFO_RELEVANCE_GATE_ENABLED", "true")
    _write_meta(tmp_path, monkeypatch)
    d = ZoomInfoProvider().gate({"title": "Casino night downtown"}, _target())
    assert d is not None and d.drop is True
    assert d.matched_exclude == "casino"


def test_zoominfo_gate_keeps_with_identity_rescue(tmp_path, monkeypatch):
    monkeypatch.setenv("ZOOMINFO_RELEVANCE_GATE_ENABLED", "true")
    _write_meta(tmp_path, monkeypatch)
    d = ZoomInfoProvider().gate({"title": "Magna opens near a casino"}, _target())
    assert d is not None and d.drop is False


def test_zoominfo_gate_loads_metadata_once(tmp_path, monkeypatch):
    """The companion file is read once per provider instance, not per candidate."""
    monkeypatch.setenv("ZOOMINFO_RELEVANCE_GATE_ENABLED", "true")
    _write_meta(tmp_path, monkeypatch)
    calls = {"n": 0}
    real = relevance_gate.load_target_metadata

    def _counting(path="target_metadata.yaml"):
        calls["n"] += 1
        return real(path)

    monkeypatch.setattr(relevance_gate, "load_target_metadata", _counting)
    p = ZoomInfoProvider()
    p.gate({"title": "a"}, _target())
    p.gate({"title": "b"}, _target())
    assert calls["n"] == 1


# ===========================================================================
# FakeDiscoveryProvider + provider registry
# ===========================================================================

def test_fake_provider_returns_scripted_candidates_and_records_calls():
    cands = [{"url": "https://x/a", "provider": "fake"}]
    p = FakeDiscoveryProvider("fake", cands)
    assert p.eligible(_target()) is True
    assert p.discover(_target()) == cands
    assert len(p.discover_calls) == 1


def test_fake_provider_can_be_ineligible_and_can_raise():
    p = FakeDiscoveryProvider("fake", eligible=False)
    assert p.eligible(_target()) is False
    boom = FakeDiscoveryProvider("fake", discover_error=RuntimeError("down"))
    with pytest.raises(RuntimeError):
        boom.discover(_target())


def test_discovery_providers_registry_order_and_singleton():
    providers = discovery._discovery_providers()
    assert [p.name for p in providers] == ["serper", "zoominfo"]
    # Cached singleton — same list instance across calls.
    assert discovery._discovery_providers() is providers
    discovery._reset_discovery_providers()
    assert discovery._discovery_providers() is not providers


# ── Serper time window ────────────────────────────────────────────────────────
# Google stopped honoring the hour-count form: on 2026-08-27 a live probe
# (scripts/probe_serper.py) showed `tbs=qdr:h24` returning exactly the
# past-hour `qdr:h` results for five large companies while the documented
# past-day `qdr:d` returned 2–9 each — the 2026-08-26 yield collapse (#63).
# The window must therefore be expressed in days, never hours.

@pytest.mark.parametrize("hours, expected", [
    (24, "qdr:d"),
    (48, "qdr:d2"),
    (72, "qdr:d3"),
    (1, "qdr:d"),    # sub-day lookbacks round *up* to a day — never an hour form
    (25, "qdr:d2"),
])
def test_serper_time_filter_is_expressed_in_whole_days(hours, expected):
    assert discovery.serper_time_filter(hours) == expected


def test_discover_urls_sends_a_day_window_not_an_hour_count(monkeypatch):
    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"news": []}

    def fake_post(url, json, headers, timeout):
        captured.update(json)
        return FakeResponse()

    monkeypatch.setenv("SERPER_API_KEY", "test_key")
    monkeypatch.setattr("discovery.requests.post", fake_post)

    discovery.discover_urls("test query", 24, 2)

    assert captured["tbs"] == "qdr:d"
    assert not captured["tbs"].startswith("qdr:h")
