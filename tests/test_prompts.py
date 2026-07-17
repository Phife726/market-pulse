"""Direct tests for prompts.py — the pure prompt-assembly module.

The prompt-contract tests live here: no FakeLLM, no repo fake, no patching —
the builders are pure functions over dicts. The engine tests keep one thin
wiring check each (e.g. the non-English-body regression in test_pipeline.py)
proving the assembled spec actually crosses the LLM seam.
"""
import prompts
from prompts import MacroPrompt, PromptSpec

_ENGLISH_ANCHORS = ("business English", "regardless of the source article")


def _article(headline, *, score=5, url="https://example.com/a", url_hash="h",
             segment="Healthcare", category="competitors", impact="Some impact."):
    return {
        "headline": headline,
        "americhem_impact_score": score,
        "source_url": url,
        "url_hash": url_hash,
        "commercial_segment": segment,
        "category": category,
        "americhem_impact": impact,
        "sentiment_tag": "Neutral",
        "entities_mentioned": ["Acme"],
    }


def _insight_spec(cfg=None) -> PromptSpec:
    return prompts.insight_prompt(
        cfg or {},
        article_text="Body text.",
        source_url="https://news.com/article",
        trigger_entity="Dow",
        category="competitors",
    )


# ---------------------------------------------------------------------------
# English-output rule — single source of truth, present in every prompt
# ---------------------------------------------------------------------------

def test_english_rule_single_source_and_present_in_all_three_prompts():
    for anchor in _ENGLISH_ANCHORS:
        assert anchor in prompts.ENGLISH_OUTPUT_RULE
    specs = [
        _insight_spec(),
        prompts.macro_prompt([_article("A")]),
        prompts.thematic_prompt({"Healthcare": [_article("A"), _article("B")]}),
    ]
    for spec in specs:
        assert prompts.ENGLISH_OUTPUT_RULE in spec.system


# ---------------------------------------------------------------------------
# Spec plumbing — the LLM-seam keyword contract, temperatures, fingerprint
# ---------------------------------------------------------------------------

def test_kwargs_matches_llm_seam_keywords():
    kwargs = _insight_spec().kwargs()
    assert set(kwargs) == {"system", "user", "temperature", "context"}


def test_each_prompt_ships_its_temperature_and_context_label():
    ins = _insight_spec()
    assert ins.temperature == 0.2
    assert ins.context == "entity 'Dow'"

    mac = prompts.macro_prompt([_article("A")])
    assert mac.temperature == 0.3
    assert mac.context == "macro summary"

    the = prompts.thematic_prompt({"Healthcare": [_article("A"), _article("B")]})
    assert the.temperature is None
    assert the.context == "thematic synthesis"


def test_system_fingerprint_is_stable_and_wording_sensitive():
    a, b = _insight_spec(), _insight_spec()
    assert a.system_fingerprint == b.system_fingerprint
    assert len(a.system_fingerprint) == 12
    other = prompts.macro_prompt([_article("A")])
    assert a.system_fingerprint != other.system_fingerprint


# ---------------------------------------------------------------------------
# Insight prompt — RULE 4/5 config injection, fallbacks, brace safety, URL echo
# ---------------------------------------------------------------------------

def test_insight_rule4_injects_labels_and_descriptions():
    """RULE 4 must include the configured segment labels and their full
    descriptions — editing the yaml changes how the LLM classifies."""
    cfg = {
        "commercial_segments": {
            "healthcare": {"label": "Healthcare", "description": "Med devices."},
            "fibers": {"label": "Fibers", "description": "Synthetic fiber chains."},
        }
    }
    system = _insight_spec(cfg).system
    assert "RULE 4 — COMMERCIAL SEGMENT" in system
    assert "Healthcare: Med devices." in system
    assert "Fibers: Synthetic fiber chains." in system


def test_insight_rule5_injects_labels_and_descriptions():
    cfg = {
        "signal_types": {
            "competitive": {"label": "Competitive", "description": "Comp moves."},
            "regulatory": {"label": "Regulatory", "description": "Gov actions."},
        }
    }
    system = _insight_spec(cfg).system
    assert "RULE 5 — SIGNAL TYPE" in system
    assert "Competitive: Comp moves." in system
    assert "Regulatory: Gov actions." in system


def test_insight_prompt_includes_both_rules_with_descriptions():
    cfg = {
        "commercial_segments": {
            "engineered_resins": {
                "label": "Engineered Resins",
                "description": "High-performance compounds.",
            },
        },
        "signal_types": {
            "supply_chain": {
                "label": "Supply Chain",
                "description": "Resin pricing, force majeure.",
            },
        },
    }
    system = _insight_spec(cfg).system
    assert "RULE 4 — COMMERCIAL SEGMENT" in system
    assert "RULE 5 — SIGNAL TYPE" in system
    assert "Engineered Resins" in system
    assert "High-performance compounds." in system
    assert "Supply Chain" in system


def test_insight_prompt_falls_back_to_canned_lists_on_empty_config():
    system = _insight_spec({}).system
    assert "Enterprise / Cross-Segment" in system      # fallback segment list
    assert "Supply Chain | Technology | Macro" in system  # fallback signal list


def test_insight_prompt_preserves_literal_json_braces():
    """The DISCARD sentinel and the output schema carry literal JSON braces —
    a .format() regression in assembly cannot pass this test."""
    system = _insight_spec().system
    assert '{"americhem_impact": "DISCARD"}' in system
    assert '"americhem_impact_score": <integer 1-10 per Rule 3>' in system


def test_insight_user_prompt_injects_source_url_verbatim():
    """The canonical URL is injected so the model echoes it deterministically —
    the deduplication invariant."""
    spec = _insight_spec()
    assert "Source URL: https://news.com/article" in spec.user
    assert "Body text." in spec.user


# ---------------------------------------------------------------------------
# Macro prompt — citation contract, constants drift guard, ranking, capping
# ---------------------------------------------------------------------------

def test_macro_digest_ids_match_source_pack_ids():
    """Every [n] marker in the digest has a pack entry with id == n and vice
    versa — the contract the citation validator relies on."""
    articles = [_article(f"H{i}", score=9 - i, url_hash=f"h{i}") for i in range(5)]
    mp = prompts.macro_prompt(articles)
    assert isinstance(mp, MacroPrompt)
    for s in mp.source_pack:
        assert f"[{s['id']}] " in mp.user
        assert s["headline"] in mp.user
    assert [s["id"] for s in mp.source_pack] == list(range(1, 6))


def test_macro_system_promises_exactly_what_the_validators_enforce():
    """Drift guard: every macro condition and every bullet label appears
    literally in the system prompt — pins the f-string wiring that the
    generate_macro_summary validators depend on."""
    system = prompts.macro_prompt([_article("A")]).system
    for condition in prompts.VALID_MACRO_CONDITIONS:
        assert condition in system
    for label in prompts.EXEC_BULLET_LABELS:
        assert f'"label": "{label}"' in system
    assert "No action required." in system              # Low-Signal forced bullet
    assert f"Cite 1 to {prompts.MAX_EXECUTIVE_BULLET_CITATIONS}" in system


def test_source_pack_orders_by_materiality_then_headline_then_hash():
    articles = [
        _article("Bravo", score=5, url_hash="h2"),
        _article("Alpha", score=9, url_hash="h1"),
        _article("Charlie", score=5, url_hash="h0"),
    ]
    pack = prompts.macro_prompt(articles).source_pack
    # Materiality 9 first; remaining two (score 5) tie-break by headline asc.
    assert [s["headline"] for s in pack] == ["Alpha", "Bravo", "Charlie"]
    assert [s["id"] for s in pack] == [1, 2, 3]


def test_source_pack_is_deterministic_for_same_set():
    articles = [_article(f"H{i}", score=i % 4, url_hash=f"h{i}") for i in range(10)]
    a = prompts.macro_prompt(list(articles)).source_pack
    b = prompts.macro_prompt(list(reversed(articles))).source_pack
    assert [(s["id"], s["headline"]) for s in a] == [(s["id"], s["headline"]) for s in b]


def test_source_pack_caps_at_max_but_user_reports_total_count():
    articles = [_article(f"H{i:02d}", score=5, url_hash=f"h{i:02d}") for i in range(60)]
    mp = prompts.macro_prompt(articles)
    assert len(mp.source_pack) == prompts.MAX_MACRO_SUMMARY_SOURCE_PACK_ARTICLES
    assert mp.source_pack[-1]["id"] == prompts.MAX_MACRO_SUMMARY_SOURCE_PACK_ARTICLES
    assert "(60 articles)" in mp.user                   # total, not ranked count
    assert "[41]" not in mp.user                        # digest capped with the pack


def test_source_pack_entry_shape_and_domain():
    pack = prompts.macro_prompt(
        [_article("Alpha", url="https://www.Reuters.com:443/x", segment="Auto")]
    ).source_pack
    assert pack[0] == {
        "id": 1,
        "headline": "Alpha",
        "url": "https://www.Reuters.com:443/x",
        "domain": "reuters.com",                        # port stripped, www stripped, lowercased
        "segment": "Auto",
        "score": 5,
    }


# ---------------------------------------------------------------------------
# Macro prompt — macro-outlook contract (PR 2)
# ---------------------------------------------------------------------------

def test_macro_outlook_direction_enum_defined_and_promised():
    """VALID_MACRO_DIRECTIONS is a small enum owned in prompts.py, and every
    value appears literally in the macro system prompt."""
    assert prompts.VALID_MACRO_DIRECTIONS == frozenset({"Rising", "Stable", "Declining"})
    system = prompts.macro_prompt([_article("A")]).system
    for direction in prompts.VALID_MACRO_DIRECTIONS:
        assert direction in system


def test_macro_outlook_promises_canonical_segment_labels():
    """affected_segments must use the canonical commercial-segment labels — the
    transportation labels in full, and NOT informal variants."""
    system = prompts.macro_prompt([_article("A")]).system
    assert "Transportation - Automotive" in system
    assert "Transportation - Non-Automotive" in system
    assert "Transportation - Aerospace" in system
    assert "Enterprise / Cross-Segment" in system
    # Informal variants that would fail insight.VALID_COMMERCIAL_SEGMENTS.
    assert "Building & Construction" not in system


def test_macro_outlook_requires_citation_and_materiality():
    """The macro_outlook contract requires at least one citation per signal and
    excludes generic commentary without an Americhem implication."""
    system = prompts.macro_prompt([_article("A")]).system
    assert "macro_outlook" in system
    assert "current_condition" in system
    assert "affected_segments" in system
    assert "americhem_implication" in system
    # Citation-mandatory + materiality language (structural, not just prose).
    assert "at least one" in system.lower()
    assert "citation_source_ids" in system
    # The JSON example must show single braces (no unrendered f-string doubles).
    assert "{{" not in system and "}}" not in system


def test_macro_prompt_promises_signal_cap():
    """The macro system prompt promises the same signal cap the validator
    enforces, and the product cap is 3 (reduced from 6 on 2026-07-17)."""
    mp = prompts.macro_prompt([_article("Manufacturing PMI slips again", score=8)])
    assert f"up to {prompts.MAX_MACRO_OUTLOOK_SIGNALS}," in mp.system
    assert "up to 3," in mp.system


def test_rank_macro_articles_reserves_quota_for_macro_signals():
    """Low-materiality Macro-signal rows survive into the source pack even when
    40+ higher-materiality non-macro rows would otherwise crowd them out."""
    non_macro = [
        _article(f"NM{i:02d}", score=9, url_hash=f"h{i:02d}", category="competitors")
        for i in range(40)
    ]
    for a in non_macro:
        a["signal_type"] = "Competitive"
    macro = [
        {**_article(f"MACRO{i}", score=2, url_hash=f"m{i}", category="macro_manufacturing"),
         "signal_type": "Macro"}
        for i in range(3)
    ]
    mp = prompts.macro_prompt(non_macro + macro)
    pack_headlines = {s["headline"] for s in mp.source_pack}
    assert len(mp.source_pack) == prompts.MAX_MACRO_SUMMARY_SOURCE_PACK_ARTICLES
    for i in range(3):
        assert f"MACRO{i}" in pack_headlines


# ---------------------------------------------------------------------------
# Thematic prompt — grouped-text serialization
# ---------------------------------------------------------------------------

def test_thematic_user_contains_category_blocks_and_impact_lines():
    groups = {
        "Healthcare": [
            _article("A", score=8, impact="Hospital demand up."),
            _article("B", score=7, impact="Device approvals accelerating."),
        ],
    }
    spec = prompts.thematic_prompt(groups)
    assert "CATEGORY: Healthcare" in spec.user
    assert "[Acme | impact:8/10 | Neutral] Hospital demand up." in spec.user
    assert "Device approvals accelerating." in spec.user
    assert "exactly one synthesis paragraph" in spec.system
