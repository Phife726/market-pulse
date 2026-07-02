"""Print the assembled LLM prompts with placeholder inputs — operator tool.

Workflow: reword a rule in prompts.py (or a segment description in
market_pulse_config.yaml), run `python scripts/show_prompts.py > after.txt`,
diff against a dump taken before the change, and eyeball the assembled
result — zero API spend, zero pipeline runs. The fingerprint printed per
prompt is PromptSpec.system_fingerprint, the same identity a run would log.
"""
import os
import sys

import yaml

# When run as `python scripts/show_prompts.py`, sys.path[0] is scripts/, not
# the repo root — add the repo root so prompts/insight import. The config path
# below uses the same root, so the tool is cwd-independent end to end.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)

import prompts  # noqa: E402

_STUB_ARTICLE = {
    "headline": "<HEADLINE>",
    "americhem_impact_score": 7,
    "sentiment_tag": "Neutral",
    "commercial_segment": "<SEGMENT>",
    "category": "<CATEGORY>",
    "americhem_impact": "<SO WHAT>",
    "source_url": "https://example.com/<ARTICLE>",
    "url_hash": "<HASH>",
    "entities_mentioned": ["<ENTITY>"],
}


def _load_config() -> dict:
    """Load the live config from the repo root; warn LOUDLY on any failure.

    A silent fallback would print fallback-taxonomy prompts whose fingerprints
    don't match production — the exact confusion this tool exists to prevent."""
    path = os.path.join(_REPO_ROOT, "market_pulse_config.yaml")
    try:
        with open(path, "r") as fh:
            return yaml.safe_load(fh) or {}
    except Exception as exc:
        print(
            f"WARNING: could not load {path} ({exc}); rendering with fallback "
            "taxonomy lists — fingerprints will NOT match production runs.",
            file=sys.stderr,
        )
        return {}


def main() -> None:
    config = _load_config()

    specs = {
        "insight synthesis": prompts.insight_prompt(
            config,
            article_text="<ARTICLE TEXT>",
            source_url="https://example.com/<ARTICLE>",
            trigger_entity="<ENTITY>",
            category="<CATEGORY>",
        ),
        "macro summary": prompts.macro_prompt([_STUB_ARTICLE]),
        "thematic synthesis": prompts.thematic_prompt(
            {"<SEGMENT>": [_STUB_ARTICLE, _STUB_ARTICLE]}
        ),
    }

    for name, spec in specs.items():
        rule = "=" * 75
        print(rule)
        print(f"# {name} — fingerprint {spec.system_fingerprint} — "
              f"temperature {spec.temperature}")
        print(rule)
        print("--- system ---")
        print(spec.system)
        print("--- user ---")
        print(spec.user)
        print()


if __name__ == "__main__":
    main()
