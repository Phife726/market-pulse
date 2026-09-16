"""docs/runbooks — operator runbooks, pinned to the code they describe.

A runbook rots the moment a workflow input, a config key or a log line it
quotes is renamed. Each pin here reads the real artefact (the workflow
file, the shipped config, the engine source) and requires the runbook to
name it exactly, so a rename fails here rather than at 2 a.m.
"""
import pathlib
import re

import yaml

from tests.conftest import CONFIG_PATH, REPO_ROOT

RUNBOOK = REPO_ROOT / "docs" / "runbooks" / "no-email-but-green-run.md"


def _text() -> str:
    return RUNBOOK.read_text(encoding="utf-8")


def test_runbook_exists_and_claude_md_links_it():
    assert RUNBOOK.is_file()
    assert "docs/runbooks/no-email-but-green-run.md" in (REPO_ROOT / "CLAUDE.md").read_text(encoding="utf-8")


def test_runbook_resend_command_names_a_real_workflow_and_input():
    """The delivery-only re-send: the production workflow file must exist and
    expose the `run_ingestion` input the command sets to false."""
    text = _text()
    assert "gh workflow run market_pulse.yml --ref main -f run_ingestion=false" in text
    wf = yaml.safe_load((REPO_ROOT / ".github" / "workflows" / "market_pulse.yml").read_text())
    inputs = wf[True]["workflow_dispatch"]["inputs"] if True in wf else wf["on"]["workflow_dispatch"]["inputs"]
    assert "run_ingestion" in inputs
    assert "false" in inputs["run_ingestion"]["options"]


def test_runbook_block_instructions_name_the_real_config_key():
    text = _text()
    cfg = yaml.safe_load(CONFIG_PATH.read_text())
    assert "blocked_domains" in cfg["security"]
    assert "security:" in text and "blocked_domains:" in text
    assert "market_pulse_config.yaml" in text


def test_runbook_quotes_log_lines_that_the_code_actually_emits():
    """Every quoted log fragment must appear in the engine source, so a
    reworded log line fails here instead of sending the reader searching
    for text that no longer exists."""
    text = _text()
    source = "\n".join(
        (REPO_ROOT / f).read_text(encoding="utf-8")
        for f in ("delivery_engine.py", "mailer.py", "link_reputation.py", "ingestion_engine.py")
    )
    quoted = re.findall(r"`log:([^`]+)`", text)
    assert quoted, "the runbook marks log fragments as `log:...`"
    for fragment in quoted:
        assert fragment in source, f"runbook quotes a log line the code does not emit: {fragment!r}"


def test_runbook_sql_targets_the_real_column():
    text = _text()
    schema = (REPO_ROOT / "schema.sql").read_text(encoding="utf-8")
    assert "source_url" in schema and "daily_intelligence" in schema
    assert re.search(r"delete\s+from\s+daily_intelligence", text, re.I)
    assert "source_url" in text
