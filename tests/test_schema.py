"""Pins on the shipped schema and migrations (`schema.sql`, `migrations/`).

A fresh database initializes from `schema.sql` alone, and an existing one
applies the migrations in order — so every column the code writes or filters
on must be declared in both places. The run-mode pins are issue #100 /
ADR 0001: production reads filter on `daily_intelligence.run_mode`, and a
filter on a column that does not exist is a failed strict read.
"""
import re

from tests.conftest import REPO_ROOT

SCHEMA = (REPO_ROOT / "schema.sql").read_text(encoding="utf-8")
MIGRATION_008 = REPO_ROOT / "migrations" / "008_add_run_mode_to_daily_intelligence.sql"


def _table_block(schema: str, table: str) -> str:
    m = re.search(rf"create table if not exists {table} \((.*?)\n\);", schema, re.S)
    assert m, f"{table} not declared in schema.sql"
    return m.group(1)


def test_schema_declares_run_mode_on_daily_intelligence_with_the_production_default():
    block = _table_block(SCHEMA, "daily_intelligence")
    assert re.search(r"run_mode text not null default 'production'", block)


def test_schema_keeps_url_hash_unique_across_run_modes():
    """One row per URL whichever mode wrote it (ADR 0001) — the index stays
    on url_hash alone, never (url_hash, run_mode)."""
    assert re.search(r"create unique index if not exists \S+\s+on daily_intelligence \(url_hash\);", SCHEMA)
    assert "(url_hash, run_mode)" not in SCHEMA


def test_the_ad_hoc_view_exposes_run_mode_as_its_last_column():
    """Last, because CREATE OR REPLACE VIEW may only append columns; a column
    inserted mid-list fails on an existing database (42P16), which is what
    happened the first time migration 008 was run."""
    view = SCHEMA[SCHEMA.index("create or replace view todays_intelligence"):]
    assert re.search(r"end as alert_tier,\s*(--[^\n]*\n\s*)?run_mode\s*\nfrom daily_intelligence", view)
    migration_view = MIGRATION_008.read_text(encoding="utf-8")
    assert re.search(r"end as alert_tier,\s*(--[^\n]*\n\s*)?run_mode\s*\nfrom daily_intelligence", migration_view)


def test_migration_008_adds_run_mode_idempotently():
    assert MIGRATION_008.exists(), "migrations/008_add_run_mode_to_daily_intelligence.sql missing"
    sql = MIGRATION_008.read_text(encoding="utf-8")
    assert re.search(
        r"alter table daily_intelligence\s+add column if not exists run_mode text not null default 'production';",
        sql,
    )
