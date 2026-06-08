"""Unit tests for PostgreSQL migrations (run against SQLite in-memory).

Verifies:
  1. Alembic revision chain is valid (0001 → 0002, head = 0002).
  2. All 7 tables are created after upgrade to head.
  3. license_tiers seed data matches the multitenancy doc §3.
  4. ingest_status column defaults to 'pending'.
"""
import os
import sys
import tempfile
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory

# Allow running from project root without installing the package
REPO_ROOT = Path(__file__).resolve().parents[3]
DB_DIR = REPO_ROOT / "db"


@pytest.fixture(scope="module")
def alembic_cfg():
    cfg = Config(str(DB_DIR / "alembic.ini"))
    cfg.set_main_option("script_location", str(DB_DIR / "alembic"))
    return cfg


@pytest.fixture(scope="module")
def migrated_engine(alembic_cfg):
    """SQLite file-based database upgraded to Alembic head."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    url = f"sqlite:///{db_path}"
    engine = sa.create_engine(url)
    alembic_cfg.set_main_option("sqlalchemy.url", url)
    os.environ["QUOTA_DB_URL"] = url
    try:
        command.upgrade(alembic_cfg, "head")
        yield engine
    finally:
        engine.dispose()
        os.environ.pop("QUOTA_DB_URL", None)
        Path(db_path).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Revision chain
# ---------------------------------------------------------------------------

EXPECTED_REVISIONS = {"0001", "0002"}
EXPECTED_HEAD = "0002"


def test_revision_chain_is_valid(alembic_cfg):
    script = ScriptDirectory.from_config(alembic_cfg)
    revisions = {rev.revision for rev in script.walk_revisions()}
    assert revisions == EXPECTED_REVISIONS


def test_head_revision(alembic_cfg):
    script = ScriptDirectory.from_config(alembic_cfg)
    heads = script.get_heads()
    assert heads == [EXPECTED_HEAD]


def test_downgrade_revision_links(alembic_cfg):
    """0002 depends on 0001; 0001 has no parent."""
    script = ScriptDirectory.from_config(alembic_cfg)
    rev_0002 = script.get_revision("0002")
    rev_0001 = script.get_revision("0001")
    assert rev_0002.down_revision == "0001"
    assert rev_0001.down_revision is None


# ---------------------------------------------------------------------------
# Schema — all 7 tables present
# ---------------------------------------------------------------------------

EXPECTED_TABLES = {
    "license_tiers",
    "tenant_licenses",
    "quota_overrides",
    "usage_history",
    "workspaces",
    "workspace_sources",
    "source_file_status",
}


def test_all_tables_created(migrated_engine):
    inspector = sa.inspect(migrated_engine)
    tables = set(inspector.get_table_names())
    missing = EXPECTED_TABLES - tables
    assert not missing, f"Missing tables after migration: {missing}"


@pytest.mark.parametrize("table", sorted(EXPECTED_TABLES))
def test_table_exists(migrated_engine, table):
    inspector = sa.inspect(migrated_engine)
    assert table in inspector.get_table_names()


# ---------------------------------------------------------------------------
# Column checks on key tables
# ---------------------------------------------------------------------------

def _column_names(engine, table):
    inspector = sa.inspect(engine)
    return {col["name"] for col in inspector.get_columns(table)}


def test_license_tiers_columns(migrated_engine):
    cols = _column_names(migrated_engine, "license_tiers")
    required = {
        "tier_id", "bytes_per_month", "vectors_max",
        "queries_per_day", "queries_per_min", "gpu_enabled",
        "workers_max", "users_max",
    }
    assert required <= cols


def test_source_file_status_columns(migrated_engine):
    cols = _column_names(migrated_engine, "source_file_status")
    required = {
        "id", "tenant_id", "connector_id", "file_path",
        "file_size_bytes", "content_type", "last_modified",
        "ingest_status", "error_message", "chunk_count", "indexed_at",
    }
    assert required <= cols


def test_workspace_sources_fk_column(migrated_engine):
    cols = _column_names(migrated_engine, "workspace_sources")
    assert "workspace_id" in cols


# ---------------------------------------------------------------------------
# Seed data — license_tiers
# ---------------------------------------------------------------------------

_GB = 1024 * 1024 * 1024


def _fetch_tiers(engine):
    with engine.connect() as conn:
        rows = conn.execute(sa.text("SELECT * FROM license_tiers ORDER BY tier_id")).fetchall()
    return {row[0]: row for row in rows}


def test_three_tiers_seeded(migrated_engine):
    tiers = _fetch_tiers(migrated_engine)
    assert set(tiers.keys()) == {"enterprise", "free", "pro"}


def test_free_tier_values(migrated_engine):
    tiers = _fetch_tiers(migrated_engine)
    row = dict(zip(
        ["tier_id", "bytes_per_month", "vectors_max", "queries_per_day",
         "queries_per_min", "gpu_enabled", "workers_max", "users_max"],
        tiers["free"],
    ))
    assert row["bytes_per_month"] == 1 * _GB
    assert row["vectors_max"] == 100_000
    assert row["queries_per_day"] == 100
    assert row["queries_per_min"] == 5
    assert row["gpu_enabled"] is False or row["gpu_enabled"] == 0
    assert row["workers_max"] == 1
    assert row["users_max"] == 3


def test_pro_tier_values(migrated_engine):
    tiers = _fetch_tiers(migrated_engine)
    row = dict(zip(
        ["tier_id", "bytes_per_month", "vectors_max", "queries_per_day",
         "queries_per_min", "gpu_enabled", "workers_max", "users_max"],
        tiers["pro"],
    ))
    assert row["bytes_per_month"] == 100 * _GB
    assert row["vectors_max"] == 10_000_000
    assert row["queries_per_day"] == 10_000
    assert row["queries_per_min"] == 100
    assert row["gpu_enabled"] is True or row["gpu_enabled"] == 1
    assert row["workers_max"] == 4
    assert row["users_max"] == 25


def test_enterprise_tier_unlimited_fields(migrated_engine):
    """Unlimited fields are stored as NULL."""
    tiers = _fetch_tiers(migrated_engine)
    row = dict(zip(
        ["tier_id", "bytes_per_month", "vectors_max", "queries_per_day",
         "queries_per_min", "gpu_enabled", "workers_max", "users_max"],
        tiers["enterprise"],
    ))
    assert row["bytes_per_month"] is None
    assert row["vectors_max"] is None
    assert row["queries_per_day"] is None
    assert row["queries_per_min"] is None
    assert row["gpu_enabled"] is True or row["gpu_enabled"] == 1
    assert row["workers_max"] == 16
    assert row["users_max"] is None


# ---------------------------------------------------------------------------
# Default value smoke-test for source_file_status
# ---------------------------------------------------------------------------

def test_ingest_status_default(migrated_engine):
    """Inserting a row without ingest_status should default to 'pending'."""
    with migrated_engine.begin() as conn:
        conn.execute(sa.text("""
            INSERT INTO source_file_status
                (id, tenant_id, connector_id, file_path)
            VALUES
                ('00000000-0000-4000-8000-000000000001',
                 '00000000-0000-4000-8000-000000000002',
                 'connector-test', '/test/file.pdf')
        """))
        row = conn.execute(sa.text(
            "SELECT ingest_status FROM source_file_status WHERE connector_id='connector-test'"
        )).fetchone()
    assert row is not None
    assert row[0] == "pending"
