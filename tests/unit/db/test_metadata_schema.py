"""Unit tests for the metadata schema Alembic migrations (docs/metadata-lineage.md §3).

Runs against an in-memory SQLite database so no Postgres cluster is required.
The migration creates flat (unschema'd) tables on SQLite and schema-qualified
tables (metadata.*) on PostgreSQL — these tests exercise the SQLite path.

Done-check command: pytest tests/unit/db/test_metadata_schema.py -x --tb=short -q
"""
import os
import tempfile
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory

REPO_ROOT = Path(__file__).resolve().parents[3]
DB_DIR = REPO_ROOT / "db"


@pytest.fixture(scope="module")
def alembic_cfg():
    cfg = Config(str(DB_DIR / "alembic_metadata.ini"))
    cfg.set_main_option("script_location", str(DB_DIR / "alembic_metadata"))
    return cfg


@pytest.fixture(scope="module")
def migrated_engine(alembic_cfg):
    """SQLite file-based database upgraded to metadata migration head."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    url = f"sqlite:///{db_path}"
    engine = sa.create_engine(url)
    alembic_cfg.set_main_option("sqlalchemy.url", url)
    os.environ["METADATA_DB_URL"] = url
    try:
        command.upgrade(alembic_cfg, "head")
        yield engine
    finally:
        engine.dispose()
        os.environ.pop("METADATA_DB_URL", None)
        Path(db_path).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Revision chain
# ---------------------------------------------------------------------------

EXPECTED_REVISIONS = {"5001"}
EXPECTED_HEAD = "5001"


def test_revision_chain_is_valid(alembic_cfg):
    script = ScriptDirectory.from_config(alembic_cfg)
    revisions = {rev.revision for rev in script.walk_revisions()}
    assert revisions == EXPECTED_REVISIONS


def test_head_revision(alembic_cfg):
    script = ScriptDirectory.from_config(alembic_cfg)
    heads = script.get_heads()
    assert heads == [EXPECTED_HEAD]


def test_root_revision_has_no_parent(alembic_cfg):
    script = ScriptDirectory.from_config(alembic_cfg)
    rev = script.get_revision("5001")
    assert rev.down_revision is None


# ---------------------------------------------------------------------------
# All 7 tables present after upgrade to head
# ---------------------------------------------------------------------------

EXPECTED_TABLES = {
    "schema_versions",
    "pipeline_runs",
    "entities",
    "lineage",
    "processing_steps",
    "data_quality",
    "query_results",
}


def test_all_metadata_tables_created(migrated_engine):
    inspector = sa.inspect(migrated_engine)
    tables = set(inspector.get_table_names())
    missing = EXPECTED_TABLES - tables
    assert not missing, f"Missing tables after migration: {missing}"


@pytest.mark.parametrize("table", sorted(EXPECTED_TABLES))
def test_table_exists(migrated_engine, table):
    inspector = sa.inspect(migrated_engine)
    assert table in inspector.get_table_names(), f"Table '{table}' not found"


# ---------------------------------------------------------------------------
# Column checks — key columns per table
# ---------------------------------------------------------------------------

def _cols(engine, table):
    inspector = sa.inspect(engine)
    return {col["name"] for col in inspector.get_columns(table)}


def test_schema_versions_columns(migrated_engine):
    required = {
        "id", "tenant_id", "version_number", "chunk_size", "chunk_overlap",
        "chunking_strategy", "embedding_model", "embedding_dimension",
        "embedding_backend", "index_type", "is_current", "created_at",
    }
    assert required <= _cols(migrated_engine, "schema_versions")


def test_pipeline_runs_columns(migrated_engine):
    required = {
        "id", "tenant_id", "pipeline_type", "schema_version_id",
        "config_snapshot", "status", "started_at", "finished_at",
        "entities_processed", "entities_failed", "bytes_processed",
    }
    assert required <= _cols(migrated_engine, "pipeline_runs")


def test_entities_columns(migrated_engine):
    required = {
        "id", "entity_type", "entity_key", "tenant_id",
        "pipeline_run_id", "schema_version_id", "attributes",
        "is_current", "created_at", "updated_at",
    }
    assert required <= _cols(migrated_engine, "entities")


def test_lineage_columns(migrated_engine):
    required = {
        "id", "upstream_id", "downstream_id", "relationship",
        "pipeline_run_id", "created_at",
    }
    assert required <= _cols(migrated_engine, "lineage")


def test_processing_steps_columns(migrated_engine):
    required = {
        "id", "entity_id", "pipeline_run_id", "step_type",
        "status", "started_at", "finished_at",
        "input_bytes", "output_count", "error_message", "error_code",
    }
    assert required <= _cols(migrated_engine, "processing_steps")


def test_data_quality_columns(migrated_engine):
    required = {
        "id", "entity_id", "run_id", "check_name",
        "status", "value", "threshold", "message", "checked_at",
    }
    assert required <= _cols(migrated_engine, "data_quality")


def test_query_results_columns(migrated_engine):
    required = {
        "query_id", "chunk_entity_id", "rank", "score",
        "cached", "feedback_score",
    }
    assert required <= _cols(migrated_engine, "query_results")


# ---------------------------------------------------------------------------
# Default value smoke tests
# ---------------------------------------------------------------------------

_TENANT = "00000000-0000-4000-8000-000000000001"
_SV_ID = "00000000-0000-4000-8000-000000000010"
_PR_ID = "00000000-0000-4000-8000-000000000020"
_ENT_ID = "00000000-0000-4000-8000-000000000030"


def _exec(engine, sql, **params):
    with engine.begin() as conn:
        conn.execute(sa.text(sql), params)


def _fetch(engine, sql, **params):
    with engine.connect() as conn:
        return conn.execute(sa.text(sql), params).fetchone()


def test_schema_versions_defaults(migrated_engine):
    _exec(
        migrated_engine,
        """INSERT INTO schema_versions
           (id, tenant_id, version_number, embedding_model,
            embedding_dimension, embedding_backend)
           VALUES (:id, :tid, 1, 'BAAI/bge-small-en-v1.5', 384, 'local-cpu')""",
        id=_SV_ID, tid=_TENANT,
    )
    row = _fetch(
        migrated_engine,
        "SELECT chunk_size, chunk_overlap, chunking_strategy, index_type, is_current "
        "FROM schema_versions WHERE id = :id",
        id=_SV_ID,
    )
    assert row is not None
    assert row[0] == 512        # chunk_size
    assert row[1] == 64         # chunk_overlap
    assert row[2] == "fixed"    # chunking_strategy
    assert row[3] == "IVF_FLAT" # index_type
    assert row[4] in (True, 1)  # is_current (True or 1 on SQLite)


def test_pipeline_runs_status_default(migrated_engine):
    _exec(
        migrated_engine,
        """INSERT INTO pipeline_runs (id, tenant_id, pipeline_type)
           VALUES (:id, :tid, 'ingestion')""",
        id=_PR_ID, tid=_TENANT,
    )
    row = _fetch(
        migrated_engine,
        "SELECT status, entities_processed, entities_failed, bytes_processed "
        "FROM pipeline_runs WHERE id = :id",
        id=_PR_ID,
    )
    assert row is not None
    assert row[0] == "running"
    assert row[1] == 0
    assert row[2] == 0
    assert row[3] == 0


def test_entities_defaults(migrated_engine):
    _exec(
        migrated_engine,
        """INSERT INTO entities
           (id, entity_type, entity_key, tenant_id)
           VALUES (:id, 'RawDocument', 'sha256:abc', :tid)""",
        id=_ENT_ID, tid=_TENANT,
    )
    row = _fetch(
        migrated_engine,
        "SELECT is_current FROM entities WHERE id = :id",
        id=_ENT_ID,
    )
    assert row is not None
    assert row[0] in (True, 1)


def test_processing_steps_status_default(migrated_engine):
    _ps_id = "00000000-0000-4000-8000-000000000040"
    _exec(
        migrated_engine,
        """INSERT INTO processing_steps
           (id, entity_id, pipeline_run_id, step_type)
           VALUES (:id, :eid, :rid, 'parse')""",
        id=_ps_id, eid=_ENT_ID, rid=_PR_ID,
    )
    row = _fetch(
        migrated_engine,
        "SELECT status FROM processing_steps WHERE id = :id",
        id=_ps_id,
    )
    assert row is not None
    assert row[0] == "pending"


def test_query_results_cached_default(migrated_engine):
    _qid = "00000000-0000-4000-8000-000000000050"
    _exec(
        migrated_engine,
        """INSERT INTO query_results
           (query_id, chunk_entity_id, rank, score)
           VALUES (:qid, :eid, 1, 0.95)""",
        qid=_qid, eid=_ENT_ID,
    )
    row = _fetch(
        migrated_engine,
        "SELECT cached FROM query_results WHERE query_id = :qid",
        qid=_qid,
    )
    assert row is not None
    assert row[0] in (False, 0)


# ---------------------------------------------------------------------------
# Unique constraint checks
# ---------------------------------------------------------------------------

def test_schema_versions_unique_tenant_version(migrated_engine):
    """Inserting a duplicate (tenant_id, version_number) should raise."""
    import pytest as _pytest
    from sqlalchemy.exc import IntegrityError
    with _pytest.raises(IntegrityError):
        _exec(
            migrated_engine,
            """INSERT INTO schema_versions
               (id, tenant_id, version_number, embedding_model,
                embedding_dimension, embedding_backend)
               VALUES (:id, :tid, 1, 'BAAI/bge-small-en-v1.5', 384, 'local-cpu')""",
            id="00000000-0000-4000-8000-000000000099",
            tid=_TENANT,  # same tenant + version 1 as test_schema_versions_defaults
        )


def test_entities_unique_tenant_type_key(migrated_engine):
    """Inserting a duplicate (tenant_id, entity_type, entity_key) should raise."""
    from sqlalchemy.exc import IntegrityError
    with pytest.raises(IntegrityError):
        _exec(
            migrated_engine,
            """INSERT INTO entities
               (id, entity_type, entity_key, tenant_id)
               VALUES (:id, 'RawDocument', 'sha256:abc', :tid)""",
            id="00000000-0000-4000-8000-000000000088",
            tid=_TENANT,
        )


def test_lineage_unique_edge(migrated_engine):
    """Inserting a duplicate lineage edge should raise."""
    from sqlalchemy.exc import IntegrityError
    _up = "00000000-0000-4000-8000-000000000060"
    _down = "00000000-0000-4000-8000-000000000070"
    _exec(
        migrated_engine,
        """INSERT INTO entities (id, entity_type, entity_key, tenant_id)
           VALUES (:id, 'DataSource', 'ds-1', :tid)""",
        id=_up, tid=_TENANT,
    )
    _exec(
        migrated_engine,
        """INSERT INTO entities (id, entity_type, entity_key, tenant_id)
           VALUES (:id, 'DocumentChunk', 'chunk-1', :tid)""",
        id=_down, tid=_TENANT,
    )
    _exec(
        migrated_engine,
        """INSERT INTO lineage (id, upstream_id, downstream_id, relationship)
           VALUES ('00000000-0000-4000-8000-000000000080', :up, :down, 'chunked_into')""",
        up=_up, down=_down,
    )
    with pytest.raises(IntegrityError):
        _exec(
            migrated_engine,
            """INSERT INTO lineage (id, upstream_id, downstream_id, relationship)
               VALUES ('00000000-0000-4000-8000-000000000081', :up, :down, 'chunked_into')""",
            up=_up, down=_down,
        )


# ---------------------------------------------------------------------------
# Downgrade smoke test
# ---------------------------------------------------------------------------

def test_downgrade_removes_all_tables(alembic_cfg):
    """Downgrade to base should drop all 7 tables."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    url = f"sqlite:///{db_path}"
    cfg = Config(str(DB_DIR / "alembic_metadata.ini"))
    cfg.set_main_option("script_location", str(DB_DIR / "alembic_metadata"))
    cfg.set_main_option("sqlalchemy.url", url)
    os.environ["METADATA_DB_URL"] = url
    try:
        command.upgrade(cfg, "head")
        command.downgrade(cfg, "base")
        engine = sa.create_engine(url)
        inspector = sa.inspect(engine)
        tables = set(inspector.get_table_names())
        leftover = EXPECTED_TABLES & tables
        engine.dispose()
        assert not leftover, f"Tables not dropped on downgrade: {leftover}"
    finally:
        os.environ.pop("METADATA_DB_URL", None)
        Path(db_path).unlink(missing_ok=True)
