"""metadata schema: schema_versions, pipeline_runs, entities, lineage,
processing_steps, data_quality, query_results — from docs/metadata-lineage.md §3

Revision ID: 5001
Revises:
Create Date: 2026-06-13
"""
from typing import List, Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "5001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _is_pg() -> bool:
    return op.get_bind().dialect.name == "postgresql"


def upgrade() -> None:
    pg = _is_pg()
    schema = "metadata" if pg else None

    if pg:
        op.execute("CREATE SCHEMA IF NOT EXISTS metadata")

    # UUID server default
    _uuid_default = (
        sa.text("gen_random_uuid()")
        if pg
        else sa.text(
            "(lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2)))"
            " || '-4' || substr(lower(hex(randomblob(2))),2)"
            " || '-' || substr('89ab',abs(random()) % 4 + 1, 1)"
            " || substr(lower(hex(randomblob(2))),2)"
            " || '-' || lower(hex(randomblob(6))))"
        )
    )

    def _json():
        if pg:
            from sqlalchemy.dialects.postgresql import JSONB
            return JSONB()
        return sa.JSON()

    ts = sa.TIMESTAMP(timezone=True)

    # ── schema_versions ──────────────────────────────────────────────────────
    op.create_table(
        "schema_versions",
        sa.Column("id", sa.UUID(), nullable=False, server_default=_uuid_default),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("chunk_size", sa.Integer(), nullable=False, server_default=sa.text("512")),
        sa.Column("chunk_overlap", sa.Integer(), nullable=False, server_default=sa.text("64")),
        sa.Column("chunking_strategy", sa.Text(), nullable=False, server_default=sa.text("'fixed'")),
        sa.Column("embedding_model", sa.Text(), nullable=False),
        sa.Column("embedding_model_version", sa.Text(), nullable=True),
        sa.Column("embedding_dimension", sa.Integer(), nullable=False),
        sa.Column("embedding_backend", sa.Text(), nullable=False),
        sa.Column("index_type", sa.Text(), nullable=False, server_default=sa.text("'IVF_FLAT'")),
        sa.Column("is_current", sa.Boolean(), nullable=False,
                  server_default=sa.text("true" if pg else "1")),
        sa.Column("created_at", ts, nullable=True, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("created_by", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", "version_number",
                            name="uq_schema_versions_tenant_version"),
        schema=schema,
    )
    op.create_index(
        "ix_schema_versions_tenant_current",
        "schema_versions", ["tenant_id", "is_current"],
        schema=schema,
    )

    # ── pipeline_runs ─────────────────────────────────────────────────────────
    _fq_sv = ("metadata.schema_versions.id" if pg else "schema_versions.id")

    # Build column list; computed duration_ms added via ALTER on PG only
    _pr_cols: List = [
        sa.Column("id", sa.UUID(), nullable=False, server_default=_uuid_default),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("pipeline_type", sa.Text(), nullable=False),
        sa.Column("connector_id", sa.Text(), nullable=True),
        sa.Column("schema_version_id", sa.UUID(), nullable=True),
        sa.Column("config_snapshot", _json(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'running'")),
        sa.Column("started_at", ts, nullable=True, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("finished_at", ts, nullable=True),
        sa.Column("entities_processed", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("entities_failed", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("bytes_processed", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
    ]
    if not pg:
        # SQLite: plain nullable column (computed on PG via ALTER TABLE below)
        _pr_cols.insert(-3, sa.Column("duration_ms", sa.Integer(), nullable=True))

    op.create_table(
        "pipeline_runs",
        *_pr_cols,
        sa.ForeignKeyConstraint(["schema_version_id"], [_fq_sv],
                                name="fk_pipeline_runs_schema_version"),
        sa.PrimaryKeyConstraint("id"),
        schema=schema,
    )
    if pg:
        op.execute("""
            ALTER TABLE metadata.pipeline_runs
            ADD COLUMN duration_ms INTEGER
            GENERATED ALWAYS AS (
                EXTRACT(EPOCH FROM (finished_at - started_at)) * 1000
            ) STORED
        """)
    op.create_index(
        "ix_pipeline_runs_tenant_type_started",
        "pipeline_runs", ["tenant_id", "pipeline_type", "started_at"],
        schema=schema,
    )
    op.create_index(
        "ix_pipeline_runs_status",
        "pipeline_runs", ["status"],
        schema=schema,
    )

    # ── entities ──────────────────────────────────────────────────────────────
    _fq_pr = ("metadata.pipeline_runs.id" if pg else "pipeline_runs.id")

    op.create_table(
        "entities",
        sa.Column("id", sa.UUID(), nullable=False, server_default=_uuid_default),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("entity_key", sa.Text(), nullable=False),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("pipeline_run_id", sa.UUID(), nullable=True),
        sa.Column("schema_version_id", sa.UUID(), nullable=True),
        sa.Column("attributes", _json(), nullable=False, server_default=sa.text("'{}'")),
        sa.Column("is_current", sa.Boolean(), nullable=False,
                  server_default=sa.text("true" if pg else "1")),
        sa.Column("created_at", ts, nullable=True, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", ts, nullable=True, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["pipeline_run_id"], [_fq_pr],
                                name="fk_entities_pipeline_run"),
        sa.ForeignKeyConstraint(["schema_version_id"], [_fq_sv],
                                name="fk_entities_schema_version"),
        sa.UniqueConstraint("tenant_id", "entity_type", "entity_key",
                            name="uq_entities_tenant_type_key"),
        sa.PrimaryKeyConstraint("id"),
        schema=schema,
    )
    op.create_index("ix_entities_tenant_type", "entities", ["tenant_id", "entity_type"], schema=schema)
    op.create_index("ix_entities_entity_key", "entities", ["entity_key"], schema=schema)
    op.create_index("ix_entities_pipeline_run_id", "entities", ["pipeline_run_id"], schema=schema)
    if pg:
        op.execute(
            "CREATE INDEX ix_entities_attributes_gin "
            "ON metadata.entities USING GIN (attributes)"
        )
    # SQLite: no GIN; skip the JSON attribute index (not indexable as-is)

    # ── lineage ───────────────────────────────────────────────────────────────
    _fq_ent = ("metadata.entities.id" if pg else "entities.id")

    op.create_table(
        "lineage",
        sa.Column("id", sa.UUID(), nullable=False, server_default=_uuid_default),
        sa.Column("upstream_id", sa.UUID(), nullable=False),
        sa.Column("downstream_id", sa.UUID(), nullable=False),
        sa.Column("relationship", sa.Text(), nullable=False),
        sa.Column("pipeline_run_id", sa.UUID(), nullable=True),
        sa.Column("created_at", ts, nullable=True, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["upstream_id"], [_fq_ent], name="fk_lineage_upstream"),
        sa.ForeignKeyConstraint(["downstream_id"], [_fq_ent], name="fk_lineage_downstream"),
        sa.ForeignKeyConstraint(["pipeline_run_id"], [_fq_pr], name="fk_lineage_pipeline_run"),
        sa.UniqueConstraint("upstream_id", "downstream_id", "relationship",
                            name="uq_lineage_edge"),
        sa.PrimaryKeyConstraint("id"),
        schema=schema,
    )
    op.create_index("ix_lineage_upstream_id", "lineage", ["upstream_id"], schema=schema)
    op.create_index("ix_lineage_downstream_id", "lineage", ["downstream_id"], schema=schema)
    op.create_index("ix_lineage_relationship", "lineage", ["relationship"], schema=schema)

    # ── processing_steps ──────────────────────────────────────────────────────
    _ps_cols: List = [
        sa.Column("id", sa.UUID(), nullable=False, server_default=_uuid_default),
        sa.Column("entity_id", sa.UUID(), nullable=False),
        sa.Column("pipeline_run_id", sa.UUID(), nullable=False),
        sa.Column("step_type", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("started_at", ts, nullable=True, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("finished_at", ts, nullable=True),
        sa.Column("input_bytes", sa.BigInteger(), nullable=True),
        sa.Column("output_count", sa.Integer(), nullable=True),
        sa.Column("config_used", _json(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("error_code", sa.Text(), nullable=True),
    ]
    if not pg:
        _ps_cols.insert(7, sa.Column("duration_ms", sa.Integer(), nullable=True))

    op.create_table(
        "processing_steps",
        *_ps_cols,
        sa.ForeignKeyConstraint(["entity_id"], [_fq_ent], name="fk_processing_steps_entity"),
        sa.ForeignKeyConstraint(["pipeline_run_id"], [_fq_pr],
                                name="fk_processing_steps_pipeline_run"),
        sa.PrimaryKeyConstraint("id"),
        schema=schema,
    )
    if pg:
        op.execute("""
            ALTER TABLE metadata.processing_steps
            ADD COLUMN duration_ms INTEGER
            GENERATED ALWAYS AS (
                EXTRACT(EPOCH FROM (finished_at - started_at)) * 1000
            ) STORED
        """)
    op.create_index(
        "ix_processing_steps_entity_step", "processing_steps",
        ["entity_id", "step_type"], schema=schema,
    )
    op.create_index(
        "ix_processing_steps_run_id", "processing_steps",
        ["pipeline_run_id"], schema=schema,
    )
    op.create_index(
        "ix_processing_steps_status", "processing_steps",
        ["status"], schema=schema,
    )

    # ── data_quality ──────────────────────────────────────────────────────────
    op.create_table(
        "data_quality",
        sa.Column("id", sa.UUID(), nullable=False, server_default=_uuid_default),
        sa.Column("entity_id", sa.UUID(), nullable=False),
        sa.Column("run_id", sa.UUID(), nullable=True),
        sa.Column("check_name", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("value", sa.Numeric(), nullable=True),
        sa.Column("threshold", sa.Numeric(), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("checked_at", ts, nullable=True, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["entity_id"], [_fq_ent], name="fk_data_quality_entity"),
        sa.ForeignKeyConstraint(["run_id"], [_fq_pr], name="fk_data_quality_run"),
        sa.PrimaryKeyConstraint("id"),
        schema=schema,
    )
    op.create_index(
        "ix_data_quality_entity_check", "data_quality",
        ["entity_id", "check_name"], schema=schema,
    )
    op.create_index("ix_data_quality_status", "data_quality", ["status"], schema=schema)

    # ── query_results ─────────────────────────────────────────────────────────
    op.create_table(
        "query_results",
        sa.Column("query_id", sa.UUID(), nullable=False),
        sa.Column("chunk_entity_id", sa.UUID(), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column("score", sa.Numeric(), nullable=False),
        sa.Column("cached", sa.Boolean(), nullable=False,
                  server_default=sa.text("false" if pg else "0")),
        sa.Column("feedback_score", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["chunk_entity_id"], [_fq_ent],
                                name="fk_query_results_chunk_entity"),
        sa.PrimaryKeyConstraint("query_id", "chunk_entity_id"),
        schema=schema,
    )
    op.create_index(
        "ix_query_results_chunk_entity_id", "query_results",
        ["chunk_entity_id"], schema=schema,
    )
    op.create_index(
        "ix_query_results_feedback_score", "query_results",
        ["feedback_score"], schema=schema,
    )


def downgrade() -> None:
    pg = _is_pg()
    schema = "metadata" if pg else None

    op.drop_table("query_results", schema=schema)
    op.drop_table("data_quality", schema=schema)
    op.drop_table("processing_steps", schema=schema)
    op.drop_table("lineage", schema=schema)
    op.drop_table("entities", schema=schema)
    op.drop_table("pipeline_runs", schema=schema)
    op.drop_table("schema_versions", schema=schema)

    if pg:
        op.execute("DROP SCHEMA IF EXISTS metadata")
