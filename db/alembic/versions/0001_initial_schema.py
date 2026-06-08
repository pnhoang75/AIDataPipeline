"""initial schema: license_tiers, tenant_licenses, quota_overrides, usage_history,
workspaces, workspace_sources, source_file_status

Revision ID: 0001
Revises:
Create Date: 2026-06-08
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _is_postgresql() -> bool:
    bind = op.get_bind()
    return bind.dialect.name == "postgresql"


def upgrade() -> None:
    # license_tiers — one row per tier; NULL = unlimited
    op.create_table(
        "license_tiers",
        sa.Column("tier_id", sa.Text(), nullable=False),
        sa.Column("bytes_per_month", sa.BigInteger(), nullable=True),
        sa.Column("vectors_max", sa.BigInteger(), nullable=True),
        sa.Column("queries_per_day", sa.Integer(), nullable=True),
        sa.Column("queries_per_min", sa.Integer(), nullable=True),
        sa.Column("gpu_enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("workers_max", sa.Integer(), nullable=True),
        sa.Column("users_max", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("tier_id"),
    )

    # tenant_licenses — FK to license_tiers
    op.create_table(
        "tenant_licenses",
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("tier_id", sa.Text(), nullable=True),
        sa.Column("expires_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.ForeignKeyConstraint(["tier_id"], ["license_tiers.tier_id"]),
        sa.PrimaryKeyConstraint("tenant_id"),
    )

    # quota_overrides — per-tenant metric overrides
    op.create_table(
        "quota_overrides",
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("metric", sa.Text(), nullable=False),
        sa.Column("override_value", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenant_licenses.tenant_id"]),
        sa.PrimaryKeyConstraint("tenant_id", "metric"),
    )

    if _is_postgresql():
        # PostgreSQL: partitioned by recorded_at
        op.execute("""
            CREATE TABLE usage_history (
                tenant_id   UUID        NOT NULL,
                metric      TEXT        NOT NULL,
                value       BIGINT      NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
            ) PARTITION BY RANGE (recorded_at)
        """)
        # Default partition to catch all rows (testbed only)
        op.execute("""
            CREATE TABLE usage_history_default
                PARTITION OF usage_history DEFAULT
        """)
    else:
        op.create_table(
            "usage_history",
            sa.Column("tenant_id", sa.UUID(), nullable=False),
            sa.Column("metric", sa.Text(), nullable=False),
            sa.Column("value", sa.BigInteger(), nullable=False),
            sa.Column(
                "recorded_at",
                sa.TIMESTAMP(timezone=True),
                nullable=False,
                server_default=sa.text("CURRENT_TIMESTAMP"),
            ),
        )

    # workspaces — tenant-scoped logical grouping of sources
    op.create_table(
        "workspaces",
        sa.Column(
            "id",
            sa.UUID(),
            nullable=False,
            server_default=sa.text("gen_random_uuid()") if _is_postgresql() else sa.text("(lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || substr(lower(hex(randomblob(2))),2) || '-' || substr('89ab',abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))),2) || '-' || lower(hex(randomblob(6))))"),
        ),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("owner_id", sa.UUID(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # workspace_sources — which connectors are attached to each workspace
    op.create_table(
        "workspace_sources",
        sa.Column(
            "id",
            sa.UUID(),
            nullable=False,
            server_default=sa.text("gen_random_uuid()") if _is_postgresql() else sa.text("(lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || substr(lower(hex(randomblob(2))),2) || '-' || substr('89ab',abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))),2) || '-' || lower(hex(randomblob(6))))"),
        ),
        sa.Column("workspace_id", sa.UUID(), nullable=False),
        sa.Column("connector_id", sa.Text(), nullable=False),
        sa.Column("path_prefix", sa.Text(), nullable=False),
        sa.Column(
            "added_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.ForeignKeyConstraint(["workspace_id"], ["workspaces.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("workspace_id", "connector_id", "path_prefix"),
    )

    # source_file_status — per-file ingestion tracking
    op.create_table(
        "source_file_status",
        sa.Column(
            "id",
            sa.UUID(),
            nullable=False,
            server_default=sa.text("gen_random_uuid()") if _is_postgresql() else sa.text("(lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || substr(lower(hex(randomblob(2))),2) || '-' || substr('89ab',abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))),2) || '-' || lower(hex(randomblob(6))))"),
        ),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("connector_id", sa.Text(), nullable=False),
        sa.Column("file_path", sa.Text(), nullable=False),
        sa.Column("file_size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("content_type", sa.Text(), nullable=True),
        sa.Column("last_modified", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("ingest_status", sa.Text(), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("chunk_count", sa.Integer(), nullable=True),
        sa.Column("indexed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", "connector_id", "file_path"),
    )


def downgrade() -> None:
    op.drop_table("source_file_status")
    op.drop_table("workspace_sources")
    op.drop_table("workspaces")
    if _is_postgresql():
        op.execute("DROP TABLE IF EXISTS usage_history_default")
        op.execute("DROP TABLE IF EXISTS usage_history")
    else:
        op.drop_table("usage_history")
    op.drop_table("quota_overrides")
    op.drop_table("tenant_licenses")
    op.drop_table("license_tiers")
