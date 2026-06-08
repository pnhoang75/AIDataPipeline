"""seed license_tiers: Free / Pro / Enterprise

Matches multitenancy doc §3:
  Free:       1 GB/month, 100K vectors, 100 q/day, 5 q/min, no GPU, 1 worker, 3 users
  Pro:        100 GB/month, 10M vectors, 10K q/day, 100 q/min, GPU, 4 workers, 25 users
              connector count = unlimited (no cap in license_tiers; enforced via quota_overrides)
  Enterprise: unlimited on all numeric fields, GPU, 16 workers

Revision ID: 0002
Revises: 0001
Create Date: 2026-06-08
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_GB = 1024 * 1024 * 1024

TIERS = [
    {
        "tier_id": "free",
        "bytes_per_month": 1 * _GB,
        "vectors_max": 100_000,
        "queries_per_day": 100,
        "queries_per_min": 5,
        "gpu_enabled": False,
        "workers_max": 1,
        "users_max": 3,
    },
    {
        "tier_id": "pro",
        "bytes_per_month": 100 * _GB,
        "vectors_max": 10_000_000,
        "queries_per_day": 10_000,
        "queries_per_min": 100,
        "gpu_enabled": True,
        "workers_max": 4,
        "users_max": 25,
    },
    {
        "tier_id": "enterprise",
        "bytes_per_month": None,   # unlimited
        "vectors_max": None,        # unlimited
        "queries_per_day": None,    # unlimited
        "queries_per_min": None,    # custom / unlimited
        "gpu_enabled": True,
        "workers_max": 16,
        "users_max": None,          # unlimited
    },
]


def upgrade() -> None:
    license_tiers = sa.table(
        "license_tiers",
        sa.column("tier_id", sa.Text()),
        sa.column("bytes_per_month", sa.BigInteger()),
        sa.column("vectors_max", sa.BigInteger()),
        sa.column("queries_per_day", sa.Integer()),
        sa.column("queries_per_min", sa.Integer()),
        sa.column("gpu_enabled", sa.Boolean()),
        sa.column("workers_max", sa.Integer()),
        sa.column("users_max", sa.Integer()),
    )
    op.bulk_insert(license_tiers, TIERS)


def downgrade() -> None:
    op.execute("DELETE FROM license_tiers WHERE tier_id IN ('free', 'pro', 'enterprise')")
