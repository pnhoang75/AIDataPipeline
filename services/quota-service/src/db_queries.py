"""PostgreSQL query helpers for quota limit resolution and usage history flush.

Runtime wiring (used by server.py):
    from db_queries import make_get_limit_fn, flush_usage_to_history
"""
import logging
from typing import Optional

import sqlalchemy as sa

logger = logging.getLogger(__name__)

# Map proto Metric enum names to license_tiers column names
_METRIC_TO_COLUMN: dict[str, str] = {
    "BYTES_PER_MONTH": "bytes_per_month",
    "VECTORS_STORED": "vectors_max",
    "API_CALLS_PER_DAY": "queries_per_day",
    "API_CALLS_PER_MINUTE": "queries_per_min",
    "CONCURRENT_WORKERS": "workers_max",
    "USERS_PER_TENANT": "users_max",
    # GPU_SECONDS, CONNECTOR_COUNT have no dedicated column — treated as unlimited
}

_SELECT_OVERRIDE = sa.text("""
    SELECT override_value
    FROM quota_overrides
    WHERE tenant_id = :tenant_id AND metric = :metric
""")

_SELECT_TIER_LIMIT = sa.text("""
    SELECT lt.{col}
    FROM tenant_licenses tl
    JOIN license_tiers lt ON tl.tier_id = lt.tier_id
    WHERE tl.tenant_id = :tenant_id AND tl.is_active = TRUE
    LIMIT 1
""")

_INSERT_HISTORY = sa.text("""
    INSERT INTO usage_history (tenant_id, metric, value)
    VALUES (:tenant_id, :metric, :value)
""")


def get_effective_limit(tenant_id: str, metric: str, session) -> Optional[int]:
    """Return effective quota limit for tenant+metric (override > tier default).

    Returns None for unlimited (NULL in DB or unmapped metric).
    """
    # 1. Check override table
    row = session.execute(_SELECT_OVERRIDE, {"tenant_id": tenant_id, "metric": metric}).fetchone()
    if row is not None:
        return row[0]

    # 2. Fall back to tier definition
    col = _METRIC_TO_COLUMN.get(metric)
    if col is None:
        return None  # metric not in license_tiers → unlimited

    query = sa.text(_SELECT_TIER_LIMIT.text.format(col=col))
    row = session.execute(query, {"tenant_id": tenant_id}).fetchone()
    if row is None:
        logger.warning("No active license found for tenant %s", tenant_id)
        return None
    return row[0]  # may be None (unlimited)


def make_get_limit_fn(session_factory):
    """Return a get_limit_fn suitable for QuotaService.__init__."""

    def _get_limit(tenant_id: str, metric: str) -> Optional[int]:
        with session_factory() as session:
            return get_effective_limit(tenant_id, metric, session)

    return _get_limit


def flush_usage_to_history(tenant_id: str, metric: str, amount: int, session) -> None:
    """Persist a usage record to usage_history for audit/analytics."""
    session.execute(_INSERT_HISTORY, {"tenant_id": tenant_id, "metric": metric, "value": amount})
