"""Integration tests: Quota Service with real Redis + PostgreSQL (testcontainers).

Test plan §3.5:
  - test_quota_check_increments_redis_counter
  - test_quota_check_denies_at_limit
  - test_quota_check_enterprise_never_denied
  - test_record_usage_dedup
  - test_fail_open_on_redis_error
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

testcontainers = pytest.importorskip("testcontainers", reason="testcontainers not installed")

from testcontainers.postgres import PostgresContainer  # noqa: E402
from testcontainers.redis import RedisContainer  # noqa: E402

import psycopg2  # noqa: E402
import redis as redis_lib  # noqa: E402
import sqlalchemy as sa  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

_ROOT = Path(__file__).parent.parent.parent
_QUOTA_SRC = str(_ROOT / "services" / "quota-service" / "src")
sys.path.insert(0, _QUOTA_SRC)

# Clear any cached modules so imports resolve fresh each session
for _m in ["quota_service", "db_queries", "config"]:
    sys.modules.pop(_m, None)

from quota_service import QuotaService, QuotaStatus  # noqa: E402
from db_queries import make_get_limit_fn  # noqa: E402


# ── PostgresContainer with exec-free readiness check ─────────────────────────
# Docker Desktop on macOS sometimes cleans up docker-exec instances before
# testcontainers can inspect the exit code, raising docker.errors.NotFound.
# Overriding _connect to use direct psycopg2 probes avoids the race.

class _SafePostgresContainer(PostgresContainer):
    def _connect(self) -> None:
        import time as _time
        deadline = _time.time() + 60
        while True:
            try:
                conn = psycopg2.connect(
                    host=self.get_container_host_ip(),
                    port=int(self.get_exposed_port(5432)),
                    user=self.username,
                    password=self.password,
                    dbname=self.dbname,
                    connect_timeout=2,
                )
                conn.close()
                return
            except psycopg2.OperationalError:
                if _time.time() >= deadline:
                    raise TimeoutError("PostgreSQL container not ready in 60 seconds")
                _time.sleep(1)

# ── Constants ──────────────────────────────────────────────────────────────────

_TENANT_FREE = "00000000-0000-0000-0000-000000000001"
_TENANT_ENTERPRISE = "00000000-0000-0000-0000-000000000002"
_METRIC = "API_CALLS_PER_DAY"
_FREE_LIMIT = 100

# ── Schema DDL (executed in dependency order) ──────────────────────────────────

_CREATE_STMTS = [
    """
    CREATE TABLE IF NOT EXISTS license_tiers (
        tier_id         TEXT    PRIMARY KEY,
        bytes_per_month BIGINT,
        vectors_max     BIGINT,
        queries_per_day INTEGER,
        queries_per_min INTEGER,
        gpu_enabled     BOOLEAN NOT NULL DEFAULT false,
        workers_max     INTEGER,
        users_max       INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tenant_licenses (
        tenant_id  UUID        PRIMARY KEY,
        tier_id    TEXT        REFERENCES license_tiers(tier_id),
        expires_at TIMESTAMPTZ,
        is_active  BOOLEAN     NOT NULL DEFAULT true
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quota_overrides (
        tenant_id      UUID NOT NULL REFERENCES tenant_licenses(tenant_id),
        metric         TEXT NOT NULL,
        override_value BIGINT,
        PRIMARY KEY (tenant_id, metric)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS usage_history (
        tenant_id   UUID        NOT NULL,
        metric      TEXT        NOT NULL,
        value       BIGINT      NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
    ) PARTITION BY RANGE (recorded_at)
    """,
    "CREATE TABLE IF NOT EXISTS usage_history_default PARTITION OF usage_history DEFAULT",
]

_SEED_STMTS = [
    f"""
    INSERT INTO license_tiers
        (tier_id, bytes_per_month, vectors_max, queries_per_day,
         queries_per_min, gpu_enabled, workers_max, users_max)
    VALUES
        ('free',       1073741824, 100000, {_FREE_LIMIT}, 5,    false, 1,  3),
        ('enterprise', NULL,       NULL,   NULL,          NULL, true,  16, NULL)
    ON CONFLICT (tier_id) DO NOTHING
    """,
    f"""
    INSERT INTO tenant_licenses (tenant_id, tier_id, is_active)
    VALUES
        ('{_TENANT_FREE}',       'free',       true),
        ('{_TENANT_ENTERPRISE}', 'enterprise', true)
    ON CONFLICT (tenant_id) DO NOTHING
    """,
]

# ── Session-scoped containers ─────────────────────────────────────────────────


@pytest.fixture(scope="session")
def pg_engine():
    with _SafePostgresContainer("postgres:16-alpine") as pg:
        engine = sa.create_engine(pg.get_connection_url())
        with engine.begin() as conn:
            for stmt in _CREATE_STMTS:
                conn.execute(sa.text(stmt))
            for stmt in _SEED_STMTS:
                conn.execute(sa.text(stmt))
        yield engine
        engine.dispose()


@pytest.fixture(scope="session")
def redis_session_client():
    with RedisContainer("redis:7-alpine") as rc:
        host = rc.get_container_host_ip()
        port = int(rc.get_exposed_port(6379))
        client = redis_lib.Redis(host=host, port=port, db=0, decode_responses=False)
        yield client


@pytest.fixture(autouse=True)
def _flush_redis(redis_session_client):
    """Wipe Redis before each test for isolation."""
    redis_session_client.flushall()
    yield


# ── Per-test fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def redis_client(redis_session_client):
    return redis_session_client


@pytest.fixture
def quota_svc(redis_client, pg_engine):
    factory = sessionmaker(pg_engine)
    return QuotaService(
        redis_client=redis_client,
        get_limit_fn=make_get_limit_fn(factory),
    )


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_quota_check_increments_redis_counter(quota_svc, redis_client):
    """CheckQuota with increment_on_allow=True writes the counter into Redis."""
    result = quota_svc.check_quota(_TENANT_FREE, _METRIC, amount=1, increment_on_allow=True)

    assert result.status == QuotaStatus.ALLOWED
    assert result.current_usage == 1

    key = f"quota:{_TENANT_FREE}:{_METRIC}"
    assert int(redis_client.get(key)) == 1


def test_quota_check_denies_at_limit(quota_svc, redis_client):
    """Pre-fill Redis to the Free daily limit; next CheckQuota returns DENIED and counter stays at limit."""
    key = f"quota:{_TENANT_FREE}:{_METRIC}"
    redis_client.set(key, _FREE_LIMIT)

    result = quota_svc.check_quota(_TENANT_FREE, _METRIC, amount=1, increment_on_allow=True)

    assert result.status == QuotaStatus.DENIED
    assert result.limit == _FREE_LIMIT
    # Rollback (DECRBY) must have fired; counter unchanged
    assert int(redis_client.get(key)) == _FREE_LIMIT


def test_quota_check_enterprise_never_denied(quota_svc, redis_client):
    """Enterprise tenant with NULL limit always returns UNLIMITED; Redis is untouched."""
    key = f"quota:{_TENANT_ENTERPRISE}:{_METRIC}"
    redis_client.set(key, 99999)

    result = quota_svc.check_quota(_TENANT_ENTERPRISE, _METRIC, amount=1, increment_on_allow=True)

    assert result.status == QuotaStatus.UNLIMITED
    # UNLIMITED path returns before touching Redis
    assert int(redis_client.get(key)) == 99999


def test_record_usage_dedup(quota_svc, redis_client):
    """Submitting the same event_id twice only increments the counter once."""
    first = quota_svc.record_usage(_TENANT_FREE, _METRIC, amount=1, event_id="evt-dedup-001")
    second = quota_svc.record_usage(_TENANT_FREE, _METRIC, amount=1, event_id="evt-dedup-001")

    assert first.deduped is False
    assert first.new_total == 1
    assert second.deduped is True

    key = f"quota:{_TENANT_FREE}:{_METRIC}"
    assert int(redis_client.get(key)) == 1


def test_fail_open_on_redis_error(pg_engine):
    """When Redis is unreachable the service fails open: returns ALLOWED and increments skip counter."""
    bad_redis = redis_lib.Redis(
        host="127.0.0.1",
        port=19999,
        socket_connect_timeout=0.5,
        socket_timeout=0.5,
    )
    factory = sessionmaker(pg_engine)
    skip_counter = MagicMock()

    svc = QuotaService(
        redis_client=bad_redis,
        get_limit_fn=make_get_limit_fn(factory),
        skip_counter=skip_counter,
    )

    result = svc.check_quota(_TENANT_FREE, _METRIC, amount=1, increment_on_allow=True)

    assert result.status == QuotaStatus.ALLOWED
    skip_counter.inc.assert_called_once()
