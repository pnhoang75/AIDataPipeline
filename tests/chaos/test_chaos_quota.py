"""
Chaos tests for quota-service — test plan §8.4, §8.5 (quota aspects).

  8.4  Quota service unavailability → fail-open on Redis down; zero user errors
  8.5  Redis unavailability         → Quota Service falls back; returns ALLOWED
"""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

import pytest

_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_QUOTA_SRC = os.path.join(_REPO, "services", "quota-service", "src")
if _QUOTA_SRC not in sys.path:
    sys.path.insert(0, _QUOTA_SRC)

from quota_service import QuotaService, QuotaStatus  # noqa: E402


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_quota_service(redis_client=None, limit=1000):
    redis_client = redis_client or MagicMock()
    skip_counter = MagicMock()
    return QuotaService(
        redis_client=redis_client,
        get_limit_fn=lambda tenant_id, metric: limit,
        skip_counter=skip_counter,
    ), skip_counter


# ── 8.4 Quota Service Unavailability ──────────────────────────────────────────

class TestQuotaServiceUnavailability:
    """8.4 Quota service unavailability: fail-open; zero user-facing errors."""

    def test_fail_open_when_redis_raises_connection_error(self):
        """check_quota returns ALLOWED and increments skip_counter when Redis is down."""
        redis = MagicMock()
        redis.pipeline.side_effect = ConnectionError("Redis unreachable")

        svc, skip_counter = _make_quota_service(redis_client=redis)
        result = svc.check_quota("tenant-1", "API_CALLS_PER_DAY", amount=1)

        assert result.status == QuotaStatus.ALLOWED
        skip_counter.inc.assert_called_once()

    def test_fail_open_for_all_tenants_when_redis_down(self):
        """Every tenant gets ALLOWED when Redis is unavailable (fail-open is unconditional)."""
        redis = MagicMock()
        redis.pipeline.side_effect = ConnectionError("Redis unreachable")

        svc, _ = _make_quota_service(redis_client=redis)

        for tenant in ("tenant-1", "tenant-2", "tenant-3"):
            result = svc.check_quota(tenant, "API_CALLS_PER_DAY")
            assert result.status == QuotaStatus.ALLOWED, (
                f"tenant {tenant} must get ALLOWED when Redis is down"
            )

    def test_quota_resumes_normal_enforcement_after_redis_recovery(self):
        """After Redis recovers, quota enforcement resumes correctly."""
        redis = MagicMock()
        call_count = {"n": 0}

        def _pipeline_side_effect():
            call_count["n"] += 1
            if call_count["n"] <= 2:
                raise ConnectionError("Redis unreachable")
            pipe = MagicMock()
            pipe.execute.return_value = (5, True)
            return pipe

        redis.pipeline.side_effect = _pipeline_side_effect

        svc = QuotaService(redis_client=redis, get_limit_fn=lambda t, m: 1000)

        # First two calls fail-open during outage
        for _ in range(2):
            r = svc.check_quota("tenant-1", "API_CALLS_PER_DAY")
            assert r.status == QuotaStatus.ALLOWED

        # Third call — Redis recovered — enforces normally
        r = svc.check_quota("tenant-1", "API_CALLS_PER_DAY")
        assert r.status == QuotaStatus.ALLOWED
        assert r.current_usage == 5

    def test_skip_counter_increments_per_failed_check(self):
        """skip_counter is incremented once per quota check that fails open."""
        redis = MagicMock()
        redis.pipeline.side_effect = ConnectionError("Redis unreachable")

        svc, skip_counter = _make_quota_service(redis_client=redis)

        for _ in range(3):
            svc.check_quota("tenant-1", "API_CALLS_PER_DAY")

        assert skip_counter.inc.call_count == 3


# ── 8.5 Redis Unavailability (quota aspects) ─────────────────────────────────

class TestRedisUnavailabilityQuota:
    """8.5 Redis unavailability: Quota Service fails open (ALLOWED)."""

    def test_quota_service_allows_all_on_redis_loss(self):
        """Quota service returns ALLOWED for all metrics when Redis is down."""
        redis = MagicMock()
        redis.pipeline.side_effect = ConnectionError("Redis unreachable")

        svc, _ = _make_quota_service(redis_client=redis)

        for metric in ("API_CALLS_PER_DAY", "BYTES_PER_MONTH", "GPU_SECONDS_PER_MONTH"):
            result = svc.check_quota("tenant-x", metric)
            assert result.status == QuotaStatus.ALLOWED, (
                f"metric {metric} must be ALLOWED when Redis is down"
            )

    def test_unlimited_tenant_unaffected_by_redis_loss(self):
        """Unlimited tenants (limit=None) are immediately UNLIMITED without Redis contact."""
        redis = MagicMock()
        redis.pipeline.side_effect = ConnectionError("Redis unreachable")

        svc = QuotaService(
            redis_client=redis,
            get_limit_fn=lambda t, m: None,  # NULL limit → unlimited
        )
        result = svc.check_quota("enterprise-tenant", "API_CALLS_PER_DAY")

        assert result.status == QuotaStatus.UNLIMITED
        redis.pipeline.assert_not_called()
