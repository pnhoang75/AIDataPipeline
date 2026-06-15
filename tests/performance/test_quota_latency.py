"""Performance test §6.4 — Quota Service p99 latency < 5 ms.

Uses unittest.mock to replace Redis with an in-memory mock, isolating the
quota logic overhead from network round-trips.  This is a unit-level latency
proxy: if the check_quota code path stays under 5 ms with a trivially fast
"Redis", the real system (Redis INCR < 1 ms on loopback) will also meet the
SLO.

Pass criteria (§6.4):
    p99 across 1000 serial check_quota calls < 5 ms
"""

from __future__ import annotations

import os
import sys
import time
from statistics import quantiles
from unittest.mock import MagicMock

import pytest

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "..", "..", "services", "quota-service", "src"
        )
    ),
)

from quota_service import QuotaService, QuotaStatus  # noqa: E402

SAMPLE_SIZE = 1_000
P99_LIMIT_MS = 5.0


def _make_pipe(value: int) -> MagicMock:
    pipe = MagicMock()
    pipe.execute.return_value = [value, True]
    return pipe


def _make_redis(incr_value: int = 5) -> MagicMock:
    r = MagicMock()
    r.pipeline.return_value = _make_pipe(incr_value)
    return r


def _svc(limit: int = 10_000) -> QuotaService:
    return QuotaService(
        redis_client=_make_redis(),
        get_limit_fn=lambda _tenant, _metric: limit,
    )


class TestQuotaP99Latency:
    """§6.4 Quota Service latency SLO."""

    def test_check_quota_p99_under_5ms(self):
        """1000 serial check_quota calls — p99 must be < 5 ms."""
        svc = _svc()
        latencies_ms: list[float] = []

        for i in range(SAMPLE_SIZE):
            svc.redis = _make_redis(incr_value=(i % 9_000) + 1)
            t0 = time.perf_counter()
            result = svc.check_quota("perf-tenant", "API_CALLS_PER_DAY")
            elapsed = (time.perf_counter() - t0) * 1_000
            latencies_ms.append(elapsed)
            assert result.status == QuotaStatus.ALLOWED

        p50 = quantiles(latencies_ms, n=100)[49]
        p95 = quantiles(latencies_ms, n=100)[94]
        p99 = quantiles(latencies_ms, n=100)[98]
        print(
            f"\ncheck_quota latency (ms):  p50={p50:.3f}  p95={p95:.3f}  p99={p99:.3f}"
        )
        assert p99 < P99_LIMIT_MS, (
            f"p99 {p99:.3f} ms exceeds {P99_LIMIT_MS} ms SLO"
        )

    def test_record_usage_p99_under_5ms(self):
        """1000 serial record_usage calls — p99 must be < 5 ms."""
        latencies_ms: list[float] = []

        for i in range(SAMPLE_SIZE):
            r = MagicMock()
            r.exists.return_value = False
            r.setex.return_value = True
            r.incrby.return_value = i + 1
            svc = QuotaService(
                redis_client=r,
                get_limit_fn=lambda _tenant, _metric: 10_000,
            )
            t0 = time.perf_counter()
            result = svc.record_usage("perf-tenant", "API_CALLS_PER_DAY", 1, f"evt-{i}")
            elapsed = (time.perf_counter() - t0) * 1_000
            latencies_ms.append(elapsed)
            assert not result.deduped

        p50 = quantiles(latencies_ms, n=100)[49]
        p95 = quantiles(latencies_ms, n=100)[94]
        p99 = quantiles(latencies_ms, n=100)[98]
        print(
            f"\nrecord_usage latency (ms): p50={p50:.3f}  p95={p95:.3f}  p99={p99:.3f}"
        )
        assert p99 < P99_LIMIT_MS, (
            f"p99 {p99:.3f} ms exceeds {P99_LIMIT_MS} ms SLO"
        )

    def test_unlimited_tenant_p99_under_5ms(self):
        """check_quota for an unlimited tenant (NULL limit) — p99 < 5 ms."""
        svc = QuotaService(
            redis_client=_make_redis(),
            get_limit_fn=lambda _tenant, _metric: None,
        )
        latencies_ms: list[float] = []

        for _ in range(SAMPLE_SIZE):
            t0 = time.perf_counter()
            result = svc.check_quota("enterprise-tenant", "API_CALLS_PER_DAY")
            elapsed = (time.perf_counter() - t0) * 1_000
            latencies_ms.append(elapsed)
            assert result.status == QuotaStatus.UNLIMITED

        p99 = quantiles(latencies_ms, n=100)[98]
        print(f"\nunlimited fast-path p99: {p99:.3f} ms")
        assert p99 < P99_LIMIT_MS
