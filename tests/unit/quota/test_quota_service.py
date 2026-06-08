"""Unit tests for Quota Service — test plan §2.5."""
import os
import sys
from unittest.mock import MagicMock

import pytest

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "services", "quota-service", "src"),
)

from quota_service import QuotaService, QuotaStatus
from db_queries import get_effective_limit


# ─── helpers ─────────────────────────────────────────────────────────────────

def _pipe(incr_result):
    """Mock Redis pipeline whose execute() yields [incr_result, True]."""
    pipe = MagicMock()
    pipe.execute.return_value = [incr_result, True]
    return pipe


def _svc(redis_client, limit, skip_counter=None):
    return QuotaService(
        redis_client=redis_client,
        get_limit_fn=lambda tenant_id, metric: limit,
        skip_counter=skip_counter,
    )


# ─── tests ───────────────────────────────────────────────────────────────────

def test_check_quota_allows_under_limit():
    """Redis INCR returns 5; limit is 10; response is ALLOWED."""
    redis = MagicMock()
    redis.pipeline.return_value = _pipe(5)

    result = _svc(redis, limit=10).check_quota("tenant-a", "API_CALLS_PER_DAY")

    assert result.status == QuotaStatus.ALLOWED
    assert result.current_usage == 5
    assert result.limit == 10
    redis.decrby.assert_not_called()


def test_check_quota_denies_over_limit():
    """Redis INCR returns 11; limit is 10; DECRBY rollback called; response is DENIED."""
    redis = MagicMock()
    redis.pipeline.return_value = _pipe(11)

    result = _svc(redis, limit=10).check_quota("tenant-a", "API_CALLS_PER_DAY")

    assert result.status == QuotaStatus.DENIED
    assert result.current_usage == 10  # 11 - amount(1) after rollback
    assert result.limit == 10
    redis.decrby.assert_called_once()


def test_check_quota_unlimited_always_allows():
    """Enterprise tenant has limit=None (unlimited); response is UNLIMITED, Redis untouched."""
    redis = MagicMock()

    result = _svc(redis, limit=None).check_quota("enterprise-tenant", "API_CALLS_PER_DAY")

    assert result.status == QuotaStatus.UNLIMITED
    redis.pipeline.assert_not_called()


def test_record_usage_deduplicates_on_event_id():
    """Same event_id submitted twice; Redis dedup key prevents double-counting."""
    redis = MagicMock()
    redis.exists.side_effect = [0, 1]  # first: not seen; second: already seen
    redis.incrby.return_value = 1

    svc = QuotaService(redis_client=redis, get_limit_fn=lambda t, m: 100)

    first = svc.record_usage("tenant-a", "API_CALLS_PER_DAY", amount=1, event_id="evt-001")
    second = svc.record_usage("tenant-a", "API_CALLS_PER_DAY", amount=1, event_id="evt-001")

    assert first.deduped is False
    assert first.new_total == 1
    assert second.deduped is True
    redis.incrby.assert_called_once()  # only incremented once


def test_quota_check_respects_override():
    """quota_overrides row present; effective limit comes from override, not tier default."""
    session = MagicMock()
    # Override row returns 50 (tier default would be 1000)
    session.execute.return_value.fetchone.return_value = (50,)

    limit = get_effective_limit("tenant-a", "API_CALLS_PER_DAY", session)

    assert limit == 50
    # Only one DB query issued — override found, tier lookup skipped
    assert session.execute.call_count == 1


def test_fail_open_on_redis_unavailable():
    """Redis raises ConnectionError; gRPC returns ALLOWED with quota_check_skipped_total incremented."""
    redis = MagicMock()
    redis.pipeline.side_effect = ConnectionError("Redis connection refused")

    skip_counter = MagicMock()
    result = _svc(redis, limit=10, skip_counter=skip_counter).check_quota(
        "tenant-a", "API_CALLS_PER_DAY"
    )

    assert result.status == QuotaStatus.ALLOWED
    skip_counter.inc.assert_called_once()
