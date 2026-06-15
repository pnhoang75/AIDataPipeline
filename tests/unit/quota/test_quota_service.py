"""Unit tests for Quota Service — test plan §2.5."""
import os
import sys
from unittest.mock import MagicMock

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry import trace

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


def test_checks_counter_incremented_on_allowed():
    """quota_checks_total is incremented with status=ALLOWED on a successful check."""
    redis = MagicMock()
    redis.pipeline.return_value = _pipe(5)

    checks_counter = MagicMock()
    svc = QuotaService(
        redis_client=redis,
        get_limit_fn=lambda t, m: 10,
        checks_counter=checks_counter,
    )
    result = svc.check_quota("tenant-a", "API_CALLS_PER_DAY")

    assert result.status == QuotaStatus.ALLOWED
    checks_counter.labels.assert_called_once_with(
        tenant_id="tenant-a", metric="API_CALLS_PER_DAY", status="ALLOWED"
    )
    checks_counter.labels.return_value.inc.assert_called_once()


def test_exceeded_counter_incremented_on_denied():
    """quota_exceeded_total is incremented when a check is denied."""
    redis = MagicMock()
    redis.pipeline.return_value = _pipe(11)

    checks_counter = MagicMock()
    exceeded_counter = MagicMock()
    svc = QuotaService(
        redis_client=redis,
        get_limit_fn=lambda t, m: 10,
        checks_counter=checks_counter,
        exceeded_counter=exceeded_counter,
    )
    result = svc.check_quota("tenant-a", "API_CALLS_PER_DAY")

    assert result.status == QuotaStatus.DENIED
    exceeded_counter.labels.assert_called_once_with(
        tenant_id="tenant-a", metric="API_CALLS_PER_DAY"
    )
    exceeded_counter.labels.return_value.inc.assert_called_once()


def test_usage_ratio_gauge_set_on_check():
    """quota_usage_ratio gauge is set to current/limit after an ALLOWED check."""
    redis = MagicMock()
    redis.pipeline.return_value = _pipe(5)

    usage_ratio_gauge = MagicMock()
    svc = QuotaService(
        redis_client=redis,
        get_limit_fn=lambda t, m: 10,
        usage_ratio_gauge=usage_ratio_gauge,
    )
    svc.check_quota("tenant-a", "API_CALLS_PER_DAY")

    usage_ratio_gauge.labels.assert_called_once_with(
        tenant_id="tenant-a", metric="API_CALLS_PER_DAY"
    )
    usage_ratio_gauge.labels.return_value.set.assert_called_once_with(0.5)


def test_checks_counter_unlimited_tenant():
    """quota_checks_total is incremented with status=UNLIMITED for enterprise tenants."""
    redis = MagicMock()

    checks_counter = MagicMock()
    svc = QuotaService(
        redis_client=redis,
        get_limit_fn=lambda t, m: None,  # unlimited
        checks_counter=checks_counter,
    )
    result = svc.check_quota("enterprise", "API_CALLS_PER_DAY")

    assert result.status == QuotaStatus.UNLIMITED
    checks_counter.labels.assert_called_once_with(
        tenant_id="enterprise", metric="API_CALLS_PER_DAY", status="UNLIMITED"
    )
    checks_counter.labels.return_value.inc.assert_called_once()


# ─── OTel span tests ─────────────────────────────────────────────────────────

import quota_service as qs_mod


@pytest.fixture
def span_exporter_quota():
    """Patch quota_service module tracer; restore on teardown."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")

    orig = qs_mod._tracer
    qs_mod._tracer = tracer
    yield exporter
    qs_mod._tracer = orig
    exporter.clear()


class TestOTelSpans:
    def test_check_quota_emits_redis_incrby_span(self, span_exporter_quota):
        """check_quota (increment_on_allow=True) emits a redis.quota.incrby span."""
        redis = MagicMock()
        redis.pipeline.return_value = _pipe(5)

        _svc(redis, limit=10).check_quota("tenant-a", "API_CALLS_PER_DAY")

        spans = span_exporter_quota.get_finished_spans()
        span = next((s for s in spans if s.name == "redis.quota.incrby"), None)
        assert span is not None, "redis.quota.incrby span was not emitted"
        assert span.attributes["db.system"] == "redis"
        assert span.attributes["quota.tenant_id"] == "tenant-a"
        assert span.attributes["quota.metric"] == "API_CALLS_PER_DAY"
        assert span.attributes["quota.current_after_incr"] == 5

    def test_check_quota_readonly_emits_redis_get_span(self, span_exporter_quota):
        """check_quota (increment_on_allow=False) emits a redis.quota.get span."""
        redis = MagicMock()
        redis.get.return_value = "3"

        _svc(redis, limit=10).check_quota(
            "tenant-b", "API_CALLS_PER_DAY", increment_on_allow=False
        )

        spans = span_exporter_quota.get_finished_spans()
        span = next((s for s in spans if s.name == "redis.quota.get"), None)
        assert span is not None, "redis.quota.get span was not emitted"
        assert span.attributes["db.system"] == "redis"
        assert span.attributes["quota.tenant_id"] == "tenant-b"
        assert span.attributes["quota.current"] == 3

    def test_record_usage_emits_span(self, span_exporter_quota):
        """record_usage emits a redis.quota.record_usage span."""
        redis = MagicMock()
        redis.exists.return_value = 0
        redis.incrby.return_value = 7

        svc = QuotaService(redis_client=redis, get_limit_fn=lambda t, m: 100)
        svc.record_usage("tenant-a", "API_CALLS_PER_DAY", amount=1, event_id="evt-x")

        spans = span_exporter_quota.get_finished_spans()
        span = next((s for s in spans if s.name == "redis.quota.record_usage"), None)
        assert span is not None, "redis.quota.record_usage span was not emitted"
        assert span.attributes["db.system"] == "redis"
        assert span.attributes["quota.tenant_id"] == "tenant-a"
        assert span.attributes["quota.metric"] == "API_CALLS_PER_DAY"
        assert span.attributes["quota.deduped"] is False
        assert span.attributes["quota.new_total"] == 7

    def test_record_usage_dedup_span_marked(self, span_exporter_quota):
        """record_usage emits span with deduped=True when event already seen."""
        redis = MagicMock()
        redis.exists.return_value = 1  # already seen

        svc = QuotaService(redis_client=redis, get_limit_fn=lambda t, m: 100)
        result = svc.record_usage("tenant-a", "API_CALLS_PER_DAY", amount=1, event_id="dup")

        assert result.deduped is True
        spans = span_exporter_quota.get_finished_spans()
        span = next((s for s in spans if s.name == "redis.quota.record_usage"), None)
        assert span is not None
        assert span.attributes["quota.deduped"] is True
