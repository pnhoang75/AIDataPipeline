"""Core quota enforcement logic — Redis INCR pattern + fail-open on Redis down.

Designed for direct testability: dependencies (Redis client, limit resolver,
Prometheus counter) are injected at construction time.
"""
import logging
from enum import IntEnum
from typing import Callable, Optional

from opentelemetry import trace

logger = logging.getLogger(__name__)
_tracer = trace.get_tracer(__name__)


class QuotaStatus(IntEnum):
    UNSPECIFIED = 0
    ALLOWED = 1
    DENIED = 2
    UNLIMITED = 3  # NULL limit — enterprise tenants


class CheckResult:
    __slots__ = ("status", "current_usage", "limit", "deny_reason")

    def __init__(
        self,
        status: QuotaStatus,
        current_usage: int = 0,
        limit: int = 0,
        deny_reason: str = "",
    ) -> None:
        self.status = status
        self.current_usage = current_usage
        self.limit = limit
        self.deny_reason = deny_reason


class RecordResult:
    __slots__ = ("new_total", "deduped")

    def __init__(self, new_total: int = 0, deduped: bool = False) -> None:
        self.new_total = new_total
        self.deduped = deduped


# Default TTL per metric type (seconds).
_METRIC_TTL: dict[str, int] = {
    "API_CALLS_PER_DAY": 86_400,
    "API_CALLS_PER_MINUTE": 60,
    "BYTES_PER_MONTH": 86_400 * 31,
    "GPU_SECONDS_PER_MONTH": 86_400 * 31,
    "VECTORS_STORED": 86_400 * 365,
    "CONCURRENT_WORKERS": 86_400,
    "CONNECTOR_COUNT": 86_400,
    "USERS_PER_TENANT": 86_400,
}
_DEFAULT_TTL = 86_400


class QuotaService:
    """Quota enforcement: check, record, and query usage.

    Args:
        redis_client: redis-py compatible client (sync).
        get_limit_fn: callable(tenant_id, metric) → int | None
            Returns the effective limit (override takes priority over tier
            default).  None means unlimited (Enterprise / NULL in DB).
        skip_counter: optional object with an ``.inc()`` method, incremented
            when Redis is unavailable.  Pass a Prometheus Counter or a mock.
        checks_counter: optional Counter with labels [tenant_id, metric, status].
        exceeded_counter: optional Counter with labels [tenant_id, metric].
        usage_ratio_gauge: optional Gauge with labels [tenant_id, metric].
    """

    def __init__(
        self,
        redis_client,
        get_limit_fn: Callable[[str, str], Optional[int]],
        skip_counter=None,
        checks_counter=None,
        exceeded_counter=None,
        usage_ratio_gauge=None,
    ) -> None:
        self.redis = redis_client
        self._get_limit = get_limit_fn
        self._skip_counter = skip_counter
        self._checks_counter = checks_counter
        self._exceeded_counter = exceeded_counter
        self._usage_ratio_gauge = usage_ratio_gauge

    # ── metrics helpers ───────────────────────────────────────────────────────

    def _record_check_metrics(self, tenant_id: str, metric: str, result: "CheckResult") -> None:
        status_name = QuotaStatus(result.status).name
        if self._checks_counter is not None:
            self._checks_counter.labels(
                tenant_id=tenant_id, metric=metric, status=status_name
            ).inc()
        if result.status == QuotaStatus.DENIED and self._exceeded_counter is not None:
            self._exceeded_counter.labels(tenant_id=tenant_id, metric=metric).inc()
        if self._usage_ratio_gauge is not None and result.limit > 0:
            self._usage_ratio_gauge.labels(tenant_id=tenant_id, metric=metric).set(
                result.current_usage / result.limit
            )

    # ── key helpers ──────────────────────────────────────────────────────────

    def _quota_key(self, tenant_id: str, metric: str) -> str:
        return f"quota:{tenant_id}:{metric}"

    def _dedup_key(self, event_id: str) -> str:
        return f"dedup:{event_id}"

    def _ttl(self, metric: str) -> int:
        return _METRIC_TTL.get(metric, _DEFAULT_TTL)

    # ── CheckQuota ────────────────────────────────────────────────────────────

    def check_quota(
        self,
        tenant_id: str,
        metric: str,
        amount: int = 1,
        increment_on_allow: bool = True,
    ) -> CheckResult:
        """Atomic check-and-increment (Redis INCR pattern).

        Returns UNLIMITED immediately for tenants with no cap (limit=None).
        Fails open on Redis errors: returns ALLOWED and increments the
        skip_counter so the anomaly is observable.
        """
        limit = self._get_limit(tenant_id, metric)
        if limit is None:
            if self._checks_counter is not None:
                self._checks_counter.labels(
                    tenant_id=tenant_id, metric=metric, status="UNLIMITED"
                ).inc()
            return CheckResult(QuotaStatus.UNLIMITED, current_usage=0, limit=0)

        try:
            key = self._quota_key(tenant_id, metric)
            if increment_on_allow:
                with _tracer.start_as_current_span("redis.quota.incrby") as span:
                    span.set_attribute("db.system", "redis")
                    span.set_attribute("db.operation", "INCRBY+EXPIRE")
                    span.set_attribute("quota.tenant_id", tenant_id)
                    span.set_attribute("quota.metric", metric)
                    pipe = self.redis.pipeline()
                    pipe.incrby(key, amount)
                    pipe.expire(key, self._ttl(metric))
                    current, _ = pipe.execute()
                    span.set_attribute("quota.current_after_incr", current)
                if current > limit:
                    self.redis.decrby(key, amount)  # rollback
                    result = CheckResult(
                        QuotaStatus.DENIED,
                        current_usage=current - amount,
                        limit=limit,
                        deny_reason=f"Quota exceeded for metric {metric}",
                    )
                    self._record_check_metrics(tenant_id, metric, result)
                    return result
                result = CheckResult(QuotaStatus.ALLOWED, current_usage=current, limit=limit)
                self._record_check_metrics(tenant_id, metric, result)
                return result
            else:
                with _tracer.start_as_current_span("redis.quota.get") as span:
                    span.set_attribute("db.system", "redis")
                    span.set_attribute("db.operation", "GET")
                    span.set_attribute("quota.tenant_id", tenant_id)
                    span.set_attribute("quota.metric", metric)
                    # Read-only check — caller records separately
                    current = int(self.redis.get(key) or 0)
                    span.set_attribute("quota.current", current)
                if current + amount > limit:
                    result = CheckResult(
                        QuotaStatus.DENIED, current_usage=current, limit=limit
                    )
                    self._record_check_metrics(tenant_id, metric, result)
                    return result
                result = CheckResult(QuotaStatus.ALLOWED, current_usage=current, limit=limit)
                self._record_check_metrics(tenant_id, metric, result)
                return result

        except Exception:
            if self._skip_counter is not None:
                self._skip_counter.inc()
            logger.warning(
                "Redis unavailable during quota check for %s/%s; failing open",
                tenant_id,
                metric,
            )
            return CheckResult(QuotaStatus.ALLOWED, current_usage=0, limit=limit)

    # ── RecordUsage ───────────────────────────────────────────────────────────

    def record_usage(
        self,
        tenant_id: str,
        metric: str,
        amount: int,
        event_id: str,
    ) -> RecordResult:
        """Record usage with idempotency: duplicate event_ids within 24 h are ignored."""
        with _tracer.start_as_current_span("redis.quota.record_usage") as span:
            span.set_attribute("db.system", "redis")
            span.set_attribute("quota.tenant_id", tenant_id)
            span.set_attribute("quota.metric", metric)
            span.set_attribute("quota.amount", amount)
            dedup_key = self._dedup_key(event_id)
            if self.redis.exists(dedup_key):
                span.set_attribute("quota.deduped", True)
                return RecordResult(deduped=True)
            # Mark event_id as seen (24-hour TTL)
            self.redis.setex(dedup_key, 86_400, 1)
            key = self._quota_key(tenant_id, metric)
            new_total = int(self.redis.incrby(key, amount))
            span.set_attribute("quota.deduped", False)
            span.set_attribute("quota.new_total", new_total)
        return RecordResult(new_total=new_total, deduped=False)

    # ── GetUsage ──────────────────────────────────────────────────────────────

    def get_usage(self, tenant_id: str, metric: str) -> dict:
        """Return current usage and effective limit for a tenant + metric."""
        key = self._quota_key(tenant_id, metric)
        current = int(self.redis.get(key) or 0)
        limit = self._get_limit(tenant_id, metric)
        return {
            "tenant_id": tenant_id,
            "metric": metric,
            "current": current,
            "limit": 0 if limit is None else limit,
            "unlimited": limit is None,
        }
