"""
Chaos tests for rag-api — test plan §8.2, §8.3, §8.5 (RAG aspects).

  8.2  PostgreSQL primary failure → RAG API query path is DB-independent
  8.3  Milvus unavailability     → circuit opens after 5 failures; closes after probe
  8.5  Redis unavailability      → RAG API degrades gracefully (200, no cache)
"""
from __future__ import annotations

import inspect
import os
import sys
import time
from unittest.mock import MagicMock, patch

import pytest

_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_RAG_SRC = os.path.join(_REPO, "services", "rag-api", "src")
if _RAG_SRC not in sys.path:
    sys.path.insert(0, _RAG_SRC)

from circuit_breaker import CircuitBreaker, CircuitBreakerOpen  # noqa: E402
from app import RagService, ServiceUnavailableError  # noqa: E402
from config import Config as RagConfig  # noqa: E402
from models import QueryRequest  # noqa: E402


# ── helpers ───────────────────────────────────────────────────────────────────

def _make_hit(chunk_id="c-1", text="hello", score=0.9, source_type="s3", doc_id="doc-1"):
    h = MagicMock()
    h.chunk_id = chunk_id
    h.text = text
    h.score = score
    h.source_type = source_type
    h.doc_id = doc_id
    h.metadata = {}
    return h


def _make_rag_service(
    milvus_hits=None,
    redis_get_side_effect=None,
    milvus_search_side_effect=None,
    cb_failure_threshold=5,
    cb_recovery_timeout=30.0,
):
    milvus = MagicMock()
    if milvus_search_side_effect is not None:
        milvus.search.side_effect = milvus_search_side_effect
    else:
        milvus.search.return_value = milvus_hits or []

    redis = MagicMock()
    if redis_get_side_effect is not None:
        redis.get.side_effect = redis_get_side_effect
    else:
        redis.get.return_value = None  # cache miss

    embedder = MagicMock()
    embedder.embed.return_value = [0.1] * 384

    cb = CircuitBreaker(
        failure_threshold=cb_failure_threshold,
        recovery_timeout=cb_recovery_timeout,
        name="milvus",
    )

    cfg = RagConfig()
    cfg.redis_ttl_seconds = 300

    svc = RagService(
        milvus_searcher=milvus,
        redis_client=redis,
        embedder=embedder,
        circuit_breaker=cb,
        cfg=cfg,
    )
    return svc, milvus, redis, cb


# ── 8.2 PostgreSQL Primary Failure ───────────────────────────────────────────

class TestPostgresPrimaryFailure:
    """8.2 PostgreSQL primary failure: RAG API query path is DB-independent."""

    def test_rag_query_succeeds_with_no_postgres_dependency(self):
        """RAG API POST /v1/query returns results without touching any PostgreSQL client."""
        hit = _make_hit(chunk_id="c-1", text="Paris is the capital", score=0.92)
        service, milvus, redis, cb = _make_rag_service(milvus_hits=[hit])

        assert not hasattr(service, "_db"), "RagService must not hold a DB connection"
        assert not hasattr(service, "_pg"), "RagService must not hold a PostgreSQL client"

        req = QueryRequest(query="capital of France", top_k=1)
        resp = service.query(req, "tenant-1")

        assert len(resp.results) == 1
        assert resp.results[0].chunk_id == "c-1"

    def test_rag_query_uses_only_redis_and_milvus(self):
        """Confirmed: query path calls only embedder, redis, and milvus — no SQL."""
        service, milvus, redis, cb = _make_rag_service()

        req = QueryRequest(query="test query", top_k=3)
        service.query(req, "tenant-1")

        service._embedder.embed.assert_called_once()
        milvus.search.assert_called_once()

    def test_rag_source_has_no_sql_markers(self):
        """RagService.query source code contains no SQL client imports or calls."""
        source = inspect.getsource(RagService.query)

        sql_markers = ["sqlalchemy", "psycopg2", "pg8000", "asyncpg", ".execute("]
        for marker in sql_markers:
            assert marker not in source, (
                f"RagService.query must not contain '{marker}' — "
                "it must be independent of PostgreSQL for HA failover transparency"
            )


# ── 8.3 Milvus Unavailability ─────────────────────────────────────────────────

class TestMilvusUnavailability:
    """8.3 Milvus unavailability: circuit opens after 5 failures; closes after probe."""

    def test_circuit_opens_after_5_consecutive_failures(self):
        """Five Milvus search failures trip the circuit breaker to OPEN state."""
        service, milvus, redis, cb = _make_rag_service(
            milvus_search_side_effect=ConnectionError("milvus unreachable"),
            cb_failure_threshold=5,
        )
        req = QueryRequest(query="test", top_k=3)
        for _ in range(5):
            with pytest.raises(ServiceUnavailableError):
                service.query(req, "tenant-1")

        assert cb.state == CircuitBreaker.OPEN

    def test_subsequent_queries_rejected_without_milvus_call_when_circuit_open(self):
        """Once circuit is open, all calls raise ServiceUnavailableError without calling Milvus."""
        service, milvus, redis, cb = _make_rag_service(
            milvus_search_side_effect=ConnectionError("milvus unreachable"),
            cb_failure_threshold=5,
        )
        req = QueryRequest(query="test", top_k=3)
        for _ in range(5):
            try:
                service.query(req, "tenant-1")
            except ServiceUnavailableError:
                pass

        assert cb.state == CircuitBreaker.OPEN
        milvus.search.reset_mock()

        with pytest.raises(ServiceUnavailableError):
            service.query(req, "tenant-1")

        milvus.search.assert_not_called()

    def test_circuit_half_opens_after_recovery_timeout(self):
        """After recovery_timeout the circuit enters HALF_OPEN state."""
        service, milvus, redis, cb = _make_rag_service(
            milvus_search_side_effect=ConnectionError("milvus unreachable"),
            cb_failure_threshold=5,
            cb_recovery_timeout=0.1,
        )
        req = QueryRequest(query="test", top_k=3)
        for _ in range(5):
            try:
                service.query(req, "tenant-1")
            except ServiceUnavailableError:
                pass

        assert cb.state == CircuitBreaker.OPEN
        time.sleep(0.15)
        assert cb.state == CircuitBreaker.HALF_OPEN

    def test_successful_probe_closes_circuit(self):
        """A successful Milvus call in HALF_OPEN state closes the circuit."""
        service, milvus, redis, cb = _make_rag_service(
            milvus_search_side_effect=ConnectionError("milvus unreachable"),
            cb_failure_threshold=5,
            cb_recovery_timeout=0.1,
        )
        req = QueryRequest(query="test", top_k=3)
        for _ in range(5):
            try:
                service.query(req, "tenant-1")
            except ServiceUnavailableError:
                pass

        time.sleep(0.15)
        assert cb.state == CircuitBreaker.HALF_OPEN

        # Milvus recovers
        milvus.search.side_effect = None
        milvus.search.return_value = []
        service.query(req, "tenant-1")

        assert cb.state == CircuitBreaker.CLOSED


# ── 8.5 Redis Unavailability (RAG aspects) ────────────────────────────────────

class TestRedisUnavailabilityRag:
    """8.5 Redis unavailability: RAG API returns 200 in degraded mode (no cache)."""

    def test_rag_query_returns_results_when_redis_raises(self):
        """ConnectionError from Redis does not prevent Milvus results being returned."""
        hit = _make_hit(chunk_id="c-1", text="The sky is blue", score=0.88)
        service, milvus, redis, cb = _make_rag_service(
            milvus_hits=[hit],
            redis_get_side_effect=ConnectionError("Redis unreachable"),
        )

        req = QueryRequest(query="colour of sky", top_k=1)
        resp = service.query(req, "tenant-1")

        assert len(resp.results) == 1
        assert resp.cached is False
        milvus.search.assert_called_once()

    def test_redis_unavailable_counter_incremented(self):
        """redis_unavailable_total counter increments when Redis raises ConnectionError."""
        service, milvus, redis, cb = _make_rag_service(
            redis_get_side_effect=ConnectionError("Redis unreachable"),
        )

        with patch("app._METRICS_ENABLED", True), patch("app._REDIS_UNAVAIL") as mock_counter:
            req = QueryRequest(query="test", top_k=1)
            service.query(req, "tenant-1")
            mock_counter.inc.assert_called_once()

    def test_cache_write_failure_does_not_abort_response(self):
        """setex failure after Milvus search is silently suppressed; result still returned."""
        hit = _make_hit(chunk_id="c-2", text="text", score=0.75)
        service, milvus, redis, cb = _make_rag_service(milvus_hits=[hit])
        redis.get.return_value = None  # cache miss
        redis.setex.side_effect = ConnectionError("Redis unreachable on write")

        req = QueryRequest(query="test query", top_k=1)
        resp = service.query(req, "tenant-1")

        assert len(resp.results) == 1
