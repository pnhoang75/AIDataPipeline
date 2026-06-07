import json
import os
import sys
from unittest.mock import MagicMock, call, patch

import pytest

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "services", "rag-api", "src"),
)

from app import RagService, ServiceUnavailableError, _make_cache_key
from circuit_breaker import CircuitBreaker
from config import Config
from models import QueryRequest, QueryResult


# ─── helpers ─────────────────────────────────────────────────────────────────

def _make_hit(
    chunk_id="c1",
    text="The Eiffel Tower is in Paris",
    score=0.9,
    source_type="s3",
    doc_id="doc-1",
    metadata=None,
):
    hit = MagicMock()
    hit.chunk_id = chunk_id
    hit.text = text
    hit.score = score
    hit.source_type = source_type
    hit.doc_id = doc_id
    hit.metadata = metadata or {}
    return hit


def _make_hits(scores):
    return [_make_hit(chunk_id=f"c{i}", score=s) for i, s in enumerate(scores)]


def _make_service(
    milvus=None,
    redis=None,
    embedder=None,
    cb=None,
    milvus_hits=None,
):
    if milvus is None:
        milvus = MagicMock()
        milvus.search.return_value = milvus_hits if milvus_hits is not None else [_make_hit()]
    if redis is None:
        redis = MagicMock()
        redis.get.return_value = None  # cache miss by default
    if embedder is None:
        embedder = MagicMock()
        embedder.embed.return_value = [0.1] * 384
    if cb is None:
        cb = CircuitBreaker(failure_threshold=5, recovery_timeout=30.0, name="milvus")
    return RagService(milvus, redis, embedder, cb, Config()), milvus, redis, embedder, cb


def _query(service, query="what is Paris?", top_k=5, tenant_id="acme", **kwargs):
    return service.query(QueryRequest(query=query, top_k=top_k, **kwargs), tenant_id)


# ─── test_query_returns_top_k_results ────────────────────────────────────────

class TestTopK:
    def test_query_returns_top_k_results(self):
        """Milvus mock returns 10 hits; API enforces top_k=5 in response."""
        hits_10 = [_make_hit(chunk_id=f"c{i}", score=0.9 - i * 0.05) for i in range(10)]
        service, milvus, redis, embedder, cb = _make_service(milvus_hits=hits_10)

        response = _query(service, top_k=5)

        milvus.search.assert_called_once()
        call_kwargs = milvus.search.call_args[1]
        assert call_kwargs["top_k"] == 5
        assert len(response.results) == 5


# ─── test_cache_hit_skips_milvus ─────────────────────────────────────────────

class TestCacheHit:
    def test_cache_hit_skips_milvus(self):
        """Redis cache hit returns cached results; Milvus is never called."""
        cached_results = [
            {"chunk_id": "c1", "text": "cached text", "score": 0.8,
             "source_type": "s3", "doc_id": "doc-1", "metadata": {}}
        ]
        redis = MagicMock()
        redis.get.return_value = json.dumps(cached_results)

        service, milvus, _, _, _ = _make_service(redis=redis)

        response = _query(service)

        milvus.search.assert_not_called()
        assert response.cached is True
        assert len(response.results) == 1
        assert response.results[0].chunk_id == "c1"


# ─── test_cache_miss_stores_result ───────────────────────────────────────────

class TestCacheMiss:
    def test_cache_miss_stores_result(self):
        """Cache miss → Milvus search → Redis setex called with TTL 300 s."""
        redis = MagicMock()
        redis.get.return_value = None

        service, milvus, _, _, _ = _make_service(redis=redis)

        _query(service, query="test query", top_k=5, tenant_id="acme")

        expected_key = _make_cache_key("test query", 5, None, "acme_docs")
        redis.setex.assert_called_once()
        key_arg, ttl_arg, _ = redis.setex.call_args[0]
        assert key_arg == expected_key
        assert ttl_arg == 300


# ─── test_redis_unavailable_degrades_gracefully ──────────────────────────────

class TestRedisDegradation:
    def test_redis_unavailable_degrades_gracefully(self):
        """Redis ConnectionError on get → request still returns 200 with Milvus results."""
        redis = MagicMock()
        redis.get.side_effect = ConnectionError("Redis is down")

        service, milvus, _, _, _ = _make_service(redis=redis)

        response = _query(service)

        # Should not raise; Milvus is still called
        milvus.search.assert_called_once()
        assert len(response.results) >= 1
        assert response.cached is False
        # setex is NOT called because cache_available=False
        redis.setex.assert_not_called()


# ─── test_collection_derived_from_tenant_header ──────────────────────────────

class TestTenantCollection:
    def test_collection_derived_from_tenant_header(self):
        """X-Tenant-ID: acme → Milvus searched on collection 'acme_docs'."""
        service, milvus, _, _, _ = _make_service()

        _query(service, tenant_id="acme")

        call_kwargs = milvus.search.call_args[1]
        assert call_kwargs["collection"] == "acme_docs"

    def test_collection_not_derived_from_request_body(self):
        """collection field in request body is ignored; only X-Tenant-ID matters."""
        service, milvus, _, _, _ = _make_service()

        service.query(
            QueryRequest(query="test", collection="evil_tenant_docs"),
            "acme",
        )

        call_kwargs = milvus.search.call_args[1]
        assert call_kwargs["collection"] == "acme_docs"
        assert call_kwargs["collection"] != "evil_tenant_docs"


# ─── test_milvus_circuit_breaker_opens_after_5_failures ──────────────────────

class TestCircuitBreaker:
    def test_milvus_circuit_breaker_opens_after_5_failures(self):
        """5 consecutive Milvus TimeoutErrors → circuit opens; 6th request raises without calling Milvus."""
        milvus = MagicMock()
        milvus.search.side_effect = TimeoutError("connection timed out")

        service, milvus, _, _, cb = _make_service(milvus=milvus)

        for _ in range(5):
            with pytest.raises(ServiceUnavailableError):
                _query(service)

        assert cb._state == CircuitBreaker.OPEN
        assert milvus.search.call_count == 5

        # 6th request — circuit is open; Milvus must NOT be called
        with pytest.raises(ServiceUnavailableError):
            _query(service)

        assert milvus.search.call_count == 5  # still 5

    def test_circuit_breaker_half_open_probe_after_30s(self):
        """Circuit open for ≥30 s → next call is a half-open probe; on success, circuit closes."""
        milvus = MagicMock()
        milvus.search.side_effect = TimeoutError("timeout")

        service, milvus, redis, embedder, cb = _make_service(milvus=milvus)

        # Open the circuit with 5 failures
        for _ in range(5):
            with pytest.raises(ServiceUnavailableError):
                _query(service)

        assert cb._state == CircuitBreaker.OPEN
        opened_at = cb._opened_at

        # Milvus now works
        milvus.search.side_effect = None
        milvus.search.return_value = [_make_hit()]
        milvus.search.reset_mock()

        # Simulate 31 seconds having elapsed since the circuit opened
        with patch("circuit_breaker.time.monotonic", return_value=opened_at + 31.0):
            response = _query(service)

        # Probe was made (Milvus was called once)
        assert milvus.search.call_count == 1
        # Circuit closed after successful probe
        assert cb._state == CircuitBreaker.CLOSED
        assert len(response.results) >= 1


# ─── test_source_filter_passed_to_milvus_search ──────────────────────────────

class TestSourceFilter:
    def test_source_filter_passed_to_milvus_search(self):
        """source_filter='s3' is forwarded to milvus.search as scalar filter argument."""
        service, milvus, _, _, _ = _make_service()

        service.query(QueryRequest(query="test", source_filter="s3"), "acme")

        call_kwargs = milvus.search.call_args[1]
        assert call_kwargs["source_filter"] == "s3"


# ─── test_min_score_filters_results ──────────────────────────────────────────

class TestMinScore:
    def test_min_score_filters_results(self):
        """Milvus returns scores [0.9, 0.6, 0.3]; min_score=0.5 returns only first two."""
        hits = _make_hits([0.9, 0.6, 0.3])
        service, milvus, _, _, _ = _make_service(milvus_hits=hits)

        response = service.query(
            QueryRequest(query="test", top_k=10, min_score=0.5),
            "acme",
        )

        assert len(response.results) == 2
        assert all(r.score >= 0.5 for r in response.results)
