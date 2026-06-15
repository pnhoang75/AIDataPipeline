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


# ─── test_usage_publisher_called_on_query ────────────────────────────────────

class TestUsagePublishing:
    def test_usage_event_published_on_cache_miss(self):
        """Successful non-cached query publishes pipeline.rag.query usage event."""
        usage_publisher = MagicMock()
        service, milvus, redis, embedder, cb = _make_service()
        service._usage_publisher = usage_publisher

        _query(service, tenant_id="acme")

        usage_publisher.publish_rag_query.assert_called_once()
        call_kwargs = usage_publisher.publish_rag_query.call_args[1]
        assert call_kwargs["tenant_id"] == "acme"
        assert call_kwargs["cached"] is False
        assert isinstance(call_kwargs["duration_ms"], float)
        assert call_kwargs["duration_ms"] >= 0

    def test_usage_event_published_on_cache_hit(self):
        """Cache hit also publishes usage event with cached=True."""
        cached_results = [
            {"chunk_id": "c1", "text": "text", "score": 0.8,
             "source_type": "s3", "doc_id": "doc-1", "metadata": {}}
        ]
        redis = MagicMock()
        redis.get.return_value = json.dumps(cached_results)

        usage_publisher = MagicMock()
        service, _, _, _, _ = _make_service(redis=redis)
        service._usage_publisher = usage_publisher

        _query(service, tenant_id="tenant-x")

        usage_publisher.publish_rag_query.assert_called_once()
        call_kwargs = usage_publisher.publish_rag_query.call_args[1]
        assert call_kwargs["tenant_id"] == "tenant-x"
        assert call_kwargs["cached"] is True

    def test_no_usage_publisher_does_not_raise(self):
        """No usage_publisher set → query completes normally without error."""
        service, _, _, _, _ = _make_service()
        service._usage_publisher = None

        response = _query(service)

        assert response is not None

    def test_usage_publisher_failure_does_not_abort_query(self):
        """UsagePublisher raising an error does not propagate to the caller."""
        usage_publisher = MagicMock()
        usage_publisher.publish_rag_query.side_effect = Exception("Kafka unavailable")
        service, _, _, _, _ = _make_service()
        service._usage_publisher = usage_publisher

        # Should not raise
        response = _query(service)
        assert response is not None


# ─── test_usage_publisher_unit ────────────────────────────────────────────────

class TestUsagePublisherUnit:
    def test_publish_rag_query_produces_cloudevent(self):
        """UsagePublisher.publish_rag_query produces a CloudEvent to the correct topic."""
        from events import UsagePublisher

        producer = MagicMock()
        pub = UsagePublisher(producer, topic="usage-events")

        pub.publish_rag_query(
            tenant_id="acme",
            duration_ms=42.5,
            result_count=3,
            cached=False,
        )

        producer.produce.assert_called_once()
        call_kwargs = producer.produce.call_args[1]
        assert call_kwargs["topic"] == "usage-events"
        assert call_kwargs["key"] == b"acme"
        payload = json.loads(call_kwargs["value"].decode())
        assert payload["type"] == "pipeline.rag.query"
        assert payload["subject"] == "acme"
        assert payload["data"]["tenant_id"] == "acme"
        assert payload["data"]["duration_ms"] == 42.5
        assert payload["data"]["result_count"] == 3
        assert payload["data"]["cached"] is False

    def test_publish_rag_query_producer_failure_is_swallowed(self):
        """Producer.produce() raising does not propagate out of publish_rag_query."""
        from events import UsagePublisher

        producer = MagicMock()
        producer.produce.side_effect = Exception("broker unavailable")
        pub = UsagePublisher(producer, topic="usage-events")

        # Should not raise
        pub.publish_rag_query(tenant_id="t", duration_ms=1.0, result_count=0, cached=False)


# ─── OTel span tests ─────────────────────────────────────────────────────────

import app as app_mod
import milvus_searcher as ms_mod
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


@pytest.fixture
def span_exporter_rag():
    """Patch both app and milvus_searcher module tracers; restore on teardown."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")

    orig_app_tracer = app_mod._tracer
    orig_ms_tracer = ms_mod._tracer
    app_mod._tracer = tracer
    ms_mod._tracer = tracer
    yield exporter
    app_mod._tracer = orig_app_tracer
    ms_mod._tracer = orig_ms_tracer
    exporter.clear()


class TestOTelSpans:
    def test_milvus_search_span_emitted(self, span_exporter_rag):
        """milvus_searcher.search() emits a milvus.search span with expected attributes."""
        from milvus_searcher import MilvusSearcher

        searcher = MilvusSearcher.__new__(MilvusSearcher)
        searcher._host = "localhost"
        searcher._port = 19530
        searcher._dim = 384

        with patch("pymilvus.Collection"), patch(
            "pymilvus.utility"
        ) as mock_util:
            mock_util.has_collection.return_value = False
            searcher.search("tenant1_docs", [0.1] * 384, top_k=5)

        spans = span_exporter_rag.get_finished_spans()
        span = next((s for s in spans if s.name == "milvus.search"), None)
        assert span is not None, "milvus.search span was not emitted"
        assert span.attributes["db.system"] == "milvus"
        assert span.attributes["milvus.collection"] == "tenant1_docs"
        assert span.attributes["milvus.top_k"] == 5

    def test_redis_get_span_on_cache_miss(self, span_exporter_rag):
        """RagService.query() emits a redis.get span on every request."""
        service, _, redis, _, _ = _make_service()
        redis.get.return_value = None  # cache miss

        _query(service)

        spans = span_exporter_rag.get_finished_spans()
        span = next((s for s in spans if s.name == "redis.get"), None)
        assert span is not None, "redis.get span was not emitted"
        assert span.attributes["db.system"] == "redis"
        assert span.attributes["db.operation"] == "GET"
        assert span.attributes["redis.cache_hit"] is False

    def test_redis_get_span_on_cache_hit(self, span_exporter_rag):
        """RagService.query() emits redis.get with cache_hit=True on a cache hit."""
        service, _, redis, _, _ = _make_service()
        cached = json.dumps([{"chunk_id": "c1", "text": "x", "score": 0.9,
                               "source_type": "s3", "doc_id": "d1", "metadata": {}}])
        redis.get.return_value = cached.encode()

        _query(service)

        spans = span_exporter_rag.get_finished_spans()
        span = next((s for s in spans if s.name == "redis.get"), None)
        assert span is not None
        assert span.attributes["redis.cache_hit"] is True

    def test_redis_setex_span_on_cache_miss(self, span_exporter_rag):
        """RagService.query() emits a redis.setex span when storing a new result."""
        service, _, redis, _, _ = _make_service()
        redis.get.return_value = None  # cache miss → triggers setex

        _query(service)

        spans = span_exporter_rag.get_finished_spans()
        span = next((s for s in spans if s.name == "redis.setex"), None)
        assert span is not None, "redis.setex span was not emitted"
        assert span.attributes["db.system"] == "redis"
        assert span.attributes["db.operation"] == "SETEX"
