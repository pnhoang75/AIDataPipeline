import hashlib
import json
import time as _time
import uuid
from typing import List, Optional

import structlog
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from opentelemetry import trace

from logging_config import bind_request_context, clear_request_context
from circuit_breaker import CircuitBreaker, CircuitBreakerOpen
from config import Config, config as _default_config
from models import QueryRequest, QueryResponse, QueryResult

logger = structlog.get_logger(__name__)
_tracer = trace.get_tracer(__name__)

try:
    from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
    _REQ_TOTAL = Counter("rag_requests_total", "Total RAG query requests")
    _CACHE_HITS = Counter("rag_cache_hits_total", "Cache hits")
    _REDIS_UNAVAIL = Counter("redis_unavailable_total", "Redis connection errors on query path")
    _REQ_LATENCY = Histogram(
        "rag_request_duration_seconds",
        "RAG query request latency",
        buckets=[0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0],
    )
    _METRICS_ENABLED = True
except ImportError:
    _METRICS_ENABLED = False


def _make_cache_key(query: str, top_k: int, source_filter: Optional[str], collection: str) -> str:
    raw = json.dumps(
        {"col": collection, "k": top_k, "q": query, "sf": source_filter},
        sort_keys=True,
    )
    return "rag:" + hashlib.sha256(raw.encode()).hexdigest()


class ServiceUnavailableError(Exception):
    pass


class RagService:
    """Core RAG query logic: Redis cache → embed → Milvus ANN search → filter."""

    def __init__(
        self,
        milvus_searcher,
        redis_client,
        embedder,
        circuit_breaker: CircuitBreaker,
        cfg: Config = None,
        usage_publisher=None,
        metadata_publisher=None,
    ):
        self._milvus = milvus_searcher
        self._redis = redis_client
        self._embedder = embedder
        self._cb = circuit_breaker
        self._cfg = cfg or _default_config
        self._usage_publisher = usage_publisher
        self._metadata_publisher = metadata_publisher

    def query(self, request: QueryRequest, tenant_id: str) -> QueryResponse:
        _start = _time.time()
        if _METRICS_ENABLED:
            _REQ_TOTAL.inc()

        # Collection is always derived from the tenant header, never from request body
        collection = f"{tenant_id}_docs"
        cache_key = _make_cache_key(request.query, request.top_k, request.source_filter, collection)
        request_id = str(uuid.uuid4())

        # ── Redis cache check ────────────────────────────────────────────────
        cache_available = True
        try:
            with _tracer.start_as_current_span("redis.get") as span:
                span.set_attribute("db.system", "redis")
                span.set_attribute("db.operation", "GET")
                span.set_attribute("db.redis.cache_key_prefix", "rag:")
                cached_raw = self._redis.get(cache_key)
                span.set_attribute("redis.cache_hit", cached_raw is not None)
            if cached_raw is not None:
                if _METRICS_ENABLED:
                    _CACHE_HITS.inc()
                    _REQ_LATENCY.observe(_time.time() - _start)
                cached_results = json.loads(cached_raw)
                _elapsed_cache_ms = (_time.time() - _start) * 1000
                if self._usage_publisher is not None:
                    try:
                        self._usage_publisher.publish_rag_query(
                            tenant_id=tenant_id,
                            duration_ms=_elapsed_cache_ms,
                            result_count=len(cached_results),
                            cached=True,
                        )
                    except Exception:
                        pass
                if self._metadata_publisher is not None:
                    try:
                        _chunks = [
                            {"entity_key": r["chunk_id"], "rank": i + 1, "score": r["score"]}
                            for i, r in enumerate(cached_results)
                        ]
                        self._metadata_publisher.publish_rag_query(
                            query_id=request_id,
                            tenant_id=tenant_id,
                            query_text=request.query,
                            top_k=request.top_k,
                            source_filter=request.source_filter,
                            collection=collection,
                            latency_ms=_elapsed_cache_ms,
                            cached=True,
                            retrieved_chunks=_chunks,
                        )
                    except Exception:
                        pass
                return QueryResponse(
                    results=cached_results,
                    cached=True,
                    request_id=request_id,
                )
        except ConnectionError:
            cache_available = False
            if _METRICS_ENABLED:
                _REDIS_UNAVAIL.inc()
            logger.warning("Redis unavailable; proceeding without cache")

        # ── Embed query ──────────────────────────────────────────────────────
        vector = self._embedder.embed(request.query)

        # ── Milvus ANN search via circuit breaker ────────────────────────────
        try:
            hits = self._cb.call(
                self._milvus.search,
                collection=collection,
                vector=vector,
                top_k=request.top_k,
                source_filter=request.source_filter,
            )
        except CircuitBreakerOpen as e:
            raise ServiceUnavailableError("Milvus circuit breaker is open") from e
        except Exception as e:
            raise ServiceUnavailableError(f"Milvus search failed: {e}") from e

        # ── Apply top_k limit and min_score filter ───────────────────────────
        hits = hits[: request.top_k]
        results = [
            QueryResult(
                chunk_id=h.chunk_id,
                text=h.text,
                score=h.score,
                source_type=h.source_type,
                doc_id=h.doc_id,
                metadata=getattr(h, "metadata", {}),
            )
            for h in hits
            if h.score >= request.min_score
        ]

        # ── Store in cache ───────────────────────────────────────────────────
        if cache_available:
            try:
                with _tracer.start_as_current_span("redis.setex") as span:
                    span.set_attribute("db.system", "redis")
                    span.set_attribute("db.operation", "SETEX")
                    span.set_attribute("db.redis.cache_key_prefix", "rag:")
                    span.set_attribute("redis.ttl_seconds", self._cfg.redis_ttl_seconds)
                    self._redis.setex(
                        cache_key,
                        self._cfg.redis_ttl_seconds,
                        json.dumps([r.model_dump() for r in results]),
                    )
            except ConnectionError:
                pass

        elapsed_ms = (_time.time() - _start) * 1000
        if _METRICS_ENABLED:
            _REQ_LATENCY.observe(elapsed_ms / 1000)
        if self._usage_publisher is not None:
            try:
                self._usage_publisher.publish_rag_query(
                    tenant_id=tenant_id,
                    duration_ms=elapsed_ms,
                    result_count=len(results),
                    cached=False,
                )
            except Exception:
                pass
        if self._metadata_publisher is not None:
            try:
                _chunks = [
                    {"entity_key": r.chunk_id, "rank": i + 1, "score": r.score}
                    for i, r in enumerate(results)
                ]
                self._metadata_publisher.publish_rag_query(
                    query_id=request_id,
                    tenant_id=tenant_id,
                    query_text=request.query,
                    top_k=request.top_k,
                    source_filter=request.source_filter,
                    collection=collection,
                    latency_ms=elapsed_ms,
                    cached=False,
                    retrieved_chunks=_chunks,
                )
            except Exception:
                pass
        return QueryResponse(results=results, cached=False, request_id=request_id)


# ─── FastAPI application ──────────────────────────────────────────────────────

app = FastAPI(title="RAG API", version="1.0.0")
_service: Optional[RagService] = None


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    tenant_id = request.headers.get("X-Tenant-ID", "")
    traceparent = request.headers.get("traceparent", "")
    trace_id, span_id = "", ""
    if traceparent:
        parts = traceparent.split("-")
        if len(parts) == 4:
            trace_id, span_id = parts[1], parts[2]
    clear_request_context()
    bind_request_context(tenant_id=tenant_id, trace_id=trace_id, span_id=span_id)
    return await call_next(request)


def get_service() -> RagService:
    svc = getattr(app.state, "service", None) or _service
    if svc is None:
        raise RuntimeError("RagService not initialised")
    return svc


@app.post("/v1/query", response_model=QueryResponse)
async def query_endpoint(
    request: QueryRequest,
    x_tenant_id: str = Header(default="default"),
    service: RagService = Depends(get_service),
):
    try:
        return service.query(request, x_tenant_id)
    except ServiceUnavailableError as exc:
        raise HTTPException(
            status_code=503,
            detail={"error": "MILVUS_UNAVAILABLE", "message": str(exc)},
        ) from exc


@app.get("/v1/health")
async def health_endpoint(service: RagService = Depends(get_service)):
    return {"status": "ok", "milvus_circuit_breaker": service._cb.state}


@app.get("/v1/collections")
async def collections_endpoint(service: RagService = Depends(get_service)):
    return {"collections": []}


@app.get("/metrics")
async def metrics_endpoint():
    from fastapi.responses import Response
    if _METRICS_ENABLED:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
    return Response(content="# metrics disabled\n", media_type="text/plain")
