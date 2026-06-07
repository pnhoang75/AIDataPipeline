import hashlib
import json
import logging
import uuid
from typing import List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException

from circuit_breaker import CircuitBreaker, CircuitBreakerOpen
from config import Config, config as _default_config
from models import QueryRequest, QueryResponse, QueryResult

logger = logging.getLogger(__name__)

try:
    from prometheus_client import Counter
    _REQ_TOTAL = Counter("rag_requests_total", "Total RAG query requests")
    _CACHE_HITS = Counter("rag_cache_hits_total", "Cache hits")
    _REDIS_UNAVAIL = Counter("redis_unavailable_total", "Redis connection errors on query path")
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
    ):
        self._milvus = milvus_searcher
        self._redis = redis_client
        self._embedder = embedder
        self._cb = circuit_breaker
        self._cfg = cfg or _default_config

    def query(self, request: QueryRequest, tenant_id: str) -> QueryResponse:
        if _METRICS_ENABLED:
            _REQ_TOTAL.inc()

        # Collection is always derived from the tenant header, never from request body
        collection = f"{tenant_id}_docs"
        cache_key = _make_cache_key(request.query, request.top_k, request.source_filter, collection)
        request_id = str(uuid.uuid4())

        # ── Redis cache check ────────────────────────────────────────────────
        cache_available = True
        try:
            cached_raw = self._redis.get(cache_key)
            if cached_raw is not None:
                if _METRICS_ENABLED:
                    _CACHE_HITS.inc()
                return QueryResponse(
                    results=json.loads(cached_raw),
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
                self._redis.setex(
                    cache_key,
                    self._cfg.redis_ttl_seconds,
                    json.dumps([r.model_dump() for r in results]),
                )
            except ConnectionError:
                pass

        return QueryResponse(results=results, cached=False, request_id=request_id)


# ─── FastAPI application ──────────────────────────────────────────────────────

app = FastAPI(title="RAG API", version="1.0.0")
_service: Optional[RagService] = None


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
