# rag-api

FastAPI service that accepts natural-language queries, embeds them with BAAI/bge-small-en-v1.5,
performs ANN search in Milvus, and returns top-K ranked chunks. Redis caches results for 300 s.
A circuit breaker (failure_threshold=5, recovery_timeout=30 s) guards Milvus calls.

## Relevant design docs
- docs/ai-data-pipeline-design.md §2.6
- docs/ai-data-pipeline-error-handling.md §2.4, §4

## Key endpoints
- `POST /v1/query   { query, top_k, source_filter?, min_score? }`  — returns top-K chunks
- `GET  /v1/health`  — circuit breaker state + liveness
- `GET  /v1/collections` — list available collections

## Query flow
1. Derive `collection = {X-Tenant-ID}_docs` (request body `collection` field is ignored)
2. Redis cache check (TTL 300 s); cache miss → continue
3. Embed query (LocalCPUBackend, lazy-load sentence-transformers)
4. Milvus ANN search via circuit breaker
5. Apply `top_k` truncation then `min_score` filter
6. Store result in Redis; return to client

## Error behaviour
- Redis `ConnectionError` → degrade gracefully (skip cache, still serve Milvus results)
- Milvus unavailable → circuit opens after 5 failures → 503 `MILVUS_UNAVAILABLE`
- Circuit half-open after 30 s → probe request; success → circuit closes

## Key dependencies
- Milvus: `${MILVUS_HOST}:${MILVUS_PORT}`
- Redis: `${REDIS_HOST}:${REDIS_PORT}`
- Circuit breaker: `${CB_FAILURE_THRESHOLD}` / `${CB_RECOVERY_TIMEOUT}`

## How to run tests
```
pytest tests/unit/rag/ -x --tb=short -q
```

## Known constraints
- `collection` field in POST body is intentionally ignored; collection always = `{tenant_id}_docs`
- `RagService` is a plain Python class; unit tests instantiate it directly without TestClient
```
