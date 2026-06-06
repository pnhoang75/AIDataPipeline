# AI Data Pipeline — Error Handling & Retry Strategy

**Applies to:** all four pipeline documents  
**Principle:** fail fast at the edge, retry with backoff in the interior, route unrecoverable failures to DLQs, surface everything in metrics.

---

## 1. Timeout Budget Per Hop

Each synchronous call has a hard deadline. The RAG query path must complete in under 500 ms p99; the budget is allocated as follows.

```
RAG query end-to-end budget: 500 ms

  Client → Kong (JWT verify + quota check):   ~15 ms
  Kong → RAG API (network):                    ~2 ms
  RAG API: Redis cache check:                  ~2 ms  (HIT → return, 19 ms total)
  RAG API: embed query:                       ~80 ms  (CPU BGE-small)
  RAG API: Milvus ANN search:                 ~30 ms
  RAG API: assemble response:                  ~5 ms
                                             ───────
  Total (cache miss, no LLM):               ~134 ms  ← well within 500 ms

  With LLM generation (generate=true):
  + LLM first token latency:                ~800 ms  (streaming mitigates perceived latency)

Ingestion per-document budget (soft, not SLA):
  Connector → Kafka publish:                  ~10 ms
  Document Processor (parse + chunk):        ~500 ms  (large PDF)
  Embedding Worker (32-chunk batch):         ~750 ms  (CPU) / ~75 ms (GPU)
  Milvus insert:                              ~20 ms
```

Timeouts are configured in each service's environment:

```yaml
# pipeline-config ConfigMap
KAFKA_PRODUCE_TIMEOUT_MS: "5000"
MILVUS_CONNECT_TIMEOUT_MS: "3000"
MILVUS_SEARCH_TIMEOUT_MS: "10000"
REDIS_CONNECT_TIMEOUT_MS: "500"
REDIS_OP_TIMEOUT_MS: "200"
EMBEDDING_INFERENCE_TIMEOUT_MS: "30000"
LLM_REQUEST_TIMEOUT_MS: "60000"
QUOTA_GRPC_TIMEOUT_MS: "100"        # fail-open if quota service is slow
```

---

## 2. Retry Policies Per Component

### 2.1 Source Connectors

Connectors use a simple watermark: if a publish fails, the watermark is not advanced, so the same file is retried on the next poll cycle.

| Failure | Behaviour |
|---|---|
| Kafka produce timeout | Retry with exponential backoff (100 ms × 2^n, cap 30 s, max 5 attempts); then skip file and emit `connector_errors_total{reason="kafka_timeout"}` |
| Source unreachable (S3 / NFS) | Back off poll interval (×2 each failure, cap 10× normal interval); alert `ConnectorDown` after 2 min |
| Invalid content-type | Log + skip; update `source_file_status.ingest_status = 'error'`; do not retry automatically |
| Transient auth error (S3 403) | Retry 3× with 5 s delay; then alert |

### 2.2 Document Processor

Consumes from `raw-documents` with consumer group `doc-processor`.

```python
# confluent-kafka consumer config
{
    "enable.auto.commit": False,          # manual commit after successful publish
    "auto.offset.reset": "earliest",
    "max.poll.interval.ms": 300000,       # 5 min — allows large PDF processing
    "session.timeout.ms": 45000,
}
```

| Failure | Behaviour |
|---|---|
| Fetch content fails (S3 get 503) | Retry 3× with exponential backoff (1 s, 4 s, 16 s); then route message to `dlq-raw-documents` |
| Parse error (corrupt PDF) | Route to `dlq-raw-documents`; update `source_file_status = 'error'`; commit offset |
| Chunk publish to Kafka fails | Retry up to 5× (1 s backoff); if all fail, route to `dlq-raw-documents`; do not commit source offset |
| OOM (very large document) | Pod restarted by K8s; message re-delivered (idempotent: `doc_id` is content hash) |

### 2.3 Embedding Worker

Consumes from `document-chunks` with consumer group `embedding-worker`.

| Failure | Behaviour |
|---|---|
| Embedding inference timeout | Retry batch once; if fails again, route individual chunks to `dlq-document-chunks`; continue with rest of batch |
| OpenAI API rate limit (429) | Respect `Retry-After` header; pause batch; retry after delay |
| Milvus insert fails (unavailable) | Retry 5× with backoff (2 s × 2^n, cap 60 s); if Milvus stays down, pause consumer (back-pressure) |
| Milvus insert fails (schema mismatch) | Route chunk to `dlq-document-chunks`; emit alert `EmbeddingSchemaError` |
| Duplicate chunk_id | Milvus upsert (not insert) — idempotent by design |

### 2.4 RAG API

All errors are synchronous (request/response). No retries on behalf of the client — clients must retry themselves.

| Failure | HTTP status | Behaviour |
|---|---|---|
| Redis unavailable | 200 (degraded) | Cache miss; proceed without cache; emit `redis_unavailable_total` |
| Embedding inference timeout | 503 | Return `EMBEDDING_ERROR`; log |
| Milvus search timeout | 503 | Return `MILVUS_UNAVAILABLE`; circuit breaker may open |
| Milvus search returns 0 results | 200 | Return empty `results: []`; this is not an error |
| LLM timeout (`generate=true`) | 504 (stream) or partial 200 | For non-stream: return partial results without answer; for stream: send `[DONE]` event with `error` field |
| Quota Service gRPC timeout | 200 (fail-open) | Log warning; allow request through; emit `quota_check_skipped_total` |

### 2.5 Kong API Gateway

| Failure | Behaviour |
|---|---|
| Keycloak JWKS unreachable | Serve cached public keys (TTL 60 s); after cache expiry, return 401 to all requests until Keycloak recovers |
| Quota Service gRPC unreachable | Fail-open: allow request through; emit Kong plugin error log |
| Upstream (RAG API) returns 503 | Kong does **not** retry (idempotent GET only); return 503 to client |
| Rate limit reached | 429 with `Retry-After` header; does not call upstream |

---

## 3. Dead Letter Queue Processing

### DLQ topic layout

| DLQ topic | Source topic | Retention |
|---|---|---|
| `dlq-raw-documents` | `raw-documents` | 14 days |
| `dlq-document-chunks` | `document-chunks` | 14 days |

### DLQ message envelope

Every DLQ message wraps the original payload with failure context:

```json
{
  "dlq_id": "uuid",
  "original_topic": "raw-documents",
  "original_partition": 2,
  "original_offset": 14592,
  "original_timestamp": 1717401600,
  "failure_reason": "parse_error",
  "failure_detail": "pdfplumber: encrypted PDF — password required",
  "failure_count": 1,
  "failed_at": 1717401700,
  "original_payload": { ... }
}
```

### Replaying DLQs

A `dlq-replayer` CronJob (daily, or triggered manually) processes DLQ messages:

```python
# Replay logic: fix → re-publish → commit DLQ offset
for msg in dlq_consumer.poll():
    envelope = json.loads(msg.value())
    if envelope["failure_reason"] == "kafka_timeout":
        # transient — re-publish to original topic directly
        original_producer.produce(
            topic=envelope["original_topic"],
            value=json.dumps(envelope["original_payload"])
        )
    elif envelope["failure_reason"] == "parse_error":
        # non-recoverable — skip, update file status
        update_file_status(envelope, status="error")
    dlq_consumer.commit()
```

Trigger manual replay from the admin UI:
```
POST /api/admin/dlq/{topic}/replay
  body: { filter: { failure_reason: "kafka_timeout" }, max_messages: 1000 }
```

---

## 4. Circuit Breaker Configuration

Applied at the RAG API and Embedding Worker for calls to Milvus and the embedding inference backend.

```python
# Using 'circuitbreaker' library (pip install circuitbreaker)
from circuitbreaker import circuit

@circuit(
    failure_threshold=5,       # open after 5 consecutive failures
    recovery_timeout=30,       # wait 30 s before half-open probe
    expected_exception=(MilvusException, TimeoutError),
    name="milvus"
)
async def milvus_search(collection, vector, top_k):
    return await milvus_client.search(collection, [vector], top_k=top_k)
```

| Breaker | failure_threshold | recovery_timeout | Metric |
|---|---|---|---|
| Milvus (RAG API) | 5 | 30 s | `circuit_breaker_state{name="milvus"}` |
| Milvus (Embedder) | 3 | 60 s | `circuit_breaker_state{name="milvus_insert"}` |
| Embedding backend | 3 | 120 s | `circuit_breaker_state{name="embedding"}` |
| OpenAI API | 5 | 30 s | `circuit_breaker_state{name="openai"}` |

When a breaker is **open**, the Embedding Worker pauses its Kafka consumer (via `consumer.pause(partitions)`) to create natural back-pressure rather than flooding the DLQ.

---

## 5. Idempotency Guarantees

| Component | Idempotency mechanism |
|---|---|
| Connectors | Watermark not advanced until Kafka ACK. Re-delivery produces duplicate `event_id`; processors deduplicate on `doc_id` (SHA-256 of `content_ref`) |
| Document Processor | `doc_id` is deterministic hash; re-processing same file produces same chunks. Milvus upsert on `chunk_id` prevents duplicates |
| Embedding Worker | Milvus `upsert` (not `insert`) on `chunk_id` — safe to replay |
| RAG API | Stateless; every request is independent |
| Quota Service | `RecordUsage` uses Redis `INCR` — idempotent only if called once per logical event; use a dedup key (`event_id`) in Redis with TTL 24 h |

---

## 6. Error Metrics Reference

Every service exposes these counters via Prometheus:

| Metric | Labels | Description |
|---|---|---|
| `pipeline_errors_total` | `service`, `error_type` | Total errors by service and type |
| `pipeline_retries_total` | `service`, `operation` | Retry attempts |
| `dlq_messages_produced_total` | `topic` | Messages sent to DLQ |
| `dlq_messages_replayed_total` | `topic`, `reason` | Messages replayed from DLQ |
| `circuit_breaker_state` | `name` | 0=closed 1=open 2=half-open |
| `kafka_consumer_lag` | `group`, `topic` | Consumer group lag |
| `quota_check_skipped_total` | `tenant_id` | Requests allowed through on quota service timeout |

---

## 7. Error Response Envelope

All HTTP APIs (RAG API + BFF) return errors in a consistent JSON envelope:

```json
{
  "error": "MILVUS_UNAVAILABLE",
  "message": "Milvus is not reachable — please retry",
  "detail": {
    "circuit_breaker": "open",
    "recovery_in_seconds": 22
  },
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "retry_after": 22
}
```

**Standard error codes:**

| Code | HTTP | Meaning |
|---|---|---|
| `INVALID_REQUEST` | 400 | Schema/validation failure |
| `UNAUTHORIZED` | 401 | Missing or expired JWT |
| `FORBIDDEN` | 403 | Insufficient role or OPA deny |
| `NOT_FOUND` | 404 | Resource does not exist |
| `CONFLICT` | 409 | Duplicate resource or state conflict |
| `QUOTA_EXCEEDED` | 429 | Tenant over quota; check `retry_after` |
| `EMBEDDING_ERROR` | 503 | Embedding backend unavailable |
| `MILVUS_UNAVAILABLE` | 503 | Milvus unreachable or circuit open |
| `LLM_ERROR` | 503 | LLM timeout or API error |
| `DIMENSION_CHANGE_REQUIRES_REINDEX` | 409 | Set `reindex_confirmed: true` to proceed |
| `INTERNAL_ERROR` | 500 | Unexpected server error |

All 500 responses include a `request_id` for log correlation.
