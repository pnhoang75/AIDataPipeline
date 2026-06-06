# AI Data Pipeline — Test Plan

**Version:** 1.1  
**Date:** 2026-06-05  
**Scope:** Unit · Integration · End-to-End · Performance · Security · Chaos

---

## Claude Pro Testing Strategy

Tests are written and run **within the same session that implements the feature** (see implementation plan sessions). This keeps context small and avoids re-reading large codebases across sessions.

### Rules for test sessions

1. **Write tests in the same session as the implementation.** Each implementation session ends with a passing test suite — do not leave test-writing for a separate session.
2. **Run only the tests for the current service.** Never run the full test suite in a single Claude session — it floods context with output. Use `pytest tests/unit/<service>/` not `pytest tests/`.
3. **Integration tests have their own session.** Each integration test file is written in a dedicated session after both involved services are implemented (e.g., session 1-E: Connector→Kafka→Processor integration tests).
4. **E2E and chaos tests run outside Claude.** These are long-running (2–10 min each) and produce large output. Run them in a terminal via `! pytest tests/e2e/` and only paste the failure summary into Claude if something breaks.
5. **Performance tests never run in Claude.** Run locust from a terminal; share only the summary table (p50/p95/p99/RPS) if you need Claude to help diagnose.
6. **Use `pytest -x --tb=short`** to stop at first failure and keep output compact when running inside a Claude session.

### Test ownership by implementation session

| Implementation session | Tests written in that session |
|---|---|
| 1-B (S3 Connector) | `tests/unit/connectors/test_s3_connector.py` |
| 1-C (NFS Connector) | `tests/unit/connectors/test_nfs_connector.py` |
| 1-D (Doc Processor) | `tests/unit/processor/test_processor.py` |
| 1-E (Connector integration) | `tests/integration/test_connector_kafka.py` |
| 1-F (Embedding Worker) | `tests/unit/embedder/test_embedder.py` |
| 1-G (Embedder integration) | `tests/integration/test_embedder_milvus.py` |
| 1-H (RAG API) | `tests/unit/rag/test_rag_api.py` |
| 2-D (Quota Service) | `tests/unit/quota/test_quota_service.py` |
| 2-E (Quota integration) | `tests/integration/test_quota_service.py` |
| 3-A (BFF scaffold) | `tests/unit/bff/test_auth_middleware.py` |
| 3-B/C (BFF endpoints) | `tests/unit/bff/test_admin_endpoints.py`, `test_user_endpoints.py` |
| 4-A/B (Operator) | `tests/unit/operator/test_dataconnector.py`, `test_tenantworkspace.py` |
| 4-D/E (User sources + fixes) | `tests/unit/bff/test_user_sources.py`, `tests/security/test_ssrf.py`, `tests/security/test_path_traversal.py` |
| 4-G (Self-service E2E) | `tests/e2e/test_self_service_wizard.py` — run in terminal |
| 5-B/C/D (Lineage instrumentation) | `tests/unit/connectors/test_metadata_events.py`, `tests/unit/embedder/test_metadata_events.py` |
| 5-E (Lineage integration) | `tests/integration/test_lineage.py` |
| 6-D (Upgrade E2E) | `tests/e2e/test_coordinated_upgrade.py` — run in terminal |
| 6-F (Chaos) | `tests/chaos/` — run in terminal only |

---

## 1. Testing Strategy

```
Layer 1 — Unit           Fast, isolated, no external deps. Run in Claude session alongside impl.
Layer 2 — Integration    Real Kafka/PostgreSQL/Redis via testcontainers. Dedicated session per pair.
Layer 3 — End-to-End     Full kind cluster. Run in terminal; paste failures into Claude.
Layer 4 — Performance    locust + custom producers. Run in terminal; share summary table only.
Layer 5 — Security       Custom pytest. Run in Claude session alongside impl (compact output).
Layer 6 — Chaos          chaos-mesh / toxiproxy. Run in terminal only.
```

**Tooling:**

| Layer | Tool |
|---|---|
| Unit | `pytest` + `unittest.mock` |
| Integration | `pytest` + `testcontainers-python` (Kafka, Postgres, Redis, MinIO) |
| E2E | `pytest` + `kind` cluster + `kubernetes` Python client |
| Contract | `schemathesis` (OpenAPI fuzz) + `grpc_testing` (proto) |
| Performance | `locust` (HTTP), `kcat` + custom producer (Kafka throughput) |
| Security | `pytest` (custom), `trivy`, `kube-bench`, `kubescape` |
| Chaos | `chaos-mesh` or `toxiproxy` for network faults; `kubectl delete pod` for pod failure |

---

## 2. Unit Tests

### 2.1 Source Connectors

**File:** `tests/unit/connectors/`

| Test | What it verifies |
|---|---|
| `test_s3_connector_watermark_not_advanced_on_publish_failure` | If Kafka produce throws, watermark Redis key is not updated |
| `test_s3_connector_skips_file_on_invalid_content_type` | Files not in `fileTypes` allowlist produce no event and write `error` to `source_file_status` |
| `test_nfs_connector_inotify_emits_event_on_new_file` | Dropping a file into the watched path produces a `RawDocumentEvent` within 1 s |
| `test_connector_event_schema_valid` | Serialised `RawDocumentEvent` passes Avro/JSON schema validation |
| `test_connector_retries_kafka_on_timeout` | Kafka produce mock raises `KafkaTimeoutError`; connector retries up to 5× with backoff |
| `test_connector_skips_after_max_retries` | After 5 failures, connector emits `connector_errors_total{reason="kafka_timeout"}` and does not advance watermark |

### 2.2 Document Processor

**File:** `tests/unit/processor/`

| Test | What it verifies |
|---|---|
| `test_pdf_parser_extracts_text` | `pdfplumber` parser returns non-empty text from a fixture PDF |
| `test_docx_parser_extracts_text` | `python-docx` parser handles fixture DOCX |
| `test_fixed_chunker_produces_correct_sizes` | Chunks are ≤512 tokens; adjacent chunks overlap by exactly 64 tokens (verified with `tiktoken`) |
| `test_fixed_chunker_handles_short_document` | Document shorter than 512 tokens produces exactly 1 chunk |
| `test_chunker_generates_deterministic_chunk_ids` | Same content always produces identical `chunk_id` values |
| `test_parse_error_routes_to_dlq` | Corrupt PDF mock raises `pdfplumber` exception; processor sends to `dlq-raw-documents` and commits offset |
| `test_offset_not_committed_on_chunk_publish_failure` | If `document-chunks` produce fails all retries, source offset is NOT committed (re-delivery guaranteed) |
| `test_chunk_event_schema_valid` | `DocumentChunk` Kafka message passes schema validation |

### 2.3 Embedding Worker

**File:** `tests/unit/embedder/`

| Test | What it verifies |
|---|---|
| `test_local_cpu_backend_returns_correct_dimension` | `LocalCPUBackend.embed_batch(["hello"])` returns `float[384]` |
| `test_batch_accumulates_up_to_32_chunks` | Worker waits for 32 chunks before calling `embed_batch` |
| `test_batch_flushes_after_500ms_timeout` | With only 5 chunks in the buffer, flush fires after 500 ms |
| `test_milvus_upsert_called_with_correct_fields` | `embed_batch` result is passed to `milvus.upsert` with `chunk_id`, `text`, `embedding`, `tenant_id` |
| `test_duplicate_chunk_id_uses_upsert_not_insert` | Replaying the same `chunk_id` calls `upsert`, not `insert` — no duplicate error |
| `test_embedding_timeout_routes_chunk_to_dlq` | `embed_batch` mock raises `TimeoutError`; chunk goes to `dlq-document-chunks` after 1 retry |
| `test_openai_backend_respects_retry_after_header` | OpenAI mock returns 429 with `Retry-After: 2`; worker sleeps ≥2 s before retrying |
| `test_backend_selected_by_env_var` | `EMBEDDING_BACKEND=openai` instantiates `OpenAIBackend`; `local-cpu` instantiates `LocalCPUBackend` |

### 2.4 RAG API

**File:** `tests/unit/rag/`

| Test | What it verifies |
|---|---|
| `test_query_returns_top_k_results` | Milvus mock returns 10 vectors; API returns exactly `top_k=5` |
| `test_cache_hit_skips_milvus` | Redis mock returns a cached result; Milvus mock is never called |
| `test_cache_miss_stores_result` | Redis cache miss → Milvus search → `SET cache_key` with TTL 300 s |
| `test_redis_unavailable_degrades_gracefully` | Redis mock raises `ConnectionError`; request still returns 200 with Milvus results |
| `test_collection_derived_from_tenant_header` | `X-Tenant-ID: acme` → Milvus is searched on collection `acme_docs` |
| `test_collection_not_derived_from_request_body` | Request body contains `collection: evil_tenant_docs`; API ignores it and uses `X-Tenant-ID` |
| `test_milvus_circuit_breaker_opens_after_5_failures` | 5 consecutive Milvus `TimeoutError` → circuit breaker opens; 6th request returns 503 without calling Milvus |
| `test_circuit_breaker_half_open_probe_after_30s` | Circuit open for 30 s → probe request made; on success, circuit closes |
| `test_source_filter_passed_to_milvus_search` | `source_filter: s3` → Milvus search receives scalar filter `source_type == 's3'` |
| `test_min_score_filters_results` | Milvus returns results with scores [0.9, 0.6, 0.3]; `min_score=0.5` returns only the first two |

### 2.5 Quota Service

**File:** `tests/unit/quota/`

| Test | What it verifies |
|---|---|
| `test_check_quota_allows_under_limit` | Redis INCR returns 5; limit is 10; response is `ALLOWED` |
| `test_check_quota_denies_over_limit` | Redis INCR returns 11; limit is 10; DECRBY rollback called; response is `DENIED` |
| `test_check_quota_unlimited_always_allows` | Enterprise tenant has limit=0 (unlimited); response is `UNLIMITED` regardless of usage |
| `test_record_usage_deduplicates_on_event_id` | Same `event_id` submitted twice; Redis dedup key prevents double-counting |
| `test_quota_check_respects_override` | `quota_overrides` row present; effective limit comes from override, not tier default |
| `test_fail_open_on_redis_unavailable` | Redis mock raises `ConnectionError`; gRPC returns `ALLOWED` with `quota_check_skipped_total` counter incremented |

### 2.6 BFF

**File:** `tests/unit/bff/`

| Test | What it verifies |
|---|---|
| `test_source_test_blocks_rfc1918_addresses` | `/api/sources/test` with `endpoint: postgresql://192.168.1.1:5432/db` returns 400 `SSRF_BLOCKED` |
| `test_source_test_blocks_loopback` | `endpoint: postgresql://127.0.0.1:5432/db` returns 400 |
| `test_source_test_blocks_internal_k8s_service` | `endpoint: postgresql://quota-db.infrastructure.svc:5432/db` returns 400 |
| `test_source_test_allows_public_ip` | `endpoint: postgresql://203.0.113.5:5432/db` passes the SSRF check (actual connection mocked) |
| `test_nfs_browse_rejects_path_traversal` | `path=../../etc/passwd` returns 400 with `PATH_TRAVERSAL_BLOCKED` |
| `test_nfs_browse_rejects_path_outside_prefix` | `allowed_path_prefix=/exports/acme`; `path=/exports/other` returns 400 |
| `test_nfs_browse_allows_valid_subpath` | `path=/exports/acme/reports` with correct prefix passes |
| `test_upload_skips_connector_quota_check` | `source_type=upload` → `CheckQuota(CONNECTOR_COUNT)` mock is never called |
| `test_non_upload_calls_connector_quota_check` | `source_type=s3` → `CheckQuota(CONNECTOR_COUNT)` is called |
| `test_user_cannot_delete_other_users_connector` | Alice's JWT submits `DELETE /sources/conn-123` (owned by Bob); returns 403 |
| `test_tenant_scoping_on_workspace_list` | `X-Tenant-ID: acme` → only `acme` workspaces returned; `corp` workspaces excluded |

### 2.7 Pipeline Operator

**File:** `tests/unit/operator/`

| Test | What it verifies |
|---|---|
| `test_dataconnector_creates_deployment_without_poll_interval` | No `pollInterval` in spec → `apply_deployment` called, `apply_cronjob` not called |
| `test_dataconnector_creates_cronjob_with_poll_interval` | `pollInterval: 5m` in spec → `apply_cronjob` called |
| `test_embeddingconfig_blocks_dimension_change_without_flag` | `old_dim=384`, `new_dim=1024`, `reindexConfirmed=false` → `TemporaryError` raised, status set to `BlockedDimensionChange` |
| `test_embeddingconfig_allows_dimension_change_with_flag` | Same but `reindexConfirmed=true` → `patch_configmap` and `rollout_restart` called |
| `test_tenantworkspace_creates_upload_watcher_cronjob` | TenantWorkspace reconcile creates `upload-watcher-{tenant_id}` CronJob |
| `test_tenantworkspace_delete_removes_upload_watcher` | TenantWorkspace delete triggers `delete_cronjob(upload-watcher-{tenant_id})` |
| `test_connector_role_lists_exact_secret_names` | Operator creates per-connector `Role` with `resourceNames: ["connector-acme-s3-creds"]` — not a wildcard |

---

## 3. Integration Tests

All integration tests spin up real dependencies via `testcontainers-python`.

**File:** `tests/integration/`

### 3.1 Connector → Kafka

```python
# Fixture: real Kafka (testcontainers), real MinIO (testcontainers), real Redis (testcontainers)
def test_s3_connector_publishes_event_for_new_object():
    # Upload a PDF to MinIO
    # Start S3Connector pointing at the bucket
    # Assert: within 5 s, raw-documents topic has 1 message
    # Assert: message deserialises to RawDocumentEvent with correct source_type, content_ref, tenant_id
    # Assert: Redis watermark key is updated

def test_s3_connector_watermark_prevents_duplicate_events():
    # Upload PDF, run connector (watermark advances)
    # Run connector again without uploading anything
    # Assert: only 1 message total in raw-documents

def test_connector_writes_pending_status_to_postgres():
    # Real PostgreSQL (testcontainers)
    # Upload PDF, run connector
    # Assert: source_file_status row with status='pending' exists
```

### 3.2 Document Processor → Kafka

```python
def test_processor_parses_pdf_and_produces_chunks():
    # Produce RawDocumentEvent to raw-documents with content_ref pointing to MinIO PDF
    # Start DocumentProcessor consumer
    # Assert: document-chunks topic has ≥1 message within 10 s
    # Assert: each ChunkEvent has doc_id, chunk_id, chunk_index, text (non-empty), tenant_id

def test_processor_routes_corrupt_pdf_to_dlq():
    # Produce event with content_ref pointing to a corrupt PDF
    # Assert: dlq-raw-documents has 1 message; source_file_status shows status='error'

def test_processor_does_not_commit_offset_on_chunk_publish_failure():
    # Mock document-chunks producer to always fail
    # Assert: after retries, source message offset is NOT committed (re-deliverable)
```

### 3.3 Embedding Worker → Milvus

```python
def test_embedder_writes_vector_to_milvus():
    # Produce ChunkEvent to document-chunks
    # Start EmbeddingWorker with LocalCPUBackend
    # Assert: Milvus (real, via testcontainers) has 1 entity with correct chunk_id and 384-dim vector

def test_embedder_upsert_is_idempotent():
    # Produce the same ChunkEvent twice
    # Assert: Milvus entity count is 1 (upsert, not insert)

def test_embedder_updates_postgres_status_to_indexed():
    # After embedder processes a chunk
    # Assert: source_file_status.ingest_status = 'indexed', chunk_count set
```

### 3.4 RAG API

```python
def test_rag_query_returns_relevant_chunk():
    # Pre-populate Milvus with known embeddings for tenant 'test'
    # POST /v1/query with X-Tenant-ID: test, query matching a seeded document
    # Assert: response has ≥1 result with score > 0.5, text contains expected content

def test_rag_query_respects_tenant_isolation():
    # Seed Milvus with 'tenant_a' and 'tenant_b' collections
    # Query with X-Tenant-ID: tenant_a
    # Assert: results only contain chunks with source from tenant_a
    # Assert: no tenant_b chunks appear

def test_rag_query_cache_stores_and_retrieves():
    # First query: cache miss → Milvus queried; result stored in Redis
    # Second identical query: cache hit → Milvus NOT called again (verify via spy)
    # Assert: X-Cache: HIT header on second response
```

### 3.5 Quota Service

```python
def test_quota_check_increments_redis_counter():
    # RegisterTenant(tenant_id='acme', tier=FREE)
    # CheckQuota(tenant_id='acme', metric=API_CALLS_PER_DAY, amount=1, increment_on_allow=True)
    # Assert: Redis key quota:acme:api_calls:{today} = 1

def test_quota_check_denies_at_limit():
    # Set Redis key to 100 (Free limit)
    # CheckQuota → assert status=DENIED, rollback DECRBY called (key stays at 100)

def test_quota_check_enterprise_never_denied():
    # RegisterTenant(tier=ENTERPRISE)
    # Set Redis key to 99999
    # CheckQuota → assert status=UNLIMITED
```

### 3.6 BFF API

```python
def test_bff_create_connector_applies_dataconnector_cr(k8s_client):
    # POST /api/admin/connectors with valid ConnectorCreate body + admin JWT
    # Assert: DataConnector CR exists in k8s with correct spec

def test_bff_quota_endpoint_calls_quota_service():
    # GET /api/admin/quota with admin JWT
    # Assert: gRPC ListUsage was called; response contains tenant usage rows

def test_bff_user_workspace_scoped_to_tenant():
    # Create workspaces for tenant_a and tenant_b in DB
    # GET /api/workspaces with X-Tenant-ID: tenant_a
    # Assert: only tenant_a's workspaces returned
```

---

## 4. End-to-End Tests

Run against a real kind cluster with all components deployed. Tests are slow (2–10 min each).

**File:** `tests/e2e/`

### 4.1 Full Ingestion Pipeline

```
test_e2e_pdf_ingested_end_to_end:
  1. Upload a 5-page PDF to MinIO (tenant=acme)
  2. Wait up to 60 s for source_file_status.ingest_status = 'indexed'
  3. POST /v1/query with query matching known content in the PDF
  4. Assert: response has ≥1 result with score > 0.5 and page_number set
  5. Assert: raw-documents and document-chunks consumer lags = 0
```

```
test_e2e_dlq_replay:
  1. Upload an encrypted (password-protected) PDF to MinIO
  2. Wait for source_file_status.ingest_status = 'error'
  3. Assert: dlq-raw-documents has 1 message with failure_reason='parse_error'
  4. POST /api/admin/dlq/dlq-raw-documents/replay with filter={failure_reason: 'parse_error'}
  5. Assert: DLQ message consumed; file status remains 'error' (non-recoverable)
```

```
test_e2e_nfs_connector_ingests_directory:
  1. Write 3 text files to the NFS extra-mount
  2. Wait for all 3 files to reach ingest_status='indexed'
  3. Query for content from one of the files
  4. Assert: chunk text contains expected text, source_type=nfs
```

```
test_e2e_kafka_stream_connector:
  1. Produce 5 messages to the upstream Kafka topic the stream connector bridges
  2. Wait for 5 RawDocumentEvents to appear in raw-documents
  3. Wait for source_file_status = 'indexed' for all 5
```

### 4.2 Multi-Tenancy Isolation

```
test_e2e_tenant_isolation:
  1. Ingest documents for tenant_a and tenant_b
  2. Query as tenant_a → assert only tenant_a results
  3. Query as tenant_b → assert only tenant_b results
  4. Attempt cross-tenant collection override in request body → assert ignored
```

```
test_e2e_free_tier_quota_enforced:
  1. Send 100 RAG queries as a Free-tier tenant (limit = 100/day)
  2. 101st query → assert 429 with QUOTA_EXCEEDED
  3. Assert Retry-After header present
```

```
test_e2e_connector_quota_enforced:
  1. As Free-tier user, create 2 connectors via self-service wizard API
  2. 3rd POST /api/sources/create → assert 429 QUOTA_EXCEEDED
  3. As Pro-tier user, create 5+ connectors → all succeed (unlimited)
```

### 4.3 Self-Service Wizard

```
test_e2e_user_creates_s3_connector_and_ingests:
  1. POST /api/sources/test with valid MinIO creds → assert success=true, preview has files
  2. POST /api/sources/create with source_type=s3 → assert 201, status=provisioning
  3. Wait for DataConnector CR to reach status.state=Running
  4. Wait for source_file_status = 'indexed' for at least 1 file
```

```
test_e2e_file_upload_ingested:
  1. POST /api/sources/upload with a PDF file → assert 202, upload_session_id returned
  2. Wait up to 60 s for upload-watcher CronJob to fire
  3. Wait for source_file_status = 'indexed'
  4. Query for content from the uploaded PDF → assert ≥1 result
```

```
test_e2e_connector_deletion_removes_cr:
  1. Create a connector via POST /api/sources/create
  2. DELETE /api/sources/{id}
  3. Assert: DataConnector CR no longer exists in K8s
  4. Assert: K8s Secret connector-{slug}-creds no longer exists (cleanup verified)
```

### 4.4 Tenant Provisioning

```
test_e2e_new_tenant_provisioning:
  1. POST /api/admin/tenants with {name, slug, license_type: pro, admin_email}
  2. Assert: Keycloak Organization created
  3. Assert: Milvus collection {slug}_docs created
  4. Assert: TenantWorkspace CR reaches status.state=Provisioned
  5. Assert: Quota Service responds to GetUsage for new tenant_id
  6. Assert: upload-watcher CronJob upload-watcher-{slug} exists in ai-pipeline namespace
```

### 4.5 Pipeline Upgrade

```
test_e2e_coordinated_upgrade:
  1. Ingest documents with pipeline version 1.0.0
  2. Bump PipelineCluster.spec.version to 1.1.0 (mock image tag exists)
  3. Assert: UpgradeInProgress condition = True
  4. Assert: connector CronJobs suspended during upgrade
  5. Assert: consumer lag drains to 0 before doc-processor is rolled
  6. Assert: all components reach new version; UpgradeInProgress cleared
  7. Assert: RAG query still works post-upgrade (no data loss)
```

---

## 5. Contract Tests

### 5.1 BFF OpenAPI Contract (schemathesis)

```bash
# Run schemathesis against the running BFF
schemathesis run docs/api/bff-api.openapi.yaml \
  --url https://pipeline.local/api \
  --auth "Bearer <test-jwt>" \
  --checks all \
  --hypothesis-max-examples 100
```

**Asserts:**
- No endpoint returns a response body that violates its OpenAPI schema
- No unhandled 500 errors
- Required fields in responses are always present
- Error envelopes always include `request_id`

### 5.2 RAG API OpenAPI Contract

```bash
schemathesis run docs/api/rag-api.openapi.yaml \
  --url https://api.pipeline.local/v1 \
  --auth "Bearer <test-jwt>" \
  --checks all
```

### 5.3 Quota Service gRPC Contract

```python
# tests/contract/test_quota_grpc.py
def test_check_quota_response_shape():
    response = stub.CheckQuota(CheckQuotaRequest(tenant_id='acme', metric=API_CALLS_PER_DAY, amount=1))
    assert response.HasField('status')
    assert response.current_usage >= 0
    assert response.limit >= 0

def test_record_usage_dedup_field_present():
    response = stub.RecordUsage(RecordUsageRequest(tenant_id='acme', metric=API_CALLS_PER_DAY, amount=1, event_id='uuid-1'))
    assert hasattr(response, 'deduped')
```

---

## 6. Performance Tests

**File:** `tests/performance/`  
**Tool:** `locust` for HTTP; custom Kafka producer for throughput.

### 6.1 RAG API Latency (Target: p99 < 500 ms at 100 QPS)

```python
# locustfile.py
class RAGUser(HttpUser):
    wait_time = between(0.5, 1.5)

    @task
    def query(self):
        self.client.post("/v1/query",
            json={"query": "test query", "top_k": 5},
            headers={"Authorization": f"Bearer {TOKEN}", "X-Tenant-ID": "perf-tenant"})
```

**Ramp:** 0 → 100 users over 2 min, sustain 5 min.

**Pass criteria:**
- p50 < 150 ms, p95 < 300 ms, p99 < 500 ms
- Error rate < 0.1%
- Redis cache hit rate > 30% (same queries repeated)

### 6.2 Ingestion Throughput (Target: 100 docs/min on CPU)

```python
# tests/performance/test_ingestion_throughput.py
def test_ingestion_rate_100_docs_per_minute():
    # Upload 100 PDFs (avg 1 MB each) to MinIO
    # Start timer
    # Poll source_file_status until all 100 reach status='indexed'
    # Assert: total wall-clock time < 60 s
    # Assert: Kafka consumer lag stays < 500 throughout
```

### 6.3 Embedding Batch Throughput

```python
def test_embedding_worker_processes_32_chunk_batch_under_750ms():
    # Produce 32 ChunkEvents to document-chunks simultaneously
    # Measure time from first produce to Milvus insert ACK for all 32
    # Assert: wall-clock time < 750 ms (CPU BGE-small target per design doc)
```

### 6.4 Quota Service Latency (Target: p99 < 5 ms)

```python
def test_quota_check_p99_latency():
    # Fire 1000 CheckQuota gRPC calls serially
    # Assert: p99 latency < 5 ms (Redis INCR fast path)
```

### 6.5 Milvus Search Scaling

```python
def test_milvus_search_10k_vectors():
    # Seed 10,000 vectors (IVF_FLAT index)
    # Run 100 concurrent ANN searches, top_k=10
    # Assert: p99 < 30 ms

def test_milvus_search_1m_vectors():
    # Seed 1,000,000 vectors (switch to HNSW index)
    # Same concurrent search test
    # Assert: p99 < 50 ms; recall@10 > 0.95
```

---

## 7. Security Tests

**File:** `tests/security/`

### 7.1 Authentication & Authorization

| Test | Expected result |
|---|---|
| Request to `POST /v1/query` with no JWT | 401 from Kong |
| Request with expired JWT | 401 |
| Request with valid JWT but wrong `pipeline-admin` role on admin endpoint | 403 from BFF |
| Request with forged `X-Tenant-ID` header (added before Kong) | Ignored; Kong overwrites with value from JWT `org_id` |
| `pipeline-user` JWT accessing `GET /api/admin/tenants` | 403 |
| Admin JWT from tenant A accessing tenant B's workspaces | 403 |

### 7.2 SSRF (POST /api/sources/test)

| Input | Expected result |
|---|---|
| `endpoint: postgresql://10.0.0.1:5432/db` | 400 `SSRF_BLOCKED` |
| `endpoint: postgresql://172.16.0.1:5432/db` | 400 `SSRF_BLOCKED` |
| `endpoint: postgresql://192.168.1.1:5432/db` | 400 `SSRF_BLOCKED` |
| `endpoint: postgresql://127.0.0.1:5432/db` | 400 `SSRF_BLOCKED` |
| `endpoint: postgresql://[::1]:5432/db` | 400 `SSRF_BLOCKED` |
| `endpoint: postgresql://quota-db.infrastructure.svc:5432/db` | 400 `SSRF_BLOCKED` |
| `endpoint: file:///etc/passwd` | 400 `SSRF_BLOCKED` |
| `endpoint: postgresql://203.0.113.5:5432/db` | Pass SSRF check (connection attempt mocked) |

### 7.3 NFS Path Traversal (GET /api/sources/{id}/browse/{path})

| Input path | allowed_path_prefix | Expected result |
|---|---|---|
| `../../etc/passwd` | `/exports/acme` | 400 `PATH_TRAVERSAL_BLOCKED` |
| `/exports/other-tenant/secret` | `/exports/acme` | 400 `PATH_TRAVERSAL_BLOCKED` |
| `/exports/acme/../other-tenant` | `/exports/acme` | 400 `PATH_TRAVERSAL_BLOCKED` |
| `/exports/acme/reports` | `/exports/acme` | 200 OK |
| `/exports/acme` | `/exports/acme` | 200 OK |

### 7.4 Tenant Isolation

```python
def test_milvus_collection_not_overridable_from_request():
    # User JWT for tenant 'acme' (org_id=acme)
    # POST /v1/query body includes collection='corp_docs'
    # Assert: Milvus was called with collection='acme_docs', not 'corp_docs'

def test_cross_tenant_connector_deletion_blocked():
    # Connector conn-123 was created by user in tenant 'acme'
    # JWT for tenant 'corp' calls DELETE /api/sources/conn-123
    # Assert: 404 (connector not visible to other tenant) or 403

def test_workspace_scoping_prevents_cross_tenant_file_access():
    # Workspace ws-acme belongs to tenant 'acme'
    # User from tenant 'corp' calls GET /api/workspaces/ws-acme/files
    # Assert: 404 (workspace not found for this tenant)
```

### 7.5 Connector Ownership

```python
def test_user_cannot_delete_other_users_connector():
    # Alice (pipeline-user) creates connector conn-alice
    # Bob (pipeline-user, same tenant) calls DELETE /api/sources/conn-alice
    # Assert: 403 Forbidden

def test_tenant_admin_can_delete_any_connector_in_tenant():
    # Alice creates conn-alice
    # Tenant admin (pipeline-admin role) calls DELETE /api/sources/conn-alice
    # Assert: 204 No Content
```

### 7.6 Static Security Scanning

```bash
# Image vulnerability scanning (run in CI on every push)
trivy image pipeline-rag-api:latest --exit-code 1 --severity CRITICAL

# Kubernetes manifest linting
kubescape scan framework nsa k8s/

# CIS Kubernetes benchmark
kube-bench run --targets master,node
```

**Pass criteria:**
- Zero CRITICAL CVEs in all custom images
- `kubescape` score ≥ 80%
- `kube-bench` Level 1 remediation rate ≥ 95%

---

## 8. Chaos Tests

Run weekly against a staging kind cluster. Each test verifies the system degrades gracefully and recovers automatically.

**File:** `tests/chaos/`

### 8.1 Kafka Broker Failure

```
Precondition:  Active ingestion running (50 docs/min)
Action:        kubectl delete pod ai-pipeline-kafka-kafka-0 -n infrastructure
Expected:      Strimzi restarts broker; connectors retry with backoff (ConnectorDown alert fires after 2 min)
               After broker recovers: consumer lag drains; no messages lost (verify via offset comparison)
Recovery SLO:  < 60 s to resume ingestion; DLQ empty
```

### 8.2 PostgreSQL Primary Failure

```
Precondition:  Active ingestion + RAG queries
Action:        kubectl delete pod quota-db-1 -n infrastructure (primary pod)
Expected:      CloudNativePG promotes replica to primary within 30 s
               BFF retries DB connection; RAG API continues (no DB dependency on query path)
               source_file_status writes resume after failover
Recovery SLO:  < 30 s CNPG failover; < 5 s BFF reconnect
```

### 8.3 Milvus Unavailability

```
Action:        kubectl scale deployment milvus -n infrastructure --replicas=0
Expected:      RAG API circuit breaker opens after 5 failures (failure_threshold=5)
               Subsequent queries return 503 with circuit_breaker:open detail
               Embedding Worker pauses Kafka consumer (back-pressure via consumer.pause())
               Alert MilvusUnreachable fires
After restore: kubectl scale deployment milvus --replicas=1
               Circuit breaker half-opens after 30 s; probe succeeds; circuit closes
               Embedding Worker resumes consumer; lag drains
```

### 8.4 Quota Service Unavailability

```
Action:        kubectl scale deployment quota-service --replicas=0
Expected:      Kong quota-check plugin times out (100 ms); fails open
               RAG queries are allowed through; quota_check_skipped_total counter increments
               Alert fires on sustained quota_check_skipped_total spike
               BFF connector creation proceeds (fail-open behavior per design)
Recovery SLO:  Zero user-facing errors during quota service outage (fail-open)
```

### 8.5 Redis Unavailability

```
Action:        kubectl scale deployment redis --replicas=0
Expected:      RAG API cache misses on all queries; returns 200 (degraded mode)
               redis_unavailable_total counter increments
               Quota Service falls back to PostgreSQL for limit reads
               Query latency increases (no cache) but stays < 500 ms p99
```

### 8.6 Network Partition (toxiproxy)

```
Action:        Add 200ms latency + 10% packet loss between ai-pipeline and infrastructure namespaces
Expected:      Kafka producers retry with backoff; consumer lag grows temporarily
               RAG API latency increases; p99 may breach 500ms — alert fires
               After toxiproxy rule removed: system self-heals within 30 s
```

### 8.7 Embedding Worker OOM

```
Action:        Submit a 50 MB single-document PDF (triggers OOM in doc processor)
Expected:      K8s OOMKills the pod; pod is restarted by Deployment controller
               Message is re-delivered (offset not committed pre-crash)
               Idempotency: same doc_id produces same chunk_ids; Milvus upsert handles re-delivery
               No duplicate vectors in Milvus
```

---

## 9. Metadata & Lineage Tests

**File:** `tests/integration/test_lineage.py`

```python
def test_lineage_upstream_traces_to_datasource():
    # Ingest a PDF from S3
    # Wait for Embedding entity to appear in metadata.entities
    # Call GET /api/lineage/upstream/{chunk_id}
    # Assert: chain contains DocumentChunk → RawDocument → DataSource
    # Assert: DataSource.attributes.source_path starts with 's3://'

def test_lineage_downstream_returns_all_derived_entities():
    # Given a known RawDocument source_path
    # Call GET /api/lineage/downstream/{source_path}
    # Assert: response contains counts for DocumentChunk and Embedding entity types

def test_stale_embeddings_detected_after_model_change():
    # Ingest documents with schema_version V1 (bge-small, dim=384)
    # Update EmbeddingConfig → operator creates schema_version V2 (bge-large, dim=1024)
    # Call GET /api/lineage/stale/{tenant_id}
    # Assert: all embeddings from V1 appear in the stale list

def test_upload_path_publishes_datasource_metadata_event():
    # POST /api/sources/upload with a PDF
    # Assert: metadata-events Kafka topic has a message with entity_type='DataSource'
    #         and source_type='upload' within 10 s

def test_rag_query_provenance_links_back_to_source():
    # POST /v1/query; record query_id from response
    # Call GET /api/lineage/provenance/{query_id}
    # Assert: each result row has source_file, embedding_model, chunk_size, indexed_at populated
```

---

## 10. CI/CD Test Matrix

| Trigger | Layers Run | Estimated Duration |
|---|---|---|
| Push to feature branch | Unit | 3–5 min |
| PR opened / updated | Unit + Integration + Security + Contract | 15–20 min |
| Merge to main | Unit + Integration + E2E + Security + Contract | 45–60 min |
| Nightly (main) | All layers including Performance | 90–120 min |
| Weekly | All layers including Chaos | 3–4 hours |

**Required to merge:**
- Unit: 100% pass
- Integration: 100% pass
- Security (SSRF, path traversal, auth): 100% pass
- Contract (schemathesis): zero schema violations, zero 500s
- `trivy`: no CRITICAL CVEs
- Coverage gate: ≥80% line coverage on all Python services

---

## 11. Claude Pro Session Commands

Copy-paste these commands to run tests efficiently inside a Claude Code session without flooding context.

**Unit tests for one service (fast, use inside Claude session):**
```bash
# Run with short traceback; stop at first failure
pytest tests/unit/connectors/ -x --tb=short -q
pytest tests/unit/rag/ -x --tb=short -q
pytest tests/unit/bff/ -x --tb=short -q
```

**Security tests (compact output, safe to run in Claude session):**
```bash
pytest tests/security/ -x --tb=short -q
```

**Integration tests for one pair (use testcontainers; run in Claude session with /compact ready):**
```bash
pytest tests/integration/test_connector_kafka.py -x --tb=short -q
pytest tests/integration/test_lineage.py -x --tb=short -q
```

**E2E and performance — run in terminal with `!`, paste only the summary:**
```bash
# In Claude Code: type ! to run in terminal without loading output into context
! pytest tests/e2e/ -x --tb=short -q 2>&1 | tail -20
! locust -f tests/performance/locustfile.py --headless -u 100 -r 10 -t 5m --only-summary
```

**Chaos tests — always terminal only:**
```bash
! pytest tests/chaos/ -x --tb=short -q 2>&1 | tail -30
```

**Coverage report (run at end of a service session, not mid-session):**
```bash
! pytest tests/unit/<service>/ --cov=services/<service> --cov-report=term-missing -q 2>&1 | tail -15
```

---

## 11. Test Data & Fixtures

| Fixture | Contents | Used By |
|---|---|---|
| `fixtures/sample.pdf` | 5-page PDF with known text ("revenue grew 24% YoY") | Unit + Integration + E2E |
| `fixtures/corrupt.pdf` | Invalid PDF binary | DLQ + error-handling tests |
| `fixtures/encrypted.pdf` | Password-protected PDF | DLQ replay tests |
| `fixtures/large.pdf` | 50 MB single-page scan | OOM + throughput tests |
| `fixtures/sample.docx` | 3-page DOCX | Parser unit tests |
| `fixtures/sample.csv` | 100-row CSV with text column | Parser unit tests |
| `fixtures/tenant_a_seed_vectors.json` | Pre-computed 384-dim vectors for RAG isolation tests | Milvus seed fixtures |
| `fixtures/jwts/admin.jwt` | Valid `pipeline-admin` JWT (signed with test key) | Auth tests |
| `fixtures/jwts/user_acme.jwt` | Valid `pipeline-user` JWT, tenant=acme | Tenant isolation tests |
| `fixtures/jwts/user_corp.jwt` | Valid `pipeline-user` JWT, tenant=corp | Cross-tenant tests |
| `fixtures/jwts/expired.jwt` | Expired JWT | Auth rejection tests |
