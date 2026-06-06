# AI Data Pipeline — Metadata & Data Lineage

**Extends:** `ai-data-pipeline-design.md`  
**New components:** Metadata Service · lineage schema (PostgreSQL) · OpenMetadata (optional catalog overlay)

---

## 1. Goals

| Goal | Description |
|---|---|
| **Provenance** | Given any vector returned by RAG, trace it back to the source file, page, and paragraph |
| **Impact analysis** | Given a changed or deleted source file, identify all downstream chunks and vectors affected |
| **Staleness detection** | Find embeddings generated with an older model or chunking config that need re-indexing |
| **Audit trail** | Record who ingested what, when, and with which configuration |
| **Quality tracking** | Surface data quality issues (empty chunks, duplicates, low embedding norm) per entity |
| **Run history** | Replay or compare pipeline runs by their configuration snapshot |

---

## 2. Entity Model

The lineage graph has six first-class entity types connected by directed edges.

```
DataSource ──discovered_in──► RawDocument ──chunked_into──► DocumentChunk
                                                                    │
                                                              embedded_by
                                                                    │
                                                                    ▼
VectorCollection ◄──stored_in── Embedding ◄──────────────── (Embedding)
       │
  retrieved_by
       │
       ▼
  RAGQuery
```

### Entity types

| Type | Natural key | Represents |
|---|---|---|
| `DataSource` | `tenant_id + source_type + endpoint` | An S3 bucket, NFS path, DB table, or Kafka topic |
| `RawDocument` | `doc_id` (SHA-256 of content) | A single file or database row, content-addressed |
| `DocumentChunk` | `chunk_id` (`doc_id:index`) | One 512-token text window |
| `Embedding` | `embedding_id` | A float vector stored in Milvus, with model provenance |
| `VectorCollection` | `collection_name` | A Milvus collection with its current schema version |
| `RAGQuery` | `query_id` | A search query + the chunks it retrieved |

### Cross-cutting entities

| Type | Purpose |
|---|---|
| `PipelineRun` | Groups all entities produced in one batch; carries config snapshot |
| `SchemaVersion` | Records chunking + embedding config at a point in time; referenced by chunks and embeddings |
| `DataQualityCheck` | Quality observation attached to any entity |

---

## 3. PostgreSQL Lineage Schema

All lineage tables live in the `metadata` schema within the existing CloudNativePG cluster. This avoids a new database service while keeping lineage concerns logically separated.

```sql
-- ── Schema setup ────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS metadata;

-- ── Schema / config versions ─────────────────────────────────────────────────
-- Each change to chunking params or embedding model creates a new version.
-- All downstream entities reference it; staleness is detected by comparing
-- the entity's schema_version_id to the current active version.
CREATE TABLE metadata.schema_versions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL,
    version_number  INTEGER NOT NULL,
    chunk_size      INTEGER NOT NULL DEFAULT 512,
    chunk_overlap   INTEGER NOT NULL DEFAULT 64,
    chunking_strategy TEXT NOT NULL DEFAULT 'fixed',
    embedding_model TEXT NOT NULL,
    embedding_model_version TEXT,
    embedding_dimension INTEGER NOT NULL,
    embedding_backend TEXT NOT NULL,   -- local-cpu | local-gpu | openai
    index_type      TEXT NOT NULL DEFAULT 'IVF_FLAT',
    is_current      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT now(),
    created_by      TEXT,              -- user_id or "pipeline-operator"
    UNIQUE (tenant_id, version_number)
);

CREATE INDEX ON metadata.schema_versions (tenant_id, is_current);

-- ── Pipeline runs ─────────────────────────────────────────────────────────────
-- Every ingestion/processing/embedding batch is a run. Entities record their run.
CREATE TABLE metadata.pipeline_runs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           UUID NOT NULL,
    pipeline_type       TEXT NOT NULL,  -- ingestion | processing | embedding | reindex
    connector_id        TEXT,
    schema_version_id   UUID REFERENCES metadata.schema_versions,
    config_snapshot     JSONB NOT NULL DEFAULT '{}',  -- exact env vars at run start
    status              TEXT NOT NULL DEFAULT 'running',
    started_at          TIMESTAMPTZ DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    duration_ms         INTEGER GENERATED ALWAYS AS (
                            EXTRACT(EPOCH FROM (finished_at - started_at)) * 1000
                        ) STORED,
    entities_processed  INTEGER NOT NULL DEFAULT 0,
    entities_failed     INTEGER NOT NULL DEFAULT 0,
    bytes_processed     BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX ON metadata.pipeline_runs (tenant_id, pipeline_type, started_at DESC);
CREATE INDEX ON metadata.pipeline_runs (status) WHERE status = 'running';

-- ── Entities registry ─────────────────────────────────────────────────────────
-- Single table for all entity types; attributes stored as JSONB.
CREATE TABLE metadata.entities (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type     TEXT NOT NULL,
    entity_key      TEXT NOT NULL,     -- natural key (content hash, chunk_id, etc.)
    tenant_id       UUID NOT NULL,
    pipeline_run_id UUID REFERENCES metadata.pipeline_runs,
    schema_version_id UUID REFERENCES metadata.schema_versions,
    attributes      JSONB NOT NULL DEFAULT '{}',
    is_current      BOOLEAN NOT NULL DEFAULT TRUE,  -- false = superseded by newer version
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (tenant_id, entity_type, entity_key)
);

CREATE INDEX ON metadata.entities (tenant_id, entity_type);
CREATE INDEX ON metadata.entities (entity_key);
CREATE INDEX ON metadata.entities (pipeline_run_id);
CREATE INDEX ON metadata.entities USING GIN (attributes);  -- JSON attribute search

-- ── Lineage edges ─────────────────────────────────────────────────────────────
-- Directed edges: upstream produced / contains / influences downstream.
CREATE TABLE metadata.lineage (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    upstream_id     UUID NOT NULL REFERENCES metadata.entities,
    downstream_id   UUID NOT NULL REFERENCES metadata.entities,
    relationship    TEXT NOT NULL,
    -- discovered_in | chunked_into | embedded_by | stored_in | retrieved_by
    pipeline_run_id UUID REFERENCES metadata.pipeline_runs,
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (upstream_id, downstream_id, relationship)
);

CREATE INDEX ON metadata.lineage (upstream_id);
CREATE INDEX ON metadata.lineage (downstream_id);
CREATE INDEX ON metadata.lineage (relationship);

-- ── Processing steps ──────────────────────────────────────────────────────────
-- Fine-grained log of each step applied to an entity within a run.
CREATE TABLE metadata.processing_steps (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id       UUID NOT NULL REFERENCES metadata.entities,
    pipeline_run_id UUID NOT NULL REFERENCES metadata.pipeline_runs,
    step_type       TEXT NOT NULL,  -- fetch | parse | chunk | embed | index | reindex
    status          TEXT NOT NULL DEFAULT 'pending',
    started_at      TIMESTAMPTZ DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    duration_ms     INTEGER GENERATED ALWAYS AS (
                        EXTRACT(EPOCH FROM (finished_at - started_at)) * 1000
                    ) STORED,
    input_bytes     BIGINT,
    output_count    INTEGER,        -- chunks produced, vectors written, etc.
    config_used     JSONB,          -- step-level config override (if any)
    error_message   TEXT,
    error_code      TEXT
);

CREATE INDEX ON metadata.processing_steps (entity_id, step_type);
CREATE INDEX ON metadata.processing_steps (pipeline_run_id);
CREATE INDEX ON metadata.processing_steps (status) WHERE status IN ('pending','running');

-- ── Data quality checks ───────────────────────────────────────────────────────
CREATE TABLE metadata.data_quality (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id   UUID NOT NULL REFERENCES metadata.entities,
    run_id      UUID REFERENCES metadata.pipeline_runs,
    check_name  TEXT NOT NULL,
    status      TEXT NOT NULL,  -- passed | failed | warning
    value       NUMERIC,
    threshold   NUMERIC,
    message     TEXT,
    checked_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX ON metadata.data_quality (entity_id, check_name);
CREATE INDEX ON metadata.data_quality (status) WHERE status IN ('failed','warning');

-- ── RAG query result linkage ──────────────────────────────────────────────────
-- Which chunks were returned for a query; supports feedback and recall analysis.
CREATE TABLE metadata.query_results (
    query_id        UUID NOT NULL,   -- references entities WHERE entity_type='RAGQuery'
    chunk_entity_id UUID NOT NULL REFERENCES metadata.entities,
    rank            INTEGER NOT NULL,
    score           NUMERIC NOT NULL,
    cached          BOOLEAN NOT NULL DEFAULT FALSE,
    feedback_score  INTEGER,         -- -1 | 0 | 1 (thumbs down / neutral / up)
    PRIMARY KEY (query_id, chunk_entity_id)
);

CREATE INDEX ON metadata.query_results (chunk_entity_id);
CREATE INDEX ON metadata.query_results (feedback_score) WHERE feedback_score IS NOT NULL;
```

---

## 4. Entity Attributes (JSONB schema per type)

### DataSource

```json
{
  "source_type":   "s3",
  "endpoint":      "s3://acme-reports/",
  "connector_id":  "connector-acme-s3",
  "file_types":    ["pdf", "docx"],
  "last_scanned_at": "2026-06-05T10:00:00Z",
  "total_files":   1204,
  "total_bytes":   4412358656
}
```

### RawDocument

```json
{
  "source_id":      "entity-uuid-of-datasource",
  "source_path":    "s3://acme-reports/2024-Q4/board-deck.pdf",
  "content_type":   "application/pdf",
  "size_bytes":     8806163,
  "content_hash":   "sha256:abc123...",
  "etag":           "\"d41d8cd98f00b204e9800998ecf8427e\"",
  "version":        1,
  "page_count":     42,
  "author":         "Finance Team",
  "title":          "Q4 2024 Board Deck",
  "language":       "en"
}
```

### DocumentChunk

```json
{
  "doc_id":              "sha256:abc123...",
  "chunk_index":         3,
  "total_chunks":        12,
  "token_count":         498,
  "char_offset_start":   4821,
  "char_offset_end":     6204,
  "page_number":         7,
  "text_preview":        "Revenue grew 24% YoY driven by...",
  "schema_version_id":   "uuid-of-schema-version"
}
```

### Embedding

```json
{
  "chunk_id":          "sha256:abc123...:3",
  "model_name":        "BAAI/bge-small-en-v1.5",
  "model_version":     "1.0",
  "dimension":         384,
  "backend":           "local-cpu",
  "milvus_pk":         7834921,
  "collection_name":   "acme_docs",
  "embedding_norm":    0.9823,
  "schema_version_id": "uuid-of-schema-version"
}
```

### RAGQuery

```json
{
  "user_id":          "user-uuid",
  "query_text_hash":  "sha256:xyz...",
  "top_k":            5,
  "source_filter":    null,
  "collection":       "acme_docs",
  "latency_ms":       87,
  "cached":           false,
  "model_used":       "BAAI/bge-small-en-v1.5",
  "generate":         false
}
```

---

## 5. Metadata Service

A dedicated Python **FastAPI** service (`metadata-service`) that:
- Consumes Kafka `metadata-events` topic to record entities and lineage
- Exposes REST + GraphQL APIs for lineage traversal and impact analysis
- Powers lineage views in the React SPA and the admin dashboard

### Kafka event topic: `metadata-events`

Each pipeline stage publishes a structured event. The Metadata Service is the single consumer.

```json
// Connector — on file discovery
{
  "specversion": "1.0",
  "type":        "metadata.entity.created",
  "source":      "connector/acme-s3",
  "subject":     "RawDocument/sha256:abc...",
  "data": {
    "entity_type":      "RawDocument",
    "entity_key":       "sha256:abc...",
    "tenant_id":        "tenant-uuid",
    "pipeline_run_id":  "run-uuid",
    "attributes":       { /* see above */ },
    "upstream": [
      { "entity_type": "DataSource",
        "entity_key": "acme/s3/acme-reports/",
        "relationship": "discovered_in" }
    ]
  }
}

// Document Processor — on chunk production
{
  "type": "metadata.entity.created",
  "source": "doc-processor",
  "subject": "DocumentChunk/sha256:abc...:3",
  "data": {
    "entity_type":      "DocumentChunk",
    "entity_key":       "sha256:abc...:3",
    "pipeline_run_id":  "run-uuid",
    "schema_version_id":"schema-v-uuid",
    "attributes":       { /* see above */ },
    "upstream": [
      { "entity_type": "RawDocument",
        "entity_key": "sha256:abc...",
        "relationship": "chunked_into" }
    ],
    "quality_checks": [
      { "check_name": "min_token_count", "status": "passed", "value": 498, "threshold": 50 },
      { "check_name": "not_empty",       "status": "passed" }
    ]
  }
}

// Embedding Worker — on vector storage
{
  "type": "metadata.entity.created",
  "source": "embedding-worker",
  "subject": "Embedding/sha256:abc...:3:bge-small:v1",
  "data": {
    "entity_type":      "Embedding",
    "pipeline_run_id":  "run-uuid",
    "schema_version_id":"schema-v-uuid",
    "attributes":       { /* see above */ },
    "upstream": [
      { "entity_type": "DocumentChunk",
        "entity_key": "sha256:abc...:3",
        "relationship": "embedded_by" },
      { "entity_type": "VectorCollection",
        "entity_key": "acme_docs",
        "relationship": "stored_in" }
    ],
    "quality_checks": [
      { "check_name": "embedding_norm", "status": "passed", "value": 0.9823, "threshold": 0.5 }
    ]
  }
}

// RAG API — on query execution
{
  "type": "metadata.entity.created",
  "source": "rag-api",
  "subject": "RAGQuery/query-uuid",
  "data": {
    "entity_type":  "RAGQuery",
    "entity_key":   "query-uuid",
    "attributes":   { /* see above */ },
    "retrieved_chunks": [
      { "entity_key": "sha256:abc...:3", "rank": 1, "score": 0.847 },
      { "entity_key": "sha256:def...:7", "rank": 2, "score": 0.821 }
    ]
  }
}
```

---

## 6. Key Lineage Queries

### Upstream: chunk → original source

```sql
-- Given a chunk_id, find the original source file and ingestion context
WITH RECURSIVE upstream AS (
    SELECT e.id, e.entity_type, e.entity_key, e.attributes,
           l.relationship, 0 AS depth
    FROM metadata.entities e
    JOIN metadata.lineage l ON l.upstream_id = e.id
    WHERE e.entity_key = :chunk_id

    UNION ALL

    SELECT e.id, e.entity_type, e.entity_key, e.attributes,
           l.relationship, u.depth + 1
    FROM metadata.entities e
    JOIN metadata.lineage l ON l.upstream_id = e.id
    JOIN upstream u ON l.downstream_id = u.id
    WHERE u.depth < 5
)
SELECT entity_type, entity_key,
       attributes->>'source_path'  AS source_path,
       attributes->>'content_type' AS content_type,
       attributes->>'ingested_at'  AS ingested_at,
       relationship, depth
FROM upstream
ORDER BY depth DESC;
```

### Downstream: source file → all derived vectors (impact set)

```sql
-- Given a source file path, find every chunk and embedding derived from it
WITH RECURSIVE downstream AS (
    SELECT e.id, e.entity_type, e.entity_key, 0 AS depth
    FROM metadata.entities e
    WHERE e.entity_type = 'RawDocument'
      AND e.attributes->>'source_path' = :source_path
      AND e.tenant_id = :tenant_id

    UNION ALL

    SELECT e.id, e.entity_type, e.entity_key, d.depth + 1
    FROM metadata.entities e
    JOIN metadata.lineage l ON l.downstream_id = e.id
    JOIN downstream d ON l.upstream_id = d.id
    WHERE d.depth < 4
)
SELECT entity_type,
       COUNT(*)                     AS count,
       ARRAY_AGG(entity_key)        AS entity_keys
FROM downstream
WHERE depth > 0
GROUP BY entity_type;
```

### Staleness: embeddings generated with an outdated schema version

```sql
-- Find all embeddings in a collection that used an older schema version
SELECT e.entity_key AS embedding_id,
       e.attributes->>'chunk_id'    AS chunk_id,
       e.attributes->>'model_name'  AS model_name,
       sv.version_number            AS schema_version,
       sv.embedding_model           AS old_model,
       cv.embedding_model           AS current_model
FROM metadata.entities e
JOIN metadata.schema_versions sv ON sv.id = e.schema_version_id
JOIN metadata.schema_versions cv ON cv.tenant_id = sv.tenant_id
                                 AND cv.is_current = TRUE
WHERE e.entity_type = 'Embedding'
  AND e.tenant_id = :tenant_id
  AND sv.id <> cv.id           -- different from current
ORDER BY sv.version_number;
```

### Provenance: RAG result → full processing chain

```sql
-- Given a query_id, return full provenance of each retrieved chunk
SELECT
    qr.rank,
    qr.score,
    chunk.entity_key                        AS chunk_id,
    chunk.attributes->>'text_preview'       AS text_preview,
    chunk.attributes->>'page_number'        AS page,
    doc.attributes->>'source_path'          AS source_file,
    doc.attributes->>'content_type'         AS format,
    sv.embedding_model                      AS embedding_model,
    sv.chunk_size                           AS chunk_size,
    pr.started_at                           AS indexed_at
FROM metadata.query_results qr
JOIN metadata.entities chunk   ON chunk.id = qr.chunk_entity_id
JOIN metadata.lineage l_chunk  ON l_chunk.downstream_id = chunk.id
                               AND l_chunk.relationship = 'chunked_into'
JOIN metadata.entities doc     ON doc.id = l_chunk.upstream_id
JOIN metadata.schema_versions sv ON sv.id = chunk.schema_version_id
JOIN metadata.pipeline_runs pr ON pr.id = chunk.pipeline_run_id
WHERE qr.query_id = :query_id
ORDER BY qr.rank;
```

---

## 7. Data Quality Checks

Checks are recorded as `DataQualityCheck` entities linked to any entity type.

| Check name | Applies to | Failure condition |
|---|---|---|
| `not_empty` | RawDocument | `size_bytes == 0` |
| `min_token_count` | DocumentChunk | `token_count < 50` |
| `not_duplicate` | RawDocument | same `content_hash` already exists for this tenant |
| `embedding_norm` | Embedding | `norm < 0.5` (very short or noisy text) |
| `parse_success` | RawDocument | parse step returned error |
| `language_detected` | RawDocument | language not in tenant allowlist |
| `chunk_completeness` | RawDocument | `actual_chunks < expected_chunks * 0.9` |

Failing checks set `entity.attributes.quality_status = 'failed'` and trigger a `DataQualityFailed` Kafka event that alerts the admin dashboard.

---

## 8. OpenMetadata Integration (optional enterprise layer)

For enterprise deployments requiring a full data catalog with browse, search, and lineage UI, **OpenMetadata** can be deployed alongside the Metadata Service.

The Metadata Service acts as an OpenMetadata **Custom Connector** that pushes entity and lineage records to the OpenMetadata API on every pipeline run.

```python
# Metadata Service → OpenMetadata push (after each run)
from metadata.generated.schema.entity.data.table import Table
from metadata.ingestion.ometa.ometa_api import OpenMetadata

def push_lineage_to_openmetadata(run_id: str):
    client = OpenMetadata(config)
    for edge in db.query_lineage_by_run(run_id):
        client.add_lineage(
            from_entity=EntityReference(id=edge.upstream_om_id, type=edge.upstream_type),
            to_entity=EntityReference(id=edge.downstream_om_id, type=edge.downstream_type)
        )
```

OpenMetadata provides:
- Visual lineage graph (upstream/downstream in browser)
- Full-text search across all entities and attributes
- Data profiling integration
- Glossary and tagging

**K8s deployment (optional):**

```yaml
helm install openmetadata open-metadata/openmetadata \
  -n metadata --create-namespace \
  --set openmetadata.config.database.host=quota-db-rw.infrastructure.svc \
  --set openmetadata.config.elasticsearch.enabled=false  # use OpenSearch
```

---

## 9. Metadata Service K8s Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: metadata-service
  namespace: ai-pipeline
spec:
  replicas: 1
  template:
    spec:
      containers:
        - name: api
          image: pipeline-metadata-service:latest
          env:
            - {name: DATABASE_URL,
               valueFrom: {secretKeyRef: {name: quota-db-app, key: uri}}}
            - {name: KAFKA_BOOTSTRAP,
               value: "ai-pipeline-kafka-kafka-bootstrap.infrastructure.svc:9092"}
            - {name: METADATA_EVENTS_TOPIC, value: "metadata-events"}
```

The Metadata Service creates a Strimzi `KafkaTopic` CR for `metadata-events` on startup, and a `KafkaUser` with consume-only ACL.

---

## 10. Pipeline Operator — SchemaVersion reconciliation

When `EmbeddingConfig` changes, the Pipeline Operator creates a new `SchemaVersion` record before triggering the rolling restart:

```python
@kopf.on.update('ai-pipeline.io', 'v1alpha1', 'embeddingconfigs', field='spec')
async def reconcile_embedding(spec, old, new, **kwargs):
    # Create new SchemaVersion in metadata DB
    version = await metadata_client.create_schema_version({
        "tenant_id":           spec["tenantId"],
        "chunk_size":          spec["chunkSize"],
        "chunk_overlap":       spec["chunkOverlap"],
        "embedding_model":     spec["model"],
        "embedding_dimension": spec["dimension"],
        "embedding_backend":   spec["backend"],
    })
    # Mark previous version as not current
    await metadata_client.deactivate_previous_versions(spec["tenantId"])
    # ... rest of reconcile (config patch, rolling restart)
```

---

## 11. Trade-offs

| Decision | Choice | Rationale | Downside |
|---|---|---|---|
| Lineage store | PostgreSQL (existing) | No new service; recursive CTEs handle graph traversal for our scale | Not optimised for large graph traversal; consider Neo4j at millions of nodes |
| Entity model | Single `entities` table + JSONB attributes | Flexible schema; easy to add new entity types without migrations | JSONB queries are slower than typed columns; add GIN index per attribute |
| Event-driven capture | Kafka `metadata-events` topic | Decoupled; pipeline stages don't wait on metadata writes | Metadata may lag ingestion by seconds; eventual consistency |
| OpenMetadata | Optional overlay | Avoid mandatory heavy dependency; use only when catalog features needed | Duplication between metadata DB and OpenMetadata — keep metadata DB authoritative |
| Quality checks | In-process + event-driven | Checks run at the point of production; no separate scan job | Adding new checks requires code changes in each worker; consider pluggable check registry |
