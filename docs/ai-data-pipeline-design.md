# AI Data Pipeline — System Design

**Version:** 1.0  
**Target environment:** Kubernetes (kind cluster, CPU-only testbed → GPU-capable production)  
**Stack:** Python · Kafka · Milvus · FastAPI · LangChain-compatible

---

## 1. Requirements

### Functional

- Ingest documents from four source types: block storage (S3/MinIO), NFS file server, relational databases, and live Kafka streams
- Parse and chunk documents of multiple formats: PDF, DOCX, HTML, plain text, JSON, CSV
- Generate vector embeddings via a pluggable backend (CPU local model for testbed, GPU or API-based for production)
- Persist vectors plus metadata in Milvus
- Expose a RAG query endpoint (REST + LangChain-compatible VectorStore interface)
- Support re-ingestion and updates when source documents change

### Non-Functional

- **Scale:** GBs of indexed data, under 100 QPS on the RAG endpoint
- **Latency:** p99 RAG query under 500 ms (excluding LLM generation)
- **No GPU** on the kind testbed — all embedding runs on CPU
- **GPU-ready:** switching to GPU in production requires only a config change, no code change
- **Kubernetes-native:** every component runs as a K8s workload; Helm-managed
- **Observable:** Prometheus metrics + structured JSON logs on every service
- **Resilient:** failed messages land in dead-letter queues; connectors are idempotent

### Constraints

- kind cluster (single-machine multi-node) for local development
- Python throughout for consistency and ecosystem fit (sentence-transformers, PyMilvus, FastAPI)
- Kafka as the central message bus (already specified)

---

## 2. Component Architecture

### 2.1 Source Connectors

Each connector is a **Python Kubernetes Deployment** (or CronJob for batch sources) responsible for monitoring one source type and publishing raw document references to the `raw-documents` Kafka topic. Connectors are intentionally thin — they do not parse content.

**Connector interface (abstract base):**

```python
class SourceConnector(ABC):
    @abstractmethod
    def poll(self) -> Iterator[RawDocumentEvent]: ...

    @abstractmethod
    def ack(self, event_id: str) -> None: ...
```

**S3 / Block Storage Connector**
- Polls MinIO (or S3 in production) for new/modified objects using a watermark stored in Redis
- Alternatively driven by MinIO bucket event notifications → Kafka (more reactive)
- Publishes object key + bucket + ETag; actual content fetched downstream

**NFS Connector**
- NFS share mounted as a Kubernetes PersistentVolume
- Uses `watchdog` (inotify on Linux) for real-time detection; falls back to periodic tree diff
- Filters by configurable file extension allowlist

**Database Connector**
- **Recommended:** Debezium CDC via Kafka Connect — captures row-level changes without polling overhead
- **Simple alternative:** watermark polling (`SELECT ... WHERE updated_at > :last_seen`) for read-only access
- Serialises row content as JSON, published as a raw document event

**Kafka Stream Connector**
- Bridges an existing upstream Kafka topic into the `raw-documents` topic
- Applies a configurable transformation/filter if schemas differ
- Deployed as a KStreams-style consumer-producer in Python (confluent-kafka)

**Raw document event schema (Avro / JSON):**

```json
{
  "event_id": "uuid",
  "source_type": "s3 | nfs | database | stream",
  "source_id": "bucket/key or /mnt/path or table/pk or topic/offset",
  "content_ref": "s3://bucket/key or inline:<base64>",
  "content_type": "application/pdf | text/plain | ...",
  "metadata": { "title": "...", "author": "...", "tags": [] },
  "ingested_at": 1717401600
}
```

---

### 2.2 Kafka Topics

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `raw-documents` | 4 | 7 days | Raw doc events from all connectors |
| `document-chunks` | 8 | 3 days | Parsed and chunked text ready for embedding |
| `embedding-events` | 4 | 1 day | Completion/error audit trail |
| `dlq-raw-documents` | 1 | 14 days | Failed raw doc processing |
| `dlq-document-chunks` | 1 | 14 days | Failed embedding |

Partition key: `doc_id` — ensures all chunks from the same document land on the same partition and are processed in order.

---

### 2.3 Document Processor

A Python **Deployment** (2–4 replicas) that consumes `raw-documents`, fetches content, parses it, and publishes chunks to `document-chunks`.

**Parser selection by MIME type:**

| Format | Library |
|---|---|
| PDF | `pdfplumber` (layout-aware) |
| DOCX | `python-docx` |
| HTML | `BeautifulSoup4` + `html2text` |
| Plain text | built-in |
| CSV | `pandas` |
| JSON | built-in + `jq`-style flattening |

**Chunking strategy (configurable per source):**

- **Fixed-size with overlap** (default): 512-token chunks, 64-token overlap. Uses `tiktoken` for token counting.
- **Sentence-based**: uses `spacy` sentence boundary detection, respects paragraph structure.
- **Semantic** (optional, expensive): splits at embedding similarity drop-offs. Best RAG quality.

---

### 2.4 Embedding Worker

A Python **Deployment** that consumes `document-chunks` and writes embeddings to Milvus. The embedding backend is fully pluggable via a protocol interface.

| Backend | Model | Device | Dimension | Notes |
|---|---|---|---|---|
| `LocalCPUBackend` | `BAAI/bge-small-en-v1.5` | CPU | 384 | Default for kind testbed; ~90 MB |
| `LocalGPUBackend` | `BAAI/bge-large-en-v1.5` | CUDA | 1024 | Switch for production GPU nodes |
| `OpenAIBackend` | `text-embedding-3-small` | API | 1536 | No local compute; for evaluation |

Selected via environment variable: `EMBEDDING_BACKEND=local-cpu | local-gpu | openai`

---

### 2.5 Milvus Vector Database

**Testbed:** Milvus standalone mode  
**Production:** Milvus cluster via the Milvus Operator

**Collection schema:**

```
id          INT64       auto-id primary key
doc_id      VARCHAR(256)
chunk_id    VARCHAR(256) unique
source_type VARCHAR(64)
text        VARCHAR(65535)
embedding   FLOAT_VECTOR(384)   # 1024 for GPU backend
created_at  INT64
metadata    JSON
```

**Index:** `IVF_FLAT` (testbed) → `HNSW M=16, ef_construction=200` (production)

---

### 2.6 RAG API Service

A **FastAPI** application exposing REST + LangChain-compatible `VectorStore`.

```
POST /v1/query    { query, top_k, source_filter?, collection? }
GET  /v1/health
GET  /v1/collections
```

**Query flow:** embed query → Redis cache check → Milvus ANN search → return top-K chunks → optional LLM generation

---

## 3. Kubernetes Layout

| Namespace | Contents |
|---|---|
| `ai-pipeline` | Connectors, doc processor, embedding worker, RAG API |
| `infrastructure` | Kafka, Milvus, MinIO, Redis |
| `monitoring` | Prometheus, Grafana, Loki |

---

## 4. Trade-off Analysis

| Decision | Choice | Rationale | Downside |
|---|---|---|---|
| Message bus | Kafka (KRaft, single broker) | Durable, replayable, fan-out | Heavier than Redis Streams for small scale |
| Embedding model | `BAAI/bge-small-en-v1.5` | Strong CPU performance (384-dim) | Lower recall than `bge-large`; upgrading changes dimension → re-index |
| Vector DB | Milvus standalone | K8s-native; same API for cluster mode | Heavier than Chroma/Qdrant for testbed |
| Milvus index | IVF_FLAT → HNSW | IVF_FLAT: no build overhead; HNSW: better recall/QPS | HNSW build time grows with corpus |
| Chunking | Fixed-size + overlap | Predictable, fast | Less semantically coherent splits |
| LangChain compat | Custom `VectorStore` subclass | Plugs into LangChain/LangGraph | Version drift risk |

---

## 5. Roadmap

1. Semantic chunking
2. Milvus cluster mode
3. CDC via Debezium
4. Dimension upgrade (blue/green collection strategy)
5. Multi-tenancy
6. Streaming RAG responses (SSE)
7. Feedback loop (user ratings → retrieval quality)
8. Connector plugin registry (K8s CRD)
