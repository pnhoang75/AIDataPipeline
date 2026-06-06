# AI Data Pipeline — Sequence Diagrams

Four key flows rendered in Mermaid. Renders natively on GitHub.

---

## 1. Document Ingestion Flow

End-to-end path from a source file arriving at a connector to its embedding being stored in Milvus.

```mermaid
sequenceDiagram
    autonumber
    participant Src  as Source<br/>(S3 / NFS / DB / Stream)
    participant Con  as Connector Pod
    participant Redis as Redis<br/>(watermark)
    participant K1   as Kafka<br/>raw-documents
    participant DP   as Document Processor
    participant K2   as Kafka<br/>document-chunks
    participant EW   as Embedding Worker
    participant Milvus as Milvus
    participant PG   as PostgreSQL<br/>(file status)
    participant OM   as OpenMeter

    Note over Con,Redis: Poll cycle (every pollInterval or event-driven)

    Con->>Redis: GET watermark:{connector_id}
    Redis-->>Con: last_seen timestamp / offset

    Con->>Src: List objects modified after watermark
    Src-->>Con: [file_a.pdf, file_b.docx, ...]

    loop For each new file
        Con->>PG: INSERT source_file_status (pending)
        Con->>K1: Produce RawDocumentEvent<br/>{event_id, source_type, content_ref, tenant_id}
        Con->>Redis: SET watermark:{connector_id} = now()
    end

    Note over DP: Consumer group: doc-processor

    K1->>DP: Poll RawDocumentEvent (batch)
    DP->>Src: Fetch content via content_ref
    Src-->>DP: Raw bytes (PDF / DOCX / ...)

    DP->>DP: Parse (pdfplumber / python-docx / ...)<br/>→ plain text

    DP->>DP: Chunk text<br/>(512 tokens / 64 overlap)

    loop For each chunk
        DP->>K2: Produce ChunkEvent<br/>{doc_id, chunk_id, chunk_index, text, tenant_id}
    end

    alt Parse error
        DP->>PG: UPDATE file_status = 'error', error_message
        DP->>K1: Produce to dlq-raw-documents
    end

    Note over EW: Consumer group: embedding-worker

    K2->>EW: Poll ChunkEvent (batch of 32)
    EW->>EW: embed_batch(texts)<br/>(BGE-small / BGE-large / OpenAI)

    EW->>Milvus: Insert vectors into {tenant_id}_docs collection
    Milvus-->>EW: Insert ACK

    EW->>PG: UPDATE file_status = 'indexed',<br/>chunk_count, indexed_at

    EW->>OM: Publish CloudEvent<br/>pipeline.embedding.batch<br/>{gpu_seconds, bytes, tenant_id}

    alt Embedding error
        EW->>PG: UPDATE file_status = 'error'
        EW->>K2: Produce to dlq-document-chunks
    end
```

---

## 2. RAG Query Flow

Path from a user's HTTP request through Kong, the RAG API, and back with results.

```mermaid
sequenceDiagram
    autonumber
    participant Client as Client<br/>(Browser / LangChain)
    participant KC    as Keycloak<br/>(JWKS)
    participant Kong  as Kong API Gateway
    participant QS    as Quota Service
    participant RAG   as RAG API
    participant Redis as Redis<br/>(query cache)
    participant Milvus as Milvus
    participant LLM   as LLM<br/>(optional)
    participant K3    as Kafka<br/>usage-events

    Client->>Kong: POST /v1/query<br/>Authorization: Bearer <jwt>

    Note over Kong: Plugin chain (ordered)

    Kong->>KC: GET /protocol/openid-connect/certs (JWKS)<br/>(cached 60s)
    KC-->>Kong: Public key
    Kong->>Kong: Verify RS256 signature<br/>Extract org_id → X-Tenant-ID

    Kong->>QS: gRPC CheckQuota<br/>{tenant_id, metric: api_calls_per_day, amount: 1}
    QS->>QS: INCR quota:{tenant_id}:api_calls:{today}
    QS-->>Kong: {allowed: true, current: 42, limit: 10000}

    alt Quota exceeded
        QS-->>Kong: {allowed: false, deny_reason: "daily limit reached"}
        Kong-->>Client: 429 Too Many Requests<br/>{error: QUOTA_EXCEEDED, retry_after: 38400}
    end

    Kong->>RAG: POST /v1/query<br/>X-Tenant-ID: acme

    RAG->>RAG: hash(query + params) → cache_key

    RAG->>Redis: GET cache_key
    Redis-->>RAG: cache miss

    RAG->>RAG: embed(query) → float[384]<br/>(same backend as ingestion)

    RAG->>Milvus: search(collection=acme_docs,<br/>vector=query_emb, top_k=5, ef=64)
    Milvus-->>RAG: [{chunk_id, text, score, metadata}, ...]

    RAG->>Redis: SET cache_key = results (TTL 60s)

    alt generate=true
        RAG->>LLM: chat(system_prompt + chunks + query)
        LLM-->>RAG: generated answer tokens
    end

    RAG-->>Kong: 200 QueryResponse<br/>{results, latency_ms, cached: false}

    RAG->>K3: Produce CloudEvent<br/>pipeline.rag.query<br/>{tenant_id, latency_ms}

    Kong-->>Client: 200 QueryResponse
```

---

## 3. Tenant Provisioning Flow

Creating a new tenant end-to-end via the admin UI.

```mermaid
sequenceDiagram
    autonumber
    participant Admin  as Admin User
    participant UI     as React SPA
    participant BFF    as Pipeline Mgmt API
    participant KC     as Keycloak Admin API
    participant PG     as PostgreSQL
    participant PO     as Pipeline Operator (kopf)
    participant Milvus as Milvus
    participant Kafka  as Strimzi<br/>(KafkaUser CR)
    participant QS     as Quota Service

    Admin->>UI: Fill "New Tenant" form<br/>{name, slug, license: pro, admin_email}

    UI->>BFF: POST /api/admin/tenants

    BFF->>KC: POST /admin/realms/ai-pipeline/organizations<br/>{name, domains: [slug]}
    KC-->>BFF: {org_id: uuid}

    BFF->>KC: POST /admin/realms/ai-pipeline/users<br/>{email: admin_email}
    KC-->>BFF: {user_id: uuid}
    BFF->>KC: PUT user → join org, assign role: admin

    BFF->>PG: INSERT tenant_licenses<br/>{tenant_id: org_id, tier: pro}

    BFF->>PO: kubectl apply TenantWorkspace CR<br/>{tenantId: slug, milvusCollection: slug_docs,<br/>vectorDimension: 384, licenseRef: pro}

    Note over PO: Operator reconciles TenantWorkspace

    PO->>Milvus: create_collection(slug_docs, schema)
    Milvus-->>PO: OK

    PO->>Kafka: Apply KafkaUser CR<br/>{name: tenant-slug-processor,<br/>acl: produce raw-documents, consume document-chunks}

    PO->>QS: RegisterTenant(tenant_id, license_tier: pro)
    QS->>PG: INSERT quota limits from license_tiers[pro]
    QS-->>PO: OK

    PO->>PO: Set TenantWorkspace.status.state = Provisioned

    BFF->>PO: Watch TenantWorkspace until state = Provisioned
    PO-->>BFF: Provisioned

    BFF-->>UI: 201 Tenant<br/>{id, name, slug, license_type: pro, status: active}

    UI-->>Admin: "Tenant Acme Corp created"<br/>Send invite email to admin_email
```

---

## 4. Coordinated Pipeline Upgrade Flow

Bumping `PipelineCluster.spec.version` triggers the Pipeline Operator's 7-step upgrade sequence.

```mermaid
sequenceDiagram
    autonumber
    participant Eng   as Engineer
    participant Git   as Git / ArgoCD
    participant PO    as Pipeline Operator
    participant Con   as Connector CronJobs
    participant Kafka as Kafka<br/>(consumer lag)
    participant DP    as Doc Processor Pods
    participant EW    as Embedding Workers
    participant RAG   as RAG API Pods
    participant Prom  as Prometheus

    Eng->>Git: git commit — bump PipelineCluster.spec.version: "1.3.0" → "1.4.0"
    Eng->>Git: git push origin main → PR → merge

    Git->>Git: ArgoCD detects drift
    Git->>PO: Sync PipelineCluster CR (version: 1.4.0)

    Note over PO: Step 1 — Signal upgrade start

    PO->>PO: Set condition UpgradeInProgress=True
    PO->>PO: Record upgrade start time + Event

    Note over PO: Step 2 — Pause connectors

    PO->>Con: Patch all DataConnector CRs: paused=true
    Con-->>PO: CronJobs suspended

    Note over PO: Step 3 — Drain Kafka

    loop Poll every 10s (max 10 min)
        PO->>Kafka: AdminClient.list_consumer_group_offsets
        Kafka-->>PO: consumer_lag{doc-processor, document-chunks}
        alt lag == 0
            PO->>PO: Proceed to step 4
        end
    end

    Note over PO: Step 4 — Roll Document Processor

    PO->>DP: Patch Deployment image → pipeline-processor:1.4.0
    DP-->>PO: RollingUpdate complete (readiness probe passes)

    Note over PO: Step 5 — Roll Embedding Workers

    PO->>EW: Patch Deployment image → pipeline-embedder:1.4.0
    EW-->>PO: RollingUpdate complete

    Note over PO: Step 6 — Roll RAG API + smoke test

    PO->>RAG: Patch Deployment image → pipeline-rag:1.4.0
    RAG-->>PO: RollingUpdate complete

    PO->>RAG: GET /v1/health
    RAG-->>PO: {status: ok}

    Note over PO: Step 7 — Resume + clear

    PO->>Con: Patch all DataConnectors: paused=false
    Con-->>PO: CronJobs resumed

    PO->>PO: Clear UpgradeInProgress condition
    PO->>PO: Write K8s Event: upgrade complete

    Git->>Git: ArgoCD marks Application Synced + Healthy

    Note over Prom: Post-upgrade monitoring (5 min window)

    Prom->>Prom: Evaluate RAGLatencyHigh rule
    alt p99 > 1s
        Prom->>Eng: Alert — RAGLatencyHigh (severity: warning)
        Eng->>Git: git revert — bump version back to "1.3.0"
        Git->>PO: Sync PipelineCluster CR (version: 1.3.0)
        Note over PO: Operator replays steps in reverse
    end
```
