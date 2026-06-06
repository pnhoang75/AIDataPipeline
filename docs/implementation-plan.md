# AI Data Pipeline — Phased Implementation Plan

**Version:** 1.1  
**Date:** 2026-06-05  
**Scope:** kind testbed → production-ready Kubernetes system

---

## Claude Pro Implementation Strategy

Claude Pro has two hard constraints that shape how all implementation work is structured:

**Context window:** Each conversation accumulates tokens from file reads, tool output, and edits. A conversation that reads 10 large files and runs several test suites can exhaust usable context within 1–2 hours, causing slower responses and potential truncation.

**Usage limits:** Claude Pro resets on a rolling window. Long, unfocused sessions burn the budget faster than short, focused ones.

### Rules for every implementation session

1. **One service per conversation.** Start a new Claude Code conversation for each service or K8s manifest group. Never implement two unrelated services in the same session.
2. **Read only what's needed.** Open this plan and the relevant design doc at the start of the session. Do not read docs for other phases.
3. **Use `/compact`** when a conversation exceeds ~30 exchanges — it compresses history and extends the session.
4. **Commit after every working unit.** A passing test suite or a deployable manifest is a commit boundary. This preserves progress if the session ends.
5. **Write a per-service `CLAUDE.md`** at the root of each service directory (see §below). Claude reads this automatically, eliminating the need to re-explain context across sessions.
6. **Verify before moving on.** Run tests and `kubectl apply --dry-run` before closing a session. Broken state left between sessions costs a full re-read to diagnose.

### Session sizing guide

| Session type | Scope | Typical duration |
|---|---|---|
| Manifest / config | 1 operator install + its CRDs | 30–45 min |
| Service scaffold | One Python service skeleton + Dockerfile + K8s Deployment | 45–60 min |
| Service feature | One feature (e.g., chunking strategy, circuit breaker) + its unit tests | 45–90 min |
| Integration test | One integration test file for one service pair | 30–60 min |
| E2E test | One E2E scenario | 45–60 min |
| Debugging | One failing test or broken deploy | 30–60 min |

### Per-service `CLAUDE.md` template

Place a `CLAUDE.md` in every service directory (`services/rag-api/CLAUDE.md`, `services/embedding-worker/CLAUDE.md`, etc.) with this structure:

```markdown
# <Service Name>

## Purpose
One sentence.

## Relevant design docs
- docs/ai-data-pipeline-design.md §2.X
- docs/ai-data-pipeline-error-handling.md §2.X

## Key dependencies
- Kafka bootstrap: ${KAFKA_BOOTSTRAP}
- Milvus host: ${MILVUS_HOST}

## How to run locally
docker compose up kafka milvus redis
pytest tests/unit/

## Known constraints
- List any non-obvious invariants Claude should not break
```

### Session sequence per phase

Each phase below is broken into **numbered sessions**. Work the sessions in order. Each session is self-contained: it has a defined input, output, and done-check so you can hand it to Claude with a one-line prompt like _"Complete Session 1-A as described in the implementation plan."_

---

## Phase 0 — Foundation: Cluster & Operators

**Goal:** Stand up a stable, fully-instrumented Kubernetes cluster with all infrastructure operators installed in dependency order so every subsequent phase deploys into a known-good base.

### Key Tasks

- Provision a kind cluster with labeled worker nodes (min 3 nodes: control-plane, infra, workload)
- Apply namespace manifest for `ai-pipeline`, `infrastructure`, `monitoring`; label `ai-pipeline` with `pod-security.kubernetes.io/enforce: restricted`
- Apply default-deny-all `NetworkPolicy` to all three namespaces
- Install **cert-manager** first (v1.14); create `selfsigned-ca` ClusterIssuer and `pipeline-ca-issuer` ClusterIssuer
- Install infrastructure operators in order: **Strimzi** (0.41), **CloudNativePG** (1.23), **Milvus Operator** (0.9), **MinIO Operator** (5.0), **Redis Operator** (0.15)
- Install **OPA Gatekeeper** (3.16); set `failurePolicy: Ignore` for testbed; define `K8sRequireNonRootUser`, `K8sRequireReadOnlyRootFS`, `K8sRequireResourceLimits`, `K8sBlockLatestImageTag` ConstraintTemplates
- Install **kube-prometheus-stack** (58.x) with Prometheus, AlertManager, and Grafana in `monitoring`
- Install **OTel Operator** (0.57) and create `OpenTelemetryCollector` + `Instrumentation` CR for Python auto-instrumentation (10% sampling on testbed)
- Install **Grafana Operator** (5.x) for `GrafanaDashboard` CRD support
- Install **Bitnami Sealed Secrets** for testbed secret management; document the `kubeseal` workflow
- Install **ArgoCD** (6.x); configure a GitOps repo; create the `ai-pipeline-infra` Application CR with `selfHeal: true` and `prune: true`
- Bootstrap the MinIO Tenant (`pipeline-store`) with buckets: `pipeline-backups`, `opa-bundles`, `pipeline-artifacts`
- Bootstrap the CloudNativePG `quota-db` cluster (1 instance for testbed); configure CNPG scheduled backup to MinIO; enable `pgaudit` extension
- Bootstrap the Redis Operator standalone instance in `infrastructure`
- Commit all manifests to the GitOps repo; validate ArgoCD syncs cleanly to `Synced / Healthy`

### Exit Criteria

- All operator pods are `Running` in their respective namespaces
- ArgoCD Application shows `Synced / Healthy`
- Prometheus scrapes operators' `/metrics` endpoints; Grafana dashboard loads
- `kubectl apply` of a pod without `runAsNonRoot: true` is rejected or warns (Gatekeeper)
- `kubectl exec` into `quota-db` pod and `SELECT 1;` returns successfully
- A sealed secret can be decrypted and mounted in a test pod

### Sessions

| Session | Prompt to Claude | Done when |
|---|---|---|
| **0-A** | "Create the kind cluster config (3 nodes), namespace manifests with PSS labels, and default-deny NetworkPolicy for ai-pipeline, infrastructure, and monitoring namespaces. Commit to k8s/base/." | `kubectl get ns` shows all 3 namespaces |
| **0-B** | "Write Helm install scripts for cert-manager, then Strimzi, CNPG, Milvus Operator, MinIO Operator, Redis Operator in the correct bootstrap order. Verify each CRD registers before proceeding to the next." | All operator pods Running |
| **0-C** | "Install OPA Gatekeeper with failurePolicy:Ignore. Write ConstraintTemplate and Constraint CRs for K8sRequireNonRootUser, K8sRequireReadOnlyRootFS, K8sRequireResourceLimits, K8sBlockLatestImageTag." | Non-compliant test pod is warned |
| **0-D** | "Install kube-prometheus-stack, OTel Operator, and Grafana Operator. Create the OpenTelemetryCollector CR and Python Instrumentation CR." | Prometheus UI reachable; Grafana loads |
| **0-E** | "Install Bitnami Sealed Secrets and ArgoCD. Bootstrap the MinIO Tenant, CloudNativePG quota-db cluster, and Redis standalone. Wire ArgoCD Application CR to the GitOps repo." | ArgoCD shows Synced/Healthy |

**Estimated Effort:** L (5 sessions × ~45 min)

---

## Phase 1 — Core Pipeline: Connectors → Kafka → Processor → Embedder → Milvus → RAG API

**Goal:** Deliver an end-to-end functional pipeline that ingests documents from S3/MinIO, processes and chunks them, generates embeddings, stores vectors in Milvus, and answers RAG queries via FastAPI — all without multi-tenancy.

### Key Tasks

**Kafka**
- Create Strimzi `Kafka` CR (`ai-pipeline-kafka`, 1 broker, KRaft mode, persistent 20 Gi)
- Create `KafkaTopic` CRs: `raw-documents` (4p/7d), `document-chunks` (8p/3d), `embedding-events` (4p/1d), `dlq-raw-documents` (1p/14d), `dlq-document-chunks` (1p/14d), `metadata-events` (4p/7d)

**S3/MinIO Connector**
- Implement `SourceConnector` abstract base with `poll()` and `ack()` interface
- Implement `S3Connector`: watermark stored in Redis (`HSET connector:{id}:watermark`); publish `RawDocumentEvent` (Avro/JSON schema as per design doc) to `raw-documents`
- Implement `NFS Connector`: `watchdog`/inotify with periodic tree-diff fallback; extension allowlist; PersistentVolume mount
- Add `source_file_status` PostgreSQL table migration (Flyway or Alembic); connector writes `pending` rows on discovery
- Build connector Docker image; create `connector-sa` ServiceAccount and `connector-role` (read ConfigMap `pipeline-config` + named secrets only — **fix: list exact secret names, no wildcards**)
- Apply NetworkPolicy `allow-connectors-to-kafka` and `allow-connectors-to-postgres`

**Document Processor**
- Implement consumers with manual commit (`enable.auto.commit: false`) and consumer group `doc-processor`
- Implement parser selection by MIME type: `pdfplumber` (PDF), `python-docx` (DOCX), `BeautifulSoup4`+`html2text` (HTML), `pandas` (CSV), built-in (plain text, JSON)
- Implement fixed-size chunker with 512-token chunks / 64-token overlap using `tiktoken`; make chunk size and overlap configurable via `pipeline-config` ConfigMap
- Publish `DocumentChunk` events to `document-chunks`; route parse failures to `dlq-raw-documents`; update `source_file_status` to `error` on failure
- Deploy as `Deployment` (2 replicas); apply `doc-processor-sa` ServiceAccount; apply NetworkPolicy
- Retry policy: fetch failures retry 3× (1 s/4 s/16 s) then DLQ; parse errors go directly to DLQ (commit offset)

**Embedding Worker**
- Implement pluggable `EmbeddingBackend` protocol; implement `LocalCPUBackend` with `BAAI/bge-small-en-v1.5` (384-dim, ~90 MB); implement `OpenAIBackend` stub
- Backend selected via `EMBEDDING_BACKEND` env var
- Batch consumer: accumulate up to 32 chunks or 500 ms, whichever comes first
- Write embeddings to Milvus collection (`documents`) using PyMilvus; create IVF_FLAT index on testbed
- Publish completion events to `embedding-events`; route embedding failures to `dlq-document-chunks`
- Update `source_file_status` to `indexed` with `chunk_count`
- Deploy as `Deployment` (2 replicas); mount model cache as `emptyDir` (1 Gi sizeLimit); apply NetworkPolicy

**Milvus**
- Apply Milvus Operator `Milvus` CR in standalone mode (2 Gi memory request)
- Create collection schema: `id`, `doc_id`, `chunk_id`, `source_type`, `text`, `embedding FLOAT_VECTOR(384)`, `created_at`, `metadata JSON`
- Create IVF_FLAT index on `embedding` field

**RAG API**
- Implement `POST /v1/query` (embed → Redis cache check → Milvus ANN search → return top-K), `GET /v1/health`, `GET /v1/collections`
- Implement `LangChainVectorStore` subclass for LangChain compatibility
- Redis query cache: key = SHA-256(query+top_k+filter), TTL = 300 s
- Expose Prometheus metrics: `rag_query_duration_seconds`, `rag_cache_hits_total`, `rag_query_errors_total`
- Deploy as `Deployment` (2 replicas) behind a K8s Service; apply NetworkPolicy

**Observability**
- Annotate all pods with `instrumentation.opentelemetry.io/inject-python: "true"`
- Create `ServiceMonitor` CRs for doc-processor, embedding-worker, rag-api
- Create `PrometheusRule` CR: `KafkaConsumerLagHigh` (>5000, 5 m), `ConnectorDown` (up==0, 2 m), `RAGLatencyHigh` (p99>1 s, 3 m)
- Add `pipeline-config` ConfigMap with all timeout values from the error-handling doc

### Exit Criteria

- Place a PDF in MinIO; within 60 s the connector publishes to `raw-documents`, processor publishes to `document-chunks`, worker writes to Milvus, and `source_file_status` shows `indexed`
- `POST /v1/query` with a relevant query returns ≥1 chunk with `score > 0.5` and p99 latency < 500 ms
- Kafka consumer lag for `document-chunks` stays near 0 at idle
- DLQ topics are empty after a clean run
- Prometheus shows metrics from all three services

### Sessions

| Session | Prompt to Claude | Done when |
|---|---|---|
| **1-A** | "Create the Strimzi Kafka CR, all 6 KafkaTopic CRs, and the pipeline-config ConfigMap with all timeout values from docs/ai-data-pipeline-error-handling.md §1. Apply and verify topics exist." | `kafka-topics.sh --list` shows all 6 topics |
| **1-B** | "Scaffold the S3 connector service (services/connector-s3/): SourceConnector ABC, S3Connector implementation, RawDocumentEvent schema, watermark logic, source_file_status Postgres write. Write unit tests (mock Kafka + MinIO + Redis). No Docker or K8s yet." | Unit tests pass |
| **1-C** | "Add the NFS connector to services/connector-nfs/. Reuse the SourceConnector base. Add watchdog/inotify + tree-diff fallback. Write unit tests." | Unit tests pass |
| **1-D** | "Scaffold the Document Processor (services/doc-processor/): Kafka consumer (manual commit), all parsers, fixed-size chunker, DLQ routing. Write unit tests covering each parser and the offset-not-committed-on-failure case." | Unit tests pass |
| **1-E** | "Write integration tests for Connector→Kafka→Processor using testcontainers (Kafka + MinIO + Redis + PostgreSQL). Tests: happy path PDF, corrupt PDF→DLQ, watermark prevents duplicates." | Integration tests pass |
| **1-F** | "Scaffold the Embedding Worker (services/embedding-worker/): LocalCPUBackend, batch accumulator (32 chunks / 500ms), Milvus upsert, DLQ routing, status update. Write unit tests (mock Milvus)." | Unit tests pass |
| **1-G** | "Write integration tests for Processor→Embedder→Milvus using testcontainers. Tests: vector written, idempotent upsert, embedding timeout→DLQ, postgres status=indexed." | Integration tests pass |
| **1-H** | "Scaffold the RAG API (services/rag-api/): POST /v1/query, GET /v1/health, Redis cache, circuit breaker, Milvus ANN search, Prometheus metrics. Write unit tests for all cases in the test plan §2.4." | Unit tests pass |
| **1-I** | "Write Dockerfiles for connector-s3, connector-nfs, doc-processor, embedding-worker, rag-api. Write K8s Deployment + ServiceAccount + NetworkPolicy manifests for each. Apply to kind; run smoke test: upload PDF, verify /v1/query returns a result." | End-to-end smoke test passes |
| **1-J** | "Add ServiceMonitor CRs and PrometheusRule CR (KafkaConsumerLagHigh, ConnectorDown, RAGLatencyHigh). Verify Prometheus scrapes all services and rules appear in the alert UI." | All metrics visible in Prometheus |

**Estimated Effort:** XL (10 sessions × 45–90 min)

---

## Phase 2 — Multi-Tenancy & Security

**Goal:** Enforce tenant isolation, authentication, authorization, quota limits, and usage metering across all pipeline components using Keycloak, Kong, OPA, and the Quota Service.

### Key Tasks

**Keycloak**
- Install Keycloak Operator (24.x); configure realm `ai-pipeline` with Organizations enabled
- Configure OIDC client for the SPA (PKCE, implicit flow disabled) and a confidential client for the BFF
- Define realm roles: `pipeline-admin`, `pipeline-user`, `pipeline-viewer`
- Configure JWT claims mapper to include `org_id`, `org_name`, `license_type`, `quota_tier`, `roles`
- Create seed organizations: `free-tier-demo`, `pro-tier-demo`

**Kong OSS**
- Install Kong Ingress Controller; expose port 443 via NodePort/LoadBalancer for kind
- Configure Kong Route `/v1/*` → RAG API with plugin chain: `jwt` (RS256 JWKS from Keycloak) → `request-transformer` (extract `org_id` → `X-Tenant-ID`) → `rate-limiting-advanced` → upstream
- Write Lua quota-check plugin (gRPC call to Quota Service, fail-open with 100 ms timeout)
- Configure Kong Route `/api/*` → BFF with `jwt` + `request-transformer` plugins

**PostgreSQL Schema**
- Apply migrations: `license_tiers`, `tenant_licenses`, `quota_overrides`, `usage_history` (partitioned by month) tables in `public` schema; `workspaces`, `workspace_sources`, `source_file_status` tables; full `metadata` schema (Phase 5 will populate it, but schema can be created here)
- Enable `pgaudit` logging for `tenant_licenses` and `quota_overrides` tables
- Seed `license_tiers` rows: Free, Pro, Enterprise per the quota table in the multitenancy doc

**Quota Service**
- Implement Python gRPC service with proto: `CheckQuota`, `RecordUsage`, `GetUsage`
- Redis counter pattern: `INCRBY` + `EXPIRE` per tenant/metric key; rollback on limit exceeded
- Back daily/monthly counters with PostgreSQL `usage_history` (batch flush every 60 s)
- Deploy as `Deployment` (1 replica); expose port 50051; create cert-manager `Certificate` for mTLS; apply `quota-service-sa` ServiceAccount and NetworkPolicy

**OPA Gatekeeper Policies**
- Deploy `K8sRequireTenantLabel` constraint on `ai-pipeline` namespace pods
- Deploy `K8sBlockCrossNamespaceSecret` constraint on connector pods
- Implement Rego policy bundle in MinIO `opa-bundles` bucket: GPU access by license tier, connector type allowlist by license tier, collection access scoped to `{tenant_id}_docs`
- Configure OPA bundle polling from MinIO every 60 s

**Tenant Isolation**
- Modify RAG API: derive Milvus collection name from `X-Tenant-ID` header (never from request body)
- Modify Embedding Worker: include `tenant_id` in Kafka message headers; write to per-tenant Milvus collection `{tenant_id}_docs`
- Add Kafka ACL `KafkaUser` CRs: produce-only per connector, consume-only per processor/worker, scoped by tenant label
- Enterprise tenant provisioning: create dedicated namespace + `ResourceQuota` CR

**OpenMeter**
- Deploy OpenMeter in `infrastructure`; connect to Kafka `usage-events` topic
- Define meters: `bytes_ingested`, `api_calls`, `gpu_seconds` (with `tenant_id` group-by per design doc)
- RAG API publishes `pipeline.rag.query` event to `usage-events` on each query
- Embedding Worker publishes `pipeline.embedding.batch` event with `gpu_seconds`

**Security Hardening**
- Mount all secrets as files (not env vars); update Deployment specs for all Phase 1 services
- Apply restricted PodSecurityStandard to `ai-pipeline` namespace (label already set in Phase 0)
- Add `seccompProfile: RuntimeDefault`, `readOnlyRootFilesystem: true`, `runAsNonRoot: true` to all pod specs; add `emptyDir` mounts for `/tmp` and model cache
- Fix connector-sa RBAC: **replace `resourceNames: ["kafka-connector-*"]` wildcard with a label-selector-based approach** — annotate KafkaUser secrets with `role: connector-creds` and grant access via a separate Role that selects by label, or enumerate exact secret names at provisioning time via the Pipeline Operator

### Exit Criteria

- Unauthenticated requests to `POST /v1/query` return 401 from Kong
- A Free-tier tenant cannot use `connector_type: database` (OPA denies)
- After 100 RAG queries, a Free-tier tenant receives 429 quota-exceeded
- Two tenants' queries return results only from their own `{tenant_id}_docs` Milvus collections — cross-tenant data does not appear
- `kubectl auth can-i get secret --as=system:serviceaccount:ai-pipeline:connector-sa` for an unlisted secret name returns `no`
- All pods pass `kubectl get pods -n ai-pipeline` with no `Privileged` or `root` security context violations

### Sessions

| Session | Prompt to Claude | Done when |
|---|---|---|
| **2-A** | "Install Keycloak Operator 24.x. Configure realm ai-pipeline with Organizations. Create OIDC clients for SPA (PKCE) and BFF. Define roles pipeline-admin, pipeline-user. Add JWT claims mapper for org_id, license_type, roles. Seed two orgs." | JWT from Keycloak contains org_id and roles |
| **2-B** | "Install Kong OSS Ingress Controller. Configure routes /v1/* → rag-api and /api/* → BFF with jwt + request-transformer plugins. Write the Lua quota-check Kong plugin (gRPC to Quota Service, 100ms timeout, fail-open)." | Unauthenticated request returns 401 |
| **2-C** | "Write and apply all PostgreSQL migrations (Alembic): license_tiers, tenant_licenses, quota_overrides, usage_history, workspaces, workspace_sources, source_file_status. Seed license_tiers rows for Free/Pro/Enterprise matching the multitenancy doc (Pro=unlimited connectors)." | `\dt` in psql shows all tables; seed data present |
| **2-D** | "Implement the Quota Service (services/quota-service/): Python gRPC server, CheckQuota/RecordUsage/GetUsage RPCs, Redis INCR pattern, PostgreSQL usage_history flush. Write all unit tests from test plan §2.5." | Unit tests pass |
| **2-E** | "Write integration tests for Quota Service using testcontainers (Redis + PostgreSQL). Tests from test plan §3.5: check/allow/deny, dedup, enterprise unlimited, fail-open on Redis error." | Integration tests pass |
| **2-F** | "Deploy Quota Service to kind: Deployment, ServiceAccount, cert-manager Certificate (mTLS), NetworkPolicy. Wire Kong Lua plugin to call it. Verify: 100 RAG queries exhaust Free-tier limit; 101st returns 429." | Quota enforcement live in cluster |
| **2-G** | "Implement OPA Rego policies: GPU access by license tier, connector type allowlist by license tier, collection access scoped to {tenant_id}_docs. Deploy bundle to MinIO opa-bundles bucket. Apply OPA ConfigMap to Kong." | Free-tier tenant blocked from database connector type |
| **2-H** | "Add tenant isolation to RAG API and Embedding Worker: derive Milvus collection from X-Tenant-ID only; include tenant_id in Kafka headers; write per-tenant Milvus collections. Write security tests from test plan §7.4." | Cross-tenant query test returns no foreign results |
| **2-I** | "Deploy OpenMeter. Add CloudEvent publishing to RAG API (pipeline.rag.query) and Embedding Worker (pipeline.embedding.batch). Harden all pod specs: secrets as files, readOnlyRootFilesystem, runAsNonRoot, seccompProfile." | Pod security violations absent; OpenMeter receives events |
| **2-J** | "Fix connector-sa RBAC: remove wildcard resourceNames. Write the per-connector Role/RoleBinding creation logic to be added to the Pipeline Operator in Phase 4. Write security tests from test plan §7.1." | Security tests pass; RBAC audit shows no wildcard rules |

**Estimated Effort:** XL (10 sessions × 45–90 min)

---

## Phase 3 — UI & BFF (Pipeline Management API)

**Goal:** Deliver the React SPA and FastAPI BFF so admins can monitor and configure the pipeline, and authenticated users can manage workspaces and browse data sources.

### Key Tasks

**BFF (Pipeline Management API)**
- Scaffold FastAPI app with dependency-injected auth middleware: validate `Authorization: Bearer` JWT, extract `tenant_id` from `X-Tenant-ID` header (injected by Kong)
- Implement admin endpoints: `GET /api/admin/pipeline/status` (K8s pod status + Kafka consumer lag via AdminClient), `GET|POST|PATCH|DELETE /api/admin/connectors` (K8s ConfigMap CRUD via `kubernetes-asyncio`), `GET|PUT /api/admin/pipeline/config`, `GET|POST /api/admin/tenants` + `PATCH /api/admin/tenants/{id}/license` (Keycloak Admin REST), `GET|POST /api/admin/tenants/{id}/users`, `GET /api/admin/quota` + `PUT /api/admin/quota/{tenant_id}/{metric}` (Quota Service gRPC)
- Implement user endpoints (all scoped by JWT `org_id`): `GET|POST|DELETE /api/workspaces`, `GET /api/sources`, `GET /api/sources/{id}/browse/{path}` (MinIO list_objects / k8s exec ls / Kafka list_topics), `POST|DELETE /api/workspaces/{id}/sources`, `GET /api/workspaces/{id}/files` (Milvus metadata + `source_file_status` table), `POST /api/workspaces/{id}/files/{file_id}/reindex`
- Apply `bff-sa` ServiceAccount and `bff-role` (ConfigMap CRUD, pod read, Deployment/CronJob read+patch, DataConnector CRD CRUD)
- Create cert-manager Certificate for BFF mTLS to Quota Service and Keycloak Admin

**React SPA**
- Bootstrap with Vite + React 18 + TypeScript; install shadcn/ui (Radix + Tailwind), React Admin, TanStack Query, @react-keycloak/web, React Router v6, Zustand
- Implement OIDC PKCE flow: redirect to Keycloak → exchange code → store tokens in memory; silent iframe refresh every 60 s; inject `Authorization` header into all Axios requests
- Implement role-guarded routes: `<RequireRole role="pipeline-admin">` wraps `/admin/*`; `<RequireRole role="pipeline-user">` wraps `/workspace/*`
- Build Admin screens: Dashboard (component status + queue depth + throughput from BFF), Connectors (CRUD table), Pipeline tuning (chunk size, embedding backend, Milvus index params), Tenants & users (license tier, quota usage progress bars, user invite), Quota management (per-tenant table, inline override editing)
- Build User screens: Workspaces (card grid, create/delete), Data sources (tree browser: S3 buckets / NFS folders / DB tables / Kafka topics), File browser (paginated table: name, type, size, modified, chunk count, ingest status badge)
- Build nginx Deployment serving the Vite production build; configure `/api/*` proxy to BFF

**K8s Deployment**
- `pipeline-ui` Deployment (1 replica): nginx:alpine container serving static build
- `pipeline-mgmt-api` Deployment (1 replica): BFF with env vars for Keycloak URL, Milvus host, Quota Service address, DB URL
- Add `ServiceMonitor` for BFF metrics endpoint; add Kong Route for `/api/*`

### Exit Criteria

- Visiting the cluster URL redirects to Keycloak login; after login, admin sees the Dashboard screen with live pod statuses
- Admin can create a connector via the Connectors screen and the `DataConnector` CR appears in K8s
- User can browse S3 buckets in the Data sources screen and see file listings
- User can view the File browser for a workspace and see `indexed` status on ingested files
- A user without `pipeline-admin` role cannot access `/admin/*` routes (UI hides, BFF returns 403)

### Sessions

| Session | Prompt to Claude | Done when |
|---|---|---|
| **3-A** | "Scaffold the BFF (services/bff/): FastAPI app, JWT auth middleware (validate Bearer + extract X-Tenant-ID), error envelope middleware. Write unit tests for auth middleware and tenant scoping." | Unit tests pass |
| **3-B** | "Implement BFF admin endpoints: pipeline status, connector CRUD (K8s ConfigMap + DataConnector CR), pipeline config. Write unit tests with mocked kubernetes-asyncio client." | Unit tests pass |
| **3-C** | "Implement BFF tenant + quota admin endpoints (Keycloak Admin REST + Quota Service gRPC). Implement user workspace + sources endpoints. Write unit tests for tenant scoping (test_bff_user_workspace_scoped_to_tenant)." | Unit tests pass |
| **3-D** | "Deploy BFF to kind: Dockerfile, Deployment, bff-sa ServiceAccount, bff-role, cert-manager cert, Kong route /api/*. Smoke test: admin JWT gets pipeline status; user JWT gets workspace list." | BFF live; smoke test passes |
| **3-E** | "Bootstrap the React SPA (frontend/): Vite + React 18 + TypeScript + shadcn/ui + TanStack Query + @react-keycloak/web + React Router + Zustand. Implement OIDC PKCE flow + RequireRole guards + Axios interceptor. Serve from nginx on kind." | Login flow works; wrong-role route returns to login |
| **3-F** | "Build Admin screens: Dashboard, Connectors CRUD table, Pipeline tuning form, Tenants & users (license + quota bars + invite), Quota management table." | All 5 admin screens render with live data |
| **3-G** | "Build User screens: Workspaces card grid, Data sources tree browser, File browser paginated table with status badges. Write unit tests for auth rejection (§7.1 test table)." | All 3 user screens render; auth tests pass |

**Estimated Effort:** L (7 sessions × 45–75 min)

---

## Phase 4 — Self-Service Wizard & User Sources (with Bug Fixes)

**Goal:** Allow authenticated users to add their own data sources through a 4-step wizard without admin involvement, with all known review issues corrected.

### Key Tasks

**API Endpoints**
- Implement `POST /api/sources/create`: validate request → `CheckQuota(CONNECTOR_COUNT)` → `kubectl apply DataConnector CR` → optional workspace attachment → return `201 { status: provisioning }`
- Implement `POST /api/sources/test`: test connection only, no CR created; **fix SSRF risk: add server-side allowlist validator that blocks RFC-1918 and loopback ranges (10.x, 172.16-31.x, 192.168.x, 127.x, ::1) for all database connection strings and HTTP endpoints before attempting connection; return 400 for blocked addresses**
- Implement `POST /api/sources/upload`: multipart upload → stream directly to MinIO at `{tenant_id}/uploads/{session_id}/`; **fix: upload path must skip `CheckQuota(CONNECTOR_COUNT)` since no DataConnector CR is created**; **fix: after successful upload, publish `DataSource` metadata event to `metadata-events` Kafka topic** (entity_type: DataSource, source_type: upload, path: `{tenant_id}/uploads/{session_id}/`)
- Implement `POST|POST|DELETE /api/sources/{id}/pause|resume|{id}` for lifecycle management

**Wizard Frontend**
- Step 1 — Choose type: S3, NFS, Database, Kafka stream, File upload (5 cards)
- Step 2 — Configure connection: dynamic form per type; credentials stored as K8s Secret `connector-{slug}-creds` via BFF; File upload type shows drag-and-drop zone, bypasses this step
- Step 3 — Test & preview: call `POST /api/sources/test` (15 s timeout); show connection latency + first 10 file previews; Step 4 blocked until test passes; upload type skips this step
- Step 4 — Name & settings: source name, sync frequency, file type filter, max file size, optional workspace attachment; **fix: add `start_paused` boolean field to `UserSourceCreate` schema (default: false); when true, DataConnector CR is created with `spec.paused: true` so Pipeline Operator creates the Deployment/CronJob in a paused/suspended state**; **fix: align Pro tier connector quota to the multitenancy doc value (unlimited connectors for Pro) — remove the contradictory `maxConnectors: 4` cap from the UI validator and from the `CheckQuota` call for Pro license tokens**
- Success screen: live ingestion progress bar polling `GET /api/workspaces/{id}/files` every 5 s

**NFS Path Traversal Fix**
- In `browse_source` for NFS connector type: **validate `path` parameter against `connector.allowed_path_prefix` before issuing `kubectl exec ls`; reject with 400 if `path` does not start with the configured prefix or contains `..` sequences; log the rejection**

**TenantWorkspace Operator Fix**
- **Fix: add upload-watcher CronJob reconciliation to the `TenantWorkspace` operator handler**: on `TenantWorkspace` create/update, apply a CronJob named `upload-watcher-{tenant_id}` (runs every 30 s) that polls `{tenant_id}/uploads/` in MinIO and publishes new files to `raw-documents` Kafka topic; CronJob must be deleted on `TenantWorkspace` delete

**connector-sa RBAC Fix**
- **Fix: replace `resourceNames: ["kafka-connector-*"]` wildcard (which K8s does not support for secrets)**: Pipeline Operator `reconcile_connector` must create a dedicated `Role` for each tenant with the exact secret names listed (e.g., `resourceNames: ["connector-acme-s3-creds", "connector-acme-s3-tls"]`); bind this Role to `connector-sa` via a per-connector `RoleBinding`; operator must delete the Role/RoleBinding on DataConnector delete

### Exit Criteria

- Free-tier user can add ≤2 connectors; third attempt is rejected with quota-exceeded message in the wizard
- Pro-tier user can add more than 4 connectors (unlimited cap verified)
- `POST /api/sources/test` with a connection string targeting `192.168.1.1` returns `400 SSRF_BLOCKED`
- NFS browse with path `../../etc/passwd` returns 400; valid subpath returns file listing
- File upload places files in MinIO; within 60 s the upload-watcher CronJob publishes to `raw-documents` and files appear as `indexed` in the File browser
- Upload flow does not call `CheckQuota(CONNECTOR_COUNT)` (verified via BFF trace/log)
- Upload flow publishes a `DataSource` metadata event to `metadata-events` topic (verified via Kafka consumer)
- A source created with "Start ingestion immediately" unchecked is created in `paused` state

### Sessions

| Session | Prompt to Claude | Done when |
|---|---|---|
| **4-A** | "Implement the Pipeline Operator (services/pipeline-operator/) using kopf: scaffold, CRD apply on startup, DataConnector reconcile (Deployment vs CronJob, KafkaTopic, KafkaUser). Fix: create per-connector Role with exact secret names + RoleBinding; delete them on CR delete. Write unit tests from test plan §2.7." | Unit tests pass |
| **4-B** | "Add TenantWorkspace and EmbeddingConfig reconcile handlers to the Pipeline Operator. Fix: TenantWorkspace must create upload-watcher-{tenant_id} CronJob and delete it on workspace deletion. Fix: EmbeddingConfig dimension-change guard (BlockedDimensionChange status unless reindexConfirmed=true)." | Operator reconcile tests pass |
| **4-C** | "Deploy Pipeline Operator to kind: Dockerfile, Deployment, pipeline-operator-sa, pipeline-operator ClusterRole. Apply CRDs from docs/api/pipeline-operator-crds.yaml. Smoke test: create a DataConnector CR → Deployment + KafkaTopic + KafkaUser appear; delete CR → resources deleted." | Operator live; smoke test passes |
| **4-D** | "Implement BFF user-source endpoints: POST /sources/create (quota check → CR apply → workspace attach), POST /sources/test (with SSRF allowlist blocking RFC-1918/loopback), POST /sources/upload (skip quota, publish DataSource metadata event), pause/resume/delete. Write security unit tests: SSRF (§7.2 full table), NFS path traversal (§7.3 full table), ownership (§7.5)." | All security unit tests pass |
| **4-E** | "Fix browse_source NFS path: validate path against connector.allowed_path_prefix; reject with 400 on traversal or out-of-prefix path. Fix Pro tier connector quota: remove the 4-connector cap for Pro license tokens (Pro=unlimited per multitenancy doc). Add start_paused field to UserSourceCreate schema." | SSRF + traversal + quota tests pass |
| **4-F** | "Build the 4-step Add Data Source wizard in the React SPA: type selector, dynamic credential form, test-and-preview step (call /sources/test, block Step 4 until pass), name/settings step with start_paused toggle. Wire to POST /sources/create." | Wizard completes and connector CR appears in K8s |
| **4-G** | "Write E2E tests for the self-service wizard from test plan §4.3: user creates S3 connector → ingests files; file upload → upload-watcher picks up → indexed; connector deletion removes CR and Secret; Free/Pro quota enforcement." | E2E tests pass |

**Estimated Effort:** L (7 sessions × 45–90 min)

---

## Phase 5 — Metadata & Lineage Service

**Goal:** Capture full provenance for every entity produced by the pipeline and expose lineage traversal APIs so admins and users can trace any RAG result back to its source.

### Key Tasks

**Database Schema**
- Apply migrations for the `metadata` schema in the existing `quota-db` CloudNativePG cluster: `schema_versions`, `pipeline_runs`, `entities`, `lineage`, `processing_steps`, `data_quality`, `query_results` tables with all indexes from the design doc
- Add GIN index on `metadata.entities.attributes` for JSON attribute search

**Pipeline Stage Instrumentation**
- S3/NFS Connector: publish `metadata.entity.created` CloudEvents to `metadata-events` on file discovery (entity_type: `DataSource` + `RawDocument`, with `discovered_in` lineage edge)
- Document Processor: publish `metadata.entity.created` for each `DocumentChunk` with `chunked_into` edge; include `quality_checks` array (`not_empty`, `min_token_count`, `not_duplicate`)
- Embedding Worker: publish `metadata.entity.created` for each `Embedding` with `embedded_by` + `stored_in` edges; include `embedding_norm` quality check; create `SchemaVersion` record when embedding config changes
- RAG API: publish `metadata.entity.created` for each `RAGQuery` with `retrieved_by` edges; populate `query_results` rows

**Metadata Service**
- Scaffold FastAPI service with a background Kafka consumer (`metadata-events` topic, consumer group `metadata-service`, consume-only KafkaUser)
- Consumer: upsert entities via `INSERT ... ON CONFLICT DO UPDATE`; insert lineage edges; insert `processing_steps`; insert `data_quality` rows; mark entity `quality_status: failed` and publish `DataQualityFailed` Kafka event when a check fails
- Implement REST API endpoints: `GET /api/lineage/upstream/{chunk_id}` (recursive CTE upstream query), `GET /api/lineage/downstream/{source_path}` (impact set query), `GET /api/lineage/stale/{tenant_id}` (embeddings with outdated schema version), `GET /api/lineage/provenance/{query_id}` (full provenance query), `GET /api/runs` (pipeline run history), `GET /api/quality/{tenant_id}` (failed/warned quality checks)
- Implement `SchemaVersion` reconciliation hook: Pipeline Operator calls Metadata Service to create a new `SchemaVersion` record before triggering EmbeddingConfig rolling restart; deactivate previous versions
- Deploy as `Deployment` (1 replica); create `KafkaTopic` CR for `metadata-events` on startup; apply NetworkPolicy; add `ServiceMonitor`

**Pipeline Operator Integration**
- Update `reconcile_embedding` handler to call Metadata Service API to record new `SchemaVersion` and deactivate previous before rolling restart

**UI Integration**
- Add lineage panel to File browser: for a selected file, call `GET /api/lineage/downstream/{source_path}` and show count of derived chunks and embeddings
- Add "Data Quality" tab to the admin Dashboard showing failed/warned checks from `GET /api/quality/{tenant_id}`, linking to the offending entity
- Wire `DataQualityFailed` Kafka events to a Prometheus counter (`data_quality_failures_total{check_name, tenant_id}`) via a Kafka exporter sidecar; add `PrometheusRule` alert for `data_quality_failures_total > 10` over 5 m

**Optional: OpenMetadata**
- If enterprise catalog is required: deploy OpenMetadata Helm chart in `metadata` namespace; implement push connector in Metadata Service that sends entity/lineage records to OpenMetadata API after each pipeline run

### Exit Criteria

- After ingesting a PDF, `GET /api/lineage/upstream/{chunk_id}` returns a chain: `DocumentChunk → RawDocument → DataSource` with correct `source_path`
- After updating the embedding model (EmbeddingConfig spec change), `GET /api/lineage/stale/{tenant_id}` returns all embeddings generated with the prior model
- A corrupt PDF triggers `data_quality.status = 'failed'` for `parse_success` check; this appears on the admin Dashboard quality tab
- RAG query provenance endpoint returns `rank`, `score`, `source_file`, `embedding_model`, and `indexed_at` for each retrieved chunk
- Metadata Service consumer lag stays ≤ 100 messages at steady ingestion rate

### Sessions

| Session | Prompt to Claude | Done when |
|---|---|---|
| **5-A** | "Write and apply Alembic migrations for the full metadata schema (metadata.schema_versions, pipeline_runs, entities, lineage, processing_steps, data_quality, query_results) with all indexes from docs/metadata-lineage.md §3." | `\dt metadata.*` shows all 7 tables |
| **5-B** | "Add metadata.entity.created CloudEvent publishing to the S3 and NFS connectors: DataSource entity on startup, RawDocument entity on file discovery with discovered_in lineage edge. Write unit tests." | Unit tests pass |
| **5-C** | "Add metadata event publishing to the Document Processor (DocumentChunk + chunked_into edge + quality checks) and Embedding Worker (Embedding + embedded_by/stored_in edges + embedding_norm check). Write unit tests." | Unit tests pass |
| **5-D** | "Add RAGQuery metadata event publishing to the RAG API. Scaffold the Metadata Service (services/metadata-service/): FastAPI + Kafka consumer (metadata-events), entity upsert, lineage edge insert, quality check insert, DataQualityFailed event on failure. Write unit tests." | Unit tests pass |
| **5-E** | "Implement Metadata Service REST endpoints: upstream, downstream, stale, provenance, runs, quality. Write integration tests from test plan §9 using testcontainers (Kafka + PostgreSQL)." | Integration tests pass |
| **5-F** | "Deploy Metadata Service to kind. Add SchemaVersion reconciliation hook to Pipeline Operator EmbeddingConfig handler. Add lineage panel to File browser UI and Data Quality tab to Dashboard. Write E2E lineage test (test_e2e_pdf_lineage_traces_to_datasource)." | Lineage chain visible in UI; E2E test passes |

**Estimated Effort:** L (6 sessions × 45–75 min)

---

## Phase 6 — Observability, Hardening & Production Readiness

**Goal:** Harden all components for production workloads: complete metrics coverage, alerting, log aggregation, chaos validation, performance tuning, and the upgrade/rollback path via the Pipeline Operator.

### Key Tasks

**Observability Completeness**
- Add structured JSON logging (`structlog`) to every service; include `tenant_id`, `trace_id`, `span_id`, `service`, `level`, `message` fields on every log line
- Deploy Loki in `monitoring`; configure Promtail DaemonSet to ship pod logs to Loki; configure Grafana Loki datasource
- Add Kong access log shipping to Loki (include `sub`, `X-Tenant-ID`, `quota_checked`, `cached` fields)
- Enable PostgreSQL `pgaudit` log shipping to Loki; configure 90-day Loki retention and 1-year MinIO cold-storage lifecycle policy
- Build Grafana dashboards: Pipeline Overview (throughput, lag, error rate), RAG Performance (latency p50/p95/p99, cache hit rate), Quota Usage (per tenant usage vs. limit gauges), Data Quality (failed checks heatmap), Lineage Coverage (entities per tenant over time)
- Add `TenantNearQuota` PrometheusRule alert (`quota_usage_ratio > 0.8`, 1 m)
- Add Quota Service Prometheus metrics: `quota_checks_total`, `quota_exceeded_total{tenant_id, metric}`, `quota_grpc_duration_seconds`
- Complete OTel trace coverage: add spans for Milvus search, Redis cache, Kafka produce/consume, gRPC Quota calls; verify Tempo receives traces

**Pipeline Operator — Upgrade Sequence**
- Implement the full coordinated upgrade state machine in `PipelineCluster` reconciler: `UpgradeInProgress` condition → pause connector CronJobs → poll consumer lag = 0 (timeout 10 m) → rolling restart doc-processor → rolling restart embedding-worker → rolling restart RAG API + smoke test (`GET /v1/health`) → resume connectors → clear condition
- Implement rollback: detect `spec.version` rollback → replay upgrade sequence in reverse order
- Implement the dimension-change guard in `EmbeddingConfig` reconciler: block update and set `BlockedDimensionChange` status unless `spec.reindexConfirmed: true`
- Automate `ServiceMonitor` and `PrometheusRule` creation for each `DataConnector` CR created by the operator

**Gatekeeper Production Hardening**
- Flip Gatekeeper `failurePolicy` from `Ignore` to `Fail` in the production cluster overlay
- Enable `K8sBlockLatestImageTag` constraint enforcement in production
- Add `K8sBlockCrossNamespaceSecret` constraint scoped to connector pods

**Performance Tuning**
- Tune embedding worker batch size and concurrency for target throughput; benchmark CPU embedding latency per batch size (8/16/32/64 chunks)
- Switch Milvus index from `IVF_FLAT` to `HNSW (M=16, ef_construction=200)` once corpus exceeds 100 K vectors; validate recall vs. QPS tradeoff
- Configure Redis query cache TTL tuning based on observed hit rate (target > 40% for repeat workloads)
- Tune Kafka consumer `max.poll.interval.ms` and `session.timeout.ms` for large-PDF processing latency
- Load test `POST /v1/query` at 100 QPS; verify p99 < 500 ms with HNSW index

**HA & Resilience**
- Scale Kafka to 3 brokers; set `replication.factor: 3`, `min.insync.replicas: 2` on all topics
- Scale CloudNativePG to 2 instances (primary + read replica) with automatic failover
- Scale Milvus to cluster mode (`MilvusCluster` CR): 2 queryNodes, 2 dataNodes, 1 indexNode
- Configure Redis Sentinel for HA
- Configure PodDisruptionBudgets for doc-processor, embedding-worker, RAG API (min available: 1)
- Run chaos tests: kill a Kafka broker → verify no message loss; kill the primary PostgreSQL pod → verify CNPG auto-promotes replica within 30 s; kill an embedding-worker pod → verify Kafka rebalance completes and processing resumes

**CI/CD & GitOps Finalization**
- Add GitHub Actions / CI pipeline: lint (ruff, mypy), unit tests, Docker build + push with commit SHA tag (no `latest` in production)
- Add ArgoCD Image Updater for automated image promotion from staging to production
- Document rollback procedure: `git revert` the image tag commit → ArgoCD syncs → Pipeline Operator runs coordinated downgrade
- Enable ArgoCD ApplicationSets for per-environment overlays (kind/testbed, staging, production)

**Security Final Pass**
- Run `kube-bench` (CIS K8s Benchmark) against the cluster; remediate all Level 1 findings
- Run `trivy image` scan on all custom images in CI; gate on CRITICAL CVEs
- Verify External Secrets Operator (ESO) is configured for production secrets store (AWS Secrets Manager or Vault); remove Sealed Secrets from production overlay
- Review and document certificate rotation runbook (cert-manager auto-renews; verify renewal alerts fire at 15 days before expiry)
- Perform final RBAC audit: `kubectl auth can-i --list --as=system:serviceaccount:ai-pipeline:{sa}` for each ServiceAccount; document in ops runbook

### Exit Criteria

- All services emit structured JSON logs; logs are queryable in Grafana/Loki with `tenant_id` filter
- Full Grafana dashboard suite is populated with real data; all PrometheusRule alerts fire correctly in a test scenario
- Upgrade sequence: bumping `PipelineCluster.spec.version` in Git triggers ArgoCD sync, Pipeline Operator completes the coordinated upgrade with no Kafka message loss and no RAG API downtime > 5 s
- Rollback sequence: `git revert` restores previous version; Pipeline Operator completes rollback within 5 m
- Chaos test results: no data loss during Kafka broker failure; CNPG failover completes within 30 s
- Load test: 100 QPS sustained for 5 m; p99 < 500 ms; error rate < 0.1%
- `kube-bench` Level 1 pass rate ≥ 95%; no CRITICAL CVEs in any image
- Gatekeeper `failurePolicy: Fail` active; test that a non-compliant pod is rejected at admission

### Sessions

| Session | Prompt to Claude | Done when |
|---|---|---|
| **6-A** | "Add structlog JSON logging to all 6 services (connector, processor, embedder, rag-api, quota-service, bff). Include tenant_id, trace_id, span_id on every log line. Deploy Loki + Promtail DaemonSet. Configure Grafana Loki datasource. Ship Kong access logs and pgaudit logs to Loki." | Logs queryable in Grafana by tenant_id |
| **6-B** | "Build 5 Grafana dashboards: Pipeline Overview, RAG Performance (p50/p95/p99), Quota Usage (per-tenant gauges), Data Quality (heatmap), Lineage Coverage. Add TenantNearQuota PrometheusRule. Add Quota Service Prometheus metrics." | All 5 dashboards show live data |
| **6-C** | "Complete OTel trace coverage: add spans for Milvus search, Redis cache, Kafka produce/consume, gRPC quota calls. Verify traces appear in Tempo. Add Instrumentation CR annotation to all pods." | Traces visible in Grafana/Tempo |
| **6-D** | "Implement Pipeline Operator upgrade state machine in PipelineCluster reconciler: UpgradeInProgress condition → pause connectors → drain Kafka lag → roll doc-processor → roll embedder → roll RAG API + smoke test → resume connectors → clear condition. Implement rollback. Write E2E upgrade test (test_e2e_coordinated_upgrade)." | E2E upgrade test passes |
| **6-E** | "Set up GitHub Actions CI: lint (ruff + mypy), pytest unit+integration, Docker build + push with commit SHA tag, trivy CRITICAL CVE gate. Add ArgoCD Image Updater for staging→production promotion." | CI pipeline green; `:latest` tag rejected in prod |
| **6-F** | "Scale for HA: Kafka 3 brokers (replication.factor:3), CNPG 2 instances, Milvus cluster mode, Redis Sentinel. Add PodDisruptionBudgets. Run chaos tests from test plan §8: Kafka broker failure, PostgreSQL failover, Milvus unavailability, Redis loss." | All 7 chaos scenarios pass |
| **6-G** | "Run performance tests from test plan §6: locust 100 QPS RAG latency, ingestion throughput 100 docs/min, Quota Service p99 <5ms. Run kube-bench and kubescape. Flip Gatekeeper to failurePolicy:Fail. Install External Secrets Operator for production overlay. Final RBAC audit." | Load test targets met; kube-bench Level 1 ≥95% |

**Estimated Effort:** XL (7 sessions × 60–120 min)

---

## Summary Table

| Phase | Sessions | Total sessions | Effort |
|---|---|---|---|
| 0 | 0-A … 0-E | 5 | L |
| 1 | 1-A … 1-J | 10 | XL |
| 2 | 2-A … 2-J | 10 | XL |
| 3 | 3-A … 3-G | 7 | L |
| 4 | 4-A … 4-G | 7 | L |
| 5 | 5-A … 5-F | 6 | L |
| 6 | 6-A … 6-G | 7 | XL |
| **Total** | | **52 sessions** | |

**Effort tiers:** S = 1–2 days · M = 3–5 days · L = 1–2 weeks · XL = 3–5 weeks  
**Session budget:** 52 sessions × ~1 hour average = ~52 hours of Claude Pro time  
**Pacing recommendation:** 2–3 sessions per day maximum to stay within Claude Pro rolling limits; start each day with a fresh conversation.

---

## Known-Issue Fix Traceability

| Issue | Fixed In | Where |
|---|---|---|
| connector-sa RBAC: wildcards don't work in K8s `resourceNames` | Phase 2 + Phase 4 | Phase 2 security hardening; Phase 4 DataConnector reconciler creates per-connector Role with exact names |
| Upload-watcher CronJob missing from TenantWorkspace operator reconcile | Phase 4 | TenantWorkspace operator fix task |
| SSRF risk in `/api/sources/test` | Phase 4 | RFC-1918 allowlist validator in BFF |
| NFS path traversal in `browse_source` | Phase 4 | `allowed_path_prefix` validation before `kubectl exec` |
| Pro tier connector quota contradicts multitenancy doc | Phase 4 | Remove cap from UI validator and `CheckQuota` call for Pro tokens |
| Upload type incorrectly calls `CheckQuota(CONNECTOR_COUNT)` | Phase 4 | Upload branch skips quota check |
| `start_paused` boolean missing from `UserSourceCreate` schema | Phase 4 | Schema field addition + operator support |
| Upload path does not publish to `metadata-events` | Phase 4 | Upload handler publishes DataSource metadata event after MinIO write |
