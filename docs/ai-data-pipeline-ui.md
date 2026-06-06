# AI Data Pipeline — UI & Pipeline Management API

**Extends:** `ai-data-pipeline-multitenancy.md`  
**New components:** React SPA · Pipeline Management API (BFF) · nginx

---

## 1. Component Stack (all open source)

| Layer | Technology | Role |
|---|---|---|
| Frontend SPA | **React 18 + Vite + TypeScript** | Single-page app; served from nginx pod |
| UI components | **shadcn/ui** (Radix + Tailwind) | Accessible, themeable component library |
| Admin CRUD | **React Admin** | Wires REST resources to tables/forms |
| Data fetching | **TanStack Query** | Cache, background refresh, optimistic updates |
| Auth (client) | **@react-keycloak/web** | OIDC silent refresh, token injection into Axios |
| Routing | **React Router v6** | Role-guarded routes (admin vs user) |
| State | **Zustand** | Lightweight global state |
| Backend (BFF) | **FastAPI + Python** | Aggregates Keycloak Admin, Milvus, Kafka Admin, MinIO, Quota Service |
| Reverse proxy | **nginx** (K8s Deployment) | Serves static build; proxies `/api/*` to BFF via Kong |

---

## 2. Auth Flow (OIDC + PKCE)

```
1. User visits https://pipeline.acme.com
2. React app → redirects to Keycloak /authorize (PKCE)
3. Keycloak shows login page
4. Keycloak redirects back with auth code → exchange for access + refresh token
5. Tokens stored in memory (NOT localStorage)
6. Every Axios request injects Authorization: Bearer <access_token>
7. Kong validates token; extracts org_id, roles; adds X-Tenant-ID header
8. Silent iframe refresh every 60 s
```

**Role-based routing:**

```tsx
<Route path="/admin/*" element={
  <RequireRole role="pipeline-admin"><AdminLayout /></RequireRole>
} />
<Route path="/workspace/*" element={
  <RequireRole role="pipeline-user"><UserLayout /></RequireRole>
} />
```

---

## 3. Pipeline Management API (BFF)

### Admin endpoints (require `pipeline-admin` role)

```
GET    /api/admin/pipeline/status
GET    /api/admin/connectors
POST   /api/admin/connectors
PATCH  /api/admin/connectors/{id}
DELETE /api/admin/connectors/{id}

GET    /api/admin/pipeline/config
PUT    /api/admin/pipeline/config

GET    /api/admin/tenants
POST   /api/admin/tenants
PATCH  /api/admin/tenants/{id}/license
GET    /api/admin/tenants/{id}/users
POST   /api/admin/tenants/{id}/users

GET    /api/admin/quota
PUT    /api/admin/quota/{tenant_id}/{metric}
```

### User endpoints (scoped to caller's tenant)

```
GET    /api/workspaces
POST   /api/workspaces
DELETE /api/workspaces/{id}

GET    /api/sources
GET    /api/sources/{id}/browse/{path}

POST   /api/workspaces/{id}/sources
DELETE /api/workspaces/{id}/sources/{src}

GET    /api/workspaces/{id}/files
POST   /api/workspaces/{id}/files/{file_id}/reindex
```

### BFF backend calls

| Endpoint | Calls |
|---|---|
| `/api/admin/pipeline/status` | Kubernetes API (pod status) + Kafka AdminClient (consumer lag) |
| `/api/admin/connectors` | K8s ConfigMap CRUD via `kubernetes-asyncio` |
| `/api/admin/pipeline/config` | K8s ConfigMap read/write |
| `/api/admin/tenants` | Keycloak Admin REST API |
| `/api/admin/quota` | Quota Service gRPC |
| `/api/sources/{id}/browse` | MinIO `list_objects()` / K8s exec ls / Kafka `list_topics()` |
| `/api/workspaces/{id}/files` | Milvus metadata + `source_file_status` PostgreSQL table |

---

## 4. Workspace & Source Data Model

```sql
CREATE TABLE workspaces (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID NOT NULL,
    owner_id    UUID NOT NULL,
    name        TEXT NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE workspace_sources (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID REFERENCES workspaces,
    connector_id TEXT NOT NULL,
    path_prefix  TEXT NOT NULL,
    added_at     TIMESTAMPTZ DEFAULT now(),
    UNIQUE(workspace_id, connector_id, path_prefix)
);

CREATE TABLE source_file_status (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID NOT NULL,
    connector_id    TEXT NOT NULL,
    file_path       TEXT NOT NULL,
    file_size_bytes BIGINT,
    content_type    TEXT,
    last_modified   TIMESTAMPTZ,
    ingest_status   TEXT DEFAULT 'pending',  -- pending|indexing|indexed|error
    error_message   TEXT,
    chunk_count     INTEGER,
    indexed_at      TIMESTAMPTZ,
    UNIQUE(tenant_id, connector_id, file_path)
);
```

---

## 5. Source Browser Implementation

```python
@router.get("/sources/{connector_id}/browse/{path:path}")
async def browse_source(connector_id: str, path: str, tenant_id: str = Header(...)):
    connector = await get_connector_config(connector_id, tenant_id)
    match connector.source_type:
        case "s3":
            objects = await minio_client.list_objects(connector.bucket, prefix=path, delimiter="/")
            return [BrowseEntry(name=o.object_name, type="folder" if o.is_dir else "file",
                                size=o.size, modified=o.last_modified) for o in objects]
        case "nfs":
            return await k8s_exec_ls(connector.pod_name, path)
        case "database":
            tables = await pg_list_tables(connector.connection_string, schema=path or "public")
            return [BrowseEntry(name=t, type="table") for t in tables]
        case "stream":
            topics = await kafka_admin.list_topics()
            return [BrowseEntry(name=t, type="topic") for t in topics
                    if t.startswith(connector.topic_prefix)]
```

---

## 6. K8s Deployment

```yaml
# nginx + React SPA
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pipeline-ui
  namespace: ai-pipeline
spec:
  replicas: 1
  template:
    spec:
      containers:
        - name: nginx
          image: pipeline-ui:latest   # npm run build → nginx:alpine
          ports: [{containerPort: 80}]
---
# Pipeline Management API (BFF)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pipeline-mgmt-api
  namespace: ai-pipeline
spec:
  replicas: 1
  template:
    spec:
      serviceAccountName: pipeline-mgmt-sa
      containers:
        - name: api
          image: pipeline-mgmt-api:latest
          env:
            - {name: KEYCLOAK_URL, value: "http://keycloak.infrastructure.svc:8080"}
            - {name: MILVUS_HOST,  value: "milvus.infrastructure.svc"}
            - {name: QUOTA_SERVICE_ADDR, value: "quota-service.ai-pipeline.svc:50051"}
```

---

## 7. Trade-offs

| Decision | Choice | Rationale | Downside |
|---|---|---|---|
| Frontend framework | React + Vite | Largest ecosystem | Heavy JS bundle |
| Admin UI | React Admin | 80% of boilerplate eliminated | Opinionated; hard to customise beyond its model |
| BFF pattern | Dedicated FastAPI BFF | Single typed API; enforces tenant scoping | Extra service; single point of failure for the UI |
| File metadata | PostgreSQL table | Queryable, filterable | Connector pods need DB write access |
| NFS browsing | `kubectl exec` into connector pod | Reuses existing NFS mount | Fragile if pod restarts |
| Token storage | In-memory only | Mitigates XSS token theft | Lost on page refresh; handled by silent OIDC refresh |

---

## 8. Screen Inventory

| Screen | Role | Description |
|---|---|---|
| Dashboard | Admin | Component status, queue depth, throughput metrics |
| Connectors | Admin | CRUD source connectors; last-run time, doc counts, errors |
| Pipeline tuning | Admin | Chunk size/overlap, embedding backend, Milvus index params |
| Tenants & users | Admin | License tier, quota usage bars, invite users |
| Quota management | Admin | Per-tenant usage table; inline override editing |
| Workspaces | User | Grid of workspace cards; create/delete |
| Data sources | User | Tree: S3 buckets / NFS folders / DB tables / Kafka topics |
| **Add data source wizard** | **User** | **4-step self-service wizard (see §9)** |
| File browser | User | Paginated table: name, type, size, modified, chunk count, ingest status |

---

## 9. Add Data Source Wizard

Users add their own sources without admin involvement, subject to connector quota. Triggered from the Data sources screen via "Add source".

### Step 1 — Choose type

| Type | When to use |
|---|---|
| Cloud storage (S3) | S3-compatible buckets (AWS S3, MinIO, GCS via S3 compat) |
| NFS / File server | Directories on the NFS mount already provisioned by admin |
| Database | PostgreSQL/MySQL tables with a text or document column |
| Kafka stream | Live topics — messages treated as document events |
| File upload | Browser-direct upload of PDF/DOCX/TXT/CSV up to 100 MB each |

### Step 2 — Configure connection

Dynamic form per source type. Credentials entered inline are stored as a K8s Secret named `connector-{slug}-creds`; the raw values are never returned by the API (`writeOnly: true` in schema).

**File upload path:** no form fields — user gets a drag-and-drop zone. Files go directly to MinIO under `{tenant_id}/uploads/{session_id}/`. No `DataConnector` CR is created; an upload-watcher CronJob (provisioned by TenantWorkspace operator) picks them up within 30 s.

### Step 3 — Test & preview

Calls `POST /api/sources/test` (15 s timeout). Returns connection latency and a preview of the first 10 files. Step 4 is blocked until test passes (upload type skips this step).

### Step 4 — Name & settings

- Source name, sync frequency, file type filter, max file size
- Optional: attach immediately to an existing workspace
- "Start ingestion immediately" checkbox (default on)

### Submit flow

```
POST /api/sources/create
  → BFF: QuotaService.CheckQuota(CONNECTOR_COUNT)
  → BFF: kubectl apply DataConnector CR
  → Pipeline Operator reconciles → Deployment/CronJob + KafkaTopic + KafkaUser
  → If workspace_id set: POST /api/workspaces/{id}/sources
  → 201 UserSource { status: provisioning }
```

Success screen shows a live ingestion progress bar polling every 5 s.

### Connector quota

| License | Max connectors |
|---|---|
| Free | 2 |
| Pro | 4 |
| Enterprise | Unlimited |

### New API endpoints

```
POST   /api/sources/create          create connector (all types except upload)
POST   /api/sources/test            test connection without creating anything
POST   /api/sources/upload          multipart file upload
POST   /api/sources/{id}/pause      pause sync
POST   /api/sources/{id}/resume     resume sync
DELETE /api/sources/{id}            delete (does not remove indexed vectors)
```
