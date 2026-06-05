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
| File browser | User | Paginated table: name, type, size, modified, chunk count, ingest status |
