# AI Data Pipeline — Multi-Tenancy, User Management & Quota Licensing

**Extends:** `ai-data-pipeline-design.md`  
**New components:** Keycloak · Kong OSS · OPA · Quota Service · OpenMeter · CloudNativePG

---

## 1. Open Source Component Selection

| Concern | Component | Why |
|---|---|---|
| Identity & auth | **Keycloak 24+** | OIDC/OAuth2, Organizations (multi-tenancy), user federation, RBAC |
| API gateway | **Kong OSS** (+ Kong Ingress Controller) | JWT, rate-limiting, request-transformer plugins; K8s-native |
| Policy engine | **OPA (Open Policy Agent)** | Decoupled Rego policies; integrates with Kong |
| Quota enforcement | **Custom Python service** | Thin gRPC service backed by Redis + PostgreSQL |
| Usage metering | **OpenMeter** | Kafka-native event ingestion; aggregation windows; REST API |
| Quota/license store | **PostgreSQL** via **CloudNativePG** | Relational model for license tiers + quota limits |
| Real-time counters | **Redis** (existing) | INCR + EXPIRE pattern |

---

## 2. Tenant & User Data Model

### Hierarchy

```
License Tier (Free / Pro / Enterprise)
    └── Organization (Tenant)
            ├── quota limits (derived from license tier, overridable)
            └── Users
                    └── roles: admin | developer | viewer
```

### JWT claims (issued by Keycloak)

```json
{
  "sub": "user-uuid",
  "email": "alice@acme.com",
  "org_id": "tenant-uuid",
  "org_name": "acme",
  "license_type": "pro",
  "quota_tier": "pro",
  "roles": ["developer"]
}
```

---

## 3. License Tiers & Quota Definitions

| Limit | Free | Pro | Enterprise |
|---|---|---|---|
| Data ingested / month | 1 GB | 100 GB | Unlimited |
| Vectors stored (total) | 100 K | 10 M | Unlimited |
| RAG queries / day | 100 | 10 000 | Unlimited |
| RAG queries / minute | 5 | 100 | Custom |
| GPU embedding | No | Yes | Yes |
| Concurrent ingestion workers | 1 | 4 | 16 |
| Source connectors | 2 | All | All |
| Users per tenant | 3 | 25 | Unlimited |

### PostgreSQL schema

```sql
CREATE TABLE license_tiers (
    tier_id         TEXT PRIMARY KEY,
    bytes_per_month BIGINT,          -- NULL = unlimited
    vectors_max     BIGINT,
    queries_per_day INTEGER,
    queries_per_min INTEGER,
    gpu_enabled     BOOLEAN DEFAULT FALSE,
    workers_max     INTEGER,
    users_max       INTEGER
);

CREATE TABLE tenant_licenses (
    tenant_id  UUID PRIMARY KEY,
    tier_id    TEXT REFERENCES license_tiers,
    expires_at TIMESTAMPTZ,          -- NULL = perpetual
    is_active  BOOLEAN DEFAULT TRUE
);

CREATE TABLE quota_overrides (
    tenant_id      UUID REFERENCES tenant_licenses,
    metric         TEXT,
    override_value BIGINT,
    PRIMARY KEY (tenant_id, metric)
);

CREATE TABLE usage_history (
    tenant_id   UUID,
    metric      TEXT,
    value       BIGINT,
    recorded_at TIMESTAMPTZ DEFAULT now()
) PARTITION BY RANGE (recorded_at);
```

---

## 4. Quota Service

A lightweight Python gRPC service. Single source of truth for real-time quota state.

```protobuf
service QuotaService {
    rpc CheckQuota(CheckRequest)   returns (CheckResponse);
    rpc RecordUsage(RecordRequest) returns (RecordResponse);
    rpc GetUsage(UsageRequest)     returns (UsageResponse);
}
```

**Real-time counter pattern (Redis):**

```python
def check_and_increment(tenant_id, metric, amount, limit) -> bool:
    key = quota_key(tenant_id, metric)
    pipe = redis.pipeline()
    pipe.incrby(key, amount)
    pipe.expire(key, ttl_for_metric(metric))
    current, _ = pipe.execute()
    if current > limit:
        redis.decrby(key, amount)   # rollback
        return False
    return True
```

---

## 5. Kong API Gateway — Plugin Chain

```
Client → Kong Ingress → Route: /v1/*
    Plugin 1: jwt                  — validate Keycloak RS256 token
    Plugin 2: request-transformer  — extract org_id → X-Tenant-ID header
    Plugin 3: quota-check (Lua)    — gRPC call to Quota Service
    Plugin 4: rate-limiting-advanced
    Plugin 5: opa-authz (optional)
    → Upstream: RAG API
```

---

## 6. OPA Policy Design

```rego
package pipeline.authz

default allow = false

allow { input.action == "use_gpu"; input.license_type == "pro" }
allow { input.action == "use_gpu"; input.license_type == "enterprise" }

allow {
    input.action == "query_collection"
    input.collection_name == concat("_", [input.tenant_id, "docs"])
}

allowed_connectors := {"s3", "nfs"} { input.license_type == "free" }
allowed_connectors := {"s3", "nfs", "database", "stream"} { input.license_type != "free" }

allow {
    input.action == "use_connector"
    input.connector_type == allowed_connectors[_]
}
```

---

## 7. OpenMeter — Usage Metering

```yaml
meters:
  - slug: gpu_seconds
    eventType: pipeline.embedding.batch
    valueProperty: $.data.gpu_seconds
    groupBy:
      tenant_id: $.data.tenant_id

  - slug: bytes_ingested
    eventType: pipeline.ingest.document
    valueProperty: $.data.bytes
    groupBy:
      tenant_id: $.data.tenant_id

  - slug: api_calls
    eventType: pipeline.rag.query
    valueProperty: "1"
    groupBy:
      tenant_id: $.data.tenant_id
```

---

## 8. Tenant Isolation

**Kafka:** `tenant_id` in message headers; Kafka ACLs (KafkaUser CRs) limit each connector to produce-only.

**Milvus:** Collection naming `{tenant_id}_docs`. RAG API derives collection from JWT `org_id` — never from the request body.

**K8s:** Free/Pro share `ai-pipeline` namespace. Enterprise tenants get a dedicated namespace + `ResourceQuota`.

---

## 9. Auth Flows

### RAG Query

```
1. User authenticates → Keycloak issues JWT
2. POST /v1/query  Authorization: Bearer <jwt>
3. Kong: JWT validate → extract X-Tenant-ID → quota-check → rate-limit
4. RAG API: reads X-Tenant-ID, searches Milvus collection {tenant_id}_docs
5. RAG API: publishes usage event to Kafka usage-events
6. OpenMeter: aggregates api_calls for tenant
```

---

## 10. Trade-offs

| Decision | Choice | Rationale | Downside |
|---|---|---|---|
| IAM | Keycloak Organizations | Native multi-tenancy in 24+; no custom tenant table | Organizations feature relatively new |
| API gateway | Kong OSS | Mature plugin ecosystem; Lua custom plugins | Custom Lua plugin debugging is painful |
| Quota enforcement | Kong pre-check + Quota Service record | Fail-fast at gateway | ~5–10 ms latency per request for gRPC check |
| Metering | OpenMeter + Kafka events | Decoupled; handles aggregation windows | Younger OSS project; pin versions carefully |
| Tenant isolation in Milvus | Collection-per-tenant | Hard isolation boundary | Collection count grows; each costs memory |
| K8s isolation | Shared namespace (Free/Pro), dedicated (Enterprise) | Cost-efficient for small tenants | Mixed-model increases operational complexity |
