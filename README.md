# AI Data Pipeline

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A full-stack, multi-tenant RAG (Retrieval-Augmented Generation) data pipeline, designed and built Kubernetes-native from the ground up — source connectors, document processing, embedding generation, vector storage, and a query API, with quota management, GitOps deployment, and a security/compliance posture suitable for a real multi-tenant SaaS.

Built end-to-end across a 52-session autonomous implementation plan (see [`docs/sessions.json`](docs/sessions.json) and [`docs/execution-progress.json`](docs/execution-progress.json)).

## What it does

1. **Ingest** documents from S3/MinIO, NFS shares, or direct upload, per-tenant.
2. **Process** them — parsing and chunking PDF, DOCX, HTML, plain text, JSON, and CSV.
3. **Embed** chunks into 384-dim vectors (`BAAI/bge-small-en-v1.5`, CPU-only on the testbed, GPU-ready in production via config).
4. **Store** vectors + metadata in Milvus, with full lineage tracked in Postgres.
5. **Serve** retrieval-augmented queries via a REST/LangChain-compatible RAG API.
6. **Enforce** per-tenant quotas, auth (Keycloak Organizations + OPA Gatekeeper policy), and network isolation throughout.

## Architecture

```
Source (S3 / NFS / upload)
        │
        ▼
  connector-s3 / connector-nfs  ──▶  Kafka (raw-documents)
        │
        ▼
   doc-processor (parse, chunk)  ──▶  Kafka (chunked-documents)
        │
        ▼
  embedding-worker (CPU/GPU)  ──▶  Milvus (vectors) + metadata-events topic
        │
        ▼
     rag-api  ◀── BFF ◀── frontend (React)
        │
   quota-service (gRPC + Redis)   pipeline-operator (kopf, CRD-driven provisioning)
   metadata-service (lineage)     Keycloak / Kong / OPA Gatekeeper (authn/authz)
```

Full design rationale, sequence diagrams, and component contracts live in [`docs/`](docs/):

| Doc | Covers |
|---|---|
| [`ai-data-pipeline-design.md`](docs/ai-data-pipeline-design.md) | Requirements, component architecture, data contracts |
| [`ai-data-pipeline-multitenancy.md`](docs/ai-data-pipeline-multitenancy.md) | Tenant isolation, quota tiers |
| [`ai-data-pipeline-security.md`](docs/ai-data-pipeline-security.md) | AuthN/Z, RBAC, SSRF/path-validation hardening |
| [`ai-data-pipeline-operators.md`](docs/ai-data-pipeline-operators.md) | `pipeline-operator` CRDs and reconcile logic |
| [`ai-data-pipeline-error-handling.md`](docs/ai-data-pipeline-error-handling.md) | DLQs, retries, idempotency |
| [`ai-data-pipeline-ui.md`](docs/ai-data-pipeline-ui.md) | Frontend/BFF contract |
| [`metadata-lineage.md`](docs/metadata-lineage.md) | Postgres lineage schema |
| [`implementation-plan.md`](docs/implementation-plan.md) / [`test-plan.md`](docs/test-plan.md) | Build and test plan this repo was executed against |
| [`diagrams/`](docs/diagrams) | Architecture and sequence diagrams |
| [`api/`](docs/api) | OpenAPI specs (BFF, RAG API) + quota-service gRPC proto + pipeline-operator CRDs |

## Repository layout

```
services/
  connector-s3/         source connector — MinIO/S3
  connector-nfs/         source connector — NFS share polling
  doc-processor/         parsing + chunking
  embedding-worker/       embedding generation + Milvus writer
  rag-api/                retrieval API (REST + LangChain VectorStore)
  quota-service/          gRPC quota enforcement (Redis-backed counters)
  bff/                    backend-for-frontend
  pipeline-operator/      kopf operator — CRD-driven tenant/pipeline provisioning
  metadata-service/       lineage + pipeline-run tracking API
frontend/                 React + Vite SPA
k8s/
  base/                   namespaces, NetworkPolicies, kind cluster config
  operators/              Helm values + CRs for infra operators (ArgoCD, Keycloak, Kong,
                          Gatekeeper, cert-manager, sealed-secrets, monitoring stack, OTel)
  pipeline/               app workloads, CRDs, RBAC, Kafka topics, Milvus, quota DB
  overlays/               staging / production overlays
tests/
  unit/ integration/ e2e/ security/ chaos/ performance/
scripts/
  auto-execute.sh         autonomous session executor
docs/                     design docs, implementation/test plans, session registry
logs/sessions/            per-session execution logs
.sessions-done/           sentinel files marking completed sessions
reports/                  kube-bench / trivy security scan output
```

## Stack

- **Languages:** Python (services), TypeScript/React (frontend)
- **Messaging:** Kafka (Strimzi KRaft, single-broker testbed)
- **Vector store:** Milvus (standalone testbed → cluster in production)
- **Relational store:** PostgreSQL via CloudNativePG
- **Cache/counters:** Redis
- **Auth:** Keycloak 24+ (Organizations), Kong OSS (gateway), OPA Gatekeeper (policy)
- **GitOps:** ArgoCD
- **Observability:** Prometheus, Grafana, Loki, Tempo, OpenTelemetry
- **Orchestration:** kopf-based Python operator + Kubernetes CRDs

## Running locally

The project targets a [`kind`](https://kind.sigs.k8s.io/) cluster (config in [`k8s/base/kind-config.yaml`](k8s/base/kind-config.yaml)):

```bash
kind create cluster --config k8s/base/kind-config.yaml
kubectl apply -f k8s/base/namespaces.yaml -f k8s/base/network-policies.yaml
# Install infra operators (ArgoCD, CNPG, Strimzi, Milvus operator, etc.) per k8s/operators/
# Then sync the pipeline workloads (k8s/pipeline/) via ArgoCD or kubectl apply
```

Run service unit tests scoped per-service to keep feedback fast:

```bash
pytest tests/unit/<service>/ -x --tb=short -q
```

On a resource-constrained laptop, the full stack (all operators + monitoring + app services) can exceed a single Docker Desktop VM's CPU/DNS budget. A reduced footprint — Kafka, Milvus, Postgres, Redis, and the five data-path services (`connector-nfs`, `doc-processor`, `embedding-worker`, `rag-api`, `metadata-service`) with ArgoCD/monitoring/Keycloak/Kong scaled to 0 — is sufficient to exercise the full ingest → chunk → embed → store → retrieve path.

## Status

All 52 planned implementation sessions are complete (`docs/execution-progress.json`). The full ingest-to-retrieval path has been validated end-to-end against the real service code.

## License

MIT — see [LICENSE](LICENSE).
