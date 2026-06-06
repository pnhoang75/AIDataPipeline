# AI Data Pipeline — Security Architecture

**Extends:** `ai-data-pipeline-multitenancy.md` and `ai-data-pipeline-operators.md`  
**Covers:** network policies, mTLS, secret management, K8s RBAC, pod security

---

## 1. Security Layers Overview

```
Layer 1 — Perimeter      Kong API Gateway  (JWT auth, rate limiting, quota)
Layer 2 — Network        K8s NetworkPolicies (namespace isolation)
Layer 3 — Transport      mTLS via cert-manager (inter-service)
Layer 4 — Secret         External Secrets Operator → secrets store
Layer 5 — K8s RBAC       ServiceAccount + Role/ClusterRole per workload
Layer 6 — Pod            PodSecurityStandard (restricted), non-root, read-only FS
Layer 7 — Policy         OPA Gatekeeper (admission webhook constraints)
```

---

## 2. Network Policies

Deny-all by default within each namespace; explicit allow rules per service pair.

### 2.1 Default deny-all

Applied to every namespace:

```yaml
# Applied to: ai-pipeline, infrastructure, monitoring
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: default-deny-all
  namespace: ai-pipeline   # repeat per namespace
spec:
  podSelector: {}
  policyTypes: [Ingress, Egress]
```

### 2.2 ai-pipeline namespace policies

```yaml
# Allow connectors → Kafka (produce only)
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-connectors-to-kafka
  namespace: ai-pipeline
spec:
  podSelector:
    matchLabels:
      role: connector
  policyTypes: [Egress]
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: infrastructure
          podSelector:
            matchLabels:
              app.kubernetes.io/name: kafka
      ports:
        - protocol: TCP
          port: 9093   # TLS listener
---
# Allow connectors → PostgreSQL (file status writes)
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-connectors-to-postgres
  namespace: ai-pipeline
spec:
  podSelector:
    matchLabels:
      role: connector
  policyTypes: [Egress]
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: infrastructure
          podSelector:
            matchLabels:
              cnpg.io/cluster: quota-db
      ports:
        - protocol: TCP
          port: 5432
---
# Allow doc-processor → Kafka (consume raw-documents, produce chunks)
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-processor-to-kafka
  namespace: ai-pipeline
spec:
  podSelector:
    matchLabels:
      app: doc-processor
  policyTypes: [Egress]
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: infrastructure
          podSelector:
            matchLabels:
              app.kubernetes.io/name: kafka
      ports:
        - {protocol: TCP, port: 9093}
---
# Allow embedding-worker → Kafka + Milvus + PostgreSQL
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-embedding-worker-egress
  namespace: ai-pipeline
spec:
  podSelector:
    matchLabels:
      app: embedding-worker
  policyTypes: [Egress]
  egress:
    - to:   # Kafka
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: infrastructure
          podSelector:
            matchLabels:
              app.kubernetes.io/name: kafka
      ports: [{protocol: TCP, port: 9093}]
    - to:   # Milvus
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: infrastructure
          podSelector:
            matchLabels:
              app.kubernetes.io/instance: milvus
      ports: [{protocol: TCP, port: 19530}]
    - to:   # PostgreSQL
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: infrastructure
          podSelector:
            matchLabels:
              cnpg.io/cluster: quota-db
      ports: [{protocol: TCP, port: 5432}]
---
# Allow RAG API ← Kong ingress, → Milvus + Redis + Kafka
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-rag-api
  namespace: ai-pipeline
spec:
  podSelector:
    matchLabels:
      app: rag-api
  policyTypes: [Ingress, Egress]
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kong-system
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: infrastructure
      ports:
        - {protocol: TCP, port: 19530}  # Milvus
        - {protocol: TCP, port: 6379}   # Redis
        - {protocol: TCP, port: 9093}   # Kafka (usage events)
---
# Allow quota-service ← kong + rag-api + embedding-worker, → Redis + PostgreSQL
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-quota-service
  namespace: ai-pipeline
spec:
  podSelector:
    matchLabels:
      app: quota-service
  policyTypes: [Ingress, Egress]
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kong-system
        - podSelector:
            matchLabels:
              app: rag-api
        - podSelector:
            matchLabels:
              app: embedding-worker
      ports: [{protocol: TCP, port: 50051}]  # gRPC
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: infrastructure
      ports:
        - {protocol: TCP, port: 6379}   # Redis
        - {protocol: TCP, port: 5432}   # PostgreSQL
```

### 2.3 Infrastructure namespace policy (Kafka egress — inter-broker only)

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: kafka-allow-pipeline-ingress
  namespace: infrastructure
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: kafka
  policyTypes: [Ingress]
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: ai-pipeline
      ports:
        - {protocol: TCP, port: 9093}  # TLS
    - from:
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: kafka  # inter-broker
      ports:
        - {protocol: TCP, port: 9093}
        - {protocol: TCP, port: 9091}  # controller
```

---

## 3. mTLS Topology

cert-manager issues certificates. Services use them for mutual TLS on their gRPC and internal HTTP connections.

### Certificate issuance

```yaml
# One Certificate per service that accepts inbound connections
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: quota-service-tls
  namespace: ai-pipeline
spec:
  dnsNames:
    - quota-service.ai-pipeline.svc.cluster.local
  issuerRef:
    name: pipeline-ca-issuer
    kind: ClusterIssuer
  secretName: quota-service-tls
  duration: 2160h   # 90 days
  renewBefore: 360h # renew 15 days before expiry
```

### mTLS service matrix

| Client | Server | Protocol | mTLS |
|---|---|---|---|
| Kong | RAG API | HTTPS | One-way TLS (Kong terminates) |
| Kong | BFF | HTTPS | One-way TLS |
| Kong | Quota Service | gRPC | mTLS |
| RAG API | Milvus | gRPC | mTLS |
| Embedding Worker | Milvus | gRPC | mTLS |
| BFF | Quota Service | gRPC | mTLS |
| BFF | Keycloak Admin | HTTPS | One-way TLS |
| All pipeline pods | Kafka | SASL_SSL | TLS + SASL (KafkaUser credential) |
| All pipeline pods | PostgreSQL | TLS | mTLS (CNPG client cert) |
| Connectors | MinIO | HTTPS | One-way TLS |

### gRPC mTLS configuration (Python)

```python
import grpc

def create_mtls_channel(host: str, port: int, cert_dir: str) -> grpc.Channel:
    with open(f"{cert_dir}/tls.crt", "rb") as f:
        client_cert = f.read()
    with open(f"{cert_dir}/tls.key", "rb") as f:
        client_key = f.read()
    with open(f"{cert_dir}/ca.crt", "rb") as f:
        ca_cert = f.read()

    credentials = grpc.ssl_channel_credentials(
        root_certificates=ca_cert,
        private_key=client_key,
        certificate_chain=client_cert,
    )
    return grpc.secure_channel(f"{host}:{port}", credentials)

# Usage in RAG API → Milvus
milvus_channel = create_mtls_channel(
    "milvus.infrastructure.svc.cluster.local", 19530,
    cert_dir="/etc/certs/milvus"
)
```

---

## 4. Secret Management

### 4.1 External Secrets Operator (recommended)

Secrets live in a secrets store (AWS Secrets Manager, HashiCorp Vault, or GCP Secret Manager). The External Secrets Operator (ESO) syncs them into K8s Secrets.

```yaml
# ExternalSecret pulls from AWS Secrets Manager
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: minio-creds
  namespace: infrastructure
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: aws-secretsmanager
    kind: ClusterSecretStore
  target:
    name: minio-creds       # K8s Secret name
    creationPolicy: Owner
  data:
    - secretKey: ACCESS_KEY
      remoteRef:
        key: pipeline/minio
        property: access_key
    - secretKey: SECRET_KEY
      remoteRef:
        key: pipeline/minio
        property: secret_key
```

### 4.2 Kind testbed — sealed secrets

For the kind testbed (no external secrets store), use **Bitnami Sealed Secrets**:

```bash
# Install
helm install sealed-secrets sealed-secrets/sealed-secrets -n kube-system

# Seal a secret
kubectl create secret generic minio-creds \
  --from-literal=ACCESS_KEY=minioadmin \
  --from-literal=SECRET_KEY=minioadmin \
  --dry-run=client -o yaml | \
  kubeseal --format yaml > k8s/sealed-minio-creds.yaml

# Commit the sealed secret to git — safe to store
git add k8s/sealed-minio-creds.yaml
```

### 4.3 Secrets inventory

| Secret name | Namespace | Contents | Rotation |
|---|---|---|---|
| `minio-creds` | infrastructure | MinIO access/secret key | On compromise |
| `kafka-connector-{tenant}` | ai-pipeline | Kafka client TLS cert (from KafkaUser CR) | Auto (Strimzi) |
| `quota-db-app` | infrastructure | PostgreSQL connection URI | 90 days (CNPG) |
| `quota-service-tls` | ai-pipeline | gRPC mTLS cert | 90 days (cert-manager) |
| `milvus-client-tls` | ai-pipeline | Milvus gRPC client cert | 90 days |
| `keycloak-admin` | infrastructure | Keycloak admin password | Manual (ops) |
| `openai-api-key` | ai-pipeline | OpenAI API key (optional) | Manual (ops) |
| `pipeline-ca-secret` | cert-manager | Root CA private key | Never (rotate = re-issue all certs) |

All secrets are mounted as files (not env vars) to prevent accidental logging:

```yaml
volumeMounts:
  - name: quota-service-tls
    mountPath: /etc/certs/quota-service
    readOnly: true
volumes:
  - name: quota-service-tls
    secret:
      secretName: quota-service-tls
```

---

## 5. Kubernetes RBAC Matrix

One ServiceAccount per workload. Each has the minimum permissions needed.

### ServiceAccount inventory

| ServiceAccount | Namespace | Workloads |
|---|---|---|
| `connector-sa` | ai-pipeline | All connector pods |
| `doc-processor-sa` | ai-pipeline | Document Processor |
| `embedding-worker-sa` | ai-pipeline | Embedding Worker |
| `rag-api-sa` | ai-pipeline | RAG API |
| `bff-sa` | ai-pipeline | Pipeline Mgmt API (BFF) |
| `quota-service-sa` | ai-pipeline | Quota Service |
| `pipeline-operator-sa` | ai-pipeline | Pipeline Operator |
| `argocd-sa` | argocd | ArgoCD application controller |

### Role definitions

```yaml
# connector-sa: read-only on its own ConfigMap (connector config)
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: connector-role
  namespace: ai-pipeline
rules:
  - apiGroups: [""]
    resources: ["configmaps"]
    resourceNames: ["pipeline-config"]
    verbs: ["get", "watch"]
  - apiGroups: [""]
    resources: ["secrets"]
    resourceNames: ["kafka-connector-*"]
    verbs: ["get"]
---
# bff-sa: read/write ConfigMaps + read pods + patch Deployments
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: bff-role
  namespace: ai-pipeline
rules:
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get", "list", "create", "update", "patch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["apps"]
    resources: ["deployments", "cronjobs"]
    verbs: ["get", "list", "patch"]
  - apiGroups: ["ai-pipeline.io"]
    resources: ["dataconnectors", "tenantworkspaces", "embeddingconfigs", "pipelineclusters"]
    verbs: ["get", "list", "create", "update", "patch", "delete"]
---
# pipeline-operator-sa: full CRUD on pipeline CRDs + K8s workloads
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: pipeline-operator-role
rules:
  - apiGroups: ["ai-pipeline.io"]
    resources: ["*"]
    verbs: ["*"]
  - apiGroups: ["kafka.strimzi.io"]
    resources: ["kafkatopics", "kafkausers"]
    verbs: ["get", "list", "create", "update", "patch", "delete"]
  - apiGroups: ["apps"]
    resources: ["deployments", "cronjobs"]
    verbs: ["get", "list", "create", "update", "patch", "delete"]
  - apiGroups: [""]
    resources: ["namespaces", "resourcequotas", "serviceaccounts", "configmaps"]
    verbs: ["get", "list", "create", "update", "patch"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch"]
```

### RoleBinding summary

```yaml
# Example: bind connector-sa to connector-role
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: connector-rb
  namespace: ai-pipeline
subjects:
  - kind: ServiceAccount
    name: connector-sa
    namespace: ai-pipeline
roleRef:
  kind: Role
  name: connector-role
  apiGroup: rbac.authorization.k8s.io
```

---

## 6. Pod Security

All pipeline pods run under the Kubernetes `restricted` PodSecurityStandard:

```yaml
# Namespace-level enforcement
apiVersion: v1
kind: Namespace
metadata:
  name: ai-pipeline
  labels:
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/audit: restricted
    pod-security.kubernetes.io/warn: restricted
```

Required pod spec fields for compliance:

```yaml
spec:
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    fsGroup: 1000
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: app
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop: [ALL]
      # Writable paths via emptyDir mounts only:
      volumeMounts:
        - name: tmp
          mountPath: /tmp
        - name: model-cache
          mountPath: /app/.cache
  volumes:
    - name: tmp
      emptyDir: {}
    - name: model-cache
      emptyDir:
        sizeLimit: 1Gi   # BGE-small model cache
```

---

## 7. OPA Gatekeeper Constraints Reference

| Constraint | Scope | Policy |
|---|---|---|
| `K8sRequireTenantLabel` | ai-pipeline Pods | All pods must have `tenant` label |
| `K8sBlockCrossNamespaceSecret` | ai-pipeline connector Pods | Connectors may only reference secrets in their own namespace |
| `K8sRequireNonRootUser` | all namespaces | `runAsNonRoot: true` required |
| `K8sRequireReadOnlyRootFS` | ai-pipeline | `readOnlyRootFilesystem: true` required |
| `K8sBlockLatestImageTag` | ai-pipeline | Image tag must not be `latest` in production |
| `K8sRequireResourceLimits` | ai-pipeline | All containers must declare CPU + memory limits |

---

## 8. Audit Logging

Kong access logs include JWT subject (`sub`) and `X-Tenant-ID` on every request. Shipped to Loki:

```
2026-06-05T10:23:11Z  POST /v1/query  200  134ms
  tenant=acme  user=alice@acme.com  quota_checked=true  cached=false
```

PostgreSQL audit log (pgaudit extension on CloudNativePG) records all `SELECT/INSERT/UPDATE/DELETE` on sensitive tables (`tenant_licenses`, `quota_overrides`).

Audit log retention: 90 days in Loki; 1 year in cold storage (MinIO lifecycle policy).
