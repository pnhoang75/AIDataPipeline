# AI Data Pipeline — Operator Layer

**Extends:** `ai-data-pipeline-ui.md`  
**New components:** Strimzi · CloudNativePG · Milvus Operator · MinIO Operator · Redis Operator · Keycloak Operator · kube-prometheus-stack · OTel Operator · Grafana Operator · cert-manager · OPA Gatekeeper · Custom Pipeline Operator (kopf) · ArgoCD

---

## 1. Operator Inventory

| Operator | Helm chart | Version | Manages |
|---|---|---|---|
| Strimzi | `strimzi/strimzi-kafka-operator` | 0.41 | Kafka cluster, topics, users, KafkaConnect |
| CloudNativePG | `cnpg/cloudnative-pg` | 1.23 | PostgreSQL cluster, backups, pooler |
| Milvus Operator | `milvus/milvus-operator` | 0.9 | Milvus standalone + cluster |
| MinIO Operator | `minio-operator/operator` | 5.0 | MinIO tenants, pools, console |
| Redis Operator | `ot-helm/redis-operator` | 0.15 | Redis standalone / sentinel / cluster |
| Keycloak Operator | `keycloak/keycloak` | 24.x | Keycloak instance, realms, clients |
| kube-prometheus-stack | `prometheus-community/kube-prometheus-stack` | 58.x | Prometheus, AlertManager, Grafana |
| OTel Operator | `open-telemetry/opentelemetry-operator` | 0.57 | Collectors, auto-instrumentation |
| Grafana Operator | `grafana/grafana-operator` | 5.x | GrafanaDashboard, GrafanaDatasource CRDs |
| cert-manager | `jetstack/cert-manager` | 1.14 | TLS certificates, cluster issuers |
| OPA Gatekeeper | `gatekeeper/gatekeeper` | 3.16 | Admission constraints, policy enforcement |
| **Pipeline Operator** | `./charts/pipeline-operator` | — | PipelineCluster, DataConnector, EmbeddingConfig, TenantWorkspace |
| ArgoCD | `argo/argo-cd` | 6.x | GitOps sync, upgrade orchestration |

---

## 2. Bootstrap Order

```bash
# 0. cert-manager first — everything else needs TLS
helm install cert-manager jetstack/cert-manager -n cert-manager --create-namespace \
  --set installCRDs=true

# 1. Infrastructure operators
kubectl apply -f https://strimzi.io/install/latest?namespace=infrastructure
helm install cnpg cnpg/cloudnative-pg -n cnpg-system --create-namespace
helm install milvus-operator milvus/milvus-operator -n milvus-operator --create-namespace
helm install minio-operator minio-operator/operator -n minio-operator --create-namespace
helm install redis-operator ot-helm/redis-operator -n ot-operators --create-namespace

# 2. Security
helm install gatekeeper gatekeeper/gatekeeper -n gatekeeper-system --create-namespace

# 3. Observability
helm install kube-prom prometheus-community/kube-prometheus-stack \
  -n monitoring --create-namespace -f helm/prometheus-values.yaml
helm install otel-operator open-telemetry/opentelemetry-operator \
  -n opentelemetry-operator-system --create-namespace
helm install grafana-operator grafana/grafana-operator -n monitoring

# 4. Application operators
helm install keycloak-operator keycloak/keycloak -n infrastructure -f helm/keycloak-values.yaml

# 5. Custom pipeline operator (last — depends on Strimzi + Milvus CRDs)
helm install pipeline-operator ./charts/pipeline-operator -n ai-pipeline --create-namespace

# 6. GitOps controller
helm install argocd argo/argo-cd -n argocd --create-namespace -f helm/argocd-values.yaml
```

---

## 3. Strimzi — Kafka

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: ai-pipeline-kafka
  namespace: infrastructure
spec:
  kafka:
    version: 3.7.0
    replicas: 1          # 3 in production
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
    config:
      offsets.topic.replication.factor: 1
      default.replication.factor: 1
    storage:
      type: persistent-claim
      size: 20Gi
  entityOperator:        # manages KafkaTopic + KafkaUser CRDs
    topicOperator: {}
    userOperator: {}
```

**KafkaTopic CR example:**

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: raw-documents
  labels:
    strimzi.io/cluster: ai-pipeline-kafka
spec:
  partitions: 4
  replicas: 1
  config:
    retention.ms: 604800000   # 7 days
```

**KafkaUser CR (produce-only ACL per connector):**

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaUser
metadata:
  name: connector-acme-s3
  labels:
    strimzi.io/cluster: ai-pipeline-kafka
spec:
  authentication:
    type: tls
  authorization:
    type: simple
    acls:
      - resource: {type: topic, name: raw-documents}
        operations: [Write, Describe]
```

**Upgrade:** Rolling broker upgrade one at a time. Two-phase: inter-broker protocol first, then message format. Zero downtime for ≥3 replicas.

---

## 4. CloudNativePG — PostgreSQL

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: quota-db
  namespace: infrastructure
spec:
  instances: 1           # 2+ in production
  imageName: ghcr.io/cloudnative-pg/postgresql:16.2
  storage:
    size: 10Gi
  backup:
    barmanObjectStore:
      destinationPath: s3://pipeline-backups/cnpg/
      endpointURL: http://minio.infrastructure.svc:9000
      s3Credentials:
        accessKeyId:    {name: minio-creds, key: ACCESS_KEY}
        secretAccessKey:{name: minio-creds, key: SECRET_KEY}
    retentionPolicy: 7d
  monitoring:
    enablePodMonitor: true   # auto-creates ServiceMonitor
---
apiVersion: postgresql.cnpg.io/v1
kind: ScheduledBackup
metadata:
  name: quota-db-daily
spec:
  schedule: "0 2 * * *"
  cluster:
    name: quota-db
```

---

## 5. Milvus Operator

```yaml
# Testbed: standalone
apiVersion: milvus.io/v1beta1
kind: Milvus
metadata:
  name: milvus
  namespace: infrastructure
spec:
  mode: standalone
  components:
    image: milvusdb/milvus:v2.4.1
    resources:
      requests: {memory: 2Gi, cpu: "1"}
      limits:   {memory: 4Gi}
  dependencies:
    etcd:    {inCluster: {values: {replicaCount: 1}}}
    storage: {inCluster: {values: {mode: standalone}}}
```

```yaml
# Production: cluster mode
apiVersion: milvus.io/v1beta1
kind: MilvusCluster
metadata:
  name: milvus
  namespace: infrastructure
spec:
  components:
    queryNode: {replicas: 2}
    dataNode:  {replicas: 2}
    indexNode: {replicas: 1}
    proxy:     {replicas: 1}
  dependencies:
    etcd:    {external: true, endpoints: ["etcd.infrastructure.svc:2379"]}
    storage: {external: true, endpoint: minio.infrastructure.svc:9000}
```

---

## 6. MinIO Operator

```yaml
apiVersion: minio.min.io/v2
kind: Tenant
metadata:
  name: pipeline-store
  namespace: infrastructure
spec:
  image: minio/minio:RELEASE.2024-04-06T05-26-02Z
  pools:
    - name: pool-0
      servers: 1
      volumesPerServer: 1
      volumeClaimTemplate:
        spec:
          resources: {requests: {storage: 50Gi}}
  credsSecret:    {name: minio-creds}
  prometheusOperator: true   # auto-creates ServiceMonitor
  buckets:
    - {name: pipeline-backups}
    - {name: opa-bundles}
    - {name: pipeline-artifacts}
```

---

## 7. kube-prometheus-stack

```yaml
# ServiceMonitor — auto-created by Pipeline Operator for each worker
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: embedding-worker
  namespace: ai-pipeline
  labels:
    release: kube-prom
spec:
  selector:
    matchLabels: {app: embedding-worker}
  endpoints:
    - port: metrics
      path: /metrics
      interval: 15s
---
# PrometheusRule — pipeline alerts
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: pipeline-alerts
  namespace: ai-pipeline
spec:
  groups:
    - name: pipeline
      rules:
        - alert: KafkaConsumerLagHigh
          expr: kafka_consumer_lag{topic="document-chunks"} > 5000
          for: 5m
          labels: {severity: warning}

        - alert: ConnectorDown
          expr: up{job=~"connector-.*"} == 0
          for: 2m
          labels: {severity: critical}

        - alert: TenantNearQuota
          expr: quota_usage_ratio{metric="bytes_per_month"} > 0.8
          for: 1m
          labels: {severity: warning}

        - alert: RAGLatencyHigh
          expr: histogram_quantile(0.99, rate(rag_query_duration_seconds_bucket[5m])) > 1.0
          for: 3m
          labels: {severity: warning}
```

---

## 8. OpenTelemetry Operator

```yaml
apiVersion: opentelemetry.io/v1alpha1
kind: OpenTelemetryCollector
metadata:
  name: pipeline-collector
  namespace: ai-pipeline
spec:
  mode: deployment
  config: |
    receivers:
      otlp:
        protocols:
          grpc: {endpoint: 0.0.0.0:4317}
    processors:
      batch: {}
    exporters:
      otlp:
        endpoint: tempo.monitoring.svc:4317
        tls: {insecure: true}
    service:
      pipelines:
        traces:
          receivers: [otlp]
          processors: [batch]
          exporters: [otlp]
---
apiVersion: opentelemetry.io/v1alpha1
kind: Instrumentation
metadata:
  name: python-instrumentation
  namespace: ai-pipeline
spec:
  python:
    image: ghcr.io/open-telemetry/opentelemetry-operator/autoinstrumentation-python:0.44b0
  sampler:
    type: parentbased_traceidratio
    argument: "0.1"    # 10% on testbed
  exporter:
    endpoint: http://pipeline-collector-collector.ai-pipeline.svc:4317
```

Pods opt in via annotation:
```yaml
annotations:
  instrumentation.opentelemetry.io/inject-python: "true"
```

---

## 9. cert-manager

```yaml
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata: {name: selfsigned-ca}
spec: {selfSigned: {}}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: pipeline-ca
  namespace: cert-manager
spec:
  isCA: true
  commonName: ai-pipeline-ca
  secretName: pipeline-ca-secret
  issuerRef: {name: selfsigned-ca, kind: ClusterIssuer}
---
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata: {name: pipeline-ca-issuer}
spec:
  ca: {secretName: pipeline-ca-secret}
```

In production, swap to ACME (Let's Encrypt) — only the `ClusterIssuer` spec changes.

---

## 10. OPA Gatekeeper

```yaml
apiVersion: templates.gatekeeper.sh/v1
kind: ConstraintTemplate
metadata: {name: k8srequiretenantlabel}
spec:
  crd:
    spec:
      names: {kind: K8sRequireTenantLabel}
  targets:
    - target: admission.k8s.gatekeeper.sh
      rego: |
        package k8srequiretenantlabel
        violation[{"msg": msg}] {
          not input.review.object.metadata.labels.tenant
          msg := "Pod must have a 'tenant' label"
        }
---
apiVersion: constraints.gatekeeper.sh/v1beta1
kind: K8sRequireTenantLabel
metadata: {name: require-tenant-label}
spec:
  match:
    kinds: [{apiGroups: [""], kinds: ["Pod"]}]
    namespaces: ["ai-pipeline"]
```

---

## 11. Custom Pipeline Operator (kopf)

### CRDs

| CRD | Scope | Purpose |
|---|---|---|
| `PipelineCluster` | cluster | Top-level config; version field drives coordinated upgrades |
| `DataConnector` | namespace | One per source; operator creates Deployment or CronJob + KafkaTopic + KafkaUser |
| `EmbeddingConfig` | namespace | Embedding backend/model/batch; operator triggers rolling restart on change |
| `TenantWorkspace` | namespace | Provisions Milvus collection + K8s ResourceQuota + Kafka ACLs |

### DataConnector reconcile

```python
@kopf.on.create('ai-pipeline.io', 'v1alpha1', 'dataconnectors')
@kopf.on.update('ai-pipeline.io', 'v1alpha1', 'dataconnectors')
async def reconcile_connector(spec, name, patch, **kwargs):
    await k8s_client.apply_kafka_topic('raw-documents', partitions=4)
    await k8s_client.apply_kafka_user(
        name=f"connector-{spec['tenantId']}-{spec['sourceType']}",
        acls=[{'topic': 'raw-documents', 'operations': ['Write']}]
    )
    if spec.get('pollInterval'):
        await k8s_client.apply_cronjob(name=f"connector-{name}", ...)
    else:
        await k8s_client.apply_deployment(name=f"connector-{name}", ...)
    patch.status['state'] = 'Running'
```

### EmbeddingConfig reconcile (dimension-change guard)

```python
@kopf.on.update('ai-pipeline.io', 'v1alpha1', 'embeddingconfigs', field='spec')
async def reconcile_embedding(spec, old, new, patch, **kwargs):
    old_dim, new_dim = old['spec'].get('dimension'), new['spec'].get('dimension')
    if old_dim and new_dim and old_dim != new_dim:
        patch.status['state'] = 'BlockedDimensionChange'
        raise kopf.TemporaryError(
            f"Dimension change {old_dim}→{new_dim} requires re-index. "
            "Set spec.reindexConfirmed: true to proceed.", delay=60)
    await k8s_client.patch_configmap('pipeline-config', {
        'EMBEDDING_BACKEND': spec['backend'],
        'EMBEDDING_MODEL':   spec['model'],
        'EMBEDDING_DEVICE':  spec['device'],
    })
    await k8s_client.rollout_restart('deployment', 'embedding-worker')
    patch.status['state'] = 'Applied'
```

### Upgrade sequence (triggered by `PipelineCluster.spec.version` bump)

```
1. Operator sets UpgradeInProgress condition
2. Pause connector CronJobs
3. Drain Kafka — wait for consumer lag = 0
4. Roll doc-processor → new image
5. Roll embedding workers → new image
6. Roll RAG API → new image + smoke test
7. Resume connectors; clear UpgradeInProgress
```

Rollback: set `spec.version` back to previous tag — operator replays in reverse.

---

## 12. ArgoCD — GitOps

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: ai-pipeline-infra
  namespace: argocd
spec:
  source:
    repoURL: https://github.com/acme/ai-pipeline-gitops
    targetRevision: main
    path: apps/infrastructure
  destination:
    server: https://kubernetes.default.svc
    namespace: infrastructure
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
      - ServerSideApply=true
```

**Upgrade workflow:** bump version in Git → PR review → merge → ArgoCD syncs → Pipeline Operator runs coordinated upgrade → PrometheusRule fires on regression → rollback via `git revert`.

---

## 13. Trade-offs

| Decision | Choice | Rationale | Downside |
|---|---|---|---|
| Kafka operator | Strimzi | OSS, battle-tested, KRaft, Entity Operator | Complex major-version upgrades; must follow Strimzi support matrix |
| Custom operator language | kopf (Python) | Consistent with Python stack; fast to develop | Less performant at high CRD volume; acceptable for tens of CRs |
| Operator lifecycle | ArgoCD GitOps | Declarative, auditable, rollback via git revert | Sync loops can conflict with manual `kubectl` changes |
| Milvus upgrade | Operator-managed rolling | Zero-downtime for cluster mode | Standalone needs brief restart; plan maintenance window |
| Tracing sampling | 10% on testbed | Reduces storage/CPU overhead | May miss infrequent error traces; increase temporarily for debugging |
| Gatekeeper failure policy | `Ignore` (testbed) / `Fail` (prod) | Prevents downed webhook blocking pod creation on testbed | Must flip to `Fail` before production — policy violations silently admitted otherwise |
