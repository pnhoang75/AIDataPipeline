#!/usr/bin/env bash
# Session 0-B: Install infrastructure operators via Helm in dependency order.
# cert-manager → Strimzi → CNPG → Milvus Operator → MinIO Operator → Redis Operator
# Each operator's CRDs are verified before moving to the next.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

wait_for_pods() {
  local ns="$1" label="$2" expected="$3"
  echo "    Waiting for pods (ns=$ns label=$label count>=$expected)..."
  local attempts=0
  until [ "$(kubectl get pods -n "$ns" -l "$label" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')" -ge "$expected" ]; do
    sleep 5
    attempts=$((attempts + 1))
    [ $attempts -ge 60 ] && { echo "ERROR: Timeout waiting for $label in $ns"; exit 1; }
  done
  echo "    Ready."
}

wait_for_crd() {
  local crd="$1"
  echo "    Waiting for CRD $crd..."
  until kubectl get crd "$crd" >/dev/null 2>&1; do sleep 3; done
  echo "    CRD $crd registered."
}

echo "==> Adding Helm repos"
helm repo add jetstack         https://charts.jetstack.io
helm repo add strimzi          https://strimzi.io/charts/
helm repo add cnpg             https://cloudnative-pg.github.io/charts
helm repo add milvus-operator  https://zilliztech.github.io/milvus-operator/
helm repo add minio-operator   https://operator.min.io
helm repo add redis-operator   https://spotahome.github.io/redis-operator
helm repo update

# ── cert-manager ────────────────────────────────────────────────────────────
echo ""
echo "==> [1/6] cert-manager v1.14"
helm upgrade --install cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --create-namespace \
  --version "1.14.*" \
  --set installCRDs=true \
  --set global.leaderElection.namespace=cert-manager \
  --wait --timeout 5m

wait_for_crd "certificates.cert-manager.io"
wait_for_crd "clusterissuers.cert-manager.io"

kubectl apply -f "$REPO_ROOT/k8s/operators/cert-manager-issuers.yaml"
echo "    cert-manager OK"

# ── Strimzi ─────────────────────────────────────────────────────────────────
echo ""
echo "==> [2/6] Strimzi Kafka Operator v0.41"
helm upgrade --install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
  --namespace infrastructure \
  --version "0.41.*" \
  --set watchNamespaces="{infrastructure}" \
  --wait --timeout 5m

wait_for_crd "kafkas.kafka.strimzi.io"
wait_for_crd "kafkatopics.kafka.strimzi.io"
echo "    Strimzi OK"

# ── CloudNativePG ────────────────────────────────────────────────────────────
echo ""
echo "==> [3/6] CloudNativePG v1.23"
helm upgrade --install cnpg cnpg/cloudnative-pg \
  --namespace cnpg-system \
  --create-namespace \
  --version "0.21.*" \
  --wait --timeout 5m

wait_for_crd "clusters.postgresql.cnpg.io"
echo "    CNPG OK"

# ── Milvus Operator ──────────────────────────────────────────────────────────
echo ""
echo "==> [4/6] Milvus Operator v0.9"
helm upgrade --install milvus-operator milvus-operator/milvus-operator \
  --namespace milvus-operator \
  --create-namespace \
  --version "0.9.*" \
  --wait --timeout 5m

wait_for_crd "milvuses.milvus.io"
echo "    Milvus Operator OK"

# ── MinIO Operator ───────────────────────────────────────────────────────────
echo ""
echo "==> [5/6] MinIO Operator v5.0"
helm upgrade --install minio-operator minio-operator/operator \
  --namespace minio-operator \
  --create-namespace \
  --version "5.0.*" \
  --wait --timeout 5m

wait_for_crd "tenants.minio.min.io"
echo "    MinIO Operator OK"

# ── Redis Operator ───────────────────────────────────────────────────────────
echo ""
echo "==> [6/6] Redis Operator v0.15"
helm upgrade --install redis-operator redis-operator/redis-operator \
  --namespace redis-operator \
  --create-namespace \
  --version "3.2.*" \
  --wait --timeout 5m

wait_for_crd "redisfailovers.databases.spotahome.com"
echo "    Redis Operator OK"

echo ""
echo "==> Verifying all operator pods"
kubectl get pods -n cert-manager
kubectl get pods -n infrastructure
kubectl get pods -n cnpg-system
kubectl get pods -n milvus-operator
kubectl get pods -n minio-operator
kubectl get pods -n redis-operator

echo ""
echo "Session 0-B complete. All infrastructure operators are running."
echo "Next step: run scripts/bootstrap-gatekeeper.sh (session 0-C)"
