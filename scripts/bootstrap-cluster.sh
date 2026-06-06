#!/usr/bin/env bash
# Bootstrap the ai-pipeline kind cluster and apply base manifests.
# Run once after Docker Desktop is started.
set -euo pipefail

CLUSTER_NAME="ai-pipeline"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "==> Checking prerequisites"
for cmd in kind kubectl docker; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "ERROR: $cmd not found"; exit 1; }
done

docker info >/dev/null 2>&1 || { echo "ERROR: Docker daemon is not running"; exit 1; }

echo "==> Creating kind cluster '$CLUSTER_NAME'"
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "    Cluster already exists, skipping create"
else
  kind create cluster --config "$REPO_ROOT/k8s/base/kind-config.yaml" --wait 120s
fi

echo "==> Setting kubectl context"
kubectl cluster-info --context "kind-${CLUSTER_NAME}"

echo "==> Labeling nodes"
kubectl label node "${CLUSTER_NAME}-control-plane" node-role=control-plane --overwrite
kubectl label node "${CLUSTER_NAME}-worker"        node-role=infra       --overwrite
kubectl label node "${CLUSTER_NAME}-worker2"       node-role=workload    --overwrite 2>/dev/null || true

echo "==> Applying namespaces"
kubectl apply -f "$REPO_ROOT/k8s/base/namespaces.yaml"

echo "==> Applying NetworkPolicies"
kubectl apply -f "$REPO_ROOT/k8s/base/network-policies.yaml"

echo "==> Verifying namespaces"
kubectl get ns ai-pipeline infrastructure monitoring

echo ""
echo "Session 0-A complete. Cluster is ready."
echo "Next step: run scripts/bootstrap-operators.sh (session 0-B)"
