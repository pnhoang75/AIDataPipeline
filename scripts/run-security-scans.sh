#!/usr/bin/env bash
# Security scanning script — session 6-G
#
# Runs kube-bench (CIS benchmark) and kubescape (NSA framework) against the
# kind testbed cluster.  Results are written to reports/.
#
# Prerequisites:
#   - kubectl configured for the ai-pipeline kind cluster
#   - kube-bench installed: https://github.com/aquasecurity/kube-bench
#   - kubescape installed:  curl -s https://raw.githubusercontent.com/kubescape/kubescape/master/install.sh | /bin/bash
#
# Usage:
#   bash scripts/run-security-scans.sh

set -euo pipefail

REPORTS_DIR="$(cd "$(dirname "$0")/.." && pwd)/reports"
mkdir -p "$REPORTS_DIR"

echo "=== kube-bench CIS Kubernetes Benchmark ==="
# Run inside the control-plane node (kind mounts /etc/kubernetes)
kubectl run kube-bench \
  --image=aquasec/kube-bench:latest \
  --restart=Never \
  --overrides='
  {
    "spec": {
      "hostPID": true,
      "nodeSelector": {"node-role.kubernetes.io/control-plane": ""},
      "tolerations": [{"key": "node-role.kubernetes.io/control-plane", "operator": "Exists", "effect": "NoSchedule"}],
      "containers": [{
        "name": "kube-bench",
        "image": "aquasec/kube-bench:latest",
        "command": ["kube-bench", "run", "--targets", "master,etcd,node,policies"],
        "volumeMounts": [
          {"name": "var-lib-etcd",    "mountPath": "/var/lib/etcd"},
          {"name": "var-lib-kubelet", "mountPath": "/var/lib/kubelet"},
          {"name": "var-lib-kube-scheduler", "mountPath": "/var/lib/kube-scheduler"},
          {"name": "var-lib-kube-controller-manager", "mountPath": "/var/lib/kube-controller-manager"},
          {"name": "etc-systemd",     "mountPath": "/etc/systemd"},
          {"name": "lib-systemd",     "mountPath": "/lib/systemd/"},
          {"name": "srv-kubernetes",  "mountPath": "/srv/kubernetes/"},
          {"name": "etc-kubernetes",  "mountPath": "/etc/kubernetes"},
          {"name": "usr-local-mount-bin", "mountPath": "/usr/local/mount-bin"},
          {"name": "etc-cni-netd",    "mountPath": "/etc/cni/net.d/"},
          {"name": "opt-cni-bin",     "mountPath": "/opt/cni/bin/"},
          {"name": "tmp",             "mountPath": "/tmp"}
        ]
      }],
      "volumes": [
        {"name": "var-lib-etcd",    "hostPath": {"path": "/var/lib/etcd"}},
        {"name": "var-lib-kubelet", "hostPath": {"path": "/var/lib/kubelet"}},
        {"name": "var-lib-kube-scheduler", "hostPath": {"path": "/var/lib/kube-scheduler"}},
        {"name": "var-lib-kube-controller-manager", "hostPath": {"path": "/var/lib/kube-controller-manager"}},
        {"name": "etc-systemd",     "hostPath": {"path": "/etc/systemd"}},
        {"name": "lib-systemd",     "hostPath": {"path": "/lib/systemd/"}},
        {"name": "srv-kubernetes",  "hostPath": {"path": "/srv/kubernetes/"}},
        {"name": "etc-kubernetes",  "hostPath": {"path": "/etc/kubernetes"}},
        {"name": "usr-local-mount-bin", "hostPath": {"path": "/usr/local/mount-bin"}},
        {"name": "etc-cni-netd",    "hostPath": {"path": "/etc/cni/net.d/"}},
        {"name": "opt-cni-bin",     "hostPath": {"path": "/opt/cni/bin/"}},
        {"name": "tmp",             "hostPath": {"path": "/tmp"}}
      ]
    }
  }' \
  --wait --timeout=120s 2>/dev/null || true

kubectl logs kube-bench 2>/dev/null | tee "$REPORTS_DIR/kube-bench-full.txt" | \
  grep -E "^\[|^==|Level|TOTAL|pass rate" | tee "$REPORTS_DIR/kube-bench-summary.txt"

kubectl delete pod kube-bench --ignore-not-found=true 2>/dev/null || true

echo ""
echo "=== kubescape NSA Framework Scan (manifests) ==="
kubescape scan framework nsa k8s/ \
  --format text \
  --output "$REPORTS_DIR/kubescape-full.txt" 2>/dev/null | \
  grep -E "PASS|FAIL|Score|Control" | \
  tee "$REPORTS_DIR/kubescape-summary.txt"

echo ""
echo "Reports written to $REPORTS_DIR/"
echo "  kube-bench-summary.txt"
echo "  kubescape-summary.txt"
