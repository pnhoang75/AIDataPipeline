#!/usr/bin/env bash
# promote-to-production.sh <COMMIT_SHA>
# Updates k8s/overlays/production/kustomization.yaml with the given SHA tag,
# then commits so ArgoCD can sync the change to production.
#
# Rollback: git revert <this-commit> → ArgoCD syncs → Pipeline Operator
# runs the coordinated downgrade sequence.
set -euo pipefail

SHA="${1:?Usage: $0 <COMMIT_SHA>}"
KUST="k8s/overlays/production/kustomization.yaml"

if [[ ! "${SHA}" =~ ^[0-9a-f]{40}$ ]]; then
  echo "ERROR: SHA must be a 40-character hex string, got: ${SHA}" >&2
  exit 1
fi

# Replace all PROMOTE_FROM_STAGING or existing SHA tags in the production overlay
sed -i.bak "s/newTag: .*/newTag: ${SHA}/g" "${KUST}"
rm -f "${KUST}.bak"

git add "${KUST}"
git commit -m "chore(production): promote images to ${SHA:0:12}

Promoted from staging after successful verification.
To rollback: git revert HEAD"

echo "Promoted ${SHA:0:12} to production. Push and ArgoCD will sync."
