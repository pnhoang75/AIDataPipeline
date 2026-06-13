"""Per-connector RBAC creation logic for the Pipeline Operator.

The operator creates one Role + RoleBinding per DataConnector CR.
Each Role grants access only to that connector's own secret (exact name,
never a wildcard) — see CLAUDE.md known-issues: connector-sa RBAC.
"""

from __future__ import annotations


def connector_secret_name(connector_name: str) -> str:
    """Canonical secret name for a connector's credentials.

    Matches the BFF naming convention: connector-{slug}-creds.
    """
    return f"connector-{connector_name}-creds"


def make_connector_role(connector_name: str, namespace: str) -> dict:
    """Return a Kubernetes Role manifest for one DataConnector.

    Grants read-only access to:
    - the shared pipeline-config ConfigMap
    - this connector's own credentials secret (exact resourceName, no wildcards)
    """
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": {
            "name": f"connector-{connector_name}-role",
            "namespace": namespace,
        },
        "rules": [
            {
                "apiGroups": [""],
                "resources": ["configmaps"],
                "resourceNames": ["pipeline-config"],
                "verbs": ["get", "watch"],
            },
            {
                "apiGroups": [""],
                "resources": ["secrets"],
                "resourceNames": [connector_secret_name(connector_name)],
                "verbs": ["get"],
            },
        ],
    }


def make_connector_role_binding(
    connector_name: str,
    service_account: str,
    namespace: str,
) -> dict:
    """Return a RoleBinding binding connector-sa to its per-connector Role."""
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {
            "name": f"connector-{connector_name}-rb",
            "namespace": namespace,
        },
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": service_account,
                "namespace": namespace,
            }
        ],
        "roleRef": {
            "kind": "Role",
            "name": f"connector-{connector_name}-role",
            "apiGroup": "rbac.authorization.k8s.io",
        },
    }


def audit_role_for_wildcards(role: dict) -> list[str]:
    """Return a list of offending rule descriptions if any resourceNames contain wildcards.

    Used by tests and CI to assert no wildcard resourceNames exist.
    """
    offences: list[str] = []
    for rule in role.get("rules", []):
        for rn in rule.get("resourceNames", []):
            if "*" in rn:
                offences.append(
                    f"resource={rule.get('resources')} resourceName={rn!r} contains wildcard"
                )
    return offences
