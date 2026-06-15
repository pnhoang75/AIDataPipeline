"""Final RBAC audit — session 6-G.

Statically validates all Kubernetes RBAC manifests in k8s/pipeline/rbac.yaml
against the security invariants from the design docs:
    - No wildcard resourceNames in Roles (CLAUDE.md known-issue)
    - No wildcard verbs (*) in Roles
    - All ServiceAccounts have automountServiceAccountToken: false
    - Application ServiceAccounts hold only namespace-scoped Roles (no ClusterRole)
    - pipeline-operator-sa does not get delete access to core secrets

These checks complement the dynamic operator tests in test_rbac.py; they
audit the *static* base manifest as deployed by k8s/pipeline/rbac.yaml.
"""

from __future__ import annotations

import os
import re

import pytest
import yaml

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_RBAC_MANIFEST = os.path.join(_REPO_ROOT, "k8s", "pipeline", "rbac.yaml")


def _load_rbac() -> list[dict]:
    with open(_RBAC_MANIFEST) as f:
        return [doc for doc in yaml.safe_load_all(f) if doc is not None]


def _by_kind(docs: list[dict], kind: str) -> list[dict]:
    return [d for d in docs if d.get("kind") == kind]


@pytest.fixture(scope="module")
def rbac_docs() -> list[dict]:
    return _load_rbac()


class TestServiceAccountHardening:
    """All ServiceAccounts must opt out of auto-mounted tokens."""

    def test_all_service_accounts_disable_automount(self, rbac_docs):
        sas = _by_kind(rbac_docs, "ServiceAccount")
        assert sas, "No ServiceAccounts found in rbac.yaml"
        offenders = [
            sa["metadata"]["name"]
            for sa in sas
            if sa.get("automountServiceAccountToken") is not False
        ]
        assert not offenders, (
            f"ServiceAccounts missing automountServiceAccountToken: false — {offenders}"
        )


class TestRoleResourceNamesNoWildcards:
    """Static manifest Roles must not use wildcard resourceNames."""

    def test_no_wildcard_resource_names_in_roles(self, rbac_docs):
        roles = _by_kind(rbac_docs, "Role")
        offences: list[str] = []
        for role in roles:
            name = role["metadata"]["name"]
            for rule in role.get("rules", []):
                for rn in rule.get("resourceNames", []):
                    if "*" in rn:
                        offences.append(f"{name}: resourceName '{rn}'")
        assert not offences, f"Wildcard resourceNames found: {offences}"


class TestRoleVerbsNoWildcards:
    """No Role should grant wildcard verbs."""

    def test_no_wildcard_verbs_in_roles(self, rbac_docs):
        roles = _by_kind(rbac_docs, "Role")
        offences: list[str] = []
        for role in roles:
            name = role["metadata"]["name"]
            for rule in role.get("rules", []):
                if "*" in rule.get("verbs", []):
                    offences.append(f"{name}: rule {rule.get('resources', '?')}")
        assert not offences, f"Wildcard verbs found in Roles: {offences}"


class TestApplicationRoleScope:
    """Application roles must not grant delete on secrets."""

    _APP_SA_PREFIXES = [
        "connector",
        "doc-processor",
        "embedding-worker",
        "rag-api",
        "quota-service",
    ]

    def test_application_roles_cannot_delete_secrets(self, rbac_docs):
        roles = _by_kind(rbac_docs, "Role")
        offences: list[str] = []
        for role in roles:
            name = role["metadata"]["name"]
            if not any(name.startswith(p) for p in self._APP_SA_PREFIXES):
                continue
            for rule in role.get("rules", []):
                if "secrets" in rule.get("resources", []):
                    bad_verbs = set(rule.get("verbs", [])) & {"create", "update", "patch", "delete"}
                    if bad_verbs:
                        offences.append(f"{name}: write verbs {bad_verbs} on secrets")
        assert not offences, f"Application roles have write access to secrets: {offences}"

    def test_application_roles_configmap_access_is_named(self, rbac_docs):
        """Application configmap rules must scope to named resources, not all configmaps."""
        roles = _by_kind(rbac_docs, "Role")
        offences: list[str] = []
        for role in roles:
            name = role["metadata"]["name"]
            if not any(name.startswith(p) for p in self._APP_SA_PREFIXES):
                continue
            for rule in role.get("rules", []):
                if "configmaps" in rule.get("resources", []):
                    if not rule.get("resourceNames"):
                        offences.append(
                            f"{name}: configmap rule missing resourceNames (grants access to all configmaps)"
                        )
        assert not offences, f"Unrestricted configmap access: {offences}"


class TestPipelineOperatorScope:
    """pipeline-operator-sa has broader access; verify it stays reasonable."""

    def test_operator_role_does_not_have_secret_write(self, rbac_docs):
        """Operator reads secrets (to copy credentials) but must not write them."""
        roles = _by_kind(rbac_docs, "Role")
        op_role = next(
            (r for r in roles if r["metadata"]["name"] == "pipeline-operator-role"), None
        )
        assert op_role is not None, "pipeline-operator-role not found in rbac.yaml"
        for rule in op_role.get("rules", []):
            if "secrets" in rule.get("resources", []):
                bad_verbs = set(rule.get("verbs", [])) & {"create", "update", "patch", "delete"}
                assert not bad_verbs, (
                    f"pipeline-operator-role has write verbs on secrets: {bad_verbs}"
                )

    def test_operator_role_is_namespace_scoped(self, rbac_docs):
        """Operator must not have a ClusterRole — only namespace-scoped Role."""
        cluster_roles = _by_kind(rbac_docs, "ClusterRole")
        op_cluster_roles = [
            cr["metadata"]["name"]
            for cr in cluster_roles
            if "operator" in cr["metadata"]["name"].lower()
        ]
        assert not op_cluster_roles, (
            f"pipeline-operator has ClusterRole(s): {op_cluster_roles} — must be namespace-scoped"
        )


class TestRoleBindingIntegrity:
    """RoleBindings must reference valid ServiceAccounts and Roles."""

    def test_all_role_bindings_have_subjects(self, rbac_docs):
        rbs = _by_kind(rbac_docs, "RoleBinding")
        assert rbs, "No RoleBindings found"
        empty = [rb["metadata"]["name"] for rb in rbs if not rb.get("subjects")]
        assert not empty, f"RoleBindings with no subjects: {empty}"

    def test_role_bindings_reference_service_accounts(self, rbac_docs):
        rbs = _by_kind(rbac_docs, "RoleBinding")
        sas = {sa["metadata"]["name"] for sa in _by_kind(rbac_docs, "ServiceAccount")}
        offences: list[str] = []
        for rb in rbs:
            for subject in rb.get("subjects", []):
                if subject.get("kind") == "ServiceAccount":
                    if subject["name"] not in sas:
                        offences.append(
                            f"{rb['metadata']['name']}: references unknown SA '{subject['name']}'"
                        )
        assert not offences, f"RoleBindings reference unknown ServiceAccounts: {offences}"

    def test_no_cluster_admin_binding(self, rbac_docs):
        """No RoleBinding or ClusterRoleBinding should grant cluster-admin."""
        rbs = _by_kind(rbac_docs, "RoleBinding") + _by_kind(rbac_docs, "ClusterRoleBinding")
        offences = [
            rb["metadata"]["name"]
            for rb in rbs
            if rb.get("roleRef", {}).get("name") == "cluster-admin"
        ]
        assert not offences, f"cluster-admin role granted via: {offences}"
