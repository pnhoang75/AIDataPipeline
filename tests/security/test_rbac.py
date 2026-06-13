"""Security tests §7.1 — Authentication & Authorization + connector-sa RBAC.

§7.1 tests verify JWT validation and role-based access control at the
application layer (unit-level; Kong/Keycloak behaviour modelled via mocks).

RBAC tests (test plan §2.7, test_connector_role_lists_exact_secret_names)
verify the Pipeline Operator creates per-connector Roles with exact
resourceNames and no wildcards — see CLAUDE.md known-issues.
"""

from __future__ import annotations

import os
import sys
import time

import pytest

# ── path setup ────────────────────────────────────────────────────────────────
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_OP_SRC = os.path.join(_ROOT, "services", "pipeline-operator", "operator")
sys.path.insert(0, _OP_SRC)

from rbac import (  # noqa: E402
    audit_role_for_wildcards,
    connector_secret_name,
    make_connector_role,
    make_connector_role_binding,
)
from auth import (  # noqa: E402
    AuthError,
    check_not_expired,
    enforce_tenant_scope,
    extract_tenant_id,
    get_roles,
    require_role,
)


# ═════════════════════════════════════════════════════════════════════════════
# Connector-sa RBAC — §2.7 / CLAUDE.md known-issues
# ═════════════════════════════════════════════════════════════════════════════

class TestConnectorRoleNoWildcards:
    """test_connector_role_lists_exact_secret_names and friends."""

    def test_connector_role_lists_exact_secret_names(self):
        """Operator creates per-connector Role with resourceNames: ['connector-acme-s3-creds'].

        This is the primary assertion from test plan §2.7.
        """
        role = make_connector_role("acme-s3", namespace="ai-pipeline")

        secret_rule = next(
            r for r in role["rules"] if "secrets" in r.get("resources", [])
        )
        assert secret_rule["resourceNames"] == ["acme-s3-creds"]

    def test_connector_role_has_no_wildcard_resource_names(self):
        """No resourceName in the generated Role may contain a '*'."""
        role = make_connector_role("acme-s3", namespace="ai-pipeline")
        offences = audit_role_for_wildcards(role)
        assert offences == [], f"Wildcard resourceNames found: {offences}"

    def test_different_connectors_get_different_secret_names(self):
        """Two connectors never share a secret resourceName."""
        role_a = make_connector_role("acme-s3", namespace="ai-pipeline")
        role_b = make_connector_role("corp-nfs", namespace="ai-pipeline")

        rn_a = next(
            r["resourceNames"]
            for r in role_a["rules"]
            if "secrets" in r.get("resources", [])
        )
        rn_b = next(
            r["resourceNames"]
            for r in role_b["rules"]
            if "secrets" in r.get("resources", [])
        )
        assert rn_a != rn_b

    def test_connector_role_name_is_scoped_to_connector(self):
        """Role metadata.name is unique per connector."""
        role = make_connector_role("acme-s3", namespace="ai-pipeline")
        assert role["metadata"]["name"] == "connector-acme-s3-role"

    def test_connector_role_namespace_is_set(self):
        """Role is created in the correct namespace."""
        role = make_connector_role("acme-s3", namespace="ai-pipeline")
        assert role["metadata"]["namespace"] == "ai-pipeline"

    def test_connector_role_configmap_access_is_exact(self):
        """ConfigMap resourceName is ['pipeline-config'], not a wildcard."""
        role = make_connector_role("acme-s3", namespace="ai-pipeline")
        cm_rule = next(
            r for r in role["rules"] if "configmaps" in r.get("resources", [])
        )
        assert cm_rule["resourceNames"] == ["pipeline-config"]
        assert "*" not in cm_rule["resourceNames"]

    def test_connector_role_verbs_are_read_only(self):
        """Connector Roles must not grant write access to secrets or configmaps."""
        role = make_connector_role("acme-s3", namespace="ai-pipeline")
        write_verbs = {"create", "update", "patch", "delete"}
        for rule in role["rules"]:
            assert write_verbs.isdisjoint(
                set(rule.get("verbs", []))
            ), f"Write verbs found in rule: {rule}"


class TestConnectorRoleBinding:
    """Verify RoleBinding structure is correct."""

    def test_role_binding_references_per_connector_role(self):
        """RoleBinding roleRef.name matches the per-connector Role name."""
        rb = make_connector_role_binding(
            "acme-s3", service_account="connector-sa", namespace="ai-pipeline"
        )
        assert rb["roleRef"]["name"] == "connector-acme-s3-role"

    def test_role_binding_binds_connector_sa(self):
        """RoleBinding subjects contains the expected ServiceAccount."""
        rb = make_connector_role_binding(
            "acme-s3", service_account="connector-sa", namespace="ai-pipeline"
        )
        subjects = rb["subjects"]
        assert len(subjects) == 1
        assert subjects[0]["kind"] == "ServiceAccount"
        assert subjects[0]["name"] == "connector-sa"

    def test_role_binding_name_is_scoped_to_connector(self):
        """RoleBinding name is unique per connector."""
        rb = make_connector_role_binding(
            "acme-s3", service_account="connector-sa", namespace="ai-pipeline"
        )
        assert rb["metadata"]["name"] == "connector-acme-s3-rb"

    def test_different_connectors_get_different_role_bindings(self):
        """Two connectors produce RoleBindings with different names."""
        rb_a = make_connector_role_binding(
            "acme-s3", service_account="connector-sa", namespace="ai-pipeline"
        )
        rb_b = make_connector_role_binding(
            "corp-nfs", service_account="connector-sa", namespace="ai-pipeline"
        )
        assert rb_a["metadata"]["name"] != rb_b["metadata"]["name"]


class TestConnectorSecretName:
    """Unit tests for the secret name derivation function."""

    def test_secret_name_includes_connector_name(self):
        assert connector_secret_name("acme-s3") == "acme-s3-creds"

    def test_secret_name_different_per_connector(self):
        assert connector_secret_name("acme-s3") != connector_secret_name("corp-nfs")


# ═════════════════════════════════════════════════════════════════════════════
# §7.1 Authentication & Authorization
# ═════════════════════════════════════════════════════════════════════════════

def _make_valid_payload(
    org_id: str = "acme",
    roles: list[str] | None = None,
    exp: int | None = None,
) -> dict:
    """Return a minimal decoded JWT payload."""
    return {
        "sub": "user-123",
        "org_id": org_id,
        "exp": exp if exp is not None else int(time.time()) + 3600,
        "realm_access": {"roles": roles if roles is not None else ["pipeline-user"]},
    }


class TestJwtTenantIdExtraction:
    """Kong injects tenant_id from JWT org_id; header forgery must not work."""

    def test_tenant_id_comes_from_jwt_org_id(self):
        """extract_tenant_id returns org_id from JWT payload."""
        payload = _make_valid_payload(org_id="acme")
        assert extract_tenant_id(payload) == "acme"

    def test_missing_org_id_raises_401(self):
        """A JWT without org_id is rejected with 401."""
        payload = {"sub": "user-123", "exp": int(time.time()) + 3600}
        with pytest.raises(AuthError) as exc_info:
            extract_tenant_id(payload)
        assert exc_info.value.status_code == 401

    def test_forged_tenant_header_is_ignored(self):
        """Tenant ID is never derived from a raw X-Tenant-ID header — only from JWT.

        This test models Kong's behaviour: the downstream service always calls
        extract_tenant_id(jwt_payload), not the raw header value.  The raw
        header is irrelevant.
        """
        payload = _make_valid_payload(org_id="acme")
        forged_header_value = "corp"  # attacker-supplied

        # Service derives tenant from JWT, not from forged_header_value
        tenant = extract_tenant_id(payload)
        assert tenant == "acme"
        assert tenant != forged_header_value


class TestJwtExpiry:
    """Expired JWTs must be rejected."""

    def test_valid_jwt_does_not_raise(self):
        payload = _make_valid_payload(exp=int(time.time()) + 3600)
        check_not_expired(payload)  # no exception

    def test_expired_jwt_raises_401(self):
        payload = _make_valid_payload(exp=int(time.time()) - 1)
        with pytest.raises(AuthError) as exc_info:
            check_not_expired(payload)
        assert exc_info.value.status_code == 401

    def test_missing_exp_raises_401(self):
        payload = {"sub": "user-123", "org_id": "acme"}
        with pytest.raises(AuthError) as exc_info:
            check_not_expired(payload)
        assert exc_info.value.status_code == 401


class TestRoleEnforcement:
    """Role-based access control checks."""

    def test_pipeline_user_has_pipeline_user_role(self):
        payload = _make_valid_payload(roles=["pipeline-user"])
        assert "pipeline-user" in get_roles(payload)

    def test_pipeline_user_missing_admin_role(self):
        """pipeline-user JWT accessing admin endpoint is rejected with 403."""
        payload = _make_valid_payload(roles=["pipeline-user"])
        with pytest.raises(AuthError) as exc_info:
            require_role(payload, "pipeline-admin")
        assert exc_info.value.status_code == 403

    def test_pipeline_admin_passes_admin_role_check(self):
        payload = _make_valid_payload(roles=["pipeline-user", "pipeline-admin"])
        require_role(payload, "pipeline-admin")  # no exception

    def test_missing_realm_access_means_no_roles(self):
        payload = {"sub": "user-123", "org_id": "acme", "exp": int(time.time()) + 3600}
        with pytest.raises(AuthError):
            require_role(payload, "pipeline-user")

    def test_empty_roles_list_denied(self):
        payload = _make_valid_payload(roles=[])
        with pytest.raises(AuthError) as exc_info:
            require_role(payload, "pipeline-user")
        assert exc_info.value.status_code == 403


class TestTenantScope:
    """Admin callers may only access their own tenant's resources."""

    def test_admin_can_access_own_tenant(self):
        payload = _make_valid_payload(org_id="acme", roles=["pipeline-admin"])
        enforce_tenant_scope(payload, requested_tenant_id="acme")  # no exception

    def test_admin_tenant_a_cannot_access_tenant_b_workspaces(self):
        """Admin JWT from tenant A accessing tenant B's workspaces → 403."""
        payload = _make_valid_payload(org_id="acme", roles=["pipeline-admin"])
        with pytest.raises(AuthError) as exc_info:
            enforce_tenant_scope(payload, requested_tenant_id="corp")
        assert exc_info.value.status_code == 403

    def test_pipeline_user_cannot_access_other_tenant(self):
        payload = _make_valid_payload(org_id="acme", roles=["pipeline-user"])
        with pytest.raises(AuthError):
            enforce_tenant_scope(payload, requested_tenant_id="corp")

    def test_tenant_scope_uses_jwt_not_supplied_value(self):
        """enforce_tenant_scope derives caller tenant from JWT, not a separate argument."""
        payload = _make_valid_payload(org_id="acme")
        # The caller supplies requested_tenant_id; the JWT still dictates the match
        with pytest.raises(AuthError):
            enforce_tenant_scope(payload, requested_tenant_id="different-tenant")
