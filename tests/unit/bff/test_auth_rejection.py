"""
Auth rejection unit tests — §7.1 of the test plan.

These tests exercise the BFF-layer auth and tenant-scoping enforcement. Kong-level
behaviors (e.g. /v1/query, header overwriting) are represented here as the equivalent
BFF-facing behavior: any protected endpoint without a valid JWT returns 401, and a
mismatched X-Tenant-ID returns 403.
"""
import os
import sys
from unittest.mock import AsyncMock, patch

import pytest
from jose import JWTError

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "services", "bff", "src"),
)

from app import app  # noqa: E402
from fastapi.testclient import TestClient

client = TestClient(app, raise_server_exceptions=False)

_ADMIN_CLAIMS = {
    "sub": "admin-1",
    "email": "admin@acme.com",
    "org_id": "tenant-acme",
    "org_name": "acme",
    "license_type": "pro",
    "quota_tier": "pro",
    "roles": ["pipeline-admin"],
}

_USER_CLAIMS = {
    "sub": "user-1",
    "email": "alice@acme.com",
    "org_id": "tenant-acme",
    "org_name": "acme",
    "license_type": "pro",
    "quota_tier": "pro",
    "roles": ["developer"],
}

_AUTH = {"Authorization": "Bearer valid.token"}


# ── §7.1 row 1: no JWT → 401 ──────────────────────────────────────────────────

def test_no_jwt_on_protected_endpoint_returns_401():
    """BFF rejects any request without an Authorization header with 401."""
    resp = client.get("/api/workspaces")
    assert resp.status_code == 401
    assert resp.json()["error"] == "MISSING_TOKEN"


def test_no_jwt_on_admin_endpoint_returns_401():
    resp = client.get("/api/admin/tenants")
    assert resp.status_code == 401
    assert resp.json()["error"] == "MISSING_TOKEN"


# ── §7.1 row 2: expired JWT → 401 ─────────────────────────────────────────────

def test_expired_jwt_returns_401():
    """Expired token is rejected with INVALID_TOKEN regardless of which endpoint is hit."""
    with patch("auth.decode_token", side_effect=JWTError("Signature has expired")):
        resp = client.get("/api/workspaces", headers=_AUTH)
    assert resp.status_code == 401
    body = resp.json()
    assert body["error"] == "INVALID_TOKEN"
    assert "expired" in body["message"].lower()


def test_expired_jwt_on_admin_endpoint_returns_401():
    with patch("auth.decode_token", side_effect=JWTError("Signature has expired")):
        resp = client.get("/api/admin/tenants", headers=_AUTH)
    assert resp.status_code == 401
    assert resp.json()["error"] == "INVALID_TOKEN"


# ── §7.1 row 3: wrong role on admin endpoint → 403 ───────────────────────────

def test_non_admin_role_on_admin_endpoint_returns_403():
    """Valid JWT with developer role cannot access admin endpoints."""
    with patch("auth.decode_token", return_value=_USER_CLAIMS):
        resp = client.get("/api/admin/pipeline/status", headers=_AUTH)
    assert resp.status_code == 403
    assert resp.json()["error"] == "FORBIDDEN"


def test_non_admin_role_on_connector_admin_endpoint_returns_403():
    with patch("auth.decode_token", return_value=_USER_CLAIMS):
        resp = client.get("/api/admin/connectors", headers=_AUTH)
    assert resp.status_code == 403
    assert resp.json()["error"] == "FORBIDDEN"


# ── §7.1 row 4: forged X-Tenant-ID → BFF enforces mismatch → 403 ─────────────

def test_mismatched_x_tenant_id_returns_403():
    """
    Kong overwrites X-Tenant-ID with the JWT's org_id in production.
    If a forged header slips through with a different value, BFF rejects it.
    """
    with patch("auth.decode_token", return_value=_USER_CLAIMS):
        resp = client.get(
            "/api/workspaces",
            headers={**_AUTH, "X-Tenant-ID": "tenant-evil"},
        )
    assert resp.status_code == 403
    assert resp.json()["error"] == "TENANT_MISMATCH"


def test_matching_x_tenant_id_is_accepted():
    """When Kong correctly sets X-Tenant-ID == JWT org_id, the request succeeds."""
    with patch("auth.decode_token", return_value=_USER_CLAIMS), \
         patch("db_client.get_workspaces", new_callable=AsyncMock, return_value=[]):
        resp = client.get(
            "/api/workspaces",
            headers={**_AUTH, "X-Tenant-ID": "tenant-acme"},
        )
    assert resp.status_code == 200


# ── §7.1 row 5: pipeline-user JWT on admin/tenants → 403 ─────────────────────

def test_pipeline_user_cannot_access_admin_tenants():
    """`pipeline-user` role is not `pipeline-admin`; BFF returns 403."""
    user_claims = {**_USER_CLAIMS, "roles": ["pipeline-user"]}
    with patch("auth.decode_token", return_value=user_claims):
        resp = client.get("/api/admin/tenants", headers=_AUTH)
    assert resp.status_code == 403
    assert resp.json()["error"] == "FORBIDDEN"


def test_pipeline_user_cannot_post_admin_tenants():
    user_claims = {**_USER_CLAIMS, "roles": ["pipeline-user"]}
    with patch("auth.decode_token", return_value=user_claims):
        resp = client.post("/api/admin/tenants", json={"name": "evil", "license_type": "pro"}, headers=_AUTH)
    assert resp.status_code == 403
    assert resp.json()["error"] == "FORBIDDEN"


def test_developer_role_cannot_access_admin_quota():
    with patch("auth.decode_token", return_value=_USER_CLAIMS):
        resp = client.get("/api/admin/quota", headers=_AUTH)
    assert resp.status_code == 403
    assert resp.json()["error"] == "FORBIDDEN"


# ── §7.1 row 6: admin JWT from tenant A cannot access tenant B's workspaces ──

def test_cross_tenant_workspace_access_returns_404():
    """
    An admin JWT from tenant-acme cannot see tenant-corp's workspace.
    The BFF scopes queries to the JWT's org_id; cross-tenant workspace IDs
    return 404 (no information leakage about the workspace's existence).
    """
    admin_a = {**_ADMIN_CLAIMS, "org_id": "tenant-acme"}
    with patch("auth.decode_token", return_value=admin_a), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=None):
        resp = client.get("/api/workspaces/ws-corp-123/files", headers=_AUTH)
    assert resp.status_code == 404


def test_cross_tenant_workspace_delete_returns_404():
    admin_a = {**_ADMIN_CLAIMS, "org_id": "tenant-acme"}
    with patch("auth.decode_token", return_value=admin_a), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=None):
        resp = client.delete("/api/workspaces/ws-corp-456", headers=_AUTH)
    assert resp.status_code == 404


def test_workspace_list_is_always_scoped_to_jwt_tenant():
    """Even an admin cannot list another tenant's workspaces; list is always JWT-scoped."""
    mock_list = AsyncMock(return_value=[])
    admin_a = {**_ADMIN_CLAIMS, "org_id": "tenant-acme"}
    with patch("auth.decode_token", return_value=admin_a), \
         patch("db_client.get_workspaces", mock_list):
        resp = client.get("/api/workspaces", headers=_AUTH)
    assert resp.status_code == 200
    # Verify query was scoped to tenant-acme, not any other tenant
    assert mock_list.call_args[0][0] == "tenant-acme"
