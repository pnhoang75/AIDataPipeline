import os
import sys
from unittest.mock import patch

import pytest
from jose import JWTError

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "services", "bff", "src"),
)

from app import app  # noqa: E402 — path must be inserted first
from fastapi.testclient import TestClient

client = TestClient(app, raise_server_exceptions=False)


def _claims(
    sub="user-1",
    email="alice@acme.com",
    org_id="tenant-abc",
    org_name="acme",
    license_type="pro",
    quota_tier="pro",
    roles=None,
) -> dict:
    return {
        "sub": sub,
        "email": email,
        "org_id": org_id,
        "org_name": org_name,
        "license_type": license_type,
        "quota_tier": quota_tier,
        "roles": roles if roles is not None else ["developer"],
    }


# ─── Missing / malformed Authorization header ─────────────────────────────────

def test_no_auth_header_returns_401():
    resp = client.get("/api/whoami")
    assert resp.status_code == 401
    body = resp.json()
    assert body["error"] == "MISSING_TOKEN"


def test_basic_auth_scheme_returns_401():
    resp = client.get("/api/whoami", headers={"Authorization": "Basic dXNlcjpwYXNz"})
    assert resp.status_code == 401
    assert resp.json()["error"] == "MISSING_TOKEN"


def test_bearer_with_no_token_returns_401():
    resp = client.get("/api/whoami", headers={"Authorization": "Bearer"})
    assert resp.status_code == 401
    assert resp.json()["error"] == "MISSING_TOKEN"


# ─── Invalid / expired JWT ────────────────────────────────────────────────────

def test_invalid_jwt_returns_401():
    with patch("auth.decode_token", side_effect=JWTError("signature verification failed")):
        resp = client.get("/api/whoami", headers={"Authorization": "Bearer garbage.token.here"})
    assert resp.status_code == 401
    assert resp.json()["error"] == "INVALID_TOKEN"


def test_expired_jwt_returns_401():
    with patch("auth.decode_token", side_effect=JWTError("Signature has expired")):
        resp = client.get("/api/whoami", headers={"Authorization": "Bearer expired.token.here"})
    assert resp.status_code == 401
    body = resp.json()
    assert body["error"] == "INVALID_TOKEN"
    assert "expired" in body["message"].lower()


# ─── Missing org_id claim ─────────────────────────────────────────────────────

def test_jwt_missing_org_id_returns_401():
    claims_without_org = {k: v for k, v in _claims().items() if k != "org_id"}
    with patch("auth.decode_token", return_value=claims_without_org):
        resp = client.get("/api/whoami", headers={"Authorization": "Bearer valid.but.no.org"})
    assert resp.status_code == 401
    assert resp.json()["error"] == "MISSING_ORG_ID"


# ─── Valid JWT — happy paths ───────────────────────────────────────────────────

def test_valid_jwt_no_tenant_header_succeeds():
    with patch("auth.decode_token", return_value=_claims()):
        resp = client.get("/api/whoami", headers={"Authorization": "Bearer valid.token"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["org_id"] == "tenant-abc"
    assert data["email"] == "alice@acme.com"


def test_valid_jwt_matching_tenant_header_succeeds():
    with patch("auth.decode_token", return_value=_claims()):
        resp = client.get(
            "/api/whoami",
            headers={
                "Authorization": "Bearer valid.token",
                "X-Tenant-ID": "tenant-abc",
            },
        )
    assert resp.status_code == 200
    assert resp.json()["org_id"] == "tenant-abc"


# ─── Tenant scoping enforcement ───────────────────────────────────────────────

def test_tenant_id_mismatch_returns_403():
    """X-Tenant-ID injected by Kong must match the JWT's org_id."""
    with patch("auth.decode_token", return_value=_claims(org_id="tenant-abc")):
        resp = client.get(
            "/api/whoami",
            headers={
                "Authorization": "Bearer valid.token",
                "X-Tenant-ID": "tenant-other",
            },
        )
    assert resp.status_code == 403
    assert resp.json()["error"] == "TENANT_MISMATCH"


def test_tenant_scoping_org_id_comes_from_jwt():
    """org_id in the response is always derived from JWT, not from any request field."""
    with patch("auth.decode_token", return_value=_claims(org_id="tenant-xyz")):
        resp = client.get("/api/whoami", headers={"Authorization": "Bearer valid.token"})
    assert resp.status_code == 200
    assert resp.json()["org_id"] == "tenant-xyz"


# ─── Role-based access control ────────────────────────────────────────────────

def test_admin_endpoint_without_admin_role_returns_403():
    with patch("auth.decode_token", return_value=_claims(roles=["developer"])):
        resp = client.get(
            "/api/admin/health",
            headers={"Authorization": "Bearer valid.token"},
        )
    assert resp.status_code == 403
    assert resp.json()["error"] == "FORBIDDEN"


def test_admin_endpoint_with_admin_role_succeeds():
    with patch("auth.decode_token", return_value=_claims(roles=["pipeline-admin"])):
        resp = client.get(
            "/api/admin/health",
            headers={"Authorization": "Bearer valid.token"},
        )
    assert resp.status_code == 200
    assert resp.json()["tenant"] == "tenant-abc"


def test_admin_endpoint_with_multiple_roles_including_admin_succeeds():
    with patch("auth.decode_token", return_value=_claims(roles=["developer", "pipeline-admin"])):
        resp = client.get(
            "/api/admin/health",
            headers={"Authorization": "Bearer valid.token"},
        )
    assert resp.status_code == 200


# ─── Error envelope ───────────────────────────────────────────────────────────

def test_error_envelope_includes_request_id():
    resp = client.get("/api/whoami")
    body = resp.json()
    assert "request_id" in body
    assert body["request_id"] is not None


def test_health_endpoint_requires_no_auth():
    resp = client.get("/api/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
