"""Security tests — §7.2 SSRF, §7.3 NFS path traversal, §7.5 connector ownership.

Tests validate server-side enforcement of:
- RFC-1918 + loopback blocking on POST /api/sources/test
- Path traversal blocking on GET /api/sources/{id}/browse/{path}
- Connector ownership: only owner or tenant admin can delete
"""
import json
import os
import sys
from unittest.mock import AsyncMock, patch

import pytest

# ── path setup ────────────────────────────────────────────────────────────────
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_BFF_SRC = os.path.join(_ROOT, "services", "bff", "src")
sys.path.insert(0, _BFF_SRC)

from app import app  # noqa: E402
from fastapi.testclient import TestClient

client = TestClient(app, raise_server_exceptions=False)

_AUTH_HEADER = {"Authorization": "Bearer valid.token"}

_ALICE_CLAIMS = {
    "sub": "user-alice",
    "email": "alice@acme.com",
    "org_id": "tenant-abc",
    "org_name": "acme",
    "license_type": "free",
    "quota_tier": "free",
    "roles": ["developer"],
}
_BOB_CLAIMS = {**_ALICE_CLAIMS, "sub": "user-bob"}
_ADMIN_CLAIMS = {**_ALICE_CLAIMS, "sub": "admin-1", "roles": ["pipeline-admin"]}

_ALICE_CM = {
    "name": "connector-conn-alice",
    "data": {
        "id": "conn-alice",
        "name": "alice-s3",
        "source_type": "s3",
        "config": "{}",
        "tenant_id": "tenant-abc",
        "owner_id": "user-alice",
        "start_paused": "false",
    },
    "labels": {"tenant-id": "tenant-abc"},
}

_NFS_CM = {
    "name": "connector-nfs-1",
    "data": {
        "id": "nfs-1",
        "name": "acme-nfs",
        "source_type": "nfs",
        "config": json.dumps({"allowed_path_prefix": "/exports/acme"}),
        "tenant_id": "tenant-abc",
        "owner_id": "user-alice",
        "start_paused": "false",
    },
    "labels": {"tenant-id": "tenant-abc"},
}


# ── §7.2 SSRF — POST /api/sources/test ────────────────────────────────────────

@pytest.mark.parametrize("endpoint,desc", [
    ("postgresql://10.0.0.1:5432/db", "RFC-1918 10.x"),
    ("postgresql://172.16.0.1:5432/db", "RFC-1918 172.16.x"),
    ("postgresql://192.168.1.1:5432/db", "RFC-1918 192.168.x"),
    ("postgresql://127.0.0.1:5432/db", "loopback IPv4"),
    ("postgresql://[::1]:5432/db", "loopback IPv6"),
    ("postgresql://quota-db.infrastructure.svc:5432/db", "k8s internal .svc"),
    ("file:///etc/passwd", "file:// scheme"),
])
def test_ssrf_blocked(endpoint, desc):
    with patch("auth.decode_token", return_value=_ALICE_CLAIMS):
        resp = client.post(
            "/api/sources/test",
            json={"endpoint": endpoint},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 400, f"Expected 400 for {desc}: got {resp.status_code}"
    assert resp.json()["error"] == "SSRF_BLOCKED", f"Expected SSRF_BLOCKED for {desc}"


def test_ssrf_allows_public_ip():
    """Public IP 203.0.113.5 (TEST-NET-3) must pass the SSRF check."""
    with patch("auth.decode_token", return_value=_ALICE_CLAIMS):
        resp = client.post(
            "/api/sources/test",
            json={"endpoint": "postgresql://203.0.113.5:5432/db"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# ── §7.3 NFS path traversal — GET /api/sources/{id}/browse/{path} ─────────────

@pytest.mark.parametrize("encoded_path,desc", [
    ("..%2F..%2Fetc%2Fpasswd", "relative traversal ../../etc/passwd"),
    ("%2Fexports%2Fother-tenant%2Fsecret", "sibling tenant path"),
    ("%2Fexports%2Facme%2F..%2Fother-tenant", "traversal via .."),
])
def test_nfs_path_traversal_blocked(encoded_path, desc):
    with patch("auth.decode_token", return_value=_ALICE_CLAIMS), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=_NFS_CM)):
        resp = client.get(
            f"/api/sources/nfs-1/browse/{encoded_path}",
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 400, f"Expected 400 for {desc}: got {resp.status_code}"
    assert resp.json()["error"] == "PATH_TRAVERSAL_BLOCKED", f"Expected PATH_TRAVERSAL_BLOCKED for {desc}"


@pytest.mark.parametrize("encoded_path,desc", [
    ("%2Fexports%2Facme%2Freports", "valid subpath /exports/acme/reports"),
    ("%2Fexports%2Facme", "exact prefix /exports/acme"),
])
def test_nfs_path_valid(encoded_path, desc):
    listing = [{"name": "file.txt"}]
    with patch("auth.decode_token", return_value=_ALICE_CLAIMS), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=_NFS_CM)), \
         patch("k8s_client.browse_nfs_path", AsyncMock(return_value=listing)):
        resp = client.get(
            f"/api/sources/nfs-1/browse/{encoded_path}",
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 200, f"Expected 200 for {desc}: got {resp.status_code}"


# ── §7.5 Connector ownership ───────────────────────────────────────────────────

def test_user_cannot_delete_other_users_connector():
    """Bob (same tenant) cannot delete Alice's connector."""
    with patch("auth.decode_token", return_value=_BOB_CLAIMS), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=_ALICE_CM)):
        resp = client.delete("/api/sources/conn-alice", headers=_AUTH_HEADER)
    assert resp.status_code == 403
    assert resp.json()["error"] == "FORBIDDEN"


def test_tenant_admin_can_delete_any_connector_in_tenant():
    """Tenant admin can delete any connector regardless of owner."""
    with patch("auth.decode_token", return_value=_ADMIN_CLAIMS), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=_ALICE_CM)), \
         patch("k8s_client.delete_configmap", AsyncMock()), \
         patch("k8s_client.delete_custom_object", AsyncMock()):
        resp = client.delete("/api/sources/conn-alice", headers=_AUTH_HEADER)
    assert resp.status_code == 204


def test_cross_tenant_connector_deletion_blocked():
    """Connector from tenant-abc is invisible to tenant-xyz (returns 404)."""
    with patch("auth.decode_token", return_value={**_ALICE_CLAIMS, "org_id": "tenant-xyz"}), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=None)):
        resp = client.delete("/api/sources/conn-alice", headers=_AUTH_HEADER)
    assert resp.status_code == 404


def test_owner_can_delete_own_connector():
    """Alice can delete her own connector."""
    with patch("auth.decode_token", return_value=_ALICE_CLAIMS), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=_ALICE_CM)), \
         patch("k8s_client.delete_configmap", AsyncMock()), \
         patch("k8s_client.delete_custom_object", AsyncMock()):
        resp = client.delete("/api/sources/conn-alice", headers=_AUTH_HEADER)
    assert resp.status_code == 204
