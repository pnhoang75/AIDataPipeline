"""Security tests — §7.3 NFS path traversal, Pro quota fix, start_paused schema.

Tests validate:
- browse_source rejects paths outside allowed_path_prefix with 400 PATH_TRAVERSAL_BLOCKED
- browse_source accepts valid subpaths and the exact prefix
- Pro license tokens bypass CONNECTOR_COUNT quota check entirely
- start_paused field is accepted by UserSourceCreate and propagated to the CR spec
"""
import json
import os
import sys
from unittest.mock import AsyncMock, call, patch

import pytest

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_BFF_SRC = os.path.join(_ROOT, "services", "bff", "src")
sys.path.insert(0, _BFF_SRC)

from app import app  # noqa: E402
from fastapi.testclient import TestClient

client = TestClient(app, raise_server_exceptions=False)

_AUTH = {"Authorization": "Bearer valid.token"}

_FREE_CLAIMS = {
    "sub": "user-alice",
    "email": "alice@acme.com",
    "org_id": "tenant-abc",
    "org_name": "acme",
    "license_type": "free",
    "quota_tier": "free",
    "roles": ["developer"],
}

_PRO_CLAIMS = {**_FREE_CLAIMS, "license_type": "pro", "quota_tier": "pro"}

_ENTERPRISE_CLAIMS = {**_FREE_CLAIMS, "license_type": "enterprise", "quota_tier": "enterprise"}

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

_S3_CM = {
    "name": "connector-s3-1",
    "data": {
        "id": "s3-1",
        "name": "my-s3",
        "source_type": "s3",
        "config": "{}",
        "tenant_id": "tenant-abc",
        "owner_id": "user-alice",
        "start_paused": "false",
    },
    "labels": {"tenant-id": "tenant-abc"},
}


# ── §7.3 NFS Path Traversal — GET /api/sources/{id}/browse/{path} ─────────────

@pytest.mark.parametrize("encoded_path,desc", [
    # Relative traversal sequences
    ("..%2F..%2Fetc%2Fpasswd", "relative ../../etc/passwd"),
    # Sibling tenant path (outside prefix)
    ("%2Fexports%2Fother-tenant%2Fsecret", "sibling tenant /exports/other-tenant/secret"),
    # Traversal via .. after the prefix
    ("%2Fexports%2Facme%2F..%2Fother-tenant", "traversal /exports/acme/../other-tenant"),
    # Absolute path completely outside prefix
    ("%2Fetc%2Fpasswd", "absolute /etc/passwd outside prefix"),
    # Double-encoded traversal
    ("%2Fexports%2Facme%2F..%252F..%2Fetc", "double-encoded traversal"),
])
def test_nfs_path_traversal_blocked(encoded_path, desc):
    with patch("auth.decode_token", return_value=_FREE_CLAIMS), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=_NFS_CM)):
        resp = client.get(
            f"/api/sources/nfs-1/browse/{encoded_path}",
            headers=_AUTH,
        )
    assert resp.status_code == 400, f"Expected 400 for {desc}: got {resp.status_code}"
    body = resp.json()
    assert body["error"] == "PATH_TRAVERSAL_BLOCKED", (
        f"Expected PATH_TRAVERSAL_BLOCKED for {desc}: got {body}"
    )


@pytest.mark.parametrize("encoded_path,desc", [
    ("%2Fexports%2Facme%2Freports", "valid subpath /exports/acme/reports"),
    ("%2Fexports%2Facme", "exact prefix /exports/acme"),
    ("%2Fexports%2Facme%2F2024%2Fq1", "nested subpath /exports/acme/2024/q1"),
])
def test_nfs_path_valid(encoded_path, desc):
    listing = [{"name": "file.txt", "size": 1024}]
    with patch("auth.decode_token", return_value=_FREE_CLAIMS), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=_NFS_CM)), \
         patch("k8s_client.browse_nfs_path", AsyncMock(return_value=listing)):
        resp = client.get(
            f"/api/sources/nfs-1/browse/{encoded_path}",
            headers=_AUTH,
        )
    assert resp.status_code == 200, f"Expected 200 for {desc}: got {resp.status_code}"
    assert resp.json()["entries"] == listing


def test_nfs_browse_requires_auth():
    resp = client.get("/api/sources/nfs-1/browse/%2Fexports%2Facme")
    assert resp.status_code == 401


def test_nfs_browse_returns_path_and_entries():
    listing = [{"name": "report.pdf"}]
    with patch("auth.decode_token", return_value=_FREE_CLAIMS), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=_NFS_CM)), \
         patch("k8s_client.browse_nfs_path", AsyncMock(return_value=listing)):
        resp = client.get(
            "/api/sources/nfs-1/browse/%2Fexports%2Facme%2Freports",
            headers=_AUTH,
        )
    assert resp.status_code == 200
    data = resp.json()
    assert "entries" in data
    assert data["entries"] == listing


# ── Pro quota fix — POST /api/sources/create ─────────────────────────────────

def test_pro_user_creates_connector_without_quota_check():
    """Pro license must skip CheckQuota(CONNECTOR_COUNT) entirely."""
    quota_spy = AsyncMock(return_value={"allowed": True, "status": "UNLIMITED"})
    cm_mock = AsyncMock(return_value=_S3_CM)
    cr_mock = AsyncMock(return_value={})
    with patch("auth.decode_token", return_value=_PRO_CLAIMS), \
         patch("quota_client.check_quota", quota_spy), \
         patch("k8s_client.create_configmap", cm_mock), \
         patch("k8s_client.create_custom_object", cr_mock):
        resp = client.post(
            "/api/sources/create",
            json={"name": "pro-s3", "source_type": "s3"},
            headers=_AUTH,
        )
    assert resp.status_code == 201
    # Pro tier must NOT call quota check — it is unconditionally unlimited
    quota_spy.assert_not_called()


def test_enterprise_user_creates_connector_without_quota_check():
    """Enterprise license must skip CheckQuota(CONNECTOR_COUNT) entirely."""
    quota_spy = AsyncMock(return_value={"allowed": True, "status": "UNLIMITED"})
    cm_mock = AsyncMock(return_value=_S3_CM)
    cr_mock = AsyncMock(return_value={})
    with patch("auth.decode_token", return_value=_ENTERPRISE_CLAIMS), \
         patch("quota_client.check_quota", quota_spy), \
         patch("k8s_client.create_configmap", cm_mock), \
         patch("k8s_client.create_custom_object", cr_mock):
        resp = client.post(
            "/api/sources/create",
            json={"name": "ent-s3", "source_type": "s3"},
            headers=_AUTH,
        )
    assert resp.status_code == 201
    quota_spy.assert_not_called()


def test_free_user_connector_quota_enforced():
    """Free tier must still call quota check and respect DENIED."""
    quota_denied = AsyncMock(return_value={"allowed": False, "status": "DENIED"})
    with patch("auth.decode_token", return_value=_FREE_CLAIMS), \
         patch("quota_client.check_quota", quota_denied):
        resp = client.post(
            "/api/sources/create",
            json={"name": "free-s3", "source_type": "s3"},
            headers=_AUTH,
        )
    assert resp.status_code == 402
    assert resp.json()["error"] == "QUOTA_EXCEEDED"
    quota_denied.assert_called_once_with("tenant-abc", "CONNECTOR_COUNT")


def test_free_user_connector_quota_allowed():
    """Free tier connector is created when quota is below limit."""
    quota_ok = AsyncMock(return_value={"allowed": True, "status": "ALLOWED"})
    cm_mock = AsyncMock(return_value=_S3_CM)
    cr_mock = AsyncMock(return_value={})
    with patch("auth.decode_token", return_value=_FREE_CLAIMS), \
         patch("quota_client.check_quota", quota_ok), \
         patch("k8s_client.create_configmap", cm_mock), \
         patch("k8s_client.create_custom_object", cr_mock):
        resp = client.post(
            "/api/sources/create",
            json={"name": "free-s3", "source_type": "s3"},
            headers=_AUTH,
        )
    assert resp.status_code == 201


# ── start_paused field — POST /api/sources/create ────────────────────────────

def test_start_paused_default_false():
    """Omitting start_paused defaults to false; CR spec.startPaused is False."""
    quota_ok = AsyncMock(return_value={"allowed": True, "status": "ALLOWED"})
    cm_mock = AsyncMock(return_value=_S3_CM)
    cr_mock = AsyncMock(return_value={})
    with patch("auth.decode_token", return_value=_FREE_CLAIMS), \
         patch("quota_client.check_quota", quota_ok), \
         patch("k8s_client.create_configmap", cm_mock), \
         patch("k8s_client.create_custom_object", cr_mock):
        resp = client.post(
            "/api/sources/create",
            json={"name": "src", "source_type": "s3"},
            headers=_AUTH,
        )
    assert resp.status_code == 201
    assert resp.json()["start_paused"] is False
    _, _, cm_data, _ = cm_mock.call_args[0]
    assert cm_data["start_paused"] == "false"
    cr_body = cr_mock.call_args[0][4]
    assert cr_body["spec"]["startPaused"] is False


def test_start_paused_true_propagated_to_cr():
    """start_paused=true must appear in ConfigMap data and CR spec.startPaused."""
    quota_ok = AsyncMock(return_value={"allowed": True, "status": "ALLOWED"})
    cm_mock = AsyncMock(return_value=_S3_CM)
    cr_mock = AsyncMock(return_value={})
    with patch("auth.decode_token", return_value=_FREE_CLAIMS), \
         patch("quota_client.check_quota", quota_ok), \
         patch("k8s_client.create_configmap", cm_mock), \
         patch("k8s_client.create_custom_object", cr_mock):
        resp = client.post(
            "/api/sources/create",
            json={"name": "paused-src", "source_type": "s3", "start_paused": True},
            headers=_AUTH,
        )
    assert resp.status_code == 201
    assert resp.json()["start_paused"] is True
    _, _, cm_data, _ = cm_mock.call_args[0]
    assert cm_data["start_paused"] == "true"
    cr_body = cr_mock.call_args[0][4]
    assert cr_body["spec"]["startPaused"] is True


def test_start_paused_schema_accepts_false_explicitly():
    """Explicitly passing start_paused=false is accepted."""
    quota_ok = AsyncMock(return_value={"allowed": True, "status": "ALLOWED"})
    cm_mock = AsyncMock(return_value=_S3_CM)
    cr_mock = AsyncMock(return_value={})
    with patch("auth.decode_token", return_value=_FREE_CLAIMS), \
         patch("quota_client.check_quota", quota_ok), \
         patch("k8s_client.create_configmap", cm_mock), \
         patch("k8s_client.create_custom_object", cr_mock):
        resp = client.post(
            "/api/sources/create",
            json={"name": "active-src", "source_type": "s3", "start_paused": False},
            headers=_AUTH,
        )
    assert resp.status_code == 201
    assert resp.json()["start_paused"] is False
