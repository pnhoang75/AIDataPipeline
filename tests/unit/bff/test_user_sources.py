"""Unit tests for BFF user-source endpoints (POST /sources/create, /test, /upload, etc.)."""
import io
import json
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "services", "bff", "src"),
)

from app import app  # noqa: E402
from fastapi.testclient import TestClient

client = TestClient(app, raise_server_exceptions=False)

_AUTH_HEADER = {"Authorization": "Bearer valid.token"}
_USER_CLAIMS = {
    "sub": "user-alice",
    "email": "alice@acme.com",
    "org_id": "tenant-abc",
    "org_name": "acme",
    "license_type": "free",
    "quota_tier": "free",
    "roles": ["developer"],
}
_ADMIN_CLAIMS = {**_USER_CLAIMS, "sub": "admin-1", "roles": ["pipeline-admin"]}
_PRO_CLAIMS = {**_USER_CLAIMS, "license_type": "pro", "quota_tier": "pro"}

_CONNECTOR_CM = {
    "name": "connector-conn-1",
    "data": {
        "id": "conn-1",
        "name": "my-s3",
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
        "name": "my-nfs",
        "source_type": "nfs",
        "config": json.dumps({"allowed_path_prefix": "/exports/acme"}),
        "tenant_id": "tenant-abc",
        "owner_id": "user-alice",
        "start_paused": "false",
    },
    "labels": {"tenant-id": "tenant-abc"},
}


def _mock_user(sub="user-alice", org_id="tenant-abc", claims=None):
    c = claims or {**_USER_CLAIMS, "sub": sub, "org_id": org_id}
    return patch("auth.decode_token", return_value=c)


def _mock_admin():
    return patch("auth.decode_token", return_value=_ADMIN_CLAIMS)


# ── POST /api/sources/create ───────────────────────────────────────────────────

def test_create_source_requires_auth():
    resp = client.post("/api/sources/create", json={"name": "x", "source_type": "s3"})
    assert resp.status_code == 401


def test_create_source_returns_201_on_quota_allowed():
    quota_ok = AsyncMock(return_value={"allowed": True, "status": "ALLOWED"})
    cm_mock = AsyncMock(return_value=_CONNECTOR_CM)
    cr_mock = AsyncMock(return_value={})
    with _mock_user(), \
         patch("quota_client.check_quota", quota_ok), \
         patch("k8s_client.create_configmap", cm_mock), \
         patch("k8s_client.create_custom_object", cr_mock):
        resp = client.post(
            "/api/sources/create",
            json={"name": "my-s3", "source_type": "s3"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "my-s3"
    assert data["source_type"] == "s3"
    assert data["tenant_id"] == "tenant-abc"
    assert data["status"] == "provisioning"


def test_create_source_quota_exceeded_returns_402():
    quota_denied = AsyncMock(return_value={"allowed": False, "status": "DENIED"})
    with _mock_user(), patch("quota_client.check_quota", quota_denied):
        resp = client.post(
            "/api/sources/create",
            json={"name": "x", "source_type": "s3"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 402
    assert resp.json()["error"] == "QUOTA_EXCEEDED"


def test_create_source_stores_owner_id():
    quota_ok = AsyncMock(return_value={"allowed": True, "status": "ALLOWED"})
    cm_mock = AsyncMock(return_value=_CONNECTOR_CM)
    cr_mock = AsyncMock(return_value={})
    with _mock_user(sub="user-alice"), \
         patch("quota_client.check_quota", quota_ok), \
         patch("k8s_client.create_configmap", cm_mock), \
         patch("k8s_client.create_custom_object", cr_mock):
        client.post(
            "/api/sources/create",
            json={"name": "x", "source_type": "s3"},
            headers=_AUTH_HEADER,
        )
    _, _, call_data, _ = cm_mock.call_args[0]
    assert call_data["owner_id"] == "user-alice"
    assert call_data["tenant_id"] == "tenant-abc"


def test_create_source_start_paused_field():
    quota_ok = AsyncMock(return_value={"allowed": True, "status": "ALLOWED"})
    cm_mock = AsyncMock(return_value=_CONNECTOR_CM)
    cr_mock = AsyncMock(return_value={})
    with _mock_user(), \
         patch("quota_client.check_quota", quota_ok), \
         patch("k8s_client.create_configmap", cm_mock), \
         patch("k8s_client.create_custom_object", cr_mock):
        resp = client.post(
            "/api/sources/create",
            json={"name": "paused-src", "source_type": "s3", "start_paused": True},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 201
    assert resp.json()["start_paused"] is True
    _, _, call_data, _ = cm_mock.call_args[0]
    assert call_data["start_paused"] == "true"


def test_create_source_pro_quota_unlimited():
    """Pro-tier quota returns UNLIMITED; connector is created without error."""
    quota_unlimited = AsyncMock(return_value={"allowed": True, "status": "UNLIMITED"})
    cm_mock = AsyncMock(return_value=_CONNECTOR_CM)
    cr_mock = AsyncMock(return_value={})
    with patch("auth.decode_token", return_value=_PRO_CLAIMS), \
         patch("quota_client.check_quota", quota_unlimited), \
         patch("k8s_client.create_configmap", cm_mock), \
         patch("k8s_client.create_custom_object", cr_mock):
        resp = client.post(
            "/api/sources/create",
            json={"name": "pro-src", "source_type": "s3"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 201


# ── POST /api/sources/test ─────────────────────────────────────────────────────

def test_source_test_requires_auth():
    resp = client.post("/api/sources/test", json={"endpoint": "postgresql://203.0.113.5/db"})
    assert resp.status_code == 401


def test_source_test_allows_public_ip():
    with _mock_user():
        resp = client.post(
            "/api/sources/test",
            json={"endpoint": "postgresql://203.0.113.5:5432/db"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_source_test_blocks_10_x():
    with _mock_user():
        resp = client.post(
            "/api/sources/test",
            json={"endpoint": "postgresql://10.0.0.1:5432/db"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 400
    assert resp.json()["error"] == "SSRF_BLOCKED"


def test_source_test_blocks_172_16():
    with _mock_user():
        resp = client.post(
            "/api/sources/test",
            json={"endpoint": "postgresql://172.16.0.1:5432/db"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 400
    assert resp.json()["error"] == "SSRF_BLOCKED"


def test_source_test_blocks_192_168():
    with _mock_user():
        resp = client.post(
            "/api/sources/test",
            json={"endpoint": "postgresql://192.168.1.1:5432/db"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 400
    assert resp.json()["error"] == "SSRF_BLOCKED"


def test_source_test_blocks_loopback():
    with _mock_user():
        resp = client.post(
            "/api/sources/test",
            json={"endpoint": "postgresql://127.0.0.1:5432/db"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 400
    assert resp.json()["error"] == "SSRF_BLOCKED"


def test_source_test_blocks_ipv6_loopback():
    with _mock_user():
        resp = client.post(
            "/api/sources/test",
            json={"endpoint": "postgresql://[::1]:5432/db"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 400
    assert resp.json()["error"] == "SSRF_BLOCKED"


def test_source_test_blocks_k8s_svc():
    with _mock_user():
        resp = client.post(
            "/api/sources/test",
            json={"endpoint": "postgresql://quota-db.infrastructure.svc:5432/db"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 400
    assert resp.json()["error"] == "SSRF_BLOCKED"


def test_source_test_blocks_file_scheme():
    with _mock_user():
        resp = client.post(
            "/api/sources/test",
            json={"endpoint": "file:///etc/passwd"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 400
    assert resp.json()["error"] == "SSRF_BLOCKED"


# ── POST /api/sources/upload ───────────────────────────────────────────────────

def test_upload_requires_auth():
    resp = client.post(
        "/api/sources/upload",
        files={"file": ("doc.pdf", b"content", "application/pdf")},
    )
    assert resp.status_code == 401


def test_upload_returns_201_and_publishes_event():
    minio_mock = AsyncMock(return_value="tenant-abc/uploads/session-1/doc.pdf")
    kafka_mock = AsyncMock(return_value=None)
    with _mock_user(), \
         patch("minio_client.upload_file", minio_mock), \
         patch("kafka_client.publish_event", kafka_mock):
        resp = client.post(
            "/api/sources/upload",
            files={"file": ("doc.pdf", b"pdf content", "application/pdf")},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == "uploaded"
    assert "session_id" in data


def test_upload_skips_quota_check():
    """Upload must NOT call quota_client.check_quota."""
    minio_mock = AsyncMock(return_value="tenant-abc/uploads/s/f.pdf")
    kafka_mock = AsyncMock(return_value=None)
    quota_spy = AsyncMock(return_value={"allowed": True, "status": "ALLOWED"})
    with _mock_user(), \
         patch("minio_client.upload_file", minio_mock), \
         patch("kafka_client.publish_event", kafka_mock), \
         patch("quota_client.check_quota", quota_spy):
        client.post(
            "/api/sources/upload",
            files={"file": ("f.pdf", b"data", "application/pdf")},
            headers=_AUTH_HEADER,
        )
    quota_spy.assert_not_called()


def test_upload_publishes_datasource_metadata_event():
    minio_mock = AsyncMock(return_value="tenant-abc/uploads/session-1/doc.pdf")
    kafka_mock = AsyncMock(return_value=None)
    with _mock_user(), \
         patch("minio_client.upload_file", minio_mock), \
         patch("kafka_client.publish_event", kafka_mock):
        client.post(
            "/api/sources/upload",
            files={"file": ("doc.pdf", b"data", "application/pdf")},
            headers=_AUTH_HEADER,
        )
    kafka_mock.assert_awaited_once()
    topic, payload = kafka_mock.call_args[0]
    assert topic == "metadata-events"
    assert payload["entity_type"] == "DataSource"
    assert payload["source_type"] == "upload"
    assert payload["tenant_id"] == "tenant-abc"


# ── Pause / Resume ─────────────────────────────────────────────────────────────

def test_pause_source_owner_succeeds():
    patch_mock = AsyncMock(return_value={"data": {**_CONNECTOR_CM["data"], "start_paused": "true"}})
    with _mock_user(sub="user-alice"), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=_CONNECTOR_CM)), \
         patch("k8s_client.patch_configmap", patch_mock):
        resp = client.post("/api/sources/conn-1/pause", headers=_AUTH_HEADER)
    assert resp.status_code == 200
    assert resp.json()["status"] == "paused"


def test_pause_source_non_owner_rejected():
    with _mock_user(sub="user-bob"), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=_CONNECTOR_CM)):
        resp = client.post("/api/sources/conn-1/pause", headers=_AUTH_HEADER)
    assert resp.status_code == 403
    assert resp.json()["error"] == "FORBIDDEN"


def test_resume_source_owner_succeeds():
    patch_mock = AsyncMock(return_value={"data": {**_CONNECTOR_CM["data"], "start_paused": "false"}})
    with _mock_user(sub="user-alice"), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=_CONNECTOR_CM)), \
         patch("k8s_client.patch_configmap", patch_mock):
        resp = client.post("/api/sources/conn-1/resume", headers=_AUTH_HEADER)
    assert resp.status_code == 200
    assert resp.json()["status"] == "running"


# ── DELETE /api/sources/{id} ───────────────────────────────────────────────────

def test_delete_source_requires_auth():
    resp = client.delete("/api/sources/conn-1")
    assert resp.status_code == 401


def test_delete_source_owner_returns_204():
    with _mock_user(sub="user-alice"), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=_CONNECTOR_CM)), \
         patch("k8s_client.delete_configmap", AsyncMock()), \
         patch("k8s_client.delete_custom_object", AsyncMock()):
        resp = client.delete("/api/sources/conn-1", headers=_AUTH_HEADER)
    assert resp.status_code == 204


def test_delete_source_non_owner_returns_403():
    with _mock_user(sub="user-bob"), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=_CONNECTOR_CM)):
        resp = client.delete("/api/sources/conn-1", headers=_AUTH_HEADER)
    assert resp.status_code == 403
    assert resp.json()["error"] == "FORBIDDEN"


def test_delete_source_not_found_returns_404():
    with _mock_user(), patch("k8s_client.get_configmap", AsyncMock(return_value=None)):
        resp = client.delete("/api/sources/missing", headers=_AUTH_HEADER)
    assert resp.status_code == 404


def test_delete_source_admin_can_delete_any():
    with _mock_admin(), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=_CONNECTOR_CM)), \
         patch("k8s_client.delete_configmap", AsyncMock()), \
         patch("k8s_client.delete_custom_object", AsyncMock()):
        resp = client.delete("/api/sources/conn-1", headers=_AUTH_HEADER)
    assert resp.status_code == 204


def test_delete_source_wrong_tenant_returns_404():
    with _mock_user(org_id="tenant-xyz"), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=None)):
        resp = client.delete("/api/sources/conn-1", headers=_AUTH_HEADER)
    assert resp.status_code == 404


# ── GET /api/sources/{id}/browse ──────────────────────────────────────────────

def test_browse_valid_path_returns_200():
    listing = [{"name": "report.pdf"}]
    with _mock_user(), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=_NFS_CM)), \
         patch("k8s_client.browse_nfs_path", AsyncMock(return_value=listing)):
        resp = client.get(
            "/api/sources/nfs-1/browse/%2Fexports%2Facme%2Freports",
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 200
    data = resp.json()
    assert data["entries"] == listing


def test_browse_traversal_blocked():
    with _mock_user(), \
         patch("k8s_client.get_configmap", AsyncMock(return_value=_NFS_CM)):
        resp = client.get(
            "/api/sources/nfs-1/browse/..%2F..%2Fetc%2Fpasswd",
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 400
    assert resp.json()["error"] == "PATH_TRAVERSAL_BLOCKED"


def test_browse_requires_auth():
    resp = client.get("/api/sources/nfs-1/browse/exports/acme")
    assert resp.status_code == 401
