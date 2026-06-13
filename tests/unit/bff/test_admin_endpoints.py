import os
import sys
from unittest.mock import AsyncMock, patch

import pytest

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
    "org_id": "tenant-abc",
    "org_name": "acme",
    "license_type": "pro",
    "quota_tier": "pro",
    "roles": ["pipeline-admin"],
}
_USER_CLAIMS = {**_ADMIN_CLAIMS, "roles": ["developer"]}
_AUTH_HEADER = {"Authorization": "Bearer valid.token"}


def _mock_admin(org_id: str = "tenant-abc"):
    return patch("auth.decode_token", return_value={**_ADMIN_CLAIMS, "org_id": org_id})


def _mock_user():
    return patch("auth.decode_token", return_value=_USER_CLAIMS)


def _existing_cm(
    connector_id: str = "cid1",
    tenant_id: str = "tenant-abc",
    source_type: str = "s3",
    name: str = "original-name",
) -> dict:
    return {
        "name": f"connector-{connector_id}",
        "data": {
            "id": connector_id,
            "name": name,
            "source_type": source_type,
            "config": "{}",
            "tenant_id": tenant_id,
            "start_paused": "false",
        },
        "labels": {},
    }


# ── Pipeline status ───────────────────────────────────────────────────────────

def test_pipeline_status_returns_pod_list():
    pods = [
        {"name": "connector-s3-abc", "status": "Running", "ready": True},
        {"name": "embedding-worker-xyz", "status": "Pending", "ready": False},
    ]
    with _mock_admin(), patch("k8s_client.list_pods", new_callable=AsyncMock, return_value=pods):
        resp = client.get("/api/admin/pipeline/status", headers=_AUTH_HEADER)
    assert resp.status_code == 200
    data = resp.json()
    assert data["tenant"] == "tenant-abc"
    assert len(data["services"]) == 2
    assert data["services"][0]["name"] == "connector-s3-abc"
    assert data["services"][0]["status"] == "Running"
    assert data["services"][0]["ready"] is True
    assert data["services"][1]["ready"] is False


def test_pipeline_status_non_admin_returns_403():
    with _mock_user():
        resp = client.get("/api/admin/pipeline/status", headers=_AUTH_HEADER)
    assert resp.status_code == 403
    assert resp.json()["error"] == "FORBIDDEN"


def test_pipeline_status_requires_auth():
    resp = client.get("/api/admin/pipeline/status")
    assert resp.status_code == 401


def test_pipeline_status_empty_cluster():
    with _mock_admin(), patch("k8s_client.list_pods", new_callable=AsyncMock, return_value=[]):
        resp = client.get("/api/admin/pipeline/status", headers=_AUTH_HEADER)
    assert resp.status_code == 200
    assert resp.json()["services"] == []


# ── List connectors ───────────────────────────────────────────────────────────

def test_list_connectors_returns_tenant_connectors():
    cms = [
        {
            "name": "connector-id1",
            "data": {
                "id": "id1",
                "name": "my-s3",
                "source_type": "s3",
                "config": '{"bucket": "mybucket"}',
                "tenant_id": "tenant-abc",
                "start_paused": "false",
            },
            "labels": {"tenant-id": "tenant-abc"},
        }
    ]
    with _mock_admin(), patch("k8s_client.list_configmaps", new_callable=AsyncMock, return_value=cms):
        resp = client.get("/api/admin/connectors", headers=_AUTH_HEADER)
    assert resp.status_code == 200
    items = resp.json()
    assert len(items) == 1
    assert items[0]["id"] == "id1"
    assert items[0]["name"] == "my-s3"
    assert items[0]["source_type"] == "s3"
    assert items[0]["config"] == {"bucket": "mybucket"}
    assert items[0]["tenant_id"] == "tenant-abc"
    assert items[0]["start_paused"] is False


def test_list_connectors_empty_returns_empty_list():
    with _mock_admin(), patch("k8s_client.list_configmaps", new_callable=AsyncMock, return_value=[]):
        resp = client.get("/api/admin/connectors", headers=_AUTH_HEADER)
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_connectors_non_admin_returns_403():
    with _mock_user():
        resp = client.get("/api/admin/connectors", headers=_AUTH_HEADER)
    assert resp.status_code == 403


def test_list_connectors_uses_tenant_label_selector():
    mock_list = AsyncMock(return_value=[])
    with _mock_admin(org_id="tenant-xyz"), patch("k8s_client.list_configmaps", mock_list):
        client.get("/api/admin/connectors", headers=_AUTH_HEADER)
    call_kwargs = mock_list.call_args
    label_sel = call_kwargs[1].get("label_selector") or call_kwargs[0][1]
    assert "tenant-xyz" in label_sel


# ── Create connector ──────────────────────────────────────────────────────────

def test_create_connector_returns_201():
    mock_cm = AsyncMock(return_value={})
    mock_cr = AsyncMock(return_value={})
    with _mock_admin(), \
         patch("k8s_client.create_configmap", mock_cm), \
         patch("k8s_client.create_custom_object", mock_cr):
        resp = client.post(
            "/api/admin/connectors",
            json={"name": "my-s3", "source_type": "s3", "config": {"bucket": "b1"}},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "my-s3"
    assert data["source_type"] == "s3"
    assert data["config"] == {"bucket": "b1"}
    assert data["tenant_id"] == "tenant-abc"
    assert data["start_paused"] is False
    assert "id" in data


def test_create_connector_calls_both_configmap_and_cr():
    mock_cm = AsyncMock(return_value={})
    mock_cr = AsyncMock(return_value={})
    with _mock_admin(), \
         patch("k8s_client.create_configmap", mock_cm), \
         patch("k8s_client.create_custom_object", mock_cr):
        resp = client.post(
            "/api/admin/connectors",
            json={"name": "my-nfs", "source_type": "nfs", "config": {}},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 201
    assert mock_cm.called
    assert mock_cr.called


def test_create_connector_start_paused_true():
    with _mock_admin(), \
         patch("k8s_client.create_configmap", new_callable=AsyncMock, return_value={}), \
         patch("k8s_client.create_custom_object", new_callable=AsyncMock, return_value={}):
        resp = client.post(
            "/api/admin/connectors",
            json={"name": "paused-s3", "source_type": "s3", "start_paused": True},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 201
    assert resp.json()["start_paused"] is True


def test_create_connector_non_admin_returns_403():
    with _mock_user():
        resp = client.post(
            "/api/admin/connectors",
            json={"name": "x", "source_type": "s3"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 403


# ── PATCH connector ───────────────────────────────────────────────────────────

def test_patch_connector_updates_name():
    cm = _existing_cm()
    updated_cm = {
        "name": "connector-cid1",
        "data": {**cm["data"], "name": "new-name"},
        "labels": {},
    }
    with _mock_admin(), \
         patch("k8s_client.get_configmap", new_callable=AsyncMock, return_value=cm), \
         patch("k8s_client.patch_configmap", new_callable=AsyncMock, return_value=updated_cm):
        resp = client.patch(
            "/api/admin/connectors/cid1",
            json={"name": "new-name"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 200
    assert resp.json()["name"] == "new-name"


def test_patch_connector_updates_config():
    cm = _existing_cm()
    new_config = {"bucket": "new-bucket", "region": "us-west-2"}
    updated_cm = {
        "name": "connector-cid1",
        "data": {**cm["data"], "config": '{"bucket": "new-bucket", "region": "us-west-2"}'},
        "labels": {},
    }
    with _mock_admin(), \
         patch("k8s_client.get_configmap", new_callable=AsyncMock, return_value=cm), \
         patch("k8s_client.patch_configmap", new_callable=AsyncMock, return_value=updated_cm):
        resp = client.patch(
            "/api/admin/connectors/cid1",
            json={"config": new_config},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 200
    assert resp.json()["config"] == new_config


def test_patch_connector_not_found_returns_404():
    with _mock_admin(), \
         patch("k8s_client.get_configmap", new_callable=AsyncMock, return_value=None):
        resp = client.patch(
            "/api/admin/connectors/nonexistent",
            json={"name": "x"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 404
    assert resp.json()["error"] == "NOT_FOUND"


def test_patch_connector_wrong_tenant_returns_403():
    cm = _existing_cm(tenant_id="other-tenant")
    with _mock_admin(org_id="tenant-abc"), \
         patch("k8s_client.get_configmap", new_callable=AsyncMock, return_value=cm):
        resp = client.patch(
            "/api/admin/connectors/cid1",
            json={"name": "x"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 403


def test_patch_connector_non_admin_returns_403():
    with _mock_user():
        resp = client.patch(
            "/api/admin/connectors/cid1",
            json={"name": "x"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 403


# ── DELETE connector ──────────────────────────────────────────────────────────

def test_delete_connector_returns_204():
    cm = _existing_cm()
    with _mock_admin(), \
         patch("k8s_client.get_configmap", new_callable=AsyncMock, return_value=cm), \
         patch("k8s_client.delete_configmap", new_callable=AsyncMock, return_value=None), \
         patch("k8s_client.delete_custom_object", new_callable=AsyncMock, return_value=None):
        resp = client.delete("/api/admin/connectors/cid1", headers=_AUTH_HEADER)
    assert resp.status_code == 204


def test_delete_connector_calls_both_configmap_and_cr_delete():
    cm = _existing_cm()
    mock_del_cm = AsyncMock(return_value=None)
    mock_del_cr = AsyncMock(return_value=None)
    with _mock_admin(), \
         patch("k8s_client.get_configmap", new_callable=AsyncMock, return_value=cm), \
         patch("k8s_client.delete_configmap", mock_del_cm), \
         patch("k8s_client.delete_custom_object", mock_del_cr):
        client.delete("/api/admin/connectors/cid1", headers=_AUTH_HEADER)
    assert mock_del_cm.called
    assert mock_del_cr.called


def test_delete_connector_not_found_returns_404():
    with _mock_admin(), \
         patch("k8s_client.get_configmap", new_callable=AsyncMock, return_value=None):
        resp = client.delete("/api/admin/connectors/nonexistent", headers=_AUTH_HEADER)
    assert resp.status_code == 404
    assert resp.json()["error"] == "NOT_FOUND"


def test_delete_connector_wrong_tenant_returns_403():
    cm = _existing_cm(tenant_id="other-tenant")
    with _mock_admin(org_id="tenant-abc"), \
         patch("k8s_client.get_configmap", new_callable=AsyncMock, return_value=cm):
        resp = client.delete("/api/admin/connectors/cid1", headers=_AUTH_HEADER)
    assert resp.status_code == 403


def test_delete_connector_non_admin_returns_403():
    with _mock_user():
        resp = client.delete("/api/admin/connectors/cid1", headers=_AUTH_HEADER)
    assert resp.status_code == 403


def test_delete_connector_tolerates_missing_cr():
    cm = _existing_cm()
    with _mock_admin(), \
         patch("k8s_client.get_configmap", new_callable=AsyncMock, return_value=cm), \
         patch("k8s_client.delete_configmap", new_callable=AsyncMock, return_value=None), \
         patch("k8s_client.delete_custom_object", new_callable=AsyncMock, side_effect=Exception("not found")):
        resp = client.delete("/api/admin/connectors/cid1", headers=_AUTH_HEADER)
    assert resp.status_code == 204


# ── Pipeline config ───────────────────────────────────────────────────────────

def test_get_pipeline_config_returns_defaults_when_no_configmap():
    with _mock_admin(), \
         patch("k8s_client.get_configmap", new_callable=AsyncMock, return_value=None):
        resp = client.get("/api/admin/pipeline/config", headers=_AUTH_HEADER)
    assert resp.status_code == 200
    data = resp.json()
    assert data["chunk_size"] == 512
    assert data["chunk_overlap"] == 50
    assert data["embedding_backend"] == "bge-small-en-v1.5"
    assert data["milvus_index_type"] == "IVF_FLAT"
    assert data["milvus_nlist"] == 128


def test_get_pipeline_config_returns_stored_values():
    cm = {
        "name": "pipeline-config",
        "data": {
            "chunk_size": "256",
            "chunk_overlap": "25",
            "embedding_backend": "bge-large-en",
            "milvus_index_type": "HNSW",
            "milvus_nlist": "64",
        },
        "labels": {},
    }
    with _mock_admin(), \
         patch("k8s_client.get_configmap", new_callable=AsyncMock, return_value=cm):
        resp = client.get("/api/admin/pipeline/config", headers=_AUTH_HEADER)
    assert resp.status_code == 200
    data = resp.json()
    assert data["chunk_size"] == 256
    assert data["chunk_overlap"] == 25
    assert data["embedding_backend"] == "bge-large-en"
    assert data["milvus_index_type"] == "HNSW"
    assert data["milvus_nlist"] == 64


def test_get_pipeline_config_non_admin_returns_403():
    with _mock_user():
        resp = client.get("/api/admin/pipeline/config", headers=_AUTH_HEADER)
    assert resp.status_code == 403


def test_put_pipeline_config_creates_configmap_when_not_exists():
    mock_get = AsyncMock(return_value=None)
    mock_create = AsyncMock(return_value={})
    with _mock_admin(), \
         patch("k8s_client.get_configmap", mock_get), \
         patch("k8s_client.create_configmap", mock_create):
        resp = client.put(
            "/api/admin/pipeline/config",
            json={
                "chunk_size": 256,
                "chunk_overlap": 25,
                "embedding_backend": "bge-small-en-v1.5",
                "milvus_index_type": "IVF_FLAT",
                "milvus_nlist": 128,
            },
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 200
    assert resp.json()["chunk_size"] == 256
    assert mock_create.called


def test_put_pipeline_config_updates_existing_configmap():
    existing = {
        "name": "pipeline-config",
        "data": {
            "chunk_size": "512",
            "chunk_overlap": "50",
            "embedding_backend": "bge-small-en-v1.5",
            "milvus_index_type": "IVF_FLAT",
            "milvus_nlist": "128",
        },
        "labels": {},
    }
    mock_get = AsyncMock(return_value=existing)
    mock_patch = AsyncMock(return_value={})
    with _mock_admin(), \
         patch("k8s_client.get_configmap", mock_get), \
         patch("k8s_client.patch_configmap", mock_patch):
        resp = client.put(
            "/api/admin/pipeline/config",
            json={
                "chunk_size": 256,
                "chunk_overlap": 25,
                "embedding_backend": "bge-small-en-v1.5",
                "milvus_index_type": "IVF_FLAT",
                "milvus_nlist": 128,
            },
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 200
    assert resp.json()["chunk_size"] == 256
    assert mock_patch.called


def test_put_pipeline_config_non_admin_returns_403():
    with _mock_user():
        resp = client.put(
            "/api/admin/pipeline/config",
            json={
                "chunk_size": 256,
                "chunk_overlap": 25,
                "embedding_backend": "bge-small-en-v1.5",
                "milvus_index_type": "IVF_FLAT",
                "milvus_nlist": 128,
            },
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 403
