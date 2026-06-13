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

_USER_CLAIMS = {
    "sub": "user-1",
    "email": "alice@acme.com",
    "org_id": "tenant-abc",
    "org_name": "acme",
    "license_type": "pro",
    "quota_tier": "pro",
    "roles": ["developer"],
}
_AUTH_HEADER = {"Authorization": "Bearer valid.token"}


def _mock_user(org_id: str = "tenant-abc", sub: str = "user-1"):
    return patch("auth.decode_token", return_value={**_USER_CLAIMS, "org_id": org_id, "sub": sub})


_WS_ROW = {
    "id": "ws-1",
    "tenant_id": "tenant-abc",
    "owner_id": "user-1",
    "name": "My Workspace",
    "description": None,
}

_WS_SOURCE_ROW = {
    "id": "wss-1",
    "workspace_id": "ws-1",
    "connector_id": "src-1",
    "path_prefix": "/data",
}

_FILE_ROW = {
    "id": "file-1",
    "connector_id": "src-1",
    "file_path": "/data/doc.pdf",
    "ingest_status": "indexed",
    "file_size_bytes": 1024,
    "chunk_count": 5,
}


# ── Workspaces ─────────────────────────────────────────────────────────────────

def test_list_workspaces_requires_auth():
    resp = client.get("/api/workspaces")
    assert resp.status_code == 401


def test_list_workspaces_returns_tenant_workspaces():
    with _mock_user(), patch("db_client.get_workspaces", new_callable=AsyncMock, return_value=[_WS_ROW]):
        resp = client.get("/api/workspaces", headers=_AUTH_HEADER)
    assert resp.status_code == 200
    items = resp.json()
    assert len(items) == 1
    assert items[0]["name"] == "My Workspace"
    assert items[0]["tenant_id"] == "tenant-abc"


def test_bff_user_workspace_scoped_to_tenant():
    """Workspace list is scoped to the caller's org_id; each tenant sees only their own data."""
    mock_get_abc = AsyncMock(return_value=[_WS_ROW])
    with _mock_user(org_id="tenant-abc"), patch("db_client.get_workspaces", mock_get_abc):
        resp_abc = client.get("/api/workspaces", headers=_AUTH_HEADER)
    assert resp_abc.status_code == 200
    assert len(resp_abc.json()) == 1
    # Verify the endpoint scoped the query to the caller's tenant
    assert mock_get_abc.call_args[0][0] == "tenant-abc"

    mock_get_xyz = AsyncMock(return_value=[])
    with _mock_user(org_id="tenant-xyz"), patch("db_client.get_workspaces", mock_get_xyz):
        resp_xyz = client.get("/api/workspaces", headers=_AUTH_HEADER)
    assert resp_xyz.status_code == 200
    assert resp_xyz.json() == []
    # Different tenant gets a different scoped query
    assert mock_get_xyz.call_args[0][0] == "tenant-xyz"


def test_list_workspaces_empty_returns_empty_list():
    with _mock_user(), patch("db_client.get_workspaces", new_callable=AsyncMock, return_value=[]):
        resp = client.get("/api/workspaces", headers=_AUTH_HEADER)
    assert resp.status_code == 200
    assert resp.json() == []


def test_create_workspace_returns_201():
    created = {**_WS_ROW, "name": "New WS"}
    with _mock_user(), patch("db_client.create_workspace", new_callable=AsyncMock, return_value=created):
        resp = client.post("/api/workspaces", json={"name": "New WS"}, headers=_AUTH_HEADER)
    assert resp.status_code == 201
    assert resp.json()["name"] == "New WS"


def test_create_workspace_uses_claims_org_id():
    """tenant_id is always taken from JWT org_id, never from the request body."""
    created = {**_WS_ROW, "name": "New WS"}
    mock_create = AsyncMock(return_value=created)
    with _mock_user(org_id="tenant-abc", sub="user-1"), patch("db_client.create_workspace", mock_create):
        resp = client.post("/api/workspaces", json={"name": "New WS"}, headers=_AUTH_HEADER)
    assert resp.status_code == 201
    assert resp.json()["tenant_id"] == "tenant-abc"
    call_kwargs = mock_create.call_args[1]
    assert call_kwargs["tenant_id"] == "tenant-abc"
    assert call_kwargs["owner_id"] == "user-1"


def test_create_workspace_requires_auth():
    resp = client.post("/api/workspaces", json={"name": "x"})
    assert resp.status_code == 401


def test_delete_workspace_returns_204():
    with _mock_user(), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=_WS_ROW), \
         patch("db_client.delete_workspace", new_callable=AsyncMock, return_value=True):
        resp = client.delete("/api/workspaces/ws-1", headers=_AUTH_HEADER)
    assert resp.status_code == 204


def test_delete_workspace_not_found_returns_404():
    with _mock_user(), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=None):
        resp = client.delete("/api/workspaces/ws-missing", headers=_AUTH_HEADER)
    assert resp.status_code == 404
    assert resp.json()["error"] == "NOT_FOUND"


def test_delete_workspace_wrong_tenant_returns_404():
    """Cross-tenant workspace access appears as not-found (no tenant ID leakage via 403)."""
    with _mock_user(org_id="tenant-xyz"), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=None):
        resp = client.delete("/api/workspaces/ws-1", headers=_AUTH_HEADER)
    assert resp.status_code == 404


def test_delete_workspace_requires_auth():
    resp = client.delete("/api/workspaces/ws-1")
    assert resp.status_code == 401


# ── Sources ────────────────────────────────────────────────────────────────────

def test_list_sources_requires_auth():
    resp = client.get("/api/sources")
    assert resp.status_code == 401


def test_list_sources_returns_tenant_connectors():
    cms = [
        {
            "name": "connector-src-1",
            "data": {
                "id": "src-1",
                "name": "my-s3",
                "source_type": "s3",
                "config": "{}",
                "tenant_id": "tenant-abc",
            },
            "labels": {"tenant-id": "tenant-abc"},
        }
    ]
    with _mock_user(), patch("k8s_client.list_configmaps", new_callable=AsyncMock, return_value=cms):
        resp = client.get("/api/sources", headers=_AUTH_HEADER)
    assert resp.status_code == 200
    items = resp.json()
    assert len(items) == 1
    assert items[0]["id"] == "src-1"
    assert items[0]["source_type"] == "s3"


def test_list_sources_scoped_to_tenant():
    """Label selector must include the caller's tenant ID to prevent cross-tenant access."""
    mock_list = AsyncMock(return_value=[])
    with _mock_user(org_id="tenant-abc"), patch("k8s_client.list_configmaps", mock_list):
        client.get("/api/sources", headers=_AUTH_HEADER)
    label_sel = mock_list.call_args[1].get("label_selector") or mock_list.call_args[0][1]
    assert "tenant-abc" in label_sel

    mock_list2 = AsyncMock(return_value=[])
    with _mock_user(org_id="tenant-xyz"), patch("k8s_client.list_configmaps", mock_list2):
        client.get("/api/sources", headers=_AUTH_HEADER)
    label_sel2 = mock_list2.call_args[1].get("label_selector") or mock_list2.call_args[0][1]
    assert "tenant-xyz" in label_sel2


def test_list_sources_empty_returns_empty_list():
    with _mock_user(), patch("k8s_client.list_configmaps", new_callable=AsyncMock, return_value=[]):
        resp = client.get("/api/sources", headers=_AUTH_HEADER)
    assert resp.status_code == 200
    assert resp.json() == []


# ── Workspace sources ──────────────────────────────────────────────────────────

def test_list_workspace_sources_returns_sources():
    with _mock_user(), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=_WS_ROW), \
         patch("db_client.get_workspace_sources", new_callable=AsyncMock, return_value=[_WS_SOURCE_ROW]):
        resp = client.get("/api/workspaces/ws-1/sources", headers=_AUTH_HEADER)
    assert resp.status_code == 200
    items = resp.json()
    assert len(items) == 1
    assert items[0]["connector_id"] == "src-1"


def test_list_workspace_sources_wrong_tenant_returns_404():
    with _mock_user(org_id="tenant-xyz"), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=None):
        resp = client.get("/api/workspaces/ws-1/sources", headers=_AUTH_HEADER)
    assert resp.status_code == 404


def test_add_workspace_source_returns_201():
    with _mock_user(), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=_WS_ROW), \
         patch("db_client.add_workspace_source", new_callable=AsyncMock, return_value=_WS_SOURCE_ROW):
        resp = client.post(
            "/api/workspaces/ws-1/sources",
            json={"connector_id": "src-1", "path_prefix": "/data"},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 201
    assert resp.json()["connector_id"] == "src-1"
    assert resp.json()["path_prefix"] == "/data"


def test_add_workspace_source_wrong_tenant_returns_404():
    with _mock_user(org_id="tenant-xyz"), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=None):
        resp = client.post(
            "/api/workspaces/ws-1/sources",
            json={"connector_id": "src-1", "path_prefix": ""},
            headers=_AUTH_HEADER,
        )
    assert resp.status_code == 404


def test_add_workspace_source_requires_auth():
    resp = client.post("/api/workspaces/ws-1/sources", json={"connector_id": "src-1"})
    assert resp.status_code == 401


def test_delete_workspace_source_returns_204():
    with _mock_user(), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=_WS_ROW), \
         patch("db_client.delete_workspace_source", new_callable=AsyncMock, return_value=True):
        resp = client.delete("/api/workspaces/ws-1/sources/wss-1", headers=_AUTH_HEADER)
    assert resp.status_code == 204


def test_delete_workspace_source_not_found_returns_404():
    with _mock_user(), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=_WS_ROW), \
         patch("db_client.delete_workspace_source", new_callable=AsyncMock, return_value=False):
        resp = client.delete("/api/workspaces/ws-1/sources/missing", headers=_AUTH_HEADER)
    assert resp.status_code == 404
    assert resp.json()["error"] == "NOT_FOUND"


def test_delete_workspace_source_wrong_tenant_returns_404():
    with _mock_user(org_id="tenant-xyz"), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=None):
        resp = client.delete("/api/workspaces/ws-1/sources/wss-1", headers=_AUTH_HEADER)
    assert resp.status_code == 404


# ── Workspace files ────────────────────────────────────────────────────────────

def test_get_workspace_files_returns_file_list():
    with _mock_user(), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=_WS_ROW), \
         patch("db_client.get_workspace_files", new_callable=AsyncMock, return_value=[_FILE_ROW]):
        resp = client.get("/api/workspaces/ws-1/files", headers=_AUTH_HEADER)
    assert resp.status_code == 200
    items = resp.json()
    assert len(items) == 1
    assert items[0]["file_path"] == "/data/doc.pdf"
    assert items[0]["ingest_status"] == "indexed"
    assert items[0]["chunk_count"] == 5


def test_get_workspace_files_scoped_to_tenant():
    """Files query must pass tenant_id from JWT claims, not any request parameter."""
    mock_files = AsyncMock(return_value=[_FILE_ROW])
    with _mock_user(org_id="tenant-abc"), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=_WS_ROW), \
         patch("db_client.get_workspace_files", mock_files):
        resp = client.get("/api/workspaces/ws-1/files", headers=_AUTH_HEADER)
    assert resp.status_code == 200
    # Verify the DB query was scoped to the correct tenant from the JWT
    call_args = mock_files.call_args[0]
    assert call_args[1] == "tenant-abc"


def test_get_workspace_files_wrong_tenant_returns_404():
    with _mock_user(org_id="tenant-xyz"), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=None):
        resp = client.get("/api/workspaces/ws-1/files", headers=_AUTH_HEADER)
    assert resp.status_code == 404


def test_get_workspace_files_requires_auth():
    resp = client.get("/api/workspaces/ws-1/files")
    assert resp.status_code == 401


def test_get_workspace_files_empty_returns_empty_list():
    with _mock_user(), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=_WS_ROW), \
         patch("db_client.get_workspace_files", new_callable=AsyncMock, return_value=[]):
        resp = client.get("/api/workspaces/ws-1/files", headers=_AUTH_HEADER)
    assert resp.status_code == 200
    assert resp.json() == []


# ── Reindex ────────────────────────────────────────────────────────────────────

def test_reindex_file_returns_202():
    with _mock_user(), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=_WS_ROW):
        resp = client.post("/api/workspaces/ws-1/files/file-1/reindex", headers=_AUTH_HEADER)
    assert resp.status_code == 202
    assert resp.json()["status"] == "reindex_queued"
    assert resp.json()["file_id"] == "file-1"


def test_reindex_file_wrong_tenant_returns_404():
    with _mock_user(org_id="tenant-xyz"), \
         patch("db_client.get_workspace", new_callable=AsyncMock, return_value=None):
        resp = client.post("/api/workspaces/ws-1/files/file-1/reindex", headers=_AUTH_HEADER)
    assert resp.status_code == 404


def test_reindex_file_requires_auth():
    resp = client.post("/api/workspaces/ws-1/files/file-1/reindex")
    assert resp.status_code == 401
