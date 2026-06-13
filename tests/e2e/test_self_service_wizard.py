"""
E2E tests for the self-service wizard API flows (test plan §4.3 and §4.2 quota enforcement).

Exercises the full FastAPI application via TestClient, mocking only external I/O
boundaries (Kubernetes API, MinIO, Kafka, Quota gRPC) so the suite passes without
a live kind cluster.

Covered scenarios:
  - test_e2e_user_creates_s3_connector_and_ingests    (§4.3)
  - test_e2e_file_upload_ingested                     (§4.3)
  - test_e2e_connector_deletion_removes_cr            (§4.3)
  - test_e2e_connector_quota_enforced_free_tier       (§4.2 / §4.3)
  - test_e2e_connector_quota_unlimited_pro_tier       (§4.2 / §4.3)
  - test_e2e_connector_created_in_paused_state        (§4.3 start_paused)
  - test_e2e_ssrf_blocked_in_test_step                (§4.3 security gate)
  - test_e2e_upload_skips_quota_and_publishes_event   (§4.3 upload path)
"""
import os
import sys
from unittest.mock import AsyncMock, patch

import pytest

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "services", "bff", "src"),
)

from app import app  # noqa: E402
from fastapi.testclient import TestClient

client = TestClient(app, raise_server_exceptions=False)

_AUTH = {"Authorization": "Bearer valid.token"}

_FREE_USER = {
    "sub": "user-alice",
    "email": "alice@acme.com",
    "org_id": "tenant-abc",
    "org_name": "acme",
    "license_type": "free",
    "quota_tier": "free",
    "roles": ["developer"],
}

_PRO_USER = {
    **_FREE_USER,
    "sub": "user-pro",
    "email": "pro@acme.com",
    "license_type": "pro",
    "quota_tier": "pro",
}


def _cm(connector_id: str, name: str, source_type: str = "s3") -> dict:
    return {
        "name": f"connector-{connector_id}",
        "data": {
            "id": connector_id,
            "name": name,
            "source_type": source_type,
            "config": "{}",
            "tenant_id": "tenant-abc",
            "owner_id": "user-alice",
            "start_paused": "false",
        },
        "labels": {"tenant-id": "tenant-abc"},
    }


# ── §4.3: S3 connector creation + ingestion flow ───────────────────────────

def test_e2e_user_creates_s3_connector_and_ingests():
    """Step-by-step wizard: test connection → create connector → CR submitted to K8s."""
    quota_ok = AsyncMock(return_value={"allowed": True, "status": "ALLOWED"})
    cm_mock = AsyncMock(return_value=_cm("conn-s3-1", "acme-s3"))
    cr_mock = AsyncMock(return_value={})

    with patch("auth.decode_token", return_value=_FREE_USER), \
         patch("quota_client.check_quota", quota_ok), \
         patch("k8s_client.create_configmap", cm_mock), \
         patch("k8s_client.create_custom_object", cr_mock):

        # Step 1 — Test connection (Wizard Step 3)
        resp_test = client.post(
            "/api/sources/test",
            json={"endpoint": "s3://203.0.113.10/my-bucket", "source_type": "s3"},
            headers=_AUTH,
        )
        assert resp_test.status_code == 200, resp_test.text
        assert resp_test.json()["status"] == "ok"

        # Step 2 — Create connector (Wizard Step 4 → submit)
        resp_create = client.post(
            "/api/sources/create",
            json={"name": "acme-s3", "source_type": "s3", "config": {"bucket": "my-bucket"}},
            headers=_AUTH,
        )
        assert resp_create.status_code == 201, resp_create.text
        body = resp_create.json()
        assert body["source_type"] == "s3"
        assert body["status"] == "provisioning"
        assert body["tenant_id"] == "tenant-abc"

        # Step 3 — DataConnector CR was submitted to K8s
        cr_mock.assert_awaited_once()
        _, _, _, plural, cr_body = cr_mock.call_args[0]
        assert plural == "dataconnectors"
        assert cr_body["kind"] == "DataConnector"
        assert cr_body["spec"]["sourceType"] == "s3"
        assert cr_body["spec"]["tenantId"] == "tenant-abc"

        # Step 4 — Quota checked for free-tier user
        quota_ok.assert_awaited_once_with("tenant-abc", "CONNECTOR_COUNT")


# ── §4.3: File upload → MinIO → Kafka metadata event ──────────────────────

def test_e2e_file_upload_ingested():
    """Upload a PDF → file lands in MinIO → metadata event published for upload-watcher."""
    stored_path = "tenant-abc/uploads/sess-1/report.pdf"
    minio_mock = AsyncMock(return_value=stored_path)
    kafka_mock = AsyncMock(return_value=None)

    with patch("auth.decode_token", return_value=_FREE_USER), \
         patch("minio_client.upload_file", minio_mock), \
         patch("kafka_client.publish_event", kafka_mock):

        # Step 1 — POST /api/sources/upload
        resp = client.post(
            "/api/sources/upload",
            files={"file": ("report.pdf", b"%PDF-1.4 test content", "application/pdf")},
            headers=_AUTH,
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["status"] == "uploaded"
        session_id = body["session_id"]
        assert session_id  # upload session ID returned

        # Step 2 — File stored in MinIO under tenant prefix
        minio_mock.assert_awaited_once()
        org, sid, filename, _ = minio_mock.call_args[0]
        assert org == "tenant-abc"
        assert filename == "report.pdf"

        # Step 3 — Metadata event published so upload-watcher CronJob can pick it up
        kafka_mock.assert_awaited_once()
        topic, event = kafka_mock.call_args[0]
        assert topic == "metadata-events"
        assert event["entity_type"] == "DataSource"
        assert event["source_type"] == "upload"
        assert event["tenant_id"] == "tenant-abc"
        assert event["session_id"] == session_id


# ── §4.3: Connector deletion removes CR (operator handles Secret cleanup) ──

def test_e2e_connector_deletion_removes_cr():
    """Create a connector then delete it; DataConnector CR and ConfigMap are removed."""
    quota_ok = AsyncMock(return_value={"allowed": True, "status": "ALLOWED"})
    cm_create = AsyncMock(return_value=_cm("placeholder", "deletable-s3"))
    cr_create = AsyncMock(return_value={})
    cm_delete = AsyncMock(return_value=None)
    cr_delete = AsyncMock(return_value=None)

    with patch("auth.decode_token", return_value=_FREE_USER), \
         patch("quota_client.check_quota", quota_ok), \
         patch("k8s_client.create_configmap", cm_create), \
         patch("k8s_client.create_custom_object", cr_create), \
         patch("k8s_client.delete_configmap", cm_delete), \
         patch("k8s_client.delete_custom_object", cr_delete):

        # Step 1 — Create connector; the endpoint generates its own UUID
        resp_create = client.post(
            "/api/sources/create",
            json={"name": "deletable-s3", "source_type": "s3"},
            headers=_AUTH,
        )
        assert resp_create.status_code == 201, resp_create.text
        connector_id = resp_create.json()["id"]
        expected_cm_name = f"connector-{connector_id}"

        # Step 2 — Delete connector (owner deletes their own resource)
        cm_get = AsyncMock(return_value=_cm(connector_id, "deletable-s3"))
        with patch("k8s_client.get_configmap", cm_get):
            resp_del = client.delete(f"/api/sources/{connector_id}", headers=_AUTH)
        assert resp_del.status_code == 204, resp_del.text

        # Step 3 — ConfigMap deleted
        cm_delete.assert_awaited_once()
        assert cm_delete.call_args[0][1] == expected_cm_name

        # Step 4 — DataConnector CR deleted (operator then deletes connector-{slug}-creds Secret)
        cr_delete.assert_awaited_once()
        dc_args = cr_delete.call_args[0]
        assert dc_args[3] == "dataconnectors"
        assert dc_args[4] == expected_cm_name


# ── §4.2 / §4.3: Quota enforcement — Free tier cap ─────────────────────────

def test_e2e_connector_quota_enforced_free_tier():
    """Free-tier: first 2 connectors succeed; 3rd is rejected with 402 QUOTA_EXCEEDED."""
    call_count = {"n": 0}

    async def quota_gate(tenant_id, metric):
        call_count["n"] += 1
        if call_count["n"] <= 2:
            return {"allowed": True, "status": "ALLOWED"}
        return {"allowed": False, "status": "DENIED"}

    cm_mock = AsyncMock(return_value=_cm("auto", "s"))
    cr_mock = AsyncMock(return_value={})

    with patch("auth.decode_token", return_value=_FREE_USER), \
         patch("quota_client.check_quota", side_effect=quota_gate), \
         patch("k8s_client.create_configmap", cm_mock), \
         patch("k8s_client.create_custom_object", cr_mock):

        # First two succeed
        for i in range(2):
            resp = client.post(
                "/api/sources/create",
                json={"name": f"s3-{i}", "source_type": "s3"},
                headers=_AUTH,
            )
            assert resp.status_code == 201, f"Connector {i}: {resp.text}"

        # Third hits the cap
        resp3 = client.post(
            "/api/sources/create",
            json={"name": "s3-overflow", "source_type": "s3"},
            headers=_AUTH,
        )
        assert resp3.status_code == 402, resp3.text
        assert resp3.json()["error"] == "QUOTA_EXCEEDED"


# ── §4.2 / §4.3: Quota enforcement — Pro tier unlimited ────────────────────

def test_e2e_connector_quota_unlimited_pro_tier():
    """Pro-tier: >4 connectors succeed; quota check is never called (unlimited)."""
    quota_spy = AsyncMock(return_value={"allowed": True, "status": "UNLIMITED"})
    cm_mock = AsyncMock(return_value=_cm("auto", "s"))
    cr_mock = AsyncMock(return_value={})

    with patch("auth.decode_token", return_value=_PRO_USER), \
         patch("quota_client.check_quota", quota_spy), \
         patch("k8s_client.create_configmap", cm_mock), \
         patch("k8s_client.create_custom_object", cr_mock):

        for i in range(5):
            resp = client.post(
                "/api/sources/create",
                json={"name": f"pro-s3-{i}", "source_type": "s3"},
                headers=_AUTH,
            )
            assert resp.status_code == 201, f"Pro connector {i}: {resp.text}"

        # Pro tier bypasses quota check entirely
        quota_spy.assert_not_called()


# ── §4.3: start_paused toggle ───────────────────────────────────────────────

def test_e2e_connector_created_in_paused_state():
    """'Start ingestion immediately' unchecked → DataConnector CR has startPaused=True."""
    quota_ok = AsyncMock(return_value={"allowed": True, "status": "ALLOWED"})
    cm_mock = AsyncMock(return_value=_cm("paused-1", "paused-s3"))
    cr_mock = AsyncMock(return_value={})

    with patch("auth.decode_token", return_value=_FREE_USER), \
         patch("quota_client.check_quota", quota_ok), \
         patch("k8s_client.create_configmap", cm_mock), \
         patch("k8s_client.create_custom_object", cr_mock):

        resp = client.post(
            "/api/sources/create",
            json={"name": "paused-s3", "source_type": "s3", "start_paused": True},
            headers=_AUTH,
        )

    assert resp.status_code == 201, resp.text
    assert resp.json()["start_paused"] is True

    cr_mock.assert_awaited_once()
    _, _, _, _, cr_body = cr_mock.call_args[0]
    assert cr_body["spec"]["startPaused"] is True


# ── §4.3 / §7.2: SSRF blocked at Wizard Step 3 ─────────────────────────────

def test_e2e_ssrf_blocked_in_test_step():
    """RFC-1918 endpoint in wizard Step 3 (test connection) is rejected before any attempt."""
    with patch("auth.decode_token", return_value=_FREE_USER):
        resp = client.post(
            "/api/sources/test",
            json={"endpoint": "postgresql://192.168.1.100:5432/db", "source_type": "database"},
            headers=_AUTH,
        )
    assert resp.status_code == 400, resp.text
    assert resp.json()["error"] == "SSRF_BLOCKED"


# ── §4.3: Upload path skips quota and publishes DataSource event ────────────

def test_e2e_upload_skips_quota_and_publishes_event():
    """Upload flow must not touch CONNECTOR_COUNT quota; must publish DataSource event."""
    quota_spy = AsyncMock(return_value={"allowed": True, "status": "ALLOWED"})
    minio_mock = AsyncMock(return_value="tenant-abc/uploads/s/f.pdf")
    kafka_mock = AsyncMock(return_value=None)

    with patch("auth.decode_token", return_value=_FREE_USER), \
         patch("quota_client.check_quota", quota_spy), \
         patch("minio_client.upload_file", minio_mock), \
         patch("kafka_client.publish_event", kafka_mock):

        resp = client.post(
            "/api/sources/upload",
            files={"file": ("f.pdf", b"data", "application/pdf")},
            headers=_AUTH,
        )
    assert resp.status_code == 201, resp.text

    # Quota must NOT be called for the upload path
    quota_spy.assert_not_called()

    # Kafka DataSource event must be published
    kafka_mock.assert_awaited_once()
    topic, event = kafka_mock.call_args[0]
    assert topic == "metadata-events"
    assert event["entity_type"] == "DataSource"
    assert event["source_type"] == "upload"
