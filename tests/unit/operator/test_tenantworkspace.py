"""Unit tests for Pipeline Operator — TenantWorkspace and EmbeddingConfig reconcile (test plan §2.7).

Covers:
  test_tenantworkspace_creates_upload_watcher_cronjob
  test_tenantworkspace_delete_removes_upload_watcher
  test_embeddingconfig_blocks_dimension_change_without_flag
  test_embeddingconfig_allows_dimension_change_with_flag
"""
import os
import sys

import pytest

_OPERATOR_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..", "..", "..",
        "services", "pipeline-operator", "operator",
    )
)
if _OPERATOR_DIR not in sys.path:
    sys.path.insert(0, _OPERATOR_DIR)

import handlers  # noqa: E402


class _MockPatch:
    def __init__(self):
        self.status: dict = {}


# ---------------------------------------------------------------------------
# TenantWorkspace
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tenantworkspace_creates_upload_watcher_cronjob(monkeypatch):
    """TenantWorkspace reconcile creates upload-watcher-{tenant_id} CronJob."""
    from unittest.mock import AsyncMock

    mock_client = AsyncMock()
    monkeypatch.setattr(handlers, "_client", mock_client)

    spec = {"tenantId": "acme"}
    patch = _MockPatch()

    await handlers.reconcile_workspace(
        spec=spec, name="acme-workspace", namespace="ai-pipeline", patch=patch
    )

    mock_client.apply_cronjob.assert_called_once()
    cronjob_name = mock_client.apply_cronjob.call_args[0][0]
    assert cronjob_name == "upload-watcher-acme"
    assert patch.status["state"] == "Provisioned"


@pytest.mark.asyncio
async def test_tenantworkspace_delete_removes_upload_watcher(monkeypatch):
    """TenantWorkspace delete triggers delete_cronjob(upload-watcher-{tenant_id})."""
    from unittest.mock import AsyncMock

    mock_client = AsyncMock()
    monkeypatch.setattr(handlers, "_client", mock_client)

    spec = {"tenantId": "acme"}

    await handlers.delete_workspace(
        spec=spec, name="acme-workspace", namespace="ai-pipeline"
    )

    mock_client.delete_cronjob.assert_called_once_with(
        "upload-watcher-acme", "ai-pipeline"
    )


# ---------------------------------------------------------------------------
# EmbeddingConfig — dimension-change guard
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_embeddingconfig_blocks_dimension_change_without_flag(monkeypatch):
    """old_dim=384, new_dim=1024, reindexConfirmed=false → TemporaryError raised, status=BlockedDimensionChange."""
    from unittest.mock import AsyncMock

    mock_client = AsyncMock()
    monkeypatch.setattr(handlers, "_client", mock_client)

    spec = {
        "backend": "local",
        "model": "bge-large-en-v1.5",
        "device": "cpu",
        "dimension": 1024,
    }
    old = {"spec": {"dimension": 384}}
    new = {"spec": {"dimension": 1024}}
    patch = _MockPatch()

    with pytest.raises(handlers.TemporaryError):
        await handlers.reconcile_embedding(
            spec=spec, old=old, new=new, namespace="ai-pipeline", patch=patch
        )

    assert patch.status["state"] == "BlockedDimensionChange"
    mock_client.patch_configmap.assert_not_called()
    mock_client.rollout_restart.assert_not_called()


@pytest.mark.asyncio
async def test_embeddingconfig_allows_dimension_change_with_flag(monkeypatch):
    """old_dim=384, new_dim=1024, reindexConfirmed=true → patch_configmap and rollout_restart called."""
    from unittest.mock import AsyncMock

    mock_client = AsyncMock()
    monkeypatch.setattr(handlers, "_client", mock_client)

    spec = {
        "backend": "local",
        "model": "bge-large-en-v1.5",
        "device": "cpu",
        "dimension": 1024,
        "reindexConfirmed": True,
    }
    old = {"spec": {"dimension": 384}}
    new = {"spec": {"dimension": 1024}}
    patch = _MockPatch()

    await handlers.reconcile_embedding(
        spec=spec, old=old, new=new, namespace="ai-pipeline", patch=patch
    )

    mock_client.patch_configmap.assert_called_once()
    mock_client.rollout_restart.assert_called_once()
    assert patch.status["state"] == "Applied"
