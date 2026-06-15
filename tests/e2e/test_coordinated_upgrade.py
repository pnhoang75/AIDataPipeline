"""
E2E tests for Pipeline Operator coordinated upgrade state machine (test plan §4.5).

Exercises the PipelineCluster reconciler by mocking K8sClient and verifying
the exact call sequence without requiring a live cluster or Kafka broker.

Covered scenarios:
  - test_e2e_coordinated_upgrade                          (§4.5 upgrade happy path)
  - test_e2e_connector_suspended_before_lag_drain         (§4.5 ordering constraint)
  - test_e2e_upgrade_inprogress_set_before_suspend        (§4.5 condition timing)
  - test_e2e_upgrade_rollback                             (§4.5 rollback reverse order)
  - test_e2e_upgrade_noop_when_version_unchanged          (§4.5 idempotency)
  - test_e2e_upgrade_raises_on_smoke_test_failure         (§4.5 smoke test gate)
"""
from __future__ import annotations

import os
import sys
from unittest.mock import AsyncMock, call

import pytest

_OPERATOR_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..", "services", "pipeline-operator", "operator"
    )
)
if _OPERATOR_DIR not in sys.path:
    sys.path.insert(0, _OPERATOR_DIR)

import handlers  # noqa: E402


class _MockPatch:
    """Minimal stand-in for kopf's patch object."""

    def __init__(self) -> None:
        self.status: dict = {}


def _build_mock_client(
    *,
    lag: int = 0,
    health: int = 200,
    connector_crons: list[str] | None = None,
) -> AsyncMock:
    mc = AsyncMock()
    mc.list_cronjobs.return_value = (
        connector_crons if connector_crons is not None else ["connector-acme-s3"]
    )
    mc.get_kafka_consumer_lag.return_value = lag
    mc.http_get.return_value = health
    return mc


# ---------------------------------------------------------------------------
# Upgrade happy path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_e2e_coordinated_upgrade(monkeypatch):
    """Upgrade 1.0.0→1.1.0: all steps fire in order; state ends at Ready."""
    mc = _build_mock_client()
    monkeypatch.setattr(handlers, "_client", mc)

    patch = _MockPatch()
    await handlers.reconcile_pipeline_cluster(
        spec={"version": "1.1.0"},
        old={"spec": {"version": "1.0.0"}},
        new={"spec": {"version": "1.1.0"}},
        namespace="ai-pipeline",
        patch=patch,
    )

    assert patch.status["upgradeInProgress"] is False
    assert patch.status["state"] == "Ready"

    mc.suspend_cronjob.assert_called_once_with("connector-acme-s3", "ai-pipeline")
    mc.resume_cronjob.assert_called_once_with("connector-acme-s3", "ai-pipeline")
    mc.http_get.assert_called_once()

    restart_calls = [c for c in mc.method_calls if c[0] == "rollout_restart"]
    assert len(restart_calls) == 3
    assert restart_calls[0] == call.rollout_restart("deployment", "doc-processor", "ai-pipeline")
    assert restart_calls[1] == call.rollout_restart("deployment", "embedding-worker", "ai-pipeline")
    assert restart_calls[2] == call.rollout_restart("deployment", "rag-api", "ai-pipeline")


@pytest.mark.asyncio
async def test_e2e_connector_suspended_before_lag_drain(monkeypatch):
    """Connector CronJobs are suspended before Kafka lag is polled."""
    mc = _build_mock_client()
    call_order: list[str] = []

    async def _record_suspend(*_a, **_kw) -> None:
        call_order.append("suspend")

    async def _record_lag(*_a, **_kw) -> int:
        call_order.append("lag")
        return 0

    mc.suspend_cronjob.side_effect = _record_suspend
    mc.get_kafka_consumer_lag.side_effect = _record_lag
    monkeypatch.setattr(handlers, "_client", mc)

    await handlers.reconcile_pipeline_cluster(
        spec={"version": "1.1.0"},
        old={"spec": {"version": "1.0.0"}},
        new={"spec": {"version": "1.1.0"}},
        namespace="ai-pipeline",
        patch=_MockPatch(),
    )

    assert call_order.index("suspend") < call_order.index("lag")


@pytest.mark.asyncio
async def test_e2e_upgrade_inprogress_set_before_suspend(monkeypatch):
    """UpgradeInProgress is True before connectors are suspended."""
    mc = _build_mock_client()
    status_at_suspend: dict = {}
    patch_obj = _MockPatch()

    async def _record_suspend(*_a, **_kw) -> None:
        status_at_suspend.update(patch_obj.status)

    mc.suspend_cronjob.side_effect = _record_suspend
    monkeypatch.setattr(handlers, "_client", mc)

    await handlers.reconcile_pipeline_cluster(
        spec={"version": "1.1.0"},
        old={"spec": {"version": "1.0.0"}},
        new={"spec": {"version": "1.1.0"}},
        namespace="ai-pipeline",
        patch=patch_obj,
    )

    assert status_at_suspend.get("upgradeInProgress") is True
    assert status_at_suspend.get("state") == "UpgradeInProgress"


# ---------------------------------------------------------------------------
# Rollback — services rolled in reverse order
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_e2e_upgrade_rollback(monkeypatch):
    """Rollback 1.1.0→1.0.0: services roll in reverse order (rag-api first)."""
    mc = _build_mock_client()
    monkeypatch.setattr(handlers, "_client", mc)

    patch = _MockPatch()
    await handlers.reconcile_pipeline_cluster(
        spec={"version": "1.0.0"},
        old={"spec": {"version": "1.1.0"}},
        new={"spec": {"version": "1.0.0"}},
        namespace="ai-pipeline",
        patch=patch,
    )

    assert patch.status["state"] == "Ready"

    restart_calls = [c for c in mc.method_calls if c[0] == "rollout_restart"]
    assert len(restart_calls) == 3
    assert restart_calls[0] == call.rollout_restart("deployment", "rag-api", "ai-pipeline")
    assert restart_calls[1] == call.rollout_restart("deployment", "embedding-worker", "ai-pipeline")
    assert restart_calls[2] == call.rollout_restart("deployment", "doc-processor", "ai-pipeline")


# ---------------------------------------------------------------------------
# No-op when version is unchanged
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_e2e_upgrade_noop_when_version_unchanged(monkeypatch):
    """No rollout when old and new versions are equal."""
    mc = _build_mock_client()
    monkeypatch.setattr(handlers, "_client", mc)

    await handlers.reconcile_pipeline_cluster(
        spec={"version": "1.0.0"},
        old={"spec": {"version": "1.0.0"}},
        new={"spec": {"version": "1.0.0"}},
        namespace="ai-pipeline",
        patch=_MockPatch(),
    )

    mc.suspend_cronjob.assert_not_called()
    mc.rollout_restart.assert_not_called()
    mc.resume_cronjob.assert_not_called()


# ---------------------------------------------------------------------------
# Smoke test gate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_e2e_upgrade_raises_on_smoke_test_failure(monkeypatch):
    """TemporaryError raised when RAG API smoke test returns non-200."""
    mc = _build_mock_client(health=503)
    monkeypatch.setattr(handlers, "_client", mc)

    with pytest.raises(handlers.TemporaryError):
        await handlers.reconcile_pipeline_cluster(
            spec={"version": "1.1.0"},
            old={"spec": {"version": "1.0.0"}},
            new={"spec": {"version": "1.1.0"}},
            namespace="ai-pipeline",
            patch=_MockPatch(),
        )

    mc.suspend_cronjob.assert_called()
    mc.resume_cronjob.assert_not_called()
