"""Unit tests for Pipeline Operator — DataConnector reconcile (test plan §2.7).

Covers:
  test_dataconnector_creates_deployment_without_poll_interval
  test_dataconnector_creates_cronjob_with_poll_interval
  test_connector_role_lists_exact_secret_names
"""
import os
import sys

import pytest

# Add the operator package directory to sys.path so modules can be imported
# without the 'operator' package name (which conflicts with the stdlib module).
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
import rbac      # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class _MockPatch:
    """Minimal stand-in for kopf's patch object."""
    def __init__(self):
        self.status: dict = {}


# ---------------------------------------------------------------------------
# DataConnector workload type selection
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dataconnector_creates_deployment_without_poll_interval(monkeypatch):
    """No pollInterval → apply_deployment called; apply_cronjob never called."""
    from unittest.mock import AsyncMock

    mock_client = AsyncMock()
    monkeypatch.setattr(handlers, "_client", mock_client)

    spec = {"tenantId": "acme", "sourceType": "s3"}
    patch = _MockPatch()

    await handlers.reconcile_connector(
        spec=spec, name="acme-s3", namespace="ai-pipeline", patch=patch
    )

    mock_client.apply_deployment.assert_called_once()
    mock_client.apply_cronjob.assert_not_called()
    assert patch.status["state"] == "Running"


@pytest.mark.asyncio
async def test_dataconnector_creates_cronjob_with_poll_interval(monkeypatch):
    """pollInterval present → apply_cronjob called; apply_deployment never called."""
    from unittest.mock import AsyncMock

    mock_client = AsyncMock()
    monkeypatch.setattr(handlers, "_client", mock_client)

    spec = {"tenantId": "acme", "sourceType": "s3", "pollInterval": "5m"}
    patch = _MockPatch()

    await handlers.reconcile_connector(
        spec=spec, name="acme-s3", namespace="ai-pipeline", patch=patch
    )

    mock_client.apply_cronjob.assert_called_once()
    mock_client.apply_deployment.assert_not_called()
    assert patch.status["state"] == "Running"


# ---------------------------------------------------------------------------
# RBAC — exact secret names, no wildcards
# ---------------------------------------------------------------------------

def test_connector_role_lists_exact_secret_names():
    """Role resourceNames must list exact secret name — no wildcards allowed."""
    role = rbac.make_connector_role("acme-s3", "ai-pipeline")

    secrets_rule = next(
        r for r in role["rules"] if "secrets" in r.get("resources", [])
    )

    # Exact name: connector-{connector_name}-creds
    assert secrets_rule["resourceNames"] == ["connector-acme-s3-creds"]

    # Audit must find no wildcards
    offences = rbac.audit_role_for_wildcards(role)
    assert offences == [], f"Wildcard found in role: {offences}"
