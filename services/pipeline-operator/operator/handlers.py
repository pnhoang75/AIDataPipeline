"""kopf event handlers for the Pipeline Operator.

Business-logic functions (reconcile_connector, delete_connector) are plain
async functions so tests can call them directly with a mocked _client,
without needing kopf itself to run.

Import path note: this module supports two import modes:
  - as part of the 'operator' package  (relative imports via try/except)
  - directly with operator/ on sys.path (fallback direct imports)
"""
from __future__ import annotations

try:
    import kopf as _kopf
    _kopf_available = True
except ImportError:  # pragma: no cover
    _kopf_available = False

try:
    from .k8s_client import K8sClient
    from .rbac import make_connector_role, make_connector_role_binding
except ImportError:
    from k8s_client import K8sClient  # type: ignore[no-redef]
    from rbac import make_connector_role, make_connector_role_binding  # type: ignore[no-redef]

# Module-level singleton; replace with AsyncMock in unit tests.
_client: K8sClient = K8sClient()

_KAFKA_CLUSTER = "ai-pipeline-kafka"
_KAFKA_TOPIC = "raw-documents"
_CONNECTOR_SA = "connector-sa"


# ---------------------------------------------------------------------------
# DataConnector reconcile (create / update)
# ---------------------------------------------------------------------------

async def reconcile_connector(
    spec: dict,
    name: str,
    namespace: str,
    patch: object,
    **kwargs,
) -> None:
    """Create/update all sub-resources for a DataConnector CR."""
    tenant_id = spec["tenantId"]
    source_type = spec["sourceType"]

    await _client.apply_kafka_topic(_KAFKA_TOPIC, namespace, partitions=4)

    kafka_user_name = f"connector-{tenant_id}-{source_type}"
    await _client.apply_kafka_user(
        name=kafka_user_name,
        namespace=namespace,
        topic=_KAFKA_TOPIC,
        operations=["Write", "Describe"],
    )

    role = make_connector_role(name, namespace)
    rb = make_connector_role_binding(name, _CONNECTOR_SA, namespace)
    await _client.apply_role(role)
    await _client.apply_role_binding(rb)

    workload_name = f"connector-{name}"
    if spec.get("pollInterval"):
        schedule = _poll_interval_to_cron(spec["pollInterval"])
        await _client.apply_cronjob(workload_name, namespace, schedule, {})
    else:
        await _client.apply_deployment(workload_name, namespace, {})

    patch.status["state"] = "Running"  # type: ignore[index]


# ---------------------------------------------------------------------------
# DataConnector delete
# ---------------------------------------------------------------------------

async def delete_connector(
    spec: dict,
    name: str,
    namespace: str,
    **kwargs,
) -> None:
    """Remove all sub-resources owned by a DataConnector CR."""
    tenant_id = spec.get("tenantId", "")
    source_type = spec.get("sourceType", "")

    await _client.delete_role(f"connector-{name}-role", namespace)
    await _client.delete_role_binding(f"connector-{name}-rb", namespace)

    workload_name = f"connector-{name}"
    if spec.get("pollInterval"):
        await _client.delete_cronjob(workload_name, namespace)
    else:
        await _client.delete_deployment(workload_name, namespace)

    await _client.delete_kafka_user(
        f"connector-{tenant_id}-{source_type}", namespace
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _poll_interval_to_cron(poll_interval: str) -> str:
    """Convert a simple poll interval (e.g. '5m', '1h') to a cron schedule."""
    if poll_interval.endswith("m"):
        minutes = int(poll_interval[:-1])
        return f"*/{minutes} * * * *"
    if poll_interval.endswith("h"):
        hours = int(poll_interval[:-1])
        return f"0 */{hours} * * *"
    return "*/5 * * * *"


# ---------------------------------------------------------------------------
# kopf handler registrations (only when kopf is available)
# ---------------------------------------------------------------------------

if _kopf_available:
    @_kopf.on.create("ai-pipeline.io", "v1alpha1", "dataconnectors")  # type: ignore[misc]
    @_kopf.on.update("ai-pipeline.io", "v1alpha1", "dataconnectors")
    async def handle_connector_create_update(spec, name, namespace, patch, **kwargs):
        await reconcile_connector(
            spec=spec, name=name, namespace=namespace, patch=patch, **kwargs
        )

    @_kopf.on.delete("ai-pipeline.io", "v1alpha1", "dataconnectors")
    async def handle_connector_delete(spec, name, namespace, **kwargs):
        await delete_connector(
            spec=spec, name=name, namespace=namespace, **kwargs
        )
