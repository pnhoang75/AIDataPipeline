"""kopf event handlers for the Pipeline Operator.

Business-logic functions (reconcile_connector, delete_connector) are plain
async functions so tests can call them directly with a mocked _client,
without needing kopf itself to run.

Import path note: this module supports two import modes:
  - as part of the 'operator' package  (relative imports via try/except)
  - directly with operator/ on sys.path (fallback direct imports)
"""
from __future__ import annotations

import asyncio
import logging
import os

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

logger = logging.getLogger(__name__)

# Module-level singleton; replace with AsyncMock in unit tests.
_client: K8sClient = K8sClient()

_METADATA_SERVICE_URL = os.environ.get(
    "METADATA_SERVICE_URL", "http://metadata-service.ai-pipeline.svc:8000"
)

_KAFKA_CLUSTER = "ai-pipeline-kafka"
_KAFKA_TOPIC = "raw-documents"
_CONNECTOR_SA = "connector-sa"
_UPLOAD_WATCHER_SCHEDULE = "* * * * *"

# Upgrade state machine constants
_SERVICES_UPGRADE_ORDER = ["doc-processor", "embedding-worker", "rag-api"]
_CONNECTOR_LABEL_SELECTOR = "pipeline.ai-pipeline.io/kind=connector"
_KAFKA_CONSUMER_GROUP = "doc-processor-group"
_RAG_API_HEALTH_URL = os.environ.get(
    "RAG_API_HEALTH_URL", "http://rag-api.ai-pipeline.svc:8000/v1/health"
)
_UPGRADE_DRAIN_TIMEOUT_S = int(os.environ.get("UPGRADE_DRAIN_TIMEOUT_S", "600"))

# Expose TemporaryError so tests don't need to import kopf directly.
if _kopf_available:
    TemporaryError = _kopf.TemporaryError
else:
    class TemporaryError(RuntimeError):  # type: ignore[no-redef]
        def __init__(self, message: str, delay: int = 0) -> None:
            super().__init__(message)
            self.delay = delay


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
# TenantWorkspace reconcile (create / update)
# ---------------------------------------------------------------------------

async def reconcile_workspace(
    spec: dict,
    name: str,
    namespace: str,
    patch: object,
    **kwargs,
) -> None:
    """Provision upload-watcher CronJob for a TenantWorkspace."""
    tenant_id = spec["tenantId"]
    await _client.apply_cronjob(
        f"upload-watcher-{tenant_id}", namespace, _UPLOAD_WATCHER_SCHEDULE, {}
    )
    patch.status["state"] = "Provisioned"  # type: ignore[index]


# ---------------------------------------------------------------------------
# TenantWorkspace delete
# ---------------------------------------------------------------------------

async def delete_workspace(
    spec: dict,
    name: str,
    namespace: str,
    **kwargs,
) -> None:
    """Remove upload-watcher CronJob when TenantWorkspace is deleted."""
    tenant_id = spec.get("tenantId", "")
    await _client.delete_cronjob(f"upload-watcher-{tenant_id}", namespace)


# ---------------------------------------------------------------------------
# EmbeddingConfig reconcile (update only — dimension-change guard)
# ---------------------------------------------------------------------------

async def reconcile_embedding(
    spec: dict,
    old: dict,
    new: dict,
    namespace: str,
    patch: object,
    **kwargs,
) -> None:
    """Apply EmbeddingConfig changes; block dimension changes unless reindexConfirmed."""
    old_dim = (old.get("spec") or {}).get("dimension")
    new_dim = (new.get("spec") or {}).get("dimension")
    if old_dim and new_dim and old_dim != new_dim and not spec.get("reindexConfirmed"):
        patch.status["state"] = "BlockedDimensionChange"  # type: ignore[index]
        raise TemporaryError(
            f"Dimension change {old_dim}→{new_dim} requires re-index. "
            "Set spec.reindexConfirmed: true to proceed.",
            delay=60,
        )
    if _METADATA_SERVICE_URL and spec.get("tenantId"):
        try:
            import httpx
            async with httpx.AsyncClient() as hclient:
                await hclient.post(
                    f"{_METADATA_SERVICE_URL}/api/schema-versions",
                    json={
                        "tenant_id": spec["tenantId"],
                        "embedding_model": spec.get("model", ""),
                        "embedding_dimension": spec.get("dimension", 384),
                        "embedding_backend": spec.get("backend", "local-cpu"),
                        "created_by": "pipeline-operator",
                    },
                    timeout=10.0,
                )
        except Exception as exc:
            logger.warning("failed to record SchemaVersion in metadata service: %s", exc)

    await _client.patch_configmap("pipeline-config", namespace, {
        "EMBEDDING_BACKEND": spec.get("backend", ""),
        "EMBEDDING_MODEL": spec.get("model", ""),
        "EMBEDDING_DEVICE": spec.get("device", ""),
    })
    await _client.rollout_restart("deployment", "embedding-worker", namespace)
    patch.status["state"] = "Applied"  # type: ignore[index]


# ---------------------------------------------------------------------------
# PipelineCluster reconcile — coordinated upgrade / rollback state machine
# ---------------------------------------------------------------------------

async def reconcile_pipeline_cluster(
    spec: dict,
    old: dict,
    new: dict,
    namespace: str,
    patch: object,
    **kwargs,
) -> None:
    """Run a coordinated upgrade or rollback when PipelineCluster.spec.version changes."""
    old_version = (old.get("spec") or {}).get("version", "")
    new_version = (new.get("spec") or {}).get("version", spec.get("version", ""))

    if not old_version or old_version == new_version:
        return

    is_rollback = _version_lt(new_version, old_version)
    services = (
        list(reversed(_SERVICES_UPGRADE_ORDER)) if is_rollback else _SERVICES_UPGRADE_ORDER
    )
    await _run_coordinated_upgrade(namespace, patch, services)


async def _run_coordinated_upgrade(
    namespace: str,
    patch: object,
    services: list[str],
) -> None:
    """Execute the upgrade state machine for the given service roll order."""
    patch.status["upgradeInProgress"] = True  # type: ignore[index]
    patch.status["state"] = "UpgradeInProgress"  # type: ignore[index]

    connector_crons = await _client.list_cronjobs(namespace, _CONNECTOR_LABEL_SELECTOR)
    for cron in connector_crons:
        await _client.suspend_cronjob(cron, namespace)

    await _drain_kafka_lag(namespace)

    for svc in services:
        await _client.rollout_restart("deployment", svc, namespace)
        await _client.wait_deployment_ready(svc, namespace)

    health_status = await _client.http_get(_RAG_API_HEALTH_URL)
    if health_status != 200:
        raise TemporaryError(
            f"RAG API smoke test failed: HTTP {health_status}", delay=30
        )

    for cron in connector_crons:
        await _client.resume_cronjob(cron, namespace)

    patch.status["upgradeInProgress"] = False  # type: ignore[index]
    patch.status["state"] = "Ready"  # type: ignore[index]


async def _drain_kafka_lag(namespace: str) -> None:
    """Poll Kafka consumer lag until 0; raise TemporaryError on timeout."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _UPGRADE_DRAIN_TIMEOUT_S
    while True:
        lag = await _client.get_kafka_consumer_lag(
            _KAFKA_TOPIC, _KAFKA_CONSUMER_GROUP, namespace
        )
        if lag == 0:
            return
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TemporaryError(
                "Kafka consumer lag did not drain within timeout", delay=60
            )
        await asyncio.sleep(min(5.0, remaining))


def _version_lt(v1: str, v2: str) -> bool:
    """Return True if semantic version v1 is strictly less than v2."""
    def _parts(v: str) -> tuple:
        try:
            return tuple(int(x) for x in v.split("."))
        except ValueError:
            return (0,)
    return _parts(v1) < _parts(v2)


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

    @_kopf.on.create("ai-pipeline.io", "v1alpha1", "tenantworkspaces")  # type: ignore[misc]
    @_kopf.on.update("ai-pipeline.io", "v1alpha1", "tenantworkspaces")
    async def handle_workspace_create_update(spec, name, namespace, patch, **kwargs):
        await reconcile_workspace(
            spec=spec, name=name, namespace=namespace, patch=patch, **kwargs
        )

    @_kopf.on.delete("ai-pipeline.io", "v1alpha1", "tenantworkspaces")
    async def handle_workspace_delete(spec, name, namespace, **kwargs):
        await delete_workspace(
            spec=spec, name=name, namespace=namespace, **kwargs
        )

    @_kopf.on.update("ai-pipeline.io", "v1alpha1", "embeddingconfigs", field="spec")  # type: ignore[misc]
    async def handle_embedding_update(spec, old, new, namespace, patch, **kwargs):
        await reconcile_embedding(
            spec=spec, old=old, new=new, namespace=namespace, patch=patch, **kwargs
        )

    @_kopf.on.update("ai-pipeline.io", "v1alpha1", "pipelineclusters", field="spec.version")  # type: ignore[misc]
    async def handle_pipeline_cluster_update(spec, old, new, namespace, patch, **kwargs):
        await reconcile_pipeline_cluster(
            spec=spec, old=old, new=new, namespace=namespace, patch=patch, **kwargs
        )
