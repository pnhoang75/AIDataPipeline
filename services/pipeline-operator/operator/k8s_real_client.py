"""Real kubernetes-asyncio implementation of K8sClient for in-cluster use."""
from __future__ import annotations
import datetime

from kubernetes_asyncio import client, config
from kubernetes_asyncio.client import ApiException

try:
    from .k8s_client import K8sClient
except ImportError:
    from k8s_client import K8sClient  # type: ignore[no-redef]

_KAFKA_CLUSTER = "ai-pipeline-kafka"
_KAFKA_API_VERSION = "v1"  # Strimzi >= 0.40 dropped v1beta2
_FIELD_MANAGER = "pipeline-operator"


class K8sRealClient(K8sClient):
    """Kubernetes API client backed by kubernetes-asyncio."""

    def __init__(self) -> None:
        self._apps_v1: client.AppsV1Api | None = None
        self._batch_v1: client.BatchV1Api | None = None
        self._core_v1: client.CoreV1Api | None = None
        self._rbac_v1: client.RbacAuthorizationV1Api | None = None
        self._custom: client.CustomObjectsApi | None = None

    async def initialize(self) -> None:
        """Load kubeconfig or in-cluster credentials and create API clients."""
        try:
            config.load_incluster_config()
        except config.ConfigException:
            await config.load_kube_config()
        self._apps_v1 = client.AppsV1Api()
        self._batch_v1 = client.BatchV1Api()
        self._core_v1 = client.CoreV1Api()
        self._rbac_v1 = client.RbacAuthorizationV1Api()
        self._custom = client.CustomObjectsApi()

    # ── Kafka CRs ────────────────────────────────────────────────────────────

    async def apply_kafka_topic(
        self, name: str, namespace: str, partitions: int = 4
    ) -> None:
        body = {
            f"apiVersion": f"kafka.strimzi.io/{_KAFKA_API_VERSION}",
            "kind": "KafkaTopic",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {"strimzi.io/cluster": _KAFKA_CLUSTER},
            },
            "spec": {"partitions": partitions, "replicas": 1},
        }
        await self._apply_custom(
            group="kafka.strimzi.io",
            version=_KAFKA_API_VERSION,
            plural="kafkatopics",
            namespace=namespace,
            body=body,
        )

    async def apply_kafka_user(
        self,
        name: str,
        namespace: str,
        topic: str,
        operations: list[str],
    ) -> None:
        body = {
            f"apiVersion": f"kafka.strimzi.io/{_KAFKA_API_VERSION}",
            "kind": "KafkaUser",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {"strimzi.io/cluster": _KAFKA_CLUSTER},
            },
            "spec": {
                "authentication": {"type": "tls"},
                "authorization": {
                    "type": "simple",
                    "acls": [
                        {
                            "resource": {
                                "type": "topic",
                                "name": topic,
                                "patternType": "literal",
                            },
                            "operations": operations,
                        }
                    ],
                },
            },
        }
        await self._apply_custom(
            group="kafka.strimzi.io",
            version=_KAFKA_API_VERSION,
            plural="kafkausers",
            namespace=namespace,
            body=body,
        )

    # ── Workloads ─────────────────────────────────────────────────────────────

    async def apply_deployment(
        self, name: str, namespace: str, spec: dict
    ) -> None:
        body = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {"app": name, "managed-by": _FIELD_MANAGER},
            },
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": {"app": name}},
                "template": {
                    "metadata": {"labels": {"app": name}},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 65534,
                            "runAsGroup": 65534,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        },
                        "containers": [
                            {
                                "name": "connector",
                                "image": "registry.k8s.io/pause:3.9",
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "readOnlyRootFilesystem": True,
                                    "capabilities": {"drop": ["ALL"]},
                                },
                                "resources": {
                                    "requests": {"cpu": "10m", "memory": "16Mi"},
                                    "limits": {"cpu": "100m", "memory": "64Mi"},
                                },
                            }
                        ],
                    },
                },
            },
        }
        try:
            await self._apps_v1.create_namespaced_deployment(
                namespace=namespace, body=body
            )
        except ApiException as exc:
            if exc.status == 409:
                await self._apps_v1.patch_namespaced_deployment(
                    name=name, namespace=namespace, body=body
                )
            else:
                raise

    async def apply_cronjob(
        self, name: str, namespace: str, schedule: str, spec: dict
    ) -> None:
        body = {
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {"app": name, "managed-by": _FIELD_MANAGER},
            },
            "spec": {
                "schedule": schedule,
                "jobTemplate": {
                    "spec": {
                        "template": {
                            "spec": {
                                "automountServiceAccountToken": False,
                                "securityContext": {
                                    "runAsNonRoot": True,
                                    "runAsUser": 65534,
                                    "runAsGroup": 65534,
                                    "seccompProfile": {"type": "RuntimeDefault"},
                                },
                                "restartPolicy": "OnFailure",
                                "containers": [
                                    {
                                        "name": "watcher",
                                        "image": "registry.k8s.io/pause:3.9",
                                        "securityContext": {
                                            "allowPrivilegeEscalation": False,
                                            "readOnlyRootFilesystem": True,
                                            "capabilities": {"drop": ["ALL"]},
                                        },
                                        "resources": {
                                            "requests": {
                                                "cpu": "10m",
                                                "memory": "16Mi",
                                            },
                                            "limits": {
                                                "cpu": "50m",
                                                "memory": "32Mi",
                                            },
                                        },
                                    }
                                ],
                            }
                        }
                    }
                },
            },
        }
        try:
            await self._batch_v1.create_namespaced_cron_job(
                namespace=namespace, body=body
            )
        except ApiException as exc:
            if exc.status == 409:
                await self._batch_v1.patch_namespaced_cron_job(
                    name=name, namespace=namespace, body=body
                )
            else:
                raise

    # ── RBAC ─────────────────────────────────────────────────────────────────

    async def apply_role(self, role: dict) -> None:
        name = role["metadata"]["name"]
        namespace = role["metadata"]["namespace"]
        try:
            await self._rbac_v1.create_namespaced_role(namespace=namespace, body=role)
        except ApiException as exc:
            if exc.status == 409:
                await self._rbac_v1.patch_namespaced_role(
                    name=name, namespace=namespace, body=role
                )
            else:
                raise

    async def apply_role_binding(self, rb: dict) -> None:
        name = rb["metadata"]["name"]
        namespace = rb["metadata"]["namespace"]
        try:
            await self._rbac_v1.create_namespaced_role_binding(
                namespace=namespace, body=rb
            )
        except ApiException as exc:
            if exc.status == 409:
                await self._rbac_v1.patch_namespaced_role_binding(
                    name=name, namespace=namespace, body=rb
                )
            else:
                raise

    async def delete_role(self, name: str, namespace: str) -> None:
        try:
            await self._rbac_v1.delete_namespaced_role(name=name, namespace=namespace)
        except ApiException as exc:
            if exc.status != 404:
                raise

    async def delete_role_binding(self, name: str, namespace: str) -> None:
        try:
            await self._rbac_v1.delete_namespaced_role_binding(
                name=name, namespace=namespace
            )
        except ApiException as exc:
            if exc.status != 404:
                raise

    # ── Workload deletes ──────────────────────────────────────────────────────

    async def delete_deployment(self, name: str, namespace: str) -> None:
        try:
            await self._apps_v1.delete_namespaced_deployment(
                name=name, namespace=namespace
            )
        except ApiException as exc:
            if exc.status != 404:
                raise

    async def delete_cronjob(self, name: str, namespace: str) -> None:
        try:
            await self._batch_v1.delete_namespaced_cron_job(
                name=name, namespace=namespace
            )
        except ApiException as exc:
            if exc.status != 404:
                raise

    async def delete_kafka_user(self, name: str, namespace: str) -> None:
        try:
            await self._custom.delete_namespaced_custom_object(
                group="kafka.strimzi.io",
                version=_KAFKA_API_VERSION,
                namespace=namespace,
                plural="kafkausers",
                name=name,
            )
        except ApiException as exc:
            if exc.status != 404:
                raise

    # ── ConfigMap / rollout ───────────────────────────────────────────────────

    async def patch_configmap(
        self, name: str, namespace: str, data: dict
    ) -> None:
        try:
            await self._core_v1.patch_namespaced_config_map(
                name=name, namespace=namespace, body={"data": data}
            )
        except ApiException as exc:
            if exc.status != 404:
                raise

    async def rollout_restart(
        self, kind: str, name: str, namespace: str
    ) -> None:
        ts = datetime.datetime.utcnow().isoformat() + "Z"
        patch = {
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt": ts
                        }
                    }
                }
            }
        }
        try:
            await self._apps_v1.patch_namespaced_deployment(
                name=name, namespace=namespace, body=patch
            )
        except ApiException as exc:
            if exc.status != 404:
                raise

    # ── Helper ────────────────────────────────────────────────────────────────

    async def _apply_custom(
        self,
        group: str,
        version: str,
        plural: str,
        namespace: str,
        body: dict,
    ) -> None:
        name = body["metadata"]["name"]
        try:
            await self._custom.create_namespaced_custom_object(
                group=group,
                version=version,
                namespace=namespace,
                plural=plural,
                body=body,
            )
        except ApiException as exc:
            if exc.status == 409:
                await self._custom.patch_namespaced_custom_object(
                    group=group,
                    version=version,
                    namespace=namespace,
                    plural=plural,
                    name=name,
                    body=body,
                )
            else:
                raise
