"""Async Kubernetes client for Pipeline Operator resource management.

All methods are no-ops in the base class; production use loads kubeconfig
or in-cluster credentials via kubernetes-asyncio.  Unit tests replace the
module-level singleton with AsyncMock.
"""
from __future__ import annotations


class K8sClient:
    """Thin async interface over the K8s API for operator use cases."""

    async def apply_kafka_topic(
        self, name: str, namespace: str, partitions: int = 4
    ) -> None:
        pass

    async def apply_kafka_user(
        self,
        name: str,
        namespace: str,
        topic: str,
        operations: list[str],
    ) -> None:
        pass

    async def apply_deployment(
        self, name: str, namespace: str, spec: dict
    ) -> None:
        pass

    async def apply_cronjob(
        self, name: str, namespace: str, schedule: str, spec: dict
    ) -> None:
        pass

    async def apply_role(self, role: dict) -> None:
        pass

    async def apply_role_binding(self, rb: dict) -> None:
        pass

    async def delete_role(self, name: str, namespace: str) -> None:
        pass

    async def delete_role_binding(self, name: str, namespace: str) -> None:
        pass

    async def delete_deployment(self, name: str, namespace: str) -> None:
        pass

    async def delete_cronjob(self, name: str, namespace: str) -> None:
        pass

    async def delete_kafka_user(self, name: str, namespace: str) -> None:
        pass

    async def patch_configmap(
        self, name: str, namespace: str, data: dict
    ) -> None:
        pass

    async def rollout_restart(
        self, kind: str, name: str, namespace: str
    ) -> None:
        pass
