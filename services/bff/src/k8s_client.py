import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_initialized = False


async def _ensure_initialized() -> None:
    global _initialized
    if _initialized:
        return
    from kubernetes_asyncio import config
    try:
        config.load_incluster_config()
    except Exception:
        await config.load_kube_config()
    _initialized = True


async def list_pods(namespace: str, label_selector: str = "") -> List[Dict]:
    await _ensure_initialized()
    from kubernetes_asyncio import client
    from kubernetes_asyncio.client import ApiClient
    async with ApiClient() as api:
        v1 = client.CoreV1Api(api)
        resp = await v1.list_namespaced_pod(namespace, label_selector=label_selector)
    result = []
    for p in resp.items:
        conditions = p.status.conditions or []
        ready = any(c.type == "Ready" and c.status == "True" for c in conditions)
        result.append({
            "name": p.metadata.name,
            "status": p.status.phase or "Unknown",
            "ready": ready,
        })
    return result


async def list_configmaps(namespace: str, label_selector: str = "") -> List[Dict]:
    await _ensure_initialized()
    from kubernetes_asyncio import client
    from kubernetes_asyncio.client import ApiClient
    async with ApiClient() as api:
        v1 = client.CoreV1Api(api)
        resp = await v1.list_namespaced_config_map(namespace, label_selector=label_selector)
    return [
        {"name": cm.metadata.name, "data": cm.data or {}, "labels": cm.metadata.labels or {}}
        for cm in resp.items
    ]


async def get_configmap(namespace: str, name: str) -> Optional[Dict]:
    await _ensure_initialized()
    from kubernetes_asyncio import client
    from kubernetes_asyncio.client import ApiClient
    from kubernetes_asyncio.client.exceptions import ApiException
    async with ApiClient() as api:
        v1 = client.CoreV1Api(api)
        try:
            cm = await v1.read_namespaced_config_map(name, namespace)
            return {"name": cm.metadata.name, "data": cm.data or {}, "labels": cm.metadata.labels or {}}
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise


async def create_configmap(namespace: str, name: str, data: Dict, labels: Dict) -> Dict:
    await _ensure_initialized()
    from kubernetes_asyncio import client
    from kubernetes_asyncio.client import ApiClient
    async with ApiClient() as api:
        v1 = client.CoreV1Api(api)
        body = client.V1ConfigMap(
            metadata=client.V1ObjectMeta(name=name, namespace=namespace, labels=labels),
            data=data,
        )
        cm = await v1.create_namespaced_config_map(namespace, body)
    return {"name": cm.metadata.name, "data": cm.data or {}, "labels": cm.metadata.labels or {}}


async def patch_configmap(namespace: str, name: str, data: Dict) -> Dict:
    await _ensure_initialized()
    from kubernetes_asyncio import client
    from kubernetes_asyncio.client import ApiClient
    async with ApiClient() as api:
        v1 = client.CoreV1Api(api)
        cm = await v1.patch_namespaced_config_map(name, namespace, {"data": data})
    return {"name": cm.metadata.name, "data": cm.data or {}, "labels": cm.metadata.labels or {}}


async def delete_configmap(namespace: str, name: str) -> None:
    await _ensure_initialized()
    from kubernetes_asyncio import client
    from kubernetes_asyncio.client import ApiClient
    async with ApiClient() as api:
        v1 = client.CoreV1Api(api)
        await v1.delete_namespaced_config_map(name, namespace)


async def create_custom_object(
    group: str, version: str, namespace: str, plural: str, body: Dict
) -> Dict:
    await _ensure_initialized()
    from kubernetes_asyncio import client
    from kubernetes_asyncio.client import ApiClient
    async with ApiClient() as api:
        custom = client.CustomObjectsApi(api)
        return await custom.create_namespaced_custom_object(group, version, namespace, plural, body)


async def delete_custom_object(
    group: str, version: str, namespace: str, plural: str, name: str
) -> None:
    await _ensure_initialized()
    from kubernetes_asyncio import client
    from kubernetes_asyncio.client import ApiClient
    async with ApiClient() as api:
        custom = client.CustomObjectsApi(api)
        await custom.delete_namespaced_custom_object(group, version, namespace, plural, name)


async def browse_nfs_path(namespace: str, connector_id: str, path: str) -> List[Dict]:
    """List files at an NFS path via kubectl exec on the connector pod."""
    await _ensure_initialized()
    from kubernetes_asyncio import client, stream
    from kubernetes_asyncio.client import ApiClient
    pod_label = f"connector-id={connector_id}"
    pods = await list_pods(namespace, label_selector=pod_label)
    if not pods:
        return []
    async with ApiClient() as api:
        v1 = client.CoreV1Api(api)
        resp = await stream.ws_client.WsApiClient(api).connect_get_namespaced_pod_exec(
            pods[0]["name"],
            namespace,
            command=["ls", "-la", path],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
        )
    return [{"name": line.strip()} for line in (resp or "").splitlines() if line.strip()]
