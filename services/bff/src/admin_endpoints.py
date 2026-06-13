import json
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

import k8s_client
from auth import JWTClaims, require_admin

NAMESPACE = "ai-pipeline"
_CONNECTOR_COMPONENT = "connector"
_CONNECTOR_LABEL = "app.kubernetes.io/component"
_PIPELINE_PART_OF = "app.kubernetes.io/part-of=ai-pipeline"
PIPELINE_CONFIG_CM = "pipeline-config"
_DC_GROUP = "pipeline.example.com"
_DC_VERSION = "v1alpha1"
_DC_PLURAL = "dataconnectors"

router = APIRouter(prefix="/api/admin", tags=["admin"])


# ── Pydantic models ───────────────────────────────────────────────────────────

class ConnectorCreate(BaseModel):
    name: str
    source_type: str
    config: Dict[str, Any] = {}
    start_paused: bool = False


class ConnectorPatch(BaseModel):
    name: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    start_paused: Optional[bool] = None


class ConnectorResponse(BaseModel):
    id: str
    name: str
    source_type: str
    config: Dict[str, Any]
    tenant_id: str
    start_paused: bool


class PodStatusItem(BaseModel):
    name: str
    status: str
    ready: bool


class PipelineStatus(BaseModel):
    services: List[PodStatusItem]
    tenant: str


class PipelineConfig(BaseModel):
    chunk_size: int = 512
    chunk_overlap: int = 50
    embedding_backend: str = "bge-small-en-v1.5"
    milvus_index_type: str = "IVF_FLAT"
    milvus_nlist: int = 128


# ── Helpers ───────────────────────────────────────────────────────────────────

def _cm_to_connector(cm: Dict) -> ConnectorResponse:
    data = cm["data"]
    return ConnectorResponse(
        id=data["id"],
        name=data["name"],
        source_type=data["source_type"],
        config=json.loads(data.get("config", "{}")),
        tenant_id=data["tenant_id"],
        start_paused=data.get("start_paused", "false").lower() == "true",
    )


def _connector_labels(org_id: str) -> Dict[str, str]:
    return {_CONNECTOR_LABEL: _CONNECTOR_COMPONENT, "tenant-id": org_id}


def _connector_label_selector(org_id: str) -> str:
    return f"{_CONNECTOR_LABEL}={_CONNECTOR_COMPONENT},tenant-id={org_id}"


# ── Pipeline status ───────────────────────────────────────────────────────────

@router.get("/pipeline/status", response_model=PipelineStatus)
async def pipeline_status(claims: JWTClaims = Depends(require_admin)):
    pods = await k8s_client.list_pods(NAMESPACE, label_selector=_PIPELINE_PART_OF)
    return PipelineStatus(
        services=[PodStatusItem(**p) for p in pods],
        tenant=claims.org_id,
    )


# ── Connector CRUD ────────────────────────────────────────────────────────────

@router.get("/connectors", response_model=List[ConnectorResponse])
async def list_connectors(claims: JWTClaims = Depends(require_admin)):
    cms = await k8s_client.list_configmaps(
        NAMESPACE, label_selector=_connector_label_selector(claims.org_id)
    )
    return [_cm_to_connector(cm) for cm in cms]


@router.post("/connectors", response_model=ConnectorResponse, status_code=201)
async def create_connector(
    body: ConnectorCreate, claims: JWTClaims = Depends(require_admin)
):
    connector_id = str(uuid.uuid4())
    cm_name = f"connector-{connector_id}"
    labels = _connector_labels(claims.org_id)
    data = {
        "id": connector_id,
        "name": body.name,
        "source_type": body.source_type,
        "config": json.dumps(body.config),
        "tenant_id": claims.org_id,
        "start_paused": str(body.start_paused).lower(),
    }
    await k8s_client.create_configmap(NAMESPACE, cm_name, data, labels)
    cr = {
        "apiVersion": f"{_DC_GROUP}/{_DC_VERSION}",
        "kind": "DataConnector",
        "metadata": {"name": cm_name, "namespace": NAMESPACE, "labels": labels},
        "spec": {
            "sourceType": body.source_type,
            "configMapRef": cm_name,
            "tenantId": claims.org_id,
            "startPaused": body.start_paused,
        },
    }
    await k8s_client.create_custom_object(_DC_GROUP, _DC_VERSION, NAMESPACE, _DC_PLURAL, cr)
    return ConnectorResponse(
        id=connector_id,
        name=body.name,
        source_type=body.source_type,
        config=body.config,
        tenant_id=claims.org_id,
        start_paused=body.start_paused,
    )


@router.patch("/connectors/{connector_id}", response_model=ConnectorResponse)
async def update_connector(
    connector_id: str,
    body: ConnectorPatch,
    claims: JWTClaims = Depends(require_admin),
):
    cm_name = f"connector-{connector_id}"
    cm = await k8s_client.get_configmap(NAMESPACE, cm_name)
    if cm is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "NOT_FOUND", "message": "Connector not found"},
        )
    if cm["data"].get("tenant_id") != claims.org_id:
        raise HTTPException(
            status_code=403,
            detail={"error": "FORBIDDEN", "message": "Connector belongs to another tenant"},
        )
    data = dict(cm["data"])
    if body.name is not None:
        data["name"] = body.name
    if body.config is not None:
        data["config"] = json.dumps(body.config)
    if body.start_paused is not None:
        data["start_paused"] = str(body.start_paused).lower()
    updated = await k8s_client.patch_configmap(NAMESPACE, cm_name, data)
    return _cm_to_connector(updated)


@router.delete("/connectors/{connector_id}", status_code=204)
async def delete_connector(
    connector_id: str,
    claims: JWTClaims = Depends(require_admin),
):
    cm_name = f"connector-{connector_id}"
    cm = await k8s_client.get_configmap(NAMESPACE, cm_name)
    if cm is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "NOT_FOUND", "message": "Connector not found"},
        )
    if cm["data"].get("tenant_id") != claims.org_id:
        raise HTTPException(
            status_code=403,
            detail={"error": "FORBIDDEN", "message": "Connector belongs to another tenant"},
        )
    await k8s_client.delete_configmap(NAMESPACE, cm_name)
    try:
        await k8s_client.delete_custom_object(
            _DC_GROUP, _DC_VERSION, NAMESPACE, _DC_PLURAL, cm_name
        )
    except Exception:
        pass  # CR may not exist if it was never created


# ── Pipeline config ───────────────────────────────────────────────────────────

@router.get("/pipeline/config", response_model=PipelineConfig)
async def get_pipeline_config(claims: JWTClaims = Depends(require_admin)):
    cm = await k8s_client.get_configmap(NAMESPACE, PIPELINE_CONFIG_CM)
    if cm is None:
        return PipelineConfig()
    d = cm["data"]
    return PipelineConfig(
        chunk_size=int(d.get("chunk_size", 512)),
        chunk_overlap=int(d.get("chunk_overlap", 50)),
        embedding_backend=d.get("embedding_backend", "bge-small-en-v1.5"),
        milvus_index_type=d.get("milvus_index_type", "IVF_FLAT"),
        milvus_nlist=int(d.get("milvus_nlist", 128)),
    )


@router.put("/pipeline/config", response_model=PipelineConfig)
async def update_pipeline_config(
    body: PipelineConfig,
    claims: JWTClaims = Depends(require_admin),
):
    data = {
        "chunk_size": str(body.chunk_size),
        "chunk_overlap": str(body.chunk_overlap),
        "embedding_backend": body.embedding_backend,
        "milvus_index_type": body.milvus_index_type,
        "milvus_nlist": str(body.milvus_nlist),
    }
    existing = await k8s_client.get_configmap(NAMESPACE, PIPELINE_CONFIG_CM)
    if existing is None:
        labels = {"app.kubernetes.io/component": "pipeline-config"}
        await k8s_client.create_configmap(NAMESPACE, PIPELINE_CONFIG_CM, data, labels)
    else:
        await k8s_client.patch_configmap(NAMESPACE, PIPELINE_CONFIG_CM, data)
    return body
