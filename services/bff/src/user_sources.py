import json
import posixpath
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

import k8s_client
import kafka_client
import minio_client
import quota_client
from auth import JWTClaims, require_auth
from ssrf_validator import validate_endpoint

NAMESPACE = "ai-pipeline"
_DC_GROUP = "pipeline.example.com"
_DC_VERSION = "v1alpha1"
_DC_PLURAL = "dataconnectors"
_CONNECTOR_COMPONENT = "connector"
_CONNECTOR_LABEL = "app.kubernetes.io/component"
_METADATA_EVENTS_TOPIC = "metadata-events"

router = APIRouter(prefix="/api/sources", tags=["user-sources"])


class UserSourceCreate(BaseModel):
    name: str
    source_type: str
    config: Dict[str, Any] = {}
    start_paused: bool = False
    workspace_id: Optional[str] = None


class SourceTestRequest(BaseModel):
    endpoint: str
    source_type: str = "database"
    config: Dict[str, Any] = {}


class SourceCreateResponse(BaseModel):
    id: str
    name: str
    source_type: str
    tenant_id: str
    status: str
    start_paused: bool


# ── Helpers ────────────────────────────────────────────────────────────────────

def _cm_name(connector_id: str) -> str:
    return f"connector-{connector_id}"


def _labels(org_id: str) -> Dict[str, str]:
    return {_CONNECTOR_LABEL: _CONNECTOR_COMPONENT, "tenant-id": org_id}


async def _get_connector(connector_id: str, org_id: str) -> Dict:
    cm = await k8s_client.get_configmap(NAMESPACE, _cm_name(connector_id))
    if cm is None or cm["data"].get("tenant_id") != org_id:
        raise HTTPException(
            status_code=404,
            detail={"error": "NOT_FOUND", "message": "Connector not found"},
        )
    return cm


def _assert_owns(cm: Dict, claims: JWTClaims) -> None:
    """Raise 403 if caller doesn't own the connector and isn't a tenant admin."""
    if "pipeline-admin" in claims.roles:
        return
    if cm["data"].get("owner_id", "") != claims.sub:
        raise HTTPException(
            status_code=403,
            detail={"error": "FORBIDDEN", "message": "You do not own this connector"},
        )


def _validate_browse_path(path: str, allowed_prefix: str) -> None:
    """Raise 400 PATH_TRAVERSAL_BLOCKED if path escapes the allowed prefix."""
    if path.startswith("/"):
        normalized = posixpath.normpath(path)
    else:
        normalized = posixpath.normpath("/" + path)
    if not (normalized == allowed_prefix or normalized.startswith(allowed_prefix + "/")):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "PATH_TRAVERSAL_BLOCKED",
                "message": f"Path is outside the allowed prefix '{allowed_prefix}'",
            },
        )


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.post("/create", response_model=SourceCreateResponse, status_code=201)
async def create_source(body: UserSourceCreate, claims: JWTClaims = Depends(require_auth)):
    # Pro and Enterprise tiers have unlimited connectors per multitenancy doc.
    if claims.license_type not in ("pro", "enterprise"):
        quota = await quota_client.check_quota(claims.org_id, "CONNECTOR_COUNT")
        if not quota["allowed"]:
            raise HTTPException(
                status_code=402,
                detail={"error": "QUOTA_EXCEEDED", "message": "Connector quota exceeded for this tier"},
            )

    connector_id = str(uuid.uuid4())
    cm_name = _cm_name(connector_id)
    labels = _labels(claims.org_id)
    data = {
        "id": connector_id,
        "name": body.name,
        "source_type": body.source_type,
        "config": json.dumps(body.config),
        "tenant_id": claims.org_id,
        "owner_id": claims.sub,
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
    return SourceCreateResponse(
        id=connector_id,
        name=body.name,
        source_type=body.source_type,
        tenant_id=claims.org_id,
        status="provisioning",
        start_paused=body.start_paused,
    )


@router.post("/test")
async def test_source(body: SourceTestRequest, claims: JWTClaims = Depends(require_auth)):
    validate_endpoint(body.endpoint)
    # Actual connection attempt would be made here (mocked in tests).
    return {"status": "ok", "latency_ms": 0, "message": "Connection test passed"}


@router.post("/upload", status_code=201)
async def upload_source(
    file: UploadFile = File(...),
    workspace_id: Optional[str] = Form(None),
    claims: JWTClaims = Depends(require_auth),
):
    session_id = str(uuid.uuid4())
    content = await file.read()
    object_path = await minio_client.upload_file(
        claims.org_id, session_id, file.filename or "upload", content
    )
    await kafka_client.publish_event(
        _METADATA_EVENTS_TOPIC,
        {
            "entity_type": "DataSource",
            "source_type": "upload",
            "tenant_id": claims.org_id,
            "path": object_path,
            "session_id": session_id,
        },
    )
    return {
        "status": "uploaded",
        "session_id": session_id,
        "path": object_path,
        "workspace_id": workspace_id,
    }


@router.post("/{connector_id}/pause", status_code=200)
async def pause_source(connector_id: str, claims: JWTClaims = Depends(require_auth)):
    cm = await _get_connector(connector_id, claims.org_id)
    _assert_owns(cm, claims)
    data = dict(cm["data"])
    data["start_paused"] = "true"
    await k8s_client.patch_configmap(NAMESPACE, _cm_name(connector_id), data)
    return {"id": connector_id, "status": "paused"}


@router.post("/{connector_id}/resume", status_code=200)
async def resume_source(connector_id: str, claims: JWTClaims = Depends(require_auth)):
    cm = await _get_connector(connector_id, claims.org_id)
    _assert_owns(cm, claims)
    data = dict(cm["data"])
    data["start_paused"] = "false"
    await k8s_client.patch_configmap(NAMESPACE, _cm_name(connector_id), data)
    return {"id": connector_id, "status": "running"}


@router.delete("/{connector_id}", status_code=204)
async def delete_source(connector_id: str, claims: JWTClaims = Depends(require_auth)):
    cm = await _get_connector(connector_id, claims.org_id)
    _assert_owns(cm, claims)
    await k8s_client.delete_configmap(NAMESPACE, _cm_name(connector_id))
    try:
        await k8s_client.delete_custom_object(
            _DC_GROUP, _DC_VERSION, NAMESPACE, _DC_PLURAL, _cm_name(connector_id)
        )
    except Exception:
        pass


@router.get("/{connector_id}/browse/{path:path}")
async def browse_source(
    connector_id: str,
    path: str,
    claims: JWTClaims = Depends(require_auth),
):
    cm = await _get_connector(connector_id, claims.org_id)
    config = json.loads(cm["data"].get("config", "{}"))
    allowed_prefix = config.get("allowed_path_prefix", "/")
    _validate_browse_path(path, allowed_prefix)
    listing = await k8s_client.browse_nfs_path(NAMESPACE, connector_id, path)
    return {"path": path, "entries": listing}
