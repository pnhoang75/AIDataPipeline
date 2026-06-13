from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

import db_client
import k8s_client
from auth import JWTClaims, require_auth

NAMESPACE = "ai-pipeline"
_CONNECTOR_COMPONENT_LABEL = "app.kubernetes.io/component=connector"

router = APIRouter(prefix="/api", tags=["user"])


class WorkspaceCreate(BaseModel):
    name: str
    description: Optional[str] = None


class WorkspaceResponse(BaseModel):
    id: str
    tenant_id: str
    owner_id: str
    name: str
    description: Optional[str] = None


class SourceResponse(BaseModel):
    id: str
    name: str
    source_type: str
    tenant_id: str


class WorkspaceSourceAdd(BaseModel):
    connector_id: str
    path_prefix: str = ""


class WorkspaceSourceResponse(BaseModel):
    id: str
    workspace_id: str
    connector_id: str
    path_prefix: str


class FileStatus(BaseModel):
    id: str
    connector_id: str
    file_path: str
    ingest_status: str
    file_size_bytes: Optional[int] = None
    chunk_count: Optional[int] = None


def _str(v) -> str:
    return str(v)


# ── Workspaces ─────────────────────────────────────────────────────────────────

@router.get("/workspaces", response_model=List[WorkspaceResponse])
async def list_workspaces(claims: JWTClaims = Depends(require_auth)):
    rows = await db_client.get_workspaces(claims.org_id)
    return [
        WorkspaceResponse(
            id=_str(r["id"]),
            tenant_id=_str(r["tenant_id"]),
            owner_id=_str(r["owner_id"]),
            name=r["name"],
            description=r.get("description"),
        )
        for r in rows
    ]


@router.post("/workspaces", response_model=WorkspaceResponse, status_code=201)
async def create_workspace(body: WorkspaceCreate, claims: JWTClaims = Depends(require_auth)):
    row = await db_client.create_workspace(
        tenant_id=claims.org_id,
        owner_id=claims.sub,
        name=body.name,
        description=body.description,
    )
    return WorkspaceResponse(
        id=_str(row["id"]),
        tenant_id=_str(row["tenant_id"]),
        owner_id=_str(row["owner_id"]),
        name=row["name"],
        description=row.get("description"),
    )


@router.delete("/workspaces/{workspace_id}", status_code=204)
async def delete_workspace(workspace_id: str, claims: JWTClaims = Depends(require_auth)):
    workspace = await db_client.get_workspace(workspace_id, claims.org_id)
    if workspace is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "NOT_FOUND", "message": "Workspace not found"},
        )
    await db_client.delete_workspace(workspace_id, claims.org_id)


# ── Sources ────────────────────────────────────────────────────────────────────

@router.get("/sources", response_model=List[SourceResponse])
async def list_sources(claims: JWTClaims = Depends(require_auth)):
    label_selector = f"{_CONNECTOR_COMPONENT_LABEL},tenant-id={claims.org_id}"
    cms = await k8s_client.list_configmaps(NAMESPACE, label_selector=label_selector)
    return [
        SourceResponse(
            id=cm["data"].get("id", ""),
            name=cm["data"].get("name", ""),
            source_type=cm["data"].get("source_type", ""),
            tenant_id=cm["data"].get("tenant_id", ""),
        )
        for cm in cms
    ]


# ── Workspace sources ──────────────────────────────────────────────────────────

@router.get("/workspaces/{workspace_id}/sources", response_model=List[WorkspaceSourceResponse])
async def list_workspace_sources(workspace_id: str, claims: JWTClaims = Depends(require_auth)):
    workspace = await db_client.get_workspace(workspace_id, claims.org_id)
    if workspace is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "NOT_FOUND", "message": "Workspace not found"},
        )
    rows = await db_client.get_workspace_sources(workspace_id)
    return [
        WorkspaceSourceResponse(
            id=_str(r["id"]),
            workspace_id=_str(r["workspace_id"]),
            connector_id=r["connector_id"],
            path_prefix=r.get("path_prefix", ""),
        )
        for r in rows
    ]


@router.post(
    "/workspaces/{workspace_id}/sources",
    response_model=WorkspaceSourceResponse,
    status_code=201,
)
async def add_workspace_source(
    workspace_id: str,
    body: WorkspaceSourceAdd,
    claims: JWTClaims = Depends(require_auth),
):
    workspace = await db_client.get_workspace(workspace_id, claims.org_id)
    if workspace is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "NOT_FOUND", "message": "Workspace not found"},
        )
    row = await db_client.add_workspace_source(workspace_id, body.connector_id, body.path_prefix)
    return WorkspaceSourceResponse(
        id=_str(row["id"]),
        workspace_id=_str(row["workspace_id"]),
        connector_id=row["connector_id"],
        path_prefix=row.get("path_prefix", ""),
    )


@router.delete("/workspaces/{workspace_id}/sources/{source_id}", status_code=204)
async def delete_workspace_source(
    workspace_id: str,
    source_id: str,
    claims: JWTClaims = Depends(require_auth),
):
    workspace = await db_client.get_workspace(workspace_id, claims.org_id)
    if workspace is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "NOT_FOUND", "message": "Workspace not found"},
        )
    deleted = await db_client.delete_workspace_source(workspace_id, source_id)
    if not deleted:
        raise HTTPException(
            status_code=404,
            detail={"error": "NOT_FOUND", "message": "Source not found in workspace"},
        )


# ── Workspace files ────────────────────────────────────────────────────────────

@router.get("/workspaces/{workspace_id}/files", response_model=List[FileStatus])
async def list_workspace_files(
    workspace_id: str,
    claims: JWTClaims = Depends(require_auth),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
):
    workspace = await db_client.get_workspace(workspace_id, claims.org_id)
    if workspace is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "NOT_FOUND", "message": "Workspace not found"},
        )
    rows = await db_client.get_workspace_files(workspace_id, claims.org_id, page, per_page)
    return [
        FileStatus(
            id=_str(r["id"]),
            connector_id=r["connector_id"],
            file_path=r["file_path"],
            ingest_status=r["ingest_status"],
            file_size_bytes=r.get("file_size_bytes"),
            chunk_count=r.get("chunk_count"),
        )
        for r in rows
    ]


@router.post("/workspaces/{workspace_id}/files/{file_id}/reindex", status_code=202)
async def reindex_file(
    workspace_id: str,
    file_id: str,
    claims: JWTClaims = Depends(require_auth),
):
    workspace = await db_client.get_workspace(workspace_id, claims.org_id)
    if workspace is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "NOT_FOUND", "message": "Workspace not found"},
        )
    return {"status": "reindex_queued", "file_id": file_id}
