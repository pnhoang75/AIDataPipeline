"""BFF proxy endpoints for the Metadata Service lineage and quality APIs."""
import logging
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException

from auth import JWTClaims, require_admin, require_auth
from config import config

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/lineage/downstream/{source_path:path}")
async def lineage_downstream(
    source_path: str,
    claims: JWTClaims = Depends(require_auth),
) -> List[Dict[str, Any]]:
    """Return downstream entities derived from a source file path."""
    tenant_id = claims.org_id
    url = f"{config.metadata_service_url}/api/lineage/downstream/{source_path}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, params={"tenant_id": tenant_id})
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=exc.response.text)
    except Exception as exc:
        logger.error("metadata service error: %s", exc)
        raise HTTPException(status_code=503, detail="metadata service unavailable")


@router.get("/api/quality")
async def data_quality(
    tenant_id: Optional[str] = None,
    claims: JWTClaims = Depends(require_admin),
) -> List[Dict[str, Any]]:
    """Return failed/warned quality checks for a tenant (admin only)."""
    effective_tenant = tenant_id or claims.org_id
    url = f"{config.metadata_service_url}/api/quality/{effective_tenant}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=exc.response.text)
    except Exception as exc:
        logger.error("metadata service error: %s", exc)
        raise HTTPException(status_code=503, detail="metadata service unavailable")
