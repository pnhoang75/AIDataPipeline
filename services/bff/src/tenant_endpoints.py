from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

import keycloak_client
import quota_client
from auth import JWTClaims, require_admin

router = APIRouter(prefix="/api/admin", tags=["admin-tenant"])


class TenantCreate(BaseModel):
    name: str
    license_type: str = "free"


class TenantResponse(BaseModel):
    id: str
    name: str
    license_type: str


class LicensePatch(BaseModel):
    license_type: str


class UserInvite(BaseModel):
    email: str
    roles: List[str] = ["developer"]


class UserResponse(BaseModel):
    id: Optional[str] = None
    email: Optional[str] = None
    username: Optional[str] = None


class QuotaUsage(BaseModel):
    tenant_id: str
    metric: str
    current: int
    limit: int
    unlimited: bool


class QuotaOverridePut(BaseModel):
    value: int


@router.get("/tenants", response_model=List[TenantResponse])
async def list_tenants(claims: JWTClaims = Depends(require_admin)):
    orgs = await keycloak_client.list_organizations()
    return [TenantResponse(**o) for o in orgs]


@router.post("/tenants", response_model=TenantResponse, status_code=201)
async def create_tenant(body: TenantCreate, claims: JWTClaims = Depends(require_admin)):
    org = await keycloak_client.create_organization(body.name, body.license_type)
    return TenantResponse(**org)


@router.patch("/tenants/{tenant_id}/license", response_model=TenantResponse)
async def update_tenant_license(
    tenant_id: str,
    body: LicensePatch,
    claims: JWTClaims = Depends(require_admin),
):
    org = await keycloak_client.update_organization_license(tenant_id, body.license_type)
    if org is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "NOT_FOUND", "message": "Tenant not found"},
        )
    return TenantResponse(**org)


@router.get("/tenants/{tenant_id}/users", response_model=List[UserResponse])
async def list_tenant_users(
    tenant_id: str,
    claims: JWTClaims = Depends(require_admin),
):
    members = await keycloak_client.list_organization_members(tenant_id)
    return [UserResponse(**m) for m in members]


@router.post("/tenants/{tenant_id}/users", status_code=201)
async def invite_tenant_user(
    tenant_id: str,
    body: UserInvite,
    claims: JWTClaims = Depends(require_admin),
):
    result = await keycloak_client.invite_organization_member(tenant_id, body.email, body.roles)
    return result


@router.get("/quota", response_model=List[QuotaUsage])
async def list_quota(claims: JWTClaims = Depends(require_admin)):
    usages = await quota_client.get_all_usage()
    return [QuotaUsage(**u) for u in usages]


@router.put("/quota/{tenant_id}/{metric}")
async def set_quota_override(
    tenant_id: str,
    metric: str,
    body: QuotaOverridePut,
    claims: JWTClaims = Depends(require_admin),
):
    result = await quota_client.set_override(tenant_id, metric, body.value)
    return result
