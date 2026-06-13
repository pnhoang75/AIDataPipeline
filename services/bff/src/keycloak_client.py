from typing import Dict, List, Optional

import httpx

from config import config as _default_config


async def _get_admin_token(cfg=None) -> str:
    cfg = cfg or _default_config
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{cfg.keycloak_url}/realms/master/protocol/openid-connect/token",
            data={
                "client_id": "admin-cli",
                "username": cfg.keycloak_admin_user,
                "password": cfg.keycloak_admin_password,
                "grant_type": "password",
            },
            timeout=10.0,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]


async def list_organizations(cfg=None) -> List[Dict]:
    cfg = cfg or _default_config
    token = await _get_admin_token(cfg)
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{cfg.keycloak_url}/admin/realms/{cfg.keycloak_realm}/organizations",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10.0,
        )
        resp.raise_for_status()
        orgs = resp.json()
    return [
        {
            "id": org.get("id"),
            "name": org.get("name"),
            "license_type": (org.get("attributes", {}).get("license_type") or ["free"])[0],
        }
        for org in orgs
    ]


async def create_organization(name: str, license_type: str, cfg=None) -> Dict:
    cfg = cfg or _default_config
    token = await _get_admin_token(cfg)
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{cfg.keycloak_url}/admin/realms/{cfg.keycloak_realm}/organizations",
            json={"name": name, "attributes": {"license_type": [license_type]}},
            headers={"Authorization": f"Bearer {token}"},
            timeout=10.0,
        )
        resp.raise_for_status()
        location = resp.headers.get("Location", "")
        org_id = location.rstrip("/").split("/")[-1]
    return {"id": org_id, "name": name, "license_type": license_type}


async def get_organization(org_id: str, cfg=None) -> Optional[Dict]:
    cfg = cfg or _default_config
    token = await _get_admin_token(cfg)
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{cfg.keycloak_url}/admin/realms/{cfg.keycloak_realm}/organizations/{org_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10.0,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        org = resp.json()
    return {
        "id": org.get("id"),
        "name": org.get("name"),
        "license_type": (org.get("attributes", {}).get("license_type") or ["free"])[0],
    }


async def update_organization_license(org_id: str, license_type: str, cfg=None) -> Optional[Dict]:
    cfg = cfg or _default_config
    token = await _get_admin_token(cfg)
    async with httpx.AsyncClient() as client:
        get_resp = await client.get(
            f"{cfg.keycloak_url}/admin/realms/{cfg.keycloak_realm}/organizations/{org_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10.0,
        )
        if get_resp.status_code == 404:
            return None
        get_resp.raise_for_status()
        org = get_resp.json()
        attrs = org.get("attributes", {})
        attrs["license_type"] = [license_type]
        org["attributes"] = attrs
        put_resp = await client.put(
            f"{cfg.keycloak_url}/admin/realms/{cfg.keycloak_realm}/organizations/{org_id}",
            json=org,
            headers={"Authorization": f"Bearer {token}"},
            timeout=10.0,
        )
        put_resp.raise_for_status()
    return {"id": org_id, "name": org.get("name"), "license_type": license_type}


async def list_organization_members(org_id: str, cfg=None) -> List[Dict]:
    cfg = cfg or _default_config
    token = await _get_admin_token(cfg)
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{cfg.keycloak_url}/admin/realms/{cfg.keycloak_realm}/organizations/{org_id}/members",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10.0,
        )
        resp.raise_for_status()
        members = resp.json()
    return [
        {
            "id": m.get("id"),
            "email": m.get("email"),
            "username": m.get("username"),
        }
        for m in members
    ]


async def invite_organization_member(
    org_id: str, email: str, roles: List[str], cfg=None
) -> Dict:
    cfg = cfg or _default_config
    token = await _get_admin_token(cfg)
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{cfg.keycloak_url}/admin/realms/{cfg.keycloak_realm}/organizations/{org_id}/members/invite-user",
            json={"email": email},
            headers={"Authorization": f"Bearer {token}"},
            timeout=10.0,
        )
        resp.raise_for_status()
    return {"email": email, "org_id": org_id, "roles": roles, "status": "invited"}
