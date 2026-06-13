from typing import Dict, List

import httpx

from config import config as _default_config


async def get_usage(tenant_id: str, metric: str, cfg=None) -> Dict:
    cfg = cfg or _default_config
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{cfg.quota_service_url}/v1/check-quota",
            json={
                "tenant_id": tenant_id,
                "metric": metric,
                "amount": 0,
                "increment_on_allow": False,
            },
            timeout=5.0,
        )
        resp.raise_for_status()
        data = resp.json()
    return {
        "tenant_id": tenant_id,
        "metric": metric,
        "current": data.get("current_usage", 0),
        "limit": data.get("limit", 0),
        "unlimited": data.get("status") == "UNLIMITED",
    }


async def get_all_usage(cfg=None) -> List[Dict]:
    cfg = cfg or _default_config
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{cfg.quota_service_url}/v1/usage",
            timeout=5.0,
        )
        resp.raise_for_status()
        return resp.json()


async def set_override(tenant_id: str, metric: str, value: int, cfg=None) -> Dict:
    cfg = cfg or _default_config
    async with httpx.AsyncClient() as client:
        resp = await client.put(
            f"{cfg.quota_service_url}/v1/overrides/{tenant_id}/{metric}",
            json={"value": value},
            timeout=5.0,
        )
        resp.raise_for_status()
    return {"tenant_id": tenant_id, "metric": metric, "override_value": value}
