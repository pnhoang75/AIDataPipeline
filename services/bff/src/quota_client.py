from typing import Dict, List

import httpx
from opentelemetry import trace

from config import config as _default_config

_tracer = trace.get_tracer(__name__)


async def check_quota(tenant_id: str, metric: str, cfg=None) -> Dict:
    """Check and increment quota. Returns {"allowed": bool, "status": str}."""
    cfg = cfg or _default_config
    with _tracer.start_as_current_span("quota.check_quota") as span:
        span.set_attribute("rpc.system", "http")
        span.set_attribute("rpc.service", "QuotaService")
        span.set_attribute("rpc.method", "CheckQuota")
        span.set_attribute("quota.tenant_id", tenant_id)
        span.set_attribute("quota.metric", metric)
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{cfg.quota_service_url}/v1/check-quota",
                json={
                    "tenant_id": tenant_id,
                    "metric": metric,
                    "amount": 1,
                    "increment_on_allow": True,
                },
                timeout=5.0,
            )
            resp.raise_for_status()
            data = resp.json()
        status = data.get("status", "DENIED")
        span.set_attribute("quota.result", status)
        return {"allowed": status in ("ALLOWED", "UNLIMITED"), "status": status}


async def get_usage(tenant_id: str, metric: str, cfg=None) -> Dict:
    cfg = cfg or _default_config
    with _tracer.start_as_current_span("quota.get_usage") as span:
        span.set_attribute("rpc.system", "http")
        span.set_attribute("rpc.service", "QuotaService")
        span.set_attribute("rpc.method", "GetUsage")
        span.set_attribute("quota.tenant_id", tenant_id)
        span.set_attribute("quota.metric", metric)
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
        span.set_attribute("quota.current_usage", data.get("current_usage", 0))
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
