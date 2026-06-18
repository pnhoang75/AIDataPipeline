from __future__ import annotations

import time
from typing import Any, Optional

import httpx


class PipelineAPIError(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code
        self.message = message


class PipelineTimeoutError(Exception):
    pass


_RETRY_DELAYS = (0.5, 1.0, 2.0)


class BaseClient:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        tenant_id: str,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.tenant_id = tenant_id
        self.timeout = timeout
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "X-Tenant-ID": tenant_id,
        }

    # ------------------------------------------------------------------
    # Sync
    # ------------------------------------------------------------------

    def _client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self.base_url,
            headers=self._headers,
            timeout=self.timeout,
        )

    def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        last_exc: Optional[Exception] = None
        for attempt, delay in enumerate(_RETRY_DELAYS, start=1):
            try:
                with self._client() as client:
                    response = client.request(method, path, **kwargs)
            except httpx.TimeoutException as exc:
                raise PipelineTimeoutError(str(exc)) from exc

            if response.status_code >= 500:
                last_exc = PipelineAPIError(
                    response.status_code,
                    response.text,
                )
                if attempt < len(_RETRY_DELAYS):
                    time.sleep(delay)
                continue

            if response.status_code >= 400:
                raise PipelineAPIError(response.status_code, response.text)

            return response

        raise last_exc  # type: ignore[misc]

    def get(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("POST", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("DELETE", path, **kwargs)

    # ------------------------------------------------------------------
    # Async
    # ------------------------------------------------------------------

    def _async_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self.base_url,
            headers=self._headers,
            timeout=self.timeout,
        )

    async def arequest(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        import asyncio

        last_exc: Optional[Exception] = None
        for attempt, delay in enumerate(_RETRY_DELAYS, start=1):
            try:
                async with self._async_client() as client:
                    response = await client.request(method, path, **kwargs)
            except httpx.TimeoutException as exc:
                raise PipelineTimeoutError(str(exc)) from exc

            if response.status_code >= 500:
                last_exc = PipelineAPIError(
                    response.status_code,
                    response.text,
                )
                if attempt < len(_RETRY_DELAYS):
                    await asyncio.sleep(delay)
                continue

            if response.status_code >= 400:
                raise PipelineAPIError(response.status_code, response.text)

            return response

        raise last_exc  # type: ignore[misc]

    async def aget(self, path: str, **kwargs: Any) -> httpx.Response:
        return await self.arequest("GET", path, **kwargs)

    async def apost(self, path: str, **kwargs: Any) -> httpx.Response:
        return await self.arequest("POST", path, **kwargs)
