from __future__ import annotations

import json
import os
from typing import Optional

from .client import BaseClient
from .models import IngestResponse, SearchResult


class RagClient:
    def __init__(self, client: BaseClient) -> None:
        self._client = client

    @classmethod
    def from_env(cls) -> "RagClient":
        base_url = os.environ["PIPELINE_API_URL"]
        api_key = os.environ["PIPELINE_API_KEY"]
        tenant_id = os.environ["PIPELINE_TENANT_ID"]
        return cls(BaseClient(base_url=base_url, api_key=api_key, tenant_id=tenant_id))

    def search(
        self,
        query: str,
        collection: Optional[str] = None,
        top_k: int = 5,
        tenant_id: Optional[str] = None,
    ) -> list[SearchResult]:
        body: dict = {"query": query, "top_k": top_k}
        if collection is not None:
            body["collection"] = collection
        if tenant_id is not None:
            body["tenant_id"] = tenant_id
        response = self._client.post("/api/search", json=body)
        return [SearchResult.model_validate(item) for item in response.json()]

    def ingest(
        self,
        content: str,
        filename: str,
        source_type: str = "upload",
        metadata: Optional[dict] = None,
    ) -> IngestResponse:
        files = {"file": (filename, content.encode(), "text/plain")}
        data: dict = {"source_type": source_type}
        if metadata is not None:
            data["metadata"] = json.dumps(metadata)
        response = self._client.post("/api/sources/upload", files=files, data=data)
        return IngestResponse.model_validate(response.json())
