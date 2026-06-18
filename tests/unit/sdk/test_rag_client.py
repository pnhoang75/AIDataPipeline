"""Tests for RagClient — search(), ingest(), error handling."""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../sdk/python"))

import pytest
import respx
import httpx

from ai_pipeline_sdk.client import BaseClient, PipelineAPIError
from ai_pipeline_sdk.rag import RagClient
from ai_pipeline_sdk.models import IngestResponse, SearchResult


BASE_URL = "http://rag-api.example.com"


def make_rag_client() -> RagClient:
    client = BaseClient(base_url=BASE_URL, api_key="test-key", tenant_id="tenant-1")
    return RagClient(client)


@respx.mock
def test_search_returns_typed_search_result_list():
    """search() should deserialise the response into a typed SearchResult list."""
    respx.post(f"{BASE_URL}/api/search").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "chunk_id": "chunk-1",
                    "doc_id": "doc-1",
                    "text": "Hello world",
                    "score": 0.95,
                    "metadata": {"source": "test"},
                }
            ],
        )
    )

    rag = make_rag_client()
    results = rag.search("Hello")

    assert len(results) == 1
    result = results[0]
    assert isinstance(result, SearchResult)
    assert result.chunk_id == "chunk-1"
    assert result.doc_id == "doc-1"
    assert result.text == "Hello world"
    assert result.score == 0.95
    assert result.metadata == {"source": "test"}


@respx.mock
def test_search_multiple_results():
    """search() should return all results from the response."""
    respx.post(f"{BASE_URL}/api/search").mock(
        return_value=httpx.Response(
            200,
            json=[
                {
                    "chunk_id": "chunk-1",
                    "doc_id": "doc-1",
                    "text": "First result",
                    "score": 0.95,
                    "metadata": {},
                },
                {
                    "chunk_id": "chunk-2",
                    "doc_id": "doc-2",
                    "text": "Second result",
                    "score": 0.82,
                    "metadata": {},
                },
            ],
        )
    )

    rag = make_rag_client()
    results = rag.search("query", top_k=2)

    assert len(results) == 2
    assert results[0].chunk_id == "chunk-1"
    assert results[1].chunk_id == "chunk-2"


@respx.mock
def test_search_empty_result_list():
    """search() should handle an empty result list gracefully."""
    respx.post(f"{BASE_URL}/api/search").mock(
        return_value=httpx.Response(200, json=[])
    )

    rag = make_rag_client()
    results = rag.search("no match")

    assert results == []


@respx.mock
def test_ingest_returns_ingest_response():
    """ingest() should return a typed IngestResponse."""
    route = respx.post(f"{BASE_URL}/api/sources/upload").mock(
        return_value=httpx.Response(
            200,
            json={"job_id": "job-123", "status": "queued"},
        )
    )

    rag = make_rag_client()
    result = rag.ingest(content="Hello world", filename="test.txt")

    assert isinstance(result, IngestResponse)
    assert result.job_id == "job-123"
    assert result.status == "queued"
    assert route.called


@respx.mock
def test_ingest_with_metadata():
    """ingest() should pass metadata and source_type to the upload endpoint."""
    route = respx.post(f"{BASE_URL}/api/sources/upload").mock(
        return_value=httpx.Response(
            200,
            json={"job_id": "job-456", "status": "queued"},
        )
    )

    rag = make_rag_client()
    result = rag.ingest(
        content="Document text",
        filename="doc.txt",
        source_type="manual",
        metadata={"author": "Alice"},
    )

    assert result.job_id == "job-456"
    assert route.called


@respx.mock
def test_search_404_raises_pipeline_api_error():
    """A 404 from the search endpoint should raise PipelineAPIError immediately."""
    respx.post(f"{BASE_URL}/api/search").mock(
        return_value=httpx.Response(404, text="Not Found")
    )

    rag = make_rag_client()
    with pytest.raises(PipelineAPIError) as exc_info:
        rag.search("query")

    assert exc_info.value.status_code == 404


@respx.mock
def test_ingest_404_raises_pipeline_api_error():
    """A 404 from the upload endpoint should raise PipelineAPIError."""
    respx.post(f"{BASE_URL}/api/sources/upload").mock(
        return_value=httpx.Response(404, text="Not Found")
    )

    rag = make_rag_client()
    with pytest.raises(PipelineAPIError) as exc_info:
        rag.ingest(content="text", filename="file.txt")

    assert exc_info.value.status_code == 404


def test_from_env(monkeypatch):
    """RagClient.from_env() should read env vars and build a RagClient."""
    monkeypatch.setenv("PIPELINE_API_URL", "http://env-api.example.com")
    monkeypatch.setenv("PIPELINE_API_KEY", "env-key")
    monkeypatch.setenv("PIPELINE_TENANT_ID", "env-tenant")

    rag = RagClient.from_env()

    assert isinstance(rag, RagClient)
    assert rag._client.base_url == "http://env-api.example.com"
    assert rag._client.api_key == "env-key"
    assert rag._client.tenant_id == "env-tenant"
