"""Tests for BaseClient — auth headers, retry, timeout."""
from __future__ import annotations

import pytest
import respx
import httpx

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../sdk/python"))

from ai_pipeline_sdk.client import BaseClient, PipelineAPIError, PipelineTimeoutError


BASE_URL = "http://test-api.example.com"


def make_client(**kwargs) -> BaseClient:
    return BaseClient(
        base_url=BASE_URL,
        api_key="test-key",
        tenant_id="tenant-1",
        **kwargs,
    )


@respx.mock
def test_auth_headers_injected():
    """Authorization and X-Tenant-ID must appear on every request."""
    route = respx.get(f"{BASE_URL}/api/ping").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )

    client = make_client()
    client.get("/api/ping")

    assert route.called
    request = route.calls.last.request
    assert request.headers["Authorization"] == "Bearer test-key"
    assert request.headers["X-Tenant-ID"] == "tenant-1"


@respx.mock
def test_auth_headers_on_post():
    """POST requests also carry auth headers."""
    route = respx.post(f"{BASE_URL}/api/data").mock(
        return_value=httpx.Response(200, json={})
    )

    client = make_client()
    client.post("/api/data", json={"key": "val"})

    request = route.calls.last.request
    assert request.headers["Authorization"] == "Bearer test-key"
    assert request.headers["X-Tenant-ID"] == "tenant-1"


@respx.mock
def test_retry_fires_on_503_then_raises(monkeypatch):
    """On three consecutive 503s, retry should fire and finally raise PipelineAPIError."""
    monkeypatch.setattr("ai_pipeline_sdk.client.time.sleep", lambda _: None)

    respx.get(f"{BASE_URL}/api/fail").mock(
        return_value=httpx.Response(503, text="Service Unavailable")
    )

    client = make_client()
    with pytest.raises(PipelineAPIError) as exc_info:
        client.get("/api/fail")

    assert exc_info.value.status_code == 503
    # The route should have been called exactly 3 times (one per retry delay slot)
    assert respx.calls.call_count == 3


@respx.mock
def test_retry_recovers_on_second_attempt(monkeypatch):
    """If the second attempt returns 200, the client should succeed."""
    monkeypatch.setattr("ai_pipeline_sdk.client.time.sleep", lambda _: None)

    route = respx.get(f"{BASE_URL}/api/flaky").mock(
        side_effect=[
            httpx.Response(503, text="down"),
            httpx.Response(200, json={"ok": True}),
        ]
    )

    client = make_client()
    response = client.get("/api/flaky")

    assert response.status_code == 200
    assert route.call_count == 2


@respx.mock
def test_4xx_raises_immediately_without_retry(monkeypatch):
    """4xx errors should raise PipelineAPIError immediately, without retry."""
    monkeypatch.setattr("ai_pipeline_sdk.client.time.sleep", lambda _: None)

    route = respx.get(f"{BASE_URL}/api/missing").mock(
        return_value=httpx.Response(404, text="Not Found")
    )

    client = make_client()
    with pytest.raises(PipelineAPIError) as exc_info:
        client.get("/api/missing")

    assert exc_info.value.status_code == 404
    assert route.call_count == 1  # no retry on 4xx


@respx.mock
def test_timeout_raises_pipeline_timeout_error():
    """httpx.TimeoutException should be wrapped as PipelineTimeoutError."""
    respx.get(f"{BASE_URL}/api/slow").mock(
        side_effect=httpx.ReadTimeout("timed out", request=None)
    )

    client = make_client(timeout=0.001)
    with pytest.raises(PipelineTimeoutError):
        client.get("/api/slow")
