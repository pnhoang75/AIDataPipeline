"""Unit tests for the HTTP REST endpoint — session 2-F."""
import json
import os
import sys
import threading
import urllib.request
import urllib.error
from unittest.mock import MagicMock

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "services", "quota-service", "src"),
)

from quota_service import QuotaService, QuotaStatus
from server import _SvcHTTPServer


def _make_svc(limit, redis_incr_result=None):
    redis = MagicMock()
    if redis_incr_result is not None:
        pipe = MagicMock()
        pipe.execute.return_value = [redis_incr_result, True]
        redis.pipeline.return_value = pipe
    return QuotaService(redis_client=redis, get_limit_fn=lambda t, m: limit)


def _post(port, payload):
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/v1/check-quota",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def _start_server(svc):
    srv = _SvcHTTPServer(("127.0.0.1", 0), svc)
    port = srv.server_address[1]
    t = threading.Thread(target=srv.handle_request, daemon=True)
    t.start()
    return srv, port


def test_http_endpoint_allowed():
    svc = _make_svc(limit=10, redis_incr_result=5)
    srv, port = _start_server(svc)
    data = _post(port, {"tenant_id": "tenant-a", "metric": "API_CALLS_PER_DAY"})
    assert data["status"] == "ALLOWED"
    assert data["current_usage"] == 5


def test_http_endpoint_denied():
    svc = _make_svc(limit=10, redis_incr_result=11)
    svc.redis.decrby.return_value = 10
    srv, port = _start_server(svc)
    data = _post(port, {"tenant_id": "tenant-a", "metric": "API_CALLS_PER_DAY"})
    assert data["status"] == "DENIED"


def test_http_endpoint_unlimited():
    svc = _make_svc(limit=None)
    srv, port = _start_server(svc)
    data = _post(port, {"tenant_id": "enterprise-tenant", "metric": "API_CALLS_PER_DAY"})
    assert data["status"] == "UNLIMITED"


def test_http_healthz():
    svc = _make_svc(limit=100)
    srv = _SvcHTTPServer(("127.0.0.1", 0), svc)
    port = srv.server_address[1]
    t = threading.Thread(target=srv.handle_request, daemon=True)
    t.start()
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz") as resp:
        data = json.loads(resp.read())
    assert data["status"] == "ok"
