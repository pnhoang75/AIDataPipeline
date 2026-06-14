"""gRPC + HTTP server entry point for the Quota Service.

Proto-generated stubs (quota_service_pb2 / quota_service_pb2_grpc) are
produced at build time via:
    python -m grpc_tools.protoc -I docs/api \
        --python_out=services/quota-service/src \
        --grpc_python_out=services/quota-service/src \
        docs/api/quota-service.proto

This module is intentionally NOT imported by unit tests; tests exercise
QuotaService (quota_service.py) directly.
"""
import json
import logging
import signal
import sys
import threading
from concurrent import futures
from http.server import BaseHTTPRequestHandler, HTTPServer as _HTTPServer

from logging_config import setup_logging, bind_request_context

setup_logging("quota-service")

import grpc
import redis as redis_lib
import sqlalchemy as sa
import structlog
from sqlalchemy.orm import sessionmaker

from config import Config
from db_queries import make_get_limit_fn
from quota_service import QuotaService, QuotaStatus

logger = structlog.get_logger(__name__)

try:
    from prometheus_client import Counter, start_http_server

    _QUOTA_SKIPPED = Counter(
        "quota_check_skipped_total",
        "Quota checks skipped because Redis was unavailable",
    )
    _METRICS_ENABLED = True
except ImportError:
    _METRICS_ENABLED = False
    _QUOTA_SKIPPED = None

# Proto-generated stubs — present after protoc build step
try:
    import quota_service_pb2 as pb2
    import quota_service_pb2_grpc as pb2_grpc

    _PROTO_AVAILABLE = True
except ImportError:
    _PROTO_AVAILABLE = False
    logger.warning("Proto stubs not found; run protoc to generate them.")


# ── HTTP/1.1 REST handler (called by Kong Lua plugin) ────────────────────────

class _QuotaHTTPHandler(BaseHTTPRequestHandler):
    """Minimal HTTP/1.1 handler so Kong Lua plugin can call quota check without full gRPC framing."""

    def do_POST(self):
        if self.path == "/v1/check-quota":
            length = int(self.headers.get("Content-Length", 0))
            try:
                body = json.loads(self.rfile.read(length))
                result = self.server.svc.check_quota(
                    tenant_id=body["tenant_id"],
                    metric=body.get("metric", "API_CALLS_PER_DAY"),
                    amount=int(body.get("amount", 1)),
                )
                resp = json.dumps({
                    "status": QuotaStatus(result.status).name,
                    "current_usage": result.current_usage,
                    "limit": result.limit,
                    "deny_reason": result.deny_reason,
                }).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", len(resp))
                self.end_headers()
                self.wfile.write(resp)
            except Exception as exc:
                logging.getLogger(__name__).warning("HTTP /v1/check-quota error: %s", exc)
                self.send_response(500)
                self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        if self.path == "/healthz":
            resp = b'{"status":"ok"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", len(resp))
            self.end_headers()
            self.wfile.write(resp)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):
        pass  # suppress per-request access log noise


class _SvcHTTPServer(_HTTPServer):
    """HTTPServer with the QuotaService instance attached."""

    allow_reuse_address = True

    def __init__(self, server_address, svc: QuotaService):
        super().__init__(server_address, _QuotaHTTPHandler)
        self.svc = svc


if _PROTO_AVAILABLE:

    class QuotaServicer(pb2_grpc.QuotaServiceServicer):
        """gRPC servicer: thin adapter between proto messages and QuotaService."""

        def __init__(self, svc: QuotaService) -> None:
            self._svc = svc

        def CheckQuota(self, request, context):
            result = self._svc.check_quota(
                tenant_id=request.tenant_id,
                metric=pb2.Metric.Name(request.metric),
                amount=request.amount if request.amount > 0 else 1,
                increment_on_allow=request.increment_on_allow
                if request.HasField("increment_on_allow")
                else True,
            )
            status_map = {
                QuotaStatus.ALLOWED: pb2.ALLOWED,
                QuotaStatus.DENIED: pb2.DENIED,
                QuotaStatus.UNLIMITED: pb2.UNLIMITED,
            }
            return pb2.CheckQuotaResponse(
                status=status_map.get(result.status, pb2.QUOTA_STATUS_UNSPECIFIED),
                current_usage=result.current_usage,
                limit=result.limit,
                deny_reason=result.deny_reason,
            )

        def RecordUsage(self, request, context):
            result = self._svc.record_usage(
                tenant_id=request.tenant_id,
                metric=pb2.Metric.Name(request.metric),
                amount=request.amount,
                event_id=request.event_id,
            )
            return pb2.RecordUsageResponse(
                new_total=result.new_total,
                deduped=result.deduped,
            )

        def GetUsage(self, request, context):
            data = self._svc.get_usage(
                tenant_id=request.tenant_id,
                metric=pb2.Metric.Name(request.metric),
            )
            return pb2.GetUsageResponse(
                tenant_id=data["tenant_id"],
                metric=request.metric,
                current=data["current"],
                limit=data["limit"],
                usage_ratio=data["current"] / data["limit"] if data["limit"] > 0 else 0.0,
            )

        def Check(self, request, context):
            return pb2.HealthCheckResponse(status=pb2.HealthCheckResponse.SERVING)


def serve(cfg=None) -> None:
    if cfg is None:
        cfg = Config()

    # Redis
    redis_client = redis_lib.Redis(
        host=cfg.redis_host, port=cfg.redis_port, db=cfg.redis_db, decode_responses=True
    )

    # PostgreSQL (DB may be unavailable in testbed; static limits bypass it)
    engine = sa.create_engine(cfg.quota_db_url, pool_pre_ping=True)
    session_factory = sessionmaker(bind=engine)
    raw_db_fn = make_get_limit_fn(session_factory)
    static_limits = cfg.static_limits

    def get_limit_fn(tenant_id: str, metric: str):
        if metric in static_limits:
            return static_limits[metric]
        try:
            return raw_db_fn(tenant_id, metric)
        except Exception:
            logger.warning("DB unavailable for %s/%s; failing open (unlimited)", tenant_id, metric)
            return None

    svc = QuotaService(
        redis_client=redis_client,
        get_limit_fn=get_limit_fn,
        skip_counter=_QUOTA_SKIPPED,
    )

    # Start HTTP REST server for Kong plugin (runs regardless of gRPC availability)
    http_srv = _SvcHTTPServer(("0.0.0.0", cfg.http_port), svc)
    http_thread = threading.Thread(target=http_srv.serve_forever, daemon=True, name="http-quota")
    http_thread.start()
    logger.info("Quota HTTP server listening on :%d", cfg.http_port)

    if not _PROTO_AVAILABLE:
        logger.warning("Proto stubs not generated; gRPC server disabled. HTTP-only mode.")
        if _METRICS_ENABLED:
            start_http_server(9090)

        def _stop_http(sig, frame):
            http_srv.shutdown()
            sys.exit(0)

        signal.signal(signal.SIGTERM, _stop_http)
        signal.signal(signal.SIGINT, _stop_http)
        http_thread.join()
        return

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=cfg.grpc_workers))
    pb2_grpc.add_QuotaServiceServicer_to_server(QuotaServicer(svc), server)
    server.add_insecure_port(f"[::]:{cfg.grpc_port}")
    server.start()
    logger.info("Quota Service gRPC listening on :%d", cfg.grpc_port)

    if _METRICS_ENABLED:
        start_http_server(9090)

    def _stop(sig, frame):
        server.stop(grace=5)
        http_srv.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
