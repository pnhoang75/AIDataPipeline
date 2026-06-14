"""Structured JSON logging via structlog.

Call setup_logging(service_name) once at process startup (main.py).
Call bind_request_context(tenant_id=..., trace_id=..., span_id=...) at each
request or message entry point; call clear_request_context() on exit.
"""
import logging
import sys

import structlog


def setup_logging(service: str, level: str = "INFO") -> None:
    log_level = getattr(logging, level.upper(), logging.INFO)

    shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        _make_service_processor(service),
    ]

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
        foreign_pre_chain=[
            *shared_processors,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
        ],
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(log_level)


def _make_service_processor(service: str):
    def _add_service(logger, method, event_dict):
        event_dict.setdefault("service", service)
        return event_dict
    return _add_service


def bind_request_context(
    *, tenant_id: str = "", trace_id: str = "", span_id: str = ""
) -> None:
    structlog.contextvars.bind_contextvars(
        tenant_id=tenant_id,
        trace_id=trace_id,
        span_id=span_id,
    )


def clear_request_context() -> None:
    structlog.contextvars.clear_contextvars()
