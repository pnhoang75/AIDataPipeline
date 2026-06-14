"""Unit tests for the shared structlog logging configuration.

Tests verify that:
- setup_logging emits JSON with required fields (service, level, timestamp)
- bind_request_context injects tenant_id, trace_id, span_id on every log line
- clear_request_context removes those fields
"""
import io
import json
import logging
import os
import sys

import pytest
import structlog

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "services", "connector-s3", "src"),
)

from logging_config import (  # noqa: E402
    bind_request_context,
    clear_request_context,
    setup_logging,
)


@pytest.fixture(autouse=True)
def reset_structlog():
    """Reset structlog and stdlib logging between tests."""
    yield
    structlog.reset_defaults()
    root = logging.getLogger()
    root.handlers = []


def _capture_log_output(service: str, fn):
    """Call fn() with setup_logging configured to write to a StringIO buffer."""
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)

    setup_logging(service)
    root = logging.getLogger()
    root.handlers = [handler]

    # Reconfigure the handler formatter after setup_logging replaced it
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
    ]
    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
        foreign_pre_chain=[
            *shared_processors,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
        ],
    )
    handler.setFormatter(formatter)
    root.handlers = [handler]

    fn()
    return buf.getvalue()


class TestSetupLogging:
    def test_emits_json(self):
        lines = []

        def _emit():
            log = structlog.get_logger("test_logger")
            log.info("hello world")
            root_log = logging.getLogger("std_logger")
            root_log.info("stdlib message")

        output = _capture_log_output("test-svc", _emit)
        for raw_line in output.strip().splitlines():
            if raw_line.strip():
                doc = json.loads(raw_line)
                lines.append(doc)

        assert len(lines) >= 1
        # structlog line
        assert lines[0]["event"] == "hello world"

    def test_service_field_is_set(self):
        def _emit():
            log = structlog.get_logger("svc_logger")
            log.info("check service field")

        output = _capture_log_output("my-service", _emit)
        for raw_line in output.strip().splitlines():
            if raw_line.strip():
                doc = json.loads(raw_line)
                if doc.get("event") == "check service field":
                    assert doc.get("service") == "my-service"
                    return
        pytest.fail("Expected log line not found in output")

    def test_level_field_present(self):
        def _emit():
            log = structlog.get_logger()
            log.warning("warn event")

        output = _capture_log_output("svc", _emit)
        for raw_line in output.strip().splitlines():
            if raw_line.strip():
                doc = json.loads(raw_line)
                if doc.get("event") == "warn event":
                    assert doc.get("level") == "warning"
                    return
        pytest.fail("Expected log line not found")

    def test_timestamp_field_present(self):
        def _emit():
            log = structlog.get_logger()
            log.info("ts check")

        output = _capture_log_output("svc", _emit)
        for raw_line in output.strip().splitlines():
            if raw_line.strip():
                doc = json.loads(raw_line)
                if doc.get("event") == "ts check":
                    assert "timestamp" in doc
                    return
        pytest.fail("Expected log line not found")


class TestBindRequestContext:
    def test_tenant_id_appears_in_log(self):
        setup_logging("ctx-svc")
        clear_request_context()
        bind_request_context(tenant_id="tenant-abc", trace_id="t1", span_id="s1")

        buf = io.StringIO()
        handler = logging.StreamHandler(buf)

        shared_processors = [
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
        ]
        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(),
            foreign_pre_chain=[
                *shared_processors,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.StackInfoRenderer(),
            ],
        )
        handler.setFormatter(formatter)
        root = logging.getLogger()
        root.handlers = [handler]

        log = structlog.get_logger()
        log.info("request scoped message")

        output = buf.getvalue()
        doc = json.loads(output.strip().splitlines()[-1])
        assert doc["tenant_id"] == "tenant-abc"
        assert doc["trace_id"] == "t1"
        assert doc["span_id"] == "s1"

    def test_clear_removes_context(self):
        setup_logging("ctx-svc")
        bind_request_context(tenant_id="tenant-xyz")
        clear_request_context()

        buf = io.StringIO()
        handler = logging.StreamHandler(buf)
        shared_processors = [
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
        ]
        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(),
            foreign_pre_chain=[
                *shared_processors,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.StackInfoRenderer(),
            ],
        )
        handler.setFormatter(formatter)
        root = logging.getLogger()
        root.handlers = [handler]

        log = structlog.get_logger()
        log.info("after clear")

        output = buf.getvalue()
        doc = json.loads(output.strip().splitlines()[-1])
        assert doc.get("tenant_id", "") == ""

    def test_defaults_to_empty_strings(self):
        setup_logging("ctx-svc")
        clear_request_context()
        bind_request_context()  # no arguments

        buf = io.StringIO()
        handler = logging.StreamHandler(buf)
        shared_processors = [
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
        ]
        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(),
            foreign_pre_chain=[
                *shared_processors,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.StackInfoRenderer(),
            ],
        )
        handler.setFormatter(formatter)
        root = logging.getLogger()
        root.handlers = [handler]

        log = structlog.get_logger()
        log.info("defaults check")

        output = buf.getvalue()
        doc = json.loads(output.strip().splitlines()[-1])
        assert doc.get("tenant_id") == ""
        assert doc.get("trace_id") == ""
        assert doc.get("span_id") == ""
