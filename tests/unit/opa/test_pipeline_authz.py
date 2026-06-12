"""Unit tests for OPA Rego pipeline.authz policy.

Tests mirror the rules in k8s/pipeline/opa-policy/pipeline_authz.rego.
When the `opa` CLI is installed, tests also run via subprocess to verify the
actual Rego. Without OPA installed, the Python evaluator is used exclusively.
"""
import json
import os
import subprocess
import sys

import pytest

# ---------------------------------------------------------------------------
# Pure-Python evaluator — mirrors the Rego rules without requiring OPA
# ---------------------------------------------------------------------------

POLICY_PATH = os.path.join(
    os.path.dirname(__file__),
    "..", "..", "..", "k8s", "pipeline", "opa-policy", "pipeline_authz.rego",
)


def _allowed_connectors(license_type: str) -> set:
    if license_type == "free":
        return {"s3", "nfs"}
    return {"s3", "nfs", "database", "stream"}


def evaluate(inp: dict) -> bool:
    """Python implementation of pipeline.authz.allow."""
    action = inp.get("action", "")
    license_type = inp.get("license_type", "")
    tenant_id = inp.get("tenant_id", "")

    # GPU access: pro or enterprise only
    if action == "use_gpu":
        return license_type in ("pro", "enterprise")

    # Collection access: must equal {tenant_id}_docs
    if action == "query_collection":
        return inp.get("collection_name", "") == f"{tenant_id}_docs"

    # Connector type: must be in tier allowlist
    if action == "use_connector":
        return inp.get("connector_type", "") in _allowed_connectors(license_type)

    return False


# ---------------------------------------------------------------------------
# Optional OPA CLI evaluation (skipped if opa not installed)
# ---------------------------------------------------------------------------

def _opa_available() -> bool:
    try:
        subprocess.run(["opa", "version"], capture_output=True, check=True, timeout=5)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


def opa_eval(inp: dict) -> bool:
    """Call opa eval with the actual Rego file; returns allow result."""
    result = subprocess.run(
        [
            "opa", "eval",
            "--data", POLICY_PATH,
            "--input", "/dev/stdin",
            "data.pipeline.authz.allow",
            "--format", "raw",
        ],
        input=json.dumps(inp).encode(),
        capture_output=True,
        timeout=10,
    )
    return result.stdout.strip() == b"true"


# ---------------------------------------------------------------------------
# Tests: GPU access by license tier
# ---------------------------------------------------------------------------

class TestGpuAccess:
    def test_free_tier_blocked_from_gpu(self):
        assert evaluate({"action": "use_gpu", "license_type": "free"}) is False

    def test_pro_tier_allowed_gpu(self):
        assert evaluate({"action": "use_gpu", "license_type": "pro"}) is True

    def test_enterprise_tier_allowed_gpu(self):
        assert evaluate({"action": "use_gpu", "license_type": "enterprise"}) is True

    @pytest.mark.skipif(not _opa_available(), reason="opa CLI not installed")
    def test_rego_free_blocked(self):
        assert opa_eval({"action": "use_gpu", "license_type": "free"}) is False

    @pytest.mark.skipif(not _opa_available(), reason="opa CLI not installed")
    def test_rego_pro_allowed(self):
        assert opa_eval({"action": "use_gpu", "license_type": "pro"}) is True

    @pytest.mark.skipif(not _opa_available(), reason="opa CLI not installed")
    def test_rego_enterprise_allowed(self):
        assert opa_eval({"action": "use_gpu", "license_type": "enterprise"}) is True


# ---------------------------------------------------------------------------
# Tests: Connector type allowlist by license tier
# ---------------------------------------------------------------------------

class TestConnectorAllowlist:
    def test_free_tier_s3_allowed(self):
        assert evaluate({"action": "use_connector", "license_type": "free", "connector_type": "s3"}) is True

    def test_free_tier_nfs_allowed(self):
        assert evaluate({"action": "use_connector", "license_type": "free", "connector_type": "nfs"}) is True

    def test_free_tier_database_blocked(self):
        assert evaluate({"action": "use_connector", "license_type": "free", "connector_type": "database"}) is False

    def test_free_tier_stream_blocked(self):
        assert evaluate({"action": "use_connector", "license_type": "free", "connector_type": "stream"}) is False

    def test_pro_tier_database_allowed(self):
        assert evaluate({"action": "use_connector", "license_type": "pro", "connector_type": "database"}) is True

    def test_pro_tier_stream_allowed(self):
        assert evaluate({"action": "use_connector", "license_type": "pro", "connector_type": "stream"}) is True

    def test_enterprise_tier_all_connectors_allowed(self):
        for ct in ("s3", "nfs", "database", "stream"):
            assert evaluate({"action": "use_connector", "license_type": "enterprise", "connector_type": ct}) is True

    def test_unknown_connector_type_blocked(self):
        assert evaluate({"action": "use_connector", "license_type": "pro", "connector_type": "ftp"}) is False

    @pytest.mark.skipif(not _opa_available(), reason="opa CLI not installed")
    def test_rego_free_database_blocked(self):
        assert opa_eval({"action": "use_connector", "license_type": "free", "connector_type": "database"}) is False

    @pytest.mark.skipif(not _opa_available(), reason="opa CLI not installed")
    def test_rego_pro_database_allowed(self):
        assert opa_eval({"action": "use_connector", "license_type": "pro", "connector_type": "database"}) is True


# ---------------------------------------------------------------------------
# Tests: Collection access scoped to {tenant_id}_docs
# ---------------------------------------------------------------------------

class TestCollectionAccess:
    def test_correct_collection_allowed(self):
        inp = {"action": "query_collection", "tenant_id": "acme", "collection_name": "acme_docs"}
        assert evaluate(inp) is True

    def test_wrong_tenant_collection_blocked(self):
        inp = {"action": "query_collection", "tenant_id": "acme", "collection_name": "corp_docs"}
        assert evaluate(inp) is False

    def test_arbitrary_collection_blocked(self):
        inp = {"action": "query_collection", "tenant_id": "acme", "collection_name": "admin_data"}
        assert evaluate(inp) is False

    def test_empty_tenant_id_blocks_non_empty_collection(self):
        inp = {"action": "query_collection", "tenant_id": "", "collection_name": "acme_docs"}
        assert evaluate(inp) is False

    def test_underscore_concat_format(self):
        # collection must be {tenant_id}_docs, not {tenant_id}-docs or docs_{tenant_id}
        assert evaluate({"action": "query_collection", "tenant_id": "t1", "collection_name": "t1_docs"}) is True
        assert evaluate({"action": "query_collection", "tenant_id": "t1", "collection_name": "t1-docs"}) is False
        assert evaluate({"action": "query_collection", "tenant_id": "t1", "collection_name": "docs_t1"}) is False

    @pytest.mark.skipif(not _opa_available(), reason="opa CLI not installed")
    def test_rego_correct_collection(self):
        inp = {"action": "query_collection", "tenant_id": "acme", "collection_name": "acme_docs"}
        assert opa_eval(inp) is True

    @pytest.mark.skipif(not _opa_available(), reason="opa CLI not installed")
    def test_rego_wrong_collection_blocked(self):
        inp = {"action": "query_collection", "tenant_id": "acme", "collection_name": "corp_docs"}
        assert opa_eval(inp) is False


# ---------------------------------------------------------------------------
# Tests: Default deny
# ---------------------------------------------------------------------------

class TestDefaultDeny:
    def test_unknown_action_denied(self):
        assert evaluate({"action": "delete_tenant", "license_type": "enterprise"}) is False

    def test_empty_input_denied(self):
        assert evaluate({}) is False

    def test_no_action_denied(self):
        assert evaluate({"license_type": "enterprise"}) is False
