"""Pipeline Operator — kopf entry point.

Usage (Dockerfile / Deployment):
    kopf run --all-namespaces /app/main.py

The 'operator' directory is added to sys.path before any imports so that
handlers.py, k8s_client.py, and k8s_real_client.py can be imported directly.
This avoids the name collision with Python's built-in 'operator' module.
"""
from __future__ import annotations
import logging
import os
import sys

# Add the operator package dir to sys.path before importing its modules,
# so direct imports (handlers, k8s_real_client) shadow the stdlib 'operator'.
_OPERATOR_DIR = os.path.join(os.path.dirname(__file__), "operator")
if _OPERATOR_DIR not in sys.path:
    sys.path.insert(0, _OPERATOR_DIR)

import kopf

# Import the handlers module to register all kopf decorators.
import handlers as _handlers  # noqa: E402

from k8s_real_client import K8sRealClient  # noqa: E402

logger = logging.getLogger(__name__)


@kopf.on.startup()
async def on_startup(settings: kopf.OperatorSettings, **_kwargs) -> None:
    """Initialize the real K8s client and replace the stub singleton."""
    real = K8sRealClient()
    await real.initialize()
    _handlers._client = real
    logger.info("Pipeline Operator started — real K8s client initialized")

    # Reduce verbose kopf progress storage noise on the testbed.
    settings.persistence.progress_storage = kopf.AnnotationsProgressStorage()
