"""Integration test configuration.

Sets DOCKER_CONFIG to a temp directory with an empty credentials config so
testcontainers works on macOS Docker Desktop even when docker-credential-desktop
is not on PATH (the symlink points into the unmounted Docker.app volume).
"""

import json
import os
import tempfile

import pytest


# This must run before any Docker client is initialized, so we use autouse=True
# at session scope and place it in the earliest conftest that pytest processes.

@pytest.fixture(scope="session", autouse=True)
def _docker_config_override():
    tmp = tempfile.mkdtemp(prefix="docker-cfg-")
    config_path = os.path.join(tmp, "config.json")
    with open(config_path, "w") as fh:
        json.dump({"auths": {}}, fh)
    prev = os.environ.get("DOCKER_CONFIG")
    os.environ["DOCKER_CONFIG"] = tmp
    yield
    if prev is None:
        os.environ.pop("DOCKER_CONFIG", None)
    else:
        os.environ["DOCKER_CONFIG"] = prev
