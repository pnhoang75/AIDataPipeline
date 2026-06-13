import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class Config:
    keycloak_jwks_url: str = os.environ.get(
        "KEYCLOAK_JWKS_URL",
        "http://keycloak.infrastructure.svc.cluster.local:8080/realms/pipeline/protocol/openid-connect/certs",
    )
    keycloak_issuer: str = os.environ.get(
        "KEYCLOAK_ISSUER",
        "http://keycloak.infrastructure.svc.cluster.local:8080/realms/pipeline",
    )
    jwt_algorithms: List[str] = field(default_factory=lambda: ["RS256"])


config = Config()
