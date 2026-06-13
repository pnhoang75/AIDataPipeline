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
    keycloak_url: str = os.environ.get(
        "KEYCLOAK_URL",
        "http://keycloak.infrastructure.svc.cluster.local:8080",
    )
    keycloak_realm: str = os.environ.get("KEYCLOAK_REALM", "pipeline")
    keycloak_admin_user: str = os.environ.get("KEYCLOAK_ADMIN_USER", "admin")
    keycloak_admin_password: str = os.environ.get("KEYCLOAK_ADMIN_PASSWORD", "admin")
    quota_service_url: str = os.environ.get(
        "QUOTA_SERVICE_URL",
        "http://quota-service.ai-pipeline.svc:8081",
    )
    database_url: str = os.environ.get(
        "DATABASE_URL",
        "postgresql://bff:bff@postgres.infrastructure.svc:5432/pipeline",
    )
    jwt_algorithms: List[str] = field(default_factory=lambda: ["RS256"])


config = Config()
