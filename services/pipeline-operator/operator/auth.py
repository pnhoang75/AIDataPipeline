"""JWT auth helpers shared across services.

Kong validates the JWT signature and injects X-Tenant-ID from the JWT org_id claim.
These helpers handle the downstream enforcement logic:
- tenant_id extraction (always from JWT, never from a raw header)
- role checking (pipeline-user vs pipeline-admin)
- JWT expiry detection

Used by BFF (Phase 3) and tested here to lock down the expected behaviour.
"""

from __future__ import annotations

import time
from typing import Optional


class AuthError(Exception):
    """Raised for any authentication or authorisation failure."""
    def __init__(self, message: str, status_code: int = 401):
        super().__init__(message)
        self.status_code = status_code


def extract_tenant_id(jwt_payload: dict) -> str:
    """Return the tenant ID from the JWT org_id claim.

    Kong sets this claim after validating the Keycloak token.  Callers must
    never derive the tenant from a raw request header — that would allow
    header forgery to bypass tenant isolation.
    """
    org_id = jwt_payload.get("org_id", "")
    if not org_id:
        raise AuthError("JWT missing org_id claim", status_code=401)
    return org_id


def check_not_expired(jwt_payload: dict) -> None:
    """Raise AuthError if the JWT has expired."""
    exp = jwt_payload.get("exp")
    if exp is None:
        raise AuthError("JWT missing exp claim", status_code=401)
    if time.time() > exp:
        raise AuthError("JWT has expired", status_code=401)


def get_roles(jwt_payload: dict) -> list[str]:
    """Return the list of realm roles from the JWT payload."""
    return jwt_payload.get("realm_access", {}).get("roles", [])


def require_role(jwt_payload: dict, role: str) -> None:
    """Raise AuthError(403) if the required role is absent from the JWT."""
    if role not in get_roles(jwt_payload):
        raise AuthError(
            f"Access denied: role '{role}' required", status_code=403
        )


def enforce_tenant_scope(jwt_payload: dict, requested_tenant_id: str) -> None:
    """Raise AuthError(403) if the JWT's tenant does not match the requested tenant.

    pipeline-admin callers may only access resources within their own organisation.
    """
    caller_tenant = extract_tenant_id(jwt_payload)
    if caller_tenant != requested_tenant_id:
        raise AuthError(
            f"Access denied: tenant '{requested_tenant_id}' is not accessible",
            status_code=403,
        )
