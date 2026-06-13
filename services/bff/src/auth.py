import logging
from typing import Optional

import httpx
from fastapi import Depends, Header, HTTPException, Request
from jose import JWTError, jwt

from config import Config, config as _default_config
from models import JWTClaims

logger = logging.getLogger(__name__)

_jwks_cache: dict = {}


def _fetch_jwks(jwks_url: str) -> dict:
    if jwks_url not in _jwks_cache:
        resp = httpx.get(jwks_url, timeout=5.0)
        resp.raise_for_status()
        _jwks_cache[jwks_url] = resp.json()
    return _jwks_cache[jwks_url]


def decode_token(token: str, cfg: Optional[Config] = None) -> dict:
    cfg = cfg or _default_config
    jwks = _fetch_jwks(cfg.keycloak_jwks_url)
    return jwt.decode(
        token,
        jwks,
        algorithms=cfg.jwt_algorithms,
        issuer=cfg.keycloak_issuer,
        options={"verify_aud": False},
    )


def require_auth(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    x_tenant_id: Optional[str] = Header(default=None),
) -> JWTClaims:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401,
            detail={
                "error": "MISSING_TOKEN",
                "message": "Authorization: Bearer <token> header required",
            },
        )

    token = authorization.split(" ", 1)[1]

    try:
        claims = decode_token(token)
    except JWTError as exc:
        raise HTTPException(
            status_code=401,
            detail={"error": "INVALID_TOKEN", "message": str(exc)},
        ) from exc

    org_id = claims.get("org_id")
    if not org_id:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "MISSING_ORG_ID",
                "message": "JWT is missing required org_id claim",
            },
        )

    # X-Tenant-ID is injected by Kong; validate it matches the JWT's org_id
    if x_tenant_id and x_tenant_id != org_id:
        raise HTTPException(
            status_code=403,
            detail={
                "error": "TENANT_MISMATCH",
                "message": "X-Tenant-ID header does not match JWT org_id",
            },
        )

    return JWTClaims(
        sub=claims.get("sub", ""),
        email=claims.get("email", ""),
        org_id=org_id,
        org_name=claims.get("org_name", ""),
        license_type=claims.get("license_type", "free"),
        quota_tier=claims.get("quota_tier", "free"),
        roles=claims.get("roles", []),
    )


def require_admin(claims: JWTClaims = Depends(require_auth)) -> JWTClaims:
    if "pipeline-admin" not in claims.roles:
        raise HTTPException(
            status_code=403,
            detail={
                "error": "FORBIDDEN",
                "message": "pipeline-admin role required",
            },
        )
    return claims
