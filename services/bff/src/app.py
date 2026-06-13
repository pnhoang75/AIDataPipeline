import logging
import uuid

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from admin_endpoints import router as admin_router
from auth import JWTClaims, require_admin, require_auth
from tenant_endpoints import router as tenant_router
from user_endpoints import router as user_router

logger = logging.getLogger(__name__)

app = FastAPI(title="Pipeline Management API (BFF)", version="1.0.0")
app.include_router(admin_router)
app.include_router(tenant_router)
app.include_router(user_router)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request.state.request_id = str(uuid.uuid4())
    return await call_next(request)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    request_id = getattr(request.state, "request_id", None)
    detail = exc.detail
    if isinstance(detail, dict):
        body = {**detail, "request_id": request_id}
    else:
        body = {
            "error": "HTTP_ERROR",
            "message": str(detail),
            "request_id": request_id,
        }
    return JSONResponse(status_code=exc.status_code, content=body)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    request_id = getattr(request.state, "request_id", None)
    logger.exception("Unhandled error: %s", exc)
    return JSONResponse(
        status_code=500,
        content={
            "error": "INTERNAL_ERROR",
            "message": "An unexpected error occurred",
            "request_id": request_id,
        },
    )


@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/whoami")
async def whoami(claims: JWTClaims = Depends(require_auth)):
    return {
        "sub": claims.sub,
        "email": claims.email,
        "org_id": claims.org_id,
        "org_name": claims.org_name,
        "license_type": claims.license_type,
        "roles": claims.roles,
    }


@app.get("/api/admin/health")
async def admin_health(claims: JWTClaims = Depends(require_admin)):
    return {"status": "ok", "tenant": claims.org_id}
