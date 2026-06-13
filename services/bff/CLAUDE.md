# bff (Pipeline Management API)

FastAPI BFF service providing admin and user endpoints for the pipeline UI. All routes are
protected by JWT auth middleware that validates the Bearer token and enforces tenant scoping
via the `org_id` claim. Kong injects `X-Tenant-ID`; the BFF validates it matches `org_id`.

## Relevant design docs
- docs/ai-data-pipeline-multitenancy.md §2 (JWT claims, tenant scoping)
- docs/ai-data-pipeline-security.md (bff-sa RBAC, mTLS)
- docs/ai-data-pipeline-ui.md (BFF endpoints)

## Auth flow
1. Kong validates the RS256 JWT and injects `X-Tenant-ID = org_id` header
2. BFF `require_auth` dependency: parses Bearer token, decodes JWT, verifies `org_id` matches `X-Tenant-ID`
3. `require_admin` dependency: additionally checks `pipeline-admin` in roles
4. Tenant scoping: all queries/mutations use `claims.org_id` — never a request body field

## Key endpoints
- `GET /api/health` — liveness (no auth)
- `GET /api/whoami` — returns JWT claims (requires auth)
- `GET /api/admin/health` — admin liveness (requires pipeline-admin role)

## Error envelope format
All error responses use the envelope:
```json
{"error": "ERROR_CODE", "message": "...", "request_id": "uuid"}
```

## How to run tests
```
pytest tests/unit/bff/ -x --tb=short -q
```
