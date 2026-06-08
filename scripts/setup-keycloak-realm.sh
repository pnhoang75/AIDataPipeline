#!/usr/bin/env bash
# Configure ai-pipeline realm: roles, OIDC clients, JWT mappers, organizations
set -euo pipefail

BASE_URL="${KEYCLOAK_URL:-http://localhost:8080}"
REALM="ai-pipeline"
ADMIN_USER="${ADMIN_USER:-admin}"
ADMIN_PASS="${ADMIN_PASS}"

# Get admin token
TOKEN=$(curl -sf -X POST "${BASE_URL}/realms/master/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=${ADMIN_USER}" \
  -d "password=${ADMIN_PASS}" \
  -d "grant_type=password" \
  -d "client_id=admin-cli" | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

echo "Got admin token"

auth_header="Authorization: Bearer $TOKEN"

# Create realm roles
for role in pipeline-admin pipeline-user pipeline-viewer; do
  DESC=""
  case $role in
    pipeline-admin) DESC="Pipeline administrator — full access";;
    pipeline-user) DESC="Pipeline user — workspace and query access";;
    pipeline-viewer) DESC="Pipeline viewer — read-only access";;
  esac
  curl -sf -X POST "${BASE_URL}/admin/realms/${REALM}/roles" \
    -H "$auth_header" -H "Content-Type: application/json" \
    -d "{\"name\":\"${role}\",\"description\":\"${DESC}\"}" || true
  echo "Created role: $role"
done

# JWT claims mappers for org_id and license_type (added to both clients later)
make_attr_mapper() {
  local name="$1" attr="$2" claim="$3"
  cat <<JSON
{
  "name": "${name}",
  "protocol": "openid-connect",
  "protocolMapper": "oidc-usermodel-attribute-mapper",
  "consentRequired": false,
  "config": {
    "user.attribute": "${attr}",
    "claim.name": "${claim}",
    "jsonType.label": "String",
    "id.token.claim": "true",
    "access.token.claim": "true",
    "userinfo.token.claim": "true",
    "multivalued": "false"
  }
}
JSON
}

make_roles_mapper() {
  cat <<'JSON'
{
  "name": "roles-mapper",
  "protocol": "openid-connect",
  "protocolMapper": "oidc-realm-role-mapper",
  "consentRequired": false,
  "config": {
    "id.token.claim": "true",
    "access.token.claim": "true",
    "claim.name": "roles",
    "jsonType.label": "String",
    "multivalued": "true",
    "userinfo.token.claim": "true"
  }
}
JSON
}

# Create SPA client (public, PKCE)
SPA_CLIENT=$(cat <<'JSON'
{
  "clientId": "spa-client",
  "name": "SPA Client (PKCE)",
  "description": "Public client for the React SPA — uses PKCE, implicit flow disabled",
  "enabled": true,
  "publicClient": true,
  "standardFlowEnabled": true,
  "implicitFlowEnabled": false,
  "directAccessGrantsEnabled": false,
  "serviceAccountsEnabled": false,
  "redirectUris": ["http://localhost:3000/*","http://localhost:8000/*","http://localhost/*"],
  "webOrigins": ["http://localhost:3000","http://localhost:8000","http://localhost"],
  "attributes": {"pkce.code.challenge.method": "S256"}
}
JSON
)
curl -sf -X POST "${BASE_URL}/admin/realms/${REALM}/clients" \
  -H "$auth_header" -H "Content-Type: application/json" -d "$SPA_CLIENT" || true
echo "Created SPA client"

# Get SPA client ID
SPA_ID=$(curl -sf "${BASE_URL}/admin/realms/${REALM}/clients?clientId=spa-client" \
  -H "$auth_header" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['id'])")
echo "SPA client internal ID: $SPA_ID"

# Add mappers to SPA client
for mapper in \
  "$(make_attr_mapper org_id-mapper org_id org_id)" \
  "$(make_attr_mapper org_name-mapper org_name org_name)" \
  "$(make_attr_mapper license_type-mapper license_type license_type)" \
  "$(make_attr_mapper quota_tier-mapper quota_tier quota_tier)" \
  "$(make_roles_mapper)"; do
  curl -sf -X POST "${BASE_URL}/admin/realms/${REALM}/clients/${SPA_ID}/protocol-mappers/models" \
    -H "$auth_header" -H "Content-Type: application/json" -d "$mapper" || true
done
echo "Added mappers to SPA client"

# Create BFF client (confidential)
BFF_CLIENT=$(cat <<'JSON'
{
  "clientId": "bff-client",
  "name": "BFF Client (confidential)",
  "description": "Confidential client for the Backend-for-Frontend service",
  "enabled": true,
  "publicClient": false,
  "standardFlowEnabled": true,
  "implicitFlowEnabled": false,
  "directAccessGrantsEnabled": true,
  "serviceAccountsEnabled": true,
  "redirectUris": ["http://localhost:8000/*","http://bff.ai-pipeline.svc.cluster.local/*"],
  "webOrigins": ["+"],
  "secret": "bff-client-secret-2026"
}
JSON
)
curl -sf -X POST "${BASE_URL}/admin/realms/${REALM}/clients" \
  -H "$auth_header" -H "Content-Type: application/json" -d "$BFF_CLIENT" || true
echo "Created BFF client"

# Get BFF client ID
BFF_ID=$(curl -sf "${BASE_URL}/admin/realms/${REALM}/clients?clientId=bff-client" \
  -H "$auth_header" | python3 -c "import sys,json; print(json.load(sys.stdin)[0]['id'])")
echo "BFF client internal ID: $BFF_ID"

# Add same mappers to BFF client
for mapper in \
  "$(make_attr_mapper org_id-mapper org_id org_id)" \
  "$(make_attr_mapper org_name-mapper org_name org_name)" \
  "$(make_attr_mapper license_type-mapper license_type license_type)" \
  "$(make_attr_mapper quota_tier-mapper quota_tier quota_tier)" \
  "$(make_roles_mapper)"; do
  curl -sf -X POST "${BASE_URL}/admin/realms/${REALM}/clients/${BFF_ID}/protocol-mappers/models" \
    -H "$auth_header" -H "Content-Type: application/json" -d "$mapper" || true
done
echo "Added mappers to BFF client"

# Enable organizations on the realm (requires preview/organizations feature)
curl -sf -X PUT "${BASE_URL}/admin/realms/${REALM}" \
  -H "$auth_header" -H "Content-Type: application/json" \
  -d '{"organizationsEnabled":true}' 2>/dev/null || echo "Note: organizationsEnabled not supported in this version"

# Seed two organizations
for org_name in free-tier-demo pro-tier-demo; do
  curl -sf -X POST "${BASE_URL}/admin/realms/${REALM}/organizations" \
    -H "$auth_header" -H "Content-Type: application/json" \
    -d "{\"name\":\"${org_name}\",\"enabled\":true}" 2>/dev/null || echo "Note: organizations API not available (preview feature)"
  echo "Attempted to create org: $org_name"
done

echo ""
echo "Setup complete. Testing OIDC endpoint..."
curl -sf "${BASE_URL}/realms/${REALM}/.well-known/openid-configuration" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print('Realm:', d.get('issuer', '?'))
print('Token endpoint:', d.get('token_endpoint', '?'))
print('JWKS URI:', d.get('jwks_uri', '?'))
"
