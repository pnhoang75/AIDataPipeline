"""Validate Keycloak realm-import.yaml configuration without a live cluster."""
import pathlib
import yaml
import pytest

MANIFEST_DIR = pathlib.Path(__file__).parents[3] / "k8s" / "operators" / "keycloak"
REALM_IMPORT = MANIFEST_DIR / "realm-import.yaml"
KEYCLOAK_CR = MANIFEST_DIR / "keycloak.yaml"
SEED_ORGS = MANIFEST_DIR / "seed-orgs.yaml"


@pytest.fixture(scope="module")
def realm_import():
    return yaml.safe_load(REALM_IMPORT.read_text())


@pytest.fixture(scope="module")
def realm(realm_import):
    return realm_import["spec"]["realm"]


def test_realm_name(realm):
    assert realm["realm"] == "ai-pipeline"


def test_realm_enabled(realm):
    assert realm["enabled"] is True


def test_implicit_flow_disabled(realm):
    for client in realm["clients"]:
        assert client.get("implicitFlowEnabled") is False, (
            f"implicitFlowEnabled must be False for client {client['clientId']}"
        )


def test_spa_client_pkce(realm):
    spa = next(c for c in realm["clients"] if c["clientId"] == "spa-client")
    assert spa["publicClient"] is True
    assert spa["attributes"]["pkce.code.challenge.method"] == "S256"
    assert spa["standardFlowEnabled"] is True
    assert spa.get("directAccessGrantsEnabled") is False


def test_bff_client_confidential(realm):
    bff = next(c for c in realm["clients"] if c["clientId"] == "bff-client")
    assert bff["publicClient"] is False
    assert bff["serviceAccountsEnabled"] is True


def test_required_roles_defined(realm):
    role_names = {r["name"] for r in realm["roles"]["realm"]}
    assert "pipeline-admin" in role_names
    assert "pipeline-user" in role_names
    assert "pipeline-viewer" in role_names


def _mapper_claim_names(client):
    return {m["config"]["claim.name"] for m in client.get("protocolMappers", [])}


def test_spa_jwt_claims(realm):
    spa = next(c for c in realm["clients"] if c["clientId"] == "spa-client")
    claims = _mapper_claim_names(spa)
    for required in ("org_id", "org_name", "license_type", "quota_tier", "roles"):
        assert required in claims, f"spa-client missing claim mapper: {required}"


def test_bff_jwt_claims(realm):
    bff = next(c for c in realm["clients"] if c["clientId"] == "bff-client")
    claims = _mapper_claim_names(bff)
    for required in ("org_id", "org_name", "license_type", "quota_tier", "roles"):
        assert required in claims, f"bff-client missing claim mapper: {required}"


def test_keycloak_cr_version():
    cr = yaml.safe_load(KEYCLOAK_CR.read_text())
    assert cr["apiVersion"] == "k8s.keycloak.org/v2alpha1"
    assert cr["spec"]["image"].startswith("quay.io/keycloak/keycloak:24")


def test_keycloak_organizations_feature_enabled():
    cr = yaml.safe_load(KEYCLOAK_CR.read_text())
    features = cr["spec"].get("features", {}).get("enabled", [])
    assert "preview" in features, "Organizations requires preview feature flag"


def test_seed_orgs_job_exists():
    assert SEED_ORGS.exists(), "seed-orgs.yaml Job must exist"
    doc = yaml.safe_load(SEED_ORGS.read_text())
    assert doc["kind"] == "Job"
    script = doc["spec"]["template"]["spec"]["containers"][0]["command"][-1]
    assert "free-tier-demo" in script
    assert "pro-tier-demo" in script
