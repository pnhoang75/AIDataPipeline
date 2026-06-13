import Keycloak from "keycloak-js";

const keycloak = new Keycloak({
  url: import.meta.env.VITE_KEYCLOAK_URL ?? "http://keycloak.infrastructure.svc:8080",
  realm: import.meta.env.VITE_KEYCLOAK_REALM ?? "ai-pipeline",
  clientId: import.meta.env.VITE_KEYCLOAK_CLIENT_ID ?? "pipeline-ui",
});

export default keycloak;
