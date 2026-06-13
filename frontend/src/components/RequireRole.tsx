import { useKeycloak } from "@react-keycloak/web";
import { Navigate } from "react-router-dom";

interface RequireRoleProps {
  role: string;
  children: React.ReactNode;
}

export function RequireRole({ role, children }: RequireRoleProps) {
  const { keycloak, initialized } = useKeycloak();

  if (!initialized) {
    return null;
  }

  if (!keycloak.authenticated) {
    keycloak.login();
    return null;
  }

  if (!keycloak.hasRealmRole(role)) {
    return <Navigate to="/unauthorized" replace />;
  }

  return <>{children}</>;
}
