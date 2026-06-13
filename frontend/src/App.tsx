import { useKeycloak } from "@react-keycloak/web";
import { AppRouter } from "@/router";
import { useAuthStore } from "@/store/auth";
import { useEffect } from "react";

function App() {
  const { keycloak, initialized } = useKeycloak();
  const setRoles = useAuthStore((s) => s.setRoles);

  useEffect(() => {
    if (initialized && keycloak.authenticated && keycloak.realmAccess?.roles) {
      setRoles(keycloak.realmAccess.roles);
    }
  }, [initialized, keycloak.authenticated, keycloak.realmAccess?.roles, setRoles]);

  if (!initialized) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <p className="text-muted-foreground">Loading…</p>
      </div>
    );
  }

  return <AppRouter />;
}

export default App;
