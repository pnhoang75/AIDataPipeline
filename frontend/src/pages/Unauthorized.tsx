import { useKeycloak } from "@react-keycloak/web";
import { Button } from "@/components/ui/button";

export function Unauthorized() {
  const { keycloak } = useKeycloak();

  return (
    <div className="flex flex-col items-center justify-center min-h-screen gap-4">
      <h1 className="text-2xl font-bold">Access Denied</h1>
      <p className="text-muted-foreground">You do not have permission to view this page.</p>
      <div className="flex gap-2">
        <Button variant="outline" onClick={() => window.history.back()}>Go Back</Button>
        <Button onClick={() => keycloak.logout()}>Sign Out</Button>
      </div>
    </div>
  );
}
