import { useQuery } from "@tanstack/react-query";
import api from "@/lib/axios";

interface PodStatus {
  name: string;
  status: string;
  ready: boolean;
}

interface PipelineStatus {
  services: PodStatus[];
  tenant: string;
}

function StatusBadge({ ready, status }: { ready: boolean; status: string }) {
  const color = ready ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800";
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${color}`}>
      {status}
    </span>
  );
}

export function AdminDashboard() {
  const { data, isLoading, isError, refetch } = useQuery<PipelineStatus>({
    queryKey: ["pipeline-status"],
    queryFn: () => api.get("/admin/pipeline/status").then((r) => r.data),
    refetchInterval: 15000,
  });

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">Pipeline Dashboard</h1>
        <button
          onClick={() => refetch()}
          className="text-sm text-muted-foreground hover:text-foreground"
        >
          Refresh
        </button>
      </div>

      {isLoading && <p className="text-muted-foreground">Loading status…</p>}
      {isError && (
        <p className="text-red-600 text-sm">Failed to load pipeline status.</p>
      )}

      {data && (
        <>
          <p className="text-sm text-muted-foreground mb-4">
            Tenant: <span className="font-medium text-foreground">{data.tenant}</span>
          </p>
          <div className="rounded-md border border-border overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-muted/50">
                <tr>
                  <th className="px-4 py-3 text-left font-medium">Service</th>
                  <th className="px-4 py-3 text-left font-medium">Status</th>
                  <th className="px-4 py-3 text-left font-medium">Ready</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {data.services.length === 0 && (
                  <tr>
                    <td colSpan={3} className="px-4 py-6 text-center text-muted-foreground">
                      No services found.
                    </td>
                  </tr>
                )}
                {data.services.map((svc) => (
                  <tr key={svc.name} className="hover:bg-muted/30">
                    <td className="px-4 py-3 font-mono text-xs">{svc.name}</td>
                    <td className="px-4 py-3">
                      <StatusBadge ready={svc.ready} status={svc.status} />
                    </td>
                    <td className="px-4 py-3">
                      <span className={svc.ready ? "text-green-600" : "text-red-600"}>
                        {svc.ready ? "Yes" : "No"}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
