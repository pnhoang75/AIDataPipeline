import { useState } from "react";
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

interface QualityCheck {
  id: string;
  entity_id: string;
  entity_type: string;
  entity_key: string;
  check_name: string;
  status: string;
  value: number | null;
  threshold: number | null;
  message: string | null;
  checked_at: string;
}

function StatusBadge({ ready, status }: { ready: boolean; status: string }) {
  const color = ready ? "bg-green-100 text-green-800" : "bg-red-100 text-red-800";
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${color}`}>
      {status}
    </span>
  );
}

function QualityBadge({ status }: { status: string }) {
  const color =
    status === "failed" ? "bg-red-100 text-red-800" : "bg-yellow-100 text-yellow-800";
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${color}`}>
      {status}
    </span>
  );
}

export function AdminDashboard() {
  const [tab, setTab] = useState<"overview" | "quality">("overview");

  const { data, isLoading, isError, refetch } = useQuery<PipelineStatus>({
    queryKey: ["pipeline-status"],
    queryFn: () => api.get("/admin/pipeline/status").then((r) => r.data),
    refetchInterval: 15000,
  });

  const {
    data: qualityChecks = [],
    isLoading: qualityLoading,
    isError: qualityError,
  } = useQuery<QualityCheck[]>({
    queryKey: ["data-quality"],
    queryFn: () => api.get("/quality").then((r) => r.data),
    enabled: tab === "quality",
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

      <div className="flex gap-1 mb-6 border-b border-border">
        {(["overview", "quality"] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-2 text-sm font-medium capitalize -mb-px border-b-2 transition-colors ${
              tab === t
                ? "border-primary text-primary"
                : "border-transparent text-muted-foreground hover:text-foreground"
            }`}
          >
            {t === "quality" ? "Data Quality" : "Overview"}
          </button>
        ))}
      </div>

      {tab === "overview" && (
        <>
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
        </>
      )}

      {tab === "quality" && (
        <>
          {qualityLoading && <p className="text-muted-foreground">Loading quality checks…</p>}
          {qualityError && (
            <p className="text-red-600 text-sm">Failed to load quality checks.</p>
          )}
          {!qualityLoading && !qualityError && qualityChecks.length === 0 && (
            <p className="text-muted-foreground text-sm">No failed or warned checks.</p>
          )}
          {qualityChecks.length > 0 && (
            <div className="rounded-md border border-border overflow-hidden">
              <table className="w-full text-sm">
                <thead className="bg-muted/50">
                  <tr>
                    <th className="px-4 py-3 text-left font-medium">Entity</th>
                    <th className="px-4 py-3 text-left font-medium">Check</th>
                    <th className="px-4 py-3 text-left font-medium">Status</th>
                    <th className="px-4 py-3 text-left font-medium">Message</th>
                    <th className="px-4 py-3 text-left font-medium">When</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {qualityChecks.map((qc) => (
                    <tr key={qc.id} className="hover:bg-muted/30">
                      <td className="px-4 py-3 font-mono text-xs truncate max-w-[120px]" title={qc.entity_key}>
                        <span className="text-muted-foreground">{qc.entity_type}/</span>{qc.entity_key.slice(-12)}
                      </td>
                      <td className="px-4 py-3 font-mono text-xs">{qc.check_name}</td>
                      <td className="px-4 py-3">
                        <QualityBadge status={qc.status} />
                      </td>
                      <td className="px-4 py-3 text-muted-foreground text-xs">{qc.message ?? "—"}</td>
                      <td className="px-4 py-3 text-muted-foreground text-xs">
                        {new Date(qc.checked_at).toLocaleString()}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  );
}
