import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import api from "@/lib/axios";
import { Button } from "@/components/ui/button";

interface QuotaUsage {
  tenant_id: string;
  metric: string;
  current: number;
  limit: number;
  unlimited: boolean;
}

function ProgressBar({ value, max }: { value: number; max: number }) {
  const pct = max > 0 ? Math.min((value / max) * 100, 100) : 0;
  const color = pct >= 90 ? "bg-red-500" : pct >= 70 ? "bg-yellow-500" : "bg-green-500";
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 bg-muted rounded-full h-2 min-w-[80px]">
        <div
          className={`${color} h-2 rounded-full transition-all`}
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="text-xs text-muted-foreground whitespace-nowrap">
        {pct.toFixed(0)}%
      </span>
    </div>
  );
}

function QuotaRow({ row }: { row: QuotaUsage }) {
  const qc = useQueryClient();
  const [editing, setEditing] = useState(false);
  const [value, setValue] = useState(String(row.limit));

  const overrideMutation = useMutation({
    mutationFn: (newValue: number) =>
      api.put(`/admin/quota/${row.tenant_id}/${row.metric}`, { value: newValue }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["quota"] });
      setEditing(false);
    },
  });

  function handleSave() {
    const n = Number(value);
    if (!Number.isInteger(n) || n < 0) return;
    overrideMutation.mutate(n);
  }

  return (
    <tr className="hover:bg-muted/30">
      <td className="px-4 py-3 font-mono text-xs text-muted-foreground">
        {row.tenant_id.slice(0, 8)}…
      </td>
      <td className="px-4 py-3 text-sm">{row.metric}</td>
      <td className="px-4 py-3 text-sm text-right">{row.current.toLocaleString()}</td>
      <td className="px-4 py-3 text-sm text-right">
        {row.unlimited ? (
          <span className="text-green-600">∞</span>
        ) : (
          row.limit.toLocaleString()
        )}
      </td>
      <td className="px-4 py-3 w-40">
        {row.unlimited ? (
          <span className="text-xs text-muted-foreground">Unlimited</span>
        ) : (
          <ProgressBar value={row.current} max={row.limit} />
        )}
      </td>
      <td className="px-4 py-3 text-right">
        {editing ? (
          <div className="flex items-center justify-end gap-1">
            <input
              type="number"
              min={0}
              className="w-20 border border-input rounded px-2 py-1 text-xs bg-background"
              value={value}
              onChange={(e) => setValue(e.target.value)}
            />
            <Button
              size="sm"
              onClick={handleSave}
              disabled={overrideMutation.isPending}
            >
              {overrideMutation.isPending ? "…" : "Set"}
            </Button>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => { setEditing(false); setValue(String(row.limit)); }}
            >
              ✕
            </Button>
          </div>
        ) : (
          <Button
            variant="outline"
            size="sm"
            onClick={() => { setEditing(true); setValue(String(row.limit)); }}
          >
            Override
          </Button>
        )}
      </td>
    </tr>
  );
}

export function QuotaManagement() {
  const { data: quotas = [], isLoading, isError, refetch } = useQuery<QuotaUsage[]>({
    queryKey: ["quota"],
    queryFn: () => api.get("/admin/quota").then((r) => r.data),
    refetchInterval: 30000,
  });

  const grouped = quotas.reduce<Record<string, QuotaUsage[]>>((acc, q) => {
    (acc[q.tenant_id] ??= []).push(q);
    return acc;
  }, {});

  const tenantIds = Object.keys(grouped);

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">Quota Management</h1>
        <button
          onClick={() => refetch()}
          className="text-sm text-muted-foreground hover:text-foreground"
        >
          Refresh
        </button>
      </div>

      {isLoading && <p className="text-muted-foreground">Loading…</p>}
      {isError && <p className="text-red-600 text-sm">Failed to load quota data.</p>}

      {!isLoading && tenantIds.length === 0 && (
        <p className="text-muted-foreground text-sm">No quota records found.</p>
      )}

      {tenantIds.map((tenantId) => (
        <div key={tenantId} className="mb-6">
          <h2 className="text-sm font-medium text-muted-foreground mb-2 font-mono">
            Tenant: {tenantId.slice(0, 8)}…
          </h2>
          <div className="rounded-md border border-border overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-muted/50">
                <tr>
                  <th className="px-4 py-3 text-left font-medium">Tenant</th>
                  <th className="px-4 py-3 text-left font-medium">Metric</th>
                  <th className="px-4 py-3 text-right font-medium">Current</th>
                  <th className="px-4 py-3 text-right font-medium">Limit</th>
                  <th className="px-4 py-3 text-left font-medium">Usage</th>
                  <th className="px-4 py-3 text-right font-medium">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {grouped[tenantId].map((q) => (
                  <QuotaRow key={`${q.tenant_id}-${q.metric}`} row={q} />
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ))}
    </div>
  );
}
