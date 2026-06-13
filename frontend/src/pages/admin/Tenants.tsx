import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import api from "@/lib/axios";
import { Button } from "@/components/ui/button";

interface Tenant {
  id: string;
  name: string;
  license_type: string;
}

interface TenantUser {
  id?: string;
  email?: string;
  username?: string;
}

interface QuotaUsage {
  tenant_id: string;
  metric: string;
  current: number;
  limit: number;
  unlimited: boolean;
}

const LICENSE_TIERS = ["free", "pro", "enterprise"];

function ProgressBar({ value, max }: { value: number; max: number }) {
  const pct = max > 0 ? Math.min((value / max) * 100, 100) : 0;
  const color = pct >= 90 ? "bg-red-500" : pct >= 70 ? "bg-yellow-500" : "bg-green-500";
  return (
    <div className="flex items-center gap-2">
      <div className="flex-1 bg-muted rounded-full h-2">
        <div className={`${color} h-2 rounded-full transition-all`} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs text-muted-foreground whitespace-nowrap">
        {value}/{max}
      </span>
    </div>
  );
}

function TenantRow({ tenant }: { tenant: Tenant }) {
  const qc = useQueryClient();
  const [expanded, setExpanded] = useState(false);
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteRoles, setInviteRoles] = useState("developer");
  const [editLicense, setEditLicense] = useState(false);
  const [selectedLicense, setSelectedLicense] = useState(tenant.license_type);

  const { data: users = [], isLoading: usersLoading } = useQuery<TenantUser[]>({
    queryKey: ["tenant-users", tenant.id],
    queryFn: () => api.get(`/admin/tenants/${tenant.id}/users`).then((r) => r.data),
    enabled: expanded,
  });

  const { data: quotas = [] } = useQuery<QuotaUsage[]>({
    queryKey: ["quota"],
    queryFn: () => api.get("/admin/quota").then((r) => r.data),
  });

  const tenantQuotas = quotas.filter((q) => q.tenant_id === tenant.id);

  const licenseMutation = useMutation({
    mutationFn: (license_type: string) =>
      api.patch(`/admin/tenants/${tenant.id}/license`, { license_type }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["tenants"] });
      setEditLicense(false);
    },
  });

  const inviteMutation = useMutation({
    mutationFn: () =>
      api.post(`/admin/tenants/${tenant.id}/users`, {
        email: inviteEmail,
        roles: inviteRoles.split(",").map((r) => r.trim()),
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["tenant-users", tenant.id] });
      setInviteEmail("");
    },
  });

  return (
    <>
      <tr className="hover:bg-muted/30">
        <td className="px-4 py-3 font-medium">{tenant.name}</td>
        <td className="px-4 py-3">
          {editLicense ? (
            <div className="flex items-center gap-2">
              <select
                className="border border-input rounded-md px-2 py-1 text-xs bg-background"
                value={selectedLicense}
                onChange={(e) => setSelectedLicense(e.target.value)}
              >
                {LICENSE_TIERS.map((t) => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
              <Button
                size="sm"
                onClick={() => licenseMutation.mutate(selectedLicense)}
                disabled={licenseMutation.isPending}
              >
                Save
              </Button>
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setEditLicense(false)}
              >
                ✕
              </Button>
            </div>
          ) : (
            <div className="flex items-center gap-2">
              <span className="px-2 py-0.5 rounded bg-secondary text-secondary-foreground text-xs capitalize">
                {tenant.license_type}
              </span>
              <button
                className="text-xs text-muted-foreground hover:text-foreground"
                onClick={() => { setEditLicense(true); setSelectedLicense(tenant.license_type); }}
              >
                Edit
              </button>
            </div>
          )}
        </td>
        <td className="px-4 py-3">
          <div className="space-y-1 min-w-[160px]">
            {tenantQuotas.length === 0 && (
              <span className="text-xs text-muted-foreground">No quota data</span>
            )}
            {tenantQuotas.map((q) => (
              <div key={q.metric}>
                <p className="text-xs text-muted-foreground mb-0.5">{q.metric}</p>
                {q.unlimited ? (
                  <span className="text-xs text-green-600">Unlimited</span>
                ) : (
                  <ProgressBar value={q.current} max={q.limit} />
                )}
              </div>
            ))}
          </div>
        </td>
        <td className="px-4 py-3 text-right">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setExpanded((v) => !v)}
          >
            {expanded ? "Hide Users" : "Show Users"}
          </Button>
        </td>
      </tr>
      {expanded && (
        <tr>
          <td colSpan={4} className="px-4 pb-4 bg-muted/20">
            <div className="mt-2">
              <h3 className="text-sm font-medium mb-2">Users</h3>
              {usersLoading && <p className="text-xs text-muted-foreground">Loading…</p>}
              {!usersLoading && users.length === 0 && (
                <p className="text-xs text-muted-foreground">No users in this tenant.</p>
              )}
              {users.map((u, i) => (
                <div key={u.id ?? i} className="text-xs py-1 border-b border-border last:border-0">
                  {u.email ?? u.username ?? u.id}
                </div>
              ))}
              <div className="mt-3 flex items-center gap-2">
                <input
                  className="border border-input rounded-md px-2 py-1 text-xs bg-background flex-1"
                  placeholder="user@example.com"
                  value={inviteEmail}
                  onChange={(e) => setInviteEmail(e.target.value)}
                />
                <input
                  className="border border-input rounded-md px-2 py-1 text-xs bg-background w-32"
                  placeholder="roles (comma-sep)"
                  value={inviteRoles}
                  onChange={(e) => setInviteRoles(e.target.value)}
                />
                <Button
                  size="sm"
                  onClick={() => inviteMutation.mutate()}
                  disabled={inviteMutation.isPending || !inviteEmail}
                >
                  {inviteMutation.isPending ? "Inviting…" : "Invite"}
                </Button>
              </div>
              {inviteMutation.isSuccess && (
                <p className="text-green-600 text-xs mt-1">Invitation sent.</p>
              )}
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

export function Tenants() {
  const qc = useQueryClient();
  const [showCreate, setShowCreate] = useState(false);
  const [newName, setNewName] = useState("");
  const [newLicense, setNewLicense] = useState("free");

  const { data: tenants = [], isLoading, isError } = useQuery<Tenant[]>({
    queryKey: ["tenants"],
    queryFn: () => api.get("/admin/tenants").then((r) => r.data),
  });

  const createMutation = useMutation({
    mutationFn: () =>
      api.post("/admin/tenants", { name: newName, license_type: newLicense }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["tenants"] });
      setShowCreate(false);
      setNewName("");
      setNewLicense("free");
    },
  });

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">Tenants &amp; Users</h1>
        <Button onClick={() => setShowCreate(true)}>+ New Tenant</Button>
      </div>

      {isLoading && <p className="text-muted-foreground">Loading…</p>}
      {isError && <p className="text-red-600 text-sm">Failed to load tenants.</p>}

      {!isLoading && (
        <div className="rounded-md border border-border overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-muted/50">
              <tr>
                <th className="px-4 py-3 text-left font-medium">Tenant</th>
                <th className="px-4 py-3 text-left font-medium">License</th>
                <th className="px-4 py-3 text-left font-medium">Quota Usage</th>
                <th className="px-4 py-3 text-right font-medium">Users</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {tenants.length === 0 && (
                <tr>
                  <td colSpan={4} className="px-4 py-6 text-center text-muted-foreground">
                    No tenants yet.
                  </td>
                </tr>
              )}
              {tenants.map((t) => (
                <TenantRow key={t.id} tenant={t} />
              ))}
            </tbody>
          </table>
        </div>
      )}

      {showCreate && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-card rounded-lg shadow-lg w-80 p-6">
            <h2 className="text-lg font-semibold mb-4">New Tenant</h2>
            <div className="space-y-3">
              <div>
                <label className="block text-sm font-medium mb-1">Name</label>
                <input
                  className="w-full border border-input rounded-md px-3 py-2 text-sm bg-background"
                  value={newName}
                  onChange={(e) => setNewName(e.target.value)}
                />
              </div>
              <div>
                <label className="block text-sm font-medium mb-1">License</label>
                <select
                  className="w-full border border-input rounded-md px-3 py-2 text-sm bg-background"
                  value={newLicense}
                  onChange={(e) => setNewLicense(e.target.value)}
                >
                  {LICENSE_TIERS.map((t) => (
                    <option key={t} value={t}>{t}</option>
                  ))}
                </select>
              </div>
            </div>
            <div className="flex justify-end gap-2 mt-4">
              <Button variant="outline" onClick={() => setShowCreate(false)}>
                Cancel
              </Button>
              <Button
                onClick={() => createMutation.mutate()}
                disabled={createMutation.isPending || !newName}
              >
                {createMutation.isPending ? "Creating…" : "Create"}
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
