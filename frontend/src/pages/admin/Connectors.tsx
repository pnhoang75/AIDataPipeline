import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import api from "@/lib/axios";
import { Button } from "@/components/ui/button";

interface Connector {
  id: string;
  name: string;
  source_type: string;
  config: Record<string, unknown>;
  tenant_id: string;
  start_paused: boolean;
}

interface ConnectorForm {
  name: string;
  source_type: string;
  config: string;
  start_paused: boolean;
}

const SOURCE_TYPES = ["s3", "nfs", "postgres", "kafka", "upload"];

const defaultForm: ConnectorForm = {
  name: "",
  source_type: "s3",
  config: "{}",
  start_paused: false,
};

function ConnectorModal({
  initial,
  onClose,
  onSave,
  saving,
}: {
  initial: ConnectorForm;
  onClose: () => void;
  onSave: (form: ConnectorForm) => void;
  saving: boolean;
}) {
  const [form, setForm] = useState<ConnectorForm>(initial);
  const [configError, setConfigError] = useState("");

  function handleSave() {
    try {
      JSON.parse(form.config);
      setConfigError("");
    } catch {
      setConfigError("Config must be valid JSON");
      return;
    }
    onSave(form);
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-card rounded-lg shadow-lg w-full max-w-md p-6">
        <h2 className="text-lg font-semibold mb-4">
          {initial.name ? "Edit Connector" : "New Connector"}
        </h2>
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium mb-1">Name</label>
            <input
              className="w-full border border-input rounded-md px-3 py-2 text-sm bg-background"
              value={form.name}
              onChange={(e) => setForm({ ...form, name: e.target.value })}
            />
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Source Type</label>
            <select
              className="w-full border border-input rounded-md px-3 py-2 text-sm bg-background"
              value={form.source_type}
              onChange={(e) => setForm({ ...form, source_type: e.target.value })}
            >
              {SOURCE_TYPES.map((t) => (
                <option key={t} value={t}>{t}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium mb-1">Config (JSON)</label>
            <textarea
              className="w-full border border-input rounded-md px-3 py-2 text-sm bg-background font-mono h-24"
              value={form.config}
              onChange={(e) => setForm({ ...form, config: e.target.value })}
            />
            {configError && (
              <p className="text-red-600 text-xs mt-1">{configError}</p>
            )}
          </div>
          <div className="flex items-center gap-2">
            <input
              type="checkbox"
              id="start_paused"
              checked={form.start_paused}
              onChange={(e) => setForm({ ...form, start_paused: e.target.checked })}
            />
            <label htmlFor="start_paused" className="text-sm">Start paused</label>
          </div>
        </div>
        <div className="flex justify-end gap-2 mt-6">
          <Button variant="outline" onClick={onClose} disabled={saving}>
            Cancel
          </Button>
          <Button onClick={handleSave} disabled={saving || !form.name}>
            {saving ? "Saving…" : "Save"}
          </Button>
        </div>
      </div>
    </div>
  );
}

export function Connectors() {
  const qc = useQueryClient();
  const [modal, setModal] = useState<{ open: boolean; editing: Connector | null }>({
    open: false,
    editing: null,
  });
  const [deleteConfirm, setDeleteConfirm] = useState<string | null>(null);

  const { data: connectors = [], isLoading, isError } = useQuery<Connector[]>({
    queryKey: ["connectors"],
    queryFn: () => api.get("/admin/connectors").then((r) => r.data),
  });

  const createMutation = useMutation({
    mutationFn: (form: ConnectorForm) =>
      api.post("/admin/connectors", {
        name: form.name,
        source_type: form.source_type,
        config: JSON.parse(form.config),
        start_paused: form.start_paused,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["connectors"] });
      setModal({ open: false, editing: null });
    },
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, form }: { id: string; form: ConnectorForm }) =>
      api.patch(`/admin/connectors/${id}`, {
        name: form.name,
        config: JSON.parse(form.config),
        start_paused: form.start_paused,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["connectors"] });
      setModal({ open: false, editing: null });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (id: string) => api.delete(`/admin/connectors/${id}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["connectors"] });
      setDeleteConfirm(null);
    },
  });

  function openCreate() {
    setModal({ open: true, editing: null });
  }

  function openEdit(c: Connector) {
    setModal({ open: true, editing: c });
  }

  function handleSave(form: ConnectorForm) {
    if (modal.editing) {
      updateMutation.mutate({ id: modal.editing.id, form });
    } else {
      createMutation.mutate(form);
    }
  }

  const saving = createMutation.isPending || updateMutation.isPending;

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">Connectors</h1>
        <Button onClick={openCreate}>+ New Connector</Button>
      </div>

      {isLoading && <p className="text-muted-foreground">Loading…</p>}
      {isError && <p className="text-red-600 text-sm">Failed to load connectors.</p>}

      {!isLoading && (
        <div className="rounded-md border border-border overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-muted/50">
              <tr>
                <th className="px-4 py-3 text-left font-medium">Name</th>
                <th className="px-4 py-3 text-left font-medium">Type</th>
                <th className="px-4 py-3 text-left font-medium">Paused</th>
                <th className="px-4 py-3 text-left font-medium">Tenant</th>
                <th className="px-4 py-3 text-right font-medium">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {connectors.length === 0 && (
                <tr>
                  <td colSpan={5} className="px-4 py-6 text-center text-muted-foreground">
                    No connectors yet.
                  </td>
                </tr>
              )}
              {connectors.map((c) => (
                <tr key={c.id} className="hover:bg-muted/30">
                  <td className="px-4 py-3 font-medium">{c.name}</td>
                  <td className="px-4 py-3">
                    <span className="px-2 py-0.5 rounded bg-secondary text-secondary-foreground text-xs">
                      {c.source_type}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    {c.start_paused ? (
                      <span className="text-yellow-600 text-xs">Paused</span>
                    ) : (
                      <span className="text-green-600 text-xs">Active</span>
                    )}
                  </td>
                  <td className="px-4 py-3 font-mono text-xs text-muted-foreground">
                    {c.tenant_id.slice(0, 8)}…
                  </td>
                  <td className="px-4 py-3 text-right space-x-2">
                    <Button variant="outline" size="sm" onClick={() => openEdit(c)}>
                      Edit
                    </Button>
                    <Button
                      variant="destructive"
                      size="sm"
                      onClick={() => setDeleteConfirm(c.id)}
                    >
                      Delete
                    </Button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {modal.open && (
        <ConnectorModal
          initial={
            modal.editing
              ? {
                  name: modal.editing.name,
                  source_type: modal.editing.source_type,
                  config: JSON.stringify(modal.editing.config, null, 2),
                  start_paused: modal.editing.start_paused,
                }
              : defaultForm
          }
          onClose={() => setModal({ open: false, editing: null })}
          onSave={handleSave}
          saving={saving}
        />
      )}

      {deleteConfirm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-card rounded-lg shadow-lg p-6 w-80">
            <h2 className="text-lg font-semibold mb-2">Delete connector?</h2>
            <p className="text-sm text-muted-foreground mb-4">
              This action cannot be undone.
            </p>
            <div className="flex justify-end gap-2">
              <Button
                variant="outline"
                onClick={() => setDeleteConfirm(null)}
                disabled={deleteMutation.isPending}
              >
                Cancel
              </Button>
              <Button
                variant="destructive"
                onClick={() => deleteMutation.mutate(deleteConfirm)}
                disabled={deleteMutation.isPending}
              >
                {deleteMutation.isPending ? "Deleting…" : "Delete"}
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
