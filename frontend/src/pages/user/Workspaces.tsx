import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import api from "@/lib/axios";

interface Workspace {
  id: string;
  tenant_id: string;
  owner_id: string;
  name: string;
  description: string | null;
}

export function Workspaces() {
  const qc = useQueryClient();
  const [showCreate, setShowCreate] = useState(false);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");

  const { data: workspaces = [], isLoading, isError } = useQuery<Workspace[]>({
    queryKey: ["workspaces"],
    queryFn: () => api.get("/workspaces").then((r) => r.data),
  });

  const createMutation = useMutation({
    mutationFn: (body: { name: string; description: string }) =>
      api.post("/workspaces", body).then((r) => r.data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["workspaces"] });
      setShowCreate(false);
      setName("");
      setDescription("");
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (id: string) => api.delete(`/workspaces/${id}`),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["workspaces"] });
    },
  });

  function handleCreate(e: React.FormEvent) {
    e.preventDefault();
    if (!name.trim()) return;
    createMutation.mutate({ name: name.trim(), description: description.trim() });
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">Workspaces</h1>
        <button
          onClick={() => setShowCreate(true)}
          className="px-4 py-2 bg-primary text-primary-foreground text-sm rounded-md hover:opacity-90"
        >
          New Workspace
        </button>
      </div>

      {isLoading && <p className="text-muted-foreground">Loading workspaces…</p>}
      {isError && <p className="text-red-600 text-sm">Failed to load workspaces.</p>}

      {!isLoading && !isError && workspaces.length === 0 && (
        <div className="text-center py-16 text-muted-foreground">
          <p className="text-lg">No workspaces yet.</p>
          <p className="text-sm mt-1">Create one to organise your data sources and files.</p>
        </div>
      )}

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
        {workspaces.map((ws) => (
          <div
            key={ws.id}
            className="rounded-lg border border-border bg-card p-5 flex flex-col gap-3"
          >
            <div className="flex items-start justify-between gap-2">
              <div>
                <h3 className="font-semibold text-base">{ws.name}</h3>
                {ws.description && (
                  <p className="text-sm text-muted-foreground mt-0.5">{ws.description}</p>
                )}
              </div>
              <button
                onClick={() => deleteMutation.mutate(ws.id)}
                disabled={deleteMutation.isPending}
                className="text-xs text-red-600 hover:text-red-800 disabled:opacity-50 shrink-0"
              >
                Delete
              </button>
            </div>
            <div className="text-xs text-muted-foreground">ID: {ws.id}</div>
            <a
              href={`/workspace/files?workspace=${ws.id}`}
              className="text-sm text-primary hover:underline"
            >
              Browse files →
            </a>
          </div>
        ))}
      </div>

      {showCreate && (
        <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50">
          <div className="bg-card border border-border rounded-lg p-6 w-full max-w-md">
            <h2 className="text-lg font-semibold mb-4">Create Workspace</h2>
            <form onSubmit={handleCreate} className="space-y-4">
              <div>
                <label className="block text-sm font-medium mb-1">Name *</label>
                <input
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  required
                  className="w-full border border-border rounded-md px-3 py-2 text-sm bg-background"
                  placeholder="My Workspace"
                />
              </div>
              <div>
                <label className="block text-sm font-medium mb-1">Description</label>
                <input
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                  className="w-full border border-border rounded-md px-3 py-2 text-sm bg-background"
                  placeholder="Optional description"
                />
              </div>
              {createMutation.isError && (
                <p className="text-red-600 text-sm">Failed to create workspace.</p>
              )}
              <div className="flex justify-end gap-2">
                <button
                  type="button"
                  onClick={() => {
                    setShowCreate(false);
                    setName("");
                    setDescription("");
                  }}
                  className="px-4 py-2 text-sm border border-border rounded-md hover:bg-accent"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={createMutation.isPending || !name.trim()}
                  className="px-4 py-2 text-sm bg-primary text-primary-foreground rounded-md hover:opacity-90 disabled:opacity-50"
                >
                  {createMutation.isPending ? "Creating…" : "Create"}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
