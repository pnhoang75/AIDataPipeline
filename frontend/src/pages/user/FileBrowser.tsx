import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import api from "@/lib/axios";

interface Workspace {
  id: string;
  name: string;
}

interface FileStatus {
  id: string;
  connector_id: string;
  file_path: string;
  ingest_status: string;
  file_size_bytes: number | null;
  chunk_count: number | null;
}

interface LineageRow {
  entity_type: string;
  count: number;
  entity_keys: string[];
}

const STATUS_STYLES: Record<string, string> = {
  indexed: "bg-green-100 text-green-800",
  processing: "bg-yellow-100 text-yellow-800",
  failed: "bg-red-100 text-red-800",
  pending: "bg-gray-100 text-gray-700",
  queued: "bg-blue-100 text-blue-800",
};

function StatusBadge({ status }: { status: string }) {
  const cls = STATUS_STYLES[status] ?? "bg-gray-100 text-gray-700";
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${cls}`}>
      {status}
    </span>
  );
}

function formatBytes(bytes: number | null): string {
  if (bytes === null) return "—";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function fileName(path: string): string {
  return path.split("/").pop() ?? path;
}

export function FileBrowser() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [page, setPage] = useState(1);
  const [selectedPath, setSelectedPath] = useState<string | null>(null);
  const perPage = 50;

  const { data: workspaces = [] } = useQuery<Workspace[]>({
    queryKey: ["workspaces"],
    queryFn: () => api.get("/workspaces").then((r) => r.data),
  });

  const workspaceId = searchParams.get("workspace") ?? (workspaces[0]?.id ?? "");

  const {
    data: files = [],
    isLoading,
    isError,
  } = useQuery<FileStatus[]>({
    queryKey: ["workspace-files", workspaceId, page],
    queryFn: () =>
      api
        .get(`/workspaces/${workspaceId}/files`, { params: { page, per_page: perPage } })
        .then((r) => r.data),
    enabled: Boolean(workspaceId),
  });

  const { data: lineage = [] } = useQuery<LineageRow[]>({
    queryKey: ["file-lineage", selectedPath],
    queryFn: () =>
      api
        .get(`/lineage/downstream/${encodeURIComponent(selectedPath!)}`)
        .then((r) => r.data),
    enabled: Boolean(selectedPath),
  });

  function selectWorkspace(id: string) {
    setSearchParams({ workspace: id });
    setPage(1);
    setSelectedPath(null);
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">File Browser</h1>
      </div>

      <div className="mb-4 flex items-center gap-3">
        <label className="text-sm font-medium">Workspace:</label>
        <select
          value={workspaceId}
          onChange={(e) => selectWorkspace(e.target.value)}
          className="border border-border rounded-md px-3 py-1.5 text-sm bg-background"
        >
          {workspaces.length === 0 && <option value="">No workspaces</option>}
          {workspaces.map((ws) => (
            <option key={ws.id} value={ws.id}>
              {ws.name}
            </option>
          ))}
        </select>
      </div>

      {!workspaceId && (
        <p className="text-muted-foreground text-sm">Select a workspace to browse its files.</p>
      )}

      {workspaceId && isLoading && (
        <p className="text-muted-foreground">Loading files…</p>
      )}

      {workspaceId && isError && (
        <p className="text-red-600 text-sm">Failed to load files.</p>
      )}

      {workspaceId && !isLoading && !isError && files.length === 0 && (
        <div className="text-center py-16 text-muted-foreground">
          <p className="text-lg">No files in this workspace yet.</p>
          <p className="text-sm mt-1">Connect a data source and wait for ingestion to complete.</p>
        </div>
      )}

      {workspaceId && !isLoading && !isError && files.length > 0 && (
        <>
          <div className="rounded-md border border-border overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-muted/50">
                <tr>
                  <th className="px-4 py-3 text-left font-medium">Name</th>
                  <th className="px-4 py-3 text-left font-medium">Path</th>
                  <th className="px-4 py-3 text-left font-medium">Size</th>
                  <th className="px-4 py-3 text-left font-medium">Chunks</th>
                  <th className="px-4 py-3 text-left font-medium">Status</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {files.map((f) => (
                  <tr
                    key={f.id}
                    onClick={() => setSelectedPath(selectedPath === f.file_path ? null : f.file_path)}
                    className={`cursor-pointer hover:bg-muted/20 ${selectedPath === f.file_path ? "bg-accent/40" : ""}`}
                  >
                    <td className="px-4 py-3 font-medium">{fileName(f.file_path)}</td>
                    <td className="px-4 py-3 text-muted-foreground text-xs truncate max-w-xs" title={f.file_path}>
                      {f.file_path}
                    </td>
                    <td className="px-4 py-3 text-muted-foreground">{formatBytes(f.file_size_bytes)}</td>
                    <td className="px-4 py-3 text-muted-foreground">{f.chunk_count ?? "—"}</td>
                    <td className="px-4 py-3">
                      <StatusBadge status={f.ingest_status} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="flex items-center justify-between mt-4 text-sm text-muted-foreground">
            <span>Page {page}</span>
            <div className="flex gap-2">
              <button
                onClick={() => setPage((p) => Math.max(1, p - 1))}
                disabled={page === 1}
                className="px-3 py-1 border border-border rounded-md hover:bg-accent disabled:opacity-40"
              >
                Previous
              </button>
              <button
                onClick={() => setPage((p) => p + 1)}
                disabled={files.length < perPage}
                className="px-3 py-1 border border-border rounded-md hover:bg-accent disabled:opacity-40"
              >
                Next
              </button>
            </div>
          </div>

          {selectedPath && (
            <div className="mt-6 rounded-md border border-border p-4">
              <h2 className="text-sm font-semibold mb-2">
                Lineage — <span className="font-mono text-xs text-muted-foreground">{selectedPath}</span>
              </h2>
              {lineage.length === 0 ? (
                <p className="text-sm text-muted-foreground">No downstream entities found.</p>
              ) : (
                <div className="flex gap-6">
                  {lineage.map((row) => (
                    <div key={row.entity_type} className="text-center">
                      <p className="text-2xl font-bold">{row.count}</p>
                      <p className="text-xs text-muted-foreground">{row.entity_type}</p>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}
