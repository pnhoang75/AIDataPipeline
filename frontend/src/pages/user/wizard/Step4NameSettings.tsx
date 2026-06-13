import { useQuery } from "@tanstack/react-query";
import api from "@/lib/axios";
import type { SourceType } from "./types";

interface Workspace {
  id: string;
  name: string;
}

interface Props {
  sourceType: SourceType;
  name: string;
  onNameChange: (v: string) => void;
  syncFrequency: string;
  onSyncFrequencyChange: (v: string) => void;
  fileTypeFilter: string;
  onFileTypeFilterChange: (v: string) => void;
  maxFileSizeMb: number;
  onMaxFileSizeMbChange: (v: number) => void;
  workspaceId: string;
  onWorkspaceIdChange: (v: string) => void;
  startPaused: boolean;
  onStartPausedChange: (v: boolean) => void;
}

const SYNC_FREQUENCIES = [
  { value: "realtime", label: "Real-time" },
  { value: "hourly", label: "Hourly" },
  { value: "daily", label: "Daily" },
  { value: "weekly", label: "Weekly" },
];

export function Step4NameSettings({
  sourceType,
  name,
  onNameChange,
  syncFrequency,
  onSyncFrequencyChange,
  fileTypeFilter,
  onFileTypeFilterChange,
  maxFileSizeMb,
  onMaxFileSizeMbChange,
  workspaceId,
  onWorkspaceIdChange,
  startPaused,
  onStartPausedChange,
}: Props) {
  const { data: workspaces = [] } = useQuery<Workspace[]>({
    queryKey: ["workspaces"],
    queryFn: () => api.get("/workspaces").then((r) => r.data),
  });

  const isUpload = sourceType === "upload";

  return (
    <div className="space-y-5">
      <div>
        <label className="block text-sm font-medium mb-1">
          Source name <span className="text-red-500">*</span>
        </label>
        <input
          value={name}
          onChange={(e) => onNameChange(e.target.value)}
          placeholder="My data source"
          className="w-full border border-border rounded-md px-3 py-2 text-sm bg-background focus:outline-none focus:ring-1 focus:ring-primary"
        />
      </div>

      {!isUpload && (
        <>
          <div>
            <label className="block text-sm font-medium mb-1">Sync frequency</label>
            <select
              value={syncFrequency}
              onChange={(e) => onSyncFrequencyChange(e.target.value)}
              className="w-full border border-border rounded-md px-3 py-2 text-sm bg-background focus:outline-none focus:ring-1 focus:ring-primary"
            >
              {SYNC_FREQUENCIES.map((f) => (
                <option key={f.value} value={f.value}>{f.label}</option>
              ))}
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium mb-1">File type filter</label>
            <input
              value={fileTypeFilter}
              onChange={(e) => onFileTypeFilterChange(e.target.value)}
              placeholder="*.pdf,*.docx,*.txt"
              className="w-full border border-border rounded-md px-3 py-2 text-sm bg-background focus:outline-none focus:ring-1 focus:ring-primary"
            />
            <p className="text-xs text-muted-foreground mt-1">Comma-separated glob patterns. Use * to include all files.</p>
          </div>

          <div>
            <label className="block text-sm font-medium mb-1">Max file size (MB)</label>
            <input
              type="number"
              min={1}
              max={2048}
              value={maxFileSizeMb}
              onChange={(e) => onMaxFileSizeMbChange(Number(e.target.value))}
              className="w-full border border-border rounded-md px-3 py-2 text-sm bg-background focus:outline-none focus:ring-1 focus:ring-primary"
            />
          </div>
        </>
      )}

      <div>
        <label className="block text-sm font-medium mb-1">Attach to workspace (optional)</label>
        <select
          value={workspaceId}
          onChange={(e) => onWorkspaceIdChange(e.target.value)}
          className="w-full border border-border rounded-md px-3 py-2 text-sm bg-background focus:outline-none focus:ring-1 focus:ring-primary"
        >
          <option value="">— None —</option>
          {workspaces.map((ws) => (
            <option key={ws.id} value={ws.id}>{ws.name}</option>
          ))}
        </select>
      </div>

      {!isUpload && (
        <label className="flex items-center gap-3 cursor-pointer">
          <input
            type="checkbox"
            checked={startPaused}
            onChange={(e) => onStartPausedChange(e.target.checked)}
            className="w-4 h-4 rounded border-border accent-primary"
          />
          <div>
            <span className="text-sm font-medium">Start paused</span>
            <p className="text-xs text-muted-foreground">Leave unchecked to begin ingestion immediately after creation.</p>
          </div>
        </label>
      )}
    </div>
  );
}
