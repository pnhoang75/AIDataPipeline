import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import api from "@/lib/axios";
import { AddSourceWizard } from "./wizard/AddSourceWizard";

interface Source {
  id: string;
  name: string;
  source_type: string;
  tenant_id: string;
}

const SOURCE_TYPE_LABELS: Record<string, string> = {
  s3: "S3 Buckets",
  nfs: "NFS Folders",
  postgres: "DB Tables",
  kafka: "Kafka Topics",
  upload: "File Uploads",
};

const SOURCE_TYPE_ICON: Record<string, string> = {
  s3: "☁",
  nfs: "📁",
  postgres: "🗄",
  kafka: "⚡",
  upload: "⬆",
};

function SourceTypeGroup({ type, sources }: { type: string; sources: Source[] }) {
  const [expanded, setExpanded] = useState(true);
  const label = SOURCE_TYPE_LABELS[type] ?? type;
  const icon = SOURCE_TYPE_ICON[type] ?? "•";

  return (
    <div className="border border-border rounded-md overflow-hidden">
      <button
        onClick={() => setExpanded((v) => !v)}
        className="w-full flex items-center justify-between px-4 py-3 bg-muted/50 hover:bg-muted text-left"
      >
        <span className="flex items-center gap-2 font-medium text-sm">
          <span>{icon}</span>
          <span>{label}</span>
          <span className="text-xs text-muted-foreground font-normal">({sources.length})</span>
        </span>
        <span className="text-muted-foreground text-xs">{expanded ? "▲" : "▼"}</span>
      </button>

      {expanded && (
        <ul className="divide-y divide-border">
          {sources.length === 0 && (
            <li className="px-4 py-3 text-sm text-muted-foreground">No {label.toLowerCase()} configured.</li>
          )}
          {sources.map((src) => (
            <li key={src.id} className="px-4 py-3 flex items-center justify-between">
              <div>
                <span className="text-sm font-medium">{src.name}</span>
                <span className="ml-2 text-xs text-muted-foreground">{src.id}</span>
              </div>
              <span className="text-xs bg-muted px-2 py-0.5 rounded">{src.source_type}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export function DataSources() {
  const [showWizard, setShowWizard] = useState(false);
  const { data: sources = [], isLoading, isError } = useQuery<Source[]>({
    queryKey: ["sources"],
    queryFn: () => api.get("/sources").then((r) => r.data),
  });

  const grouped = sources.reduce<Record<string, Source[]>>((acc, src) => {
    const key = src.source_type;
    if (!acc[key]) acc[key] = [];
    acc[key].push(src);
    return acc;
  }, {});

  const orderedTypes = Object.keys(SOURCE_TYPE_LABELS).filter(
    (t) => t in grouped || sources.some((s) => s.source_type === t),
  );
  const extraTypes = Object.keys(grouped).filter((t) => !orderedTypes.includes(t));
  const displayTypes = [...orderedTypes, ...extraTypes];

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">Data Sources</h1>
        <button
          onClick={() => setShowWizard(true)}
          className="px-4 py-2 bg-primary text-primary-foreground text-sm rounded-md hover:opacity-90"
        >
          Add source
        </button>
      </div>

      {isLoading && <p className="text-muted-foreground">Loading data sources…</p>}
      {isError && <p className="text-red-600 text-sm">Failed to load data sources.</p>}

      {!isLoading && !isError && sources.length === 0 && (
        <div className="text-center py-16 text-muted-foreground">
          <p className="text-lg">No data sources connected yet.</p>
          <p className="text-sm mt-1">Ask your administrator to configure connectors.</p>
        </div>
      )}

      {!isLoading && !isError && sources.length > 0 && (
        <div className="space-y-3">
          {displayTypes.map((type) => (
            <SourceTypeGroup
              key={type}
              type={type}
              sources={grouped[type] ?? []}
            />
          ))}
        </div>
      )}

      {showWizard && <AddSourceWizard onClose={() => setShowWizard(false)} />}
    </div>
  );
}
