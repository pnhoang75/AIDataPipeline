import { useState, useEffect, type ReactNode } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import api from "@/lib/axios";
import { Button } from "@/components/ui/button";

interface PipelineConfig {
  chunk_size: number;
  chunk_overlap: number;
  embedding_backend: string;
  milvus_index_type: string;
  milvus_nlist: number;
}

const EMBEDDING_BACKENDS = ["bge-small-en-v1.5", "bge-base-en-v1.5", "bge-large-en-v1.5"];
const MILVUS_INDEX_TYPES = ["IVF_FLAT", "IVF_SQ8", "HNSW"];

export function PipelineTuning() {
  const qc = useQueryClient();
  const [form, setForm] = useState<PipelineConfig>({
    chunk_size: 512,
    chunk_overlap: 50,
    embedding_backend: "bge-small-en-v1.5",
    milvus_index_type: "IVF_FLAT",
    milvus_nlist: 128,
  });
  const [saved, setSaved] = useState(false);

  const { data, isLoading, isError } = useQuery<PipelineConfig>({
    queryKey: ["pipeline-config"],
    queryFn: () => api.get("/admin/pipeline/config").then((r) => r.data),
  });

  useEffect(() => {
    if (data) setForm(data);
  }, [data]);

  const saveMutation = useMutation({
    mutationFn: (cfg: PipelineConfig) => api.put("/admin/pipeline/config", cfg),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["pipeline-config"] });
      setSaved(true);
      setTimeout(() => setSaved(false), 3000);
    },
  });

  function field(label: string, child: ReactNode) {
    return (
      <div className="grid grid-cols-3 gap-4 items-start py-4 border-b border-border last:border-0">
        <label className="text-sm font-medium pt-2">{label}</label>
        <div className="col-span-2">{child}</div>
      </div>
    );
  }

  return (
    <div className="max-w-2xl">
      <h1 className="text-2xl font-bold mb-6">Pipeline Tuning</h1>

      {isLoading && <p className="text-muted-foreground">Loading config…</p>}
      {isError && <p className="text-red-600 text-sm">Failed to load config.</p>}

      {!isLoading && (
        <div className="bg-card rounded-lg border border-border p-6">
          <h2 className="text-base font-semibold mb-4">Chunking</h2>
          {field(
            "Chunk Size (tokens)",
            <input
              type="number"
              min={64}
              max={4096}
              className="w-full border border-input rounded-md px-3 py-2 text-sm bg-background"
              value={form.chunk_size}
              onChange={(e) => setForm({ ...form, chunk_size: Number(e.target.value) })}
            />
          )}
          {field(
            "Chunk Overlap (tokens)",
            <input
              type="number"
              min={0}
              max={512}
              className="w-full border border-input rounded-md px-3 py-2 text-sm bg-background"
              value={form.chunk_overlap}
              onChange={(e) => setForm({ ...form, chunk_overlap: Number(e.target.value) })}
            />
          )}

          <h2 className="text-base font-semibold mt-6 mb-4">Embedding</h2>
          {field(
            "Embedding Backend",
            <select
              className="w-full border border-input rounded-md px-3 py-2 text-sm bg-background"
              value={form.embedding_backend}
              onChange={(e) => setForm({ ...form, embedding_backend: e.target.value })}
            >
              {EMBEDDING_BACKENDS.map((b) => (
                <option key={b} value={b}>{b}</option>
              ))}
            </select>
          )}

          <h2 className="text-base font-semibold mt-6 mb-4">Milvus Index</h2>
          {field(
            "Index Type",
            <select
              className="w-full border border-input rounded-md px-3 py-2 text-sm bg-background"
              value={form.milvus_index_type}
              onChange={(e) => setForm({ ...form, milvus_index_type: e.target.value })}
            >
              {MILVUS_INDEX_TYPES.map((t) => (
                <option key={t} value={t}>{t}</option>
              ))}
            </select>
          )}
          {field(
            "nlist",
            <input
              type="number"
              min={1}
              max={65536}
              className="w-full border border-input rounded-md px-3 py-2 text-sm bg-background"
              value={form.milvus_nlist}
              onChange={(e) => setForm({ ...form, milvus_nlist: Number(e.target.value) })}
            />
          )}

          <div className="flex items-center gap-3 mt-6">
            <Button
              onClick={() => saveMutation.mutate(form)}
              disabled={saveMutation.isPending}
            >
              {saveMutation.isPending ? "Saving…" : "Save Changes"}
            </Button>
            {saved && (
              <span className="text-green-600 text-sm">Saved successfully.</span>
            )}
            {saveMutation.isError && (
              <span className="text-red-600 text-sm">Save failed.</span>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
