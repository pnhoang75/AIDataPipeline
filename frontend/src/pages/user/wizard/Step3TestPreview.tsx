import { useState } from "react";
import api from "@/lib/axios";
import type { SourceType, TestResult } from "./types";

interface Props {
  sourceType: SourceType;
  credentials: Record<string, string>;
  result: TestResult | null;
  onResult: (result: TestResult) => void;
}

export function Step3TestPreview({ sourceType, credentials, result, onResult }: Props) {
  const [loading, setLoading] = useState(false);

  async function runTest() {
    setLoading(true);
    try {
      const res = await api.post("/sources/test", { source_type: sourceType, credentials }, { timeout: 15000 });
      onResult(res.data as TestResult);
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } }; message?: string })
        ?.response?.data?.detail ?? (err as { message?: string })?.message ?? "Connection failed";
      onResult({ status: "error", error: msg });
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="space-y-5">
      <p className="text-sm text-muted-foreground">
        Test the connection without creating anything. Step 4 is unlocked when the test passes.
      </p>

      <button
        onClick={runTest}
        disabled={loading}
        className="px-4 py-2 text-sm bg-primary text-primary-foreground rounded-md hover:opacity-90 disabled:opacity-50"
      >
        {loading ? "Testing…" : result ? "Re-test connection" : "Test connection"}
      </button>

      {loading && (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <span className="animate-spin inline-block w-4 h-4 border-2 border-primary border-t-transparent rounded-full" />
          Testing connection (up to 15 s)…
        </div>
      )}

      {result && !loading && (
        <div className={`rounded-lg border p-4 space-y-3 ${
          result.status === "ok" ? "border-green-500/30 bg-green-500/5" : "border-red-500/30 bg-red-500/5"
        }`}>
          <div className="flex items-center gap-2">
            <span className={result.status === "ok" ? "text-green-600" : "text-red-600"}>
              {result.status === "ok" ? "✓ Connection successful" : "✗ Connection failed"}
            </span>
            {result.latency_ms !== undefined && (
              <span className="text-xs text-muted-foreground">{result.latency_ms} ms</span>
            )}
          </div>

          {result.error && (
            <p className="text-sm text-red-600">{result.error}</p>
          )}

          {result.preview && result.preview.length > 0 && (
            <div>
              <p className="text-xs font-medium text-muted-foreground mb-1.5">
                First {result.preview.length} files found:
              </p>
              <ul className="space-y-0.5">
                {result.preview.map((f, i) => (
                  <li key={i} className="flex items-center justify-between text-xs">
                    <span className="truncate">{f.name}</span>
                    {f.size !== undefined && (
                      <span className="text-muted-foreground ml-2 shrink-0">{(f.size / 1024).toFixed(0)} KB</span>
                    )}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {result.status === "ok" && (!result.preview || result.preview.length === 0) && (
            <p className="text-xs text-muted-foreground">No files found — the source may be empty.</p>
          )}
        </div>
      )}
    </div>
  );
}
