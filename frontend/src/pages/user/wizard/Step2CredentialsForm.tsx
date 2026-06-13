import { useRef, useState } from "react";
import { CREDENTIAL_FIELDS, type SourceType } from "./types";

interface Props {
  sourceType: SourceType;
  credentials: Record<string, string>;
  onChange: (creds: Record<string, string>) => void;
  files: File[];
  onFilesChange: (files: File[]) => void;
}

function UploadZone({ files, onFilesChange }: { files: File[]; onFilesChange: (files: File[]) => void }) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [dragging, setDragging] = useState(false);

  function addFiles(incoming: FileList | null) {
    if (!incoming) return;
    const next = [...files];
    Array.from(incoming).forEach((f) => {
      if (!next.find((e) => e.name === f.name && e.size === f.size)) next.push(f);
    });
    onFilesChange(next);
  }

  function removeFile(idx: number) {
    onFilesChange(files.filter((_, i) => i !== idx));
  }

  return (
    <div className="space-y-4">
      <p className="text-sm text-muted-foreground">
        Drop PDF, DOCX, TXT, or CSV files here. Max 100 MB each. An upload-watcher will process them within 30 s.
      </p>
      <div
        onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
        onDragLeave={() => setDragging(false)}
        onDrop={(e) => { e.preventDefault(); setDragging(false); addFiles(e.dataTransfer.files); }}
        onClick={() => inputRef.current?.click()}
        className={`border-2 border-dashed rounded-lg p-10 text-center cursor-pointer transition-colors ${
          dragging ? "border-primary bg-primary/5" : "border-border hover:border-primary/50 hover:bg-accent/30"
        }`}
      >
        <input
          ref={inputRef}
          type="file"
          multiple
          accept=".pdf,.docx,.txt,.csv"
          className="hidden"
          onChange={(e) => addFiles(e.target.files)}
        />
        <p className="text-sm font-medium">Drag files here or click to browse</p>
        <p className="text-xs text-muted-foreground mt-1">PDF · DOCX · TXT · CSV</p>
      </div>

      {files.length > 0 && (
        <ul className="divide-y divide-border border border-border rounded-md text-sm">
          {files.map((f, i) => (
            <li key={i} className="flex items-center justify-between px-3 py-2">
              <span className="truncate text-sm">{f.name}</span>
              <div className="flex items-center gap-3 shrink-0 ml-2">
                <span className="text-xs text-muted-foreground">{(f.size / 1024 / 1024).toFixed(1)} MB</span>
                <button
                  onClick={(e) => { e.stopPropagation(); removeFile(i); }}
                  className="text-xs text-red-600 hover:text-red-800"
                >
                  Remove
                </button>
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

export function Step2CredentialsForm({ sourceType, credentials, onChange, files, onFilesChange }: Props) {
  if (sourceType === "upload") {
    return <UploadZone files={files} onFilesChange={onFilesChange} />;
  }

  const fields = CREDENTIAL_FIELDS[sourceType];

  function set(key: string, value: string) {
    onChange({ ...credentials, [key]: value });
  }

  return (
    <div className="space-y-4">
      <p className="text-sm text-muted-foreground">
        Credentials are stored as a Kubernetes Secret and never returned by the API.
      </p>
      {fields.map((field) => (
        <div key={field.key}>
          <label className="block text-sm font-medium mb-1">
            {field.label}
            {field.required && <span className="text-red-500 ml-0.5">*</span>}
          </label>
          <input
            type={field.type ?? "text"}
            value={credentials[field.key] ?? ""}
            onChange={(e) => set(field.key, e.target.value)}
            placeholder={field.placeholder}
            className="w-full border border-border rounded-md px-3 py-2 text-sm bg-background focus:outline-none focus:ring-1 focus:ring-primary"
          />
        </div>
      ))}
    </div>
  );
}
