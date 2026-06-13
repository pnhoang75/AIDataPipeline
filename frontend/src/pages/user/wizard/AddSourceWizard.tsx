import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import api from "@/lib/axios";
import { Step1TypeSelector } from "./Step1TypeSelector";
import { Step2CredentialsForm } from "./Step2CredentialsForm";
import { Step3TestPreview } from "./Step3TestPreview";
import { Step4NameSettings } from "./Step4NameSettings";
import type { SourceType, TestResult } from "./types";

interface Props {
  onClose: () => void;
}

const STEP_LABELS = ["Choose type", "Configure", "Test & preview", "Name & settings"];

function stepLabel(stepNum: number, sourceType: SourceType | null) {
  if (sourceType === "upload" && stepNum === 3) return null;
  return STEP_LABELS[stepNum - 1];
}

export function AddSourceWizard({ onClose }: Props) {
  const qc = useQueryClient();
  const [step, setStep] = useState<1 | 2 | 3 | 4>(1);
  const [sourceType, setSourceType] = useState<SourceType | null>(null);
  const [credentials, setCredentials] = useState<Record<string, string>>({});
  const [uploadFiles, setUploadFiles] = useState<File[]>([]);
  const [testResult, setTestResult] = useState<TestResult | null>(null);
  const [name, setName] = useState("");
  const [syncFrequency, setSyncFrequency] = useState("hourly");
  const [fileTypeFilter, setFileTypeFilter] = useState("*");
  const [maxFileSizeMb, setMaxFileSizeMb] = useState(100);
  const [workspaceId, setWorkspaceId] = useState("");
  const [startPaused, setStartPaused] = useState(false);

  const createMutation = useMutation({
    mutationFn: (body: Record<string, unknown>) =>
      api.post("/sources/create", body).then((r) => r.data),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["sources"] });
      onClose();
    },
  });

  const uploadMutation = useMutation({
    mutationFn: (payload: { files: File[]; name: string; workspaceId: string }) => {
      const fd = new FormData();
      payload.files.forEach((f) => fd.append("files", f));
      fd.append("name", payload.name);
      if (payload.workspaceId) fd.append("workspace_id", payload.workspaceId);
      return api.post("/sources/upload", fd, {
        headers: { "Content-Type": "multipart/form-data" },
      }).then((r) => r.data);
    },
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["sources"] });
      onClose();
    },
  });

  function handleTypeSelect(type: SourceType) {
    setSourceType(type);
    setCredentials({});
    setTestResult(null);
  }

  function handleNext() {
    if (step === 1) setStep(2);
    else if (step === 2) setStep(sourceType === "upload" ? 4 : 3);
    else if (step === 3) setStep(4);
  }

  function handleBack() {
    if (step === 2) setStep(1);
    else if (step === 3) setStep(2);
    else if (step === 4) setStep(sourceType === "upload" ? 2 : 3);
  }

  function handleSubmit() {
    if (sourceType === "upload") {
      uploadMutation.mutate({ files: uploadFiles, name: name.trim(), workspaceId });
    } else {
      createMutation.mutate({
        source_type: sourceType,
        credentials,
        name: name.trim(),
        sync_frequency: syncFrequency,
        file_type_filter: fileTypeFilter,
        max_file_size_mb: maxFileSizeMb,
        workspace_id: workspaceId || undefined,
        start_paused: startPaused,
      });
    }
  }

  const credentialsHaveRequiredFields =
    sourceType === "upload"
      ? uploadFiles.length > 0
      : Object.values(credentials).some((v) => v.trim() !== "");

  const nextDisabled =
    (step === 1 && !sourceType) ||
    (step === 2 && !credentialsHaveRequiredFields) ||
    (step === 3 && testResult?.status !== "ok");

  const submitDisabled =
    !name.trim() || createMutation.isPending || uploadMutation.isPending;

  const isBusy = createMutation.isPending || uploadMutation.isPending;
  const hasError = createMutation.isError || uploadMutation.isError;

  return (
    <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-4">
      <div className="bg-card border border-border rounded-lg w-full max-w-2xl max-h-[90vh] flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-border shrink-0">
          <h2 className="text-lg font-semibold">Add Data Source</h2>
          <button
            onClick={onClose}
            className="text-muted-foreground hover:text-foreground text-lg leading-none"
            aria-label="Close"
          >
            ✕
          </button>
        </div>

        {/* Step indicator */}
        <div className="px-6 py-3 border-b border-border shrink-0">
          <div className="flex items-center">
            {[1, 2, 3, 4].map((n) => {
              const label = stepLabel(n, sourceType);
              if (!label) return null;
              const isCurrent = step === n;
              const isDone = step > n && !(sourceType === "upload" && n === 3);
              return (
                <div key={n} className="flex items-center">
                  {n > 1 && <div className="w-6 h-px bg-border mx-1" />}
                  <div className={`flex items-center gap-1.5 text-xs ${
                    isCurrent ? "text-primary font-medium" : isDone ? "text-muted-foreground" : "text-muted-foreground/40"
                  }`}>
                    <span className={`w-5 h-5 rounded-full flex items-center justify-center text-xs shrink-0 ${
                      isCurrent
                        ? "bg-primary text-primary-foreground"
                        : isDone
                        ? "bg-muted text-muted-foreground"
                        : "border border-border"
                    }`}>
                      {isDone ? "✓" : n}
                    </span>
                    <span className="hidden sm:inline whitespace-nowrap">{label}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Step content */}
        <div className="px-6 py-6 overflow-y-auto flex-1">
          {step === 1 && (
            <Step1TypeSelector selected={sourceType} onSelect={handleTypeSelect} />
          )}
          {step === 2 && sourceType && (
            <Step2CredentialsForm
              sourceType={sourceType}
              credentials={credentials}
              onChange={setCredentials}
              files={uploadFiles}
              onFilesChange={setUploadFiles}
            />
          )}
          {step === 3 && sourceType && sourceType !== "upload" && (
            <Step3TestPreview
              sourceType={sourceType}
              credentials={credentials}
              result={testResult}
              onResult={setTestResult}
            />
          )}
          {step === 4 && sourceType && (
            <Step4NameSettings
              sourceType={sourceType}
              name={name}
              onNameChange={setName}
              syncFrequency={syncFrequency}
              onSyncFrequencyChange={setSyncFrequency}
              fileTypeFilter={fileTypeFilter}
              onFileTypeFilterChange={setFileTypeFilter}
              maxFileSizeMb={maxFileSizeMb}
              onMaxFileSizeMbChange={setMaxFileSizeMb}
              workspaceId={workspaceId}
              onWorkspaceIdChange={setWorkspaceId}
              startPaused={startPaused}
              onStartPausedChange={setStartPaused}
            />
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-4 border-t border-border shrink-0 flex items-center justify-between gap-3">
          <button
            onClick={step === 1 ? onClose : handleBack}
            className="px-4 py-2 text-sm border border-border rounded-md hover:bg-accent"
          >
            {step === 1 ? "Cancel" : "Back"}
          </button>

          <div className="flex items-center gap-3">
            {hasError && (
              <span className="text-red-600 text-sm">Failed to create source.</span>
            )}
            {step < 4 ? (
              <button
                onClick={handleNext}
                disabled={nextDisabled}
                className="px-4 py-2 text-sm bg-primary text-primary-foreground rounded-md hover:opacity-90 disabled:opacity-50"
              >
                Next
              </button>
            ) : (
              <button
                onClick={handleSubmit}
                disabled={submitDisabled}
                className="px-4 py-2 text-sm bg-primary text-primary-foreground rounded-md hover:opacity-90 disabled:opacity-50"
              >
                {isBusy ? "Creating…" : "Create"}
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
