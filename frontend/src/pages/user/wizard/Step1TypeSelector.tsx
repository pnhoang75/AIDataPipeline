import { SOURCE_TYPE_META, type SourceType } from "./types";

interface Props {
  selected: SourceType | null;
  onSelect: (type: SourceType) => void;
}

const TYPES: SourceType[] = ["s3", "nfs", "postgres", "kafka", "upload"];

export function Step1TypeSelector({ selected, onSelect }: Props) {
  return (
    <div>
      <p className="text-sm text-muted-foreground mb-4">
        Choose the type of data source you want to connect.
      </p>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {TYPES.map((type) => {
          const meta = SOURCE_TYPE_META[type];
          const isSelected = selected === type;
          return (
            <button
              key={type}
              onClick={() => onSelect(type)}
              className={`flex items-start gap-3 p-4 rounded-lg border text-left transition-colors ${
                isSelected
                  ? "border-primary bg-primary/5 ring-1 ring-primary"
                  : "border-border hover:border-primary/50 hover:bg-accent/50"
              }`}
            >
              <span className="text-2xl mt-0.5 shrink-0">{meta.icon}</span>
              <div className="min-w-0">
                <div className="font-medium text-sm">{meta.label}</div>
                <div className="text-xs text-muted-foreground mt-0.5">{meta.description}</div>
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}
