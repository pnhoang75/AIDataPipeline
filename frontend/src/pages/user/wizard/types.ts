export type SourceType = "s3" | "nfs" | "postgres" | "kafka" | "upload";

export interface TestResult {
  status: "ok" | "error";
  latency_ms?: number;
  preview?: Array<{ name: string; size?: number }>;
  error?: string;
}

export interface CredentialField {
  key: string;
  label: string;
  placeholder?: string;
  type?: "text" | "password" | "number";
  required?: boolean;
}

export const CREDENTIAL_FIELDS: Record<Exclude<SourceType, "upload">, CredentialField[]> = {
  s3: [
    { key: "bucket_name", label: "Bucket name", placeholder: "my-bucket", required: true },
    { key: "access_key_id", label: "Access key ID", placeholder: "AKIAIOSFODNN7EXAMPLE", required: true },
    { key: "secret_access_key", label: "Secret access key", type: "password", placeholder: "••••••••", required: true },
    { key: "region", label: "Region", placeholder: "us-east-1" },
    { key: "endpoint_url", label: "Endpoint URL (optional)", placeholder: "https://minio.example.com" },
  ],
  nfs: [
    { key: "server", label: "NFS server", placeholder: "nfs.example.com", required: true },
    { key: "export_path", label: "Export path", placeholder: "/exports/data", required: true },
    { key: "allowed_path_prefix", label: "Allowed path prefix", placeholder: "/exports/data/tenant", required: true },
  ],
  postgres: [
    { key: "host", label: "Host", placeholder: "db.example.com", required: true },
    { key: "port", label: "Port", placeholder: "5432", type: "number" },
    { key: "database", label: "Database", placeholder: "mydb", required: true },
    { key: "username", label: "Username", placeholder: "postgres", required: true },
    { key: "password", label: "Password", type: "password", placeholder: "••••••••", required: true },
    { key: "table_name", label: "Table name", placeholder: "documents", required: true },
    { key: "text_column", label: "Text column", placeholder: "body", required: true },
  ],
  kafka: [
    { key: "bootstrap_servers", label: "Bootstrap servers", placeholder: "kafka:9092", required: true },
    { key: "topic_name", label: "Topic name", placeholder: "my-topic", required: true },
    { key: "consumer_group", label: "Consumer group (optional)", placeholder: "pipeline-consumer" },
  ],
};

export const SOURCE_TYPE_META: Record<SourceType, { label: string; description: string; icon: string }> = {
  s3: {
    label: "Cloud Storage (S3)",
    description: "S3-compatible buckets — AWS S3, MinIO, GCS via S3 compat",
    icon: "☁",
  },
  nfs: {
    label: "NFS / File Server",
    description: "Directories on the NFS mount provisioned by your administrator",
    icon: "📁",
  },
  postgres: {
    label: "Database",
    description: "PostgreSQL tables with a text or document column",
    icon: "🗄",
  },
  kafka: {
    label: "Kafka Stream",
    description: "Live topics — messages are treated as document events",
    icon: "⚡",
  },
  upload: {
    label: "File Upload",
    description: "Browser-direct upload of PDF / DOCX / TXT / CSV up to 100 MB each",
    icon: "⬆",
  },
};
