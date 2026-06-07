# embedding-worker

Consumes `document-chunks` Kafka topic, batches up to 32 chunks or 500 ms, generates 384-dim
embeddings via a pluggable backend, writes vectors to Milvus, publishes to `embedding-events`,
and updates `source_file_status` to `indexed`.

## Relevant design docs
- docs/ai-data-pipeline-design.md §2.4, §2.5
- docs/ai-data-pipeline-error-handling.md

## Key behaviour
- Consumer group: `embedding-worker`; `enable.auto.commit: False`
- Batch: 32 chunks OR 500 ms (whichever comes first)
- Embedding error → DLQ (`dlq-document-chunks`) + do NOT commit offset
- Milvus error → DLQ + do NOT commit offset
- Success → commit offset + publish `EmbeddingEvent` + update status=indexed

## Key dependencies
- Kafka bootstrap: ${KAFKA_BOOTSTRAP}
- Milvus: ${MILVUS_HOST}:${MILVUS_PORT}
- Postgres: ${POSTGRES_DSN}
- Embedding backend: ${EMBEDDING_BACKEND} (local-cpu | openai)

## How to run tests
```
pytest tests/unit/embedder/ -x --tb=short -q
```

## Known constraints
- `LocalCPUBackend` lazily imports `sentence_transformers`; tests inject a mock model via
  `backend._model` to avoid loading the real 90 MB model.
- `MilvusWriter.connect()` is not called in unit tests; tests pass a mock writer.
