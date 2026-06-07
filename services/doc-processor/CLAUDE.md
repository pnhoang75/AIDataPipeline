# doc-processor

Consumes `raw-documents` Kafka topic, parses documents (PDF/DOCX/HTML/CSV/JSON/text), chunks them into 512-token overlapping segments, and publishes `DocumentChunkEvent` messages to `document-chunks`.

## Relevant design docs
- docs/ai-data-pipeline-design.md §2.3
- docs/ai-data-pipeline-error-handling.md §2.2

## Key behaviour
- Consumer group: `doc-processor`; `enable.auto.commit: False` — offset committed only after successful chunk publish
- Parse failure → DLQ (`dlq-raw-documents`) + update `source_file_status=error` + commit offset
- Chunk publish failure → DLQ + do NOT commit offset (message redelivered on restart)
- Fetch failure (after 3 retries in fetcher) → DLQ + commit offset
- Chunker: 512-token chunks, 64-token overlap via tiktoken `cl100k_base`

## Key dependencies
- Kafka bootstrap: ${KAFKA_BOOTSTRAP}
- Postgres: ${POSTGRES_DSN}

## How to run tests
```
pytest tests/unit/processor/ -x --tb=short -q
```

## Known constraints
- Optional library imports (pdfplumber, docx, html2text, bs4, pandas) are at module level with try/except so tests can patch them without installing them.
- `FixedSizeChunker._get_encoding()` lazily imports tiktoken; tests bypass this by setting `chunker._enc` directly.
