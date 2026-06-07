# connector-s3

Polls MinIO/S3 for new/modified objects and publishes `RawDocumentEvent` messages to the Kafka `raw-documents` topic.

- Watermark stored in Redis HSET `connector:{connector_id}:watermark` → `last_seen` ISO timestamp
- Retry policy: exponential backoff 100ms × 2^n, cap 30s, max 5 attempts; then skip file and emit counter
- Watermark never advanced on publish failure — guarantees re-delivery on next poll
- `source_file_status` row written before Kafka publish (status=pending) or on skip (status=error)
- Run tests: `pytest tests/test_s3_connector.py -x --tb=short -q`
