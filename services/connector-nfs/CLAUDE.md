# connector-nfs

Watches an NFS share (mounted as a K8s PersistentVolume) for new/modified files and publishes `RawDocumentEvent` messages to the Kafka `raw-documents` topic.

## Relevant design docs
- docs/ai-data-pipeline-design.md §2 (NFS Connector)

## Key behaviour
- Primary: `watchdog`/inotify via `Observer` — real-time file creation/modification events queued in-process
- Fallback: periodic tree-diff against Redis set `connector:{id}:known_files` when observer is not alive
- Extension allowlist from `ALLOWED_EXTENSIONS` env var (comma-separated, e.g. `.pdf,.txt`)
- File added to `known_files` set **only after** successful Kafka publish — guarantees re-delivery on failure
- `source_file_status` row written before Kafka publish (status=pending)

## Key dependencies
- Kafka bootstrap: ${KAFKA_BOOTSTRAP}
- Redis: ${REDIS_URL}
- NFS mount: ${NFS_MOUNT_PATH} (default /mnt/nfs)

## How to run locally
```
NFS_MOUNT_PATH=/tmp/nfs-test pytest tests/unit/connectors/test_nfs_connector.py -x --tb=short -q
```

## Known constraints
- Observer is started in `__init__`; call `stop()` in teardown to avoid thread leaks
- `_make_event` silently returns None for non-files (dirs, symlinks) and disallowed extensions
