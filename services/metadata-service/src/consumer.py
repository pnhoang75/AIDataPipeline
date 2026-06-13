import json
import logging
from typing import Any, Dict

from config import Config, config as _default_config
from db import (
    insert_lineage_edge,
    insert_quality_checks,
    update_entity_quality_failed,
    upsert_entity,
)
from events import DataQualityFailedPublisher

logger = logging.getLogger(__name__)

_HANDLED_EVENT_TYPE = "metadata.entity.created"


class MetadataConsumer:
    """
    Kafka consumer for the metadata-events topic.
    Processes metadata.entity.created events: upserts entities, inserts lineage
    edges, records quality checks, and publishes DataQualityFailed events on failure.
    """

    def __init__(self, db_conn, producer, cfg: Config = None):
        self._db = db_conn
        self._cfg = cfg or _default_config
        self._dq_publisher = DataQualityFailedPublisher(producer, self._cfg.data_quality_failed_topic)

    def process_event(self, event: Dict[str, Any]) -> None:
        """Process a single decoded CloudEvent dict."""
        if event.get("type") != _HANDLED_EVENT_TYPE:
            logger.debug("ignoring event type: %s", event.get("type"))
            return

        data = event.get("data", {})
        entity_type = data.get("entity_type", "")
        entity_key = data.get("entity_key", "")
        tenant_id = data.get("tenant_id", "")
        attributes = data.get("attributes", {})
        upstream_refs = data.get("upstream", [])
        quality_checks = data.get("quality_checks", [])
        pipeline_run_id = data.get("pipeline_run_id")
        schema_version_id = data.get("schema_version_id")

        entity_id = upsert_entity(
            self._db,
            entity_type=entity_type,
            entity_key=entity_key,
            tenant_id=tenant_id,
            attributes=attributes,
            pipeline_run_id=pipeline_run_id,
            schema_version_id=schema_version_id,
        )

        for ref in upstream_refs:
            try:
                insert_lineage_edge(
                    self._db,
                    upstream_type=ref["entity_type"],
                    upstream_key=ref["entity_key"],
                    tenant_id=tenant_id,
                    downstream_id=entity_id,
                    relationship=ref["relationship"],
                    pipeline_run_id=pipeline_run_id,
                )
            except Exception as exc:
                logger.warning("lineage edge insert failed: %s", exc)

        failed_checks = insert_quality_checks(
            self._db,
            entity_id=entity_id,
            run_id=pipeline_run_id,
            quality_checks=quality_checks,
        )

        if failed_checks:
            try:
                update_entity_quality_failed(self._db, entity_id)
            except Exception as exc:
                logger.warning("quality_status update failed: %s", exc)
            self._dq_publisher.publish(
                entity_id=entity_id,
                entity_type=entity_type,
                entity_key=entity_key,
                tenant_id=tenant_id,
                failed_checks=failed_checks,
            )

        try:
            self._db.commit()
        except Exception as exc:
            logger.warning("commit failed: %s", exc)

    def run(self, kafka_consumer) -> None:
        """Consume loop — blocks until interrupted."""
        kafka_consumer.subscribe([self._cfg.metadata_events_topic])
        try:
            while True:
                msg = kafka_consumer.poll(timeout=self._cfg.poll_timeout_seconds)
                if msg is None:
                    continue
                if msg.error():
                    logger.error("consumer error: %s", msg.error())
                    continue
                try:
                    event = json.loads(msg.value().decode())
                    self.process_event(event)
                except Exception as exc:
                    logger.error("failed to process message: %s", exc)
        except KeyboardInterrupt:
            pass
        finally:
            kafka_consumer.close()
