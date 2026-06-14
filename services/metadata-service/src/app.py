import logging
import threading
from typing import Any, Dict, Generator, List, Optional

import psycopg2
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from config import config
from db import (
    create_schema_version,
    query_downstream,
    query_provenance,
    query_quality,
    query_runs,
    query_stale,
    query_upstream,
)

logger = logging.getLogger(__name__)

app = FastAPI(title="Metadata Service", version="1.0.0")
_consumer_thread: threading.Thread = None


def _get_db() -> Generator:
    if not config.database_url:
        raise HTTPException(status_code=503, detail="database not configured")
    conn = psycopg2.connect(config.database_url)
    try:
        yield conn
    finally:
        conn.close()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/lineage/upstream/{chunk_id}")
def lineage_upstream(chunk_id: str, db=Depends(_get_db)):
    return query_upstream(db, chunk_id)


@app.get("/api/lineage/downstream/{source_path:path}")
def lineage_downstream(source_path: str, tenant_id: str, db=Depends(_get_db)):
    return query_downstream(db, tenant_id, source_path)


@app.get("/api/lineage/stale/{tenant_id}")
def lineage_stale(tenant_id: str, db=Depends(_get_db)):
    return query_stale(db, tenant_id)


@app.get("/api/lineage/provenance/{query_id}")
def lineage_provenance(query_id: str, db=Depends(_get_db)):
    return query_provenance(db, query_id)


@app.get("/api/runs")
def runs(tenant_id: Optional[str] = None, db=Depends(_get_db)):
    return query_runs(db, tenant_id)


@app.get("/api/quality/{tenant_id}")
def quality(tenant_id: str, db=Depends(_get_db)):
    return query_quality(db, tenant_id)


class SchemaVersionRequest(BaseModel):
    tenant_id: str
    embedding_model: str
    embedding_dimension: int
    embedding_backend: str
    chunk_size: int = 512
    chunk_overlap: int = 64
    chunking_strategy: str = "fixed"
    index_type: str = "IVF_FLAT"
    created_by: Optional[str] = None


@app.post("/api/schema-versions", status_code=201)
def create_schema_version_endpoint(
    body: SchemaVersionRequest, db=Depends(_get_db)
) -> Dict[str, Any]:
    """Create a new SchemaVersion for a tenant, deactivating previous versions."""
    return create_schema_version(
        db,
        tenant_id=body.tenant_id,
        embedding_model=body.embedding_model,
        embedding_dimension=body.embedding_dimension,
        embedding_backend=body.embedding_backend,
        chunk_size=body.chunk_size,
        chunk_overlap=body.chunk_overlap,
        chunking_strategy=body.chunking_strategy,
        index_type=body.index_type,
        created_by=body.created_by,
    )


@app.on_event("startup")
async def startup_event():
    if not config.kafka_bootstrap or not config.database_url:
        logger.info("KAFKA_BOOTSTRAP or DATABASE_URL not set; skipping consumer startup")
        return
    try:
        from confluent_kafka import Consumer, Producer

        db_conn = psycopg2.connect(config.database_url)

        producer = Producer({"bootstrap.servers": config.kafka_bootstrap})
        kafka_consumer = Consumer(
            {
                "bootstrap.servers": config.kafka_bootstrap,
                "group.id": config.kafka_consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        from consumer import MetadataConsumer

        svc = MetadataConsumer(db_conn=db_conn, producer=producer, cfg=config)

        global _consumer_thread
        _consumer_thread = threading.Thread(
            target=svc.run,
            args=(kafka_consumer,),
            daemon=True,
            name="metadata-consumer",
        )
        _consumer_thread.start()
        logger.info("metadata consumer thread started")
    except Exception as exc:
        logger.error("failed to start consumer: %s", exc)
