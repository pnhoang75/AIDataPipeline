import uvicorn
import redis as redis_lib
import structlog

from logging_config import setup_logging, bind_request_context
from app import app, RagService
from backends import LocalCPUBackend
from circuit_breaker import CircuitBreaker
from config import config
from milvus_searcher import MilvusSearcher

setup_logging("rag-api")
logger = structlog.get_logger(__name__)


@app.on_event("startup")
async def startup() -> None:
    logger.info("RAG API starting up")

    redis_client = redis_lib.Redis(
        host=config.redis_host,
        port=config.redis_port,
        db=0,
        decode_responses=False,
        socket_connect_timeout=0.5,
        socket_timeout=0.2,
    )

    searcher = MilvusSearcher(
        host=config.milvus_host,
        port=config.milvus_port,
        dim=config.embedding_dim,
    )
    try:
        searcher.connect()
    except Exception as exc:
        logger.warning("Milvus connect failed at startup (will retry on query): %s", exc)

    embedder = LocalCPUBackend(model_name=config.embedding_model)

    cb = CircuitBreaker(
        failure_threshold=config.circuit_failure_threshold,
        recovery_timeout=config.circuit_recovery_timeout,
        name="milvus",
    )

    app.state.service = RagService(
        milvus_searcher=searcher,
        redis_client=redis_client,
        embedder=embedder,
        circuit_breaker=cb,
        cfg=config,
    )
    logger.info("RAG API ready")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
