import logging
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    chunk_id: str
    text: str
    score: float
    source_type: str
    doc_id: str
    metadata: dict = field(default_factory=dict)


class MilvusSearcher:
    """ANN search over a Milvus collection using IVF_FLAT / L2."""

    def __init__(self, host: str, port: int, dim: int = 384):
        self._host = host
        self._port = port
        self._dim = dim

    def connect(self) -> None:
        from pymilvus import connections
        connections.connect(alias="default", host=self._host, port=str(self._port))
        logger.info("Milvus connected at %s:%s", self._host, self._port)

    def search(
        self,
        collection: str,
        vector: List[float],
        top_k: int,
        source_filter: Optional[str] = None,
    ) -> List[SearchResult]:
        from pymilvus import Collection, utility

        if not utility.has_collection(collection):
            logger.warning("Collection %s not found; returning empty results", collection)
            return []

        col = Collection(collection)
        col.load()

        expr = None
        if source_filter:
            expr = f'source_type == "{source_filter}"'

        results = col.search(
            data=[vector],
            anns_field="embedding",
            param={"metric_type": "L2", "params": {"nprobe": 16}},
            limit=top_k,
            output_fields=["chunk_id", "text", "source_type", "doc_id", "metadata"],
            expr=expr,
        )

        hits = []
        for hit in results[0]:
            hits.append(
                SearchResult(
                    chunk_id=hit.entity.get("chunk_id", ""),
                    text=hit.entity.get("text", ""),
                    score=hit.distance,
                    source_type=hit.entity.get("source_type", ""),
                    doc_id=hit.entity.get("doc_id", ""),
                    metadata=hit.entity.get("metadata") or {},
                )
            )
        return hits

    def close(self) -> None:
        try:
            from pymilvus import connections
            connections.disconnect("default")
        except Exception:
            pass
