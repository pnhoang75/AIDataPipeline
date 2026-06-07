from typing import List, Optional

from pydantic import BaseModel


class QueryRequest(BaseModel):
    query: str
    top_k: int = 5
    source_filter: Optional[str] = None
    collection: Optional[str] = None  # ignored — derived from X-Tenant-ID header
    min_score: float = 0.0


class QueryResult(BaseModel):
    chunk_id: str
    text: str
    score: float
    source_type: str
    doc_id: str
    metadata: dict = {}


class QueryResponse(BaseModel):
    results: List[QueryResult]
    cached: bool = False
    request_id: str = ""
