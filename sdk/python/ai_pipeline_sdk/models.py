from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class SearchResult(BaseModel):
    chunk_id: str
    doc_id: str
    text: str
    score: float
    metadata: dict = {}


class IngestResponse(BaseModel):
    job_id: str
    status: str


class ConnectorStatus(BaseModel):
    id: str
    name: str
    type: str
    status: str
    last_poll: Optional[datetime] = None


class PipelineRun(BaseModel):
    run_id: str
    status: str
    created_at: datetime
    doc_count: int
