import json
import time
import uuid
from dataclasses import asdict, dataclass, field


@dataclass
class DocumentChunkEvent:
    doc_id: str
    chunk_id: str
    chunk_index: int
    total_chunks: int
    text: str
    source_type: str
    source_id: str
    content_type: str
    tenant_id: str
    metadata: dict = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str) -> "DocumentChunkEvent":
        return cls(**json.loads(data))

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class EmbeddingEvent:
    doc_id: str
    source_id: str
    source_type: str
    tenant_id: str
    chunk_count: int
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    embedded_at: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str) -> "EmbeddingEvent":
        return cls(**json.loads(data))

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DLQEnvelope:
    original_topic: str
    original_partition: int
    original_offset: int
    original_timestamp: int
    failure_reason: str
    failure_detail: str
    original_payload: dict
    dlq_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    failure_count: int = 1
    failed_at: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    def to_dict(self) -> dict:
        return asdict(self)
