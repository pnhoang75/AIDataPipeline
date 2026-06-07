import json
import uuid
import time
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class RawDocumentEvent:
    source_type: str
    source_id: str
    content_ref: str
    content_type: str
    tenant_id: str
    metadata: dict = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    ingested_at: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str) -> "RawDocumentEvent":
        return cls(**json.loads(data))

    def to_dict(self) -> dict:
        return asdict(self)
