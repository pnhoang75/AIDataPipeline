from .client import BaseClient, PipelineAPIError, PipelineTimeoutError
from .models import ConnectorStatus, IngestResponse, PipelineRun, SearchResult

# Lazy imports — rag.py and pipeline.py are created in later sessions
try:
    from .rag import RagClient
except ImportError:
    RagClient = None  # type: ignore[assignment,misc]

try:
    from .pipeline import PipelineClient
except ImportError:
    PipelineClient = None  # type: ignore[assignment,misc]

__all__ = [
    "BaseClient",
    "PipelineAPIError",
    "PipelineTimeoutError",
    "RagClient",
    "PipelineClient",
    "SearchResult",
    "IngestResponse",
    "ConnectorStatus",
    "PipelineRun",
]
