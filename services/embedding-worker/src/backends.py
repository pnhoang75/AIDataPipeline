from typing import List, Protocol, runtime_checkable


class RateLimitError(Exception):
    """Raised by a backend when the upstream API signals rate limiting."""

    def __init__(self, message: str, retry_after: float = 0.0):
        super().__init__(message)
        self.retry_after = retry_after


@runtime_checkable
class EmbeddingBackend(Protocol):
    @property
    def dim(self) -> int: ...

    def embed_batch(self, texts: List[str]) -> List[List[float]]: ...


class LocalCPUBackend:
    """sentence-transformers BAAI/bge-small-en-v1.5 running on CPU (384-dim)."""

    def __init__(self, model_name: str = "BAAI/bge-small-en-v1.5"):
        self._model_name = model_name
        self._model = None
        self._dim = 384

    @property
    def dim(self) -> int:
        return self._dim

    def _load(self) -> None:
        if self._model is None:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self._model_name)

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        self._load()
        vecs = self._model.encode(texts, normalize_embeddings=True)
        return vecs.tolist()


class OpenAIBackend:
    """Stub: returns zero vectors. Requires OPENAI_API_KEY for real use."""

    def __init__(self, model: str = "text-embedding-3-small", dim: int = 1536):
        self._model = model
        self._dim = dim

    @property
    def dim(self) -> int:
        return self._dim

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        return [[0.0] * self._dim for _ in texts]


def make_backend(backend_type: str, cfg) -> EmbeddingBackend:
    if backend_type == "local-cpu":
        return LocalCPUBackend(model_name=cfg.embedding_model)
    elif backend_type == "openai":
        return OpenAIBackend(model=cfg.openai_embedding_model, dim=cfg.openai_embedding_dim)
    raise ValueError(f"Unknown embedding backend: {backend_type!r}")
