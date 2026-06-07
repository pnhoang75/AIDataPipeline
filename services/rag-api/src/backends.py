from typing import List


class LocalCPUBackend:
    """BAAI/bge-small-en-v1.5 on CPU (384-dim). Lazy-loads model on first call."""

    def __init__(self, model_name: str = "BAAI/bge-small-en-v1.5"):
        self._model_name = model_name
        self._model = None

    def _load(self) -> None:
        if self._model is None:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self._model_name)

    def embed(self, text: str) -> List[float]:
        self._load()
        vec = self._model.encode([text], normalize_embeddings=True)
        return vec[0].tolist()
