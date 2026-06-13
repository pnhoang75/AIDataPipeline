import logging
from typing import List

logger = logging.getLogger(__name__)

_ENCODING_NAME = "cl100k_base"


class Chunk:
    def __init__(self, text: str, index: int, doc_id: str, token_count: int = 0):
        self.text = text
        self.index = index
        self.doc_id = doc_id
        self.chunk_id = f"{doc_id}:{index}"
        self.token_count = token_count


class FixedSizeChunker:
    def __init__(self, chunk_size: int = 512, overlap: int = 64):
        self._chunk_size = chunk_size
        self._overlap = overlap
        self._enc = None

    def _get_encoding(self):
        if self._enc is None:
            import tiktoken
            self._enc = tiktoken.get_encoding(_ENCODING_NAME)
        return self._enc

    def chunk(self, text: str, doc_id: str) -> List[Chunk]:
        enc = self._get_encoding()
        tokens = enc.encode(text)
        if not tokens:
            return []

        step = max(1, self._chunk_size - self._overlap)
        chunks = []
        start = 0
        index = 0
        while start < len(tokens):
            end = min(start + self._chunk_size, len(tokens))
            chunk_tokens = tokens[start:end]
            chunk_text = enc.decode(chunk_tokens)
            chunks.append(Chunk(text=chunk_text, index=index, doc_id=doc_id, token_count=len(chunk_tokens)))
            index += 1
            if end == len(tokens):
                break
            start += step
        return chunks
