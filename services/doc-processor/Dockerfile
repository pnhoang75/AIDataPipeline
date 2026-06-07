FROM python:3.11-slim

RUN adduser --uid 1000 --disabled-password --gecos "" appuser

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Pre-download tiktoken cl100k_base encoding into /app/.tiktoken so it is
# readable by UID 1000 at runtime (read-only rootfs; env var set below).
ENV TIKTOKEN_CACHE_DIR=/app/.tiktoken
RUN python -c "import tiktoken; tiktoken.get_encoding('cl100k_base')" && \
    chmod -R 755 /app/.tiktoken
COPY src/ .

USER 1000
CMD ["python", "main.py"]
