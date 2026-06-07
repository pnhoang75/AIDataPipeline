FROM python:3.11-slim

RUN adduser --uid 1000 --disabled-password --gecos "" appuser

WORKDIR /app
COPY requirements.txt .
# Install CPU-only torch first to avoid pulling CUDA variant via sentence-transformers
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ .

USER 1000
ENV SENTENCE_TRANSFORMERS_HOME=/tmp/st-models
EXPOSE 8000
CMD ["python", "main.py"]
