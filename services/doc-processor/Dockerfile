FROM python:3.11-slim

RUN adduser --uid 1000 --disabled-password --gecos "" appuser

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Pre-download tiktoken encoding so runtime network access is not needed
RUN python -c "import tiktoken; tiktoken.get_encoding('cl100k_base')"
COPY src/ .

USER 1000
CMD ["python", "main.py"]
