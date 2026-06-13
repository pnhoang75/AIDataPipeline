FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY operator/ ./operator/
COPY main.py .

# Run as a non-root user
USER 65534

ENTRYPOINT ["kopf", "run", "--all-namespaces", "/app/main.py"]
