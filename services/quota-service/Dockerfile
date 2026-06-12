FROM python:3.11-slim

RUN adduser --uid 1000 --disabled-password --gecos "" appuser

WORKDIR /app
COPY services/quota-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/quota-service/src/ .

USER 1000
# gRPC (50051), HTTP REST for Kong plugin (8080), Prometheus metrics (9090)
EXPOSE 50051 8080 9090
CMD ["python", "server.py"]
