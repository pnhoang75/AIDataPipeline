"""Locust load test — RAG API (§6.1 target: p99 < 500 ms at 100 QPS).

Run in terminal only — do NOT import into pytest:
    locust -f tests/performance/locustfile.py --headless -u 100 -r 10 -t 7m \
        --host http://localhost:8080 --only-summary

Pass criteria:
    p50 < 150 ms, p95 < 300 ms, p99 < 500 ms
    Error rate < 0.1%
    Redis cache hit rate > 30% (same queries repeated)

Environment variables:
    PERF_RAG_TOKEN   Bearer token for Authorization header
    PERF_TENANT_ID   Tenant ID (default: perf-tenant)
"""

import os
import random

from locust import HttpUser, between, task

TOKEN = os.getenv("PERF_RAG_TOKEN", "perf-test-token")
TENANT_ID = os.getenv("PERF_TENANT_ID", "perf-tenant")

_QUERIES = [
    "What documents are available for tenant configuration?",
    "How do I set up data connectors?",
    "Explain the quota management system",
    "What are the embedding model specifications?",
    "How does the RAG pipeline process documents?",
    "What is the ingestion pipeline workflow?",
    "How are Milvus collections managed per tenant?",
    "What security policies are enforced for connectors?",
    "How does the embedding worker handle batch processing?",
    "What metadata is tracked for each document?",
]


class RAGUser(HttpUser):
    """Simulates a user hitting the RAG API.

    100 users × ~1 RPS each ≈ 100 QPS.
    Queries are drawn from a pool of 10 to achieve >30% Redis cache hit rate.
    """

    wait_time = between(0.5, 1.5)

    @task(9)
    def query(self):
        self.client.post(
            "/v1/query",
            json={"query": random.choice(_QUERIES), "top_k": 5},
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "X-Tenant-ID": TENANT_ID,
            },
        )

    @task(1)
    def health_check(self):
        self.client.get("/healthz", name="/healthz")
