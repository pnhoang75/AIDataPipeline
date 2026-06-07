#!/usr/bin/env python3
"""End-to-end smoke test: upload PDF → poll RAG API for results."""
import sys
import time
import json
import io
import subprocess
import requests
from fpdf import FPDF
from minio import Minio
from minio.error import S3Error

MINIO_ENDPOINT = "localhost:9001"
MINIO_ACCESS = "minio"
MINIO_SECRET = "minio123456"
BUCKET = "documents"
OBJECT_NAME = "smoke-test/test-doc.pdf"

RAG_PORT = 9002
RAG_URL = f"http://localhost:{RAG_PORT}/v1/query"
HEALTH_URL = f"http://localhost:{RAG_PORT}/v1/health"

POLL_INTERVAL = 10
MAX_WAIT_SEC = 300


def create_pdf() -> bytes:
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=12)
    pdf.cell(200, 10, text="AI Data Pipeline Smoke Test Document", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(200, 10, text="This is a test document for the end-to-end smoke test.", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(200, 10, text="The pipeline ingests documents from MinIO via S3 connector.", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(200, 10, text="Documents are processed, chunked, embedded, and stored in Milvus.", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(200, 10, text="The RAG API allows semantic search over the ingested content.", new_x="LMARGIN", new_y="NEXT")
    return pdf.output()


def upload_pdf(pdf_bytes: bytes) -> None:
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
    if not client.bucket_exists(BUCKET):
        print(f"  Creating bucket '{BUCKET}'")
        client.make_bucket(BUCKET)
    else:
        print(f"  Bucket '{BUCKET}' already exists")

    try:
        client.remove_object(BUCKET, OBJECT_NAME)
        print(f"  Removed existing object to reset watermark trigger")
    except S3Error:
        pass

    client.put_object(
        BUCKET,
        OBJECT_NAME,
        io.BytesIO(pdf_bytes),
        length=len(pdf_bytes),
        content_type="application/pdf",
    )
    print(f"  Uploaded {OBJECT_NAME} ({len(pdf_bytes)} bytes) to bucket '{BUCKET}'")


def wait_for_rag_api(timeout: int = 30) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(HEALTH_URL, timeout=5)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def query_rag(query: str) -> dict:
    payload = {"query": query, "top_k": 5}
    r = requests.post(RAG_URL, json=payload, headers={"X-Tenant-ID": "default"}, timeout=30)
    r.raise_for_status()
    return r.json()


def poll_until_results(query: str, max_wait: int = MAX_WAIT_SEC):
    print(f"  Polling RAG API every {POLL_INTERVAL}s (max {max_wait}s) ...")
    deadline = time.time() + max_wait
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            result = query_rag(query)
            results = result.get("results", [])
            print(f"  Attempt {attempt}: got {len(results)} result(s)")
            if results:
                return result
        except Exception as e:
            print(f"  Attempt {attempt}: query error: {e}")
        time.sleep(POLL_INTERVAL)
    return None


def main():
    print("=== Session 1-I Smoke Test ===")
    print()

    # Step 1: Create and upload PDF
    print("[1/4] Creating test PDF...")
    pdf_bytes = create_pdf()
    print("[2/4] Uploading PDF to MinIO...")
    upload_pdf(pdf_bytes)
    print()

    # Step 2: Port-forward RAG API
    print("[3/4] Port-forwarding RAG API to localhost:9002...")
    pf = subprocess.Popen(
        ["kubectl", "port-forward", "-n", "ai-pipeline", "svc/rag-api", f"{RAG_PORT}:8000"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(3)

    try:
        if not wait_for_rag_api(timeout=20):
            print("  ERROR: RAG API health check failed")
            sys.exit(1)
        print("  RAG API is healthy")
        print()

        # Step 3: Poll for results
        print("[4/4] Waiting for pipeline to process document and index it...")
        result = poll_until_results("AI data pipeline document ingestion")

        if result is None:
            print()
            print("SMOKE TEST FAILED: No results returned within timeout.")
            print("The pipeline may still be processing — check pod logs.")
            sys.exit(1)

        print()
        print("SMOKE TEST PASSED!")
        print(f"Query returned {len(result['results'])} result(s).")
        print("First result snippet:")
        first = result["results"][0]
        text = first.get("text", first.get("chunk_text", str(first)))
        print(f"  score={first.get('score', 'N/A'):.4f}  text={text[:120]!r}")
        return result

    finally:
        pf.terminate()
        pf.wait()


if __name__ == "__main__":
    main()
