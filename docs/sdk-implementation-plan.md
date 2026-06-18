# AI Data Pipeline — SDK & MCP Implementation Plan

**Version:** 1.0
**Date:** 2026-06-18
**Scope:** Python SDK, MCP Server, TypeScript SDK — integration surfaces for the AI Data Pipeline

---

## Overview

Three integration surfaces are built across Phases 7–10:

1. **Phase 7 — Python SDK** (`sdk/python/`) — typed client with LangChain, LlamaIndex, and Haystack adapters
2. **Phase 8 — MCP Server** (`services/mcp-server/`) — Model Context Protocol server exposing pipeline tools and resources to any MCP-compatible AI client (Claude Desktop, Cursor, Zed, etc.)
3. **Phase 9 — TypeScript SDK** (`sdk-ts/`) — typed npm package with Vercel AI SDK and React hooks adapters
4. **Phase 10 — Integration & CI** — integration tests and GitHub Actions CI for all packages

---

## Repository layout added by this plan

```
sdk/
  python/
    ai_pipeline_sdk/
      __init__.py
      client.py          # BaseClient — httpx, auth, retry
      rag.py             # RagClient: search(), ingest()
      pipeline.py        # PipelineClient: connectors, pipeline runs
      metadata.py        # MetadataClient: lineage queries
      models.py          # Pydantic v2 response models
      adapters/
        __init__.py
        langchain.py     # LangChain VectorStore + Retriever
        llamaindex.py    # LlamaIndex VectorStoreIndex
        haystack.py      # Haystack DocumentStore
    pyproject.toml
    README.md
    dist/                # built by session 7-H
sdk-ts/
  src/
    generated/           # openapi-typescript output (do not hand-edit)
    client.ts            # base fetch client, auth, retry
    rag.ts               # RagClient
    pipeline.ts          # PipelineClient
    adapters/
      ai-sdk.ts          # Vercel AI SDK: createRetriever, createEmbeddingModel
      react.ts           # React hooks: usePipelineSearch, useIngest, usePipelineStatus
    index.ts             # barrel export
  tests/
    rag.test.ts
    ai-sdk.test.ts
    hooks.test.ts
  package.json
  tsconfig.json
  README.md
services/
  mcp-server/
    src/
      server.py          # FastMCP app
      config.py          # env var config
      auth.py            # API key → tenant_id mapping
      tools/
        search.py        # search_documents, list_sources
        ingest.py        # ingest_document
        pipeline.py      # get_pipeline_status, trigger_ingestion, get_connector_health
      resources/
        status.py        # pipeline://status
        lineage.py       # pipeline://lineage/{doc_id}
    requirements.txt
    Dockerfile
    CLAUDE.md
k8s/pipeline/
  mcp-server.yaml        # Deployment + Service + ConfigMap
tests/
  unit/
    sdk/                 # pytest — Python SDK unit tests
    mcp-server/          # pytest — MCP server unit tests
  integration/
    sdk/                 # pytest — integration tests (mock server)
docs/
  mcp-usage.md           # claude_desktop_config.json + curl examples
.github/
  workflows/
    sdk-ci.yml           # CI for Python SDK, MCP server, TS SDK
```

---

## Key design decisions

- **Python SDK base client:** `httpx` (sync + async), Pydantic v2 models, retry on 5xx (3 attempts, exponential backoff).
- **MCP server:** `mcp[cli]>=1.0` (FastMCP), `streamable-http` transport for k8s, `stdio` for local Claude Desktop.
- **Auth in MCP:** `X-API-Key` header → tenant_id via `TENANT_KEY_MAP` env var. Falls back to `DEFAULT_TENANT_ID`.
- **TypeScript:** `openapi-typescript` + `openapi-fetch` for type-safe generated client. Dual ESM/CJS output. React hooks are optional, gated behind `react` peer dep.
- **Adapters are optional deps:** `pip install ai-pipeline-sdk[langchain]`, `pip install ai-pipeline-sdk[llamaindex]`, `pip install ai-pipeline-sdk[haystack]`, `pip install ai-pipeline-sdk[all]`.

---

## Phase 7 — Python SDK

### Design references
- `docs/api/rag-api.openapi.yaml` — RAG search + ingest endpoints
- `docs/api/bff-api.openapi.yaml` — connector + pipeline management endpoints
- `docs/ai-data-pipeline-design.md §2.6` — RAG API component

### Session rules
1. All tests live under `tests/unit/sdk/`.
2. Use `respx` for httpx mocking (do NOT use `unittest.mock.patch` on httpx internals).
3. Tests must not make real network calls.
4. Run `pytest tests/unit/sdk/ -x --tb=short -q` after each session — never the full suite.
5. Commit after every passing run.

### Sessions — Phase 7

| Session | Name | Prompt to Claude |
|---|---|---|
| 7-A | Python SDK scaffold + base client | Create `sdk/python/` directory. Create `sdk/python/pyproject.toml` (name=`ai-pipeline-sdk`, version=`0.1.0`, Python ≥ 3.10, build system = hatchling). Create `sdk/python/ai_pipeline_sdk/__init__.py` exporting `BaseClient`, `RagClient`, `PipelineClient`. Create `sdk/python/ai_pipeline_sdk/client.py`: class `BaseClient(base_url: str, api_key: str, tenant_id: str, timeout: float = 30.0)` using `httpx.Client` and `httpx.AsyncClient`; retry up to 3 times on 5xx with exponential backoff (0.5s, 1s, 2s); inject `Authorization: Bearer {api_key}` and `X-Tenant-ID: {tenant_id}` headers on every request; raise custom `PipelineAPIError(status_code, message)` on 4xx/5xx after retries. Create `sdk/python/ai_pipeline_sdk/models.py` with Pydantic v2 models: `SearchResult(chunk_id: str, doc_id: str, text: str, score: float, metadata: dict)`, `IngestResponse(job_id: str, status: str)`, `ConnectorStatus(id: str, name: str, type: str, status: str, last_poll: datetime | None)`, `PipelineRun(run_id: str, status: str, created_at: datetime, doc_count: int)`. Create `tests/__init__.py`, `tests/unit/__init__.py`, `tests/unit/sdk/__init__.py`. Create `tests/unit/sdk/test_client.py` testing: auth headers are injected on every request, retry fires 3 times on 503 then raises `PipelineAPIError`, timeout raises `PipelineTimeoutError`. Use `respx` for mocking. Run `pytest tests/unit/sdk/test_client.py -x --tb=short -q`. Fix failures. Commit. |
| 7-B | RagClient — search + ingest | Create `sdk/python/ai_pipeline_sdk/rag.py`: class `RagClient` taking a `BaseClient` instance. Implement `search(query: str, collection: str | None = None, top_k: int = 5, tenant_id: str | None = None) -> list[SearchResult]` — sends `POST /api/search` with body `{query, collection, top_k, tenant_id}`, deserialises response into `list[SearchResult]`. Implement `ingest(content: str, filename: str, source_type: str = "upload", metadata: dict | None = None) -> IngestResponse` — sends multipart `POST /api/sources/upload` to the BFF URL, returns `IngestResponse`. Expose convenience constructor `RagClient.from_env()` reading `PIPELINE_API_URL`, `PIPELINE_API_KEY`, `PIPELINE_TENANT_ID` from environment. Create `tests/unit/sdk/test_rag_client.py` with `respx` fixtures covering: search returns typed `SearchResult` list, ingest returns `IngestResponse`, empty result list is handled, 404 raises `PipelineAPIError`. Run `pytest tests/unit/sdk/test_rag_client.py -x --tb=short -q`. Commit. |
| 7-C | PipelineClient — connector CRUD + pipeline runs | Create `sdk/python/ai_pipeline_sdk/pipeline.py`: class `PipelineClient` taking a `BaseClient` instance. Implement: `list_connectors() -> list[ConnectorStatus]` (GET `/api/connectors`), `get_connector(connector_id: str) -> ConnectorStatus` (GET `/api/connectors/{id}`), `create_nfs_connector(name: str, mount_path: str, extensions: list[str]) -> ConnectorStatus` (POST `/api/connectors` with `type="nfs"`), `create_s3_connector(name: str, bucket: str, prefix: str = "") -> ConnectorStatus` (POST `/api/connectors` with `type="s3"`), `delete_connector(connector_id: str) -> None` (DELETE `/api/connectors/{id}`), `get_pipeline_runs(limit: int = 20) -> list[PipelineRun]` (GET `/api/pipeline/runs`), `get_pipeline_run(run_id: str) -> PipelineRun`. Create `tests/unit/sdk/test_pipeline_client.py` covering: list returns typed list, create nfs/s3 sets correct type field, delete returns None on 204, 404 on get raises `PipelineAPIError`. Run `pytest tests/unit/sdk/test_pipeline_client.py -x --tb=short -q`. Commit. |
| 7-D | LangChain VectorStore + Retriever adapter | Create `sdk/python/ai_pipeline_sdk/adapters/__init__.py` and `sdk/python/ai_pipeline_sdk/adapters/langchain.py`. Implement `AIPipelineVectorStore(langchain_core.vectorstores.VectorStore)`: `__init__(rag_client: RagClient, collection: str | None = None)`; `similarity_search(query: str, k: int = 4, **kwargs) -> list[langchain_core.documents.Document]` — calls `rag_client.search(query, top_k=k)`, maps each `SearchResult` to a LangChain `Document(page_content=result.text, metadata={doc_id, score, **result.metadata})`; `add_texts(texts: Iterable[str], metadatas: list[dict] | None = None, **kwargs) -> list[str]` — calls `rag_client.ingest()` for each text, returns job IDs; `as_retriever(**kwargs) -> VectorStoreRetriever`. Use `TYPE_CHECKING` guard so `langchain-core` is only imported when available. Add `langchain-core>=0.2.0` under `[project.optional-dependencies] langchain` in pyproject.toml. Create `tests/unit/sdk/test_langchain_adapter.py` — mock `RagClient` with `unittest.mock.MagicMock`, test `similarity_search` returns `Document` list, `add_texts` returns list of strings, `as_retriever` returns a `VectorStoreRetriever`. Run `pytest tests/unit/sdk/test_langchain_adapter.py -x --tb=short -q`. Commit. |
| 7-E | LlamaIndex VectorStoreIndex adapter | Create `sdk/python/ai_pipeline_sdk/adapters/llamaindex.py`. Implement `AIPipelineVectorStore(llama_index.core.vector_stores.types.BasePydanticVectorStore)`: `stores_text = True`; `add(nodes: list[BaseNode]) -> list[str]` — calls `rag_client.ingest()` for each node's text, returns job IDs; `query(query: VectorStoreQuery) -> VectorStoreQueryResult` — calls `rag_client.search(query.query_str, top_k=query.similarity_top_k or 5)`, maps results to `NodeWithScore(node=TextNode(text=r.text, id_=r.chunk_id, metadata=r.metadata), score=r.score)`, returns `VectorStoreQueryResult(nodes=..., ids=..., similarities=...)`. Use `TYPE_CHECKING` guard. Add `llama-index-core>=0.10.0` under `[project.optional-dependencies] llamaindex`. Create `tests/unit/sdk/test_llamaindex_adapter.py` with mocked `RagClient`. Run `pytest tests/unit/sdk/test_llamaindex_adapter.py -x --tb=short -q`. Commit. |
| 7-F | Haystack DocumentStore adapter | Create `sdk/python/ai_pipeline_sdk/adapters/haystack.py`. Implement `AIPipelineDocumentStore(haystack.document_stores.types.DocumentStore)`: `write_documents(documents: list[haystack.Document], policy=None) -> int` — calls `rag_client.ingest()` for each doc, returns count; `filter_documents(filters: dict | None = None) -> list[haystack.Document]` — raises `NotImplementedError` (full filtering not supported); `query_by_embedding(query_embedding: list[float], top_k: int = 10, filters: dict | None = None) -> list[haystack.Document]` — NOTE: since the pipeline uses server-side embedding, accept a query string via `AIPipelineDocumentStore.search(query: str, top_k: int) -> list[Document]` as the primary method, and `query_by_embedding` raises `NotImplementedError` with helpful message; `count_documents() -> int` — returns -1 (unsupported, logs warning). Add `haystack-ai>=2.0.0` under `[project.optional-dependencies] haystack`. Create `tests/unit/sdk/test_haystack_adapter.py`. Run `pytest tests/unit/sdk/test_haystack_adapter.py -x --tb=short -q`. Commit. |
| 7-G | Full Python SDK unit test suite | Run `pytest tests/unit/sdk/ -x --tb=short -q`. Fix any failures. Ensure `from ai_pipeline_sdk import BaseClient, RagClient, PipelineClient` works from the repo root (add `sdk/python` to `pyproject.toml` or use editable install). Update `sdk/python/ai_pipeline_sdk/__init__.py` to export: `BaseClient`, `PipelineAPIError`, `PipelineTimeoutError`, `RagClient`, `PipelineClient`, `SearchResult`, `IngestResponse`, `ConnectorStatus`, `PipelineRun`. Run full suite again, confirm all pass. Commit. |
| 7-H | PyPI packaging — hatchling, extras, wheel build | Complete `sdk/python/pyproject.toml`: add `[build-system]` (hatchling), `[project.optional-dependencies]` with groups `langchain`, `llamaindex`, `haystack`, `all` (union of the three); add classifiers (`Development Status :: 3 - Alpha`, `Programming Language :: Python :: 3`, `Intended Audience :: Developers`); add `[project.urls]` with `Homepage`, `Repository`. Write `sdk/python/README.md` with: install instructions (`pip install ai-pipeline-sdk[langchain]` etc.), quick-start for each adapter (LangChain, LlamaIndex, Haystack, plain Python). Run `pip install build && python3 -m build sdk/python --outdir sdk/python/dist/`. Verify `ls sdk/python/dist/*.whl` succeeds. Commit. |

---

## Phase 8 — MCP Server

### Design references
- `docs/api/rag-api.openapi.yaml`
- `docs/api/bff-api.openapi.yaml`
- `docs/metadata-lineage.md §3` — lineage schema (for lineage resource)
- MCP spec: model context protocol (use `mcp[cli]>=1.0`, FastMCP API)

### Session rules
1. All tests live under `tests/unit/mcp-server/`.
2. Use `respx` to mock outbound httpx calls to rag-api and BFF.
3. Tests must not make real network calls or start the MCP server subprocess.
4. Run `pytest tests/unit/mcp-server/ -x --tb=short -q` after each session.
5. Commit after every passing run.

### Sessions — Phase 8

| Session | Name | Prompt to Claude |
|---|---|---|
| 8-A | MCP server scaffold — FastMCP, config, Dockerfile | Create `services/mcp-server/`. Create `services/mcp-server/requirements.txt`: `mcp[cli]>=1.0.0`, `httpx>=0.27.0`, `pydantic>=2.0.0`, `structlog>=23.2.0`. Create `services/mcp-server/src/config.py`: `Config` dataclass reading from env: `RAG_API_URL` (default `http://rag-api.ai-pipeline.svc:8000`), `BFF_API_URL` (default `http://bff.ai-pipeline.svc:8000`), `METADATA_API_URL` (default `http://metadata-service.ai-pipeline.svc:8000`), `DEFAULT_TENANT_ID` (default `default`), `TENANT_KEY_MAP` (comma-separated `key:tenant` pairs, e.g. `abc123:tenant-a,def456:tenant-b`), `API_KEY` (optional, single key for simple deployments). Create `services/mcp-server/src/server.py`: `from mcp.server.fastmcp import FastMCP; mcp = FastMCP("ai-pipeline-mcp")`. Import and register tools from `tools/` and resources from `resources/` (stubs for now — just `pass` bodies). Add `if __name__ == "__main__": mcp.run()`. Create `services/mcp-server/Dockerfile`: `python:3.11-slim`, COPY requirements.txt + src/, `RUN pip install -r requirements.txt`, `EXPOSE 8080`, `CMD ["python", "-m", "mcp", "run", "src/server.py", "--transport", "streamable-http", "--host", "0.0.0.0", "--port", "8080"]`. Create `services/mcp-server/CLAUDE.md` with service purpose and config env vars. Verify `cd services/mcp-server && pip install -r requirements.txt && python3 -c "from mcp.server.fastmcp import FastMCP; print('ok')"`. Commit. |
| 8-B | search_documents + ingest_document + list_sources | Create `services/mcp-server/src/tools/search.py`. Implement and register with `@mcp.tool()`: `search_documents(query: str, top_k: int = 5, tenant_id: str | None = None) -> list[dict]` — POST `{RAG_API_URL}/api/search` with body `{query, top_k, tenant_id: tenant_id or config.DEFAULT_TENANT_ID}`, returns list of `{doc_id, text, score, metadata}`. `list_sources(tenant_id: str | None = None) -> list[dict]` — GET `{BFF_API_URL}/api/sources`, returns list of source summaries. Create `services/mcp-server/src/tools/ingest.py`. Implement `ingest_document(content: str, filename: str, tenant_id: str | None = None, metadata: dict | None = None) -> dict` — POST multipart to `{BFF_API_URL}/api/sources/upload`, returns `{job_id, status}`. Create `tests/unit/mcp-server/__init__.py` and `tests/unit/mcp-server/test_search_tools.py`. Use `respx` to mock the outbound HTTP calls. Test: `search_documents` returns typed dicts, empty result handled, HTTP 500 from rag-api raises a descriptive error. Run `pytest tests/unit/mcp-server/test_search_tools.py -x --tb=short -q`. Commit. |
| 8-C | Pipeline + lineage tools and URI resources | Create `services/mcp-server/src/tools/pipeline.py`. Implement: `get_pipeline_status() -> dict` — GET `{BFF_API_URL}/api/pipeline/status`; `get_connector_health() -> list[dict]` — GET `{BFF_API_URL}/api/connectors`; `trigger_ingestion(connector_id: str) -> dict` — POST `{BFF_API_URL}/api/connectors/{connector_id}/trigger`. Create `services/mcp-server/src/resources/status.py`: `@mcp.resource("pipeline://status") def pipeline_status() -> str` — returns JSON string of pipeline status. Create `services/mcp-server/src/resources/lineage.py`: `@mcp.resource("pipeline://lineage/{doc_id}") def document_lineage(doc_id: str) -> str` — GET `{METADATA_API_URL}/api/lineage/{doc_id}`, returns JSON string. Create `tests/unit/mcp-server/test_pipeline_tools.py`. Run `pytest tests/unit/mcp-server/test_pipeline_tools.py -x --tb=short -q`. Commit. |
| 8-D | Auth + tenant context | Create `services/mcp-server/src/auth.py`: function `resolve_tenant_id(api_key: str | None, explicit_tenant: str | None) -> str`. Logic: (1) if `explicit_tenant` is provided, use it; (2) else if `api_key` is in `TENANT_KEY_MAP`, return mapped tenant; (3) else if `API_KEY` env var matches `api_key`, return `DEFAULT_TENANT_ID`; (4) else raise `ValueError("No resolvable tenant_id — provide X-API-Key header or explicit tenant_id")`. Update all tools to call `resolve_tenant_id(api_key=ctx.get("api_key"), explicit_tenant=tenant_id)` where `ctx` is passed via FastMCP context. Create `tests/unit/mcp-server/test_auth.py` covering all 4 branches. Run `pytest tests/unit/mcp-server/test_auth.py -x --tb=short -q`. Commit. |
| 8-E | Full MCP server unit test suite | Run `pytest tests/unit/mcp-server/ -x --tb=short -q`. Fix any failures. Verify the server can be imported cleanly: `cd services/mcp-server && python3 -c "import src.server; print('ok')"`. Commit. |
| 8-F | k8s manifests for mcp-server | Create `k8s/pipeline/mcp-server.yaml` with three documents separated by `---`: (1) `ConfigMap` `mcp-server-config` in `ai-pipeline` namespace with `RAG_API_URL`, `BFF_API_URL`, `METADATA_API_URL`; (2) `Deployment` `mcp-server` — 1 replica, image `ai-pipeline/mcp-server:latest`, `imagePullPolicy: Never`, envFrom both the ConfigMap and `pipeline-secrets` Secret, liveness probe `httpGet /health port 8080` initialDelaySeconds 10 periodSeconds 15, resources `requests: {cpu: 50m, memory: 128Mi} limits: {cpu: 500m, memory: 256Mi}`; (3) `Service` `mcp-server` ClusterIP port 8080. Run `kubectl apply --dry-run=client -f k8s/pipeline/mcp-server.yaml`. Fix any YAML errors. Commit. |
| 8-G | MCP usage docs + examples | Create `docs/mcp-usage.md` with sections: (1) **Claude Desktop (local stdio)** — `claude_desktop_config.json` snippet using `python -m mcp run services/mcp-server/src/server.py --transport stdio` with env vars; (2) **Remote (streamable-http)** — `claude_desktop_config.json` snippet pointing to `http://localhost:8080/mcp`; (3) **Available tools** — table of tool name, description, inputs, example; (4) **Available resources** — table of URI pattern, description, example; (5) **curl examples** for `search_documents` and `ingest_document`; (6) **Python mcp client** — 10-line example using `from mcp import ClientSession`. Commit. |

---

## Phase 9 — TypeScript SDK

### Design references
- `docs/api/rag-api.openapi.yaml`
- `docs/api/bff-api.openapi.yaml`

### Session rules
1. All tests live under `sdk-ts/tests/`.
2. Use `msw` (Mock Service Worker) for fetch mocking — do NOT mock `global.fetch` directly.
3. Tests run with `jest` + `ts-jest`.
4. `sdk-ts/` is a standalone npm package, entirely separate from `frontend/`.
5. Run `cd sdk-ts && npm test` after each session — never the frontend tests.
6. Commit after every passing run.

### Sessions — Phase 9

| Session | Name | Prompt to Claude |
|---|---|---|
| 9-A | TypeScript SDK scaffold + openapi-ts codegen | Create `sdk-ts/` directory (not inside `frontend/`). Create `sdk-ts/package.json`: `name: "@ai-pipeline/sdk"`, `version: "0.1.0"`, `private: false`. devDependencies: `typescript@^5`, `openapi-typescript@^7`, `openapi-fetch@^0.12`, `jest@^29`, `ts-jest@^29`, `@types/jest`, `msw@^2`. Dependencies: `openapi-fetch@^0.12`. Create `sdk-ts/tsconfig.json`: strict, `target: "ES2020"`, `module: "NodeNext"`, `moduleResolution: "NodeNext"`, `declaration: true`, `outDir: "dist"`. Create `sdk-ts/jest.config.cjs`: `{ preset: "ts-jest", testEnvironment: "node", roots: ["<rootDir>/tests"] }`. Run `npm install` in `sdk-ts/`. Run `npx openapi-typescript ../../docs/api/rag-api.openapi.yaml -o src/generated/rag-api.d.ts` and `npx openapi-typescript ../../docs/api/bff-api.openapi.yaml -o src/generated/bff-api.d.ts` (fix any schema warnings). Create `sdk-ts/src/index.ts` with a single export stub: `export const VERSION = "0.1.0";`. Run `npx tsc --noEmit` — fix any errors. Commit. |
| 9-B | RagClient + PipelineClient + msw tests | Create `sdk-ts/src/client.ts`: `createRagClient(baseUrl: string, apiKey: string, tenantId: string)` — returns a typed `openapi-fetch` client bound to the rag-api schema; inject `Authorization` and `X-Tenant-ID` headers. `createBffClient(baseUrl, apiKey, tenantId)` — same for bff-api. Create `sdk-ts/src/rag.ts`: class `RagClient` wrapping the generated client with: `search(query: string, options?: {topK?: number, collection?: string}): Promise<SearchResult[]>`, `ingest(content: string, filename: string, options?: {sourceType?: string, metadata?: Record<string,unknown>}): Promise<IngestResponse>`. Create `sdk-ts/src/pipeline.ts`: class `PipelineClient` with `listConnectors(): Promise<ConnectorStatus[]>`, `createConnector(config: ConnectorConfig): Promise<ConnectorStatus>`, `getPipelineRuns(limit?: number): Promise<PipelineRun[]>`. Export all types from `src/index.ts`. Create `sdk-ts/tests/rag.test.ts` using `msw` server to intercept fetch — test search returns typed array, ingest returns `IngestResponse`, 500 throws. Run `cd sdk-ts && npm test -- --testPathPattern=rag`. Commit. |
| 9-C | Vercel AI SDK adapter | Create `sdk-ts/src/adapters/ai-sdk.ts`. Add peer dependency `ai@^4` and `@ai-sdk/provider@^1` to `package.json` `peerDependencies`. Implement: `createPipelineRetriever(client: RagClient, options?: {topK?: number, collection?: string}): Retriever` — returns a Vercel AI SDK `Retriever` whose `retrieve(query)` calls `client.search(query, options)` and maps results to `{id, content, metadata}`; `createPipelineEmbeddingModel(client: RagClient): EmbeddingModel<string>` — wraps `client.search` as an embedding source (note: the pipeline server-side embeds; this model calls search and returns the top-result vector from metadata if available, else throws `NotSupportedError` with guidance to use the embedding endpoint directly). Create `sdk-ts/tests/ai-sdk.test.ts`. Run `cd sdk-ts && npm test -- --testPathPattern=ai-sdk`. Commit. |
| 9-D | React hooks | Create `sdk-ts/src/adapters/react.ts`. Add peer dependency `react@^18` and `@tanstack/react-query@^5`. Implement using React Query: `usePipelineSearch(query: string, options?: SearchOptions): {data: SearchResult[] | undefined, isLoading: boolean, error: Error | null}` — debounced (300ms), skips fetch if query is empty; `useIngest(): {ingest: (content: string, filename: string) => Promise<IngestResponse>, isPending: boolean, reset: () => void}` — uses `useMutation`; `usePipelineStatus(refetchInterval?: number = 30000): {data: PipelineStatus | undefined, isLoading: boolean}` — polls at interval. These hooks require a `PipelineQueryClientProvider` context (wraps React Query `QueryClientProvider`). Export a `PipelineProvider` component. Create `sdk-ts/tests/hooks.test.ts` using `@testing-library/react` + `msw`. Run `cd sdk-ts && npm test -- --testPathPattern=hooks`. Commit. |
| 9-E | Full TS SDK test suite + dual output | Run `cd sdk-ts && npm test`. Fix failures. Add `"build"` script to `package.json`: `tsc -p tsconfig.json`. Add `tsconfig.cjs.json` for CommonJS output (`"module": "CommonJS"`, `"outDir": "dist/cjs"`). Update build script: `tsc -p tsconfig.json && tsc -p tsconfig.cjs.json`. Add `exports` map to `package.json`: `{ ".": { "import": "./dist/index.js", "require": "./dist/cjs/index.js" }, "./adapters/ai-sdk": {...}, "./adapters/react": {...} }`. Run `npm run build`. Run `npm test` again. Commit. |
| 9-F | npm packaging — README + pack dry-run | Write `sdk-ts/README.md`: install (`npm install @ai-pipeline/sdk`), quick-start for plain TypeScript client, Vercel AI SDK adapter, React hooks, and Node.js usage. Complete `package.json`: `"files": ["dist", "README.md"]`, `"sideEffects": false`, `"keywords"`, `"repository"`, `"license": "MIT"`. Run `cd sdk-ts && npm pack --dry-run` and verify output lists dist files. Commit. |

---

## Phase 10 — Integration & CI

### Session rules
1. Integration tests use `pytest-httpserver` (Python) and `msw` (TS) — no live cluster required.
2. Mark Python integration tests with `@pytest.mark.integration`.
3. GitHub Actions uses ubuntu-latest with Python 3.11 and Node 20.

### Sessions — Phase 10

| Session | Name | Prompt to Claude |
|---|---|---|
| 10-A | Integration tests — Python SDK + MCP (mock server) | Create `tests/integration/sdk/__init__.py`. Create `tests/integration/sdk/test_rag_integration.py`: use `pytest-httpserver` to start a fake rag-api and BFF. Instantiate a real `RagClient` pointing at the fake server. Test: `search()` round-trip returns populated `SearchResult` list; `ingest()` posts multipart and returns `IngestResponse`; `LangChain AIPipelineVectorStore.similarity_search()` returns `Document` list. Create `tests/integration/sdk/test_mcp_integration.py`: start the MCP server subprocess (`python -m mcp run services/mcp-server/src/server.py --transport stdio`) using `subprocess.Popen`; send a JSON-RPC 2.0 `tools/call` message for `search_documents` over stdin; verify the JSON response contains a `result` with the expected shape; send `SIGTERM` to clean up. Mark all with `@pytest.mark.integration`. Add `pytest-httpserver` to dev deps. Run `pytest tests/integration/sdk/ -x --tb=short -q`. Commit. |
| 10-B | GitHub Actions CI | Create `.github/workflows/sdk-ci.yml` with three jobs triggered on push/PR when files under `sdk/**`, `sdk-ts/**`, `services/mcp-server/**`, or `tests/unit/sdk/**` change: (1) `python-sdk` — `actions/checkout`, `actions/setup-python@v5` (3.11), `pip install pytest respx pydantic httpx build`, `pip install -e sdk/python[all]`, `pytest tests/unit/sdk/ --tb=short -q`, `python -m build sdk/python --outdir sdk/python/dist/`; (2) `mcp-server` — same Python setup, `pip install -r services/mcp-server/requirements.txt`, `pytest tests/unit/mcp-server/ --tb=short -q`; (3) `ts-sdk` — `actions/setup-node@v4` (node 20), `cd sdk-ts && npm ci && npm run build && npm test`. All jobs run on `ubuntu-latest`. Commit. |
| 10-C | SDK README + top-level Integrations section | Verify `sdk/python/README.md` exists and has all adapter quick-start examples. Verify `sdk-ts/README.md` exists and has plain TS, Vercel AI SDK, and React hooks examples. Update the top-level `README.md`: after the "Stack" section, add an "## Integrations" section with: (1) **Python SDK** — pip install snippet + 3-line search example; (2) **MCP Server** — link to `docs/mcp-usage.md` + one-liner Claude Desktop config; (3) **TypeScript SDK** — npm install snippet + 3-line search example. Commit. |
