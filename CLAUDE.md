# AI Data Pipeline — Claude Instructions

## Project overview
Full-stack AI data pipeline on Kubernetes. Design docs in `docs/`. Implementation plan in `docs/implementation-plan.md`. Test plan in `docs/test-plan.md`.

## When running autonomously (via auto-execute.sh)

1. **Read `docs/execution-progress.json` first** to know which session you are on.
2. **Read only the design docs for the current session's phase** — not the entire docs/ folder.
3. **Run tests scoped to the current service only:**
   ```
   pytest tests/unit/<service>/ -x --tb=short -q
   ```
   Never run `pytest tests/` — it floods context.
4. **Commit after every passing test suite.** Never leave uncommitted work.
5. **Create the session sentinel when done:**
   ```
   mkdir -p .sessions-done && touch .sessions-done/<session-id>
   git add .sessions-done/<session-id> && git commit -m "session <id>: complete"
   ```
6. **Stop after the sentinel is created.** Do not begin the next session.
7. If a test fails after 5 fix attempts, add a `# TODO: fix <error>` comment, commit, and create the sentinel anyway.

## When running autonomously (via sdk-auto-execute.sh)

1. **Read `docs/sdk-execution-progress.json` first** to know which session you are on.
2. **Read only `docs/sdk-implementation-plan.md`** — specifically the phase matching the current session.
3. **Run tests scoped to the current package only:**
   ```
   pytest tests/unit/sdk/ -x --tb=short -q          # Python SDK sessions
   pytest tests/unit/mcp-server/ -x --tb=short -q   # MCP server sessions
   cd sdk-ts && npm test -- --testPathPattern=<name> # TS SDK sessions
   ```
   Never run the full test suite.
4. **Commit after every passing test suite.** Never leave uncommitted work.
5. **Create the session sentinel when done:**
   ```
   mkdir -p .sessions-done && touch .sessions-done/<session-id>
   git add .sessions-done/<session-id> && git commit -m "session <id>: complete"
   ```
6. **Stop after the sentinel is created.** Do not begin the next session.
7. If a test fails after 5 fix attempts, add a `# TODO: fix <error>` comment, commit, and create the sentinel anyway.

## Repository layout (to be built)
```
services/
  connector-s3/       # Phase 1-B
  connector-nfs/      # Phase 1-C
  doc-processor/      # Phase 1-D
  embedding-worker/   # Phase 1-F
  rag-api/            # Phase 1-H
  quota-service/      # Phase 2-D
  bff/                # Phase 3-A
  pipeline-operator/  # Phase 4-A
  metadata-service/   # Phase 5-D
  mcp-server/         # Phase 8 — MCP server
frontend/             # Phase 3-E
sdk/
  python/             # Phase 7 — Python SDK (ai-pipeline-sdk PyPI package)
sdk-ts/               # Phase 9 — TypeScript SDK (@ai-pipeline/sdk npm package)
k8s/
  base/               # namespace, NetworkPolicy manifests
  operators/          # Helm values + CRs for infrastructure operators
  pipeline/           # CRDs, Deployments, ServiceAccounts, etc.
tests/
  unit/
  integration/
  e2e/
  security/
  chaos/
  performance/
scripts/
  auto-execute.sh         # original 52-session executor
  sdk-auto-execute.sh     # SDK/MCP executor (phases 7-10)
  sessions/               # per-session prompt overrides (original)
  sdk-sessions/           # per-session prompt overrides (SDK)
docs/
  sessions.json               # 52-session registry (original)
  execution-progress.json     # original executor state
  sdk-sessions.json           # 24-session registry (SDK/MCP)
  sdk-execution-progress.json # SDK executor state
  implementation-plan.md
  sdk-implementation-plan.md
  test-plan.md
  mcp-usage.md            # MCP client examples (created in session 8-G)
logs/sessions/        # per-session claude output logs (original)
logs/sdk-sessions/    # per-session claude output logs (SDK)
.sessions-done/       # sentinel files created on session completion
reports/              # kube-bench, trivy scan outputs
```

## Key design decisions already made
- Embedding model: BAAI/bge-small-en-v1.5 (384-dim, CPU)
- Kafka: Strimzi KRaft, single broker testbed
- Milvus: standalone testbed → cluster production
- Auth: Keycloak 24+ Organizations, Kong OSS, OPA Gatekeeper
- Quota: custom Python gRPC service + Redis counters
- Operators: kopf (Python), ArgoCD GitOps

## Known issues already fixed in design (implement these fixes, don't reintroduce)
- connector-sa RBAC: NO wildcards in resourceNames — operator creates exact-name Roles
- Upload-watcher CronJob: provisioned by TenantWorkspace operator reconcile
- SSRF in /api/sources/test: block RFC-1918 + loopback before any connection attempt
- NFS browse: validate path against allowed_path_prefix before kubectl exec
- Pro tier connector quota: unlimited (not 4) — matches multitenancy doc
- Upload type: skip CheckQuota(CONNECTOR_COUNT) — no DataConnector CR created
- start_paused field: must exist in UserSourceCreate schema
- Upload metadata events: publish DataSource entity to metadata-events topic
