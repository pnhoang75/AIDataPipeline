"""PostgreSQL async client for workspace/sources/files operations.

asyncpg is imported lazily so unit tests can mock these functions without
needing a database connection available.
"""
import uuid
from typing import Dict, List, Optional

_pool = None


async def _get_pool():
    global _pool
    if _pool is None:
        from config import config as _default_config
        import asyncpg
        _pool = await asyncpg.create_pool(_default_config.database_url)
    return _pool


async def get_workspaces(tenant_id: str) -> List[Dict]:
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, tenant_id, owner_id, name, description, created_at "
            "FROM workspaces WHERE tenant_id = $1 ORDER BY created_at DESC",
            uuid.UUID(tenant_id),
        )
    return [dict(r) for r in rows]


async def create_workspace(
    tenant_id: str,
    owner_id: str,
    name: str,
    description: Optional[str] = None,
) -> Dict:
    pool = await _get_pool()
    workspace_id = str(uuid.uuid4())
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO workspaces (id, tenant_id, owner_id, name, description) "
            "VALUES ($1, $2, $3, $4, $5) RETURNING *",
            uuid.UUID(workspace_id),
            uuid.UUID(tenant_id),
            uuid.UUID(owner_id),
            name,
            description,
        )
    return dict(row)


async def get_workspace(workspace_id: str, tenant_id: str) -> Optional[Dict]:
    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, tenant_id, owner_id, name, description, created_at "
            "FROM workspaces WHERE id = $1 AND tenant_id = $2",
            uuid.UUID(workspace_id),
            uuid.UUID(tenant_id),
        )
    return dict(row) if row else None


async def delete_workspace(workspace_id: str, tenant_id: str) -> bool:
    pool = await _get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM workspaces WHERE id = $1 AND tenant_id = $2",
            uuid.UUID(workspace_id),
            uuid.UUID(tenant_id),
        )
    return result != "DELETE 0"


async def get_workspace_sources(workspace_id: str) -> List[Dict]:
    pool = await _get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, workspace_id, connector_id, path_prefix, added_at "
            "FROM workspace_sources WHERE workspace_id = $1",
            uuid.UUID(workspace_id),
        )
    return [dict(r) for r in rows]


async def add_workspace_source(
    workspace_id: str, connector_id: str, path_prefix: str
) -> Dict:
    pool = await _get_pool()
    source_id = str(uuid.uuid4())
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO workspace_sources (id, workspace_id, connector_id, path_prefix) "
            "VALUES ($1, $2, $3, $4) RETURNING *",
            uuid.UUID(source_id),
            uuid.UUID(workspace_id),
            connector_id,
            path_prefix,
        )
    return dict(row)


async def delete_workspace_source(workspace_id: str, source_id: str) -> bool:
    pool = await _get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM workspace_sources WHERE id = $1 AND workspace_id = $2",
            uuid.UUID(source_id),
            uuid.UUID(workspace_id),
        )
    return result != "DELETE 0"


async def get_workspace_files(
    workspace_id: str,
    tenant_id: str,
    page: int = 1,
    per_page: int = 50,
) -> List[Dict]:
    pool = await _get_pool()
    offset = (page - 1) * per_page
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT sfs.id, sfs.connector_id, sfs.file_path, sfs.file_size_bytes, "
            "sfs.content_type, sfs.last_modified, sfs.ingest_status, sfs.error_message, "
            "sfs.chunk_count, sfs.indexed_at "
            "FROM source_file_status sfs "
            "JOIN workspace_sources ws ON ws.connector_id = sfs.connector_id "
            "WHERE ws.workspace_id = $1 AND sfs.tenant_id = $2 "
            "ORDER BY sfs.last_modified DESC NULLS LAST "
            "LIMIT $3 OFFSET $4",
            uuid.UUID(workspace_id),
            uuid.UUID(tenant_id),
            per_page,
            offset,
        )
    return [dict(r) for r in rows]
