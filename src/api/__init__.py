"""FastAPI endpoints for mem0-platform."""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from client import Mem0Client, Mem0ClientConfig
from memory import MemoryCategory
from sandbox import SandboxConfig

logger = logging.getLogger(__name__)

client: Optional[Mem0Client] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client
    client = Mem0Client()
    logger.info("Mem0 Platform API started")
    yield
    logger.info("Mem0 Platform API stopped")


app = FastAPI(title="Mem0 Platform API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AddMemoryRequest(BaseModel):
    content: str
    category: str = "fact"
    importance: float = 0.5
    session_id: Optional[str] = None


class SearchMemoriesRequest(BaseModel):
    query: str
    session_id: Optional[str] = None
    category: Optional[str] = None
    limit: int = 10


class ExecuteCodeRequest(BaseModel):
    code: str
    session_id: Optional[str] = None
    runtime: str = "python-data"
    version: str = "3.11"
    timeout: int = 300


class CreateSnapshotRequest(BaseModel):
    files: Dict[str, str]
    sandbox_id: str = "default"


class CreateSandboxRequest(BaseModel):
    max_memory_mb: int = 512
    timeout_seconds: int = 300
    network_access: bool = True


@app.get("/health")
async def health_check() -> Dict:
    return {"status": "healthy", "version": "1.0.0"}


@app.post("/api/v1/memories")
async def add_memory(request: AddMemoryRequest) -> Dict:
    if not client:
        raise HTTPException(status_code=503, detail="Not initialized")
    try:
        category = MemoryCategory(request.category)
    except ValueError:
        category = MemoryCategory.FACT
    memory = await client.memory_add(
        content=request.content,
        category=category,
        importance=request.importance,
        session_id=request.session_id,
    )
    return memory.to_dict()


@app.post("/api/v1/memories/search")
async def search_memories(request: SearchMemoriesRequest) -> List[Dict]:
    if not client:
        raise HTTPException(status_code=503, detail="Not initialized")
    memories = await client.memory_search(
        query=request.query,
        session_id=request.session_id,
        category=MemoryCategory(request.category) if request.category else None,
        limit=request.limit,
    )
    return [m.to_dict() for m in memories]


@app.post("/api/v1/context")
async def get_context(session_id: str, limit: int = 50) -> Dict:
    if not client:
        raise HTTPException(status_code=503, detail="Not initialized")
    context = await client.memory_get_context(session_id=session_id, limit=limit)
    return {"session_id": session_id, "context": context}


@app.get("/api/v1/runtimes")
async def list_runtimes() -> List[Dict]:
    if not client:
        raise HTTPException(status_code=503, detail="Not initialized")
    return await client.runtime_list()


@app.post("/api/v1/runtimes/{name}/warm")
async def warm_runtime(name: str, version: str = "latest") -> Dict:
    if not client:
        raise HTTPException(status_code=503, detail="Not initialized")
    success = await client.runtime_warm(name, version)
    return {"status": "warmed" if success else "failed"}


@app.post("/api/v1/sandboxes")
async def create_sandbox(request: CreateSandboxRequest) -> Dict:
    if not client:
        raise HTTPException(status_code=503, detail="Not initialized")
    sandbox_id = await client.sandbox_create(
        config=SandboxConfig(
            max_memory_mb=request.max_memory_mb,
            timeout_seconds=request.timeout_seconds,
            network_access=request.network_access,
        )
    )
    return {"sandbox_id": sandbox_id}


@app.delete("/api/v1/sandboxes/{sandbox_id}")
async def delete_sandbox(sandbox_id: str) -> Dict:
    if not client:
        raise HTTPException(status_code=503, detail="Not initialized")
    success = await client.sandbox_delete(sandbox_id)
    return {"status": "deleted" if success else "not_found"}


@app.post("/api/v1/sandboxes/{sandbox_id}/execute")
async def execute_code(sandbox_id: str, request: ExecuteCodeRequest) -> Dict:
    if not client:
        raise HTTPException(status_code=503, detail="Not initialized")
    result = await client.sandbox_execute(
        sandbox_id=sandbox_id,
        code=request.code,
        timeout=request.timeout,
    )
    return {
        "success": result.success,
        "exit_code": result.exit_code,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "execution_time_ms": result.execution_time_ms,
    }


@app.post("/api/v1/snapshots")
async def create_snapshot(request: CreateSnapshotRequest) -> Dict:
    if not client:
        raise HTTPException(status_code=503, detail="Not initialized")
    snapshot_id = await client.snapshot_create(
        sandbox_id=request.sandbox_id,
        files=request.files,
    )
    return {"snapshot_id": snapshot_id}


@app.post("/api/v1/snapshots/{snapshot_id}/clone")
async def clone_snapshot(snapshot_id: str) -> Dict:
    if not client:
        raise HTTPException(status_code=503, detail="Not initialized")
    clone_id = await client.snapshot_clone(snapshot_id)
    return {"snapshot_id": clone_id}


@app.get("/api/v1/stats")
async def get_stats() -> Dict:
    if not client:
        raise HTTPException(status_code=503, detail="Not initialized")
    return client.get_stats()


if __name__ == "__main__":
    import uvicorn

    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=8000)
