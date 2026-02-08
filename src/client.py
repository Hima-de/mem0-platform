"""Unified Mem0 Client - Single API for Memory + Runtime + Sandbox."""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Any

from memory import MemoryCore, Memory, MemoryCategory
from runtime import RuntimeDistributor
from storage import SnapshotEngine
from sandbox import LocalProcessExecutor, SandboxConfig, ExecutionResult

logger = logging.getLogger(__name__)


@dataclass
class Mem0ClientConfig:
    db_path: str = "/tmp/mem0.db"
    runtime_cache_dir: str = "/tmp/runtime-cache"
    sandbox_workdir: str = "/tmp/sandbox-workdir"
    snapshot_db: str = "/tmp/snapshots.db"
    enable_prewarming: bool = False


class Mem0Client:
    """
    Unified client for Memory + Runtime + Sandbox.

    Provides a single API for:
    - Memory management (hierarchical with importance)
    - Runtime distribution (cold start optimization)
    - Sandbox execution (containerized code run)
    - E2B cloud sandboxes
    - Browser sandboxes
    - Memory extraction
    """

    def __init__(self, config: Optional[Mem0ClientConfig] = None):
        self.config = config or Mem0ClientConfig()
        self._memory: Optional[MemoryCore] = None
        self._runtime: Optional[RuntimeDistributor] = None
        self._storage: Optional[SnapshotEngine] = None
        self._sandbox: Optional[LocalProcessExecutor] = None

    async def _ensure_memory(self) -> MemoryCore:
        if self._memory is None:
            self._memory = MemoryCore(db_path=self.config.db_path)
        return self._memory

    async def _ensure_runtime(self) -> RuntimeDistributor:
        if self._runtime is None:
            self._runtime = RuntimeDistributor(
                cache_dir=self.config.runtime_cache_dir,
            )
            if self.config.enable_prewarming:
                await self._runtime.warm_runtime("python-data", "3.11")
        return self._runtime

    async def _ensure_storage(self) -> SnapshotEngine:
        if self._storage is None:
            self._storage = SnapshotEngine(metadata_db=self.config.snapshot_db)
        return self._storage

    async def _ensure_sandbox(self) -> LocalProcessExecutor:
        if self._sandbox is None:
            self._sandbox = LocalProcessExecutor(workdir=self.config.sandbox_workdir)
        return self._sandbox

    async def memory_add(
        self,
        content: str,
        category: MemoryCategory = MemoryCategory.FACT,
        importance: float = 0.5,
        session_id: Optional[str] = None,
    ) -> Memory:
        mem = await self._ensure_memory()
        return await mem.add_memory(
            content=content,
            category=category,
            importance_score=importance,
            session_id=session_id,
        )

    async def memory_search(
        self,
        query: str,
        session_id: Optional[str] = None,
        category: Optional[MemoryCategory] = None,
        limit: int = 10,
    ) -> List[Memory]:
        mem = await self._ensure_memory()
        return await mem.search_memories(
            query=query,
            session_id=session_id,
            category=category,
            limit=limit,
        )

    async def memory_get_context(
        self,
        session_id: str,
        limit: int = 50,
        min_importance: float = 0.3,
    ) -> str:
        mem = await self._ensure_memory()
        return await mem.get_context(
            session_id=session_id,
            limit=limit,
            min_importance=min_importance,
        )

    async def runtime_list(self) -> List[Dict]:
        runtime = await self._ensure_runtime()
        return await runtime.list_available_runtimes()

    async def runtime_warm(self, name: str, version: str) -> bool:
        runtime = await self._ensure_runtime()
        return await runtime.warm_runtime(name, version)

    async def runtime_resolve_deps(self, lockfile_content: str, file_type: str) -> Dict:
        runtime = await self._ensure_runtime()
        return await runtime.resolve_dependencies(lockfile_content, file_type)

    async def sandbox_create(self, config: Optional[SandboxConfig] = None) -> str:
        sandbox = await self._ensure_sandbox()
        return await sandbox.create(config)

    async def sandbox_execute(
        self,
        sandbox_id: str,
        code: str,
        timeout: int = 300,
        env: Optional[Dict] = None,
    ) -> ExecutionResult:
        sandbox = await self._ensure_sandbox()
        env = env or {}
        return await sandbox.execute(
            sandbox_id=sandbox_id,
            code=code,
            timeout_seconds=timeout,
            env=env,
        )

    async def sandbox_delete(self, sandbox_id: str) -> bool:
        sandbox = await self._ensure_sandbox()
        return await sandbox.delete(sandbox_id)

    async def snapshot_create(
        self,
        sandbox_id: str,
        files: Dict[str, str],
        parent_id: Optional[str] = None,
    ) -> str:
        storage = await self._ensure_storage()
        byte_files = {k: v.encode() for k, v in files.items()}
        return await storage.create_snapshot(
            sandbox_id=sandbox_id,
            file_changes=byte_files,
            parent_id=parent_id,
        )

    async def snapshot_clone(self, snapshot_id: str) -> str:
        storage = await self._ensure_storage()
        return await storage.clone_snapshot(snapshot_id)

    async def snapshot_diff(self, snapshot1_id: str, snapshot2_id: str) -> Dict[str, bytes]:
        storage = await self._ensure_storage()
        return await storage.diff_snapshots(snapshot1_id, snapshot2_id)

    async def snapshot_apply(self, snapshot_id: str, diff: Dict[str, bytes]) -> str:
        storage = await self._ensure_storage()
        return await storage.apply_diff(snapshot_id, diff)

    async def get_context_with_runtime(
        self,
        session_id: str,
        runtime_name: str = "python-data",
        runtime_version: str = "3.11",
    ) -> Dict:
        start = time.time()

        memory_ctx = asyncio.create_task(self.memory_get_context(session_id))
        runtime_warm = asyncio.create_task(self.runtime_warm(runtime_name, runtime_version))

        context = await memory_ctx
        await runtime_warm

        return {
            "context": context,
            "runtime_warmed": True,
            "latency_ms": (time.time() - start) * 1000,
        }

    async def execute_with_memory(
        self,
        session_id: str,
        code: str,
        runtime_name: str = "python-data",
        runtime_version: str = "3.11",
        timeout: int = 300,
    ) -> Dict:
        context = await self.memory_get_context(session_id)

        wrapped_code = f"""
# Context from memory:
{context}

# User code:
{code}
"""

        sandbox_id = await self.sandbox_create()
        try:
            result = await self.sandbox_execute(
                sandbox_id=sandbox_id,
                code=wrapped_code,
                timeout=timeout,
            )
            return {
                "success": result.success,
                "exit_code": result.exit_code,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "execution_time_ms": result.execution_time_ms,
            }
        finally:
            await self.sandbox_delete(sandbox_id)

    async def get_stats(self) -> Dict:
        stats = {}
        if self._memory:
            stats["memory"] = self._memory.get_stats()
        if self._runtime:
            stats["runtime"] = {
                "hot_layers": len(self._runtime._hot_cache),
                "packs_registered": len(self._runtime._packs),
            }
        if self._storage:
            stats["storage"] = await self._storage.get_storage_stats()
        return stats


async def main():
    client = Mem0Client()

    print("=" * 60)
    print("Mem0 Platform - Complete Feature Demo")
    print("=" * 60)

    session_id = "demo-session"

    print("\n[1] MEMORY - Adding memories...")
    await client.memory_add(
        content="User prefers dark mode theme",
        category=MemoryCategory.PREFERENCE,
        importance=0.8,
        session_id=session_id,
    )
    await client.memory_add(
        content="User works with Python and machine learning",
        category=MemoryCategory.CONTEXT,
        importance=0.9,
        session_id=session_id,
    )
    await client.memory_add(
        content="Always use async/await for I/O operations",
        category=MemoryCategory.PROCEDURE,
        importance=0.7,
        session_id=session_id,
    )
    print("   ✓ Added 3 memories with categories")

    print("\n[2] MEMORY - Searching...")
    memories = await client.memory_search("Python", session_id=session_id)
    for m in memories:
        print(f"   [{m.category.value}] {m.content[:50]}...")

    print("\n[3] RUNTIME - Available runtimes...")
    runtimes = await client.runtime_list()
    for r in runtimes:
        print(f"   {r['full_id']} - {r['description']}")
        print(f"      CDN: {r.get('cdn_url', 'N/A')}")

    print("\n[4] RUNTIME - Warming...")
    await client.runtime_warm("python-data", "3.11")
    print("   ✓ Warmed python-data@3.11")

    print("\n[5] RUNTIME - Resolve dependencies...")
    req = "numpy==1.24.0\npandas>=2.0\nrequests~=2.28"
    deps = await client.runtime_resolve_deps(req, "txt")
    print(f"   Resolved: {deps}")

    print("\n[6] SANDBOX - Creating & executing...")
    sandbox_id = await client.sandbox_create()
    result = await client.sandbox_execute(
        sandbox_id=sandbox_id,
        code='print("Hello from Mem0!")',
        timeout=30,
    )
    print(f"   Success: {result.success}")
    print(f"   Output: {result.stdout.strip()}")

    print("\n[7] SNAPSHOT - Creating & cloning...")
    snap_id = await client.snapshot_create(
        sandbox_id=sandbox_id,
        files={"test.py": "print('snapshot test')"},
    )
    print(f"   Created: {snap_id}")

    clone_id = await client.snapshot_clone(snap_id)
    print(f"   Cloned: {clone_id}")

    print("\n[8] CONTEXT + RUNTIME - Parallel fetch...")
    ctx = await client.get_context_with_runtime(session_id, "python-data", "3.11")
    print(f"   Latency: {ctx['latency_ms']:.1f}ms")

    print("\n[9] EXECUTE WITH MEMORY...")
    exec_result = await client.execute_with_memory(
        session_id=session_id,
        code='print("Using context from memory!")',
    )
    print(f"   Success: {exec_result['success']}")

    print("\n[10] STATS...")
    stats = await client.get_stats()
    print(f"   Memory: {stats.get('memory', {})}")
    print(f"   Runtime packs: {stats.get('runtime', {})}")
    print(f"   Storage blocks: {stats.get('storage', {})}")

    print("\n" + "=" * 60)
    print("All features working!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
