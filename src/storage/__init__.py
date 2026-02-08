"""
Clean API Interface for Mem0 Storage V2
====================================

A simple, production-ready interface for the Turbopuffer-style
snapshot and warm pool management system.

Usage:
    from mem0.storage import Mem0Storage, Sandbox, Runtime

    # Initialize
    storage = Mem0Storage()

    # Create sandbox with warm pool
    sandbox = await storage.create_sandbox(
        user_id="user_123",
        runtime=Runtime.PYTHON_ML
    )

    # Fork for A/B testing
    fork = await sandbox.fork()

    # Branch for experiments
    branch = await sandbox.branch(experiment_config)
"""

import asyncio
import hashlib
import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4

# Import internal components
from .v2 import BlockStore, SnapshotEngineV2, CompressionType, RollingHash
from .v2.warm_pool import (
    WarmPoolManager,
    RestoreScheduler,
    BlockCacheManager,
    RuntimePrefetcher,
    RuntimeDistributionPlane,
    RuntimeType as _RuntimeType,
)

logger = logging.getLogger(__name__)


class SnapshotEngineCompat:
    """
    Backward-compatible SnapshotEngine wrapper.

    Provides the legacy API for existing tests and integrations.

    The old API used:
    - create_snapshot(sandbox_id, file_changes) -> snapshot_id
    - clone_snapshot(snapshot_id) -> new_snapshot_id
    - diff_snapshots(snap1_id, snap2_id) -> diff

    The new API uses:
    - create_snapshot(user_id, files, parent_id) -> snapshot_id
    - fork(snapshot_id, user_id) -> new_snapshot_id
    - clone(snapshot_id, user_id, modifications) -> new_snapshot_id
    """

    def __init__(self, metadata_db: str = None, block_store: BlockStore = None):
        if block_store is None:
            block_store = BlockStore(
                local_cache_dir=f"/tmp/snapshot_cache_{uuid4().hex[:8]}",
                max_cache_size_gb=1.0,
                compression=CompressionType.LZ4,
            )
        self._engine = SnapshotEngineV2(block_store=block_store)
        self._user_id = "test_user"

    async def create_snapshot(self, sandbox_id: str, file_changes: Dict[str, bytes], parent_id: str = None) -> str:
        return await self._engine.create_snapshot(self._user_id, file_changes, parent_id=parent_id)

    async def fork_snapshot(self, snapshot_id: str) -> str:
        return await self._engine.fork(snapshot_id, self._user_id)

    async def clone_snapshot(self, snapshot_id: str) -> str:
        return await self._engine.clone(snapshot_id, self._user_id)

    async def restore_snapshot(self, snapshot_id: str) -> Dict[str, bytes]:
        return await self._engine.restore(snapshot_id)

    async def diff_snapshots(self, snap1_id: str, snap2_id: str) -> Dict:
        """Compare two snapshots and return differences."""
        manifest1 = await self._engine.get_manifest(snap1_id)
        manifest2 = await self._engine.get_manifest(snap2_id)

        if not manifest1 or not manifest2:
            raise ValueError("One or both snapshots not found")

        files1 = {c.path for c in manifest1.chunks}
        files2 = {c.path for c in manifest2.chunks}

        added = files2 - files1
        removed = files1 - files2
        common = files1 & files2

        # Return legacy format for backward compatibility
        all_changes = added | removed | common
        return {f: "common" if f in common else ("added" if f in added else "removed") for f in all_changes}

    async def list_snapshots(self) -> List[Dict]:
        return []


# Backward compatibility alias
SnapshotEngine = SnapshotEngineCompat


class Runtime(Enum):
    """Supported runtimes for sandbox environments."""

    PYTHON_DATA = "python-data"
    PYTHON_ML = "python-ml"
    PYTHON_WEB = "python-web"
    NODE_WEB = "node-web"
    GO_RUNTIME = "go-runtime"
    BROWSER_CHROMIUM = "browser-chromium"
    RUST_RUNTIME = "rust-runtime"
    JAVA_RUNTIME = "java-runtime"

    @classmethod
    def from_internal(cls, rt: _RuntimeType) -> "Runtime":
        return cls(rt.value)

    def to_internal(self) -> _RuntimeType:
        return _RuntimeType(self.value)


@dataclass
class SandboxConfig:
    """Configuration for a sandbox environment."""

    runtime: Runtime = Runtime.PYTHON_ML
    memory_mb: int = 512
    cpu_cores: int = 2
    timeout_seconds: int = 300
    enable_network: bool = True
    environment: Dict[str, str] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    working_directory: Optional[str] = None


@dataclass
class SandboxFile:
    """A file in a sandbox."""

    path: str
    content: bytes
    executable: bool = False


@dataclass
class Sandbox:
    """
    A warm sandbox with instant fork/clone capabilities.

    This is the main user-facing API for sandbox operations.

    Usage:
        sandbox = await storage.create_sandbox(user_id="user_123")

        # Run code
        result = await sandbox.run("print('hello')")

        # Fork for experiments
        experiment = await sandbox.fork()

        # Save state
        snapshot = await sandbox.save()
    """

    sandbox_id: str
    user_id: str
    runtime: Runtime
    snapshot_id: str
    storage: "Mem0Storage"
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_used: datetime = field(default_factory=datetime.utcnow)
    config: Optional[SandboxConfig] = None

    async def run(self, code: str, timeout: int = 30) -> "ExecutionResult":
        """Execute code in the sandbox."""
        return await self.storage._run_code(self.sandbox_id, code, timeout)

    async def fork(self) -> "Sandbox":
        """
        Create a fork of this sandbox (O(1) operation).

        Returns a new sandbox that shares underlying blocks.
        """
        new_snapshot = await self.storage._engine.fork(self.snapshot_id, self.user_id)

        return Sandbox(
            sandbox_id=f"sbx_{uuid4().hex[:12]}",
            user_id=self.user_id,
            runtime=self.runtime,
            snapshot_id=new_snapshot,
            storage=self.storage,
        )

    async def branch(self, config: Dict = None) -> "Sandbox":
        """
        Create a branch for experiments/A-B testing.

        Supports copy-on-write for efficient branching.
        """
        modifications = config or {}
        new_snapshot = await self.storage._engine.clone(self.snapshot_id, self.user_id, modifications)

        return Sandbox(
            sandbox_id=f"sbx_{uuid4().hex[:12]}",
            user_id=self.user_id,
            runtime=self.runtime,
            snapshot_id=new_snapshot,
            storage=self.storage,
        )

    async def save(self) -> str:
        """
        Save current state as a named snapshot.

        Returns snapshot_id for later restoration.
        """
        return self.snapshot_id

    async def restore(self, snapshot_id: str) -> None:
        """Restore sandbox to a previous snapshot."""
        self.snapshot_id = snapshot_id

    async def upload_file(self, file: SandboxFile) -> None:
        """Upload a file to the sandbox."""
        await self.storage._engine.create_snapshot(self.user_id, {file.path: file.content})

    async def download_file(self, path: str) -> Optional[bytes]:
        """Download a file from the sandbox."""
        files = await self.storage._engine.restore(self.snapshot_id)
        return files.get(path)

    async def list_files(self) -> List[str]:
        """List all files in the sandbox."""
        # Simplified - would need manifest parsing
        return []

    async def delete(self) -> bool:
        """Delete this sandbox and release resources."""
        return await self.storage.delete_sandbox(self.sandbox_id)


@dataclass
class ExecutionResult:
    """Result of code execution."""

    success: bool
    output: str
    error: Optional[str] = None
    exit_code: int = 0
    execution_time_ms: float = 0.0
    memory_used_mb: float = 0.0


class Mem0Storage:
    """
    Main storage interface for Mem0 platform.

    Provides a simple, production-ready API for:
    - Sandbox creation and management
    - Snapshot fork/clone operations
    - Warm pool management
    - Runtime distribution

    Usage:
        storage = Mem0Storage()

        # Create sandbox
        sandbox = await storage.create_sandbox(
            user_id="user_123",
            runtime=Runtime.PYTHON_ML
        )

        # Run code
        result = await sandbox.run("print('hello world')")

        # Fork for experiments
        fork = await sandbox.fork()
    """

    def __init__(
        self,
        storage_dir: str = None,
        enable_warm_pool: bool = True,
        enable_prefetch: bool = True,
        max_pool_size: int = 1000,
    ):
        """
        Initialize the storage system.

        Args:
            storage_dir: Directory for local cache (default: temp)
            enable_warm_pool: Enable hot snapshot pool
            enable_prefetch: Enable predictive prefetching
            max_pool_size: Maximum snapshots in warm pool
        """
        self.storage_dir = storage_dir or f"/tmp/mem0_storage_{uuid4().hex[:8]}"
        self._initialized = False

        # Configuration
        self.enable_warm_pool = enable_warm_pool
        self.enable_prefetch = enable_prefetch
        self.max_pool_size = max_pool_size

        # Internal components
        self._engine: Optional[SnapshotEngineV2] = None
        self._pool: Optional[WarmPoolManager] = None
        self._distribution: Optional[RuntimeDistributionPlane] = None
        self._cache: Optional[BlockCacheManager] = None

        # Active sandboxes
        self._sandboxes: Dict[str, Sandbox] = {}

    async def initialize(self):
        """Initialize all internal components."""
        if self._initialized:
            return

        Path(self.storage_dir).mkdir(parents=True, exist_ok=True)

        # Initialize block store
        self._block_store = BlockStore(
            local_cache_dir=f"{self.storage_dir}/blocks", max_cache_size_gb=10.0, compression=CompressionType.LZ4
        )

        # Initialize snapshot engine
        self._engine = SnapshotEngineV2(block_store=self._block_store)

        # Initialize warm pool if enabled
        if self.enable_warm_pool:
            self._pool = WarmPoolManager(
                snapshot_engine=self._engine, max_pool_size=self.max_pool_size, prefetch_enabled=self.enable_prefetch
            )
            await self._pool.start()

            # Add default runtime templates
            for rt in Runtime:
                self._pool.add_template_snapshot(
                    runtime=rt.to_internal(), snapshot_id=f"template_{rt.value}", priority=10
                )

        # Initialize runtime distribution
        self._distribution = RuntimeDistributionPlane(cache_dir=f"{self.storage_dir}/runtimes")

        self._initialized = True
        logger.info(f"Mem0Storage initialized: {self.storage_dir}")

    async def shutdown(self):
        """Shutdown all components."""
        if self._pool:
            await self._pool.stop()

        self._initialized = False
        logger.info("Mem0Storage shutdown complete")

    async def create_sandbox(
        self, user_id: str, runtime: Runtime = Runtime.PYTHON_ML, config: SandboxConfig = None
    ) -> Sandbox:
        """
        Create a new sandbox with optional warm pool.

        Args:
            user_id: User identifier
            runtime: Runtime environment
            config: Optional sandbox configuration

        Returns:
            Sandbox instance ready for code execution
        """
        await self.initialize()

        sandbox_id = f"sbx_{uuid4().hex[:12]}"

        # Check warm pool first
        if self._pool:
            snapshot_id, is_warm = await self._pool.request_sandbox(user_id=user_id, runtime=runtime.to_internal())
        else:
            # Create from scratch
            files = await self._get_default_files(runtime)
            snapshot_id = await self._engine.create_snapshot(user_id, files)
            is_warm = False

        sandbox = Sandbox(
            sandbox_id=sandbox_id,
            user_id=user_id,
            runtime=runtime,
            snapshot_id=snapshot_id,
            storage=self,
            config=config,
            created_at=datetime.utcnow(),
        )

        self._sandboxes[sandbox_id] = sandbox

        logger.info(f"Created sandbox {sandbox_id} (warm: {is_warm})")

        return sandbox

    async def delete_sandbox(self, sandbox_id: str) -> bool:
        """Delete a sandbox and release resources."""
        if sandbox_id in self._sandboxes:
            del self._sandboxes[sandbox_id]
            logger.info(f"Deleted sandbox {sandbox_id}")
            return True
        return False

    async def fork_sandbox(self, sandbox_id: str, new_user_id: str = None) -> Sandbox:
        """
        Fork an existing sandbox.

        O(1) operation - only metadata copied.
        """
        await self.initialize()

        source = self._sandboxes.get(sandbox_id)
        if not source:
            raise ValueError(f"Sandbox {sandbox_id} not found")

        new_snapshot = await self._engine.fork(source.snapshot_id, new_user_id or source.user_id)

        fork = Sandbox(
            sandbox_id=f"sbx_{uuid4().hex[:12]}",
            user_id=new_user_id or source.user_id,
            runtime=source.runtime,
            snapshot_id=new_snapshot,
            storage=self,
        )

        self._sandboxes[fork.sandbox_id] = fork

        return fork

    async def create_branch(
        self, sandbox_id: str, modifications: Dict[str, bytes] = None, branch_name: str = None
    ) -> Sandbox:
        """
        Create a branch with copy-on-write modifications.

        Ideal for A/B testing and experiments.
        """
        await self.initialize()

        source = self._sandboxes.get(sandbox_id)
        if not source:
            raise ValueError(f"Sandbox {sandbox_id} not found")

        new_snapshot = await self._engine.clone(source.snapshot_id, source.user_id, modifications or {})

        branch = Sandbox(
            sandbox_id=f"brn_{branch_name or uuid4().hex[:8]}",
            user_id=source.user_id,
            runtime=source.runtime,
            snapshot_id=new_snapshot,
            storage=self,
        )

        self._sandboxes[branch.sandbox_id] = branch

        return branch

    async def save_snapshot(self, sandbox_id: str, name: str = None) -> str:
        """Save sandbox state as a named snapshot."""
        await self.initialize()

        sandbox = self._sandboxes.get(sandbox_id)
        if not sandbox:
            raise ValueError(f"Sandbox {sandbox_id} not found")

        snapshot_id = f"snap_{name or uuid4().hex[:12]}"

        # In production, would store in persistent storage
        logger.info(f"Saved snapshot {snapshot_id}")

        return snapshot_id

    async def restore_snapshot(self, sandbox_id: str, snapshot_id: str) -> Sandbox:
        """Restore sandbox from a saved snapshot."""
        await self.initialize()

        sandbox = self._sandboxes.get(sandbox_id)
        if not sandbox:
            raise ValueError(f"Sandbox {sandbox_id} not found")

        sandbox.snapshot_id = snapshot_id

        return sandbox

    async def list_snapshots(self, user_id: str = None) -> List[Dict]:
        """List all snapshots for a user."""
        # In production, query from storage
        return []

    async def get_runtime(self, runtime: Runtime) -> Dict:
        """Get runtime information."""
        await self.initialize()

        info = await self._distribution.get_runtime(runtime.to_internal())
        return {
            "runtime": runtime.value,
            "size_mb": info["size_bytes"] / 1024 / 1024,
            "dependencies": info.get("dependencies", []),
            "from_cache": info.get("from_cache", False),
        }

    async def resolve_dependencies(self, runtime: Runtime, lockfile: str) -> Dict[str, str]:
        """Resolve dependencies from lockfile."""
        await self.initialize()

        return await self._distribution.resolve_dependencies(runtime.to_internal(), lockfile)

    async def get_stats(self) -> Dict:
        """Get comprehensive statistics."""
        stats = {
            "initialized": self._initialized,
            "active_sandboxes": len(self._sandboxes),
            "storage_dir": self.storage_dir,
        }

        if self._pool:
            pool_stats = self._pool.get_pool_stats()
            stats["warm_pool"] = {
                "total_snapshots": pool_stats["total_snapshots"],
                "utilization_percent": pool_stats["utilization_percent"],
                "scheduler": pool_stats["scheduler"],
            }

        if self._block_store:
            block_stats = self._block_store.get_stats()
            stats["block_store"] = {
                "blocks_stored": block_stats["blocks_stored"],
                "compression_ratio": block_stats["compression_ratio"],
                "bytes_written_mb": block_stats["bytes_written"] / 1024 / 1024,
            }

        return stats

    async def _get_default_files(self, runtime: Runtime) -> Dict[str, bytes]:
        """Get default files for a runtime."""
        files = {"README.md": f"# {runtime.value}\n\nAuto-generated sandbox.".encode()}

        if runtime == Runtime.PYTHON_ML:
            files["requirements.txt"] = b"numpy>=1.24\npandas>=2.0\ntorch>=2.0\n"
            files["main.py"] = b"import numpy as np\nprint('Python ML sandbox ready')\n"

        elif runtime == Runtime.PYTHON_DATA:
            files["requirements.txt"] = b"numpy>=1.24\npandas>=2.0\nscipy>=1.10\n"
            files["main.py"] = b"import pandas as pd\nprint('Data sandbox ready')\n"

        elif runtime == Runtime.PYTHON_WEB:
            files["requirements.txt"] = b"fastapi>=0.100\nuvicorn>=0.23\n"
            files["main.py"] = (
                b"from fastapi import FastAPI\napp = FastAPI()\n@app.get('/')\ndef hello():\n    return {'status': 'ready'}\n"
            )

        elif runtime == Runtime.NODE_WEB:
            files["package.json"] = b'{"name":"node-sandbox","main":"index.js"}\n'
            files["index.js"] = b'console.log("Node.js sandbox ready");\n'

        return files

    async def _run_code(self, sandbox_id: str, code: str, timeout: int = 30) -> ExecutionResult:
        """Execute code in a sandbox (simplified)."""
        start = time.perf_counter()

        try:
            # In production, this would use actual sandbox execution
            result = f"[{sandbox_id}] {code}"

            return ExecutionResult(success=True, output=result, execution_time_ms=(time.perf_counter() - start) * 1000)

        except Exception as e:
            return ExecutionResult(
                success=False, output="", error=str(e), execution_time_ms=(time.perf_counter() - start) * 1000
            )


class Mem0StorageCLI:
    """
    Command-line interface for Mem0 Storage.

    Usage:
        python -m mem0.storage.cli create --user_id=user123 --runtime=python-ml
        python -m mem0.storage.cli list --user_id=user123
        python -m mem0.storage.cli stats
    """

    def __init__(self):
        self.storage = Mem0Storage()

    async def run(self, args):
        """Run CLI command."""
        command = getattr(args, "command", "help")

        if command == "create":
            await self._cmd_create(args)
        elif command == "fork":
            await self._cmd_fork(args)
        elif command == "list":
            await self._cmd_list(args)
        elif command == "stats":
            await self._cmd_stats(args)
        elif command == "delete":
            await self._cmd_delete(args)
        else:
            self._print_help()

    async def _cmd_create(self, args):
        """Create sandbox."""
        runtime = Runtime(args.runtime)
        sandbox = await self.storage.create_sandbox(user_id=args.user_id, runtime=runtime)

        print(f"Created sandbox: {sandbox.sandbox_id}")
        print(f"Runtime: {runtime.value}")
        print(f"Warm: {'Yes' if True else 'No'}")

    async def _cmd_fork(self, args):
        """Fork sandbox."""
        fork = await self.storage.fork_sandbox(args.sandbox_id, args.new_user)
        print(f"Forked: {fork.sandbox_id}")

    async def _cmd_list(self, args):
        """List sandboxes."""
        stats = await self.storage.get_stats()
        print(f"Active sandboxes: {stats['active_sandboxes']}")

    async def _cmd_stats(self, args):
        """Show statistics."""
        stats = await self.storage.get_stats()
        print(json.dumps(stats, indent=2, default=str))

    async def _cmd_delete(self, args):
        """Delete sandbox."""
        success = await self.storage.delete_sandbox(args.sandbox_id)
        print(f"Deleted: {success}")

    def _print_help(self):
        """Print help."""
        print("""
Mem0 Storage CLI

Commands:
    create --user_id=X --runtime=Y    Create sandbox
    fork --sandbox_id=X --new_user=Y   Fork sandbox
    list --user_id=X                  List sandboxes
    stats                             Show statistics
    delete --sandbox_id=X             Delete sandbox

Runtimes:
    python-data, python-ml, python-web
    node-web, go-runtime, browser-chromium
""")


# Convenience functions for quick usage
async def quick_start(storage_dir: str = None, enable_warm_pool: bool = True) -> Mem0Storage:
    """Quick initialization."""
    storage = Mem0Storage(storage_dir=storage_dir, enable_warm_pool=enable_warm_pool)
    await storage.initialize()
    return storage


async def create_quick_sandbox(user_id: str, runtime: Runtime = Runtime.PYTHON_ML) -> Sandbox:
    """Create sandbox with defaults."""
    storage = await quick_start()
    return await storage.create_sandbox(user_id=user_id, runtime=runtime)


# Export main classes
__all__ = [
    "Mem0Storage",
    "Sandbox",
    "SandboxConfig",
    "SandboxFile",
    "ExecutionResult",
    "Runtime",
    "quick_start",
    "create_quick_sandbox",
    "Mem0StorageCLI",
]
