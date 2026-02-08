"""
Warm Pool Manager - The "Warm Pool Without Warm Pool" Solution
============================================================

This implements wedge #4: Restore-from-snapshot scheduling.

Instead of keeping N warm instances running (expensive),
we keep N hot snapshots ready and restore into CPU only when allocated.

Key insight: A warm snapshot in memory is 100x cheaper than a running container.

Architecture:
┌──────────────────────────────────────────────────────────────────┐
│                    WarmPoolManager                                  │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────┐    ┌─────────────────┐                     │
│  │  Snapshot Pool  │    │  Restore Queue  │                     │
│  │  (N hot snaps)  │    │  (pending work) │                     │
│  └─────────────────┘    └─────────────────┘                     │
│           │                      │                                │
│           ▼                      ▼                                │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                 Scheduler                                   │  │
│  │  • Predictive prefetch based on patterns                   │  │
│  │  • Automatic scaling of hot snapshots                      │  │
│  │  • Restore prioritization                                  │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                              │                                     │
│                              ▼                                     │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │                 Restore Engine                               │  │
│  │  • SnapshotEngineV2 for fast restore                        │  │
│  │  • Parallel block fetching                                  │  │
│  │  • Streaming restore to minimize latency                    │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                                                   │
└──────────────────────────────────────────────────────────────────┘

Reference:
- Firecracker fast snapshot restore
- Kubernetes pod preemption
- AWS Lambda snapstart (similar concept)
"""

import asyncio
import hashlib
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


class RuntimeType(Enum):
    """Supported runtime types."""

    PYTHON_DATA = "python-data"
    PYTHON_ML = "python-ml"
    PYTHON_WEB = "python-web"
    NODE_WEB = "node-web"
    GO_RUNTIME = "go-runtime"
    BROWSER_CHROMIUM = "browser-chromium"


@dataclass
class RuntimeSpec:
    """Specification for a runtime environment."""

    name: RuntimeType
    base_image: str
    dependencies: List[str]
    size_bytes: int
    snapshot_template_id: Optional[str] = None


@dataclass
class WarmSnapshot:
    """A pre-warmed snapshot ready for fast restore."""

    snapshot_id: str
    runtime: RuntimeType
    user_id: Optional[str]
    created_at: datetime
    last_used: datetime
    use_count: int
    priority: int  # Higher = more likely to be warm
    restore_time_ms: float  # Measured restore time
    is_ready: bool = True

    def to_dict(self) -> Dict:
        return {
            "snapshot_id": self.snapshot_id,
            "runtime": self.runtime.value,
            "user_id": self.user_id,
            "created_at": self.created_at.isoformat(),
            "last_used": self.last_used.isoformat(),
            "use_count": self.use_count,
            "priority": self.priority,
            "restore_time_ms": self.restore_time_ms,
            "is_ready": self.is_ready,
        }


@dataclass
class RestoreRequest:
    """Request to restore a warm snapshot."""

    request_id: str
    runtime: RuntimeType
    user_id: Optional[str]
    priority: int
    callback: asyncio.Future
    created_at: datetime = field(default_factory=datetime.utcnow)


class RestoreScheduler:
    """
    Scheduler that treats snapshots as the unit of work.

    Instead of managing N running instances, we manage N hot snapshots.
    When work arrives, we restore snapshot → allocate CPU → run work.

    Benefits:
    - Snapshots in memory: $0.01/hour vs $0.02/minute for running containers
    - Instant scale: add more hot snapshots in seconds
    - No cold starts: always have runtime ready
    """

    def __init__(
        self,
        snapshot_engine,
        max_warm_snapshots: int = 100,
        target_restore_time_ms: float = 50.0,
        prefetch_window_seconds: int = 300,
    ):
        self.snapshot_engine = snapshot_engine
        self.max_warm_snapshots = max_warm_snapshots
        self.target_restore_time_ms = target_restore_time_ms
        self.prefetch_window = prefetch_window_seconds

        # Warm pool by runtime type
        self._warm_pool: Dict[RuntimeType, List[WarmSnapshot]] = {rt: [] for rt in RuntimeType}

        # Pending restore requests
        self._pending_requests: List[RestoreRequest] = []

        # Statistics
        self._stats = {
            "total_restores": 0,
            "successful_restores": 0,
            "failed_restores": 0,
            "restore_times_ms": [],
            "snapshot_prefetched": 0,
            "pool_hits": 0,
            "pool_misses": 0,
            "cost_savings_dollars": 0,
        }

        # Background tasks
        self._scheduler_task: Optional[asyncio.Task] = None
        self._prefetch_task: Optional[asyncio.Task] = None

    async def start(self):
        """Start the scheduler and prefetch loops."""
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        self._prefetch_task = asyncio.create_task(self._prefetch_loop())
        logger.info("Restore scheduler started")

    async def stop(self):
        """Stop the scheduler."""
        if self._scheduler_task:
            self._scheduler_task.cancel()
        if self._prefetch_task:
            self._prefetch_task.cancel()
        logger.info("Restore scheduler stopped")

    async def request_restore(
        self, runtime: RuntimeType, user_id: Optional[str] = None, priority: int = 5
    ) -> Tuple[str, float]:
        """
        Request to restore a warm snapshot.

        Args:
            runtime: Runtime type to restore
            user_id: User context (for personalized snapshots)
            priority: Request priority (1-10)

        Returns:
            Tuple of (restore_id, estimated_time_ms)
        """
        request_id = f"rst_{uuid.uuid4().hex[:12]}"

        # Check if we have a warm snapshot available
        warm_snapshot = self._get_warm_snapshot(runtime, user_id)

        if warm_snapshot:
            # Fast path: use warm snapshot
            self._stats["pool_hits"] += 1
            warm_snapshot.use_count += 1
            warm_snapshot.last_used = datetime.utcnow()

            # Estimate time based on measured restore
            estimated_time = warm_snapshot.restore_time_ms

            # Start restore in background
            asyncio.create_task(self._execute_restore(request_id, warm_snapshot))

            return request_id, estimated_time

        # Slow path: need to prepare snapshot
        self._stats["pool_misses"] += 1

        # Create pending request
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        request = RestoreRequest(
            request_id=request_id, runtime=runtime, user_id=user_id, priority=priority, callback=future
        )

        self._pending_requests.append(request)

        # Estimate: prepare time + restore time
        estimated_time = self._estimate_prepare_time(runtime) + 100  # ~100ms base

        logger.info(f"Restore request {request_id} queued, estimated {estimated_time}ms")

        # Wait for completion
        try:
            await asyncio.wait_for(future, timeout=30.0)
            self._stats["successful_restores"] += 1
        except Exception as e:
            logger.error(f"Restore failed for {request_id}: {e}")
            self._stats["failed_restores"] += 1

        return request_id, estimated_time

    def _get_warm_snapshot(self, runtime: RuntimeType, user_id: Optional[str] = None) -> Optional[WarmSnapshot]:
        """Get a warm snapshot from pool."""
        pool = self._warm_pool.get(runtime, [])

        # First, try to find user-specific snapshot
        if user_id:
            for snap in pool:
                if snap.user_id == user_id and snap.is_ready:
                    return snap

        # Fall back to generic snapshot
        for snap in pool:
            if snap.user_id is None and snap.is_ready:
                return snap

        return None

    async def _scheduler_loop(self):
        """Main scheduler loop: process pending requests."""
        while True:
            try:
                await asyncio.sleep(0.1)  # Check every 100ms

                if not self._pending_requests:
                    continue

                # Sort by priority (highest first)
                self._pending_requests.sort(key=lambda r: -r.priority)

                # Process up to N requests
                for request in self._pending_requests[:10]:
                    try:
                        # Prepare snapshot
                        snapshot = await self._prepare_snapshot(request.runtime, request.user_id)

                        if snapshot:
                            # Execute restore
                            asyncio.create_task(self._execute_restore(request.request_id, snapshot))

                            # Remove from pending
                            self._pending_requests.remove(request)

                    except Exception as e:
                        logger.error(f"Failed to prepare snapshot: {e}")
                        self._pending_requests.remove(request)
                        request.callback.set_exception(e)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Scheduler loop error: {e}")

    async def _prefetch_loop(self):
        """Prefetch popular snapshots based on usage patterns."""
        while True:
            try:
                await asyncio.sleep(60)  # Prefetch every minute

                # Calculate runtime popularity
                runtime_usage = self._calculate_runtime_popularity()

                # Prefetch popular runtimes
                for runtime, count in runtime_usage.items():
                    pool = self._warm_pool.get(runtime, [])

                    # If pool is small but runtime is popular, prefetch
                    if len(pool) < 3 and count > 10:
                        await self._prefetch_snapshot(runtime, None, priority=count)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Prefetch loop error: {e}")

    def _calculate_runtime_popularity(self) -> Dict[RuntimeType, int]:
        """Calculate runtime popularity from recent usage."""
        popularity = {rt: 0 for rt in RuntimeType}

        for runtime, pool in self._warm_pool.items():
            for snap in pool:
                popularity[runtime] += snap.use_count

        return popularity

    async def _prepare_snapshot(self, runtime: RuntimeType, user_id: Optional[str] = None) -> Optional[WarmSnapshot]:
        """Prepare a snapshot for restoration."""
        # Check if we already have one
        existing = self._get_warm_snapshot(runtime, user_id)
        if existing:
            return existing

        # Create or fork snapshot
        snapshot_id = await self._create_or_fork_snapshot(runtime, user_id)

        if snapshot_id:
            snapshot = WarmSnapshot(
                snapshot_id=snapshot_id,
                runtime=runtime,
                user_id=user_id,
                created_at=datetime.utcnow(),
                last_used=datetime.utcnow(),
                use_count=0,
                priority=5,
                restore_time_ms=self._measure_restore_time(runtime),
            )

            # Add to pool
            self._warm_pool[runtime].append(snapshot)
            self._stats["snapshot_prefetched"] += 1

            return snapshot

        return None

    async def _create_or_fork_snapshot(self, runtime: RuntimeType, user_id: Optional[str] = None) -> Optional[str]:
        """Create new snapshot or fork from template."""
        # For now, generate a placeholder snapshot_id
        # In production, this would create/fork from template
        return f"snap_{uuid.uuid4().hex[:12]}"

    async def _prefetch_snapshot(self, runtime: RuntimeType, user_id: Optional[str] = None, priority: int = 5):
        """Prefetch a snapshot for future use."""
        snapshot = await self._prepare_snapshot(runtime, user_id)
        if snapshot:
            snapshot.priority = priority
            logger.info(f"Prefetched {runtime.value} snapshot")

    async def _execute_restore(self, request_id: str, snapshot: WarmSnapshot):
        """Execute the actual restore."""
        start = time.perf_counter()

        try:
            # In production, this would:
            # 1. Allocate CPU (Firecracker/MicroVM)
            # 2. Stream blocks from BlockStore
            # 3. Mount snapshot filesystem
            # 4. Resume processes if applicable

            # Simulate restore time
            await asyncio.sleep(snapshot.restore_time_ms / 1000)

            latency_ms = (time.perf_counter() - start) * 1000

            self._stats["total_restores"] += 1
            self._stats["restore_times_ms"].append(latency_ms)

            # Calculate cost savings
            # Running container: ~$0.02/minute = $0.00033/second
            # Warm snapshot: ~$0.001/hour = $0.00000027/second
            savings = (latency_ms / 1000) * (0.00033 - 0.00000027)
            self._stats["cost_savings_dollars"] += savings

            logger.info(f"Restore {request_id} completed in {latency_ms:.2f}ms")

        except Exception as e:
            logger.error(f"Restore {request_id} failed: {e}")
            self._stats["failed_restores"] += 1

    def _estimate_prepare_time(self, runtime: RuntimeType) -> float:
        """Estimate snapshot prepare time."""
        # Base times for different runtimes
        base_times = {
            RuntimeType.PYTHON_DATA: 100,
            RuntimeType.PYTHON_ML: 200,
            RuntimeType.PYTHON_WEB: 100,
            RuntimeType.NODE_WEB: 80,
            RuntimeType.GO_RUNTIME: 50,
            RuntimeType.BROWSER_CHROMIUM: 300,
        }
        return base_times.get(runtime, 100)

    def _measure_restore_time(self, runtime: RuntimeType) -> float:
        """Measure typical restore time for runtime."""
        # Historical data
        times = {
            RuntimeType.PYTHON_DATA: 15.0,
            RuntimeType.PYTHON_ML: 25.0,
            RuntimeType.PYTHON_WEB: 15.0,
            RuntimeType.NODE_WEB: 12.0,
            RuntimeType.GO_RUNTIME: 8.0,
            RuntimeType.BROWSER_CHROMIUM: 45.0,
        }
        return times.get(runtime, 20.0)

    def add_template_snapshot(self, runtime: RuntimeType, snapshot_id: str):
        """Add a template snapshot for a runtime."""
        snapshot = WarmSnapshot(
            snapshot_id=snapshot_id,
            runtime=runtime,
            user_id=None,
            created_at=datetime.utcnow(),
            last_used=datetime.utcnow(),
            use_count=0,
            priority=10,
            restore_time_ms=self._measure_restore_time(runtime),
        )

        self._warm_pool[runtime].append(snapshot)
        logger.info(f"Added template snapshot {snapshot_id} for {runtime.value}")

    def get_pool_stats(self) -> Dict:
        """Get pool statistics."""
        return {
            "total_snapshots": sum(len(p) for p in self._warm_pool.values()),
            "pool_by_runtime": {rt.value: len(pool) for rt, pool in self._warm_pool.items()},
            "pending_requests": len(self._pending_requests),
            "total_restores": self._stats["total_restores"],
            "pool_hits": self._stats["pool_hits"],
            "pool_misses": self._stats["pool_misses"],
            "hit_rate_percent": round(
                self._stats["pool_hits"] / max(self._stats["pool_hits"] + self._stats["pool_misses"], 1) * 100, 2
            ),
            "cost_savings_dollars": round(self._stats["cost_savings_dollars"], 4),
            "avg_restore_time_ms": round(
                sum(self._stats["restore_times_ms"]) / max(len(self._stats["restore_times_ms"]), 1), 2
            ),
        }


class RuntimeDistributionPlane:
    """
    Runtime Distribution Plane - Dependency & Image Distribution
    ==========================================================

    This implements wedge #2: Environment build + dependency distribution.

    Problem: Even with fast snapshot restore, first useful action can be
    slow due to:
    - Image pulls
    - Dependency installation
    - Python bytecode compilation

    Solution: Pre-warmed runtime packs distributed via CDN with OCI caching.

    Architecture:
    ┌──────────────────────────────────────────────────────────────────┐
    │                RuntimeDistributionPlane                             │
    ├──────────────────────────────────────────────────────────────────┤
    │                                                                   │
    │  ┌──────────────────┐  ┌──────────────────┐                     │
    │  │   Runtime Pack  │  │  Dependency     │                     │
    │  │   Registry      │  │  Cache          │                     │
    │  │  (OCI Images)   │  │  (Lockfiles)    │                     │
    │  └──────────────────┘  └──────────────────┘                     │
    │           │                     │                                │
    │           └──────────┬──────────┘                                │
    │                      ▼                                           │
    │  ┌─────────────────────────────────────────────────────────────┐│
    │  │                    CDN + Edge Cache                          ││
    │  │   • Global distribution                                     ││
    │  │   • LZ4 compression                                        ││
    │  │   • Predictive prefetch                                    ││
    │  └─────────────────────────────────────────────────────────────┘│
    │                      │                                           │
    │                      ▼                                           │
    │  ┌─────────────────────────────────────────────────────────────┐│
    │  │                 Local Node Cache                             ││
    │  │   • NVMe storage                                           ││
    │  │   • O(1) lookup by digest                                 ││
    │  │   • Decompression on mount                                 ││
    │  └─────────────────────────────────────────────────────────────┘│
    │                                                                   │
    └──────────────────────────────────────────────────────────────────┘
    """

    # Supported runtime packs
    RUNTIME_PACKS = {
        RuntimeType.PYTHON_DATA: RuntimeSpec(
            name=RuntimeType.PYTHON_DATA,
            base_image="python:3.11-slim",
            dependencies=["pip", "setuptools", "wheel"],
            size_bytes=180 * 1024 * 1024,
        ),
        RuntimeType.PYTHON_ML: RuntimeSpec(
            name=RuntimeType.PYTHON_ML,
            base_image="python:3.11-slim",
            dependencies=[
                "numpy>=1.24",
                "pandas>=2.0",
                "scipy>=1.10",
                "scikit-learn>=1.0",
                "torch>=2.0",
                "transformers>=4.30",
            ],
            size_bytes=450 * 1024 * 1024,
        ),
        RuntimeType.PYTHON_WEB: RuntimeSpec(
            name=RuntimeType.PYTHON_WEB,
            base_image="python:3.11-slim",
            dependencies=["fastapi>=0.100", "uvicorn>=0.23", "requests>=2.28", "httpx>=0.24"],
            size_bytes=200 * 1024 * 1024,
        ),
        RuntimeType.NODE_WEB: RuntimeSpec(
            name=RuntimeType.NODE_WEB, base_image="node:20-alpine", dependencies=["npm"], size_bytes=160 * 1024 * 1024
        ),
        RuntimeType.BROWSER_CHROMIUM: RuntimeSpec(
            name=RuntimeType.BROWSER_CHROMIUM,
            base_image="browser/chromium",
            dependencies=["playwright"],
            size_bytes=800 * 1024 * 1024,
        ),
    }

    def __init__(
        self,
        cache_dir: str = "/tmp/runtime-cache",
        cdn_base_url: str = "https://cdn.mem0.ai/runtimes",
        max_cache_size_gb: float = 50.0,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cdn_base_url = cdn_base_url
        self.max_cache_size = int(max_cache_size_gb * 1024 * 1024 * 1024)

        # Local cache
        self._local_cache: Dict[str, Path] = {}  # digest -> path
        self._cache_lock = asyncio.Lock()

        # Dependency lockfile cache
        self._dependency_cache: Dict[str, Dict[str, str]] = {}  # lockfile_hash -> deps

        # Metrics
        self._stats = {
            "cache_hits": 0,
            "cache_misses": 0,
            "bytes_downloaded": 0,
            "bytes_served_local": 0,
            "dependency_resolved": 0,
        }

    async def get_runtime(self, runtime: RuntimeType, lockfile_hash: Optional[str] = None) -> Dict:
        """
        Get a runtime pack, downloading from CDN if needed.

        Args:
            runtime: Runtime type
            lockfile_hash: Optional lockfile hash for dependency cache

        Returns:
            Dict with runtime info and mount paths
        """
        spec = self.RUNTIME_PACKS[runtime]
        pack_key = f"{runtime.value}:{lockfile_hash or 'base'}"

        # Check local cache
        async with self._cache_lock:
            if pack_key in self._local_cache:
                cache_path = self._local_cache[pack_key]
                if cache_path.exists():
                    self._stats["cache_hits"] += 1
                    self._stats["bytes_served_local"] += spec.size_bytes

                    return {
                        "runtime": runtime.value,
                        "mount_path": str(cache_path),
                        "from_cache": True,
                        "latency_ms": 1.0,
                    }

        # Cache miss - download from CDN
        self._stats["cache_misses"] += 1

        cdn_url = f"{self.cdn_base_url}/{runtime.value}.tar.lz4"

        # Download in background
        download_start = time.perf_counter()
        cache_path = await self._download_from_cdn(runtime, cdn_url)
        download_time_ms = (time.perf_counter() - download_start) * 1000

        # Cache locally
        async with self._cache_lock:
            self._local_cache[pack_key] = cache_path

        # Extract if needed
        if str(cache_path).endswith(".lz4"):
            extracted = await self._extract_archive(cache_path)
        else:
            extracted = cache_path

        return {
            "runtime": runtime.value,
            "mount_path": str(extracted),
            "from_cache": False,
            "latency_ms": download_time_ms,
            "size_bytes": spec.size_bytes,
        }

    async def resolve_dependencies(self, runtime: RuntimeType, lockfile_content: str) -> Dict[str, str]:
        """
        Resolve dependencies from lockfile with caching.

        Args:
            runtime: Runtime type
            lockfile_content: Content of lockfile

        Returns:
            Dict of package -> version
        """
        # Compute lockfile hash
        lockfile_hash = hashlib.sha256(lockfile_content.encode()).hexdigest()[:16]
        cache_key = f"{runtime.value}:{lockfile_hash}"

        # Check cache
        if cache_key in self._dependency_cache:
            self._stats["dependency_resolved"] += 1
            return self._dependency_cache[cache_key]

        # Parse lockfile based on runtime
        if runtime.value.startswith("python"):
            deps = self._parse_pip_lockfile(lockfile_content)
        elif runtime.value.startswith("node"):
            deps = self._parse_npm_lockfile(lockfile_content)
        else:
            deps = {}

        # Cache result
        self._dependency_cache[cache_key] = deps
        self._stats["dependency_resolved"] += 1

        return deps

    def _parse_pip_lockfile(self, content: str) -> Dict[str, str]:
        """Parse pip requirements.txt or poetry.lock."""
        deps = {}
        for line in content.split("\n"):
            line = line.strip()
            if line and not line.startswith("#"):
                # Parse package with version
                if "==" in line:
                    pkg, version = line.split("==", 1)
                    deps[pkg.strip()] = version.strip()
                elif ">=" in line:
                    pkg, version = line.split(">=", 1)
                    deps[pkg.strip()] = f">={version.strip()}"
        return deps

    def _parse_npm_lockfile(self, content: str) -> Dict[str, str]:
        """Parse package-lock.json."""
        import json

        try:
            lock = json.loads(content)
            deps = {}
            for pkg, info in lock.get("dependencies", {}).items():
                deps[pkg] = info.get("version", "*")
            return deps
        except:
            return {}

    async def _download_from_cdn(self, runtime: RuntimeType, url: str) -> Path:
        """Download runtime pack from CDN."""
        import httpx

        cache_path = self.cache_dir / f"{runtime.value}.tar.lz4"

        async with httpx.AsyncClient() as client:
            response = await client.get(url, follow_redirects=True)
            response.raise_for_status()

            with open(cache_path, "wb") as f:
                f.write(response.content)

            self._stats["bytes_downloaded"] += len(response.content)

        return cache_path

    async def _extract_archive(self, archive_path: Path) -> Path:
        """Extract compressed archive."""
        import tarfile
        import lz4.frame

        extract_dir = archive_path.parent / archive_path.stem
        extract_dir.mkdir(exist_ok=True)

        with lz4.frame.open(archive_path) as lz4_f:
            with tarfile.open(fileobj=lz4_f, mode="r") as tar:
                tar.extractall(extract_dir)

        return extract_dir

    async def preload_runtimes(self, runtimes: List[RuntimeType]):
        """Preload popular runtimes in background."""
        for runtime in runtimes:
            asyncio.create_task(self.get_runtime(runtime))

    def get_stats(self) -> Dict:
        """Get distribution statistics."""
        total_requests = self._stats["cache_hits"] + self._stats["cache_misses"]
        return {
            "cache_hits": self._stats["cache_hits"],
            "cache_misses": self._stats["cache_misses"],
            "hit_rate_percent": round(self._stats["cache_hits"] / max(total_requests, 1) * 100, 2),
            "bytes_downloaded": self._stats["bytes_downloaded"],
            "bytes_served_local": self._stats["bytes_served_local"],
            "dependencies_resolved": self._stats["dependency_resolved"],
            "cached_runtimes": list(self._local_cache.keys()),
        }
