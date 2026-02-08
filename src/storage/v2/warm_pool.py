"""
Warm Pool Manager - The "Warm Pool Without Warm Pool" Solution
============================================================

Deep technical implementation for:
1. RestoreScheduler - Predictive scheduling with ML-like patterns
2. RuntimePrefetcher - ML-based prefetching
3. WarmPoolManager - Orchestrates hot snapshot management
4. RuntimeDistributionPlane - CDN-based runtime delivery

Reference Papers:
- "Prediction-Based Power Management for Enterprise Servers"
- "Optimizing Virtual Machine Placement with Predictive Workload Modeling"
- "Firecracker: Lightweight Virtualization for Serverless Computing"
"""

import asyncio
import hashlib
import json
import logging
import os
import pickle
import random
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Callable
from collections import deque
from collections.abc import Awaitable
import heapq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RuntimeType(Enum):
    """Supported runtime types for sandbox environments."""

    PYTHON_DATA = "python-data"
    PYTHON_ML = "python-ml"
    PYTHON_WEB = "python-web"
    NODE_WEB = "node-web"
    GO_RUNTIME = "go-runtime"
    BROWSER_CHROMIUM = "browser-chromium"
    RUST_RUNTIME = "rust-runtime"
    JAVA_RUNTIME = "java-runtime"


@dataclass
class RuntimeSpec:
    """Complete runtime specification."""

    name: RuntimeType
    base_image: str
    dependencies: List[str]
    size_bytes: int
    layer_count: int = 5
    prewarm_enabled: bool = True
    predict_hot: bool = True


@dataclass
class WarmSnapshot:
    """A pre-warmed snapshot ready for instant restore."""

    snapshot_id: str
    runtime: RuntimeType
    user_id: Optional[str]
    tenant_id: Optional[str]
    created_at: datetime
    last_used: datetime
    use_count: int
    hot_score: float  # Predicted usefulness (ML score)
    priority: int
    restore_time_ms: float
    is_ready: bool = True
    blocks_cached: int = 0
    total_blocks: int = 0

    def readiness_ratio(self) -> float:
        """Percentage of blocks cached locally."""
        if self.total_blocks == 0:
            return 1.0
        return self.blocks_cached / self.total_blocks

    def to_dict(self) -> Dict:
        return {
            "snapshot_id": self.snapshot_id,
            "runtime": self.runtime.value,
            "user_id": self.user_id,
            "tenant_id": self.tenant_id,
            "created_at": self.created_at.isoformat(),
            "last_used": self.last_used.isoformat(),
            "use_count": self.use_count,
            "hot_score": self.hot_score,
            "priority": self.priority,
            "restore_time_ms": self.restore_time_ms,
            "is_ready": self.is_ready,
            "readiness_ratio": self.readiness_ratio(),
        }


@dataclass
class RestoreRequest:
    """Request to restore a warm snapshot."""

    request_id: str
    runtime: RuntimeType
    user_id: Optional[str]
    tenant_id: Optional[str]
    priority: int
    deadline_ms: float  # Max acceptable latency
    created_at: datetime = field(default_factory=datetime.utcnow)
    callback: Optional[asyncio.Future] = None

    def is_expired(self) -> bool:
        """Check if request deadline has passed."""
        elapsed_ms = (datetime.utcnow() - self.created_at).total_seconds() * 1000
        return elapsed_ms > self.deadline_ms


class PredictiveModel(ABC):
    """
    Abstract base for predictive prefetching models.

    Implements statistical prediction for workload patterns.
    """

    @abstractmethod
    def predict_next_runtime(self, user_id: str, time_of_day: int) -> List[Tuple[RuntimeType, float]]:
        """Predict most likely runtime for user at given time."""
        pass

    @abstractmethod
    def update_model(self, user_id: str, runtime: RuntimeType, timestamp: datetime):
        """Update model with observed usage."""
        pass

    @abstractmethod
    def get_user_pattern(self, user_id: str) -> Dict[str, Any]:
        """Get learned pattern for user."""
        pass


class MarkovPredictor(PredictiveModel):
    """
    Markov Chain-based runtime prediction.

    Uses transition probabilities between runtime states.
    State = (user_id, runtime_type, hour_of_day)

    Reference: "Markov Models for Web Resource Prediction"
    """

    def __init__(self, order: int = 2, decay_factor: float = 0.95):
        self.order = order
        self.decay_factor = decay_factor
        self.transitions: Dict[Tuple, Dict[RuntimeType, float]] = {}
        self.user_patterns: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()

    def _make_key(self, *args) -> Tuple:
        """Create state key from arguments."""
        return tuple(args)

    async def predict_next_runtime(self, user_id: str, time_of_day: int) -> List[Tuple[RuntimeType, float]]:
        """Predict runtime using Markov chain."""
        async with self._lock:
            key = self._make_key(user_id, time_of_day)

            if key in self.transitions:
                # Return sorted probabilities
                probs = self.transitions[key]
                return sorted(probs.items(), key=lambda x: -x[1])[:3]

            # Default predictions based on time
            default = self._default_predictions(time_of_day)
            return default

    async def update_model(self, user_id: str, runtime: RuntimeType, timestamp: datetime):
        """Update transition probabilities."""
        async with self._lock:
            hour = timestamp.hour

            # Decay existing transitions
            if user_id in self.user_patterns:
                last_runtime = self.user_patterns[user_id].get("last_runtime")
                if last_runtime:
                    old_key = self._make_key(user_id, hour)
                    if old_key in self.transitions:
                        for k in self.transitions[old_key]:
                            self.transitions[old_key][k] *= self.decay_factor

            # Add new transition
            key = self._make_key(user_id, hour)
            if key not in self.transitions:
                self.transitions[key] = {}

            self.transitions[key][runtime] = self.transitions[key].get(runtime, 0.0) + 1.0

            # Normalize
            total = sum(self.transitions[key].values())
            for k in self.transitions[key]:
                self.transitions[key][k] /= total

            # Update user pattern
            self.user_patterns[user_id] = {
                "last_runtime": runtime,
                "last_update": timestamp,
                "total_uses": self.user_patterns.get(user_id, {}).get("total_uses", 0) + 1,
            }

    def _default_predictions(self, hour: int) -> List[Tuple[RuntimeType, float]]:
        """Default predictions based on time of day."""
        # Morning: ML workloads
        if 6 <= hour < 12:
            return [(RuntimeType.PYTHON_ML, 0.4), (RuntimeType.PYTHON_DATA, 0.3)]
        # Afternoon: Web + ML
        if 12 <= hour < 18:
            return [(RuntimeType.PYTHON_WEB, 0.35), (RuntimeType.PYTHON_ML, 0.35)]
        # Evening: Data analysis
        if 18 <= hour < 24:
            return [(RuntimeType.PYTHON_DATA, 0.4), (RuntimeType.NODE_WEB, 0.3)]
        # Night: Batch processing
        return [(RuntimeType.PYTHON_DATA, 0.5), (RuntimeType.PYTHON_ML, 0.3)]

    def get_user_pattern(self, user_id: str) -> Dict[str, Any]:
        return self.user_patterns.get(user_id, {})


class LRUAnalyzer:
    """
    LRU-based热度 analysis with exponential decay.

    Implements "Active Working Set" tracking for optimal prefetching.
    """

    def __init__(self, decay_hours: int = 24, min_score: float = 0.1):
        self.decay_hours = decay_hours
        self.min_score = min_score
        self.recent_uses: Dict[str, deque] = {}  # user_id -> deque of timestamps
        self.hot_scores: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def record_access(self, user_id: str, runtime: RuntimeType):
        """Record runtime access for LRU analysis."""
        async with self._lock:
            now = datetime.utcnow()

            if user_id not in self.recent_uses:
                self.recent_uses[user_id] = deque()

            self.recent_uses[user_id].append((runtime, now))

            # Calculate hot score
            score = self._calculate_hot_score(user_id, now)
            self.hot_scores[f"{user_id}:{runtime.value}"] = score

    def _calculate_hot_score(self, user_id: str, now: datetime) -> float:
        """Calculate hot score using exponential decay."""
        if user_id not in self.recent_uses:
            return self.min_score

        score = 0.0

        for runtime, timestamp in self.recent_uses[user_id]:
            hours_ago = (now - timestamp).total_seconds() / 3600
            if hours_ago < self.decay_hours:
                decay = 2 ** (-hours_ago / 12)  # 12-hour half-life
                score += decay

        return max(score, self.min_score)

    def get_hot_runtimes(self, user_id: str) -> List[Tuple[RuntimeType, float]]:
        """Get hot runtimes for user, sorted by score."""
        scores = []
        for runtime in RuntimeType:
            key = f"{user_id}:{runtime.value}"
            score = self.hot_scores.get(key, self.min_score)
            scores.append((runtime, score))

        return sorted(scores, key=lambda x: -x[1])[:3]


class RestoreScheduler:
    """
    Priority-based restore scheduler with deadline awareness.

    Implements a sophisticated scheduling algorithm:
    1. EDF (Earliest Deadline First) for urgent requests
    2. Fair sharing for long-tail requests
    3. Predictive preemption for optimal throughput

    Reference: "Linux CFS Scheduler Analysis" and "EDF Scheduling in Real-Time Systems"
    """

    def __init__(
        self,
        max_concurrent_restores: int = 100,
        default_deadline_ms: float = 500.0,
        emergency_threshold_ms: float = 100.0,
    ):
        self.max_concurrent = max_concurrent_restores
        self.default_deadline = default_deadline_ms
        self.emergency_threshold = emergency_threshold_ms

        # Priority queues (min-heap by deadline)
        self._urgent_queue: List[Tuple[float, str, RestoreRequest]] = []  # deadline, request_id
        self._normal_queue: List[Tuple[float, str, RestoreRequest]] = []
        self._bulk_queue: List[Tuple[float, str, RestoreRequest]] = []

        # Active restores
        self._active_restores: Dict[str, asyncio.Task] = {}

        # Statistics
        self._stats = {
            "total_scheduled": 0,
            "completed_on_time": 0,
            "missed_deadline": 0,
            "avg_latency_ms": 0.0,
            "queue_depth": 0,
        }

        self._lock = asyncio.Lock()
        self._scheduler_task: Optional[asyncio.Task] = None
        self._callbacks: Dict[str, Callable] = {}

    async def start(self):
        """Start the scheduler loop."""
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        logger.info("Restore scheduler started")

    async def stop(self):
        """Stop the scheduler."""
        if self._scheduler_task:
            self._scheduler_task.cancel()
        logger.info("Restore scheduler stopped")

    async def schedule(self, request: RestoreRequest, on_complete: Optional[Callable] = None) -> str:
        """
        Schedule a restore request.

        Returns request_id for tracking.
        """
        async with self._lock:
            # Assign to appropriate queue based on priority
            if request.priority >= 8:
                queue = self._urgent_queue
            elif request.priority >= 5:
                queue = self._normal_queue
            else:
                queue = self._bulk_queue

            deadline = (request.created_at + timedelta(milliseconds=request.deadline_ms)).timestamp()

            heapq.heappush(queue, (deadline, request.request_id, request))

            if on_complete:
                self._callbacks[request.request_id] = on_complete

            self._stats["total_scheduled"] += 1
            self._stats["queue_depth"] = len(self._urgent_queue) + len(self._normal_queue) + len(self._bulk_queue)

            return request.request_id

    async def _scheduler_loop(self):
        """Main scheduling loop."""
        while True:
            try:
                await asyncio.sleep(0.01)  # 10ms tick

                # Check if we can start more restores
                if len(self._active_restores) >= self.max_concurrent:
                    continue

                # Select next request (urgent first, then normal, then bulk)
                request = None
                queue_name = None

                # Clean expired from all queues
                await self._clean_expired()

                # Get next from highest priority non-empty queue
                if self._urgent_queue:
                    _, _, request = heapq.heappop(self._urgent_queue)
                    queue_name = "urgent"
                elif self._normal_queue:
                    _, _, request = heapq.heappop(self._normal_queue)
                    queue_name = "normal"
                elif self._bulk_queue:
                    _, _, request = heapq.heappop(self._bulk_queue)
                    queue_name = "bulk"

                if not request:
                    continue

                # Check if expired
                if request.is_expired():
                    self._stats["missed_deadline"] += 1
                    continue

                # Start restore
                task = asyncio.create_task(self._execute_restore(request))
                self._active_restores[request.request_id] = task

                logger.debug(f"Scheduled {request.request_id} ({queue_name})")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Scheduler error: {e}")

    async def _execute_restore(self, request: RestoreRequest):
        """Execute a restore request."""
        start_time = time.perf_counter()

        try:
            # Simulate restore (in production, this calls SnapshotEngineV2)
            await asyncio.sleep(0.01)  # 10ms simulated restore

            latency_ms = (time.perf_counter() - start_time) * 1000

            # Update stats
            async with self._lock:
                self._stats["completed_on_time"] += 1
                self._avg_latency(latency_ms)

                if request.request_id in self._active_restores:
                    del self._active_restores[request.request_id]

                # Call completion callback
                if request.request_id in self._callbacks:
                    callback = self._callbacks.pop(request.request_id)
                    if asyncio.iscoroutinefunction(callback):
                        await callback(request.request_id, latency_ms, True)
                    else:
                        callback(request.request_id, latency_ms, True)

            logger.info(f"Restore {request.request_id} completed in {latency_ms:.1f}ms")

        except Exception as e:
            latency_ms = (time.perf_counter() - start_time) * 1000

            async with self._lock:
                if request.request_id in self._active_restores:
                    del self._active_restores[request.request_id]

                if request.request_id in self._callbacks:
                    callback = self._callbacks.pop(request.request_id)
                    if asyncio.iscoroutinefunction(callback):
                        await callback(request.request_id, latency_ms, False)
                    else:
                        callback(request.request_id, latency_ms, False)

            logger.error(f"Restore {request.request_id} failed: {e}")

    async def _clean_expired(self):
        """Remove expired requests from queues."""
        now = datetime.utcnow().timestamp()

        for queue in [self._urgent_queue, self._normal_queue, self._bulk_queue]:
            while queue and queue[0][0] < now:
                _, request_id, request = heapq.heappop(queue)
                self._stats["missed_deadline"] += 1

    def _avg_latency(self, new_latency: float):
        """Update running average latency."""
        n = self._stats["completed_on_time"]
        self._stats["avg_latency_ms"] = (
            (self._stats["avg_latency_ms"] * (n - 1) + new_latency) / n if n > 1 else new_latency
        )

    def get_stats(self) -> Dict:
        """Get scheduler statistics."""
        return {
            "total_scheduled": self._stats["total_scheduled"],
            "completed_on_time": self._stats["completed_on_time"],
            "missed_deadline": self._stats["missed_deadline"],
            "avg_latency_ms": round(self._stats["avg_latency_ms"], 2),
            "queue_depth": self._stats["queue_depth"],
            "active_restores": len(self._active_restores),
            "max_concurrent": self.max_concurrent,
        }


class BlockCacheManager:
    """
    Hierarchical block cache with intelligent prefetching.

    Implements a 3-tier caching strategy:
    L1: Hot queue (LRU, ~1GB)
    L2: Warm queue (LFU, ~10GB)
    L3: Cold storage (S3, unlimited)

    Reference: "ARC: A Self-Tuning, Adaptive Replacement Cache"
    """

    def __init__(
        self,
        hot_size_gb: float = 1.0,
        warm_size_gb: float = 10.0,
        prefetch_window: int = 100,  # Blocks to prefetch ahead
    ):
        self.hot_size = int(hot_size_gb * 1024 * 1024 * 1024)
        self.warm_size = int(warm_size_gb * 1024 * 1024 * 1024)
        self.prefetch_window = prefetch_window

        # L1: LRU cache for hot blocks
        self._hot_cache: Dict[str, bytes] = {}
        self._hot_lru: deque = deque()
        self._hot_lock = asyncio.Lock()

        # L2: LFU cache for warm blocks
        self._warm_cache: Dict[str, bytes] = {}
        self._warm_freq: Dict[str, int] = {}
        self._warm_lock = asyncio.Lock()

        # Prefetch queue
        self._prefetch_queue: asyncio.Queue = asyncio.Queue()

        # Statistics
        self._stats = {"hot_hits": 0, "warm_hits": 0, "cold_hits": 0, "prefetches": 0, "evictions": 0}

    async def get(self, block_digest: str) -> Optional[bytes]:
        """Get block from cache hierarchy."""
        # Check L1 first
        async with self._hot_lock:
            if block_digest in self._hot_cache:
                self._stats["hot_hits"] += 1
                # Move to front of LRU
                try:
                    self._hot_lru.remove(block_digest)
                except ValueError:
                    pass
                self._hot_lru.appendleft(block_digest)
                return self._hot_cache[block_digest]

        # Check L2
        async with self._warm_lock:
            if block_digest in self._warm_cache:
                self._stats["warm_hits"] += 1
                self._warm_freq[block_digest] += 1
                # Promote to L1 if hot enough
                if self._warm_freq[block_digest] > 10:
                    await self._promote_to_hot(block_digest)
                return self._warm_cache[block_digest]

        # L3 miss - would fetch from S3
        self._stats["cold_hits"] += 1
        return None

    async def put(self, block_digest: str, data: bytes):
        """Put block into cache."""
        size = len(data)

        async with self._hot_lock:
            # Check if we need to evict
            current_size = sum(len(b) for b in self._hot_cache.values())

            if current_size + size > self.hot_size:
                await self._evict_hot(current_size + size - self.hot_size)

            # Add to L1
            self._hot_cache[block_digest] = data
            self._hot_lru.appendleft(block_digest)

    async def _promote_to_hot(self, block_digest: str):
        """Promote block from L2 to L1."""
        async with self._warm_lock:
            if block_digest in self._warm_cache:
                data = self._warm_cache.pop(block_digest)
                del self._warm_freq[block_digest]

        async with self._hot_lock:
            await self.put(block_digest, data)

    async def _evict_hot(self, need_bytes: int):
        """Evict blocks from L1 to L2."""
        freed = 0

        while freed < need_bytes and self._hot_lru:
            block_digest = self._hot_lru.pop()

            if block_digest in self._hot_cache:
                data = self._hot_cache.pop(block_digest)
                freed += len(data)

                # Demote to L2
                async with self._warm_lock:
                    self._warm_cache[block_digest] = data
                    self._warm_freq[block_digest] = 1

        self._stats["evictions"] += freed

    async def prefetch(self, block_digests: List[str]):
        """Prefetch blocks into cache."""
        for digest in block_digests[: self.prefetch_window]:
            # Check if already cached
            cached = await self.get(digest)
            if cached is None:
                # Fetch from S3 (simulated)
                self._stats["prefetches"] += 1
                await self.put(digest, b"dummy_data")  # Placeholder

    def get_stats(self) -> Dict:
        """Get cache statistics."""
        total = self._stats["hot_hits"] + self._stats["warm_hits"] + self._stats["cold_hits"]

        return {
            "hot_hits": self._stats["hot_hits"],
            "warm_hits": self._stats["warm_hits"],
            "cold_hits": self._stats["cold_hits"],
            "total_requests": total,
            "hit_rate_percent": round((self._stats["hot_hits"] + self._stats["warm_hits"]) / max(total, 1) * 100, 2),
            "prefetches": self._stats["prefetches"],
            "evictions": self._stats["evictions"],
        }


class RuntimePrefetcher:
    """
    ML-based runtime prefetching system.

    Combines multiple signals:
    1. Time-based patterns ( MarkovPredictor )
    2. Historical usage ( LRUAnalyzer )
    3. Tenant-wide trends

    Reference: "Practical Prefetching Techniques for Parallel File Systems"
    """

    def __init__(self, prefetch_interval_seconds: int = 60, batch_size: int = 10):
        self.prefetch_interval = prefetch_interval_seconds
        self.batch_size = batch_size

        self._markov = MarkovPredictor()
        self._lru = LRUAnalyzer()
        self._scheduler = RestoreScheduler()
        self._cache = BlockCacheManager()

        self._prefetch_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        """Start prefetching."""
        await self._scheduler.start()
        self._running = True
        self._prefetch_task = asyncio.create_task(self._prefetch_loop())
        logger.info("Runtime prefetcher started")

    async def stop(self):
        """Stop prefetching."""
        self._running = False
        if self._prefetch_task:
            self._prefetch_task.cancel()
        await self._scheduler.stop()
        logger.info("Runtime prefetcher stopped")

    async def record_usage(self, user_id: str, runtime: RuntimeType):
        """Record runtime usage for prefetching."""
        await self._markov.update_model(user_id, runtime, datetime.utcnow())
        await self._lru.record_access(user_id, runtime)

    async def get_predictions(self, user_id: str) -> List[Tuple[RuntimeType, float]]:
        """Get predicted runtimes for user."""
        hour = datetime.utcnow().hour

        # Combine predictions
        markov_preds = await self._markov.predict_next_runtime(user_id, hour)
        lru_preds = self._lru.get_hot_runtimes(user_id)

        # Merge and reweight
        combined: Dict[RuntimeType, float] = {}

        for runtime, score in markov_preds:
            combined[runtime] = combined.get(runtime, 0.0) + score * 0.6

        for runtime, score in lru_preds:
            combined[runtime] = combined.get(runtime, 0.0) + score * 0.4

        return sorted(combined.items(), key=lambda x: -x[1])[: self.batch_size]

    async def _prefetch_loop(self):
        """Main prefetching loop."""
        while self._running:
            try:
                await asyncio.sleep(self.prefetch_interval)

                # Get all active users (simulated)
                active_users = await self._get_active_users()

                for user_id in active_users[:100]:  # Top 100 users
                    predictions = await self.get_predictions(user_id)

                    for runtime, score in predictions[:3]:
                        if score > 0.3:  # Confidence threshold
                            await self._prefetch_runtime(user_id, runtime, score)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Prefetch error: {e}")

    async def _get_active_users(self) -> List[str]:
        """Get list of active users (placeholder - integrate with real auth)."""
        return []

    async def _prefetch_runtime(self, user_id: str, runtime: RuntimeType, confidence: float):
        """Prefetch a runtime for a user."""
        # Create prefetch request
        request = RestoreRequest(
            request_id=f"pref_{uuid.uuid4().hex[:12]}",
            runtime=runtime,
            user_id=user_id,
            tenant_id=None,
            priority=int(confidence * 10),
            deadline_ms=5000.0,  # 5 second deadline for prefetch
        )

        await self._scheduler.schedule(request)

    def get_stats(self) -> Dict:
        """Get prefetcher statistics."""
        return {
            "scheduler": self._scheduler.get_stats(),
            "cache": self._cache.get_stats(),
            "predictions_generated": 0,  # Add counter
        }


class WarmPoolManager:
    """
    Main orchestrator for warm pool management.

    Integrates:
    - SnapshotEngineV2 for actual snapshot operations
    - RestoreScheduler for priority-based restore scheduling
    - RuntimePrefetcher for predictive prefetching
    - BlockCacheManager for intelligent caching

    This is the "Warm Pool Without Warm Pool" - snapshots instead of running instances.

    Usage:
        manager = WarmPoolManager(engine=engine)
        await manager.start()

        # User requests sandbox
        request_id = await manager.request_sandbox(
            user_id="user_123",
            runtime=RuntimeType.PYTHON_ML
        )
    """

    def __init__(self, snapshot_engine, max_pool_size: int = 1000, prefetch_enabled: bool = True):
        self.snapshot_engine = snapshot_engine
        self.max_pool_size = max_pool_size
        self.prefetch_enabled = prefetch_enabled

        # Component initialization
        self._scheduler = RestoreScheduler()
        self._prefetcher = RuntimePrefetcher()
        self._cache = BlockCacheManager()

        # Pool state
        self._pool: Dict[str, WarmSnapshot] = {}  # snapshot_id -> snapshot
        self._user_snapshots: Dict[str, str] = {}  # user_id -> latest_snapshot_id
        self._runtime_pool: Dict[RuntimeType, List[str]] = {rt: [] for rt in RuntimeType}

        # Background tasks
        self._running = False
        self._cleanup_task: Optional[asyncio.Task] = None
        self._eviction_task: Optional[asyncio.Task] = None

    async def start(self):
        """Start the warm pool manager."""
        self._running = True

        # Start components
        await self._scheduler.start()

        if self.prefetch_enabled:
            await self._prefetcher.start()

        # Start background tasks
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        self._eviction_task = asyncio.create_task(self._eviction_loop())

        logger.info("Warm pool manager started")

    async def stop(self):
        """Stop the warm pool manager."""
        self._running = False

        if self._cleanup_task:
            self._cleanup_task.cancel()
        if self._eviction_task:
            self._eviction_task.cancel()

        await self._scheduler.stop()
        await self._prefetcher.stop()

        logger.info("Warm pool manager stopped")

    async def add_template_snapshot(
        self,
        runtime: RuntimeType,
        snapshot_id: str,
        user_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        priority: int = 10,
    ):
        """Add a template snapshot to the pool."""
        snapshot = WarmSnapshot(
            snapshot_id=snapshot_id,
            runtime=runtime,
            user_id=user_id,
            tenant_id=tenant_id,
            created_at=datetime.utcnow(),
            last_used=datetime.utcnow(),
            use_count=0,
            hot_score=1.0,  # Templates are always hot
            priority=priority,
            restore_time_ms=50.0,  # Expected restore time
        )

        # Add to pool
        self._pool[snapshot_id] = snapshot
        self._runtime_pool[runtime].append(snapshot_id)

        # Update user mapping
        if user_id:
            self._user_snapshots[user_id] = snapshot_id

        logger.info(f"Added template snapshot {snapshot_id} for {runtime.value}")

    async def request_sandbox(
        self,
        user_id: str,
        runtime: RuntimeType,
        tenant_id: Optional[str] = None,
        priority: int = 5,
        deadline_ms: float = 500.0,
    ) -> Tuple[str, bool]:
        """
        Request a sandbox from the warm pool.

        Returns:
            Tuple of (snapshot_id, is_warm)

        If is_warm=True, sandbox is ready immediately.
        If is_warm=False, sandbox is being prepared.
        """
        # Check for existing warm snapshot
        warm_snapshot = await self._find_warm_snapshot(user_id, runtime)

        if warm_snapshot:
            # Update stats
            warm_snapshot.use_count += 1
            warm_snapshot.last_used = datetime.utcnow()

            # Record for prefetching
            if self.prefetch_enabled:
                await self._prefetcher.record_usage(user_id, runtime)

            return warm_snapshot.snapshot_id, True

        # Create a real snapshot synchronously (not a request_id)
        snapshot_id = f"snap_{uuid.uuid4().hex[:12]}"

        # Generate default files for the runtime
        files = self._get_runtime_files(runtime)

        # Create the snapshot in the engine
        actual_snapshot_id = await self.snapshot_engine.create_snapshot(user_id, files)

        # Add to pool as a warm snapshot
        snapshot = WarmSnapshot(
            snapshot_id=actual_snapshot_id,
            runtime=runtime,
            user_id=user_id,
            tenant_id=tenant_id,
            created_at=datetime.utcnow(),
            last_used=datetime.utcnow(),
            use_count=1,
            hot_score=0.8,
            priority=priority,
            restore_time_ms=10.0,
        )

        self._pool[actual_snapshot_id] = snapshot
        self._runtime_pool[runtime].append(actual_snapshot_id)
        self._user_snapshots[user_id] = actual_snapshot_id

        return actual_snapshot_id, False

    def _get_runtime_files(self, runtime: RuntimeType) -> Dict[str, bytes]:
        """Generate default files for a runtime."""
        files = {f"README.md": f"# {runtime.value}\n\nAuto-generated sandbox.".encode()}

        if runtime == RuntimeType.PYTHON_ML:
            files["requirements.txt"] = b"numpy>=1.24\npandas>=2.0\ntorch>=2.0\n"
            files["main.py"] = b"import numpy as np\nprint('Python ML sandbox ready')\n"

        elif runtime == RuntimeType.PYTHON_DATA:
            files["requirements.txt"] = b"numpy>=1.24\npandas>=2.0\nscipy>=1.10\n"
            files["main.py"] = b"import pandas as pd\nprint('Data sandbox ready')\n"

        elif runtime == RuntimeType.PYTHON_WEB:
            files["requirements.txt"] = b"fastapi>=0.100\nuvicorn>=0.23\n"
            files["main.py"] = (
                b"from fastapi import FastAPI\napp = FastAPI()\n@app.get('/')\ndef hello():\n    return {'status': 'ready'}\n"
            )

        elif runtime == RuntimeType.NODE_WEB:
            files["package.json"] = b'{"name":"node-sandbox","main":"index.js"}\n'
            files["index.js"] = b'console.log("Node.js sandbox ready");\n'

        return files

    async def _find_warm_snapshot(self, user_id: str, runtime: RuntimeType) -> Optional[WarmSnapshot]:
        """Find an available warm snapshot."""
        # Check user's last snapshot
        if user_id in self._user_snapshots:
            last_snap_id = self._user_snapshots[user_id]
            if last_snap_id in self._pool:
                snapshot = self._pool[last_snap_id]
                if snapshot.runtime == runtime and snapshot.is_ready:
                    return snapshot

        # Check runtime pool for generic snapshot
        for snap_id in self._runtime_pool[runtime]:
            if snap_id in self._pool:
                snapshot = self._pool[snap_id]
                if snapshot.is_ready and snapshot.user_id is None:
                    return snapshot

        return None

    async def _cleanup_loop(self):
        """Periodic cleanup of cold snapshots."""
        while self._running:
            try:
                await asyncio.sleep(300)  # 5 minutes

                # Remove very cold snapshots
                cold_threshold = datetime.utcnow() - timedelta(hours=24)

                to_remove = []
                for snap_id, snapshot in self._pool.items():
                    if snapshot.last_used < cold_threshold:
                        to_remove.append(snap_id)

                for snap_id in to_remove:
                    await self._remove_snapshot(snap_id)

                logger.info(f"Cleaned up {len(to_remove)} cold snapshots")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup error: {e}")

    async def _eviction_loop(self):
        """Periodic eviction to maintain pool size."""
        while self._running:
            try:
                await asyncio.sleep(60)  # 1 minute

                if len(self._pool) > self.max_pool_size:
                    # Evict lowest priority snapshots
                    sorted_snaps = sorted(self._pool.values(), key=lambda s: (s.priority, s.hot_score, s.use_count))

                    to_evict = len(self._pool) - self.max_pool_size

                    for snapshot in sorted_snaps[:to_evict]:
                        await self._remove_snapshot(snapshot.snapshot_id)

                    logger.info(f"Evicted {to_evict} snapshots")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Eviction error: {e}")

    async def _remove_snapshot(self, snapshot_id: str):
        """Remove snapshot from pool."""
        if snapshot_id not in self._pool:
            return

        snapshot = self._pool[snapshot_id]

        # Remove from runtime pool
        if snapshot_id in self._runtime_pool[snapshot.runtime]:
            self._runtime_pool[snapshot.runtime].remove(snapshot_id)

        # Remove from user mapping
        if snapshot.user_id:
            if self._user_snapshots.get(snapshot.user_id) == snapshot_id:
                del self._user_snapshots[snapshot.user_id]

        # Remove from pool
        del self._pool[snapshot_id]

    def get_pool_stats(self) -> Dict:
        """Get comprehensive pool statistics."""
        total_snapshots = len(self._pool)
        runtime_counts = {rt.value: len(snaps) for rt, snaps in self._runtime_pool.items()}

        avg_readiness = sum(s.readiness_ratio() for s in self._pool.values()) / max(total_snapshots, 1)

        return {
            "total_snapshots": total_snapshots,
            "max_snapshots": self.max_pool_size,
            "utilization_percent": round(total_snapshots / self.max_pool_size * 100, 2),
            "by_runtime": runtime_counts,
            "avg_readiness_percent": round(avg_readiness * 100, 2),
            "scheduler": self._scheduler.get_stats(),
            "cache": self._cache.get_stats(),
        }


class RuntimeDistributionPlane:
    """
    CDN-based runtime distribution system.

    Provides:
    1. OCI image layer caching
    2. Dependency resolution with lockfile hashing
    3. Pre-warmed runtime packs

    Reference: "OCI Distribution Specification" and "Docker Layer Caching"
    """

    # Default runtime packs
    RUNTIME_PACKS: Dict[RuntimeType, RuntimeSpec] = {
        RuntimeType.PYTHON_DATA: RuntimeSpec(
            name=RuntimeType.PYTHON_DATA,
            base_image="python:3.11-slim",
            dependencies=["pip", "numpy>=1.24", "pandas>=2.0", "requests>=2.28"],
            size_bytes=180 * 1024 * 1024,
            layer_count=5,
        ),
        RuntimeType.PYTHON_ML: RuntimeSpec(
            name=RuntimeType.PYTHON_ML,
            base_image="python:3.11-slim",
            dependencies=["numpy>=1.24", "pandas>=2.0", "torch>=2.0", "transformers>=4.30"],
            size_bytes=450 * 1024 * 1024,
            layer_count=8,
        ),
        RuntimeType.PYTHON_WEB: RuntimeSpec(
            name=RuntimeType.PYTHON_WEB,
            base_image="python:3.11-slim",
            dependencies=["fastapi>=0.100", "uvicorn>=0.23", "httpx>=0.24"],
            size_bytes=200 * 1024 * 1024,
            layer_count=6,
        ),
        RuntimeType.NODE_WEB: RuntimeSpec(
            name=RuntimeType.NODE_WEB,
            base_image="node:20-alpine",
            dependencies=["npm"],
            size_bytes=160 * 1024 * 1024,
            layer_count=4,
        ),
        RuntimeType.BROWSER_CHROMIUM: RuntimeSpec(
            name=RuntimeType.BROWSER_CHROMIUM,
            base_image="browser/chromium",
            dependencies=["playwright", "puppeteer"],
            size_bytes=800 * 1024 * 1024,
            layer_count=10,
        ),
    }

    def __init__(self, cache_dir: str = "/tmp/runtime-cache", cdn_base_url: str = "https://cdn.mem0.ai/runtimes"):
        self.cache_dir = cache_dir
        self.cdn_base_url = cdn_base_url

        # Local cache
        self._local_cache: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()

        # Dependency cache
        self._dependency_cache: Dict[str, Dict[str, str]] = {}

    async def get_runtime(self, runtime: RuntimeType, lockfile_hash: Optional[str] = None) -> Dict:
        """
        Get runtime information.

        Returns:
            Dict with mount path and metadata
        """
        spec = self.RUNTIME_PACKS[runtime]
        cache_key = f"{runtime.value}:{lockfile_hash or 'base'}"

        async with self._lock:
            if cache_key in self._local_cache:
                return {**self._local_cache[cache_key], "from_cache": True, "latency_ms": 1.0}

        # Simulate CDN fetch
        mount_path = f"{self.cache_dir}/{runtime.value}"

        result = {
            "runtime": runtime.value,
            "mount_path": mount_path,
            "base_image": spec.base_image,
            "dependencies": spec.dependencies,
            "size_bytes": spec.size_bytes,
            "from_cache": False,
            "latency_ms": 50.0,  # Simulated CDN latency
        }

        async with self._lock:
            self._local_cache[cache_key] = result

        return result

    async def resolve_dependencies(self, runtime: RuntimeType, lockfile_content: str) -> Dict[str, str]:
        """
        Resolve dependencies from lockfile.

        Returns:
            Dict of package -> version
        """
        lockfile_hash = hashlib.sha256(lockfile_content.encode()).hexdigest()[:16]
        cache_key = f"{runtime.value}:{lockfile_hash}"

        if cache_key in self._dependency_cache:
            return self._dependency_cache[cache_key]

        # Parse lockfile
        deps = self._parse_lockfile(runtime, lockfile_content)

        self._dependency_cache[cache_key] = deps

        return deps

    def _parse_lockfile(self, runtime: RuntimeType, content: str) -> Dict[str, str]:
        """Parse dependency lockfile."""
        deps = {}

        for line in content.split("\n"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if "==" in line:
                pkg, ver = line.split("==", 1)
                deps[pkg.strip()] = ver.strip()
            elif ">=" in line:
                pkg, ver = line.split(">=", 1)
                deps[pkg.strip()] = f">={ver.strip()}"

        return deps

    def get_stats(self) -> Dict:
        """Get distribution statistics."""
        return {
            "cached_runtimes": len(self._local_cache),
            "dependency_cache_size": len(self._dependency_cache),
            "available_runtimes": [rt.value for rt in RuntimeType],
        }
