"""
Mem0 Prometheus Metrics
======================

Production-ready Prometheus metrics for Mem0 Platform.
Provides comprehensive observability for sandbox operations,
API keys, storage, and system metrics.

Key Features:
- Counter metrics for operations (fork, clone, snapshot, etc.)
- Histogram metrics for latency measurements
- Gauge metrics for current state (active sandboxes, pool size)
- Automatic label dimensioning (runtime, tier, status)
- Thread-safe concurrent updates
- HTTP metrics endpoint (Prometheus compatible)

Usage:
    from src.monitoring.metrics import Mem0Metrics, get_metrics

    metrics = Mem0Metrics()
    metrics.record_fork(latency_ms=0.5, runtime="python-ml")
    metrics.record_snapshot(size_bytes=1024, success=True)

    # Expose metrics endpoint
    from prometheus_client import make_asgi_app
    app = make_asgi_app(metrics.registry)
"""

import asyncio
import gc
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple
from uuid import uuid4

try:
    from prometheus_client import (
        Counter,
        Histogram,
        Gauge,
        REGISTRY,
        CollectorRegistry,
        generate_latest,
        CONTENT_TYPE_LATEST,
        start_http_server,
    )

    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    Counter = None
    Histogram = None
    Gauge = None
    REGISTRY = None
    CollectorRegistry = None
    generate_latest = None
    CONTENT_TYPE_LATEST = None
    start_http_server = None


logger = logging.getLogger(__name__)


class MetricType(str, Enum):
    """Types of Prometheus metrics."""

    COUNTER = "counter"
    HISTOGRAM = "histogram"
    GAUGE = "gauge"
    SUMMARY = "summary"


@dataclass
class MetricConfig:
    """Configuration for a single metric."""

    name: str
    metric_type: MetricType
    description: str
    labels: List[str] = field(default_factory=list)
    buckets: Optional[List[float]] = None


class MetricsConfig:
    """Configuration for all metrics."""

    def __init__(
        self,
        namespace: str = "mem0",
        subsystem: str = "platform",
        include_process_metrics: bool = True,
        include_python_metrics: bool = True,
    ):
        self.namespace = namespace
        self.subsystem = subsystem
        self.include_process_metrics = include_process_metrics
        self.include_python_metrics = include_python_metrics

        self._metric_configs: List[MetricConfig] = []
        self._register_default_metrics()

    def _register_default_metrics(self):
        """Register default Mem0 metrics."""
        self._metric_configs.extend(
            [
                MetricConfig(
                    name="sandbox_created_total",
                    metric_type=MetricType.COUNTER,
                    description="Total number of sandboxes created",
                    labels=["runtime", "tier"],
                ),
                MetricConfig(
                    name="sandbox_fork_total",
                    metric_type=MetricType.COUNTER,
                    description="Total number of sandbox forks",
                    labels=["runtime", "status"],
                ),
                MetricConfig(
                    name="sandbox_clone_total",
                    metric_type=MetricType.COUNTER,
                    description="Total number of sandbox clones",
                    labels=["runtime", "status"],
                ),
                MetricConfig(
                    name="sandbox_delete_total",
                    metric_type=MetricType.COUNTER,
                    description="Total number of sandboxes deleted",
                    labels=["runtime"],
                ),
                MetricConfig(
                    name="sandbox_active",
                    metric_type=MetricType.GAUGE,
                    description="Current number of active sandboxes",
                    labels=["runtime", "user_id"],
                ),
                MetricConfig(
                    name="snapshot_created_total",
                    metric_type=MetricType.COUNTER,
                    description="Total number of snapshots created",
                    labels=["user_id", "incremental"],
                ),
                MetricConfig(
                    name="snapshot_restored_total",
                    metric_type=MetricType.COUNTER,
                    description="Total number of snapshots restored",
                    labels=["user_id", "source"],
                ),
                MetricConfig(
                    name="snapshot_size_bytes",
                    metric_type=MetricType.HISTOGRAM,
                    description="Size of snapshots in bytes",
                    labels=["user_id"],
                    buckets=[1024, 10240, 102400, 1048576, 10485760, 104857600],
                ),
                MetricConfig(
                    name="fork_latency_seconds",
                    metric_type=MetricType.HISTOGRAM,
                    description="Fork operation latency in seconds",
                    labels=["runtime"],
                    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
                ),
                MetricConfig(
                    name="clone_latency_seconds",
                    metric_type=MetricType.HISTOGRAM,
                    description="Clone operation latency in seconds",
                    labels=["runtime"],
                    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0],
                ),
                MetricConfig(
                    name="snapshot_latency_seconds",
                    metric_type=MetricType.HISTOGRAM,
                    description="Snapshot creation latency in seconds",
                    labels=["user_id"],
                    buckets=[0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0],
                ),
                MetricConfig(
                    name="restore_latency_seconds",
                    metric_type=MetricType.HISTOGRAM,
                    description="Snapshot restore latency in seconds",
                    labels=["user_id"],
                    buckets=[0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0],
                ),
                MetricConfig(
                    name="block_stored_total",
                    metric_type=MetricType.COUNTER,
                    description="Total number of blocks stored",
                    labels=["compression", "tier"],
                ),
                MetricConfig(
                    name="block_size_bytes",
                    metric_type=MetricType.HISTOGRAM,
                    description="Size of blocks in bytes",
                    labels=["compression"],
                    buckets=[1024, 8192, 65536, 262144, 1048576, 4194304],
                ),
                MetricConfig(
                    name="compression_ratio",
                    metric_type=MetricType.HISTOGRAM,
                    description="Compression ratio achieved",
                    labels=["compression"],
                    buckets=[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0],
                ),
                MetricConfig(
                    name="cache_hit_total",
                    metric_type=MetricType.COUNTER,
                    description="Total cache hits",
                    labels=["cache_type"],
                ),
                MetricConfig(
                    name="cache_miss_total",
                    metric_type=MetricType.COUNTER,
                    description="Total cache misses",
                    labels=["cache_type"],
                ),
                MetricConfig(
                    name="cache_size_bytes",
                    metric_type=MetricType.GAUGE,
                    description="Current cache size in bytes",
                    labels=["cache_type"],
                ),
                MetricConfig(
                    name="api_key_created_total",
                    metric_type=MetricType.COUNTER,
                    description="Total API keys created",
                    labels=["tier", "key_type"],
                ),
                MetricConfig(
                    name="api_key_revoked_total",
                    metric_type=MetricType.COUNTER,
                    description="Total API keys revoked",
                    labels=["tier", "reason"],
                ),
                MetricConfig(
                    name="api_request_total",
                    metric_type=MetricType.COUNTER,
                    description="Total API requests",
                    labels=["endpoint", "method", "status_code", "tier"],
                ),
                MetricConfig(
                    name="api_request_latency_seconds",
                    metric_type=MetricType.HISTOGRAM,
                    description="API request latency in seconds",
                    labels=["endpoint", "method"],
                    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
                ),
                MetricConfig(
                    name="rate_limit_exceeded_total",
                    metric_type=MetricType.COUNTER,
                    description="Total rate limit exceeded events",
                    labels=["tier", "endpoint"],
                ),
                MetricConfig(
                    name="warm_pool_size",
                    metric_type=MetricType.GAUGE,
                    description="Current warm pool size",
                    labels=["runtime"],
                ),
                MetricConfig(
                    name="warm_pool_request_total",
                    metric_type=MetricType.COUNTER,
                    description="Total warm pool requests",
                    labels=["runtime", "status"],
                ),
                MetricConfig(
                    name="prefetch_total",
                    metric_type=MetricType.COUNTER,
                    description="Total prefetch operations",
                    labels=["runtime", "status"],
                ),
                MetricConfig(
                    name="runtime_download_total",
                    metric_type=MetricType.COUNTER,
                    description="Total runtime downloads",
                    labels=["runtime", "source"],
                ),
                MetricConfig(
                    name="runtime_size_bytes",
                    metric_type=MetricType.HISTOGRAM,
                    description="Runtime image sizes in bytes",
                    labels=["runtime"],
                    buckets=[1048576, 10485760, 52428800, 104857600, 262144000],
                ),
            ]
        )


class Mem0Metrics:
    """
    Mem0 Platform Metrics Collector.

    Provides thread-safe metrics collection with Prometheus integration.

    Usage:
        metrics = Mem0Metrics()

        # Record operations
        metrics.record_fork(runtime="python-ml", latency_ms=0.5)
        metrics.record_snapshot(size_bytes=1024, user_id="user123")

        # Get metrics for Prometheus
        metrics_output = metrics.get_metrics()

        # Start HTTP server
        metrics.start_http_server(port=9090)
    """

    def __init__(
        self,
        config: Optional[MetricsConfig] = None,
        registry: Optional[CollectorRegistry] = None,
    ):
        if not PROMETHEUS_AVAILABLE:
            logger.warning("prometheus_client not installed. Metrics will be disabled.")

        self.config = config or MetricsConfig()
        self._lock = threading.RLock()

        if registry:
            self._registry = registry
        else:
            self._registry = REGISTRY

        self._metrics: Dict[str, Any] = {}
        self._initialized = False
        self._start_time = time.time()

        self._initialize_metrics()

    def _initialize_metrics(self):
        """Initialize all metrics in the registry."""
        if self._initialized:
            return

        with self._lock:
            for cfg in self.config._metric_configs:
                full_name = f"{self.config.namespace}_{self.config.subsystem}_{cfg.name}"

                if cfg.metric_type == MetricType.COUNTER:
                    self._metrics[full_name] = Counter(
                        full_name,
                        cfg.description,
                        cfg.labels,
                        registry=self._registry,
                    )
                elif cfg.metric_type == MetricType.HISTOGRAM:
                    buckets = cfg.buckets or Histogram.DEFAULT_BUCKETS
                    self._metrics[full_name] = Histogram(
                        full_name,
                        cfg.description,
                        cfg.labels,
                        buckets=buckets,
                        registry=self._registry,
                    )
                elif cfg.metric_type == MetricType.GAUGE:
                    self._metrics[full_name] = Gauge(
                        full_name,
                        cfg.description,
                        cfg.labels,
                        registry=self._registry,
                    )

            self._initialized = True

    def _get_metric(self, name: str) -> Any:
        """Get a metric by name."""
        full_name = f"{self.config.namespace}_{self.config.subsystem}_{name}"
        return self._metrics.get(full_name)

    def record_fork(self, latency_ms: float, runtime: str, status: str = "success"):
        """Record a fork operation."""
        if not PROMETHEUS_AVAILABLE:
            return

        latency_seconds = latency_ms / 1000.0

        fork_latency = self._get_metric("fork_latency_seconds")
        if fork_latency:
            fork_latency.labels(runtime=runtime).observe(latency_seconds)

        fork_total = self._get_metric("sandbox_fork_total")
        if fork_total:
            fork_total.labels(runtime=runtime, status=status).inc()

    def record_clone(self, latency_ms: float, runtime: str, status: str = "success"):
        """Record a clone operation."""
        if not PROMETHEUS_AVAILABLE:
            return

        latency_seconds = latency_ms / 1000.0

        clone_latency = self._get_metric("clone_latency_seconds")
        if clone_latency:
            clone_latency.labels(runtime=runtime).observe(latency_seconds)

        clone_total = self._get_metric("sandbox_clone_total")
        if clone_total:
            clone_total.labels(runtime=runtime, status=status).inc()

    def record_snapshot(
        self,
        size_bytes: int,
        user_id: str,
        incremental: bool = False,
        success: bool = True,
        latency_ms: Optional[float] = None,
    ):
        """Record a snapshot creation."""
        if not PROMETHEUS_AVAILABLE:
            return

        snapshot_size = self._get_metric("snapshot_size_bytes")
        if snapshot_size:
            snapshot_size.labels(user_id=user_id).observe(size_bytes)

        snapshot_total = self._get_metric("snapshot_created_total")
        if snapshot_total:
            snapshot_total.labels(user_id=user_id, incremental=str(incremental)).inc()

        if latency_ms:
            latency_seconds = latency_ms / 1000.0
            snapshot_latency = self._get_metric("snapshot_latency_seconds")
            if snapshot_latency:
                snapshot_latency.labels(user_id=user_id).observe(latency_seconds)

    def record_restore(
        self,
        size_bytes: int,
        user_id: str,
        source: str = "local",
        latency_ms: Optional[float] = None,
    ):
        """Record a snapshot restore."""
        if not PROMETHEUS_AVAILABLE:
            return

        restore_total = self._get_metric("snapshot_restored_total")
        if restore_total:
            restore_total.labels(user_id=user_id, source=source).inc()

        if latency_ms:
            latency_seconds = latency_ms / 100.0
            restore_latency = self._get_metric("restore_latency_seconds")
            if restore_latency:
                restore_latency.labels(user_id=user_id).observe(latency_seconds)

    def record_sandbox_created(self, runtime: str, tier: str = "free"):
        """Record sandbox creation."""
        if not PROMETHEUS_AVAILABLE:
            return

        created = self._get_metric("sandbox_created_total")
        if created:
            created.labels(runtime=runtime, tier=tier).inc()

    def record_sandbox_deleted(self, runtime: str):
        """Record sandbox deletion."""
        if not PROMETHEUS_AVAILABLE:
            return

        deleted = self._get_metric("sandbox_delete_total")
        if deleted:
            deleted.labels(runtime=runtime).inc()

    def set_active_sandboxes(self, count: int, runtime: str, user_id: str):
        """Set current number of active sandboxes."""
        if not PROMETHEUS_AVAILABLE:
            return

        active = self._get_metric("sandbox_active")
        if active:
            active.labels(runtime=runtime, user_id=user_id).set(count)

    def record_block_stored(
        self,
        size_bytes: int,
        compression: str = "lz4",
        tier: str = "hot",
    ):
        """Record a block being stored."""
        if not PROMETHEUS_AVAILABLE:
            return

        block_total = self._get_metric("block_stored_total")
        if block_total:
            block_total.labels(compression=compression, tier=tier).inc()

        block_size = self._get_metric("block_size_bytes")
        if block_size:
            block_size.labels(compression=compression).observe(size_bytes)

    def record_compression_ratio(self, ratio: float, compression: str = "lz4"):
        """Record compression ratio."""
        if not PROMETHEUS_AVAILABLE:
            return

        hist = self._get_metric("compression_ratio")
        if hist:
            hist.labels(compression=compression).observe(ratio)

    def record_cache_hit(self, cache_type: str = "local"):
        """Record cache hit."""
        if not PROMETHEUS_AVAILABLE:
            return

        hit = self._get_metric("cache_hit_total")
        if hit:
            hit.labels(cache_type=cache_type).inc()

    def record_cache_miss(self, cache_type: str = "local"):
        """Record cache miss."""
        if not PROMETHEUS_AVAILABLE:
            return

        miss = self._get_metric("cache_miss_total")
        if miss:
            miss.labels(cache_type=cache_type).inc()

    def set_cache_size(self, size_bytes: int, cache_type: str = "local"):
        """Set current cache size."""
        if not PROMETHEUS_AVAILABLE:
            return

        size = self._get_metric("cache_size_bytes")
        if size:
            size.labels(cache_type=cache_type).set(size_bytes)

    def record_api_key_created(self, tier: str, key_type: str):
        """Record API key creation."""
        if not PROMETHEUS_AVAILABLE:
            return

        created = self._get_metric("api_key_created_total")
        if created:
            created.labels(tier=tier, key_type=key_type).inc()

    def record_api_key_revoked(self, tier: str, reason: str = "manual"):
        """Record API key revocation."""
        if not PROMETHEUS_AVAILABLE:
            return

        revoked = self._get_metric("api_key_revoked_total")
        if revoked:
            revoked.labels(tier=tier, reason=reason).inc()

    def record_api_request(
        self,
        endpoint: str,
        method: str,
        status_code: int,
        tier: str,
        latency_ms: float,
    ):
        """Record API request."""
        if not PROMETHEUS_AVAILABLE:
            return

        request_total = self._get_metric("api_request_total")
        if request_total:
            request_total.labels(endpoint=endpoint, method=method, status_code=str(status_code), tier=tier).inc()

        latency_seconds = latency_ms / 1000.0
        latency = self._get_metric("api_request_latency_seconds")
        if latency:
            latency.labels(endpoint=endpoint, method=method).observe(latency_seconds)

    def record_rate_limit_exceeded(self, tier: str, endpoint: str):
        """Record rate limit exceeded."""
        if not PROMETHEUS_AVAILABLE:
            return

        exceeded = self._get_metric("rate_limit_exceeded_total")
        if exceeded:
            exceeded.labels(tier=tier, endpoint=endpoint).inc()

    def set_warm_pool_size(self, size: int, runtime: str):
        """Set current warm pool size."""
        if not PROMETHEUS_AVAILABLE:
            return

        pool = self._get_metric("warm_pool_size")
        if pool:
            pool.labels(runtime=runtime).set(size)

    def record_warm_pool_request(self, runtime: str, status: str = "hit"):
        """Record warm pool request."""
        if not PROMETHEUS_AVAILABLE:
            return

        request = self._get_metric("warm_pool_request_total")
        if request:
            request.labels(runtime=runtime, status=status).inc()

    def record_prefetch(self, runtime: str, status: str = "success"):
        """Record prefetch operation."""
        if not PROMETHEUS_AVAILABLE:
            return

        prefetch = self._get_metric("prefetch_total")
        if prefetch:
            prefetch.labels(runtime=runtime, status=status).inc()

    def record_runtime_download(self, runtime: str, source: str = "cache"):
        """Record runtime download."""
        if not PROMETHEUS_AVAILABLE:
            return

        download = self._get_metric("runtime_download_total")
        if download:
            download.labels(runtime=runtime, source=source).inc()

    def set_runtime_size(self, size_bytes: int, runtime: str):
        """Set runtime image size."""
        if not PROMETHEUS_AVAILABLE:
            return

        size = self._get_metric("runtime_size_bytes")
        if size:
            size.labels(runtime=runtime).observe(size_bytes)

    def record_block_deduplication(self, original_bytes: int, deduplicated_bytes: int):
        """Record block deduplication savings."""
        if not PROMETHEUS_AVAILABLE:
            return

        saved = original_bytes - deduplicated_bytes
        ratio = original_bytes / max(deduplicated_bytes, 1)

        self.record_compression_ratio(ratio)

    def get_metrics(self) -> bytes:
        """Get metrics in Prometheus format."""
        if not PROMETHEUS_AVAILABLE:
            return b"# Prometheus client not installed"

        return generate_latest(self._registry)

    def get_content_type(self) -> str:
        """Get content type for metrics."""
        return CONTENT_TYPE_LATEST

    def start_http_server(self, port: int = 9090, addr: str = ""):
        """Start Prometheus HTTP metrics server."""
        if not PROMETHEUS_AVAILABLE:
            logger.warning("Cannot start HTTP server: prometheus_client not installed")
            return

        start_http_server(port, addr=addr)
        logger.info(f"Prometheus metrics server started on port {port}")

    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary for reporting."""
        if not PROMETHEUS_AVAILABLE:
            return {"error": "prometheus_client not installed"}

        summary = {
            "uptime_seconds": time.time() - self._start_time,
            "metrics_count": len(self._metrics),
        }

        for name, metric in self._metrics.items():
            if isinstance(metric, Gauge):
                try:
                    summary[f"{name}_value"] = metric._value.get()
                except Exception:
                    pass

        return summary


class MetricsMiddleware:
    """ASGI middleware for recording HTTP metrics."""

    def __init__(self, app, metrics: Mem0Metrics):
        self.app = app
        self.metrics = metrics

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "GET")
        path = scope.get("path", "/")

        endpoint = self._normalize_path(path)

        async def wrapped_send(message):
            if message["type"] == "http.response.start":
                status_code = message.get("status", 200)
                tier = self._get_tier(scope)
                self.metrics.record_api_request(
                    endpoint=endpoint,
                    method=method,
                    status_code=status_code,
                    tier=tier,
                    latency_ms=0,
                )
            await send(message)

        await self.app(scope, receive, wrapped_send)

    def _normalize_path(self, path: str) -> str:
        """Normalize path for grouping (remove IDs)."""
        import re

        return re.sub(r"/[a-f0-9-]{36}", "/{id}", path)

    def _get_tier(self, scope: dict) -> str:
        """Get tier from scope (usually from headers or auth)."""
        return "unknown"


def track_operation(metric_name: str, labels: Optional[Dict[str, str]] = None):
    """Decorator to track operation latency."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                latency_ms = (time.perf_counter() - start) * 1000

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                latency_ms = (time.perf_counter() - start) * 1000

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


class MetricsCollector:
    """
    Background metrics collector for Mem0.

    Collects system metrics and exposes them periodically.
    """

    def __init__(self, metrics: Mem0Metrics, interval: float = 15.0):
        self.metrics = metrics
        self.interval = interval
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        """Start the background collector."""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._collect_loop())
        logger.info("Metrics collector started")

    async def stop(self):
        """Stop the background collector."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Metrics collector stopped")

    async def _collect_loop(self):
        """Background collection loop."""
        while self._running:
            try:
                self._collect_system_metrics()
            except Exception as e:
                logger.warning(f"Failed to collect system metrics: {e}")

            await asyncio.sleep(self.interval)

    def _collect_system_metrics(self):
        """Collect system-level metrics."""
        if not PROMETHEUS_AVAILABLE:
            return

        process = self._get_process_metrics()

        gc_stats = gc.get_stats()
        self.metrics.set_cache_size(
            sum(s.get("collected", 0) + s.get("uncollectable", 0) for s in gc_stats),
            cache_type="gc",
        )

    def _get_process_metrics(self) -> Dict[str, float]:
        """Get process-level metrics."""
        try:
            import psutil

            process = psutil.Process()
            return {
                "memory_bytes": process.memory_info().rss,
                "cpu_percent": process.cpu_percent(),
                "open_files": len(process.open_files()),
                "threads": process.num_threads(),
            }
        except ImportError:
            return {}


_global_metrics: Optional[Mem0Metrics] = None


def get_metrics() -> Mem0Metrics:
    """Get or create global metrics instance."""
    global _global_metrics
    if _global_metrics is None:
        _global_metrics = Mem0Metrics()
    return _global_metrics


def init_metrics(
    namespace: str = "mem0",
    subsystem: str = "platform",
) -> Mem0Metrics:
    """Initialize global metrics."""
    global _global_metrics
    config = MetricsConfig(namespace=namespace, subsystem=subsystem)
    _global_metrics = Mem0Metrics(config=config)
    return _global_metrics


__all__ = [
    "Mem0Metrics",
    "MetricsConfig",
    "MetricsMiddleware",
    "MetricsCollector",
    "track_operation",
    "get_metrics",
    "init_metrics",
    "PROMETHEUS_AVAILABLE",
]
