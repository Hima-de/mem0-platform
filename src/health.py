"""Health Checks - Comprehensive System Health Monitoring."""

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable
from pathlib import Path


class HealthStatus(str, Enum):
    """Health check statuses."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """Result of a health check."""

    name: str
    status: HealthStatus
    message: str
    latency_ms: float
    details: Dict[str, Any] = None
    timestamp: str = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow().isoformat()
        if self.details is None:
            self.details = {}


class HealthChecker:
    """Comprehensive health checker for all subsystems."""

    def __init__(self):
        self._checks: Dict[str, Callable] = {}
        self._last_results: Dict[str, HealthCheckResult] = {}
        self._start_time = time.time()

    def register_check(self, name: str, check_func: Callable) -> None:
        """Register a health check function."""
        self._checks[name] = check_func

    async def run_check(self, name: str) -> HealthCheckResult:
        """Run a single health check."""
        if name not in self._checks:
            return HealthCheckResult(
                name=name,
                status=HealthStatus.UNKNOWN,
                message=f"Check '{name}' not registered",
                latency_ms=0,
            )

        start = time.time()
        try:
            if asyncio.iscoroutinefunction(self._checks[name]):
                result = await self._checks[name]()
            else:
                result = self._checks[name]()

            latency_ms = (time.time() - start) * 1000

            if isinstance(result, dict):
                return HealthCheckResult(
                    name=name,
                    status=HealthStatus(result.get("status", "healthy")),
                    message=result.get("message", ""),
                    latency_ms=latency_ms,
                    details=result.get("details", {}),
                )
            elif isinstance(result, HealthCheckResult):
                result.latency_ms = latency_ms
                return result
            else:
                return HealthCheckResult(
                    name=name,
                    status=HealthStatus.HEALTHY if result else HealthStatus.UNHEALTHY,
                    message="Check completed",
                    latency_ms=latency_ms,
                )
        except Exception as e:
            latency_ms = (time.time() - start) * 1000
            return HealthCheckResult(
                name=name,
                status=HealthStatus.UNHEALTHY,
                message=str(e),
                latency_ms=latency_ms,
                details={"error": type(e).__name__},
            )

    async def run_all_checks(self) -> Dict[str, HealthCheckResult]:
        """Run all registered health checks."""
        results = {}
        for name in self._checks:
            results[name] = await self.run_check(name)
        self._last_results = results
        return results

    def get_overall_status(self) -> HealthStatus:
        """Get overall system health status."""
        if not self._last_results:
            return HealthStatus.UNKNOWN

        statuses = [r.status for r in self._last_results.values()]

        if any(s == HealthStatus.UNHEALTHY for s in statuses):
            return HealthStatus.UNHEALTHY
        elif any(s == HealthStatus.DEGRADED for s in statuses):
            return HealthStatus.DEGRADED
        return HealthStatus.HEALTHY

    def get_summary(self) -> Dict:
        """Get health check summary."""
        results = self._last_results

        overall = self.get_overall_status()

        return {
            "status": overall.value,
            "uptime_seconds": time.time() - self._start_time,
            "checks": {
                name: {
                    "status": result.status.value,
                    "message": result.message,
                    "latency_ms": round(result.latency_ms, 2),
                }
                for name, result in results.items()
            },
            "summary": {
                "total": len(results),
                "healthy": sum(1 for r in results.values() if r.status == HealthStatus.HEALTHY),
                "degraded": sum(1 for r in results.values() if r.status == HealthStatus.DEGRADED),
                "unhealthy": sum(1 for r in results.values() if r.status == HealthStatus.UNHEALTHY),
            },
            "timestamp": datetime.utcnow().isoformat(),
        }


# Pre-configured health checks
health_checker = HealthChecker()


def create_memory_check(get_stats: Callable) -> Callable:
    """Create memory health check."""

    async def check() -> Dict:
        try:
            stats = get_stats()
            memories = stats.get("memories", 0)

            if memories < 0:
                return {
                    "status": "unhealthy",
                    "message": "Invalid memory count",
                    "details": stats,
                }

            return {
                "status": "healthy",
                "message": f"Memory subsystem operational with {memories} memories",
                "details": stats,
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "message": str(e),
            }

    return check


def create_storage_check(get_stats: Callable) -> Callable:
    """Create storage health check."""

    async def check() -> Dict:
        try:
            stats = get_stats()
            blocks = stats.get("blocks", {}).get("count", 0)
            snapshots = stats.get("snapshots", {}).get("count", 0)

            return {
                "status": "healthy",
                "message": f"Storage operational: {blocks} blocks, {snapshots} snapshots",
                "details": stats,
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "message": str(e),
            }

    return check


def create_runtime_check(get_stats: Callable) -> Callable:
    """Create runtime health check."""

    async def check() -> Dict:
        try:
            stats = get_stats()
            packs = stats.get("packs_registered", 0)
            hot_layers = stats.get("hot_layers", 0)

            return {
                "status": "healthy",
                "message": f"Runtime operational: {packs} packs, {hot_layers} hot layers",
                "details": stats,
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "message": str(e),
            }

    return check


# Liveness/readiness probes
class ProbeServer:
    """Kubernetes probe server."""

    def __init__(self, health_checker: HealthChecker):
        self.health_checker = health_checker

    async def liveness_probe(self) -> Dict:
        """Kubernetes liveness probe."""
        return {"status": "alive"}

    async def readiness_probe(self) -> Dict:
        """Kubernetes readiness probe."""
        status = self.health_checker.get_overall_status()
        if status == HealthStatus.UNHEALTHY:
            return {
                "status": "not_ready",
                "reason": "Unhealthy components detected",
            }
        return {"status": "ready"}


# Health check endpoints for FastAPI
def setup_health_checks(app, health_checker: HealthChecker):
    """Setup health check endpoints."""

    @app.get("/health")
    async def health():
        """Main health endpoint."""
        return await health_checker.run_all_checks()

    @app.get("/health/live")
    async def liveness():
        """Kubernetes liveness probe."""
        return {"status": "alive"}

    @app.get("/health/ready")
    async def readiness():
        """Kubernetes readiness probe."""
        status = health_checker.get_overall_status()
        if status == HealthStatus.UNHEALTHY:
            return {"status": "not_ready"}, 503
        return {"status": "ready"}

    @app.get("/health/stats")
    async def stats():
        """Detailed health statistics."""
        return health_checker.get_summary()
