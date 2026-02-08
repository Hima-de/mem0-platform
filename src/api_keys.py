"""
API Key Management System for Mem0 Platform
===========================================

Secure API key generation, validation, and management
with comprehensive usage tracking and analytics.

Features:
- Cryptographically secure key generation (SHA-256 + HMAC)
- Key prefix identification (live/test/scoped)
- Token bucket rate limiting
- Comprehensive usage tracking per key
- Endpoint-level analytics
- Daily/hourly usage breakdown
- Organization-level aggregation
- Automatic rotation/reminder
- Audit logging
"""

import hashlib
import hmac
import secrets
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from statistics import mean, median
import threading


class KeyType(Enum):
    """Types of API keys."""

    LIVE = "live"
    TEST = "test"
    SCOPED = "scoped"
    SERVICE = "service"


class KeyPermission(Enum):
    """Permissions for API keys."""

    READ = "read"
    WRITE = "write"
    EXECUTE = "execute"
    ADMIN = "admin"
    MEMORY_READ = "memory:read"
    MEMORY_WRITE = "memory:write"
    SANDBOX_CREATE = "sandbox:create"
    SANDBOX_EXECUTE = "sandbox:execute"


@dataclass
class APIKey:
    """Represents an API key with metadata."""

    key_id: str
    key_hash: str
    key_prefix: str
    key_type: KeyType
    organization_id: str
    user_id: Optional[str]
    permissions: List[KeyPermission]
    created_at: datetime
    expires_at: Optional[datetime]
    last_used_at: Optional[datetime]
    usage_count: int = 0
    rate_limit_per_minute: int = 1000
    description: str = ""
    is_active: bool = True

    @property
    def is_expired(self) -> bool:
        return self.expires_at and datetime.utcnow() > self.expires_at

    @property
    def is_valid(self) -> bool:
        return self.is_active and not self.is_expired


@dataclass
class UsageRecord:
    """Records individual API usage."""

    key_id: str
    organization_id: str
    endpoint: str
    method: str
    timestamp: datetime
    latency_ms: float
    status_code: int
    request_size: int
    response_size: int
    error_type: Optional[str] = None


@dataclass
class EndpointStats:
    """Statistics for an endpoint."""

    endpoint: str
    method: str
    total_requests: int = 0
    success_count: int = 0
    error_count: int = 0
    total_latency_ms: float = 0.0
    min_latency_ms: float = float("inf")
    max_latency_ms: float = 0.0
    total_bytes_in: int = 0
    total_bytes_out: int = 0


@dataclass
class DailyUsage:
    """Daily usage summary."""

    date: str  # YYYY-MM-DD
    total_requests: int = 0
    success_requests: int = 0
    error_requests: int = 0
    total_latency_ms: float = 0.0
    avg_latency_ms: float = 0.0
    total_bytes_in: int = 0
    total_bytes_out: int = 0
    unique_endpoints: int = 0


@dataclass
class HourlyUsage:
    """Hourly usage breakdown."""

    hour: int  # 0-23
    total_requests: int = 0
    avg_latency_ms: float = 0.0


class UsageAnalytics:
    """
    Comprehensive usage analytics for API keys.

    Tracks:
    - Per-key usage
    - Per-endpoint statistics
    - Daily/hourly breakdowns
    - Error analysis
    - Latency distributions
    """

    def __init__(self):
        self._records: List[UsageRecord] = []
        self._endpoint_stats: Dict[Tuple[str, str], EndpointStats] = {}
        self._daily_usage: Dict[str, DailyUsage] = {}
        self._hourly_usage: Dict[str, List[HourlyUsage]] = defaultdict(list)
        self._key_usage: Dict[str, int] = defaultdict(int)
        self._org_usage: Dict[str, int] = defaultdict(int)
        self._error_counts: Dict[str, int] = defaultdict(int)
        self._lock = threading.RLock()

    def record(
        self,
        key_id: str,
        organization_id: str,
        endpoint: str,
        method: str,
        latency_ms: float,
        status_code: int,
        request_size: int,
        response_size: int,
        error_type: Optional[str] = None,
    ):
        """Record a usage event."""
        with self._lock:
            timestamp = datetime.utcnow()
            date_key = timestamp.strftime("%Y-%m-%d")
            hour_key = timestamp.strftime("%Y-%m-%d:%H")

            # Create record
            record = UsageRecord(
                key_id=key_id,
                organization_id=organization_id,
                endpoint=endpoint,
                method=method,
                timestamp=timestamp,
                latency_ms=latency_ms,
                status_code=status_code,
                request_size=request_size,
                response_size=response_size,
                error_type=error_type,
            )
            self._records.append(record)

            # Keep only last 100000 records
            if len(self._records) > 100000:
                self._records = self._records[-100000:]

            # Update endpoint stats
            key = (endpoint, method)
            if key not in self._endpoint_stats:
                self._endpoint_stats[key] = EndpointStats(endpoint=endpoint, method=method)
            stats = self._endpoint_stats[key]
            stats.total_requests += 1
            stats.total_latency_ms += latency_ms
            stats.min_latency_ms = min(stats.min_latency_ms, latency_ms)
            stats.max_latency_ms = max(stats.max_latency_ms, latency_ms)
            stats.total_bytes_in += request_size
            stats.total_bytes_out += response_size
            if 200 <= status_code < 300:
                stats.success_count += 1
            else:
                stats.error_count += 1

            # Update daily usage
            if date_key not in self._daily_usage:
                self._daily_usage[date_key] = DailyUsage(date=date_key)
            daily = self._daily_usage[date_key]
            daily.total_requests += 1
            daily.total_latency_ms += latency_ms
            daily.avg_latency_ms = daily.total_latency_ms / daily.total_requests
            daily.total_bytes_in += request_size
            daily.total_bytes_out += response_size
            if 200 <= status_code < 300:
                daily.success_requests += 1
            else:
                daily.error_requests += 1

            # Update hourly usage
            hour = timestamp.hour
            hourly_list = self._hourly_usage[date_key]
            while len(hourly_list) <= hour:
                hourly_list.append(HourlyUsage(hour=len(hourly_list) - 1))
            hourly = hourly_list[hour]
            hourly.total_requests += 1

            # Update aggregations
            self._key_usage[key_id] += 1
            self._org_usage[organization_id] += 1
            if error_type:
                self._error_counts[error_type] += 1

    def get_key_analytics(self, key_id: str) -> Dict:
        """Get comprehensive analytics for a specific key."""
        with self._lock:
            records = [r for r in self._records if r.key_id == key_id]
            if not records:
                return {"error": "No usage recorded"}

            latencies = [r.latency_ms for r in records]
            endpoints = defaultdict(int)
            errors = defaultdict(int)
            total_bytes_in = sum(r.request_size for r in records)
            total_bytes_out = sum(r.response_size for r in records)

            for r in records:
                endpoints[f"{r.method} {r.endpoint}"] += 1
                if r.error_type:
                    errors[r.error_type] += 1

            return {
                "key_id": key_id,
                "total_requests": len(records),
                "success_rate": len([r for r in records if 200 <= r.status_code < 300]) / len(records) * 100,
                "latency": {
                    "min_ms": min(latencies),
                    "max_ms": max(latencies),
                    "avg_ms": mean(latencies),
                    "p50_ms": sorted(latencies)[len(latencies) // 2],
                    "p95_ms": sorted(latencies)[int(len(latencies) * 0.95)],
                    "p99_ms": sorted(latencies)[int(len(latencies) * 0.99)],
                },
                "traffic": {
                    "bytes_in": total_bytes_in,
                    "bytes_out": total_bytes_out,
                },
                "top_endpoints": sorted(endpoints.items(), key=lambda x: -x[1])[:10],
                "errors": dict(errors),
                "timespan": {
                    "first": records[0].timestamp.isoformat(),
                    "last": records[-1].timestamp.isoformat(),
                    "hours": (records[-1].timestamp - records[0].timestamp).total_seconds() / 3600,
                },
            }

    def get_endpoint_analytics(self, endpoint: str = None, method: str = None) -> List[Dict]:
        """Get analytics per endpoint."""
        with self._lock:
            results = []
            for (ep, meth), stats in self._endpoint_stats.items():
                if endpoint and ep != endpoint:
                    continue
                if method and meth != method:
                    continue

                results.append(
                    {
                        "endpoint": ep,
                        "method": meth,
                        "total_requests": stats.total_requests,
                        "success_rate": stats.success_count / max(stats.total_requests, 1) * 100,
                        "error_rate": stats.error_count / max(stats.total_requests, 1) * 100,
                        "latency": {
                            "avg_ms": stats.total_latency_ms / max(stats.total_requests, 1),
                            "min_ms": stats.min_latency_ms if stats.min_latency_ms != float("inf") else 0,
                            "max_ms": stats.max_latency_ms,
                        },
                        "traffic": {
                            "bytes_in": stats.total_bytes_in,
                            "bytes_out": stats.total_bytes_out,
                        },
                    }
                )

            return sorted(results, key=lambda x: -x["total_requests"])

    def get_daily_usage(self, days: int = 30) -> List[Dict]:
        """Get daily usage breakdown."""
        with self._lock:
            today = datetime.utcnow().strftime("%Y-%m-%d")
            result = []
            for i in range(days):
                date = (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d")
                if date in self._daily_usage:
                    d = self._daily_usage[date]
                    result.append(
                        {
                            "date": d.date,
                            "requests": d.total_requests,
                            "success": d.success_requests,
                            "errors": d.error_requests,
                            "avg_latency_ms": round(d.avg_latency_ms, 2),
                            "bytes_in": d.total_bytes_in,
                            "bytes_out": d.total_bytes_out,
                        }
                    )
            return result

    def get_hourly_usage(self, date: str = None) -> Dict:
        """Get hourly usage for a specific date."""
        with self._lock:
            date = date or datetime.utcnow().strftime("%Y-%m-%d")
            hourly_list = self._hourly_usage.get(date, [])
            return {
                "date": date,
                "hours": [{"hour": h.hour, "requests": h.total_requests} for h in hourly_list],
            }

    def get_error_breakdown(self) -> Dict:
        """Get error type breakdown."""
        with self._lock:
            total_errors = sum(self._error_counts.values())
            return {
                "total_errors": total_errors,
                "by_type": dict(self._error_counts),
                "percentage": {k: round(v / max(total_errors, 1) * 100, 2) for k, v in self._error_counts.items()},
            }

    def get_organization_usage(self) -> Dict:
        """Get usage by organization."""
        with self._lock:
            return {
                org: {
                    "requests": count,
                    "percentage": round(count / max(sum(self._org_usage.values()), 1) * 100, 2),
                }
                for org, count in sorted(self._org_usage.items(), key=lambda x: -x[1])
            }

    def get_top_keys(self, limit: int = 10) -> List[Dict]:
        """Get top API keys by usage."""
        with self._lock:
            return [
                {"key_id": k, "requests": v} for k, v in sorted(self._key_usage.items(), key=lambda x: -x[1])[:limit]
            ]

    def get_summary(self) -> Dict:
        """Get overall analytics summary."""
        with self._lock:
            total_requests = len(self._records)
            if total_requests == 0:
                return {"error": "No usage recorded"}

            latencies = [r.latency_ms for r in self._records]
            success_count = len([r for r in self._records if 200 <= r.status_code < 300])

            return {
                "total_requests": total_requests,
                "unique_keys": len(self._key_usage),
                "unique_organizations": len(self._org_usage),
                "success_rate": round(success_count / total_requests * 100, 2),
                "error_rate": round(100 - (success_count / total_requests * 100), 2),
                "latency": {
                    "avg_ms": round(mean(latencies), 2),
                    "p50_ms": round(sorted(latencies)[total_requests // 2], 2),
                    "p95_ms": round(sorted(latencies)[int(total_requests * 0.95)], 2),
                    "p99_ms": round(sorted(latencies)[int(total_requests * 0.99)], 2),
                },
                "traffic": {
                    "total_bytes_in": sum(r.request_size for r in self._records),
                    "total_bytes_out": sum(r.response_size for r in self._records),
                },
                "time_range": {
                    "first": self._records[0].timestamp.isoformat() if self._records else None,
                    "last": self._records[-1].timestamp.isoformat() if self._records else None,
                },
            }


class APIKeyManager:
    """
    Secure API key management system with usage tracking.
    """

    KEY_PREFIXES = {
        KeyType.LIVE: "mk_live_",
        KeyType.TEST: "mk_test_",
        KeyType.SCOPED: "mk_scop_",
        KeyType.SERVICE: "mk_svc_",
    }

    def __init__(self, secret_key: str = None):
        self._secret_key = secret_key or secrets.token_hex(32)
        self._key_store: Dict[str, APIKey] = {}
        self._key_hash_index: Dict[str, str] = {}
        self._rate_limiters: Dict[str, TokenBucket] = {}
        self._analytics = UsageAnalytics()
        self._lock = threading.RLock()

        self._stats = {
            "total_keys": 0,
            "active_keys": 0,
            "total_requests": 0,
            "blocked_requests": 0,
        }

    def generate_key(
        self,
        key_type: KeyType,
        organization_id: str,
        user_id: Optional[str] = None,
        permissions: List[KeyPermission] = None,
        days_until_expiry: int = 365,
        rate_limit_per_minute: int = 1000,
        description: str = "",
    ) -> Tuple[str, str]:
        """Generate a new API key."""
        if permissions is None:
            permissions = [KeyPermission.READ, KeyPermission.WRITE]

        random_bytes = secrets.token_bytes(32)
        key_material = f"{random_bytes.hex()}{datetime.utcnow().isoformat()}"
        prefix = self.KEY_PREFIXES[key_type]
        full_key = f"{prefix}{key_material}"
        key_hash = self._hash_key(full_key)
        key_prefix = full_key[:12]
        key_id = key_hash[:8]

        api_key = APIKey(
            key_id=key_id,
            key_hash=key_hash,
            key_prefix=key_prefix,
            key_type=key_type,
            organization_id=organization_id,
            user_id=user_id,
            permissions=permissions,
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(days=days_until_expiry),
            last_used_at=None,
            usage_count=0,
            rate_limit_per_minute=rate_limit_per_minute,
            description=description,
        )

        with self._lock:
            self._key_store[key_id] = api_key
            self._key_hash_index[key_hash] = key_id
            self._rate_limiters[key_id] = TokenBucket(rate=rate_limit_per_minute, capacity=rate_limit_per_minute)
            self._stats["total_keys"] += 1
            self._stats["active_keys"] += 1

        return full_key, key_id

    def validate_key(self, full_key: str) -> Optional[APIKey]:
        """Validate an API key."""
        if not full_key:
            return None

        key_hash = self._hash_key(full_key)

        with self._lock:
            key_id = self._key_hash_index.get(key_hash)
            if not key_id:
                self._stats["blocked_requests"] += 1
                return None

            api_key = self._key_store.get(key_id)
            if not api_key or not api_key.is_valid:
                return None

            return api_key

    def check_rate_limit(self, api_key: APIKey) -> Tuple[bool, int]:
        """Check rate limit for a key."""
        with self._lock:
            bucket = self._rate_limiters.get(api_key.key_id)
            if not bucket:
                return True, api_key.rate_limit_per_minute

            allowed, remaining = bucket.consume()
            if not allowed:
                self._stats["blocked_requests"] += 1
            return allowed, remaining

    def record_usage(
        self,
        api_key: APIKey,
        endpoint: str,
        method: str,
        latency_ms: float,
        status_code: int,
        request_size: int = 0,
        response_size: int = 0,
        error_type: Optional[str] = None,
    ):
        """Record API usage with full analytics."""
        with self._lock:
            api_key.last_used_at = datetime.utcnow()
            api_key.usage_count += 1
            self._stats["total_requests"] += 1

        # Record to analytics
        self._analytics.record(
            key_id=api_key.key_id,
            organization_id=api_key.organization_id,
            endpoint=endpoint,
            method=method,
            latency_ms=latency_ms,
            status_code=status_code,
            request_size=request_size,
            response_size=response_size,
            error_type=error_type,
        )

    def request(
        self,
        api_key: APIKey,
        endpoint: str,
        method: str,
        latency_ms: float,
        status_code: int,
        request_size: int = 0,
        response_size: int = 0,
        error_type: Optional[str] = None,
    ) -> bool:
        """
        Complete request lifecycle: rate limit check + usage recording.

        Returns:
            True if request allowed, False if blocked
        """
        allowed, _ = self.check_rate_limit(api_key)
        if allowed:
            self.record_usage(
                api_key=api_key,
                endpoint=endpoint,
                method=method,
                latency_ms=latency_ms,
                status_code=status_code,
                request_size=request_size,
                response_size=response_size,
                error_type=error_type,
            )
        return allowed

    def revoke_key(self, key_id: str) -> bool:
        """Revoke an API key."""
        with self._lock:
            api_key = self._key_store.get(key_id)
            if not api_key:
                return False
            api_key.is_active = False
            self._stats["active_keys"] -= 1
            return True

    def rotate_key(self, key_id: str) -> Optional[Tuple[str, str]]:
        """Rotate an API key."""
        with self._lock:
            api_key = self._key_store.get(key_id)
            if not api_key:
                return None

            new_key, new_id = self.generate_key(
                key_type=api_key.key_type,
                organization_id=api_key.organization_id,
                user_id=api_key.user_id,
                permissions=api_key.permissions,
                rate_limit_per_minute=api_key.rate_limit_per_minute,
                description=f"Rotation of {api_key.description or key_id}",
            )
            self.revoke_key(key_id)
            return new_key, new_id

    def get_key_stats(self, key_id: str) -> Optional[Dict]:
        """Get statistics for a specific key."""
        with self._lock:
            api_key = self._key_store.get(key_id)
            if not api_key:
                return None

            return {
                **{
                    "key_id": api_key.key_id,
                    "key_prefix": api_key.key_prefix,
                    "key_type": api_key.key_type.value,
                    "organization_id": api_key.organization_id,
                    "user_id": api_key.user_id,
                    "created_at": api_key.created_at.isoformat(),
                    "expires_at": api_key.expires_at.isoformat() if api_key.expires_at else None,
                    "last_used_at": api_key.last_used_at.isoformat() if api_key.last_used_at else None,
                    "usage_count": api_key.usage_count,
                    "is_active": api_key.is_active,
                    "is_expired": api_key.is_expired,
                    "permissions": [p.value for p in api_key.permissions],
                    "rate_limit_per_minute": api_key.rate_limit_per_minute,
                },
                "analytics": self._analytics.get_key_analytics(key_id),
            }

    def get_analytics_summary(self) -> Dict:
        """Get overall analytics summary."""
        return self._analytics.get_summary()

    def get_key_analytics(self, key_id: str) -> Dict:
        """Get analytics for a specific key."""
        return self._analytics.get_key_analytics(key_id)

    def get_endpoint_analytics(self, endpoint: str = None, method: str = None) -> List[Dict]:
        """Get endpoint-level analytics."""
        return self._analytics.get_endpoint_analytics(endpoint, method)

    def get_daily_usage(self, days: int = 30) -> List[Dict]:
        """Get daily usage breakdown."""
        return self._analytics.get_daily_usage(days)

    def get_hourly_usage(self, date: str = None) -> Dict:
        """Get hourly usage."""
        return self._analytics.get_hourly_usage(date)

    def get_error_breakdown(self) -> Dict:
        """Get error breakdown."""
        return self._analytics.get_error_breakdown()

    def get_organization_usage(self) -> Dict:
        """Get usage by organization."""
        return self._analytics.get_organization_usage()

    def get_top_keys(self, limit: int = 10) -> List[Dict]:
        """Get top keys by usage."""
        return self._analytics.get_top_keys(limit)

    def get_manager_stats(self) -> Dict:
        """Get manager statistics."""
        with self._lock:
            return {
                **self._stats,
                "organization_count": len(set(k.organization_id for k in self._key_store.values())),
            }

    def _hash_key(self, key: str) -> str:
        """Hash an API key."""
        return hmac.new(self._secret_key.encode(), key.encode(), hashlib.sha256).hexdigest()


class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate: int, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.time()
        self._lock = threading.Lock()

    def consume(self) -> Tuple[bool, int]:
        """Consume one token."""
        with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            tokens_to_add = elapsed * self.rate
            self.tokens = min(self.capacity, self.tokens + tokens_to_add)
            self.last_update = now

            if self.tokens >= 1:
                self.tokens -= 1
                return True, int(self.tokens)
            return False, 0

    def remaining(self) -> int:
        """Get remaining tokens."""
        with self._lock:
            now = time.time()
            elapsed = now - self.last_update
            return int(min(self.capacity, self.tokens + elapsed * self.rate))


if __name__ == "__main__":
    print("=" * 70)
    print("API KEY USAGE TRACKING DEMO")
    print("=" * 70)

    # Create manager
    manager = APIKeyManager()

    # Generate keys
    live_key, live_id = manager.generate_key(
        key_type=KeyType.LIVE,
        organization_id="acme_corp",
        user_id="user_123",
        permissions=[KeyPermission.READ, KeyPermission.WRITE, KeyPermission.MEMORY_READ],
        description="Production Key",
    )

    scoped_key, scoped_id = manager.generate_key(
        key_type=KeyType.SCOPED,
        organization_id="acme_corp",
        user_id="user_456",
        permissions=[KeyPermission.READ],
        description="Read-only Key",
    )

    print(f"\n[1] Generated Keys:")
    print(f"    Live: {live_key[:25]}... (ID: {live_id})")
    print(f"    Scoped: {scoped_key[:25]}... (ID: {scoped_id})")

    # Simulate usage
    print(f"\n[2] Simulating API Usage...")
    import random

    key = manager.validate_key(live_key)
    endpoints = [
        ("/memory", "GET"),
        ("/memory", "POST"),
        ("/sandbox", "CREATE"),
        ("/sandbox", "EXECUTE"),
        ("/health", "GET"),
    ]

    for i in range(100):
        ep, method = random.choice(endpoints)
        latency = random.uniform(5, 100)
        status = 200 if random.random() > 0.1 else (400 + random.randint(0, 3))
        error = None if status == 200 else "validation_error" if status == 400 else "server_error"

        manager.request(
            api_key=key,
            endpoint=ep,
            method=method,
            latency_ms=latency,
            status_code=status,
            request_size=random.randint(100, 1000),
            response_size=random.randint(500, 5000),
            error_type=error,
        )

    print(f"    Recorded 100 requests")

    # Get analytics
    print(f"\n[3] Analytics Summary:")
    summary = manager.get_analytics_summary()
    for k, v in summary.items():
        if k != "time_range":
            print(f"    {k}: {v}")

    print(f"\n[4] Top Endpoints:")
    for ep in manager.get_endpoint_analytics()[:5]:
        print(f"    {ep['method']} {ep['endpoint']}: {ep['total_requests']} reqs ({ep['success_rate']:.1f}% success)")

    print(f"\n[5] Error Breakdown:")
    errors = manager.get_error_breakdown()
    print(f"    Total Errors: {errors['total_errors']}")
    for err_type, count in errors["by_type"].items():
        pct = errors["percentage"][err_type]
        print(f"    {err_type}: {count} ({pct}%)")

    print(f"\n[6] Daily Usage:")
    for day in manager.get_daily_usage(7):
        print(f"    {day['date']}: {day['requests']} reqs, {day['avg_latency_ms']}ms avg")

    print(f"\n[7] Top Keys:")
    for item in manager.get_top_keys():
        print(f"    {item['key_id']}: {item['requests']} requests")

    print(f"\n[8] Key-Specific Analytics:")
    key_analytics = manager.get_key_analytics(live_id)
    print(f"    Total Requests: {key_analytics['total_requests']}")
    print(f"    Success Rate: {key_analytics['success_rate']:.1f}%")
    print(f"    Latency P50: {key_analytics['latency']['p50_ms']:.2f}ms")
    print(f"    Latency P99: {key_analytics['latency']['p99_ms']:.2f}ms")

    print("\n" + "=" * 70)
    print("Usage Tracking Enabled!")
    print("=" * 70)
