"""
Production Redis Caching Layer for 10 Million Users.

This module provides a multi-tier caching system optimized for:
- Sub-millisecond latency for hot data
- Automatic cache invalidation
- Rate limiting support
- Session management
- Distributed locking
"""

import asyncio
import json
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Optional, Any, Dict, List, Union
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
import redis.asyncio as redis
from redis.asyncio.connection import ConnectionPool

logger = logging.getLogger(__name__)


@dataclass
class CacheConfig:
    """Redis cluster configuration."""

    host: str = "localhost"
    port: int = 6379
    password: Optional[str] = None
    db: int = 0
    max_connections: int = 100
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0
    retry_on_timeout: bool = True
    encoding: str = "utf-8"
    decode_responses: bool = True


@dataclass
class CacheStats:
    """Cache statistics."""

    hits: int = 0
    misses: int = 0
    evictions: int = 0
    errors: int = 0
    avg_latency_ms: float = 0.0


class CacheTier:
    """Cache tier with TTL configuration."""

    L1_MEMORY = "memory"  # Hot data, 1 minute TTL
    L2_SESSION = "session"  # Session data, 1 hour TTL
    L3_USER = "user"  # User preferences, 24 hour TTL
    L4_EMBEDDING = "embedding"  # Embeddings cache, 7 day TTL
    L5_RUNTIME = "runtime"  # Runtime pack metadata, 7 day TTL


class RedisCacheManager:
    """
    Production Redis cache manager with multi-tier caching,
    connection pooling, and automatic failover support.
    """

    # TTL configurations in seconds
    TTL_CONFIG = {
        CacheTier.L1_MEMORY: 60,  # 1 minute
        CacheTier.L2_SESSION: 3600,  # 1 hour
        CacheTier.L3_USER: 86400,  # 24 hours
        CacheTier.L4_EMBEDDING: 604800,  # 7 days
        CacheTier.L5_RUNTIME: 604800,  # 7 days
    }

    def __init__(self, config: CacheConfig = None):
        self.config = config or CacheConfig()
        self.pool: Optional[ConnectionPool] = None
        self.client: Optional[redis.Redis] = None
        self.stats = CacheStats()
        self._latency_samples: List[float] = []
        self._max_latency_samples = 1000

    async def connect(self):
        """Initialize Redis connection pool."""
        self.pool = ConnectionPool(
            host=self.config.host,
            port=self.config.port,
            password=self.config.password,
            db=self.config.db,
            max_connections=self.config.max_connections,
            socket_timeout=self.config.socket_timeout,
            socket_connect_timeout=self.config.socket_connect_timeout,
            retry_on_timeout=self.config.retry_on_timeout,
            encoding=self.config.encoding,
            decode_responses=self.config.decode_responses,
        )
        self.client = redis.Redis(connection_pool=self.pool)

        # Test connection
        try:
            await self.client.ping()
            logger.info(f"Redis connected to {self.config.host}:{self.config.port}")
        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
            raise

    async def disconnect(self):
        """Close Redis connection pool."""
        if self.client:
            await self.client.close()
        if self.pool:
            await self.pool.disconnect()
        logger.info("Redis disconnected")

    def _get_key(self, tier: str, *args) -> str:
        """Generate cache key with tier prefix."""
        key_parts = [tier] + [str(arg) for arg in args]
        return ":".join(key_parts)

    def _serialize(self, value: Any) -> str:
        """Serialize value to string."""
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float, bool)):
            return str(value)
        return json.dumps(value, default=str)

    def _deserialize(self, value: str) -> Any:
        """Deserialize value from string."""
        if value is None:
            return None
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value

    async def get(self, tier: str, *keys: str, default: Any = None) -> Any:
        """Get value from cache."""
        start = datetime.utcnow()
        try:
            key = self._get_key(tier, *keys)
            value = await self.client.get(key)

            latency_ms = (datetime.utcnow() - start).total_seconds() * 1000
            self._record_latency(latency_ms)

            if value is not None:
                self.stats.hits += 1
                return self._deserialize(value)

            self.stats.misses += 1
            return default

        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache get error: {e}")
            return default

    async def set(self, tier: str, *keys: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in cache with optional TTL."""
        start = datetime.utcnow()
        try:
            key = self._get_key(tier, *keys)
            serialized = self._serialize(value)
            ttl = ttl or self.TTL_CONFIG.get(tier, 3600)

            await self.client.setex(key, ttl, serialized)

            latency_ms = (datetime.utcnow() - start).total_seconds() * 1000
            self._record_latency(latency_ms)

            return True

        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache set error: {e}")
            return False

    async def delete(self, tier: str, *keys: str) -> bool:
        """Delete key from cache."""
        try:
            key = self._get_key(tier, *keys)
            await self.client.delete(key)
            return True
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache delete error: {e}")
            return False

    async def delete_pattern(self, tier: str, pattern: str) -> int:
        """Delete all keys matching pattern."""
        try:
            full_pattern = self._get_key(tier, pattern)
            keys = await self.client.keys(full_pattern)
            if keys:
                return await self.client.delete(*keys)
            return 0
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache delete_pattern error: {e}")
            return 0

    async def get_or_set(self, tier: str, *keys: str, default: Any = None, ttl: Optional[int] = None) -> Any:
        """Get value from cache, or set and return default if not found."""
        value = await self.get(tier, *keys)
        if value is not None:
            return value

        # Set default if provided
        if default is not None:
            await self.set(tier, *keys, value=default, ttl=ttl)

        return default

    async def increment(self, tier: str, *keys: str, amount: int = 1) -> Optional[int]:
        """Increment counter in cache."""
        try:
            key = self._get_key(tier, *keys)
            return await self.client.incrby(key, amount)
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache increment error: {e}")
            return None

    async def expire(self, tier: str, *keys: str, ttl: int) -> bool:
        """Set expiration on existing key."""
        try:
            key = self._get_key(tier, *keys)
            return await self.client.expire(key, ttl)
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache expire error: {e}")
            return False

    @asynccontextmanager
    async def lock(self, name: str, timeout: float = 10.0, blocking_timeout: float = 5.0):
        """Acquire distributed lock."""
        lock_key = self._get_key("lock", name)
        lock = self.client.lock(lock_key, timeout=timeout, blocking_timeout=blocking_timeout)
        acquired = await lock.acquire()
        try:
            if not acquired:
                raise RuntimeError(f"Failed to acquire lock: {name}")
            yield lock
        finally:
            await lock.release()

    async def add_to_set(self, tier: str, key: str, *values: str, ttl: Optional[int] = None) -> bool:
        """Add values to set."""
        try:
            full_key = self._get_key(tier, key)
            ttl = ttl or self.TTL_CONFIG.get(tier, 3600)

            pipeline = self.client.pipeline()
            pipeline.sadd(full_key, *values)
            pipeline.expire(full_key, ttl)
            await pipeline.execute()
            return True
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache sadd error: {e}")
            return False

    async def get_set_members(self, tier: str, key: str) -> List[str]:
        """Get all members of set."""
        try:
            full_key = self._get_key(tier, key)
            return await self.client.smembers(full_key)
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache smembers error: {e}")
            return []

    async def hash_set(self, tier: str, key: str, field: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set field in hash."""
        try:
            full_key = self._get_key(tier, key)
            ttl = ttl or self.TTL_CONFIG.get(tier, 3600)

            pipeline = self.client.pipeline()
            pipeline.hset(full_key, field, self._serialize(value))
            pipeline.expire(full_key, ttl)
            await pipeline.execute()
            return True
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache hset error: {e}")
            return False

    async def hash_get(self, tier: str, key: str, field: str, default: Any = None) -> Any:
        """Get field from hash."""
        try:
            full_key = self._get_key(tier, key)
            value = await self.client.hget(full_key, field)
            if value is not None:
                return self._deserialize(value)
            return default
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache hget error: {e}")
            return default

    async def hash_get_all(self, tier: str, key: str) -> Dict[str, Any]:
        """Get all fields from hash."""
        try:
            full_key = self._get_key(tier, key)
            values = await self.client.hgetall(full_key)
            return {k: self._deserialize(v) for k, v in values.items()}
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache hgetall error: {e}")
            return {}

    async def zadd(self, tier: str, key: str, score: float, member: str, ttl: Optional[int] = None) -> bool:
        """Add member to sorted set with score."""
        try:
            full_key = self._get_key(tier, key)
            ttl = ttl or self.TTL_CONFIG.get(tier, 3600)

            pipeline = self.client.pipeline()
            pipeline.zadd(full_key, {member: score})
            pipeline.expire(full_key, ttl)
            await pipeline.execute()
            return True
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache zadd error: {e}")
            return False

    async def zrangebyscore(
        self, tier: str, key: str, min_score: float, max_score: float, with_scores: bool = False
    ) -> List[Union[str, tuple]]:
        """Get members from sorted set by score range."""
        try:
            full_key = self._get_key(tier, key)
            if with_scores:
                return await self.client.zrangebyscore(full_key, min_score, max_score, withscores=True)
            return await self.client.zrangebyscore(full_key, min_score, max_score)
        except Exception as e:
            self.stats.errors += 1
            logger.error(f"Cache zrangebyscore error: {e}")
            return []

    def _record_latency(self, latency_ms: float):
        """Record latency sample."""
        self._latency_samples.append(latency_ms)
        if len(self._latency_samples) > self._max_latency_samples:
            self._latency_samples = self._latency_samples[-self._max_latency_samples :]

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total = self.stats.hits + self.stats.misses
        hit_rate = (self.stats.hits / total * 100) if total > 0 else 0

        avg_latency = sum(self._latency_samples) / len(self._latency_samples) if self._latency_samples else 0

        p50_latency = (
            sorted(self._latency_samples)[int(len(self._latency_samples) * 0.5)] if self._latency_samples else 0
        )
        p99_latency = (
            sorted(self._latency_samples)[int(len(self._latency_samples) * 0.99) - 1] if self._latency_samples else 0
        )

        return {
            "hits": self.stats.hits,
            "misses": self.stats.misses,
            "hit_rate_percent": round(hit_rate, 2),
            "evictions": self.stats.evictions,
            "errors": self.stats.errors,
            "avg_latency_ms": round(avg_latency, 3),
            "p50_latency_ms": round(p50_latency, 3),
            "p99_latency_ms": round(p99_latency, 3),
        }


class MemoryCache:
    """
    High-performance in-memory cache layer for frequently accessed data.
    Uses LRU eviction and TTL-based expiration.
    """

    def __init__(self, max_size: int = 10000, default_ttl: int = 60):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: Dict[str, tuple] = {}
        self._access_order: List[str] = []
        self._lock = asyncio.Lock()

    def _make_key(self, tier: str, *args) -> str:
        """Generate cache key."""
        return ":".join([tier] + [str(arg) for arg in args])

    def _is_expired(self, expiry: datetime) -> bool:
        """Check if entry is expired."""
        return datetime.utcnow() > expiry

    async def get(self, tier: str, *keys: str, default: Any = None) -> Any:
        """Get value from memory cache."""
        async with self._lock:
            key = self._make_key(tier, *keys)

            if key not in self._cache:
                return default

            value, expiry = self._cache[key]

            if self._is_expired(expiry):
                del self._cache[key]
                self._access_order.remove(key)
                return default

            # Update access order (move to end = most recently used)
            if key in self._access_order:
                self._access_order.remove(key)
            self._access_order.append(key)

            return value

    async def set(self, tier: str, *keys: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set value in memory cache."""
        async with self._lock:
            key = self._make_key(tier, *keys)
            expiry = datetime.utcnow() + timedelta(seconds=ttl or self.default_ttl)

            # Evict oldest entries if at capacity
            while len(self._cache) >= self.max_size:
                oldest_key = self._access_order.pop(0)
                del self._cache[oldest_key]

            self._cache[key] = (value, expiry)
            self._access_order.append(key)

            return True

    async def delete(self, tier: str, *keys: str) -> bool:
        """Delete key from cache."""
        async with self._lock:
            key = self._make_key(tier, *keys)

            if key in self._cache:
                del self._cache[key]
                self._access_order.remove(key)
                return True

            return False

    async def clear_tier(self, tier: str) -> int:
        """Clear all keys in a tier."""
        async with self._lock:
            keys_to_delete = [k for k in self._cache.keys() if k.startswith(tier + ":")]
            for key in keys_to_delete:
                del self._cache[key]
                if key in self._access_order:
                    self._access_order.remove(key)
            return len(keys_to_delete)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "utilization_percent": round(len(self._cache) / self.max_size * 100, 2),
        }


class RateLimiter:
    """
    Distributed rate limiter using Redis sliding window algorithm.
    Supports per-user and global rate limiting.
    """

    def __init__(self, redis_cache: RedisCacheManager):
        self.redis = redis_cache

    async def check_rate_limit(
        self, user_id: str, endpoint: str, max_requests: int, window_seconds: int
    ) -> Dict[str, Any]:
        """
        Check if request is within rate limit.

        Returns:
            {
                "allowed": bool,
                "remaining": int,
                "reset_at": int,  # Unix timestamp
                "retry_after": int  # Seconds until reset
            }
        """
        key = f"ratelimit:{user_id}:{endpoint}"

        now = int(datetime.utcnow().timestamp())
        window_start = now - window_seconds

        try:
            pipeline = self.redis.client.pipeline()

            # Remove old entries outside window
            pipeline.zremrangebyscore(key, "-inf", window_start)

            # Count current requests in window
            pipeline.zcard(key)

            # Add current request
            pipeline.zadd(key, {f"{now}:{id(self)}": now})

            # Set expiry on the key
            pipeline.expire(key, window_seconds)

            results = await pipeline.execute()
            current_count = results[1]

            remaining = max(0, max_requests - current_count)
            reset_at = now + window_seconds
            retry_after = max(0, window_seconds - (now - window_start))

            return {
                "allowed": current_count <= max_requests,
                "remaining": remaining,
                "reset_at": reset_at,
                "retry_after": retry_after,
            }

        except Exception as e:
            logger.error(f"Rate limit check error: {e}")
            # Fail open - allow request on error
            return {"allowed": True, "remaining": max_requests, "reset_at": now + window_seconds, "retry_after": 0}

    async def record_rate_limit_violation(self, user_id: str, endpoint: str) -> bool:
        """Record rate limit violation for analytics."""
        key = f"ratelimit:violations:{user_id}"
        try:
            await self.redis.client.incr(key)
            await self.redis.client.expire(key, 86400)  # 24 hour window
            return True
        except Exception as e:
            logger.error(f"Rate limit violation recording error: {e}")
            return False


class SessionManager:
    """
    Session management with Redis backend.
    Supports JWT token storage and session invalidation.
    """

    def __init__(self, redis_cache: RedisCacheManager):
        self.redis = redis_cache

    async def create_session(self, session_id: str, user_id: str, data: Dict[str, Any], ttl: int = 3600) -> bool:
        """Create new session."""
        key = self.redis._get_key(CacheTier.L2_SESSION, "sess", session_id)
        session_data = {
            "user_id": user_id,
            "data": data,
            "created_at": datetime.utcnow().isoformat(),
            "last_active": datetime.utcnow().isoformat(),
        }
        return await self.redis.client.setex(key, ttl, json.dumps(session_data))

    async def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data."""
        key = self.redis._get_key(CacheTier.L2_SESSION, "sess", session_id)
        data = await self.redis.client.get(key)
        if data:
            parsed = json.loads(data)
            # Update last active
            parsed["last_active"] = datetime.utcnow().isoformat()
            await self.redis.client.setex(key, 3600, json.dumps(parsed))
            return parsed
        return None

    async def invalidate_session(self, session_id: str) -> bool:
        """Invalidate session."""
        key = self.redis._get_key(CacheTier.L2_SESSION, "sess", session_id)
        return await self.redis.client.delete(key) > 0

    async def invalidate_all_user_sessions(self, user_id: str) -> int:
        """Invalidate all sessions for user."""
        pattern = self.redis._get_key(CacheTier.L2_SESSION, "sess", f"{user_id}:*")
        keys = await self.redis.client.keys(pattern)
        if keys:
            return await self.redis.client.delete(*keys)
        return 0

    async def refresh_session_ttl(self, session_id: str, ttl: int = 3600) -> bool:
        """Extend session TTL."""
        key = self.redis._get_key(CacheTier.L2_SESSION, "sess", session_id)
        return await self.redis.client.expire(key, ttl) > 0


# Global instances
_redis_cache: Optional[RedisCacheManager] = None
_memory_cache: Optional[MemoryCache] = None
_rate_limiter: Optional[RateLimiter] = None
_session_manager: Optional[SessionManager] = None


async def get_redis_cache() -> RedisCacheManager:
    """Get or create Redis cache instance."""
    global _redis_cache
    if _redis_cache is None:
        _redis_cache = RedisCacheManager()
        await _redis_cache.connect()
    return _redis_cache


async def get_memory_cache() -> MemoryCache:
    """Get or create memory cache instance."""
    global _memory_cache
    if _memory_cache is None:
        _memory_cache = MemoryCache(max_size=100000, default_ttl=60)
    return _memory_cache


async def get_rate_limiter() -> RateLimiter:
    """Get or create rate limiter instance."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = RateLimiter(await get_redis_cache())
    return _rate_limiter


async def get_session_manager() -> SessionManager:
    """Get or create session manager instance."""
    global _session_manager
    if _session_manager is None:
        _session_manager = SessionManager(await get_redis_cache())
    return _session_manager
