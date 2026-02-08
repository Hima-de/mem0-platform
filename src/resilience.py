"""Resilience - Circuit Breaker, Rate Limiting, Retries."""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, Dict
import uuid


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation
    OPEN = "open"  # Failing, reject all
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""

    failure_threshold: int = 5  # Failures before opening
    success_threshold: int = 3  # Successes in half-open to close
    timeout_seconds: float = 60.0  # Time before trying half-open
    monitoring_window: float = 60.0  # Time window for failure counting


class CircuitBreakerError(Exception):
    """Circuit breaker is open."""

    pass


class CircuitBreaker:
    """
    Circuit breaker for resilience.

    States:
    - CLOSED: Normal operation
    - OPEN: Failing, reject all requests
    - HALF_OPEN: Testing if service recovered
    """

    def __init__(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
    ):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._opened_time: Optional[float] = None
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        """Get current state."""
        if self._state == CircuitState.OPEN:
            if time.time() - self._opened_time >= self.config.timeout_seconds:
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
        return self._state

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function through circuit breaker."""
        async with self._lock:
            if self.state == CircuitState.OPEN:
                raise CircuitBreakerError(f"Circuit breaker '{self.name}' is OPEN")

        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            await self._on_success()
            return result
        except Exception as e:
            await self._on_failure()
            raise

    async def _on_success(self) -> None:
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._last_failure_time = None

    async def _on_failure(self) -> None:
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._opened_time = time.time()
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.config.failure_threshold:
                    self._state = CircuitState.OPEN
                    self._opened_time = time.time()

    def get_stats(self) -> Dict:
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "last_failure_time": self._last_failure_time,
        }

    def reset(self) -> None:
        """Reset circuit breaker."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None
        self._opened_time = None


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(
        self,
        max_tokens: int = 100,
        refill_rate: float = 10.0,
        refill_interval: float = 1.0,
    ):
        self.max_tokens = max_tokens
        self.refill_rate = refill_rate
        self.refill_interval = refill_interval
        self._tokens = max_tokens
        self._last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire tokens. Returns True if acquired."""
        async with self._lock:
            now = time.time()
            elapsed = now - self._last_refill

            if elapsed >= self.refill_interval:
                self._tokens = min(
                    self.max_tokens,
                    self._tokens + int(elapsed * self.refill_rate),
                )
                self._last_refill = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    async def wait_acquire(self, tokens: int = 1, timeout: float = 10.0) -> bool:
        """Wait to acquire tokens with timeout."""
        start = time.time()
        while time.time() - start < timeout:
            if await self.acquire(tokens):
                return True
            await asyncio.sleep(0.01)
        return False

    def get_stats(self) -> Dict:
        return {
            "tokens": self._tokens,
            "max_tokens": self.max_tokens,
            "refill_rate": self.refill_rate,
        }


class RetryPolicy:
    """Retry policy with exponential backoff."""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 0.1,
        max_delay: float = 10.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter

    def get_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt."""
        delay = self.base_delay * (self.exponential_base**attempt)
        delay = min(delay, self.max_delay)

        if self.jitter:
            delay *= 0.5 + hash(str(attempt)) % 100 / 100

        return delay

    def should_retry(self, attempt: int, exception: Exception) -> bool:
        """Determine if should retry."""
        if isinstance(exception, CircuitBreakerError):
            return False
        return attempt < self.max_attempts


async def with_retry(
    func: Callable,
    policy: Optional[RetryPolicy] = None,
    *args,
    **kwargs,
):
    """Execute function with retries."""
    policy = policy or RetryPolicy()

    last_exception = None

    for attempt in range(policy.max_attempts):
        try:
            return await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            if not policy.should_retry(attempt, e):
                raise
            if attempt < policy.max_attempts - 1:
                delay = policy.get_delay(attempt)
                await asyncio.sleep(delay)

    raise last_exception


@dataclass
class ConnectionPoolConfig:
    """Connection pool configuration."""

    min_size: int = 1
    max_size: int = 10
    max_idle_time: float = 300.0
    connection_timeout: float = 30.0
    health_check_interval: float = 30.0


class ConnectionPool:
    """Generic connection pool."""

    def __init__(
        self,
        factory: Callable,
        config: Optional[ConnectionPoolConfig] = None,
    ):
        self.factory = factory
        self.config = config or ConnectionPoolConfig()
        self._pool: asyncio.Queue = asyncio.Queue(maxsize=self.config.max_size)
        self._in_use: Dict[Any, float] = {}
        self._lock = asyncio.Lock()
        self._created = 0

    async def get(self) -> Any:
        """Get connection from pool."""
        try:
            conn = self._pool.get_nowait()
            if self._is_healthy(conn):
                return conn
        except asyncio.QueueEmpty:
            pass

        if self._created < self.config.max_size:
            conn = self.factory()
            if asyncio.iscoroutinefunction(self.factory):
                conn = await conn
            self._created += 1
            return conn

        raise RuntimeError("Connection pool exhausted")

    def release(self, conn: Any) -> None:
        """Release connection back to pool."""
        if self._is_healthy(conn) and self._pool.qsize() < self.config.max_size:
            self._pool.put_nowait(conn)
        else:
            self._created = max(0, self._created - 1)

    def _is_healthy(self, conn: Any) -> bool:
        """Check if connection is healthy."""
        return True

    async def close(self) -> None:
        """Close all connections."""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                if asyncio.iscoroutinefunction(conn.close):
                    await conn.close()
                elif hasattr(conn, "close"):
                    conn.close()
            except:
                pass
        self._created = 0

    def get_stats(self) -> Dict:
        return {
            "size": self._pool.qsize(),
            "created": self._created,
            "max_size": self.config.max_size,
        }


# Pre-configured circuit breakers
memory_circuit = CircuitBreaker(
    "memory",
    CircuitBreakerConfig(
        failure_threshold=5,
        timeout_seconds=30.0,
    ),
)

runtime_circuit = CircuitBreaker(
    "runtime",
    CircuitBreakerConfig(
        failure_threshold=3,
        timeout_seconds=60.0,
    ),
)

storage_circuit = CircuitBreaker(
    "storage",
    CircuitBreakerConfig(
        failure_threshold=5,
        timeout_seconds=30.0,
    ),
)

sandbox_circuit = CircuitBreaker(
    "sandbox",
    CircuitBreakerConfig(
        failure_threshold=3,
        timeout_seconds=60.0,
    ),
)

# Global rate limiter
api_rate_limiter = RateLimiter(
    max_tokens=100,
    refill_rate=10.0,
)
