"""
Mem0 Platform - Production OOP Architecture
====================================

A comprehensive, production-ready object-oriented architecture
for a scalable sandbox and memory platform.

Architecture Patterns Used:
- Factory Pattern: Creating instances without specifying concrete classes
- Strategy Pattern: Interchangeable algorithms (compression, scheduling)
- Observer Pattern: Event-driven notifications
- Singleton Pattern: Single instances (configuration, registry)
- Dependency Injection: Loose coupling between components
- Repository Pattern: Data access abstraction
- Service Layer: Business logic encapsulation
- Facade Pattern: Simplified interface to complex subsystems

Author: Mem0 Engineering Team
Version: 1.0.0
"""

from __future__ import annotations

import abc
import asyncio
import hashlib
import json
import logging
import os
import tempfile
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from functools import wraps
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Protocol,
    Type,
    TypeVar,
    Union,
    runtime_checkable,
)
from uuid import UUID


# =============================================================================
# SECTION 1: Core Primitives and Types
# =============================================================================


class AutoName(Enum):
    """Enum that uses its name as value."""

    def _generate_next_value_(name, start, count, last_values):
        return name


class Singleton(type):
    """Thread-safe singleton metaclass."""

    _instances: Dict[type, Any] = {}
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
            return cls._instances[cls]


class ImmutableMixin:
    """Mixin for immutable data classes."""

    __slots__ = ()

    def __setattr__(self, name, value):
        raise AttributeError(f"'{type(self).__name__}' is immutable")


class ComparableMixin:
    """Mixin for comparable objects."""

    __slots__ = ()

    def __lt__(self, other):
        return self._compare(other) < 0

    def __le__(self, other):
        return self._compare(other) <= 0

    def __gt__(self, other):
        return self._compare(other) > 0

    def __ge__(self, other):
        return self._compare(other) >= 0

    def __eq__(self, other):
        return self._compare(other) == 0

    @abstractmethod
    def _compare(self, other) -> int:
        """Implement comparison logic."""
        ...


T = TypeVar("T")
Result = TypeVar("Result", covariant=True)


# =============================================================================
# SECTION 2: Result Pattern for Error Handling
# =============================================================================


class Result(Generic[T], ABC):
    """Result type for error handling without exceptions."""

    @property
    @abstractmethod
    def is_success(self) -> bool: ...

    @property
    @abstractmethod
    def is_failure(self) -> bool: ...

    @property
    @abstractmethod
    def value(self) -> T: ...

    @property
    @abstractmethod
    def error(self) -> Exception: ...

    @abstractmethod
    def map(self, f: Callable[[T], Result[T]]) -> Result[T]: ...

    @abstractmethod
    def flat_map(self, f: Callable[[T], Result[T]]) -> Result[T]: ...

    @staticmethod
    def success(value: T) -> Result[T]:
        return Success(value)

    @staticmethod
    def failure(error: Exception) -> Result[T]:
        return Failure(error)


class Success(Result[T]):
    """Successful result."""

    __slots__ = ("_value",)

    def __init__(self, value: T):
        self._value = value

    @property
    def is_success(self) -> bool:
        return True

    @property
    def is_failure(self) -> bool:
        return False

    @property
    def value(self) -> T:
        return self._value

    @property
    def error(self) -> Exception:
        raise ValueError("Success has no error")

    def map(self, f: Callable[[T], Result[T]]) -> Result[T]:
        try:
            return f(self._value)
        except Exception as e:
            return Failure(e)

    def flat_map(self, f: Callable[[T], Result[T]]) -> Result[T]:
        return f(self._value)


class Failure(Result[T]):
    """Failed result."""

    __slots__ = ("_error",)

    def __init__(self, error: Exception):
        self._error = error

    @property
    def is_success(self) -> bool:
        return False

    @property
    def is_failure(self) -> bool:
        return True

    @property
    def value(self) -> T:
        raise ValueError(f"Failure has no value: {self._error}")

    @property
    def error(self) -> Exception:
        return self._error

    def map(self, f: Callable[[T], Result[T]]) -> Result[T]:
        return Failure(self._error)

    def flat_map(self, f: Callable[[T], Result[T]]) -> Result[T]:
        return Failure(self._error)


# =============================================================================
# SECTION 3: Event System (Observer Pattern)
# =============================================================================


@dataclass
class Event:
    """Base event class."""

    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)
    event_type: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)


class EventHandler(Protocol):
    """Protocol for event handlers."""

    async def handle(self, event: Event) -> None: ...


class EventBus:
    """
    Central event bus for pub/sub communication.

    Implements the Observer pattern for loose coupling.
    """

    def __init__(self):
        self._handlers: Dict[str, List[EventHandler]] = defaultdict(list)
        self._wildcards: List[EventHandler] = []
        self._lock = asyncio.Lock()

    async def subscribe(self, event_type: str, handler: EventHandler) -> None:
        """Subscribe to an event type."""
        async with self._lock:
            if "*" in event_type:
                self._wildcards.append(handler)
            else:
                self._handlers[event_type].append(handler)

    async def unsubscribe(self, event_type: str, handler: EventHandler) -> None:
        """Unsubscribe from an event type."""
        async with self._lock:
            if "*" in event_type:
                if handler in self._wildcards:
                    self._wildcards.remove(handler)
            else:
                if handler in self._handlers[event_type]:
                    self._handlers[event_type].remove(handler)

    async def publish(self, event: Event) -> None:
        """Publish an event to all handlers."""
        handlers = []

        async with self._lock:
            handlers.extend(self._handlers.get(event.event_type, []))
            handlers.extend(self._wildcards)

        for handler in handlers:
            try:
                await handler.handle(event)
            except Exception as e:
                logging.error(f"Event handler error: {e}")


# =============================================================================
# SECTION 4: Configuration Management (Singleton Pattern)
# =============================================================================


class Configuration(metaclass=Singleton):
    """
    Global configuration with environment override support.

    Singleton pattern ensures single source of truth.
    """

    def __init__(self):
        self._config: Dict[str, Any] = {
            # Storage
            "storage_dir": "/tmp/mem0_storage",
            "max_cache_size_gb": 10.0,
            "compression": "lz4",
            # Sandbox
            "sandbox_timeout": 300,
            "sandbox_memory_mb": 512,
            "sandbox_cpu_cores": 2,
            # Warm Pool
            "warm_pool_enabled": True,
            "warm_pool_size": 1000,
            "prefetch_enabled": True,
            # Runtime
            "runtime_dir": "/tmp/mem0_runtimes",
            "default_runtime": "python-ml",
            # Database
            "database_url": "postgresql://localhost/mem0",
            # Logging
            "log_level": "INFO",
            "log_format": "json",
        }

        # Override with environment
        self._load_env()

    def _load_env(self):
        """Load configuration from environment variables."""
        env_mappings = {
            "MEM0_STORAGE_DIR": "storage_dir",
            "MEM0_CACHE_SIZE_GB": "max_cache_size_gb",
            "MEM0_COMPRESSION": "compression",
            "MEM0_SANDBOX_TIMEOUT": "sandbox_timeout",
            "MEM0_WARM_POOL_SIZE": "warm_pool_size",
            "MEM0_LOG_LEVEL": "log_level",
        }

        for env_key, config_key in env_mappings.items():
            value = os.getenv(env_key)
            if value is not None:
                self._config[config_key] = self._parse_value(value)

    def _parse_value(self, value: str) -> Any:
        """Parse string value to appropriate type."""
        if value.lower() in ("true", "false"):
            return value.lower() == "true"
        try:
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            pass
        return value

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self._config.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set configuration value."""
        self._config[key] = value

    @property
    def storage_dir(self) -> str:
        return self.get("storage_dir")

    @property
    def max_cache_size_gb(self) -> float:
        return self.get("max_cache_size_gb")

    @property
    def warm_pool_enabled(self) -> bool:
        return self.get("warm_pool_enabled")

    @property
    def warm_pool_size(self) -> int:
        return self.get("warm_pool_size")

    @property
    def log_level(self) -> str:
        return self.get("log_level")

    def as_dict(self) -> Dict[str, Any]:
        """Get configuration as dictionary."""
        return dict(self._config)


# =============================================================================
# SECTION 5: Logger with Structured Output
# =============================================================================


class Logger:
    """
    Structured logger with context support.

    Provides JSON logging with automatic context enrichment.
    """

    def __init__(self, name: str = "mem0"):
        self._logger = logging.getLogger(name)
        self._context: Dict[str, Any] = {}
        self._configure()

    def _configure(self):
        """Configure logging."""
        config = Configuration()
        level = getattr(logging, config.log_level.upper(), logging.INFO)

        self._logger.setLevel(level)

        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
        )
        handler.setFormatter(formatter)

        if not self._logger.handlers:
            self._logger.addHandler(handler)

    def set_context(self, **kwargs) -> None:
        """Set context for all subsequent logs."""
        self._context.update(kwargs)

    def clear_context(self) -> None:
        """Clear logging context."""
        self._context.clear()

    def _log(self, level: int, message: str, **kwargs) -> None:
        """Log with context."""
        extra = {"context": json.dumps(self._context)}
        self._logger.log(level, message, extra=extra, **kwargs)

    def debug(self, message: str, **kwargs) -> None:
        self._log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs) -> None:
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs) -> None:
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message: str, **kwargs) -> None:
        self._log(logging.ERROR, message, **kwargs)

    def critical(self, message: str, **kwargs) -> None:
        self._log(logging.CRITICAL, message, **kwargs)

    def performance(self, operation: str, duration_ms: float, **kwargs) -> None:
        """Log performance metric."""
        self._log(
            logging.INFO,
            f"PERF: {operation} completed in {duration_ms:.2f}ms",
            operation=operation,
            duration_ms=duration_ms,
            **kwargs,
        )


# Create module logger
logger = Logger("mem0.platform")


# =============================================================================
# SECTION 6: Abstract Base Classes for Core Components
# =============================================================================


class IStorageBackend(ABC):
    """
    Abstract interface for storage backends.

    Strategy pattern for interchangeable storage implementations.
    """

    @abstractmethod
    async def write(self, key: str, data: bytes) -> bool: ...

    @abstractmethod
    async def read(self, key: str) -> Optional[bytes]: ...

    @abstractmethod
    async def delete(self, key: str) -> bool: ...

    @abstractmethod
    async def exists(self, key: str) -> bool: ...

    @abstractmethod
    async def list(self, prefix: str = "") -> List[str]: ...

    @abstractmethod
    async def get_stats(self) -> Dict[str, Any]: ...


class ICacheBackend(ABC):
    """Abstract interface for cache backends."""

    @abstractmethod
    async def get(self, key: str) -> Optional[Any]: ...

    @abstractmethod
    async def set(self, key: str, value: Any, ttl: int = 3600) -> bool: ...

    @abstractmethod
    async def delete(self, key: str) -> bool: ...

    @abstractmethod
    async def clear(self, pattern: str = "*") -> int: ...

    @abstractmethod
    async def get_stats(self) -> Dict[str, Any]: ...


class IScheduler(ABC):
    """Abstract interface for task scheduling."""

    @abstractmethod
    async def schedule(self, task: Callable, *args, priority: int = 5, delay_ms: int = 0, **kwargs) -> str: ...

    @abstractmethod
    async def cancel(self, task_id: str) -> bool: ...

    @abstractmethod
    async def get_queue_depth(self) -> int: ...


class ISandboxExecutor(ABC):
    """Abstract interface for sandbox execution."""

    @abstractmethod
    async def create(self, config: Dict[str, Any]) -> str: ...

    @abstractmethod
    async def execute(self, sandbox_id: str, code: str, timeout: int = 30) -> Dict[str, Any]: ...

    @abstractmethod
    async def delete(self, sandbox_id: str) -> bool: ...

    @abstractmethod
    async def get_stats(self, sandbox_id: str) -> Dict[str, Any]: ...


class IMemoryStore(ABC):
    """Abstract interface for memory storage."""

    @abstractmethod
    async def add(self, content: str, user_id: str, metadata: Optional[Dict] = None) -> str: ...

    @abstractmethod
    async def search(self, query: str, user_id: str, limit: int = 10) -> List[Dict[str, Any]]: ...

    @abstractmethod
    async def get_context(self, user_id: str, limit: int = 50) -> str: ...

    @abstractmethod
    async def delete(self, memory_id: str) -> bool: ...


class IRuntimeManager(ABC):
    """Abstract interface for runtime management."""

    @abstractmethod
    async def get_runtime(self, runtime_type: str) -> Dict[str, Any]: ...

    @abstractmethod
    async def warm_runtime(self, runtime_type: str) -> bool: ...

    @abstractmethod
    async def list_runtimes(self) -> List[Dict[str, Any]]: ...

    @abstractmethod
    async def resolve_dependencies(self, requirements: str, runtime_type: str) -> Dict[str, str]: ...


class IRepository(Generic[T], ABC):
    """
    Generic repository interface for data access.

    Repository pattern for data access abstraction.
    """

    @abstractmethod
    async def get(self, id: str) -> Optional[T]: ...

    @abstractmethod
    async def create(self, entity: T) -> T: ...

    @abstractmethod
    async def update(self, entity: T) -> T: ...

    @abstractmethod
    async def delete(self, id: str) -> bool: ...

    @abstractmethod
    async def list(self, **filters) -> List[T]: ...

    @abstractmethod
    async def count(self, **filters) -> int: ...


# =============================================================================
# SECTION 7: Concrete Implementations
# =============================================================================


class LocalFileStorage(IStorageBackend):
    """
    Local file system storage backend.

    Simple implementation for development and testing.
    """

    def __init__(self, base_dir: str = "/tmp/mem0_storage"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()

    async def write(self, key: str, data: bytes) -> bool:
        path = self.base_dir / key
        path.parent.mkdir(parents=True, exist_ok=True)

        async with self._lock:
            try:
                path.write_bytes(data)
                return True
            except Exception:
                return False

    async def read(self, key: str) -> Optional[bytes]:
        path = self.base_dir / key

        try:
            if path.exists():
                return path.read_bytes()
        except Exception:
            pass
        return None

    async def delete(self, key: str) -> bool:
        path = self.base_dir / key

        async with self._lock:
            try:
                if path.exists():
                    path.unlink()
                    return True
            except Exception:
                pass
        return False

    async def exists(self, key: str) -> bool:
        return (self.base_dir / key).exists()

    async def list(self, prefix: str = "") -> List[str]:
        prefix_path = self.base_dir / prefix

        if not prefix_path.exists():
            return []

        if prefix_path.is_dir():
            return [str(p.relative_to(self.base_dir)) for p in prefix_path.rglob("*")]
        return [prefix]

    async def get_stats(self) -> Dict[str, Any]:
        total_size = 0
        file_count = 0

        for path in self.base_dir.rglob("*"):
            if path.is_file():
                total_size += path.stat().st_size
                file_count += 1

        return {"total_bytes": total_size, "file_count": file_count, "storage_type": "local"}


class MemoryCache(ICacheBackend):
    """
    In-memory cache with LRU eviction.

    Fast cache for hot data.
    """

    def __init__(self, max_size: int = 10000, ttl: int = 3600):
        self.max_size = max_size
        self.default_ttl = ttl
        self._cache: Dict[str, tuple] = {}
        self._lru = deque()
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            if key not in self._cache:
                return None

            value, expiry = self._cache[key]

            if datetime.utcnow() > expiry:
                del self._cache[key]
                self._lru.remove(key)
                return None

            # Update LRU
            self._lru.remove(key)
            self._lru.append(key)

            return value

    async def set(self, key: str, value: Any, ttl: int = None) -> bool:
        ttl = ttl or self.default_ttl

        async with self._lock:
            if key in self._cache:
                self._lru.remove(key)

            expiry = datetime.utcnow() + timedelta(seconds=ttl)
            self._cache[key] = (value, expiry)
            self._lru.append(key)

            # Evict if over size
            while len(self._cache) > self.max_size:
                oldest_key = self._lru.popleft()
                del self._cache[oldest_key]

            return True

    async def delete(self, key: str) -> bool:
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                if key in self._lru:
                    self._lru.remove(key)
                return True
            return False

    async def clear(self, pattern: str = "*") -> int:
        async with self._lock:
            count = len(self._cache)
            self._cache.clear()
            self._lru.clear()
            return count

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            return {"items": len(self._cache), "max_size": self.max_size, "cache_type": "memory"}


# =============================================================================
# SECTION 8: Factory Pattern for Creating Instances
# =============================================================================


class StorageBackendFactory:
    """Factory for creating storage backend instances."""

    _implementations = {
        "local": LocalFileStorage,
        # Add S3, GCS, Azure implementations here
    }

    @classmethod
    def create(cls, backend_type: str = "local", **kwargs) -> IStorageBackend:
        """Create storage backend instance."""
        implementation = cls._implementations.get(backend_type)

        if not implementation:
            raise ValueError(f"Unknown storage backend: {backend_type}")

        return implementation(**kwargs)

    @classmethod
    def register(cls, name: str, implementation: Type[IStorageBackend]) -> None:
        """Register a new storage backend."""
        cls._implementations[name] = implementation

    @classmethod
    def list_available(cls) -> List[str]:
        """List available storage backends."""
        return list(cls._implementations.keys())


class CacheBackendFactory:
    """Factory for creating cache backend instances."""

    _implementations = {
        "memory": MemoryCache,
        # Add Redis, Memcached implementations here
    }

    @classmethod
    def create(cls, backend_type: str = "memory", **kwargs) -> ICacheBackend:
        implementation = cls._implementations.get(backend_type)

        if not implementation:
            raise ValueError(f"Unknown cache backend: {backend_type}")

        return implementation(**kwargs)

    @classmethod
    def register(cls, name: str, implementation: Type[ICacheBackend]) -> None:
        cls._implementations[name] = implementation

    @classmethod
    def list_available(cls) -> List[str]:
        return list(cls._implementations.keys())


# =============================================================================
# SECTION 9: Service Layer - Core Business Logic
# =============================================================================


class StorageService:
    """
    Storage service with caching layer.

    Service layer encapsulating storage business logic.
    """

    def __init__(self, storage_backend: IStorageBackend = None, cache_backend: ICacheBackend = None):
        self._storage = storage_backend or StorageBackendFactory.create()
        self._cache = cache_backend or CacheBackendFactory.create()
        self._event_bus = EventBus()
        self._logger = Logger("mem0.storage")

    @property
    def event_bus(self) -> EventBus:
        return self._event_bus

    async def write(self, key: str, data: bytes, use_cache: bool = True) -> Result[bool]:
        """Write data to storage with optional caching."""
        start = time.perf_counter()

        try:
            success = await self._storage.write(key, data)

            if success and use_cache:
                await self._cache.set(key, data)

            # Publish event
            await self._event_bus.publish(Event(event_type="storage.write", payload={"key": key, "size": len(data)}))

            self._logger.performance("storage.write", (time.perf_counter() - start) * 1000)

            return Result.success(success)

        except Exception as e:
            self._logger.error(f"Storage write failed: {e}")
            return Result.failure(e)

    async def read(self, key: str, use_cache: bool = True) -> Result[bytes]:
        """Read data from storage with cache fallback."""
        start = time.perf_counter()

        # Try cache first
        if use_cache:
            cached = await self._cache.get(key)
            if cached is not None:
                self._logger.performance("storage.read_cache_hit", (time.perf_counter() - start) * 1000)
                return Result.success(cached)

        # Read from storage
        try:
            data = await self._storage.read(key)

            if data is not None and use_cache:
                await self._cache.set(key, data)

            self._logger.performance("storage.read", (time.perf_counter() - start) * 1000)

            if data is not None:
                return Result.success(data)
            return Result.failure(FileNotFoundError(key))

        except Exception as e:
            self._logger.error(f"Storage read failed: {e}")
            return Result.failure(e)

    async def delete(self, key: str) -> Result[bool]:
        """Delete data from storage and cache."""
        try:
            storage_success = await self._storage.delete(key)
            cache_success = await self._cache.delete(key)

            return Result.success(storage_success or cache_success)

        except Exception as e:
            self._logger.error(f"Storage delete failed: {e}")
            return Result.failure(e)

    async def exists(self, key: str) -> bool:
        """Check if key exists in cache or storage."""
        cached = await self._cache.get(key)
        if cached is not None:
            return True
        return await self._storage.exists(key)

    async def get_stats(self) -> Dict[str, Any]:
        """Get storage and cache statistics."""
        return {"storage": await self._storage.get_stats(), "cache": await self._cache.get_stats()}


class SandboxService:
    """
    Sandbox management service.

    Service layer for sandbox lifecycle management.
    """

    def __init__(
        self,
        executor: ISandboxExecutor = None,
        storage_service: StorageService = None,
        runtime_manager: IRuntimeManager = None,
    ):
        self._executor = executor
        self._storage = storage_service or StorageService()
        self._runtime = runtime_manager
        self._event_bus = EventBus()
        self._logger = Logger("mem0.sandbox")
        self._active_sandboxes: Dict[str, Dict] = {}

    @property
    def event_bus(self) -> EventBus:
        return self._event_bus

    async def create_sandbox(self, user_id: str, runtime_type: str = "python-ml", **kwargs) -> Result[Dict[str, Any]]:
        """Create a new sandbox."""
        start = time.perf_counter()

        try:
            # Warm runtime if needed
            if self._runtime:
                await self._runtime.warm_runtime(runtime_type)

            # Create sandbox
            sandbox_id = await self._executor.create({"runtime": runtime_type, "user_id": user_id, **kwargs})

            sandbox_info = {
                "id": sandbox_id,
                "runtime": runtime_type,
                "user_id": user_id,
                "created_at": datetime.utcnow().isoformat(),
                "status": "ready",
            }

            self._active_sandboxes[sandbox_id] = sandbox_info

            # Publish event
            await self._event_bus.publish(Event(event_type="sandbox.created", payload=sandbox_info))

            self._logger.performance("sandbox.create", (time.perf_counter() - start) * 1000)

            return Result.success(sandbox_info)

        except Exception as e:
            self._logger.error(f"Sandbox creation failed: {e}")
            return Result.failure(e)

    async def execute(self, sandbox_id: str, code: str, timeout: int = 30) -> Result[Dict[str, Any]]:
        """Execute code in a sandbox."""
        start = time.perf_counter()

        try:
            result = await self._executor.execute(sandbox_id, code, timeout)

            self._logger.performance("sandbox.execute", (time.perf_counter() - start) * 1000)

            return Result.success(result)

        except Exception as e:
            self._logger.error(f"Sandbox execution failed: {e}")
            return Result.failure(e)

    async def delete_sandbox(self, sandbox_id: str) -> Result[bool]:
        """Delete a sandbox."""
        try:
            success = await self._executor.delete(sandbox_id)

            if sandbox_id in self._active_sandboxes:
                del self._active_sandboxes[sandbox_id]

            await self._event_bus.publish(Event(event_type="sandbox.deleted", payload={"sandbox_id": sandbox_id}))

            return Result.success(success)

        except Exception as e:
            self._logger.error(f"Sandbox deletion failed: {e}")
            return Result.failure(e)

    async def list_sandboxes(self, user_id: str = None) -> List[Dict[str, Any]]:
        """List active sandboxes."""
        if user_id:
            return [s for s in self._active_sandboxes.values() if s["user_id"] == user_id]
        return list(self._active_sandboxes.values())

    async def fork_sandbox(self, sandbox_id: str, new_user_id: str = None) -> Result[Dict[str, Any]]:
        """Fork a sandbox (O(1) operation with shared storage)."""
        try:
            source = self._active_sandboxes.get(sandbox_id)
            if not source:
                return Result.failure(FileNotFoundError(sandbox_id))

            # Create fork with shared storage
            fork_id = f"{sandbox_id}_fork_{uuid.uuid4().hex[:8]}"

            fork_info = {
                **source,
                "id": fork_id,
                "user_id": new_user_id or source["user_id"],
                "parent_id": sandbox_id,
                "created_at": datetime.utcnow().isoformat(),
                "forked": True,
            }

            self._active_sandboxes[fork_id] = fork_info

            return Result.success(fork_info)

        except Exception as e:
            self._logger.error(f"Sandbox fork failed: {e}")
            return Result.failure(e)


class MemoryService:
    """
    Memory management service.

    Service layer for memory operations.
    """

    def __init__(self, memory_store: IMemoryStore = None, storage_service: StorageService = None):
        self._store = memory_store
        self._storage = storage_service or StorageService()
        self._event_bus = EventBus()
        self._logger = Logger("mem0.memory")

    @property
    def event_bus(self) -> EventBus:
        return self._event_bus

    async def add_memory(
        self, content: str, user_id: str, category: str = "fact", importance: float = 0.5, **kwargs
    ) -> Result[Dict[str, Any]]:
        """Add a new memory."""
        try:
            memory_id = await self._store.add(
                content=content, user_id=user_id, metadata={"category": category, "importance": importance, **kwargs}
            )

            result = {"id": memory_id, "content": content, "category": category, "importance": importance}

            await self._event_bus.publish(Event(event_type="memory.added", payload=result))

            return Result.success(result)

        except Exception as e:
            self._logger.error(f"Memory add failed: {e}")
            return Result.failure(e)

    async def search_memories(self, query: str, user_id: str, limit: int = 10) -> Result[List[Dict[str, Any]]]:
        """Search memories."""
        try:
            results = await self._store.search(query=query, user_id=user_id, limit=limit)

            return Result.success(results)

        except Exception as e:
            self._logger.error(f"Memory search failed: {e}")
            return Result.failure(e)

    async def get_context(self, user_id: str, limit: int = 50) -> Result[str]:
        """Get context string for AI."""
        try:
            context = await self._store.get_context(user_id=user_id, limit=limit)
            return Result.success(context)

        except Exception as e:
            self._logger.error(f"Context retrieval failed: {e}")
            return Result.failure(e)


# =============================================================================
# SECTION 10: Platform Facade - Unified Interface
# =============================================================================


class Mem0Platform:
    """
    Main platform facade providing unified access to all services.

    Facade pattern for simplified client interaction.
    """

    def __init__(self):
        self._config = Configuration()
        self._logger = Logger("mem0")

        # Initialize services
        self._storage = StorageService()
        self._sandbox = SandboxService()
        self._memory = MemoryService()

        # Event bus for cross-service communication
        self._event_bus = EventBus()

        self._initialized = False

    @property
    def storage(self) -> StorageService:
        return self._storage

    @property
    def sandbox(self) -> SandboxService:
        return self._sandbox

    @property
    def memory(self) -> MemoryService:
        return self._memory

    @property
    def event_bus(self) -> EventBus:
        return self._event_bus

    @property
    def config(self) -> Configuration:
        return self._config

    async def initialize(self) -> None:
        """Initialize the platform."""
        if self._initialized:
            return

        self._logger.info("Initializing Mem0 Platform...")

        # Subscribe to events
        await self._event_bus.subscribe("sandbox.created", SandboxCreatedHandler())

        self._initialized = True
        self._logger.info("Mem0 Platform initialized")

    async def shutdown(self) -> None:
        """Shutdown the platform."""
        if not self._initialized:
            return

        self._logger.info("Shutting down Mem0 Platform...")
        self._initialized = False
        self._logger.info("Mem0 Platform shutdown complete")

    async def health_check(self) -> Dict[str, Any]:
        """Check platform health."""
        return {
            "status": "healthy" if self._initialized else "not_initialized",
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0",
            "services": {
                "storage": (await self._storage.get_stats()),
                "sandbox": {"active_count": len(self._sandbox._active_sandboxes)},
            },
        }

    async def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive platform statistics."""
        return {"platform": await self.health_check(), "config": self._config.as_dict()}


class SandboxCreatedHandler(EventHandler):
    """Handler for sandbox creation events."""

    async def handle(self, event: Event) -> None:
        """Handle sandbox created event."""
        logger = Logger("mem0.events")
        logger.info(f"Sandbox created: {event.payload.get('id')}")


# =============================================================================
# SECTION 11: Decorators for Common Operations
# =============================================================================


def retry(max_attempts: int = 3, delay_ms: int = 1000):
    """Decorator for retry logic."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(delay_ms / 1000)

            raise last_exception

        return wrapper

    return decorator


def measure_performance(logger: Logger = None):
    """Decorator for performance measurement."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.perf_counter()

            result = await func(*args, **kwargs)

            duration_ms = (time.perf_counter() - start) * 1000

            if logger:
                logger.performance(func.__name__, duration_ms)

            return result

        return wrapper

    return decorator


def singleton(cls):
    """Decorator for singleton pattern."""
    instances = {}

    @wraps(cls)
    def wrapper(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return wrapper


# =============================================================================
# SECTION 12: Main Entry Point
# =============================================================================


async def create_platform() -> Mem0Platform:
    """Create and initialize the Mem0 platform."""
    platform = Mem0Platform()
    await platform.initialize()
    return platform


async def main():
    """Main entry point for testing."""
    platform = await create_platform()

    # Run basic tests
    platform.logger.info("Running platform health check...")
    health = await platform.health_check()
    print(json.dumps(health, indent=2, default=str))

    # Create a sandbox
    platform.logger.info("Creating test sandbox...")
    result = await platform.sandbox.create_sandbox(user_id="test_user", runtime_type="python-ml")

    if result.is_success:
        sandbox = result.value
        platform.logger.info(f"Created sandbox: {sandbox['id']}")

    await platform.shutdown()

    return platform


if __name__ == "__main__":
    asyncio.run(main())


# =============================================================================
# SECTION 13: Export Public API
# =============================================================================

__all__ = [
    # Core Types
    "Result",
    "Success",
    "Failure",
    "Event",
    "EventBus",
    "EventHandler",
    # Configuration
    "Configuration",
    # Logging
    "Logger",
    # Abstract Interfaces
    "IStorageBackend",
    "ICacheBackend",
    "IScheduler",
    "ISandboxExecutor",
    "IMemoryStore",
    "IRuntimeManager",
    "IRepository",
    # Concrete Implementations
    "LocalFileStorage",
    "MemoryCache",
    # Factories
    "StorageBackendFactory",
    "CacheBackendFactory",
    # Services
    "StorageService",
    "SandboxService",
    "MemoryService",
    # Platform
    "Mem0Platform",
    "create_platform",
    # Decorators
    "retry",
    "measure_performance",
    "singleton",
]
