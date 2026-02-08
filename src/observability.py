"""Observability - Structured Logging, Metrics, and Tracing."""

import logging
import time
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Callable, Dict, Optional, TypeVar, Union
from datetime import datetime
from enum import Enum
from pathlib import Path

import json


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class LogEntry:
    """Structured log entry."""

    timestamp: str
    level: str
    message: str
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    service: str = "mem0-platform"
    version: str = "1.0.0"
    attributes: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "timestamp": self.timestamp,
            "level": self.level,
            "message": self.message,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "service": self.service,
            "version": self.version,
            "attributes": self.attributes,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


class TraceContext:
    """Distributed tracing context."""

    _trace_id: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)
    _span_id: ContextVar[Optional[str]] = ContextVar("span_id", default=None)
    _parent_span_id: ContextVar[Optional[str]] = ContextVar("parent_span_id", default=None)

    @classmethod
    def get_trace_id(cls) -> Optional[str]:
        return cls._trace_id.get()

    @classmethod
    def get_span_id(cls) -> Optional[str]:
        return cls._span_id.get()

    @classmethod
    def start_span(cls, name: str) -> "Span":
        trace_id = cls._trace_id.get() or str(uuid.uuid4())
        span_id = str(uuid.uuid4())[:16]
        parent_id = cls._span_id.get()

        cls._trace_id.set(trace_id)
        cls._parent_span_id.set(cls._span_id.get())
        cls._span_id.set(span_id)

        return Span(
            name=name,
            trace_id=trace_id,
            span_id=span_id,
            parent_id=parent_id,
        )


class Span:
    """Tracing span."""

    def __init__(
        self,
        name: str,
        trace_id: str,
        span_id: str,
        parent_id: Optional[str] = None,
    ):
        self.name = name
        self.trace_id = trace_id
        self.span_id = span_id
        self.parent_id = parent_id
        self.start_time = time.time()
        self.end_time: Optional[float] = None
        self.attributes: Dict[str, Any] = {}
        self.events: list = []
        self.status: str = "ok"

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def add_event(self, name: str, attributes: Dict[str, Any] = {}) -> None:
        self.events.append(
            {
                "name": name,
                "timestamp": datetime.utcnow().isoformat(),
                "attributes": attributes,
            }
        )

    def set_status(self, status: str) -> None:
        self.status = status

    def finish(self) -> Dict:
        self.end_time = time.time()
        return {
            "name": self.name,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_id": self.parent_id,
            "duration_ms": (self.end_time - self.start_time) * 1000,
            "attributes": self.attributes,
            "events": self.events,
            "status": self.status,
        }


class StructuredLogger:
    """Structured JSON logger."""

    def __init__(
        self,
        service: str = "mem0-platform",
        log_level: str = "INFO",
        log_file: Optional[str] = None,
    ):
        self.service = service
        self.log_level = getattr(logging, log_level.upper(), logging.INFO)
        self.log_file = log_file

        self.logger = logging.getLogger(service)
        self.logger.setLevel(self.log_level)
        self.logger.handlers = []

        formatter = logging.Formatter("%(message)s")

        if log_file:
            Path(log_file).parent.mkdir(parents=True, exist_ok=True)
            handler = logging.FileHandler(log_file)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def _log(
        self,
        level: str,
        message: str,
        attributes: Dict[str, Any] = None,
        trace_id: Optional[str] = None,
        span_id: Optional[str] = None,
    ) -> LogEntry:
        entry = LogEntry(
            timestamp=datetime.utcnow().isoformat(),
            level=level,
            message=message,
            trace_id=trace_id or TraceContext.get_trace_id(),
            span_id=span_id or TraceContext.get_span_id(),
            service=self.service,
            attributes=attributes or {},
        )

        log_method = getattr(self.logger, level.lower())
        log_method(entry.to_json())

        return entry

    def debug(self, message: str, **kwargs) -> LogEntry:
        return self._log("DEBUG", message, kwargs)

    def info(self, message: str, **kwargs) -> LogEntry:
        return self._log("INFO", message, kwargs)

    def warning(self, message: str, **kwargs) -> LogEntry:
        return self._log("WARNING", message, kwargs)

    def error(self, message: str, **kwargs) -> LogEntry:
        return self._log("ERROR", message, kwargs)

    def critical(self, message: str, **kwargs) -> LogEntry:
        return self._log("CRITICAL", message, kwargs)

    def log_operation(self, operation: str, **kwargs):
        """Decorator for logging operations."""

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            async def async_wrapper(*args, **kw):
                with self.span(operation):
                    return await func(*args, **kw)

            @wraps(func)
            def sync_wrapper(*args, **kw):
                with self.span(operation):
                    return func(*args, **kw)

            return (
                async_wrapper
                if callable(func) and hasattr(func, "__wrapped__")
                else (async_wrapper if callable(func) else func)
            )

        return decorator

    def span(self, name: str, attributes: Dict[str, Any] = None) -> Span:
        """Create a tracing span."""
        span = TraceContext.start_span(name)
        if attributes:
            for k, v in attributes.items():
                span.set_attribute(k, v)
        return span


class Metrics:
    """Metrics collector."""

    def __init__(self):
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, list] = {}
        self._start_times: Dict[str, float] = {}

    def counter(self, name: str, value: float = 1) -> None:
        if name not in self._counters:
            self._counters[name] = 0
        self._counters[name] += value

    def gauge(self, name: str, value: float) -> None:
        self._gauges[name] = value

    def histogram(self, name: str, value: float) -> None:
        if name not in self._histograms:
            self._histograms[name] = []
        self._histograms[name].append(value)

    def time(self, name: str) -> None:
        self._start_times[name] = time.time()

    def stop_time(self, name: str) -> float:
        if name in self._start_times:
            duration = time.time() - self._start_times[name]
            self.histogram(name, duration * 1000)
            del self._start_times[name]
            return duration * 1000
        return 0

    def get_stats(self) -> Dict:
        return {
            "counters": self._counters.copy(),
            "gauges": self._gauges.copy(),
            "histograms": {
                k: {
                    "count": len(v),
                    "sum": sum(v),
                    "avg": sum(v) / len(v) if v else 0,
                    "min": min(v) if v else 0,
                    "max": max(v) if v else 0,
                    "p50": sorted(v)[len(v) // 2] if v else 0,
                    "p95": sorted(v)[int(len(v) * 0.95)] if v else 0,
                    "p99": sorted(v)[int(len(v) * 0.99)] if v else 0,
                }
                for k, v in self._histograms.items()
            },
        }


# Global instances
logger = StructuredLogger()
metrics = Metrics()


# Convenience functions
def get_logger(name: str) -> StructuredLogger:
    return StructuredLogger(service=name)


def traced(name: str):
    """Decorator to trace a function."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            with logger.span(name):
                return await func(*args, **kwargs)

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            with logger.span(name):
                return func(*args, **kwargs)

        return (
            async_wrapper
            if callable(func) and hasattr(func, "__wrapped__")
            else (async_wrapper if callable(func) else func)
        )

    return decorator
