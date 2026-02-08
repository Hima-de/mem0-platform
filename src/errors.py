"""Error Handling - Custom Exceptions with Codes and Recovery."""

from enum import Enum
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime
import json


class ErrorCode(str, Enum):
    """Standardized error codes."""
    
    # Memory errors (MEM-*)
    MEMORY_NOT_FOUND = "MEM001"
    MEMORY_CREATE_FAILED = "MEM002"
    MEMORY_UPDATE_FAILED = "MEM003"
    MEMORY_DELETE_FAILED = "MEM004"
    MEMORY_SEARCH_FAILED = "MEM005"
    MEMORY Corrupted = "MEM006"
    
    # Runtime errors (RT-*)
    RUNTIME_NOT_FOUND = "RT001"
    RUNTIME_WARM_FAILED = "RT002"
    RUNTIME_DOWNLOAD_FAILED = "RT003"
    RUNTIME_VERIFY_FAILED = "RT004"
    
    # Storage errors (ST-*)
    STORAGE_NOT_FOUND = "ST001"
    STORAGE_CREATE_FAILED = "ST002"
    STORAGE_DELETE_FAILED = "ST003"
    STORAGE Corrupted = "ST004"
    STORAGE_FULL = "ST005"
    STORAGE_QUOTA_EXCEEDED = "ST006"
    
    # Sandbox errors (SB-*)
    SANDBOX_NOT_FOUND = "SB001"
    SANDBOX_CREATE_FAILED = "SB002"
    SANDBOX_EXECUTE_FAILED = "SB003"
    SANDBOX_TIMEOUT = "SB004"
    SANDBOX_MEMORY_EXCEEDED = "SB005"
    SANDBOX_PERMISSION_DENIED = "SB006"
    SANDBOX_NETWORK_ERROR = "SB007"
    
    # Auth errors (AUTH-*)
    AUTH_INVALID_TOKEN = "AUTH001"
    AUTH_EXPIRED_TOKEN = "AUTH002"
    AUTH_PERMISSION_DENIED = "AUTH003"
    AUTH_RATE_LIMITED = "AUTH004"
    
    # System errors (SYS-*)
    SYS_DATABASE_ERROR = "SYS001"
    SYS_NETWORK_ERROR = "SYS002"
    SYS_TIMEOUT = "SYS003"
    SYS_INTERNAL_ERROR = "SYS999"


class Mem0Exception(Exception):
    """Base exception for mem0 platform."""
    
    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.SYS_INTERNAL_ERROR,
        details: Optional[Dict] = None,
        recoverable: bool = True,
        cause: Optional[Exception] = None,
    ):
        self.message = message
        self.code = code
        self.details = details or {}
        self.recoverable = recoverable
        self.cause = cause
        self.timestamp = datetime.utcnow().isoformat()
        super().__init__(message)

    def to_dict(self) -> Dict:
        return {
            "error": {
                "code": self.code.value,
                "message": self.message,
                "details": self.details,
                "recoverable": self.recoverable,
                "timestamp": self.timestamp,
            }
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    def with_details(self, **kwargs) -> "Mem0Exception":
        """Add additional details to exception."""
        self.details.update(kwargs)
        return self


class MemoryException(Mem0Exception):
    """Memory-related exceptions."""
    
    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.MEMORY_NOT_FOUND,
        **kwargs,
    ):
        super().__init__(message, code, recoverable=True, **kwargs)


class RuntimeException(Mem0Exception):
    """Runtime-related exceptions."""
    
    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.RUNTIME_NOT_FOUND,
        **kwargs,
    ):
        super().__init__(message, code, recoverable=True, **kwargs)


class StorageException(Mem0Exception):
    """Storage-related exceptions."""
    
    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.STORAGE_NOT_FOUND,
        recoverable: bool = True,
        **kwargs,
    ):
        super().__init__(message, code, recoverable=recoverable, **kwargs)


class SandboxException(Mem0Exception):
    """Sandbox-related exceptions."""
    
    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.SANDBOX_NOT_FOUND,
        **kwargs,
    ):
        super().__init__(message, code, recoverable=True, **kwargs)


class AuthenticationException(Mem0Exception):
    """Authentication-related exceptions."""
    
    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.AUTH_INVALID_TOKEN,
        **kwargs,
    ):
        super().__init__(message, code, recoverable=False, **kwargs)


class SystemException(Mem0Exception):
    """System-level exceptions."""
    
    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.SYS_INTERNAL_ERROR,
        recoverable: bool = True,
        **kwargs,
    ):
        super().__init__(message, code, recoverable=recoverable, **kwargs)


class RetryableError(Mem0Exception):
    """Marker for errors that can be retried."""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            code=ErrorCode.SYS_NETWORK_ERROR,
            recoverable=True,
            **kwargs,
        )


@dataclass
class ErrorContext:
    """Context for error handling."""
    operation: str
    component: str
    user_id: Optional[str] = None
    request_id: Optional[str] = None
    session_id: Optional[str] = None
    attempt: int = 1
    max_attempts: int = 3


class ErrorHandler:
    """Centralized error handler with recovery strategies."""
    
    @staticmethod
    def handle(
        exception: Exception,
        context: ErrorContext,
        strategy: str = "retry",
    ) -> Dict:
        """Handle exception with specified strategy."""
        
        strategies = {
            "retry": ErrorHandler._retry_strategy,
            "fallback": ErrorHandler._fallback_strategy,
            "circuit_breaker": ErrorHandler._circuit_breaker_strategy,
            "escalate": ErrorHandler._escalate_strategy,
        }
        
        handler = strategies.get(strategy, ErrorHandler._retry_strategy)
        return handler(exception, context)

    @staticmethod
    def _retry_strategy(exception: Exception, context: ErrorContext) -> Dict:
        """Retry with exponential backoff."""
        if context.attempt >= context.max_attempts:
            return {
                "action": "fail",
                "message": f"Max retries ({context.max_attempts}) exceeded",
                "retry_after": None,
            }
        
        backoff = min(2 ** context.attempt * 100, 10000)
        
        return {
            "action": "retry",
            "message": str(exception),
            "retry_after": backoff,
            "attempt": context.attempt + 1,
        }

    @staticmethod
    def _fallback_strategy(exception: Exception, context: ErrorContext) -> Dict:
        """Use fallback path."""
        return {
            "action": "fallback",
            "message": str(exception),
            "fallback_available": True,
        }

    @staticmethod
    def _circuit_breaker_strategy(exception: Exception, context: ErrorContext) -> Dict:
        """Open circuit breaker."""
        return {
            "action": "circuit_open",
            "message": "Circuit breaker opened",
            "retry_after": 30000,
        }

    @staticmethod
    def _escalate_strategy(exception: Exception, context: ErrorContext) -> Dict:
        """Escalate to supervisor."""
        return {
            "action": "escalate",
            "message": str(exception),
            "priority": "high",
        }


def error_boundary(operation: str, component: str):
    """Decorator for error boundaries."""
    def decorator(func):
        import functools
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Mem0Exception:
                raise
            except Exception as e:
                context = ErrorContext(
                    operation=operation,
                    component=component,
                )
                result = ErrorHandler.handle(e, context)
                if result["action"] == "fail":
                    raise SystemException(
                        f"Operation {operation} failed: {e}",
                        cause=e,
                    )
                raise
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Mem0Exception:
                raise
            except Exception as e:
                context = ErrorContext(
                    operation=operation,
                    component=component,
                )
                result = ErrorHandler.handle(e, context)
                if result["action"] == "fail":
                    raise SystemException(
                        f"Operation {operation} failed: {e}",
                        cause=e,
                    )
                raise
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


import asyncio
