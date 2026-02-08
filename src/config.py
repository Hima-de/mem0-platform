"""Serious Configuration Management with Validation."""

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path
import json
import yaml


class ConfigError(Exception):
    """Configuration error."""


@dataclass
class MemoryConfig:
    """Memory subsystem configuration."""

    db_path: str = "/tmp/mem0.db"
    max_connections: int = 10
    pool_size: int = 5
    connection_timeout: int = 30
    enable_fts: bool = True
    fts_language: str = "porter"

    def validate(self):
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        if self.max_connections <= 0:
            raise ConfigError(f"max_connections must be positive: {self.max_connections}")
        if self.pool_size <= 0:
            raise ConfigError(f"pool_size must be positive: {self.pool_size}")


@dataclass
class RuntimeConfig:
    """Runtime distribution configuration."""

    cache_dir: str = "/tmp/runtime-cache"
    hot_cache_size: int = 10
    cdn_base: str = "https://cdn.mem0.ai/runtimes"
    download_timeout: int = 60
    max_concurrent_downloads: int = 5
    verify_checksums: bool = True

    def validate(self):
        if self.hot_cache_size <= 0:
            raise ConfigError(f"hot_cache_size must be positive: {self.hot_cache_size}")
        if self.max_concurrent_downloads <= 0:
            raise ConfigError(f"max_concurrent_downloads must be positive: {self.max_concurrent_downloads}")


@dataclass
class StorageConfig:
    """Storage/snapshot configuration."""

    storage_path: str = "/tmp/sandbox-blocks"
    metadata_db: str = "/tmp/snapshots.db"
    hot_threshold_mb: int = 1000
    compression_enabled: bool = True
    compression_min_size: int = 1024
    compression_threshold: float = 0.9
    block_cleanup_interval: int = 3600
    orphan_retention_days: int = 7

    def validate(self):
        if self.hot_threshold_mb < 1:
            raise ConfigError(f"hot_threshold_mb too small: {self.hot_threshold_mb}")
        if self.hot_threshold_mb > 100000:
            raise ConfigError(f"hot_threshold_mb too large: {self.hot_threshold_mb}")


@dataclass
class SandboxConfig:
    """Sandbox execution configuration."""

    workdir: str = "/tmp/sandbox-workdir"
    max_execution_time: int = 300
    max_memory_mb: int = 512
    max_output_size: int = 1024 * 1024
    network_enabled: bool = True
    env_whitelist: List[str] = field(default_factory=lambda: ["PATH", "HOME"])
    allowed_commands: List[str] = field(default_factory=lambda: ["python", "node"])

    def validate(self):
        if self.max_execution_time <= 0:
            raise ConfigError(f"max_execution_time must be positive: {self.max_execution_time}")
        if self.max_memory_mb <= 0:
            raise ConfigError(f"max_memory_mb must be positive: {self.max_memory_mb}")


@dataclass
class SecurityConfig:
    """Security configuration."""

    api_key: Optional[str] = None
    jwt_secret: Optional[str] = None
    jwt_algorithm: str = "HS256"
    jwt_expiry_hours: int = 24
    rate_limit_requests: int = 100
    rate_limit_window: int = 60
    enable_cors: bool = True
    cors_origins: List[str] = field(default_factory=lambda: ["*"])

    def validate(self):
        if self.jwt_expiry_hours < 1 or self.jwt_expiry_hours > 720:
            raise ConfigError(f"jwt_expiry_hours must be 1-720: {self.jwt_expiry_hours}")


@dataclass
class ObservabilityConfig:
    """Observability configuration."""

    log_level: str = "INFO"
    log_format: str = "json"
    enable_tracing: bool = False
    tracing_sample_rate: float = 0.01
    metrics_enabled: bool = True
    metrics_port: int = 9090
    health_check_interval: int = 30

    def validate(self):
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level.upper() not in valid_levels:
            raise ConfigError(f"Invalid log level: {self.log_level}. Valid: {valid_levels}")


@dataclass
class PlatformConfig:
    """Top-level platform configuration."""

    memory: MemoryConfig = field(default_factory=MemoryConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    sandbox: SandboxConfig = field(default_factory=SandboxConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)

    @classmethod
    def from_env(cls) -> "PlatformConfig":
        """Load configuration from environment variables."""
        config = cls()

        env_overrides = {
            "MEM0_DB_PATH": ("memory", "db_path"),
            "MEM0_CACHE_DIR": ("runtime", "cache_dir"),
            "MEM0_STORAGE_PATH": ("storage", "storage_path"),
            "MEM0_API_KEY": ("security", "api_key"),
            "MEM0_LOG_LEVEL": ("observability", "log_level"),
        }

        for env_var, (section, attr) in env_overrides.items():
            value = os.environ.get(env_var)
            if value:
                section_config = getattr(config, section)
                setattr(section_config, attr, value)

        return config

    @classmethod
    def from_file(cls, path: str) -> "PlatformConfig":
        """Load configuration from YAML/JSON file."""
        p = Path(path)
        if not p.exists():
            raise ConfigError(f"Config file not found: {path}")

        with open(p) as f:
            data = yaml.safe_load(f) or {}

        config = cls()
        for section, values in data.items():
            if hasattr(config, section):
                section_obj = getattr(config, section)
                for key, value in values.items():
                    if hasattr(section_obj, key):
                        setattr(section_obj, key, value)

        return config

    def validate(self) -> bool:
        """Validate entire configuration."""
        errors = []

        for section_name in ["memory", "runtime", "storage", "sandbox", "security", "observability"]:
            section = getattr(self, section_name)
            try:
                section.validate()
            except ConfigError as e:
                errors.append(f"{section_name}: {e}")

        if errors:
            raise ConfigError(f"Configuration errors:\n" + "\n".join(errors))

        return True


def create_default_config() -> PlatformConfig:
    """Create default platform configuration."""
    return PlatformConfig()
