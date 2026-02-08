"""
Tests for Mem0 Prometheus Metrics
=================================

Comprehensive tests for metrics functionality.
"""

import sys
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestMetricsConfig(unittest.TestCase):
    """Tests for MetricsConfig."""

    def test_default_config(self):
        """Test default configuration."""
        from src.monitoring.metrics import MetricsConfig

        config = MetricsConfig()

        self.assertEqual(config.namespace, "mem0")
        self.assertEqual(config.subsystem, "platform")
        self.assertTrue(config.include_process_metrics)
        self.assertTrue(config.include_python_metrics)
        self.assertGreater(len(config._metric_configs), 0)

    def test_custom_config(self):
        """Test custom configuration."""
        from src.monitoring.metrics import MetricsConfig

        config = MetricsConfig(
            namespace="custom",
            subsystem="test",
            include_process_metrics=False,
        )

        self.assertEqual(config.namespace, "custom")
        self.assertEqual(config.subsystem, "test")
        self.assertFalse(config.include_process_metrics)

    def test_default_metrics_registered(self):
        """Test that default metrics are registered."""
        from src.monitoring.metrics import MetricsConfig

        config = MetricsConfig()
        names = [m.name for m in config._metric_configs]

        self.assertIn("sandbox_created_total", names)
        self.assertIn("fork_latency_seconds", names)
        self.assertIn("snapshot_size_bytes", names)
        self.assertIn("api_request_total", names)


class TestMem0Metrics(unittest.TestCase):
    """Tests for Mem0Metrics."""

    def setUp(self):
        """Set up test fixtures."""
        from prometheus_client import CollectorRegistry
        from src.monitoring.metrics import Mem0Metrics, MetricsConfig

        self.registry = CollectorRegistry()
        self.config = MetricsConfig(namespace="test", subsystem="unit_test")
        self.metrics = Mem0Metrics(config=self.config, registry=self.registry)

    def test_record_fork(self):
        """Test recording fork operations."""
        self.metrics.record_fork(latency_ms=0.5, runtime="python-ml")
        self.metrics.record_fork(latency_ms=1.0, runtime="python-ml", status="error")

        metrics_output = self.metrics.get_metrics().decode()
        self.assertIn("fork_latency_seconds", metrics_output)
        self.assertIn("sandbox_fork_total", metrics_output)

    def test_record_clone(self):
        """Test recording clone operations."""
        self.metrics.record_clone(latency_ms=5.0, runtime="python-ml")
        self.metrics.record_clone(latency_ms=10.0, runtime="python-ml")

        metrics_output = self.metrics.get_metrics().decode()
        self.assertIn("clone_latency_seconds", metrics_output)
        self.assertIn("sandbox_clone_total", metrics_output)

    def test_record_snapshot(self):
        """Test recording snapshot operations."""
        self.metrics.record_snapshot(size_bytes=1024, user_id="user123")
        self.metrics.record_snapshot(size_bytes=2048, user_id="user456", incremental=True, success=True)

        metrics_output = self.metrics.get_metrics().decode()
        self.assertIn("snapshot_size_bytes", metrics_output)
        self.assertIn("snapshot_created_total", metrics_output)

    def test_record_block_stored(self):
        """Test recording block storage."""
        self.metrics.record_block_stored(size_bytes=65536, compression="lz4", tier="hot")
        self.metrics.record_block_stored(size_bytes=131072, compression="zstd", tier="cold")

        metrics_output = self.metrics.get_metrics().decode()
        self.assertIn("block_stored_total", metrics_output)
        self.assertIn("compression", metrics_output)

    def test_record_cache_hit_miss(self):
        """Test recording cache hits and misses."""
        self.metrics.record_cache_hit(cache_type="local")
        self.metrics.record_cache_hit(cache_type="redis")
        self.metrics.record_cache_miss(cache_type="local")

        metrics_output = self.metrics.get_metrics().decode()
        self.assertIn("cache_hit_total", metrics_output)
        self.assertIn("cache_miss_total", metrics_output)

    def test_record_api_request(self):
        """Test recording API requests."""
        self.metrics.record_api_request(
            endpoint="/api/v1/sandboxes",
            method="POST",
            status_code=201,
            tier="professional",
            latency_ms=50.0,
        )
        self.metrics.record_api_request(
            endpoint="/api/v1/sandboxes",
            method="GET",
            status_code=200,
            tier="free",
            latency_ms=25.0,
        )

        metrics_output = self.metrics.get_metrics().decode()
        self.assertIn("api_request_total", metrics_output)
        self.assertIn("api_request_latency_seconds", metrics_output)

    def test_record_api_key_created(self):
        """Test recording API key creation."""
        self.metrics.record_api_key_created(tier="professional", key_type="live")
        self.metrics.record_api_key_created(tier="enterprise", key_type="service")

        metrics_output = self.metrics.get_metrics().decode()
        self.assertIn("api_key_created_total", metrics_output)

    def test_record_rate_limit_exceeded(self):
        """Test recording rate limit exceeded."""
        self.metrics.record_rate_limit_exceeded(tier="free", endpoint="/api/v1/sandboxes")

        metrics_output = self.metrics.get_metrics().decode()
        self.assertIn("rate_limit_exceeded_total", metrics_output)

    def test_set_warm_pool_size(self):
        """Test setting warm pool size."""
        self.metrics.set_warm_pool_size(size=100, runtime="python-ml")
        self.metrics.set_warm_pool_size(size=50, runtime="node-web")

        metrics_output = self.metrics.get_metrics().decode()
        self.assertIn("warm_pool_size", metrics_output)

    def test_get_metrics_content_type(self):
        """Test getting content type."""
        content_type = self.metrics.get_content_type()
        self.assertIn("text/plain", content_type)

    def test_get_summary(self):
        """Test getting metrics summary."""
        summary = self.metrics.get_summary()

        self.assertIn("uptime_seconds", summary)
        self.assertIn("metrics_count", summary)
        self.assertGreater(summary["metrics_count"], 0)


class TestMetricsMiddleware(unittest.TestCase):
    """Tests for MetricsMiddleware."""

    def test_middleware_init(self):
        """Test middleware initialization."""
        from src.monitoring.metrics import Mem0Metrics, MetricsMiddleware, MetricsConfig
        from prometheus_client import CollectorRegistry

        config = MetricsConfig(namespace="test", subsystem="middleware_test")
        registry = CollectorRegistry()
        metrics = Mem0Metrics(config=config, registry=registry)

        mock_app = MagicMock()
        middleware = MetricsMiddleware(mock_app, metrics)

        self.assertEqual(middleware.app, mock_app)
        self.assertEqual(middleware.metrics, metrics)


class TestTrackOperationDecorator(unittest.TestCase):
    """Tests for track_operation decorator."""

    def test_sync_function(self):
        """Test decorator on synchronous function."""
        from src.monitoring.metrics import track_operation

        @track_operation("test_operation")
        def sample_function(x, y):
            return x + y

        result = sample_function(1, 2)
        self.assertEqual(result, 3)

    def test_async_function(self):
        """Test decorator on asynchronous function."""
        import asyncio
        from src.monitoring.metrics import track_operation

        @track_operation("async_test")
        async def async_function(value):
            await asyncio.sleep(0.01)
            return value * 2

        result = asyncio.run(async_function(5))
        self.assertEqual(result, 10)


class TestGlobalMetrics(unittest.TestCase):
    """Tests for global metrics instance."""

    def test_init_metrics(self):
        """Test init_metrics function."""
        from src.monitoring.metrics import init_metrics

        metrics = init_metrics(namespace="init", subsystem="test")

        self.assertIsNotNone(metrics)
        self.assertEqual(metrics.config.namespace, "init")


class TestPrometheusAvailability(unittest.TestCase):
    """Tests for Prometheus availability checking."""

    def test_prometheus_available_check(self):
        """Test Prometheus availability flag."""
        from src.monitoring.metrics import PROMETHEUS_AVAILABLE

        self.assertTrue(PROMETHEUS_AVAILABLE)


if __name__ == "__main__":
    unittest.main()
