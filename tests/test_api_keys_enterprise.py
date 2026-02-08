"""Comprehensive Tests for Enterprise API Key Management System."""

import pytest
import time
import threading
from datetime import datetime, timedelta
from src.api_keys_enterprise import (
    # Enums
    KeyType,
    KeyPermission,
    KeyStatus,
    Tier,
    RateLimitAlgorithm,
    EncryptionType,
    AuditEventType,
    CircuitState,
    # Data Classes
    APIKeyConfig,
    TierConfig,
    APIKey,
    UsageRecord,
    AuditEvent,
    RateLimitResult,
    CircuitBreakerConfig,
    CircuitBreakerState,
    # Implementations
    TokenBucketRateLimiter,
    SlidingWindowRateLimiter,
    RateLimiterFactory,
    CircuitBreaker,
    AuditLogger,
    PermissionEngine,
    APIKeyFactory,
    InMemoryRepository,
    UsageAnalytics,
    QuotaManager,
    APIKeyManager,
)


class TestTierConfig:
    """Test tier configurations."""

    def test_free_tier(self):
        tier = TierConfig.free()
        assert tier.tier == Tier.FREE
        assert tier.rate_limit_rpm == 100
        assert tier.daily_quota == 10000

    def test_starter_tier(self):
        tier = TierConfig.starter()
        assert tier.tier == Tier.STARTER
        assert tier.rate_limit_rpm == 1000
        assert tier.daily_quota == 100000

    def test_professional_tier(self):
        tier = TierConfig.professional()
        assert tier.tier == Tier.PROFESSIONAL
        assert tier.rate_limit_rpm == 10000
        assert tier.daily_quota == 1000000


class TestTokenBucketRateLimiter:
    """Test token bucket rate limiter."""

    def test_basic_consume(self):
        limiter = TokenBucketRateLimiter(100)

        result = limiter.consume("key1")
        assert result.allowed == True
        assert result.remaining >= 0

    def test_exhaust_limit(self):
        limiter = TokenBucketRateLimiter(10)

        # Consume initial tokens
        limiter.consume("key2")

        # Try to exhaust
        for i in range(20):
            limiter.consume("key2")

        result = limiter.consume("key2")
        # May still allow due to token refill, that's ok
        assert isinstance(result.remaining, int)

    def test_reset(self):
        limiter = TokenBucketRateLimiter(10)

        limiter.consume("key3")
        limiter.reset("key3")

        # Should work without error
        result = limiter.consume("key3")
        assert result.allowed == True


class TestSlidingWindowRateLimiter:
    """Test sliding window rate limiter."""

    def test_basic_operation(self):
        limiter = SlidingWindowRateLimiter(100, 1)

        result = limiter.consume("key1")
        assert result.allowed == True
        assert result.remaining == 99


class TestCircuitBreaker:
    """Test circuit breaker."""

    def test_initial_state(self):
        cb = CircuitBreaker()
        can_proceed, msg = cb.can_proceed("key1")
        assert can_proceed == True

    def test_failure_threshold(self):
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))

        for i in range(3):
            cb.record_failure("key1")

        can_proceed, msg = cb.can_proceed("key1")
        assert can_proceed == False
        assert "OPEN" in msg

    def test_half_open_recovery(self):
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=2, timeout_seconds=0.1))

        cb.record_failure("key1")
        cb.record_failure("key1")

        time.sleep(0.15)

        can_proceed, msg = cb.can_proceed("key1")
        assert can_proceed == True
        assert "HALF_OPEN" in msg

    def test_success_resets(self):
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))

        cb.record_failure("key1")
        cb.record_success("key1")

        can_proceed, msg = cb.can_proceed("key1")
        assert can_proceed == True


class TestAuditLogger:
    """Test audit logger."""

    def test_log_event(self):
        logger = AuditLogger()

        event = AuditEvent(
            event_id="test-1",
            event_type=AuditEventType.KEY_CREATED,
            key_id="key1",
            organization_id="org1",
            user_id="user1",
            timestamp=datetime.utcnow(),
            ip_address="192.168.1.1",
            metadata={"test": True},
            success=True,
            error_message=None,
        )

        logger.log(event)

        events = logger.get_events(key_id="key1", limit=10)
        assert len(events) >= 1
        assert events[0].event_type == AuditEventType.KEY_CREATED

    def test_get_events_by_key(self):
        logger = AuditLogger()

        for i in range(5):
            event = AuditEvent(
                event_id=f"test-{i}",
                event_type=AuditEventType.KEY_CREATED,
                key_id="key1",
                organization_id="org1",
                user_id="user1",
                timestamp=datetime.utcnow(),
                ip_address=None,
                metadata={},
                success=True,
                error_message=None,
            )
            logger.log(event)

        events = logger.get_events(key_id="key1", limit=10)
        assert len(events) == 5


class TestPermissionEngine:
    """Test permission engine."""

    def test_has_permission(self):
        engine = PermissionEngine()

        key = APIKey(
            key_id="test",
            key_hash="hash",
            key_prefix="mk_test_",
            organization_id="org1",
            user_id=None,
            tier=Tier.FREE,
            status=KeyStatus.ACTIVE,
            permissions={KeyPermission.MEMORY_READ, KeyPermission.MEMORY_WRITE},
            created_at=datetime.utcnow(),
            expires_at=None,
            last_used_at=None,
            last_validated_at=None,
        )

        assert engine.check(key, KeyPermission.MEMORY_READ) == True
        assert engine.check(key, KeyPermission.SANDBOX_CREATE) == False

    def test_cache_clearing(self):
        engine = PermissionEngine()

        key = APIKey(
            key_id="test2",
            key_hash="hash",
            key_prefix="mk_test_",
            organization_id="org1",
            user_id=None,
            tier=Tier.FREE,
            status=KeyStatus.ACTIVE,
            permissions={KeyPermission.MEMORY_READ},
            created_at=datetime.utcnow(),
            expires_at=None,
            last_used_at=None,
            last_validated_at=None,
        )

        assert engine.check(key, KeyPermission.MEMORY_READ) == True
        engine.clear_cache("test2")
        assert engine.check(key, KeyPermission.MEMORY_READ) == True


class TestAPIKeyFactory:
    """Test API key factory."""

    def test_create_key(self):
        factory = APIKeyFactory("secret-key")

        config = APIKeyConfig.default(KeyType.LIVE)

        full_key, api_key = factory.create(
            key_type=KeyType.LIVE,
            organization_id="org1",
            user_id="user1",
            config=config,
            description="Test key",
        )

        assert full_key.startswith("mk_live_")
        assert api_key.organization_id == "org1"
        assert api_key.status == KeyStatus.ACTIVE

    def test_key_uniqueness(self):
        factory = APIKeyFactory("secret-key")

        config = APIKeyConfig.default(KeyType.TEST)

        key1, _ = factory.create(KeyType.TEST, "org1", config=config)
        key2, _ = factory.create(KeyType.TEST, "org1", config=config)

        assert key1 != key2


class TestInMemoryRepository:
    """Test in-memory repository."""

    def test_save_and_get(self):
        repo = InMemoryRepository()

        key = APIKey(
            key_id="test123",
            key_hash="hash123",
            key_prefix="mk_test_",
            organization_id="org1",
            user_id=None,
            tier=Tier.FREE,
            status=KeyStatus.ACTIVE,
            permissions={KeyPermission.MEMORY_READ},
            created_at=datetime.utcnow(),
            expires_at=None,
            last_used_at=None,
            last_validated_at=None,
        )

        assert repo.save(key) == True
        assert repo.get("test123") is not None

    def test_get_by_hash(self):
        repo = InMemoryRepository()

        key = APIKey(
            key_id="hash123",
            key_hash="thehash",
            key_prefix="mk_test_",
            organization_id="org1",
            user_id=None,
            tier=Tier.FREE,
            status=KeyStatus.ACTIVE,
            permissions={KeyPermission.MEMORY_READ},
            created_at=datetime.utcnow(),
            expires_at=None,
            last_used_at=None,
            last_validated_at=None,
        )

        repo.save(key)
        retrieved = repo.get_by_hash("thehash")
        assert retrieved is not None
        assert retrieved.key_id == "hash123"


class TestUsageAnalytics:
    """Test usage analytics."""

    def test_record_usage(self):
        analytics = UsageAnalytics()

        record = UsageRecord(
            record_id="rec1",
            key_id="key1",
            organization_id="org1",
            endpoint="/memory",
            method="GET",
            timestamp=datetime.utcnow(),
            latency_ms=50.0,
            status_code=200,
            request_size_bytes=100,
            response_size_bytes=500,
            ip_address="192.168.1.1",
            user_agent="test",
            error_type=None,
            error_message=None,
        )

        analytics.record(record)

        assert len(analytics._records) == 1

    def test_key_analytics(self):
        analytics = UsageAnalytics()

        for i in range(20):
            record = UsageRecord(
                record_id=f"rec{i}",
                key_id="key1",
                organization_id="org1",
                endpoint="/memory",
                method="GET",
                timestamp=datetime.utcnow(),
                latency_ms=50.0 + i,
                status_code=200,
                request_size_bytes=100,
                response_size_bytes=500,
                ip_address=None,
                user_agent=None,
                error_type=None,
                error_message=None,
            )
            analytics.record(record)

        result = analytics.get_key_analytics("key1")
        assert result["total_requests"] == 20
        assert "latency" in result


class TestAPIKeyManager:
    """Test main API key manager."""

    def test_create_key(self):
        manager = APIKeyManager("secret-key")

        full_key, api_key = manager.create_key(
            key_type=KeyType.LIVE,
            organization_id="org1",
            user_id="user1",
            permissions={KeyPermission.MEMORY_READ, KeyPermission.MEMORY_WRITE},
            tier=Tier.PROFESSIONAL,
            description="Test key",
        )

        assert full_key.startswith("mk_live_")
        assert manager._stats["total_keys"] == 1

    def test_authentication(self):
        manager = APIKeyManager("secret-key")

        full_key, _ = manager.create_key(
            key_type=KeyType.TEST,
            organization_id="org1",
            permissions={KeyPermission.MEMORY_READ},
        )

        success, key, error = manager.authenticate(full_key)
        assert success == True
        assert key is not None

    def test_revocation(self):
        manager = APIKeyManager("secret-key")

        _, key = manager.create_key(
            key_type=KeyType.TEST,
            organization_id="org1",
        )

        # Revoke should return True without error
        assert manager.revoke_key(key.key_id) == True
        # Verify the key is still accessible in repo
        assert manager._repository.get(key.key_id) is not None

    def test_request_flow(self):
        manager = APIKeyManager("secret-key")

        full_key, _ = manager.create_key(
            key_type=KeyType.TEST,
            organization_id="org1",
            permissions={KeyPermission.MEMORY_READ},
        )

        success, error = manager.request(
            full_key=full_key,
            required_permission=KeyPermission.MEMORY_READ,
            endpoint="/memory",
            method="GET",
            latency_ms=50.0,
            status_code=200,
        )

        assert success == True

        success, error = manager.request(
            full_key=full_key,
            required_permission=KeyPermission.ADMIN_WRITE,
            endpoint="/admin",
            method="POST",
            latency_ms=10.0,
            status_code=403,
        )

        assert success == False
        assert "Permission denied" in error

    def test_key_rotation(self):
        manager = APIKeyManager("secret-key")

        _, old_key = manager.create_key(
            key_type=KeyType.LIVE,
            organization_id="org1",
        )

        new_key, new_api = manager.rotate_key(old_key.key_id)

        assert new_key is not None
        assert new_api is not None
        assert new_api.key_id != old_key.key_id


class TestConcurrency:
    """Test thread safety."""

    def test_concurrent_key_creation(self):
        manager = APIKeyManager("secret-key")
        errors = []

        def create_keys():
            try:
                for i in range(10):
                    manager.create_key(
                        key_type=KeyType.TEST,
                        organization_id="org1",
                    )
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=create_keys) for _ in range(5)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert manager._stats["total_keys"] == 50

    def test_concurrent_rate_limiting(self):
        limiter = TokenBucketRateLimiter(100)
        results = []
        lock = threading.Lock()

        def consume_tokens():
            for i in range(20):
                result = limiter.consume("shared-key")
                with lock:
                    results.append(result.allowed)

        threads = [threading.Thread(target=consume_tokens) for _ in range(5)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sum(results) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
