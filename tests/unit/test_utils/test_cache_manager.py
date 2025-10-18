"""
Unit tests for cache manager
"""

import pytest
import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch, MagicMock

from utils.cache import CacheManager
from utils.exceptions import CacheError


@pytest.mark.unit
class TestCacheManager:
    """Test cases for CacheManager class"""

    @pytest.fixture
    def cache_config(self):
        """Configuration for cache manager"""
        return {
            "enabled": True,
            "backend": "memory",  # Use memory backend for testing
            "ttl": 300,  # 5 minutes
            "max_size": 1000,
            "cleanup_interval": 60
        }

    @pytest.fixture
    async def cache_manager(self, cache_config):
        """Create CacheManager instance for testing"""
        cache = CacheManager(cache_config)
        await cache.initialize()
        return cache

    @pytest.mark.asyncio
    async def test_initialize(self, cache_manager):
        """Test cache manager initialization"""
        assert cache_manager is not None
        assert cache_manager.is_enabled() is True

    @pytest.mark.asyncio
    async def test_set_and_get(self, cache_manager):
        """Test setting and getting cache values"""
        key = "test_key"
        value = "test_value"

        # Set value
        await cache_manager.set(key, value)

        # Get value
        retrieved = await cache_manager.get(key)
        assert retrieved == value

    @pytest.mark.asyncio
    async def test_get_nonexistent_key(self, cache_manager):
        """Test getting non-existent key"""
        result = await cache_manager.get("nonexistent_key")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_with_default(self, cache_manager):
        """Test getting key with default value"""
        result = await cache_manager.get("nonexistent_key", default="default_value")
        assert result == "default_value"

    @pytest.mark.asyncio
    async def test_set_with_ttl(self, cache_manager):
        """Test setting value with TTL"""
        key = "ttl_key"
        value = "ttl_value"
        ttl = 2  # 2 seconds

        # Set value with TTL
        await cache_manager.set(key, value, ttl=ttl)

        # Get immediately
        retrieved = await cache_manager.get(key)
        assert retrieved == value

        # Wait for expiration
        await asyncio.sleep(ttl + 0.5)

        # Should be expired
        retrieved = await cache_manager.get(key)
        assert retrieved is None

    @pytest.mark.asyncio
    async def test_delete_key(self, cache_manager):
        """Test deleting cache key"""
        key = "delete_key"
        value = "delete_value"

        # Set value
        await cache_manager.set(key, value)
        assert await cache_manager.get(key) == value

        # Delete key
        await cache_manager.delete(key)

        # Should be deleted
        result = await cache_manager.get(key)
        assert result is None

    @pytest.mark.asyncio
    async def test_clear_cache(self, cache_manager):
        """Test clearing entire cache"""
        # Set multiple values
        await cache_manager.set("key1", "value1")
        await cache_manager.set("key2", "value2")
        await cache_manager.set("key3", "value3")

        # Verify values exist
        assert await cache_manager.get("key1") == "value1"
        assert await cache_manager.get("key2") == "value2"
        assert await cache_manager.get("key3") == "value3"

        # Clear cache
        await cache_manager.clear()

        # All values should be gone
        assert await cache_manager.get("key1") is None
        assert await cache_manager.get("key2") is None
        assert await cache_manager.get("key3") is None

    @pytest.mark.asyncio
    async def test_exists_key(self, cache_manager):
        """Test checking if key exists"""
        key = "exists_key"
        value = "exists_value"

        # Should not exist initially
        assert await cache_manager.exists(key) is False

        # Set value
        await cache_manager.set(key, value)

        # Should exist now
        assert await cache_manager.exists(key) is True

        # Delete value
        await cache_manager.delete(key)

        # Should not exist again
        assert await cache_manager.exists(key) is False

    @pytest.mark.asyncio
    async def test_increment_value(self, cache_manager):
        """Test incrementing numeric values"""
        key = "increment_key"

        # Initialize counter
        await cache_manager.set(key, 0)

        # Increment
        result = await cache_manager.increment(key)
        assert result == 1
        assert await cache_manager.get(key) == 1

        # Increment with delta
        result = await cache_manager.increment(key, delta=5)
        assert result == 6
        assert await cache_manager.get(key) == 6

    @pytest.mark.asyncio
    async def test_decrement_value(self, cache_manager):
        """Test decrementing numeric values"""
        key = "decrement_key"

        # Initialize counter
        await cache_manager.set(key, 10)

        # Decrement
        result = await cache_manager.decrement(key)
        assert result == 9
        assert await cache_manager.get(key) == 9

        # Decrement with delta
        result = await cache_manager.decrement(key, delta=5)
        assert result == 4
        assert await cache_manager.get(key) == 4

    @pytest.mark.asyncio
    async def test_set_many_get_many(self, cache_manager):
        """Test setting and getting multiple values"""
        data = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3"
        }

        # Set multiple values
        await cache_manager.set_many(data)

        # Get multiple values
        results = await cache_manager.get_many(list(data.keys()))
        assert results == data

    @pytest.mark.asyncio
    async def test_delete_many(self, cache_manager):
        """Test deleting multiple keys"""
        keys = ["delete_key1", "delete_key2", "delete_key3"]

        # Set values
        for key in keys:
            await cache_manager.set(key, f"value_{key}")

        # Verify values exist
        for key in keys:
            assert await cache_manager.get(key) is not None

        # Delete multiple keys
        await cache_manager.delete_many(keys)

        # Verify values are deleted
        for key in keys:
            assert await cache_manager.get(key) is None

    @pytest.mark.asyncio
    async def test_cache_size_limit(self, cache_manager):
        """Test cache size limit enforcement"""
        # This depends on the specific cache implementation
        # Test with small cache size
        small_cache_config = {
            "enabled": True,
            "backend": "memory",
            "max_size": 3  # Very small cache
        }

        small_cache = CacheManager(small_cache_config)
        await small_cache.initialize()

        # Add items beyond limit
        for i in range(5):
            await small_cache.set(f"key_{i}", f"value_{i}")

        # Check cache size (implementation specific)
        cache_info = await small_cache.get_info()
        assert cache_info["size"] <= 3

    @pytest.mark.asyncio
    async def test_cache_statistics(self, cache_manager):
        """Test cache statistics"""
        # Perform some operations
        await cache_manager.set("stat_key", "stat_value")
        await cache_manager.get("stat_key")
        await cache_manager.get("nonexistent_key")
        await cache_manager.delete("stat_key")

        # Get statistics
        stats = await cache_manager.get_stats()

        assert "hits" in stats
        assert "misses" in stats
        assert "sets" in stats
        assert "deletes" in stats
        assert stats["hits"] >= 1
        assert stats["misses"] >= 1

    @pytest.mark.asyncio
    async def test_cache_ttl_persistence(self, cache_manager):
        """Test TTL persistence across operations"""
        key = "ttl_persist_key"
        value = "ttl_persist_value"
        ttl = 5

        # Set with TTL
        await cache_manager.set(key, value, ttl=ttl)

        # Get value (should not reset TTL)
        await asyncio.sleep(2)
        retrieved = await cache_manager.get(key)
        assert retrieved == value

        # Wait for original TTL to expire
        await asyncio.sleep(4)  # Total 6 seconds, should be expired

        # Should be expired
        retrieved = await cache_manager.get(key)
        assert retrieved is None

    @pytest.mark.asyncio
    async def test_cache_key_patterns(self, cache_manager):
        """Test cache key pattern matching"""
        # Set values with patterned keys
        await cache_manager.set("user:1:name", "Alice")
        await cache_manager.set("user:1:email", "alice@example.com")
        await cache_manager.set("user:2:name", "Bob")
        await cache_manager.set("product:1:name", "Product1")

        # Get keys by pattern
        user_keys = await cache_manager.get_keys_by_pattern("user:1:*")
        assert len(user_keys) == 2
        assert "user:1:name" in user_keys
        assert "user:1:email" in user_keys

        # Delete by pattern
        await cache_manager.delete_by_pattern("user:*")

        # Verify deletion
        user_keys = await cache_manager.get_keys_by_pattern("user:*")
        assert len(user_keys) == 0

        # Non-matching keys should remain
        assert await cache_manager.get("product:1:name") == "Product1"

    @pytest.mark.asyncio
    async def test_cache_serialization(self, cache_manager):
        """Test cache value serialization"""
        # Test different data types
        test_values = [
            "string_value",
            42,
            3.14,
            True,
            None,
            [1, 2, 3],
            {"key": "value"},
            (1, 2, 3)
        ]

        for i, value in enumerate(test_values):
            key = f"serial_key_{i}"
            await cache_manager.set(key, value)
            retrieved = await cache_manager.get(key)
            assert retrieved == value

    @pytest.mark.asyncio
    async def test_cache_error_handling(self, cache_manager):
        """Test cache error handling"""
        # Test with invalid key type
        with pytest.raises(CacheError):
            await cache_manager.set(None, "value")

        # Test with invalid data (depends on serialization)
        # Some cache backends might not handle certain objects

    @pytest.mark.asyncio
    async def test_cache_health_check(self, cache_manager):
        """Test cache health check"""
        health = await cache_manager.health_check()

        assert isinstance(health, dict)
        assert "status" in health
        assert "size" in health
        assert "hits" in health
        assert "misses" in health

    @pytest.mark.asyncio
    async def test_cache_cleanup_expired(self, cache_manager):
        """Test cleanup of expired items"""
        # Set items with short TTL
        await cache_manager.set("expire1", "value1", ttl=1)
        await cache_manager.set("expire2", "value2", ttl=1)
        await cache_manager.set("keep", "value3", ttl=100)

        # Wait for expiration
        await asyncio.sleep(1.5)

        # Run cleanup
        await cache_manager.cleanup_expired()

        # Check what remains
        assert await cache_manager.get("expire1") is None
        assert await cache_manager.get("expire2") is None
        assert await cache_manager.get("keep") == "value3"

    @pytest.mark.asyncio
    async def test_cache_concurrent_access(self, cache_manager):
        """Test concurrent cache access"""
        async def worker(worker_id):
            key = f"worker_{worker_id}"
            await cache_manager.set(key, worker_id)
            result = await cache_manager.get(key)
            return result

        # Run multiple workers concurrently
        tasks = [worker(i) for i in range(10)]
        results = await asyncio.gather(*tasks)

        # All workers should get their own values
        for i, result in enumerate(results):
            assert result == i

    @pytest.mark.asyncio
    async def test_cache_disabled(self):
        """Test cache when disabled"""
        disabled_config = {
            "enabled": False,
            "backend": "memory"
        }

        disabled_cache = CacheManager(disabled_config)
        await disabled_cache.initialize()

        # Should be disabled
        assert disabled_cache.is_enabled() is False

        # Operations should be no-ops
        await disabled_cache.set("key", "value")
        result = await disabled_cache.get("key")
        assert result is None

    @pytest.mark.asyncio
    async def test_cache_shutdown(self, cache_manager):
        """Test cache shutdown"""
        # Set some data
        await cache_manager.set("shutdown_test", "value")

        # Shutdown cache
        await cache_manager.shutdown()

        # Cache should be shut down
        assert cache_manager.is_initialized() is False