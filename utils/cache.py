"""
Cache utilities for the quote system.
Provides in-memory caching with TTL support.
"""

import time
import asyncio
import json
from typing import Any, Dict, Optional, Callable
from functools import wraps
from datetime import datetime, timedelta
from dataclasses import dataclass, field

from utils import cache_logger
from utils.date_utils import get_shanghai_time


@dataclass
class CacheEntry:
    """缓存条目"""
    value: Any
    created_at: float = field(default_factory=time.time)
    ttl: Optional[float] = None  # 生存时间（秒）

    def is_expired(self) -> bool:
        """检查是否过期"""
        if self.ttl is None:
            return False
        return time.time() - self.created_at > self.ttl

    def get_age(self) -> float:
        """获取缓存年龄（秒）"""
        return time.time() - self.created_at


class SimpleCache:
    """简单内存缓存"""

    def __init__(self, max_size: int = 1000, default_ttl: int = 3600):
        """
        初始化缓存

        Args:
            max_size: 最大缓存条目数
            default_ttl: 默认生存时间（秒）
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        """获取缓存值"""
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None

            if entry.is_expired():
                del self._cache[key]
                return None

            cache_logger.debug(f"[Cache] Hit: {key}")
            return entry.value

    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        """设置缓存值"""
        async with self._lock:
            # 检查是否需要清理空间
            if len(self._cache) >= self.max_size and key not in self._cache:
                await self._cleanup_expired()
                if len(self._cache) >= self.max_size:
                    await self._evict_oldest()

            entry = CacheEntry(
                value=value,
                ttl=ttl or self.default_ttl
            )
            self._cache[key] = entry
            cache_logger.debug(f"[Cache] Set: {key}")
            return True

    async def delete(self, key: str) -> bool:
        """删除缓存值"""
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                cache_logger.debug(f"[Cache] Delete: {key}")
                return True
            return False

    async def exists(self, key: str) -> bool:
        """检查键是否存在"""
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return False
            if entry.is_expired():
                del self._cache[key]
                return False
            return True

    async def clear(self):
        """清空缓存"""
        async with self._lock:
            self._cache.clear()
            cache_logger.info("[Cache] Cleared all entries")

    async def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        async with self._lock:
            total_entries = len(self._cache)
            expired_entries = sum(1 for entry in self._cache.values() if entry.is_expired())
            total_memory = sum(len(str(entry.value)) for entry in self._cache.values())

            return {
                'total_entries': total_entries,
                'expired_entries': expired_entries,
                'active_entries': total_entries - expired_entries,
                'max_size': self.max_size,
                'memory_usage_bytes': total_memory,
                'hit_rate': self._calculate_hit_rate()
            }

    def _calculate_hit_rate(self) -> float:
        """计算命中率（需要扩展hit计数）"""
        # 简化版本，实际使用时需要增加hit计数
        return 0.0

    async def _cleanup_expired(self):
        """清理过期条目"""
        expired_keys = [key for key, entry in self._cache.items() if entry.is_expired()]
        for key in expired_keys:
            del self._cache[key]
        if expired_keys:
            cache_logger.debug(f"[Cache] Cleaned up {len(expired_keys)} expired entries")

    async def _evict_oldest(self):
        """驱逐最老的条目（LRU策略）"""
        if not self._cache:
            return

        oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k].created_at)
        del self._cache[oldest_key]
        cache_logger.debug(f"[Cache] Evicted oldest entry: {oldest_key}")


class AsyncCache:
    """异步缓存装饰器"""

    def __init__(self, cache: SimpleCache, key_prefix: str = "", ttl: Optional[float] = None):
        self.cache = cache
        self.key_prefix = key_prefix
        self.ttl = ttl

    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # 生成缓存键
            cache_key = self._generate_cache_key(func, args, kwargs)

            # 尝试从缓存获取
            cached_result = await self.cache.get(cache_key)
            if cached_result is not None:
                cache_logger.debug(f"[Cache] Function cache hit: {func.__name__}")
                return cached_result

            # 执行函数
            cache_logger.debug(f"[Cache] Function cache miss: {func.__name__}")
            result = await func(*args, **kwargs)

            # 存入缓存
            await self.cache.set(cache_key, result, self.ttl)
            return result

        return wrapper

    def _generate_cache_key(self, func: Callable, args: tuple, kwargs: dict) -> str:
        """生成缓存键"""
        # 将参数转换为字符串
        args_str = json.dumps(args, default=str, sort_keys=True)
        kwargs_str = json.dumps(kwargs, default=str, sort_keys=True)

        key = f"{self.key_prefix}:{func.__name__}:{args_str}:{kwargs_str}"
        # 限制键长度
        if len(key) > 250:
            key = key[:250] + "_" + str(hash(key))
        return key


class QuoteCache:
    """行情数据专用缓存"""

    def __init__(self, max_size: int = 5000, default_ttl: int = 1800):  # 30分钟默认TTL
        self.cache = SimpleCache(max_size, default_ttl)

    async def get_daily_quotes(self, instrument_id: str, start_date: datetime, end_date: datetime) -> Optional[list]:
        """获取日线行情缓存"""
        key = f"daily_quotes:{instrument_id}:{start_date.date()}:{end_date.date()}"
        return await self.cache.get(key)

    async def set_daily_quotes(self, instrument_id: str, start_date: datetime, end_date: datetime, data: list, ttl: int = 1800):
        """设置日线行情缓存"""
        key = f"daily_quotes:{instrument_id}:{start_date.date()}:{end_date.date()}"
        await self.cache.set(key, data, ttl)

    async def get_latest_quote(self, instrument_id: str) -> Optional[dict]:
        """获取最新行情缓存"""
        key = f"latest_quote:{instrument_id}"
        return await self.cache.get(key)

    async def set_latest_quote(self, instrument_id: str, data: dict, ttl: int = 300):  # 5分钟TTL
        """设置最新行情缓存"""
        key = f"latest_quote:{instrument_id}"
        await self.cache.set(key, data, ttl)

    async def get_instrument_info(self, symbol: str = None, instrument_id: str = None) -> Optional[dict]:
        """获取交易品种信息缓存"""
        if symbol:
            key = f"instrument_symbol:{symbol}"
        elif instrument_id:
            key = f"instrument_id:{instrument_id}"
        else:
            return None
        return await self.cache.get(key)

    async def set_instrument_info(self, symbol: str = None, instrument_id: str = None, data: dict = None, ttl: int = 3600):
        """设置交易品种信息缓存"""
        if symbol:
            key = f"instrument_symbol:{symbol}"
        elif instrument_id:
            key = f"instrument_id:{instrument_id}"
        else:
            return
        await self.cache.set(key, data, ttl)

    async def invalidate_instrument_cache(self, symbol: str = None, instrument_id: str = None):
        """使交易品种相关缓存失效"""
        if symbol:
            await self.cache.delete(f"instrument_symbol:{symbol}")
        if instrument_id:
            await self.cache.delete(f"instrument_id:{instrument_id}")

    async def get_trading_calendar(self, exchange: str, year: int) -> Optional[list]:
        """获取交易日历缓存"""
        key = f"trading_calendar:{exchange}:{year}"
        return await self.cache.get(key)

    async def set_trading_calendar(self, exchange: str, year: int, data: list, ttl: int = 86400):  # 24小时TTL
        """设置交易日历缓存"""
        key = f"trading_calendar:{exchange}:{year}"
        await self.cache.set(key, data, ttl)

    async def clear_expired_data(self):
        """清理过期数据"""
        await self.cache._cleanup_expired()

    async def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        return await self.cache.get_stats()


class CacheManager:
    """缓存管理器"""

    def __init__(self):
        self.quote_cache = QuoteCache()
        self.general_cache = SimpleCache(max_size=2000, default_ttl=3600)
        self.enabled = True  # 可以通过配置控制缓存开关

    async def disable_cache(self):
        """禁用缓存"""
        self.enabled = False
        await self.clear_all_cache()

    async def enable_cache(self):
        """启用缓存"""
        self.enabled = True

    async def clear_all_cache(self):
        """清空所有缓存"""
        await self.quote_cache.cache.clear()
        await self.general_cache.clear()

    async def get_overall_stats(self) -> Dict[str, Any]:
        """获取整体缓存统计信息"""
        quote_stats = await self.quote_cache.get_cache_stats()
        general_stats = await self.general_cache.get_stats()

        return {
            'cache_enabled': self.enabled,
            'quote_cache': quote_stats,
            'general_cache': general_stats,
            'timestamp': get_shanghai_time()
        }

    def cache_decorator(self, key_prefix: str = "", ttl: Optional[float] = None):
        """创建缓存装饰器"""
        if not self.enabled:
            # 如果缓存禁用，返回空的装饰器
            def decorator(func: Callable) -> Callable:
                @wraps(func)
                async def wrapper(*args, **kwargs):
                    return await func(*args, **kwargs)
                return wrapper
            return decorator

        return AsyncCache(self.general_cache, key_prefix, ttl)


# 全局缓存管理器实例
cache_manager = CacheManager()