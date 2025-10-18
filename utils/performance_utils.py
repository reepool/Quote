"""
性能监控工具模块
提供性能分析、资源监控和优化建议功能
"""

import time
import asyncio
import psutil
import gc
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, field
from functools import wraps
from collections import defaultdict, deque
import threading

import logging
logger = logging.getLogger("Performance")


@dataclass
class PerformanceMetric:
    """性能指标"""
    name: str
    value: float
    unit: str
    timestamp: float = field(default_factory=time.time)
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ResourceUsage:
    """资源使用情况"""
    cpu_percent: float
    memory_percent: float
    memory_mb: float
    disk_usage_percent: float
    active_threads: int
    open_files: int
    timestamp: float = field(default_factory=time.time)


class PerformanceMonitor:
    """性能监控器"""

    def __init__(self, max_history: int = 1000):
        self.max_history = max_history
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=max_history))
        self.counters: Dict[str, int] = defaultdict(int)
        self.timers: Dict[str, float] = defaultdict(float)
        self._lock = threading.Lock()

    def record_metric(self, metric: PerformanceMetric):
        """记录性能指标"""
        with self._lock:
            self.metrics[metric.name].append(metric)
            logger.debug(f"[Performance] {metric.name}: {metric.value}{metric.unit}")

    def increment_counter(self, name: str, value: int = 1, tags: Dict[str, str] = None):
        """增加计数器"""
        with self._lock:
            self.counters[name] += value
            metric = PerformanceMetric(
                name=name,
                value=self.counters[name],
                unit="count",
                tags=tags or {}
            )
            self.metrics[name].append(metric)

    def record_timing(self, name: str, duration: float, tags: Dict[str, str] = None):
        """记录时间"""
        with self._lock:
            self.timers[name] += duration
            metric = PerformanceMetric(
                name=name,
                value=duration,
                unit="seconds",
                tags=tags or {}
            )
            self.metrics[name].append(metric)

    def get_metric_stats(self, name: str) -> Dict[str, float]:
        """获取指标统计"""
        with self._lock:
            if name not in self.metrics or not self.metrics[name]:
                return {}

            values = [m.value for m in self.metrics[name]]
            return {
                'count': len(values),
                'min': min(values),
                'max': max(values),
                'avg': sum(values) / len(values),
                'latest': values[-1]
            }

    def get_all_metrics(self) -> Dict[str, Dict[str, float]]:
        """获取所有指标统计"""
        result = {}
        for name in self.metrics:
            stats = self.get_metric_stats(name)
            if stats:
                result[name] = stats
        return result

    def reset_metrics(self, name: str = None):
        """重置指标"""
        with self._lock:
            if name:
                self.metrics[name].clear()
                self.counters[name] = 0
                self.timers[name] = 0.0
            else:
                self.metrics.clear()
                self.counters.clear()
                self.timers.clear()


class ResourceMonitor:
    """资源监控器"""

    def __init__(self, check_interval: float = 30.0):
        self.check_interval = check_interval
        self.monitoring = False
        self.history: deque = deque(maxlen=1000)
        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None

    def start_monitoring(self):
        """开始监控"""
        if self.monitoring:
            return

        self.monitoring = True
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info("[ResourceMonitor] Started resource monitoring")

    def stop_monitoring(self):
        """停止监控"""
        if not self.monitoring:
            return

        self.monitoring = False
        self._stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join()
        logger.info("[ResourceMonitor] Stopped resource monitoring")

    def _monitor_loop(self):
        """监控循环"""
        while not self._stop_event.is_set():
            try:
                usage = self.get_current_usage()
                self.history.append(usage)

                # 检查警告阈值
                if usage.cpu_percent > 80:
                    logger.warning(f"[ResourceMonitor] High CPU usage: {usage.cpu_percent:.1f}%")

                if usage.memory_percent > 85:
                    logger.warning(f"[ResourceMonitor] High memory usage: {usage.memory_percent:.1f}%")

                if usage.disk_usage_percent > 90:
                    logger.warning(f"[ResourceMonitor] High disk usage: {usage.disk_usage_percent:.1f}%")

            except Exception as e:
                logger.error(f"[ResourceMonitor] Error monitoring resources: {e}")

            self._stop_event.wait(self.check_interval)

    def get_current_usage(self) -> ResourceUsage:
        """获取当前资源使用情况"""
        process = psutil.Process()

        # CPU使用率
        cpu_percent = psutil.cpu_percent(interval=1)

        # 内存使用情况
        memory_info = psutil.virtual_memory()
        process_memory = process.memory_info()
        memory_mb = process_memory.rss / 1024 / 1024

        # 磁盘使用情况
        disk_usage = psutil.disk_usage('/')

        return ResourceUsage(
            cpu_percent=cpu_percent,
            memory_percent=memory_info.percent,
            memory_mb=memory_mb,
            disk_usage_percent=disk_usage.percent,
            active_threads=len(process.threads()),
            open_files=len(process.open_files())
        )

    def get_average_usage(self, minutes: int = 5) -> Optional[ResourceUsage]:
        """获取平均资源使用情况"""
        if not self.history:
            return None

        cutoff_time = time.time() - (minutes * 60)
        recent_usage = [usage for usage in self.history if usage.timestamp > cutoff_time]

        if not recent_usage:
            return None

        return ResourceUsage(
            cpu_percent=sum(u.cpu_percent for u in recent_usage) / len(recent_usage),
            memory_percent=sum(u.memory_percent for u in recent_usage) / len(recent_usage),
            memory_mb=sum(u.memory_mb for u in recent_usage) / len(recent_usage),
            disk_usage_percent=sum(u.disk_usage_percent for u in recent_usage) / len(recent_usage),
            active_threads=sum(u.active_threads for u in recent_usage) / len(recent_usage),
            open_files=sum(u.open_files for u in recent_usage) / len(recent_usage)
        )


def performance_monitor(name: str, threshold: float = 1.0):
    """性能监控装饰器"""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                if duration > threshold:
                    logger.warning(f"[Performance] Slow operation: {name} took {duration:.2f}s")
                performance_monitor.record_timing(name, duration)

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start_time
                if duration > threshold:
                    logger.warning(f"[Performance] Slow operation: {name} took {duration:.2f}s")
                performance_monitor.record_timing(name, duration)

        # 判断函数是否为协程函数
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


class MemoryProfiler:
    """内存分析器"""

    @staticmethod
    def profile_memory_usage(func: Callable) -> Callable:
        """内存使用分析装饰器"""
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 强制垃圾回收
            gc.collect()

            # 记录开始内存
            process = psutil.Process()
            start_memory = process.memory_info().rss / 1024 / 1024

            try:
                result = func(*args, **kwargs)
                return result
            finally:
                # 记录结束内存
                end_memory = process.memory_info().rss / 1024 / 1024
                memory_diff = end_memory - start_memory

                if memory_diff > 10:  # 超过10MB增长
                    logger.warning(f"[Memory] {func.__name__} increased memory by {memory_diff:.1f}MB")

                performance_monitor.record_metric(PerformanceMetric(
                    name=f"{func.__name__}_memory",
                    value=memory_diff,
                    unit="MB"
                ))

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            # 强制垃圾回收
            gc.collect()

            # 记录开始内存
            process = psutil.Process()
            start_memory = process.memory_info().rss / 1024 / 1024

            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                # 记录结束内存
                end_memory = process.memory_info().rss / 1024 / 1024
                memory_diff = end_memory - start_memory

                if memory_diff > 10:  # 超过10MB增长
                    logger.warning(f"[Memory] {func.__name__} increased memory by {memory_diff:.1f}MB")

                performance_monitor.record_metric(PerformanceMetric(
                    name=f"{func.__name__}_memory",
                    value=memory_diff,
                    unit="MB"
                ))

        # 判断函数是否为协程函数
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return wrapper

    @staticmethod
    def get_memory_stats() -> Dict[str, float]:
        """获取内存统计信息"""
        process = psutil.Process()
        memory_info = process.memory_info()
        virtual_memory = psutil.virtual_memory()

        return {
            'process_memory_mb': memory_info.rss / 1024 / 1024,
            'process_vms_mb': memory_info.vms / 1024 / 1024,
            'system_memory_percent': virtual_memory.percent,
            'system_available_mb': virtual_memory.available / 1024 / 1024,
            'gc_counts': gc.get_count()  # (generation0, generation1, generation2)
        }


class PerformanceOptimizer:
    """性能优化器"""

    @staticmethod
    def optimize_batch_size(current_size: int, target_duration: float,
                          actual_duration: float, max_size: int = 1000) -> int:
        """根据执行时间动态调整批次大小"""
        if actual_duration == 0:
            return current_size

        # 计算理想的批次大小
        ideal_size = int(current_size * (target_duration / actual_duration))

        # 限制在合理范围内
        new_size = max(1, min(ideal_size, max_size))

        if new_size != current_size:
            logger.info(f"[Performance] Adjusted batch size: {current_size} -> {new_size}")

        return new_size

    @staticmethod
    def suggest_optimizations(performance_data: Dict[str, Dict[str, float]]) -> List[str]:
        """根据性能数据提供优化建议"""
        suggestions = []

        for name, stats in performance_data.items():
            if stats.get('avg', 0) > 2.0:  # 平均执行时间超过2秒
                suggestions.append(f"Consider optimizing '{name}' - average time: {stats['avg']:.2f}s")

            if stats.get('count', 0) > 1000:  # 调用次数过多
                suggestions.append(f"High call count for '{name}': {stats['count']} calls")

            if stats.get('max', 0) > 10.0:  # 最大执行时间超过10秒
                suggestions.append(f"Very slow execution for '{name}': {stats['max']:.2f}s")

        # 检查内存使用
        memory_stats = MemoryProfiler.get_memory_stats()
        if memory_stats['system_memory_percent'] > 80:
            suggestions.append("High system memory usage - consider optimizing memory consumption")

        if memory_stats['process_memory_mb'] > 500:  # 进程内存超过500MB
            suggestions.append("High process memory usage - check for memory leaks")

        return suggestions


# 全局性能监控器实例
performance_monitor = PerformanceMonitor()
resource_monitor = ResourceMonitor()