"""
Benchmark configuration and utilities for Quote System performance tests
"""

import pytest
import time
import psutil
import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Callable
from unittest.mock import Mock, AsyncMock
import pandas as pd

from tests.factories import (
    InstrumentFactory, QuoteFactory, create_performance_test_data
)
from tests.mocks import MockDataSource, MockDatabaseManager


class BenchmarkRunner:
    """Utility class for running performance benchmarks"""

    def __init__(self):
        self.results: List[Dict[str, Any]] = []

    def run_benchmark(
        self,
        func: Callable,
        name: str,
        iterations: int = 1,
        warmup_iterations: int = 0
    ) -> Dict[str, Any]:
        """Run a benchmark test"""
        # Warmup
        for _ in range(warmup_iterations):
            func()

        # Collect performance metrics
        times = []
        memory_usage = []

        for i in range(iterations):
            # Measure initial memory
            process = psutil.Process(os.getpid())
            initial_memory = process.memory_info().rss

            # Run the function
            start_time = time.time()
            result = func()
            end_time = time.time()

            # Measure final memory
            final_memory = process.memory_info().rss
            memory_delta = (final_memory - initial_memory) / 1024 / 1024  # MB

            execution_time = end_time - start_time
            times.append(execution_time)
            memory_usage.append(memory_delta)

        # Calculate statistics
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        avg_memory = sum(memory_usage) / len(memory_usage)

        benchmark_result = {
            "name": name,
            "iterations": iterations,
            "avg_time": avg_time,
            "min_time": min_time,
            "max_time": max_time,
            "avg_memory_mb": avg_memory,
            "timestamp": datetime.now().isoformat()
        }

        self.results.append(benchmark_result)
        return benchmark_result

    def get_results_summary(self) -> Dict[str, Any]:
        """Get summary of all benchmark results"""
        if not self.results:
            return {"message": "No benchmarks run yet"}

        return {
            "total_benchmarks": len(self.results),
            "results": self.results,
            "fastest_test": min(self.results, key=lambda x: x["avg_time"]),
            "slowest_test": max(self.results, key=lambda x: x["avg_time"]),
            "memory_intensive_test": max(self.results, key=lambda x: x["avg_memory_mb"])
        }


@pytest.fixture
def benchmark_runner():
    """Create benchmark runner fixture"""
    return BenchmarkRunner()


class PerformanceThresholds:
    """Performance threshold definitions"""

    # Database operation thresholds (seconds)
    DATABASE_INSERT_BATCH_100 = 0.5
    DATABASE_QUERY_1000_RECORDS = 0.1
    DATABASE_UPDATE_100_RECORDS = 0.2

    # Data source thresholds (seconds)
    DATA_SOURCE_FETCH_100_RECORDS = 1.0
    DATA_SOURCE_FETCH_1000_RECORDS = 3.0

    # API response thresholds (seconds)
    API_RESPONSE_SIMPLE = 0.1
    API_RESPONSE_COMPLEX = 0.5
    API_RESPONSE_LARGE_DATA = 2.0

    # Task execution thresholds (seconds)
    TASK_EXECUTION_LIGHTWEIGHT = 1.0
    TASK_EXECUTION_HEAVY = 10.0

    # Memory thresholds (MB)
    MEMORY_USAGE_NORMAL = 100
    MEMORY_USAGE_LARGE = 500

    @classmethod
    def check_threshold(cls, value: float, threshold: float, name: str) -> bool:
        """Check if value meets performance threshold"""
        meets_threshold = value <= threshold
        if not meets_threshold:
            print(f"⚠️  Performance warning: {name} = {value:.3f} (threshold: {threshold:.3f})")
        else:
            print(f"✅ Performance OK: {name} = {value:.3f}")
        return meets_threshold


@pytest.mark.performance
class TestSystemBenchmarks:
    """System-wide performance benchmarks"""

    def test_database_performance(self, benchmark_runner):
        """Benchmark database operations"""
        mock_db = MockDatabaseManager()

        def insert_batch_100():
            # Simulate inserting 100 records
            mock_db.configure_query_result(
                [{"id": i} for i in range(100)],
                "execute"
            )
            return mock_db.execute_many(
                "INSERT INTO test_table VALUES (?)",
                [(i,) for i in range(100)]
            )

        result = benchmark_runner.run_benchmark(
            insert_batch_100,
            "database_insert_batch_100",
            iterations=10,
            warmup_iterations=2
        )

        PerformanceThresholds.check_threshold(
            result["avg_time"],
            PerformanceThresholds.DATABASE_INSERT_BATCH_100,
            "Database Insert Batch (100 records)"
        )

    def test_data_processing_performance(self, benchmark_runner):
        """Benchmark data processing operations"""
        # Create test data
        test_data = create_performance_test_data(size=1000)
        df = pd.DataFrame(test_data["quotes"])

        def process_dataframe():
            # Simulate common data processing operations
            processed = df.copy()

            # Add calculated columns
            processed["price_change"] = processed["close"] - processed["open"]
            processed["price_change_pct"] = (processed["price_change"] / processed["open"]) * 100
            processed["volatility"] = (processed["high"] - processed["low"]) / processed["close"]

            # Filter and aggregate
            processed = processed[processed["volume"] > 0]
            result = processed.groupby("code").agg({
                "close": "mean",
                "volume": "sum"
            }).round(2)

            return len(result)

        result = benchmark_runner.run_benchmark(
            process_dataframe,
            "data_processing_1000_records",
            iterations=5,
            warmup_iterations=1
        )

        PerformanceThresholds.check_threshold(
            result["avg_time"],
            1.0,  # 1 second threshold
            "Data Processing (1000 records)"
        )

    def test_memory_usage_benchmark(self, benchmark_runner):
        """Benchmark memory usage"""
        def create_large_dataset():
            # Create a large dataset to test memory usage
            data = []
            for i in range(10000):
                data.append({
                    "id": i,
                    "value": i * 2,
                    "name": f"item_{i}",
                    "timestamp": datetime.now().isoformat()
                })
            return len(data)

        result = benchmark_runner.run_benchmark(
            create_large_dataset,
            "memory_usage_large_dataset",
            iterations=3,
            warmup_iterations=1
        )

        PerformanceThresholds.check_threshold(
            result["avg_memory_mb"],
            PerformanceThresholds.MEMORY_USAGE_NORMAL,
            "Memory Usage (Large Dataset)"
        )

    def test_concurrent_operations_performance(self, benchmark_runner):
        """Benchmark concurrent operations"""
        import asyncio

        async def concurrent_data_fetch():
            # Simulate concurrent data fetching
            mock_source = MockDataSource()

            tasks = []
            for i in range(10):
                task = mock_source.get_daily_data(
                    code=f"{i:06d}.SZ",
                    start_date=date.today() - timedelta(days=30),
                    end_date=date.today()
                )
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)
            successful_results = [r for r in results if not isinstance(r, Exception)]
            return len(successful_results)

        def run_concurrent_test():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(concurrent_data_fetch())
            finally:
                loop.close()

        result = benchmark_runner.run_benchmark(
            run_concurrent_test,
            "concurrent_data_fetch_10_sources",
            iterations=5,
            warmup_iterations=1
        )

        PerformanceThresholds.check_threshold(
            result["avg_time"],
            5.0,  # 5 second threshold
            "Concurrent Data Fetch (10 sources)"
        )

    def test_api_response_performance(self, benchmark_runner):
        """Benchmark API response times"""
        from tests.mocks import MockAPIClient

        api_client = MockAPIClient()

        # Configure API client with larger response
        large_response = {
            "data": [InstrumentFactory.create_instrument() for _ in range(1000)],
            "pagination": {"page": 1, "total": 1000}
        }
        api_client.configure_response("get", "/instruments", large_response)

        def fetch_large_api_response():
            return api_client.session.get("/instruments")

        result = benchmark_runner.run_benchmark(
            fetch_large_api_response,
            "api_response_large_data",
            iterations=20,
            warmup_iterations=5
        )

        PerformanceThresholds.check_threshold(
            result["avg_time"],
            PerformanceThresholds.API_RESPONSE_LARGE_DATA,
            "API Response (Large Data)"
        )

    def test_scheduler_performance(self, benchmark_runner):
        """Benchmark task scheduler performance"""
        from tests.mocks import MockScheduler

        scheduler = MockScheduler()

        def add_and_remove_jobs():
            # Add multiple jobs
            jobs = []
            for i in range(50):
                job_id = f"test_job_{i}"
                mock_func = Mock()
                job = scheduler.add_job(job_id, mock_func)
                jobs.append(job_id)

            # Remove all jobs
            removed_count = 0
            for job_id in jobs:
                if scheduler.remove_job(job_id):
                    removed_count += 1

            return removed_count

        result = benchmark_runner.run_benchmark(
            add_and_remove_jobs,
            "scheduler_add_remove_50_jobs",
            iterations=10,
            warmup_iterations=2
        )

        PerformanceThresholds.check_threshold(
            result["avg_time"],
            1.0,  # 1 second threshold
            "Scheduler Operations (50 jobs)"
        )

    def test_cache_performance(self, benchmark_runner):
        """Benchmark cache operations"""
        from tests.mocks import MockCacheManager

        cache = MockCacheManager()

        def cache_operations():
            # Set cache values
            for i in range(1000):
                cache.set(f"key_{i}", f"value_{i}")

            # Get cache values
            hits = 0
            for i in range(1000):
                if cache.get(f"key_{i}") == f"value_{i}":
                    hits += 1

            return hits

        result = benchmark_runner.run_benchmark(
            cache_operations,
            "cache_operations_1000_items",
            iterations=10,
            warmup_iterations=2
        )

        PerformanceThresholds.check_threshold(
            result["avg_time"],
            0.5,  # 0.5 second threshold
            "Cache Operations (1000 items)"
        )