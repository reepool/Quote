"""
Performance tests for data download functionality
Tests the performance of downloading large volumes of financial data
"""

import pytest
import time
import asyncio
from typing import List, Dict, Any
from datetime import date, timedelta
import pandas as pd
from unittest.mock import AsyncMock, Mock

from tests.factories import (
    InstrumentFactory, QuoteFactory, create_performance_test_data
)
from tests.mocks import MockDataSource, MockRateLimiter


@pytest.mark.performance
@pytest.mark.slow
class TestDataDownloadPerformance:
    """Performance tests for data download operations"""

    @pytest.fixture
    def mock_data_source(self):
        """Create mock data source with realistic performance"""
        source = MockDataSource("performance_test_source")

        # Configure realistic response times
        original_get_daily_data = source.get_daily_data

        async def delayed_get_daily_data(*args, **kwargs):
            # Simulate network delay (50-200ms)
            await asyncio.sleep(0.05 + (hash(str(args)) % 150) / 1000)
            return original_get_daily_data(*args, **kwargs)

        source.get_daily_data = AsyncMock(side_effect=delayed_get_daily_data)
        return source

    @pytest.fixture
    def performance_data(self):
        """Create performance test data"""
        return create_performance_test_data(size=1000)

    @pytest.mark.benchmark
    async def test_download_single_instrument_performance(
        self, mock_data_source, benchmark
    ):
        """Test performance of downloading data for a single instrument"""
        code = "000001.SZ"
        start_date = date.today() - timedelta(days=365)
        end_date = date.today()

        async def download_operation():
            return await mock_data_source.get_daily_data(
                code=code,
                start_date=start_date,
                end_date=end_date
            )

        result = await benchmark(download_operation)

        # Verify result
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

        # Performance assertions (adjust based on requirements)
        benchmark_result = benchmark.stats.stats.mean
        assert benchmark_result < 1.0, f"Download took {benchmark_result:.3f}s, expected < 1.0s"

    @pytest.mark.benchmark
    async def test_download_multiple_instruments_performance(
        self, mock_data_source, performance_data, benchmark
    ):
        """Test performance of downloading data for multiple instruments"""
        codes = performance_data['instruments'][:10]  # Test with 10 instruments
        start_date = date.today() - timedelta(days=90)
        end_date = date.today()

        async def download_multiple():
            tasks = []
            for instrument in codes:
                task = mock_data_source.get_daily_data(
                    code=instrument['code'],
                    start_date=start_date,
                    end_date=end_date
                )
                tasks.append(task)

            return await asyncio.gather(*tasks, return_exceptions=True)

        results = await benchmark(download_multiple)

        # Verify results
        successful_downloads = [r for r in results if not isinstance(r, Exception)]
        assert len(successful_downloads) == len(codes)

        # Performance should scale reasonably
        benchmark_result = benchmark.stats.stats.mean
        assert benchmark_result < 5.0, f"Multiple downloads took {benchmark_result:.3f}s, expected < 5.0s"

    @pytest.mark.benchmark
    async def test_download_with_rate_limiting(
        self, benchmark
    ):
        """Test performance with rate limiting enabled"""
        # Create rate limited data source
        rate_limiter = MockRateLimiter(max_requests=10, time_window=1)
        rate_limiter.configure_delay(0.1)  # 100ms delay per request

        source = MockDataSource("rate_limited_source")
        source.rate_limiter = rate_limiter

        codes = ["000001.SZ", "000002.SZ", "600000.SSE"]
        start_date = date.today() - timedelta(days=30)
        end_date = date.today()

        async def download_with_rate_limit():
            tasks = []
            for code in codes:
                # Acquire rate limit first
                await rate_limiter.acquire()

                task = source.get_daily_data(
                    code=code,
                    start_date=start_date,
                    end_date=end_date
                )
                tasks.append(task)

            return await asyncio.gather(*tasks)

        results = await benchmark(download_with_rate_limit)

        # Verify all downloads succeeded
        assert len(results) == len(codes)
        assert all(isinstance(r, pd.DataFrame) for r in results)

    @pytest.mark.benchmark
    async def test_large_dataset_download_performance(
        self, mock_data_source, benchmark
    ):
        """Test performance of downloading large datasets"""
        code = "000001.SZ"
        # Download 5 years of data
        start_date = date.today() - timedelta(days=5*365)
        end_date = date.today()

        async def download_large_dataset():
            return await mock_data_source.get_daily_data(
                code=code,
                start_date=start_date,
                end_date=end_date
            )

        result = await benchmark(download_large_dataset)

        # Verify result size (should be substantial)
        assert len(result) > 1000  # At least 1000 trading days

        # Performance should remain reasonable even for large datasets
        benchmark_result = benchmark.stats.stats.mean
        assert benchmark_result < 2.0, f"Large dataset download took {benchmark_result:.3f}s, expected < 2.0s"

    @pytest.mark.benchmark
    async def test_concurrent_download_performance(
        self, mock_data_source, benchmark
    ):
        """Test performance of concurrent downloads"""
        codes = [f"{i:06d}.SZ" for i in range(1, 21)]  # 20 instruments
        start_date = date.today() - timedelta(days=30)
        end_date = date.today()

        async def concurrent_download():
            # Create semaphore to limit concurrency
            semaphore = asyncio.Semaphore(5)  # Max 5 concurrent downloads

            async def download_with_semaphore(code):
                async with semaphore:
                    return await mock_data_source.get_daily_data(
                        code=code,
                        start_date=start_date,
                        end_date=end_date
                    )

            tasks = [download_with_semaphore(code) for code in codes]
            return await asyncio.gather(*tasks, return_exceptions=True)

        results = await benchmark(concurrent_download)

        # Verify results
        successful_downloads = [r for r in results if not isinstance(r, Exception)]
        assert len(successful_downloads) == len(codes)

        # Concurrent downloads should be faster than sequential
        benchmark_result = benchmark.stats.stats.mean
        assert benchmark_result < 3.0, f"Concurrent downloads took {benchmark_result:.3f}s, expected < 3.0s"

    @pytest.mark.benchmark
    def test_memory_usage_during_download(self, mock_data_source, benchmark):
        """Test memory usage during large download operations"""
        import psutil
        import os

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        async def memory_intensive_download():
            codes = [f"{i:06d}.SZ" for i in range(1, 51)]  # 50 instruments
            start_date = date.today() - timedelta(days=365)
            end_date = date.today()

            results = []
            for code in codes:
                data = await mock_data_source.get_daily_data(
                    code=code,
                    start_date=start_date,
                    end_date=end_date
                )
                results.append(data)

            return results

        # Run the benchmark
        loop = asyncio.get_event_loop()
        results = loop.run_until_complete(
            benchmark(memory_intensive_download)
        )

        # Check memory usage
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory

        # Memory increase should be reasonable (less than 500MB for 50 instruments)
        assert memory_increase < 500, f"Memory increased by {memory_increase:.1f}MB, expected < 500MB"
        assert len(results) == 50

    @pytest.mark.benchmark
    async def test_download_error_handling_performance(
        self, benchmark
    ):
        """Test performance impact of error handling"""
        source = MockDataSource("error_test_source")

        # Configure 20% of requests to fail
        original_get_daily_data = source.get_daily_data
        call_count = 0

        async def failing_get_daily_data(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count % 5 == 0:  # Every 5th call fails
                raise Exception("Simulated network error")
            return await original_get_daily_data(*args, **kwargs)

        source.get_daily_data = AsyncMock(side_effect=failing_get_daily_data)

        codes = [f"{i:06d}.SZ" for i in range(1, 21)]  # 20 instruments
        start_date = date.today() - timedelta(days=30)
        end_date = date.today()

        async def download_with_errors():
            tasks = []
            for code in codes:
                task = source.get_daily_data(
                    code=code,
                    start_date=start_date,
                    end_date=end_date
                )
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Separate successful and failed results
            successful = [r for r in results if not isinstance(r, Exception)]
            failed = [r for r in results if isinstance(r, Exception)]

            return {"successful": len(successful), "failed": len(failed)}

        result = await benchmark(download_with_errors)

        # Verify error handling
        assert result["successful"] > 0  # Some should succeed
        assert result["failed"] > 0     # Some should fail
        assert result["successful"] + result["failed"] == len(codes)

    @pytest.mark.benchmark
    async def test_data_processing_performance(
        self, performance_data, benchmark
    ):
        """Test performance of data processing operations"""
        large_dataset = performance_data['quotes']
        df = pd.DataFrame(large_dataset)

        async def process_data():
            # Simulate common data processing operations
            processed = df.copy()

            # Add calculated columns
            processed['price_change'] = processed['close'] - processed['open']
            processed['price_change_pct'] = (processed['price_change'] / processed['open']) * 100
            processed['volatility'] = (processed['high'] - processed['low']) / processed['close']

            # Filter and group operations
            processed = processed[processed['volume'] > 0]
            daily_stats = processed.groupby('code').agg({
                'close': ['mean', 'std', 'min', 'max'],
                'volume': 'sum'
            }).round(2)

            return daily_stats

        result = await benchmark(process_data)

        # Verify processing results
        assert len(result) > 0
        assert isinstance(result, pd.DataFrame)

        # Processing should be fast even for large datasets
        benchmark_result = benchmark.stats.stats.mean
        assert benchmark_result < 1.0, f"Data processing took {benchmark_result:.3f}s, expected < 1.0s"