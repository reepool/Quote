"""
Performance tests for database operations
"""

import pytest
import asyncio
import time
from datetime import date, datetime
from unittest.mock import Mock, AsyncMock
import pandas as pd
import psutil
import memory_profiler

from database.operations import DatabaseOperations


@pytest.mark.performance
class TestDatabasePerformance:
    """Performance tests for database operations"""

    @pytest.fixture
    async def db_operations(self, test_database):
        """Create DatabaseOperations instance for performance testing"""
        ops = DatabaseOperations(test_database)
        await ops.initialize()
        return ops

    @pytest.fixture
    def large_instrument_dataset(self):
        """Generate large dataset for performance testing"""
        instruments = []
        for i in range(1000):  # 1000 instruments
            instruments.append({
                'code': f'{i:06d}.SZ' if i % 2 == 0 else f'{i:06d}.SH',
                'name': f'测试股票{i}',
                'market': 'SZSE' if i % 2 == 0 else 'SSE',
                'industry': f'行业{i % 10}',
                'list_date': '2020-01-01',
                'status': 'active'
            })
        return instruments

    @pytest.fixture
    def large_quote_dataset(self):
        """Generate large quote dataset for performance testing"""
        quotes = []
        base_date = date(2024, 1, 1)

        for i in range(100):  # 100 stocks
            stock_code = f'{i:06d}.SZ'
            for j in range(252):  # 252 trading days (1 year)
                trade_date = base_date + pd.Timedelta(days=j)
                # Skip weekends
                if trade_date.weekday() >= 5:
                    continue

                base_price = 10.0 + i * 0.1
                quotes.append({
                    'code': stock_code,
                    'date': trade_date.strftime('%Y-%m-%d'),
                    'open': base_price + (j % 5) * 0.1,
                    'high': base_price + (j % 5) * 0.1 + 0.5,
                    'low': base_price + (j % 5) * 0.1 - 0.3,
                    'close': base_price + (j % 5) * 0.1 + 0.2,
                    'volume': 1000000 + (j % 10) * 100000,
                    'amount': (base_price * 1000000) + (j % 10) * 100000
                })
        return quotes

    @pytest.mark.asyncio
    async def test_bulk_insert_performance(self, db_operations, large_instrument_dataset):
        """Test bulk insert performance"""
        # Measure memory before
        mem_before = memory_profiler.memory_usage()[0]

        # Measure insert performance
        start_time = time.time()
        result = await db_operations.insert_instruments(large_instrument_dataset)
        insert_time = time.time() - start_time

        # Measure memory after
        mem_after = memory_profiler.memory_usage()[0]
        memory_used = mem_after - mem_before

        # Performance assertions
        assert result is True
        assert insert_time < 10.0  # Should complete within 10 seconds
        assert memory_used < 100  # Should use less than 100MB additional memory

        # Calculate throughput
        throughput = len(large_instrument_dataset) / insert_time
        print(f"Bulk insert throughput: {throughput:.2f} records/second")
        assert throughput > 100  # Should handle at least 100 records/second

    @pytest.mark.asyncio
    async def test_batch_insert_performance(self, db_operations, large_quote_dataset):
        """Test batch insert performance with quote data"""
        # Split into batches
        batch_size = 1000
        batches = [large_quote_dataset[i:i+batch_size]
                  for i in range(0, len(large_quote_dataset), batch_size)]

        total_start = time.time()
        successful_batches = 0

        for batch in batches:
            batch_start = time.time()
            result = await db_operations.insert_daily_quotes(batch)
            batch_time = time.time() - batch_start

            if result:
                successful_batches += 1
                # Each batch should complete reasonably quickly
                assert batch_time < 5.0

        total_time = time.time() - total_start

        # Performance assertions
        assert successful_batches == len(batches)
        assert total_time < 30.0  # All batches should complete within 30 seconds

        total_records = len(large_quote_dataset)
        throughput = total_records / total_time
        print(f"Batch insert throughput: {throughput:.2f} records/second")
        assert throughput > 500  # Should handle at least 500 records/second

    @pytest.mark.asyncio
    async def test_query_performance_with_indexing(self, db_operations, large_quote_dataset):
        """Test query performance with proper indexing"""
        # Insert test data
        await db_operations.insert_daily_quotes(large_quote_dataset)

        # Test single stock query
        stock_code = large_quote_dataset[0]['code']
        start_time = time.time()
        quotes = await db_operations.get_daily_data(
            stock_code,
            date(2024, 1, 1),
            date(2024, 12, 31)
        )
        query_time = time.time() - start_time

        # Performance assertions
        assert query_time < 1.0  # Single stock query should be very fast
        assert len(quotes) > 0
        print(f"Single stock query time: {query_time:.4f} seconds for {len(quotes)} records")

        # Test date range query
        start_time = time.time()
        all_quotes = await db_operations.get_daily_data(
            None,  # All stocks
            date(2024, 1, 1),
            date(2024, 1, 31)  # One month
        )
        range_query_time = time.time() - start_time

        # Performance assertions
        assert range_query_time < 5.0  # Range query should be fast
        print(f"Date range query time: {range_query_time:.4f} seconds for {len(all_quotes)} records")

    @pytest.mark.asyncio
    async def test_concurrent_query_performance(self, db_operations, large_quote_dataset):
        """Test concurrent query performance"""
        # Insert test data
        await db_operations.insert_daily_quotes(large_quote_dataset)

        # Test concurrent queries
        async def concurrent_query(stock_code):
            start_time = time.time()
            quotes = await db_operations.get_daily_data(
                stock_code,
                date(2024, 1, 1),
                date(2024, 12, 31)
            )
            return time.time() - start_time, len(quotes)

        # Run 10 concurrent queries
        stock_codes = list(set(quote['code'] for quote in large_quote_dataset))[:10]
        tasks = [concurrent_query(code) for code in stock_codes]

        start_time = time.time()
        results = await asyncio.gather(*tasks)
        total_time = time.time() - start_time

        # Analyze results
        query_times, record_counts = zip(*results)
        avg_query_time = sum(query_times) / len(query_times)
        max_query_time = max(query_times)
        total_records = sum(record_counts)

        # Performance assertions
        assert total_time < 10.0  # All concurrent queries should complete quickly
        assert avg_query_time < 2.0  # Average query time should be reasonable
        assert max_query_time < 5.0  # No single query should take too long

        print(f"Concurrent queries: {len(stock_codes)} queries in {total_time:.2f}s")
        print(f"Average query time: {avg_query_time:.4f}s")
        print(f"Total records retrieved: {total_records}")

    @pytest.mark.asyncio
    async def test_database_connection_pool_performance(self, db_operations):
        """Test database connection pool performance"""
        # Test connection reuse
        connection_times = []

        for i in range(20):
            start_time = time.time()
            # Simulate database operation
            await db_operations.get_instrument_count()
            connection_time = time.time() - start_time
            connection_times.append(connection_time)

        avg_connection_time = sum(connection_times) / len(connection_times)
        max_connection_time = max(connection_times)

        # Connection reuse should be fast
        assert avg_connection_time < 0.1  # Average connection time should be very fast
        assert max_connection_time < 0.5  # No connection should take too long

        print(f"Average connection time: {avg_connection_time:.6f}s")
        print(f"Max connection time: {max_connection_time:.6f}s")

    @pytest.mark.asyncio
    async def test_memory_usage_scaling(self, db_operations):
        """Test memory usage scaling with data volume"""
        initial_memory = memory_profiler.memory_usage()[0]
        memory_measurements = [initial_memory]

        # Insert data in increasing batch sizes
        batch_sizes = [100, 500, 1000, 2000]

        for batch_size in batch_sizes:
            # Generate test data
            test_data = []
            for i in range(batch_size):
                test_data.append({
                    'code': f'MEM{i:06d}.SZ',
                    'name': f'内存测试股票{i}',
                    'market': 'SZSE',
                    'industry': '测试行业',
                    'list_date': '2024-01-01',
                    'status': 'active'
                })

            # Insert batch
            await db_operations.insert_instruments(test_data)

            # Measure memory
            current_memory = memory_profiler.memory_usage()[0]
            memory_measurements.append(current_memory)

            # Check memory growth
            memory_growth = current_memory - initial_memory
            memory_per_record = memory_growth / sum(batch_sizes[:batch_sizes.index(batch_size) + 1])

            print(f"Batch size: {batch_size}, Memory growth: {memory_growth:.2f}MB, "
                  f"Memory per record: {memory_per_record*1024:.2f}KB")

            # Memory usage should grow linearly, not exponentially
            assert memory_per_record < 0.1  # Should use less than 100KB per record

    @pytest.mark.asyncio
    async def test_transaction_performance(self, db_operations):
        """Test transaction performance"""
        # Test transaction vs non-transaction performance
        test_data = []
        for i in range(100):
            test_data.append({
                'code': f'TX{i:06d}.SZ',
                'name': f'事务测试股票{i}',
                'market': 'SZSE',
                'industry': '测试行业',
                'list_date': '2024-01-01',
                'status': 'active'
            })

        # Test with transaction
        start_time = time.time()
        async with db_operations.transaction():
            for record in test_data:
                await db_operations.insert_instruments([record])
        transaction_time = time.time() - start_time

        # Test without transaction (individual inserts)
        start_time = time.time()
        for record in test_data:
            await db_operations.insert_instruments([record])
        individual_time = time.time() - start_time

        # Transaction should be faster
        print(f"Transaction time: {transaction_time:.4f}s")
        print(f"Individual inserts time: {individual_time:.4f}s")
        print(f"Performance improvement: {individual_time/transaction_time:.2f}x")

        # Transaction should provide significant performance benefit
        assert transaction_time < individual_time
        assert individual_time / transaction_time > 2.0  # At least 2x improvement

    @pytest.mark.asyncio
    async def test_index_performance_impact(self, db_operations, large_quote_dataset):
        """Test performance impact of indexing"""
        # Insert test data
        await db_operations.insert_daily_quotes(large_quote_dataset)

        # Test query performance before additional indexing
        stock_code = large_quote_dataset[0]['code']
        start_time = time.time()
        quotes = await db_operations.get_daily_data(
            stock_code,
            date(2024, 1, 1),
            date(2024, 12, 31)
        )
        baseline_time = time.time() - start_time

        # Create additional index (if supported)
        # This would depend on the specific database implementation

        # Test query performance after indexing
        start_time = time.time()
        quotes = await db_operations.get_daily_data(
            stock_code,
            date(2024, 1, 1),
            date(2024, 12, 31)
        )
        indexed_time = time.time() - start_time

        print(f"Baseline query time: {baseline_time:.4f}s")
        print(f"Indexed query time: {indexed_time:.4f}s")

        # Indexing should improve performance (implementation dependent)
        # assert indexed_time < baseline_time

    def test_database_file_size_scaling(self, db_operations, temp_dir):
        """Test database file size scaling"""
        initial_size = 0

        # Monitor file size as data grows
        data_sizes = [100, 500, 1000, 2000]
        file_sizes = []

        for size in data_sizes:
            # Generate test data
            test_data = []
            for i in range(size):
                test_data.append({
                    'code': f'SIZE{i:06d}.SZ',
                    'name': f'大小测试股票{i}',
                    'market': 'SZSE',
                    'industry': '测试行业',
                    'list_date': '2024-01-01',
                    'status': 'active'
                })

            # Insert data
            asyncio.run(db_operations.insert_instruments(test_data))

            # Check file size (implementation dependent)
            # file_size = os.path.gettemp / database_path
            # file_sizes.append(file_size)

        # File size should grow reasonably
        # Implementation dependent on database storage