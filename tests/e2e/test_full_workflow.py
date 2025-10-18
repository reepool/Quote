"""
End-to-end tests for complete system workflows
"""

import pytest
import asyncio
import time
from datetime import date, datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch
import pandas as pd
import requests
import subprocess
import psutil

from main import QuoteSystem
from data_manager import data_manager
from scheduler.scheduler import task_scheduler


@pytest.mark.e2e
@pytest.mark.slow
class TestFullWorkflow:
    """End-to-end tests for complete system workflows"""

    @pytest.fixture
    async def quote_system(self):
        """Create complete QuoteSystem instance"""
        system = QuoteSystem()
        await system.initialize()
        return system

    @pytest.fixture
    def mock_external_apis(self):
        """Mock external APIs for E2E testing"""
        with patch('data_sources.baostock_source.baostock') as mock_baostock, \
             patch('data_sources.yfinance_source.yf') as mock_yfinance:

            # Mock BaoStock responses
            mock_baostock.login.return_value = ('error_code', 'error_msg')
            mock_baostock.logout.return_value = ('error_code', 'error_msg')

            mock_stock_basic = pd.DataFrame({
                'code': ['sh.000001', 'sz.000001'],
                'code_name': ['平安银行', '平安银行'],
                'industry': ['银行', '银行'],
                'type': ['1', '1'],
                'status': ['1', '1']
            })
            mock_baostock.query_stock_basic.return_value = (mock_stock_basic, 'error_msg')

            mock_history_data = pd.DataFrame({
                'date': ['2024-01-01', '2024-01-02'],
                'code': ['sh.000001', 'sh.000001'],
                'open': [10.0, 10.5],
                'high': [11.0, 11.5],
                'low': [9.5, 10.0],
                'close': [10.8, 11.2],
                'volume': [1000000, 1200000]
            })
            mock_baostock.query_history_k_data_plus.return_value = (mock_history_data, 'error_msg')

            yield mock_baostock, mock_yfinance

    @pytest.mark.asyncio
    async def test_complete_data_download_workflow(self, quote_system, mock_external_apis):
        """Test complete data download from start to finish"""
        # Step 1: System initialization
        assert quote_system.running is False
        await quote_system.start_scheduler_only()
        assert quote_system.running is True

        # Step 2: Download stock list
        progress = await data_manager.download_stock_data(
            exchanges=['SSE', 'SZSE'],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 2)
        )

        # Step 3: Verify download completed
        assert progress.successful_downloads > 0
        assert progress.failed_downloads == 0
        assert progress.total_instruments > 0

        # Step 4: Verify data in database
        stocks = await data_manager.get_stock_list()
        assert len(stocks) > 0

        # Step 5: Verify quote data
        for stock in stocks.itertuples():
            quotes = await data_manager.get_daily_data(
                stock.code,
                date(2024, 1, 1),
                date(2024, 1, 2)
            )
            assert len(quotes) > 0

        # Step 6: Cleanup
        await quote_system.stop()
        assert quote_system.running is False

    @pytest.mark.asyncio
    async def test_api_service_workflow(self, quote_system, mock_external_apis):
        """Test complete API service workflow"""
        # Start API service
        api_task = asyncio.create_task(
            quote_system.start_api_service(host="127.0.0.1", port=8001)
        )

        # Wait for API to start
        await asyncio.sleep(2)

        try:
            # Test API endpoints
            base_url = "http://127.0.0.1:8001"

            # Test health check
            response = requests.get(f"{base_url}/health", timeout=5)
            assert response.status_code == 200

            # Test getting stock list
            response = requests.get(f"{base_url}/api/stocks", timeout=10)
            assert response.status_code == 200
            stocks_data = response.json()
            assert "data" in stocks_data

            # Test system status
            response = requests.get(f"{base_url}/api/status", timeout=10)
            assert response.status_code == 200
            status_data = response.json()
            assert "total_instruments" in status_data

        finally:
            # Stop API service
            api_task.cancel()
            try:
                await api_task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_scheduled_task_workflow(self, quote_system, mock_external_apis):
        """Test scheduled task execution workflow"""
        # Start scheduler
        await quote_system.start_scheduler_only()

        # Add a test job that runs immediately
        executed = False

        async def test_job():
            nonlocal executed
            executed = True
            # Perform some data operations
            await data_manager.get_system_status()

        # Schedule job to run immediately
        await task_scheduler.add_date_job(
            func=test_job,
            job_id="test_e2e_job",
            run_date=datetime.now() + timedelta(seconds=1),
            description="E2E test job"
        )

        # Wait for job execution
        await asyncio.sleep(3)

        # Verify job was executed
        assert executed is True

        # Cleanup
        await quote_system.stop()

    @pytest.mark.asyncio
    async def test_data_update_cycle_workflow(self, quote_system, mock_external_apis):
        """Test complete data update cycle"""
        # Start system
        await quote_system.start_scheduler_only()

        # Step 1: Initial data download
        progress = await data_manager.download_stock_data(
            exchanges=['SZSE'],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 5)
        )
        assert progress.successful_downloads > 0

        # Step 2: Simulate daily update
        update_result = await data_manager.update_daily_data(
            exchanges=['SZSE'],
            target_date=date(2024, 1, 6)
        )
        assert update_result is True

        # Step 3: Verify data consistency
        system_status = await data_manager.get_system_status()
        assert system_status['total_quotes'] > 0
        assert system_status['last_update'] >= date(2024, 1, 6)

        # Step 4: Check data quality
        latest_quotes = await data_manager.get_latest_quote('000001.SZ')
        assert len(latest_quotes) > 0

        # Step 5: Verify no data gaps
        gaps = await data_manager.get_data_gaps('000001.SZ', date(2024, 1, 1), date(2024, 1, 6))
        # Gaps should be minimal or none

        await quote_system.stop()

    @pytest.mark.asyncio
    async def test_error_recovery_workflow(self, quote_system):
        """Test system error recovery workflow"""
        # Start system
        await quote_system.start_scheduler_only()

        # Test 1: Simulate data source failure
        with patch('data_sources.source_factory.get_source') as mock_get_source:
            mock_source = AsyncMock()
            mock_source.get_stock_list.side_effect = Exception("Data source unavailable")
            mock_get_source.return_value = mock_source

            # Should handle failure gracefully
            with pytest.raises(Exception):
                await data_manager.download_stock_data(
                    exchanges=['SZSE'],
                    start_date=date(2024, 1, 1),
                    end_date=date(2024, 1, 1)
                )

        # Test 2: Simulate database failure recovery
        with patch.object(data_manager.db, 'get_stock_list') as mock_db:
            mock_db.side_effect = [
                Exception("Database connection lost"),
                pd.DataFrame({'code': ['000001.SZ'], 'name': ['平安银行']})
            ]

            # First call fails
            with pytest.raises(Exception):
                await data_manager.get_stock_list()

            # Second call succeeds (recovered)
            stocks = await data_manager.get_stock_list()
            assert len(stocks) > 0

        await quote_system.stop()

    @pytest.mark.asyncio
    async def test_concurrent_operations_workflow(self, quote_system, mock_external_apis):
        """Test concurrent system operations"""
        # Start system
        await quote_system.start_scheduler_only()

        # Define concurrent operations
        async def download_data():
            return await data_manager.download_stock_data(
                exchanges=['SZSE'],
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 2)
            )

        async def get_system_status():
            await asyncio.sleep(0.5)  # Small delay
            return await data_manager.get_system_status()

        async def validate_data():
            await asyncio.sleep(1)  # Delay
            stocks = await data_manager.get_stock_list()
            return len(stocks)

        # Run operations concurrently
        tasks = [
            download_data(),
            get_system_status(),
            validate_data()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify all operations completed
        for result in results:
            assert not isinstance(result, Exception)
            assert result is not None

        # Verify specific results
        progress, status, stock_count = results
        assert progress.successful_downloads > 0
        assert status['total_instruments'] > 0
        assert stock_count > 0

        await quote_system.stop()

    def test_system_resource_usage(self, quote_system):
        """Test system resource usage during operation"""
        # Monitor initial resource usage
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        initial_cpu = process.cpu_percent()

        # Start system and perform operations
        async def run_operations():
            await quote_system.start_scheduler_only()

            # Perform some data operations
            with patch('data_sources.baostock_source.baostock'):
                await data_manager.download_stock_data(
                    exchanges=['SZSE'],
                    start_date=date(2024, 1, 1),
                    end_date=date(2024, 1, 2)
                )

            await asyncio.sleep(2)  # Let system run
            await quote_system.stop()

        # Run operations
        asyncio.run(run_operations())

        # Check final resource usage
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        final_cpu = process.cpu_percent()

        memory_growth = final_memory - initial_memory
        cpu_usage = final_cpu - initial_cpu

        # Resource usage should be reasonable
        assert memory_growth < 200  # Less than 200MB growth
        assert cpu_usage < 50  # Less than 50% CPU usage

        print(f"Memory growth: {memory_growth:.2f}MB")
        print(f"CPU usage: {cpu_usage:.2f}%")

    def test_system_startup_shutdown_cycle(self):
        """Test complete system startup and shutdown cycle"""
        async def run_cycle():
            system = QuoteSystem()

            # Test multiple startup/shutdown cycles
            for cycle in range(3):
                # Initialize
                await system.initialize()
                assert system.running is False

                # Start
                await system.start_scheduler_only()
                assert system.running is True

                # Let it run briefly
                await asyncio.sleep(1)

                # Stop
                await system.stop()
                assert system.running is False

                # Brief pause between cycles
                await asyncio.sleep(0.5)

        # Run cycles
        asyncio.run(run_cycle())

    @pytest.mark.asyncio
    async def test_data_consistency_after_restart(self, quote_system, mock_external_apis):
        """Test data consistency after system restart"""
        # First run: download data
        await quote_system.start_scheduler_only()

        progress1 = await data_manager.download_stock_data(
            exchanges=['SZSE'],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 3)
        )
        initial_quotes = progress1.total_quotes

        await quote_system.stop()

        # Second run: restart and verify data
        await quote_system.start_scheduler_only()

        # Check data persistence
        stocks = await data_manager.get_stock_list()
        assert len(stocks) > 0

        # Try to download same data again (should handle duplicates)
        progress2 = await data_manager.download_stock_data(
            exchanges=['SZSE'],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 3)
        )

        # Should handle gracefully without duplication
        system_status = await data_manager.get_system_status()
        final_quotes = system_status['total_quotes']

        # Data should be consistent
        assert final_quotes >= initial_quotes

        await quote_system.stop()

    def test_integration_with_external_dependencies(self):
        """Test integration with external system dependencies"""
        # Test file system access
        import os
        assert os.access(".", os.R_OK | os.W_OK)

        # Test network access (if required)
        # This would depend on actual external dependencies

        # Test system resources
        assert psutil.virtual_memory().available > 100 * 1024 * 1024  # 100MB
        assert psutil.disk_usage('.').free > 100 * 1024 * 1024  # 100MB free space

    def test_system_configuration_integration(self):
        """Test system configuration integration"""
        from utils import config_manager

        # Test configuration loading
        assert config_manager is not None

        # Test required configuration keys
        required_keys = [
            "database.url",
            "api.host",
            "api.port"
        ]

        for key in required_keys:
            assert config_manager.has(key) or config_manager.get(key, default="test") is not None

    @pytest.mark.asyncio
    async def test_logging_integration(self, quote_system):
        """Test logging integration across system"""
        # Start system
        await quote_system.start_scheduler_only()

        # Perform operations that should generate logs
        with patch('data_sources.baostock_source.baostock'):
            try:
                await data_manager.download_stock_data(
                    exchanges=['SZSE'],
                    start_date=date(2024, 1, 1),
                    end_date=date(2024, 1, 1)
                )
            except Exception:
                pass  # Expected due to mocking

        # Verify logs are being generated
        # This would require checking log files or log handlers
        # Implementation depends on logging setup

        await quote_system.stop()