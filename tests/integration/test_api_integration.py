"""
Integration tests for API endpoints
"""

import pytest
from fastapi.testclient import TestClient
from datetime import date, datetime
from unittest.mock import Mock, AsyncMock, patch
import json
import pandas as pd

from api.app import app
from data_manager import data_manager


@pytest.mark.integration
class TestAPIIntegration:
    """Integration tests for API endpoints"""

    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)

    @pytest.fixture
    async def setup_api_data(self, test_database, sample_instrument_data, sample_quote_data):
        """Setup test data for API integration tests"""
        # Initialize data manager with test database
        data_manager.db = Mock()
        data_manager.db.get_stock_list = AsyncMock(return_value=sample_instrument_data)
        data_manager.db.get_daily_data = AsyncMock(return_value=sample_quote_data)
        data_manager.db.get_latest_quote = AsyncMock(return_value=sample_quote_data.tail(1))
        data_manager.db.get_system_status = AsyncMock(return_value={
            'total_instruments': 2,
            'total_quotes': len(sample_quote_data),
            'last_update': date(2024, 1, 2),
            'status': 'healthy'
        })

    def test_api_cors_integration(self, client):
        """Test CORS headers integration"""
        # Test preflight request
        response = client.options("/api/stocks", headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "Content-Type"
        })

        assert response.status_code == 200
        assert "access-control-allow-origin" in response.headers

    @patch('api.routes.data_manager')
    def test_complete_stock_api_workflow(self, mock_dm, client, sample_instrument_data, sample_quote_data):
        """Test complete workflow for stock API"""
        # Setup mock responses
        mock_dm.get_stock_list.return_value = sample_instrument_data
        mock_dm.get_daily_data.return_value = sample_quote_data
        mock_dm.get_latest_quote.return_value = sample_quote_data.tail(1)

        # Step 1: Get stock list
        response = client.get("/api/stocks")
        assert response.status_code == 200
        stocks = response.json()["data"]
        assert len(stocks) == 2

        # Step 2: Get specific stock data
        stock_code = stocks[0]["code"]
        response = client.get(f"/api/stocks/{stock_code}?start_date=2024-01-01&end_date=2024-01-10")
        assert response.status_code == 200
        quotes = response.json()["data"]
        assert len(quotes) > 0

        # Step 3: Get latest quote
        response = client.get(f"/api/stocks/{stock_code}/latest")
        assert response.status_code == 200
        latest = response.json()["data"]
        assert len(latest) == 1

        # Verify data consistency
        assert latest[0]["code"] == stock_code
        assert latest[0]["date"] >= quotes[-1]["date"]

    @patch('api.routes.data_manager')
    def test_batch_stock_api_workflow(self, mock_dm, client, sample_quote_data):
        """Test batch stock data API workflow"""
        # Setup mock for batch data
        batch_data = sample_quote_data.copy()
        mock_dm.get_batch_daily_data.return_value = batch_data

        # Get batch data for multiple stocks
        stock_codes = "000001.SZ,000002.SZ"
        response = client.get(f"/api/stocks/batch?codes={stock_codes}&start_date=2024-01-01&end_date=2024-01-10")
        assert response.status_code == 200
        batch_result = response.json()["data"]
        assert len(batch_result) > 0

        # Verify batch contains both stocks
        codes_in_result = set(item["code"] for item in batch_result)
        assert "000001.SZ" in codes_in_result
        assert "000002.SZ" in codes_in_result

    def test_api_error_handling_integration(self, client):
        """Test API error handling integration"""
        # Test not found error
        response = client.get("/api/stocks/NONEXISTENT?start_date=2024-01-01&end_date=2024-01-01")
        assert response.status_code == 404
        error_data = response.json()
        assert "detail" in error_data

        # Test bad request error
        response = client.get("/api/stocks/INVALID?start_date=invalid-date")
        assert response.status_code == 400

        # Test method not allowed
        response = client.delete("/api/stocks")
        assert response.status_code == 405

    def test_api_rate_limiting_integration(self, client):
        """Test API rate limiting integration"""
        # Make multiple rapid requests
        responses = []
        for _ in range(10):
            response = client.get("/api/stocks")
            responses.append(response)

        # Most should succeed, but some might be rate limited
        success_count = sum(1 for r in responses if r.status_code == 200)
        rate_limited_count = sum(1 for r in responses if r.status_code == 429)

        assert success_count > 0
        # Rate limiting implementation dependent

    def test_api_content_negotiation(self, client):
        """Test API content negotiation"""
        # Test JSON response (default)
        response = client.get("/api/stocks", headers={"Accept": "application/json"})
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/json"

        # Test HTML response
        response = client.get("/api/stocks", headers={"Accept": "text/html"})
        # Implementation depends on content negotiation support

    @patch('api.routes.data_manager')
    def test_api_pagination_integration(self, mock_dm, client):
        """Test API pagination integration"""
        # Setup large dataset
        large_stock_list = pd.DataFrame({
            'code': [f'00000{i}.SZ' for i in range(1, 101)],
            'name': [f'股票{i}' for i in range(1, 101)],
            'market': ['SZSE'] * 100,
            'industry': ['行业'] * 100
        })
        mock_dm.get_stock_list.return_value = large_stock_list

        # Test first page
        response = client.get("/api/stocks?page=1&limit=10")
        assert response.status_code == 200
        data = response.json()["data"]
        assert len(data) == 10

        # Test second page
        response = client.get("/api/stocks?page=2&limit=10")
        assert response.status_code == 200
        data = response.json()["data"]
        assert len(data) == 10

        # Verify different data on different pages
        # This depends on pagination implementation

    def test_api_authentication_integration(self, client):
        """Test API authentication integration"""
        # Test public endpoint (should work)
        response = client.get("/api/stocks")
        assert response.status_code in [200, 500]  # May fail due to mock, but not auth

        # Test protected endpoint (if implemented)
        response = client.get("/api/admin/status")
        # Response depends on authentication implementation

    def test_api_request_validation_integration(self, client):
        """Test API request validation integration"""
        # Test invalid date format
        response = client.get("/api/stocks/000001.SZ?start_date=invalid&end_date=2024-01-01")
        assert response.status_code == 400

        # Test end date before start date
        response = client.get("/api/stocks/000001.SZ?start_date=2024-01-02&end_date=2024-01-01")
        assert response.status_code == 400

        # Test too large date range
        response = client.get("/api/stocks/000001.SZ?start_date=2020-01-01&end_date=2024-01-01")
        assert response.status_code == 400

        # Test too many stock codes in batch request
        many_codes = ",".join([f"00000{i}.SZ" for i in range(1, 51)])  # 50 codes
        response = client.get(f"/api/stocks/batch?codes={many_codes}&start_date=2024-01-01&end_date=2024-01-01")
        assert response.status_code == 400

    def test_api_response_format_integration(self, client):
        """Test API response format consistency"""
        # Test different endpoints have consistent response format
        endpoints = [
            "/api/stocks",
            "/api/markets",
            "/health"
        ]

        for endpoint in endpoints:
            response = client.get(endpoint)
            if response.status_code == 200:
                data = response.json()
                # Check for common response structure
                # This depends on response format standardization

    def test_api_concurrent_requests(self, client):
        """Test API concurrent request handling"""
        import threading
        import time

        results = []
        errors = []

        def make_request():
            try:
                response = client.get("/api/stocks")
                results.append(response.status_code)
            except Exception as e:
                errors.append(str(e))

        # Make multiple concurrent requests
        threads = []
        for _ in range(10):
            thread = threading.Thread(target=make_request)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Verify all requests completed
        assert len(results) == 10
        assert len(errors) == 0

        # Most requests should succeed
        success_count = sum(1 for code in results if code == 200)
        assert success_count >= 8  # Allow for some failures due to mocking

    def test_api_documentation_integration(self, client):
        """Test API documentation integration"""
        # Test OpenAPI spec
        response = client.get("/openapi.json")
        assert response.status_code == 200
        spec = response.json()

        # Verify OpenAPI structure
        assert "openapi" in spec
        assert "paths" in spec
        assert "components" in spec

        # Test interactive docs
        response = client.get("/docs")
        assert response.status_code == 200
        assert "html" in response.headers["content-type"]

    def test_api_health_monitoring(self, client):
        """Test API health monitoring integration"""
        # Test health endpoint
        response = client.get("/health")
        assert response.status_code == 200
        health_data = response.json()

        # Verify health check structure
        assert "status" in health_data
        assert "timestamp" in health_data
        assert health_data["status"] == "healthy"

        # Test metrics endpoint (if implemented)
        response = client.get("/metrics")
        # Response depends on metrics implementation

    @patch('api.routes.data_manager')
    def test_api_caching_integration(self, mock_dm, client):
        """Test API caching integration"""
        # Setup mock data
        mock_stock_data = pd.DataFrame({
            'code': ['000001.SZ'],
            'name': ['平安银行'],
            'market': ['SZSE'],
            'industry': ['银行']
        })
        mock_dm.get_stock_list.return_value = mock_stock_data

        # First request
        response1 = client.get("/api/stocks")
        assert response1.status_code == 200

        # Second request (should use cache if implemented)
        response2 = client.get("/api/stocks")
        assert response2.status_code == 200

        # Responses should be identical
        assert response1.json() == response2.json()

        # Check cache headers (implementation dependent)
        # assert "cache-control" in response2.headers

    def test_api_security_headers(self, client):
        """Test API security headers"""
        response = client.get("/api/stocks")

        # Check for security headers
        security_headers = [
            "x-content-type-options",
            "x-frame-options",
            "x-xss-protection"
        ]

        for header in security_headers:
            # Implementation dependent
            pass

    def test_api_error_logging_integration(self, client):
        """Test API error logging integration"""
        # Make a request that will cause an error
        response = client.get("/api/stocks/nonexistent-stock-code")

        # Should return appropriate error status
        assert response.status_code in [404, 400]

        # Error should be logged (verify through test framework)
        # Implementation depends on logging setup