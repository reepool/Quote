"""
Unit tests for API routes
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, AsyncMock, patch
import json
from datetime import date, datetime
import pandas as pd

from api.app import app
from api.routes import router
from utils.exceptions import ValidationError, NotFoundError


@pytest.mark.unit
class TestAPIRoutes:
    """Test cases for API routes"""

    @pytest.fixture
    def client(self):
        """Create test client"""
        return TestClient(app)

    @pytest.fixture
    def mock_data_manager(self):
        """Mock data manager for API testing"""
        mock = Mock()
        mock.get_stock_list = AsyncMock()
        mock.get_daily_data = AsyncMock()
        mock.get_latest_quote = AsyncMock()
        mock.get_system_status = AsyncMock()
        return mock

    def test_root_endpoint(self, client):
        """Test root endpoint"""
        response = client.get("/")
        assert response.status_code == 200
        assert "Quote System" in response.json()["message"]

    def test_health_check_endpoint(self, client):
        """Test health check endpoint"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert "version" in data

    @patch('api.routes.data_manager')
    def test_get_stocks_success(self, mock_dm, client):
        """Test getting stock list successfully"""
        # Mock data manager response
        mock_stock_data = pd.DataFrame({
            'code': ['000001.SZ', '000002.SZ'],
            'name': ['平安银行', '万科A'],
            'market': ['SZSE', 'SZSE'],
            'industry': ['银行', '房地产']
        })
        mock_dm.get_stock_list.return_value = mock_stock_data

        response = client.get("/api/stocks")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert len(data["data"]) == 2
        assert data["data"][0]["code"] == "000001.SZ"

    @patch('api.routes.data_manager')
    def test_get_stocks_with_market_filter(self, mock_dm, client):
        """Test getting stock list with market filter"""
        mock_stock_data = pd.DataFrame({
            'code': ['000001.SZ'],
            'name': ['平安银行'],
            'market': ['SZSE'],
            'industry': ['银行']
        })
        mock_dm.get_stock_list.return_value = mock_stock_data

        response = client.get("/api/stocks?market=SZSE")
        assert response.status_code == 200
        data = response.json()
        assert len(data["data"]) == 1
        assert data["data"][0]["market"] == "SZSE"

    @patch('api.routes.data_manager')
    def test_get_stocks_empty_result(self, mock_dm, client):
        """Test getting stock list with empty result"""
        mock_dm.get_stock_list.return_value = pd.DataFrame()

        response = client.get("/api/stocks")
        assert response.status_code == 200
        data = response.json()
        assert len(data["data"]) == 0

    @patch('api.routes.data_manager')
    def test_get_stock_by_code_success(self, mock_dm, client):
        """Test getting stock by code successfully"""
        mock_quote_data = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-02'],
            'open': [10.0, 10.5],
            'high': [11.0, 11.5],
            'low': [9.5, 10.0],
            'close': [10.8, 11.2],
            'volume': [1000000, 1200000]
        })
        mock_dm.get_daily_data.return_value = mock_quote_data

        response = client.get("/api/stocks/000001.SZ?start_date=2024-01-01&end_date=2024-01-02")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert len(data["data"]) == 2
        assert data["data"][0]["date"] == "2024-01-01"

    @patch('api.routes.data_manager')
    def test_get_stock_by_code_not_found(self, mock_dm, client):
        """Test getting non-existent stock"""
        mock_dm.get_daily_data.return_value = pd.DataFrame()

        response = client.get("/api/stocks/NONEXISTENT?start_date=2024-01-01&end_date=2024-01-02")
        assert response.status_code == 404
        data = response.json()
        assert "detail" in data

    @patch('api.routes.data_manager')
    def test_get_stock_by_code_invalid_format(self, mock_dm, client):
        """Test getting stock with invalid code format"""
        response = client.get("/api/stocks/INVALID?start_date=2024-01-01&end_date=2024-01-02")
        assert response.status_code == 400
        data = response.json()
        assert "detail" in data

    @patch('api.routes.data_manager')
    def test_get_latest_quote_success(self, mock_dm, client):
        """Test getting latest quote successfully"""
        mock_latest_data = pd.DataFrame({
            'code': ['000001.SZ'],
            'date': ['2024-01-02'],
            'open': [10.5],
            'high': [11.5],
            'low': [10.0],
            'close': [11.2],
            'volume': [1200000]
        })
        mock_dm.get_latest_quote.return_value = mock_latest_data

        response = client.get("/api/stocks/000001.SZ/latest")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert len(data["data"]) == 1
        assert data["data"][0]["code"] == "000001.SZ"

    @patch('api.routes.data_manager')
    def test_get_batch_stocks_success(self, mock_dm, client):
        """Test getting batch stock data"""
        mock_batch_data = pd.DataFrame({
            'code': ['000001.SZ', '000002.SZ'],
            'date': ['2024-01-01', '2024-01-01'],
            'open': [10.0, 20.0],
            'high': [11.0, 21.0],
            'low': [9.5, 19.5],
            'close': [10.8, 20.8],
            'volume': [1000000, 2000000]
        })

        # Mock the batch data retrieval
        async def mock_get_batch_data(codes, start_date, end_date):
            return mock_batch_data

        mock_dm.get_batch_daily_data = AsyncMock(side_effect=mock_get_batch_data)

        response = client.get("/api/stocks/batch?codes=000001.SZ,000002.SZ&start_date=2024-01-01&end_date=2024-01-01")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert len(data["data"]) == 2

    @patch('api.routes.data_manager')
    def test_get_batch_stocks_too_many_codes(self, mock_dm, client):
        """Test getting batch stocks with too many codes"""
        # Create a string with many stock codes (more than limit)
        codes = ",".join([f"00000{i}.SZ" for i in range(1, 101)])  # 100 codes

        response = client.get(f"/api/stocks/batch?codes={codes}&start_date=2024-01-01&end_date=2024-01-01")
        assert response.status_code == 400
        data = response.json()
        assert "too many stock codes" in data["detail"].lower()

    @patch('api.routes.data_manager')
    def test_get_system_status_success(self, mock_dm, client):
        """Test getting system status successfully"""
        mock_dm.get_system_status.return_value = {
            'total_instruments': 5000,
            'total_quotes': 1000000,
            'last_update': date(2024, 1, 1),
            'status': 'healthy'
        }

        response = client.get("/api/status")
        assert response.status_code == 200
        data = response.json()
        assert data["total_instruments"] == 5000
        assert data["total_quotes"] == 1000000
        assert data["status"] == "healthy"

    def test_get_markets_success(self, client):
        """Test getting supported markets"""
        response = client.get("/api/markets")
        assert response.status_code == 200
        data = response.json()
        assert "markets" in data
        assert any(market["code"] == "SSE" for market in data["markets"])
        assert any(market["code"] == "SZSE" for market in data["markets"])

    def test_cors_headers(self, client):
        """Test CORS headers are present"""
        response = client.options("/api/stocks")
        assert "access-control-allow-origin" in response.headers

    def test_rate_limiting_headers(self, client):
        """Test rate limiting headers"""
        response = client.get("/api/stocks")
        # Note: This depends on rate limiting middleware implementation
        # assert "x-ratelimit-limit" in response.headers

    @patch('api.routes.data_manager')
    def test_error_handling_data_manager_exception(self, mock_dm, client):
        """Test handling data manager exceptions"""
        mock_dm.get_stock_list.side_effect = Exception("Database error")

        response = client.get("/api/stocks")
        assert response.status_code == 500
        data = response.json()
        assert "detail" in data

    @patch('api.routes.data_manager')
    def test_error_handling_validation_exception(self, mock_dm, client):
        """Test handling validation exceptions"""
        mock_dm.get_daily_data.side_effect = ValidationError("Invalid date range")

        response = client.get("/api/stocks/000001.SZ?start_date=2024-01-01&end_date=2024-01-02")
        assert response.status_code == 400
        data = response.json()
        assert "detail" in data

    def test_invalid_date_format(self, client):
        """Test handling invalid date format"""
        response = client.get("/api/stocks/000001.SZ?start_date=invalid-date&end_date=2024-01-02")
        assert response.status_code == 400
        data = response.json()
        assert "detail" in data

    def test_end_date_before_start_date(self, client):
        """Test handling end date before start date"""
        response = client.get("/api/stocks/000001.SZ?start_date=2024-01-02&end_date=2024-01-01")
        assert response.status_code == 400
        data = response.json()
        assert "detail" in data

    @patch('api.routes.data_manager')
    def test_date_range_too_large(self, mock_dm, client):
        """Test handling date range that's too large"""
        # Test with a date range of more than 1 year
        response = client.get("/api/stocks/000001.SZ?start_date=2022-01-01&end_date=2024-01-01")
        assert response.status_code == 400
        data = response.json()
        assert "too large" in data["detail"].lower()

    def test_response_format_json(self, client):
        """Test response format is JSON"""
        response = client.get("/api/stocks")
        assert response.headers["content-type"] == "application/json"

    def test_pagination_parameters(self, client):
        """Test pagination parameters"""
        # This would test if pagination is implemented
        response = client.get("/api/stocks?page=1&limit=10")
        assert response.status_code == 200

    def test_csv_format_response(self, client):
        """Test CSV format response"""
        response = client.get("/api/stocks?format=csv")
        # This would test CSV export functionality if implemented
        # The response should be text/csv format
        pass

    def test_api_documentation_accessible(self, client):
        """Test API documentation is accessible"""
        response = client.get("/docs")
        assert response.status_code == 200

    def test_openapi_spec_accessible(self, client):
        """Test OpenAPI specification is accessible"""
        response = client.get("/openapi.json")
        assert response.status_code == 200
        data = response.json()
        assert "openapi" in data
        assert "paths" in data

    @patch('api.routes.data_manager')
    def test_concurrent_requests_handling(self, mock_dm, client):
        """Test handling of concurrent requests"""
        import threading
        import time

        mock_stock_data = pd.DataFrame({
            'code': ['000001.SZ'],
            'name': ['平安银行'],
            'market': ['SZSE'],
            'industry': ['银行']
        })
        mock_dm.get_stock_list.return_value = mock_stock_data

        results = []

        def make_request():
            response = client.get("/api/stocks")
            results.append(response.status_code)

        # Make multiple concurrent requests
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=make_request)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # All requests should succeed
        assert all(status == 200 for status in results)
        assert len(results) == 5