"""
Test data factories for Quote System tests
Provides factories for creating realistic test data
"""

import random
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import pandas as pd
from faker import Faker

# Initialize faker
fake = Faker('zh_CN')


class InstrumentFactory:
    """Factory for creating test instrument data"""

    @staticmethod
    def create_instrument(code: str = None, name: str = None, market: str = "SZSE") -> Dict[str, Any]:
        """Create a single instrument record"""
        if code is None:
            code = f"{random.randint(000001, 999999):06d}.{market}"

        if name is None:
            name = fake.company()

        return {
            'code': code,
            'name': name,
            'market': market,
            'industry': fake.job(),
            'list_date': fake.date_between(start_date='-20y', end_date='today'),
            'status': 'active',
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }

    @staticmethod
    def create_instruments(count: int = 10, markets: List[str] = None) -> List[Dict[str, Any]]:
        """Create multiple instrument records"""
        if markets is None:
            markets = ['SZSE', 'SSE', 'BSE', 'HKEX']

        instruments = []
        for _ in range(count):
            market = random.choice(markets)
            instruments.append(InstrumentFactory.create_instrument(market=market))

        return instruments


class QuoteFactory:
    """Factory for creating test quote/daily data"""

    @staticmethod
    def create_quote(
        code: str = "000001.SZ",
        trade_date: str = None,
        base_price: float = 10.0
    ) -> Dict[str, Any]:
        """Create a single quote record"""
        if trade_date is None:
            trade_date = fake.date_between(start_date='-1y', end_date='today')

        # Generate realistic OHLCV data
        variation = random.uniform(-0.1, 0.1)  # 10% max variation
        open_price = base_price * (1 + variation)
        close_price = base_price * (1 + random.uniform(-0.05, 0.05))
        high_price = max(open_price, close_price) * (1 + random.uniform(0, 0.03))
        low_price = min(open_price, close_price) * (1 - random.uniform(0, 0.03))

        # Generate volume and amount
        volume = random.randint(100000, 10000000)
        amount = volume * (high_price + low_price) / 2

        return {
            'code': code,
            'date': trade_date,
            'open': round(open_price, 2),
            'high': round(high_price, 2),
            'low': round(low_price, 2),
            'close': round(close_price, 2),
            'volume': volume,
            'amount': round(amount, 2),
            'created_at': datetime.now()
        }

    @staticmethod
    def create_quotes(
        codes: List[str] = None,
        start_date: date = None,
        end_date: date = None,
        base_price: float = 10.0
    ) -> List[Dict[str, Any]]:
        """Create multiple quote records"""
        if codes is None:
            codes = ["000001.SZ", "000002.SZ", "600000.SSE"]

        if start_date is None:
            start_date = date.today() - timedelta(days=30)

        if end_date is None:
            end_date = date.today()

        quotes = []
        current_date = start_date

        while current_date <= end_date:
            # Skip weekends
            if current_date.weekday() < 5:  # Monday to Friday
                for code in codes:
                    # Add some price progression
                    price_variation = 1 + (current_date - start_date).days * 0.001
                    quotes.append(QuoteFactory.create_quote(
                        code=code,
                        trade_date=current_date.strftime('%Y-%m-%d'),
                        base_price=base_price * price_variation
                    ))

            current_date += timedelta(days=1)

        return quotes

    @staticmethod
    def create_quotes_dataframe(count: int = 100) -> pd.DataFrame:
        """Create quotes as pandas DataFrame"""
        codes = ["000001.SZ", "000002.SZ", "600000.SSE", "600001.SSE"]
        start_date = date.today() - timedelta(days=count // len(codes))
        end_date = date.today()

        quotes = QuoteFactory.create_quotes(codes=codes, start_date=start_date, end_date=end_date)
        return pd.DataFrame(quotes)


class DataSourceFactory:
    """Factory for creating mock data source responses"""

    @staticmethod
    def create_stock_list_response(market: str = "SZSE", count: int = 10) -> pd.DataFrame:
        """Create mock stock list response"""
        instruments = InstrumentFactory.create_instruments(count=count, markets=[market])

        return pd.DataFrame([{
            'code': inst['code'],
            'name': inst['name'],
            'industry': inst['industry'],
            'market': inst['market'],
            'list_date': inst['list_date']
        } for inst in instruments])

    @staticmethod
    def create_daily_data_response(
        code: str = "000001.SZ",
        start_date: str = "2024-01-01",
        end_date: str = "2024-01-31"
    ) -> pd.DataFrame:
        """Create mock daily data response"""
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()

        quotes = QuoteFactory.create_quotes(
            codes=[code],
            start_date=start,
            end_date=end,
            base_price=10.0
        )

        df = pd.DataFrame(quotes)
        # Remove non-standard columns for data source response
        columns_to_keep = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
        return df[columns_to_keep].sort_values('date')


class TradingCalendarFactory:
    """Factory for creating trading calendar data"""

    @staticmethod
    def create_trading_days(
        start_date: date = None,
        end_date: date = None,
        exclude_weekends: bool = True
    ) -> List[date]:
        """Create trading days list"""
        if start_date is None:
            start_date = date.today() - timedelta(days=365)

        if end_date is None:
            end_date = date.today()

        trading_days = []
        current_date = start_date

        while current_date <= end_date:
            if not exclude_weekends or current_date.weekday() < 5:
                # Randomly exclude some days as holidays (5% chance)
                if random.random() > 0.05:
                    trading_days.append(current_date)

            current_date += timedelta(days=1)

        return trading_days

    @staticmethod
    def create_holidays(
        start_date: date = None,
        end_date: date = None,
        count: int = 10
    ) -> List[date]:
        """Create holiday list"""
        if start_date is None:
            start_date = date.today() - timedelta(days=365)

        if end_date is None:
            end_date = date.today()

        holidays = []
        for _ in range(count):
            holiday = fake.date_between(start_date=start_date, end_date=end_date)
            # Ensure it's a weekday (most holidays are on weekdays)
            if holiday.weekday() < 5:
                holidays.append(holiday)

        return sorted(list(set(holidays)))


class ConfigFactory:
    """Factory for creating test configurations"""

    @staticmethod
    def create_test_config() -> Dict[str, Any]:
        """Create comprehensive test configuration"""
        return {
            "database": {
                "url": "sqlite:///:memory:",
                "echo": False,
                "pool_size": 1,
                "max_overflow": 0
            },
            "logging": {
                "level": "WARNING",
                "console_enabled": False,
                "file_enabled": False
            },
            "data_sources": {
                "baostock": {
                    "enabled": False,
                    "rate_limit": {"requests_per_second": 1}
                },
                "yfinance": {
                    "enabled": False,
                    "rate_limit": {"requests_per_second": 1}
                },
                "akshare": {
                    "enabled": False,
                    "rate_limit": {"requests_per_second": 1}
                }
            },
            "cache": {
                "enabled": True,
                "ttl": 300,
                "max_size": 1000
            },
            "api": {
                "host": "127.0.0.1",
                "port": 8001,
                "cors_origins": ["http://localhost:3000"]
            },
            "scheduler": {
                "enabled": True,
                "timezone": "Asia/Shanghai",
                "max_instances": 1
            }
        }

    @staticmethod
    def create_performance_test_config() -> Dict[str, Any]:
        """Create configuration for performance testing"""
        return {
            "database": {
                "url": "sqlite:///test_performance.db",
                "echo": False,
                "pool_size": 10,
                "max_overflow": 20
            },
            "cache": {
                "enabled": True,
                "ttl": 60,
                "max_size": 10000
            },
            "performance": {
                "benchmark_iterations": 100,
                "data_size": 1000,
                "concurrent_requests": 50
            }
        }


class APIDataFactory:
    """Factory for creating API test data"""

    @staticmethod
    def create_api_response(
        status_code: int = 200,
        data: Any = None,
        message: str = "Success"
    ) -> Dict[str, Any]:
        """Create standardized API response"""
        if data is None:
            data = {"timestamp": datetime.now().isoformat()}

        return {
            "status_code": status_code,
            "data": data,
            "message": message,
            "success": status_code < 400
        }

    @staticmethod
    def create_error_response(
        status_code: int = 400,
        error_code: str = "VALIDATION_ERROR",
        message: str = "Invalid input"
    ) -> Dict[str, Any]:
        """Create error API response"""
        return {
            "status_code": status_code,
            "error": {
                "code": error_code,
                "message": message,
                "timestamp": datetime.now().isoformat()
            },
            "success": False
        }

    @staticmethod
    def create_pagination_response(
        data: List[Any] = None,
        page: int = 1,
        page_size: int = 20,
        total: int = None
    ) -> Dict[str, Any]:
        """Create paginated API response"""
        if data is None:
            data = [InstrumentFactory.create_instrument() for _ in range(page_size)]

        if total is None:
            total = len(data) * 10  # Simulate more total records

        total_pages = (total + page_size - 1) // page_size

        return {
            "status_code": 200,
            "data": {
                "items": data,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total": total,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_prev": page > 1
                }
            },
            "message": "Success",
            "success": True
        }


# Utility functions for test data generation
def create_test_database_data(instrument_count: int = 10, quote_days: int = 30) -> Dict[str, Any]:
    """Create comprehensive test data for database testing"""
    instruments = InstrumentFactory.create_instruments(count=instrument_count)
    codes = [inst['code'] for inst in instruments]

    start_date = date.today() - timedelta(days=quote_days)
    end_date = date.today()

    quotes = QuoteFactory.create_quotes(codes=codes, start_date=start_date, end_date=end_date)

    return {
        'instruments': instruments,
        'quotes': quotes,
        'trading_days': TradingCalendarFactory.create_trading_days(start_date, end_date)
    }


def create_performance_test_data(size: int = 1000) -> Dict[str, Any]:
    """Create data for performance testing"""
    instruments = InstrumentFactory.create_instruments(count=min(100, size))
    codes = [inst['code'] for inst in instruments]

    quotes = QuoteFactory.create_quotes(
        codes=codes[:10],  # Limit to 10 instruments for manageability
        start_date=date.today() - timedelta(days=size // len(codes)),
        end_date=date.today()
    )

    return {
        'instruments': instruments,
        'quotes': quotes[:size] if len(quotes) > size else quotes,
        'size': size
    }