"""
Unit tests for date utilities
Tests the date and time handling functionality
"""

import pytest
from datetime import datetime, date, timedelta, timezone
from unittest.mock import Mock, patch
import pytz

from utils.date_utils import (
    parse_date, format_date, is_trading_day, get_next_trading_day,
    get_previous_trading_day, get_trading_days_range, get_time_ago_string,
    get_datetime_string, convert_timezone, is_market_open, get_market_status
)


@pytest.mark.unit
class TestDateUtils:
    """Test date utility functions"""

    def test_parse_date_string(self):
        """Test parsing date strings"""
        # Test valid date formats
        assert parse_date("2024-01-01") == date(2024, 1, 1)
        assert parse_date("2024/01/01") == date(2024, 1, 1)
        assert parse_date("01-01-2024") == date(2024, 1, 1)

        # Test with datetime objects
        dt = datetime(2024, 1, 1, 12, 30, 45)
        assert parse_date(dt) == date(2024, 1, 1)

        # Test with date objects
        d = date(2024, 1, 1)
        assert parse_date(d) == date(2024, 1, 1)

        # Test invalid date
        with pytest.raises(ValueError):
            parse_date("invalid-date")

    def test_format_date(self):
        """Test formatting dates"""
        d = date(2024, 1, 15)

        # Test default format
        assert format_date(d) == "2024-01-15"

        # Test custom formats
        assert format_date(d, "%Y/%m/%d") == "2024/01/15"
        assert format_date(d, "%d-%m-%Y") == "15-01-2024"
        assert format_date(d, "%B %d, %Y") == "January 15, 2024"

        # Test with datetime
        dt = datetime(2024, 1, 15, 14, 30, 0)
        assert format_date(dt) == "2024-01-15"
        assert format_date(dt, "%Y-%m-%d %H:%M") == "2024-01-15 14:30"

    def test_is_trading_day(self):
        """Test trading day detection"""
        # Test weekdays
        monday = date(2024, 1, 8)  # Monday
        tuesday = date(2024, 1, 9)  # Tuesday
        wednesday = date(2024, 1, 10)  # Wednesday
        thursday = date(2024, 1, 11)  # Thursday
        friday = date(2024, 1, 12)  # Friday

        assert is_trading_day(monday) is True
        assert is_trading_day(tuesday) is True
        assert is_trading_day(wednesday) is True
        assert is_trading_day(thursday) is True
        assert is_trading_day(friday) is True

        # Test weekends
        saturday = date(2024, 1, 13)  # Saturday
        sunday = date(2024, 1, 14)  # Sunday

        assert is_trading_day(saturday) is False
        assert is_trading_day(sunday) is False

        # Test with holidays (mocked)
        with patch('utils.date_utils.is_holiday', return_value=True):
            assert is_trading_day(monday) is False

    def test_get_next_trading_day(self):
        """Test getting next trading day"""
        # Test Friday to Monday
        friday = date(2024, 1, 12)  # Friday
        next_day = get_next_trading_day(friday)
        assert next_day == date(2024, 1, 15)  # Monday

        # Test weekday to next day
        tuesday = date(2024, 1, 9)  # Tuesday
        next_day = get_next_trading_day(tuesday)
        assert next_day == date(2024, 1, 10)  # Wednesday

        # Test with holiday (mocked)
        wednesday = date(2024, 1, 10)
        with patch('utils.date_utils.is_trading_day') as mock_is_trading:
            mock_is_trading.side_effect = lambda d: d.weekday() < 5 and d != date(2024, 1, 11)
            next_day = get_next_trading_day(wednesday)
            assert next_day == date(2024, 1, 12)  # Skips Thursday (holiday)

    def test_get_previous_trading_day(self):
        """Test getting previous trading day"""
        # Test Monday to Friday
        monday = date(2024, 1, 15)  # Monday
        prev_day = get_previous_trading_day(monday)
        assert prev_day == date(2024, 1, 12)  # Friday

        # Test weekday to previous day
        wednesday = date(2024, 1, 10)  # Wednesday
        prev_day = get_previous_trading_day(wednesday)
        assert prev_day == date(2024, 1, 9)  # Tuesday

        # Test with holiday (mocked)
        tuesday = date(2024, 1, 9)
        with patch('utils.date_utils.is_trading_day') as mock_is_trading:
            mock_is_trading.side_effect = lambda d: d.weekday() < 5 and d != date(2024, 1, 8)
            prev_day = get_previous_trading_day(tuesday)
            assert prev_day == date(2024, 1, 5)  # Skips Monday (holiday)

    def test_get_trading_days_range(self):
        """Test getting range of trading days"""
        start_date = date(2024, 1, 8)  # Monday
        end_date = date(2024, 1, 12)   # Friday

        trading_days = get_trading_days_range(start_date, end_date)

        # Should include all weekdays
        expected_days = [
            date(2024, 1, 8),   # Monday
            date(2024, 1, 9),   # Tuesday
            date(2024, 1, 10),  # Wednesday
            date(2024, 1, 11),  # Thursday
            date(2024, 1, 12),  # Friday
        ]

        assert trading_days == expected_days
        assert len(trading_days) == 5

        # Test range with weekends
        start_date = date(2024, 1, 6)   # Saturday
        end_date = date(2024, 1, 15)    # Monday

        trading_days = get_trading_days_range(start_date, end_date)
        assert len(trading_days) == 5  # Only weekdays
        assert date(2024, 1, 6) not in trading_days  # Saturday excluded
        assert date(2024, 1, 7) not in trading_days  # Sunday excluded

    def test_get_time_ago_string(self):
        """Test time ago string formatting"""
        now = datetime(2024, 1, 15, 12, 0, 0)

        # Test minutes ago
        time_5_min_ago = now - timedelta(minutes=5)
        assert get_time_ago_string(time_5_min_ago, reference_time=now) == "5分钟前"

        time_1_hour_ago = now - timedelta(hours=1)
        assert get_time_ago_string(time_1_hour_ago, reference_time=now) == "1小时前"

        # Test hours ago
        time_3_hours_ago = now - timedelta(hours=3)
        assert get_time_ago_string(time_3_hours_ago, reference_time=now) == "3小时前"

        # Test days ago
        time_1_day_ago = now - timedelta(days=1)
        assert get_time_ago_string(time_1_day_ago, reference_time=now) == "1天前"

        time_3_days_ago = now - timedelta(days=3)
        assert get_time_ago_string(time_3_days_ago, reference_time=now) == "3天前"

        # Test future time
        time_future = now + timedelta(hours=2)
        assert get_time_ago_string(time_future, reference_time=now) == "2小时后"

        # Test with string input
        time_str = "2024-01-15 10:00:00"
        assert "2小时前" in get_time_ago_string(time_str, reference_time=now)

    def test_get_datetime_string(self):
        """Test datetime string formatting"""
        dt = datetime(2024, 1, 15, 14, 30, 45)

        # Test default format
        result = get_datetime_string(dt)
        assert "2024-01-15" in result
        assert "14:30" in result

        # Test custom format
        result = get_datetime_string(dt, "%Y-%m-%d %H:%M:%S")
        assert result == "2024-01-15 14:30:45"

        # Test with date input
        d = date(2024, 1, 15)
        result = get_datetime_string(d)
        assert result == "2024-01-15 00:00:00"

    def test_convert_timezone(self):
        """Test timezone conversion"""
        # Test UTC to Asia/Shanghai
        utc_time = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        shanghai_time = convert_timezone(utc_time, "Asia/Shanghai")

        # Shanghai is UTC+8, so 12:00 UTC should be 20:00 Shanghai
        assert shanghai_time.hour == 20
        assert shanghai_time.day == 15

        # Test Asia/Shanghai to UTC
        shanghai_time = datetime(2024, 1, 15, 20, 0, 0)
        utc_time = convert_timezone(shanghai_time, "UTC", from_tz="Asia/Shanghai")

        assert utc_time.hour == 12
        assert utc_time.day == 15

        # Test with naive datetime (assumes UTC)
        naive_time = datetime(2024, 1, 15, 12, 0, 0)
        aware_time = convert_timezone(naive_time, "Asia/Shanghai")

        assert aware_time.hour == 20
        assert aware_time.tzinfo is not None

    def test_is_market_open(self):
        """Test market open status checking"""
        # Test market hours on weekday
        market_open_time = datetime(2024, 1, 15, 10, 30)  # 10:30 AM
        assert is_market_open(market_open_time, market="A股") is True

        # Test after hours
        after_hours = datetime(2024, 1, 15, 16, 30)  # 4:30 PM
        assert is_market_open(after_hours, market="A股") is False

        # Test before hours
        before_hours = datetime(2024, 1, 15, 8, 30)  # 8:30 AM
        assert is_market_open(before_hours, market="A股") is False

        # Test weekend
        weekend_time = datetime(2024, 1, 13, 10, 30)  # Saturday 10:30 AM
        assert is_market_open(weekend_time, market="A股") is False

        # Test different markets
        us_market_time = datetime(2024, 1, 15, 22, 30)  # 10:30 PM China time
        # This would be 9:30 AM US time, so US market should be open
        assert is_market_open(us_market_time, market="美股") is True

    def test_get_market_status(self):
        """Test getting detailed market status"""
        # Test open market
        open_time = datetime(2024, 1, 15, 10, 30)
        status = get_market_status(open_time, market="A股")

        assert status["is_open"] is True
        assert "开盘" in status["status"]
        assert status["next_open"] is None  # Already open

        # Test closed market
        closed_time = datetime(2024, 1, 15, 16, 30)
        status = get_market_status(closed_time, market="A股")

        assert status["is_open"] is False
        assert "收盘" in status["status"]
        assert status["next_open"] is not None  # Should have next open time

        # Test weekend
        weekend_time = datetime(2024, 1, 13, 10, 30)
        status = get_market_status(weekend_time, market="A股")

        assert status["is_open"] is False
        assert "周末" in status["status"] or "休市" in status["status"]

    def test_edge_cases(self):
        """Test edge cases and error handling"""
        # Test with None input
        with pytest.raises((ValueError, TypeError)):
            parse_date(None)

        # Test with empty string
        with pytest.raises(ValueError):
            parse_date("")

        # Test date format variations
        assert parse_date("2024-1-1") == date(2024, 1, 1)  # Single digit month/day
        assert parse_date("2024-01-01T00:00:00") == date(2024, 1, 1)  # ISO format

        # Test timezone edge cases
        naive_time = datetime(2024, 1, 1, 12, 0)
        with pytest.raises(Exception):
            convert_timezone(naive_time, "Invalid/Timezone")

    def test_performance_considerations(self):
        """Test performance of date utilities"""
        import time

        # Test performance of parsing many dates
        start_time = time.time()
        for i in range(1000):
            parse_date(f"2024-01-{(i % 28) + 1:02d}")
        parse_time = time.time() - start_time

        # Should be fast (less than 0.1 seconds for 1000 operations)
        assert parse_time < 0.1, f"Date parsing too slow: {parse_time:.3f}s"

        # Test performance of trading day calculations
        start_time = time.time()
        base_date = date(2024, 1, 1)
        for i in range(100):
            get_next_trading_day(base_date + timedelta(days=i))
        trading_day_time = time.time() - start_time

        # Should be fast (less than 0.05 seconds for 100 operations)
        assert trading_day_time < 0.05, f"Trading day calculation too slow: {trading_day_time:.3f}s"