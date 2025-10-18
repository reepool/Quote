"""
Date and time utilities for the quote system.
Provides functions for date manipulation, trading calendar, etc.
"""

from datetime import datetime, date, timedelta, timezone
import pandas as pd
from typing import List, Optional, Tuple, Union
import holidays

from utils import date_utils_logger


def ensure_date(dt: Union[date, datetime]) -> date:
    """确保日期为 date 类型"""
    if dt is None:
        return None
    return dt if isinstance(dt, date) else dt.date()


def normalize_date_range(start_date: Union[date, datetime], end_date: Union[date, datetime]) -> tuple[date, date]:
    """标准化日期范围，统一转换为 date 类型"""
    return ensure_date(start_date), ensure_date(end_date)


def get_shanghai_time() -> datetime:
    """获取上海时间 (UTC+8)"""
    return datetime.now(timezone(timedelta(hours=8)))


class DateUtils:
    """日期工具类"""

    @staticmethod
    def get_trading_day(exchange: str, target_date: date = None) -> date:
        """获取指定日期的交易日"""
        if target_date is None:
            target_date = date.today()

        # 中国交易日历
        if exchange.upper() in ['SSE', 'SZSE']:
            return DateUtils._get_cn_trading_day(target_date)
        # 香港交易日历
        elif exchange.upper() == 'HKEX':
            return DateUtils._get_hk_trading_day(target_date)
        # 美国交易日历
        elif exchange.upper() in ['NASDAQ', 'NYSE']:
            return DateUtils._get_us_trading_day(target_date)
        else:
            # 默认排除周末
            if target_date.weekday() < 5:  # 0-4是周一到周五
                return target_date
            else:
                # 返回前一个交易日
                return DateUtils.get_previous_trading_day(exchange, target_date)

    @staticmethod
    def _get_cn_trading_day(target_date: date) -> date:
        """获取中国交易日"""
        # 中国公共假期
        cn_holidays = holidays.CountryHoliday('CN', years=target_date.year)

        # 排除周末和公共假期
        if target_date.weekday() >= 5 or target_date in cn_holidays:
            return DateUtils.get_previous_trading_day('SSE', target_date)

        return target_date

    @staticmethod
    def _get_hk_trading_day(target_date: date) -> date:
        """获取香港交易日"""
        # 香港公共假期
        hk_holidays = holidays.CountryHoliday('HK', years=target_date.year)

        # 排除周末和香港假期
        if target_date.weekday() >= 5 or target_date in hk_holidays:
            return DateUtils.get_previous_trading_day('HKEX', target_date)

        return target_date

    @staticmethod
    def _get_us_trading_day(target_date: date) -> date:
        """获取美国交易日"""
        # 美国公共假期
        us_holidays = holidays.CountryHoliday('US', years=target_date.year)

        # 排除周末和美国假期
        if target_date.weekday() >= 5 or target_date in us_holidays:
            return DateUtils.get_previous_trading_day('NASDAQ', target_date)

        return target_date

    @staticmethod
    def get_previous_trading_day(exchange: str, target_date: date) -> date:
        """获取前一个交易日"""
        previous_day = target_date - timedelta(days=1)
        return DateUtils.get_trading_day(exchange, previous_day)

    @staticmethod
    def get_next_trading_day(exchange: str, target_date: date) -> date:
        """获取下一个交易日"""
        next_day = target_date + timedelta(days=1)
        max_attempts = 30  # 防止无限循环
        attempts = 0

        while attempts < max_attempts:
            trading_day = DateUtils.get_trading_day(exchange, next_day)
            if trading_day > target_date:
                return trading_day
            next_day += timedelta(days=1)
            attempts += 1

        return target_date  # 找不到则返回原日期

    @staticmethod
    def get_trading_days_in_range(exchange: str, start_date: date, end_date: date) -> List[date]:
        """获取指定日期范围内的所有交易日"""
        trading_days = []
        current_date = start_date

        while current_date <= end_date:
            trading_day = DateUtils.get_trading_day(exchange, current_date)
            if trading_day not in trading_days and trading_day >= start_date:
                trading_days.append(trading_day)
            current_date += timedelta(days=1)

        return sorted(trading_days)

    @staticmethod
    def is_trading_day(exchange: str, target_date: date) -> bool:
        """判断是否为交易日"""
        trading_day = DateUtils.get_trading_day(exchange, target_date)
        return trading_day == target_date

    @staticmethod
    def get_latest_trading_date(exchange: str) -> date:
        """获取最新的交易日"""
        today = date.today()
        return DateUtils.get_trading_day(exchange, today)

    @staticmethod
    def get_market_open_time(exchange: str) -> Tuple[datetime, datetime]:
        """获取市场开盘时间（返回当日开盘和收盘时间）"""
        now = datetime.now()

        if exchange.upper() in ['SSE', 'SZSE']:
            # A股: 9:30-11:30, 13:00-15:00
            morning_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
            morning_close = now.replace(hour=11, minute=30, second=0, microsecond=0)
            afternoon_open = now.replace(hour=13, minute=0, second=0, microsecond=0)
            afternoon_close = now.replace(hour=15, minute=0, second=0, microsecond=0)
            return morning_open, afternoon_close

        elif exchange.upper() == 'HKEX':
            # 港股: 9:30-12:00, 13:00-16:00
            morning_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
            morning_close = now.replace(hour=12, minute=0, second=0, microsecond=0)
            afternoon_open = now.replace(hour=13, minute=0, second=0, microsecond=0)
            afternoon_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
            return morning_open, afternoon_close

        elif exchange.upper() in ['NASDAQ', 'NYSE']:
            # 美股: 9:30-16:00 (美东时间)
            # 注意：这里返回的是美东时间，需要根据时区转换
            morning_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
            afternoon_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
            return morning_open, afternoon_close

        else:
            # 默认时间
            morning_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
            afternoon_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
            return morning_open, afternoon_close

    @staticmethod
    def is_market_open(exchange: str, db_ops=None) -> bool:
        """判断市场是否开盘"""
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from datetime import timezone
            ZoneInfo = timezone

        now = datetime.now()

        # 获取交易所时区
        if exchange.upper() in ['SSE', 'SZSE']:
            tz = ZoneInfo('Asia/Shanghai')
        elif exchange.upper() == 'HKEX':
            tz = ZoneInfo('Asia/Hong_Kong')
        elif exchange.upper() in ['NASDAQ', 'NYSE']:
            tz = ZoneInfo('America/New_York')
        else:
            tz = ZoneInfo('UTC')

        local_time = now.astimezone(tz)

        # 检查是否是交易日 - 优先使用交易日历表
        if db_ops:
            try:
                import asyncio
                # 如果在异步环境中，直接使用
                if asyncio.get_event_loop().is_running():
                    # 需要在异步环境中调用
                    if not asyncio.run(db_ops.is_trading_day(exchange, local_time.date())):
                        return False
                else:
                    if not db_ops.is_trading_day(exchange, local_time.date()):
                        return False
            except Exception as e:
                # fallback to DateUtils
                if not DateUtils.is_trading_day(exchange, local_time.date()):
                    return False
        else:
            # 使用DateUtils作为默认方法
            if not DateUtils.is_trading_day(exchange, local_time.date()):
                return False

        # 获取交易时间
        open_time, close_time = DateUtils.get_market_open_time(exchange)

        # 设置正确的时区
        open_time = open_time.astimezone(tz)
        close_time = close_time.astimezone(tz)

        # 检查是否在交易时间内
        if exchange.upper() in ['SSE', 'SZSE']:
            # A股有午休
            morning_open = local_time.replace(hour=9, minute=30, second=0, microsecond=0)
            morning_close = local_time.replace(hour=11, minute=30, second=0, microsecond=0)
            afternoon_open = local_time.replace(hour=13, minute=0, second=0, microsecond=0)
            afternoon_close = local_time.replace(hour=15, minute=0, second=0, microsecond=0)

            return (morning_open <= local_time <= morning_close or
                    afternoon_open <= local_time <= afternoon_close)
        else:
            # 其他市场没有午休
            return open_time <= local_time <= close_time

    @staticmethod
    def get_time_until_market_close(exchange: str, db_ops=None) -> Optional[timedelta]:
        """获取距离市场收盘还有多长时间"""
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from datetime import timezone
            ZoneInfo = timezone

        if not DateUtils.is_market_open(exchange, db_ops):
            return None

        now = datetime.now()

        # 获取交易所时区
        if exchange.upper() in ['SSE', 'SZSE']:
            tz = ZoneInfo('Asia/Shanghai')
        elif exchange.upper() == 'HKEX':
            tz = ZoneInfo('Asia/Hong_Kong')
        elif exchange.upper() in ['NASDAQ', 'NYSE']:
            tz = ZoneInfo('America/New_York')
        else:
            tz = ZoneInfo('UTC')

        local_time = now.astimezone(tz)

        # 获取收盘时间
        _, close_time = DateUtils.get_market_open_time(exchange)
        close_time = close_time.astimezone(tz)

        # 设置今天的收盘时间
        today_close = local_time.replace(
            hour=close_time.hour,
            minute=close_time.minute,
            second=close_time.second,
            microsecond=close_time.microsecond
        )

        return today_close - local_time

    @staticmethod
    def parse_date_string(date_str: str) -> Optional[date]:
        """解析日期字符串"""
        formats = [
            '%Y-%m-%d',
            '%Y/%m/%d',
            '%Y%m%d',
            '%d-%m-%Y',
            '%d/%m/%Y',
            '%m-%d-%Y',
            '%m/%d/%Y',
            '%Y年%m月%d日'
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        date_utils_logger.warning(f"Failed to parse date string: {date_str}")
        return None

    @staticmethod
    def format_date(date_obj: date, format_str: str = '%Y-%m-%d') -> str:
        """格式化日期"""
        return date_obj.strftime(format_str)

    @staticmethod
    def get_date_range(start_date: date, end_date: date) -> List[date]:
        """获取日期范围内的所有日期"""
        if start_date > end_date:
            return []

        delta = end_date - start_date
        return [start_date + timedelta(days=i) for i in range(delta.days + 1)]

    @staticmethod
    def get_last_n_trading_days(exchange: str, n: int) -> List[date]:
        """获取最近N个交易日"""
        trading_days = []
        current_date = date.today()

        while len(trading_days) < n:
            trading_day = DateUtils.get_trading_day(exchange, current_date)
            if trading_day not in trading_days:
                trading_days.append(trading_day)
            current_date -= timedelta(days=1)

        return sorted(trading_days)

    @staticmethod
    def get_month_trading_days(exchange: str, year: int, month: int) -> List[date]:
        """获取指定月份的所有交易日"""
        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = date(year, month + 1, 1) - timedelta(days=1)

        return DateUtils.get_trading_days_in_range(exchange, start_date, end_date)

    @staticmethod
    def get_year_trading_days(exchange: str, year: int) -> List[date]:
        """获取指定年份的所有交易日"""
        start_date = date(year, 1, 1)
        end_date = date(year, 12, 31)

        return DateUtils.get_trading_days_in_range(exchange, start_date, end_date)

    @staticmethod
    def format_next_run_time(next_run_time: Optional[datetime],
                           task_status: str = "running") -> str:
        """
        格式化任务下次执行时间

        Args:
            next_run_time: 下次执行时间（APScheduler返回的已带时区信息的时间）
            task_status: 任务状态 (running, paused, disabled, error)

        Returns:
            格式化的时间字符串
        """
        if next_run_time is None:
            return DateUtils._get_status_text(task_status)

        try:
            # 使用现有的get_shanghai_time获取当前时间
            current_time = get_shanghai_time()

            # APScheduler已经返回了正确的时区时间（Asia/Hong_Kong，即UTC+8）
            # 不需要时区转换，直接使用
            target_time = next_run_time

            # 计算时间差
            time_diff = target_time - current_time

            # 根据时间差选择合适的显示格式
            if time_diff.total_seconds() <= 0:
                return "已过期"
            elif time_diff.total_seconds() <= 3600:  # 1小时内
                minutes = int(time_diff.total_seconds() / 60)
                return f"{minutes}分钟后"
            elif time_diff.total_seconds() <= 86400:  # 24小时内
                return target_time.strftime("今天 %H:%M")
            elif time_diff.total_seconds() <= 604800:  # 7天内
                weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
                weekday = weekday_names[target_time.weekday()]
                return target_time.strftime(f"{weekday} %H:%M")
            else:
                # 超过7天显示完整日期
                return target_time.strftime("%m-%d %H:%M")

        except Exception as e:
            date_utils_logger.error(f"[DateUtils] 格式化时间失败: {e}")
            return "时间格式化失败"

    @staticmethod
    def format_relative_time(target_time: datetime, base_time: datetime = None) -> str:
        """
        格式化相对时间描述

        Args:
            target_time: 目标时间
            base_time: 基准时间，默认为当前时间

        Returns:
            相对时间描述字符串
        """
        if base_time is None:
            base_time = get_shanghai_time()

        try:
            # APScheduler已经返回了正确的时区时间（Asia/Hong_Kong，即UTC+8）
            # 确保两个时间都在同一时区，如果target_time没有时区信息则使用当前时间时区
            if target_time.tzinfo is None:
                target_time = target_time.replace(tzinfo=base_time.tzinfo)

            # 计算时间差
            time_diff = target_time - base_time

            if time_diff.total_seconds() < 0:
                return "已过期"
            elif time_diff.total_seconds() < 60:
                seconds = int(time_diff.total_seconds())
                return f"{seconds}秒后"
            elif time_diff.total_seconds() < 3600:
                minutes = int(time_diff.total_seconds() / 60)
                return f"{minutes}分钟后"
            elif time_diff.total_seconds() < 86400:
                hours = int(time_diff.total_seconds() / 3600)
                return f"{hours}小时后"
            elif time_diff.total_seconds() < 604800:
                days = int(time_diff.total_seconds() / 86400)
                return f"{days}天后"
            else:
                # 超过一周显示具体日期
                return target_time.strftime("%Y-%m-%d %H:%M")

        except Exception as e:
            date_utils_logger.error(f"[DateUtils] 格式化相对时间失败: {e}")
            return "时间格式化失败"

    @staticmethod
    def get_task_status_display(next_run_time: Optional[datetime],
                               task_status: str = "running",
                               task_description: str = "") -> str:
        """
        获取任务状态显示文本

        Args:
            next_run_time: 下次执行时间
            task_status: 任务状态 (running, paused, disabled, error)
            task_description: 任务描述

        Returns:
            任务状态显示文本
        """
        if task_status == "disabled":
            return "已禁用"
        elif task_status == "paused":
            return "已暂停"
        elif task_status == "error":
            return "状态异常"
        elif next_run_time is None:
            return "未安排"
        else:
            return DateUtils.format_next_run_time(next_run_time, task_status)

    @staticmethod
    def _get_status_text(task_status: str) -> str:
        """根据任务状态获取状态文本"""
        status_map = {
            "disabled": "已禁用",
            "paused": "已暂停",
            "error": "状态异常",
            "running": "未安排"
        }
        return status_map.get(task_status, "未知状态")