"""
Data validation utilities for the quote system.
Provides functions to validate and sanitize input data.
"""

import re
from datetime import datetime, date, timezone
from typing import List, Dict, Any, Optional, Union, Tuple
from pydantic import ValidationError
from utils import validation_logger


class DataValidator:
    """数据验证器"""

    # 交易所代码映射
    EXCHANGE_MAPPING = {
        'SSE': ['上海', '上交所', 'SH', 'SHSE', '上证', '沪市'],
        'SZSE': ['深圳', '深交所', 'SZ', 'SZSE', '深证', '深市'],
        'HKEX': ['香港', '港交所', '联交所', 'HK', 'HKEX', '港股'],
        'NASDAQ': ['纳斯达克', 'NASDAQ', '纳斯达克交易所'],
        'NYSE': ['纽约', '纽交所', 'NYSE', '纽约证交所']
    }

    # 交易品种类型
    INSTRUMENT_TYPES = ['STOCK', 'BOND', 'ETF', 'INDEX', 'FUND', 'REIT', 'WARRANT', 'FUTURES']

    @staticmethod
    def validate_exchange(exchange: str) -> Optional[str]:
        """验证并标准化交易所代码"""
        if not exchange:
            return None

        exchange_upper = exchange.upper().strip()

        # 直接匹配
        if exchange_upper in DataValidator.EXCHANGE_MAPPING:
            return exchange_upper

        # 模糊匹配
        for standard_code, aliases in DataValidator.EXCHANGE_MAPPING.items():
            if exchange_upper in aliases or exchange in aliases:
                return standard_code

        validation_logger.warning(f"Unknown exchange: {exchange}")
        return None

    @staticmethod
    def validate_instrument_type(instrument_type: str) -> Optional[str]:
        """验证交易品种类型"""
        if not instrument_type:
            return None

        type_upper = instrument_type.upper().strip()

        if type_upper in DataValidator.INSTRUMENT_TYPES:
            return type_upper

        validation_logger.warning(f"Unknown instrument type: {instrument_type}")
        return None

    @staticmethod
    def validate_symbol(symbol: str, exchange: str = None) -> Optional[str]:
        """验证交易代码格式"""
        if not symbol:
            return None

        symbol = symbol.strip()

        # 根据交易所验证格式
        if exchange:
            exchange_validated = DataValidator.validate_exchange(exchange)
            if exchange_validated == 'SSE':
                # 上交所: 6位数字或以6,9开头的6位数字
                if re.match(r'^\d{6}$', symbol):
                    return symbol.zfill(6) if len(symbol) < 6 else symbol
            elif exchange_validated == 'SZSE':
                # 深交所: 6位数字或以00,30开头的6位数字
                if re.match(r'^\d{6}$', symbol):
                    return symbol.zfill(6) if len(symbol) < 6 else symbol
            elif exchange_validated == 'HKEX':
                # 港交所: 通常是5位数字
                if re.match(r'^\d{5}$', symbol):
                    return symbol
            elif exchange_validated in ['NASDAQ', 'NYSE']:
                # 美股: 1-5位字母，可能包含点号
                if re.match(r'^[A-Z]{1,5}(\.[A-Z]{1,2})?$', symbol.upper()):
                    return symbol.upper()

        # 通用验证：字母数字组合
        if re.match(r'^[A-Z0-9]{1,10}$', symbol.upper()):
            return symbol.upper()

        validation_logger.warning(f"Invalid symbol format: {symbol} for exchange: {exchange}")
        return None

    @staticmethod
    def validate_date(date_input: Union[str, date, datetime]) -> Optional[date]:
        """验证日期格式"""
        if not date_input:
            return None

        if isinstance(date_input, date):
            return date_input

        if isinstance(date_input, datetime):
            return date_input.date()

        if isinstance(date_input, str):
            # 尝试解析各种日期格式
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
                    return datetime.strptime(date_input, fmt).date()
                except ValueError:
                    continue

        validation_logger.warning(f"Invalid date format: {date_input}")
        return None

    @staticmethod
    def validate_date_range(start_date: Union[str, date, datetime],
                          end_date: Union[str, date, datetime]) -> Tuple[Optional[date], Optional[date]]:
        """验证日期范围"""
        start_valid = DataValidator.validate_date(start_date)
        end_valid = DataValidator.validate_date(end_date)

        if not start_valid or not end_valid:
            return None, None

        if start_valid > end_valid:
            validation_logger.warning(f"Start date {start_valid} is after end date {end_valid}")
            return end_valid, start_valid  # 交换日期

        return start_valid, end_valid

    @staticmethod
    def validate_price(price: Union[str, float, int]) -> Optional[float]:
        """验证价格数据"""
        if price is None:
            return None

        try:
            price_float = float(price)
            if price_float >= 0:
                return price_float
        except (ValueError, TypeError):
            pass

        validation_logger.warning(f"Invalid price: {price}")
        return None

    @staticmethod
    def validate_volume(volume: Union[str, float, int]) -> Optional[int]:
        """验证成交量数据"""
        if volume is None:
            return None

        try:
            volume_int = int(float(volume))
            if volume_int >= 0:
                return volume_int
        except (ValueError, TypeError):
            pass

        validation_logger.warning(f"Invalid volume: {volume}")
        return None

    @staticmethod
    def validate_amount(amount: Union[str, float, int]) -> Optional[float]:
        """验证成交额数据"""
        if amount is None:
            return None

        try:
            amount_float = float(amount)
            if amount_float >= 0:
                return amount_float
        except (ValueError, TypeError):
            pass

        validation_logger.warning(f"Invalid amount: {amount}")
        return None

    @staticmethod
    def validate_instrument_id(instrument_id: str) -> Optional[str]:
        """验证交易品种ID格式"""
        if not instrument_id:
            return None

        # 格式: symbol.exchange (e.g., 300708.SZSE)
        if re.match(r'^[A-Z0-9]{1,10}\.[A-Z]{2,6}$', instrument_id.upper()):
            return instrument_id.upper()

        validation_logger.warning(f"Invalid instrument ID format: {instrument_id}")
        return None

    @staticmethod
    def validate_quote_data(quote_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """验证行情数据完整性"""
        if not isinstance(quote_data, dict):
            validation_logger.error("Quote data must be a dictionary")
            return None

        validated_data = {}

        # 必需字段
        required_fields = ['time', 'instrument_id', 'open', 'high', 'low', 'close']
        for field in required_fields:
            if field not in quote_data:
                validation_logger.error(f"Missing required field: {field}")
                return None

        # 验证各个字段
        validated_data['time'] = DataValidator.validate_date(quote_data['time'])
        validated_data['instrument_id'] = DataValidator.validate_instrument_id(quote_data['instrument_id'])
        validated_data['open'] = DataValidator.validate_price(quote_data['open'])
        validated_data['high'] = DataValidator.validate_price(quote_data['high'])
        validated_data['low'] = DataValidator.validate_price(quote_data['low'])
        validated_data['close'] = DataValidator.validate_price(quote_data['close'])

        # 检查价格逻辑
        if (validated_data['high'] < max(validated_data['open'], validated_data['close']) or
            validated_data['low'] > min(validated_data['open'], validated_data['close'])):
            validation_logger.error(f"Invalid price relationship: {quote_data}")
            return None

        # 可选字段
        optional_fields = ['volume', 'amount', 'factor']
        for field in optional_fields:
            if field in quote_data:
                if field == 'volume':
                    validated_data[field] = DataValidator.validate_volume(quote_data[field])
                elif field == 'amount':
                    validated_data[field] = DataValidator.validate_amount(quote_data[field])
                elif field == 'factor':
                    validated_data[field] = DataValidator.validate_price(quote_data[field]) or 1.0

        # 添加时间戳（上海时区）
        from datetime import timedelta
        shanghai_tz = timezone(timedelta(hours=8))
        validated_data['created_at'] = datetime.now(shanghai_tz)

        return validated_data

    @staticmethod
    def validate_instrument_data(instrument_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """验证交易品种数据"""
        if not isinstance(instrument_data, dict):
            validation_logger.error("Instrument data must be a dictionary")
            return None

        validated_data = {}

        # 必需字段
        required_fields = ['instrument_id', 'symbol', 'name', 'exchange', 'type', 'currency']
        for field in required_fields:
            if field not in instrument_data:
                validation_logger.error(f"Missing required field: {field}")
                return None

        # 验证各个字段
        validated_data['instrument_id'] = DataValidator.validate_instrument_id(instrument_data['instrument_id'])
        validated_data['symbol'] = DataValidator.validate_symbol(instrument_data['symbol'])
        validated_data['name'] = str(instrument_data['name']).strip()
        validated_data['exchange'] = DataValidator.validate_exchange(instrument_data['exchange'])
        validated_data['type'] = DataValidator.validate_instrument_type(instrument_data['type'])
        validated_data['currency'] = str(instrument_data['currency']).strip().upper()

        # 检查必要字段是否验证通过
        if None in [validated_data[field] for field in ['instrument_id', 'symbol', 'exchange', 'type']]:
            validation_logger.error(f"Invalid instrument data: {instrument_data}")
            return None

        # 可选字段
        if 'is_active' in instrument_data:
            validated_data['is_active'] = bool(instrument_data['is_active'])

        # 添加时间戳（上海时区）
        from datetime import timedelta
        shanghai_tz = timezone(timedelta(hours=8))
        current_time = datetime.now(shanghai_tz)
        validated_data['created_at'] = current_time
        validated_data['updated_at'] = current_time

        return validated_data

    @staticmethod
    def sanitize_string(text: str, max_length: int = 255) -> str:
        """清理字符串数据"""
        if not text:
            return ""

        # 移除特殊字符，保留基本字符
        sanitized = re.sub(r'[^\w\s\-_.,;:!?()\[\]{}]', '', str(text))
        # 截断到指定长度
        sanitized = sanitized[:max_length].strip()
        return sanitized

    @staticmethod
    def validate_api_key(api_key: str) -> bool:
        """验证API密钥格式"""
        if not api_key or len(api_key) < 8:
            return False

        # 基本格式检查：字母数字组合
        return re.match(r'^[A-Za-z0-9\-_]{8,}$', api_key) is not None

    @staticmethod
    def validate_email(email: str) -> bool:
        """验证邮箱格式"""
        if not email:
            return False

        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return re.match(pattern, email) is not None

    @staticmethod
    def validate_phone_number(phone: str) -> bool:
        """验证电话号码格式"""
        if not phone:
            return False

        # 移除所有非数字字符
        digits_only = re.sub(r'[^\d]', '', phone)

        # 检查长度（支持国际号码）
        return 8 <= len(digits_only) <= 15

    @staticmethod
    def validate_batch_size(batch_size: int, max_batch_size: int = 1000) -> int:
        """验证批次大小"""
        try:
            batch_size_int = int(batch_size)
            if batch_size_int <= 0:
                return 1  # 最小批次大小
            elif batch_size_int > max_batch_size:
                return max_batch_size  # 最大批次大小
            else:
                return batch_size_int
        except (ValueError, TypeError):
            return 1  # 默认批次大小


class QueryValidator:
    """查询参数验证器"""

    @staticmethod
    def validate_quote_query(params: Dict[str, Any]) -> Dict[str, Any]:
        """验证行情查询参数"""
        validated = {}

        # 验证标识符
        if 'instrument_id' in params:
            validated['instrument_id'] = DataValidator.validate_instrument_id(params['instrument_id'])
        elif 'symbol' in params:
            validated['symbol'] = DataValidator.validate_symbol(params['symbol'])

        # 验证日期范围
        if 'start_date' in params and 'end_date' in params:
            start_date, end_date = DataValidator.validate_date_range(
                params['start_date'], params['end_date']
            )
            validated['start_date'] = start_date
            validated['end_date'] = end_date

        # 验证返回格式
        if 'return_format' in params:
            format_str = params['return_format'].lower()
            if format_str in ['pandas', 'json', 'csv']:
                validated['return_format'] = format_str
            else:
                validated['return_format'] = 'pandas'  # 默认格式

        return validated

    @staticmethod
    def validate_instrument_query(params: Dict[str, Any]) -> Dict[str, Any]:
        """验证交易品种查询参数"""
        validated = {}

        # 验证交易所
        if 'exchange' in params:
            validated['exchange'] = DataValidator.validate_exchange(params['exchange'])

        # 验证品种类型
        if 'type' in params:
            validated['type'] = DataValidator.validate_instrument_type(params['type'])

        # 验证活跃状态
        if 'is_active' in params:
            validated['is_active'] = bool(params['is_active'])

        return validated