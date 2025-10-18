"""
安全工具模块
提供输入验证、敏感信息处理和安全检查功能
"""

import re
import hashlib
import secrets
from typing import Dict, Any, Optional, List
from utils.exceptions import ValidationError, ErrorCodes


class SecurityValidator:
    """安全验证器"""

    # 危险字符模式
    DANGEROUS_PATTERNS = [
        r'<script[^>]*>.*?</script>',  # XSS
        r'javascript:',  # JavaScript协议
        r'on\w+\s*=',  # 事件处理器
        r'union\s+select',  # SQL注入
        r'drop\s+table',  # SQL删除
        r'insert\s+into',  # SQL插入
        r'update\s+set',  # SQL更新
        r'delete\s+from',  # SQL删除
    ]

    # 敏感信息字段
    SENSITIVE_FIELDS = {
        'password', 'passwd', 'pwd',
        'token', 'secret', 'key',
        'api_key', 'api_secret', 'access_key',
        'auth', 'authorization', 'bearer',
        'private_key', 'public_key',
        'hash', 'salt', 'nonce'
    }

    @staticmethod
    def sanitize_input(text: str, max_length: int = 1000) -> str:
        """清理输入文本"""
        if not text:
            return ""

        # 转换为字符串
        text = str(text)

        # 限制长度
        if len(text) > max_length:
            raise ValidationError(
                f"Input too long: {len(text)} > {max_length}",
                ErrorCodes.VALIDATION_INVALID_FORMAT
            )

        # 移除控制字符
        text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)

        # 检查危险模式
        for pattern in SecurityValidator.DANGEROUS_PATTERNS:
            if re.search(pattern, text, re.IGNORECASE):
                raise ValidationError(
                    f"Potentially dangerous input detected: {pattern}",
                    ErrorCodes.VALIDATION_INVALID_FORMAT
                )

        # 规范化空白字符
        text = ' '.join(text.split())

        return text.strip()

    @staticmethod
    def validate_sql_params(params: Dict[str, Any]) -> Dict[str, Any]:
        """验证SQL参数"""
        validated = {}

        for key, value in params.items():
            # 检查字段名是否安全
            if not SecurityValidator._is_safe_identifier(key):
                raise ValidationError(
                    f"Unsafe field name: {key}",
                    ErrorCodes.VALIDATION_INVALID_FORMAT
                )

            # 清理值
            if isinstance(value, str):
                validated[key] = SecurityValidator.sanitize_input(value, 1000)
            elif isinstance(value, (int, float, bool)):
                validated[key] = value
            elif value is None:
                validated[key] = None
            else:
                raise ValidationError(
                    f"Unsupported parameter type for {key}: {type(value)}",
                    ErrorCodes.VALIDATION_INVALID_FORMAT
                )

        return validated

    @staticmethod
    def _is_safe_identifier(name: str) -> bool:
        """检查标识符是否安全"""
        return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name))

    @staticmethod
    def mask_sensitive_data(data: Dict[str, Any], mask_char: str = 'x') -> Dict[str, Any]:
        """遮蔽敏感数据"""
        if not isinstance(data, dict):
            return data

        masked = {}
        for key, value in data.items():
            key_lower = key.lower()
            if any(sensitive in key_lower for sensitive in SecurityValidator.SENSITIVE_FIELDS):
                if isinstance(value, str) and len(value) > 0:
                    # 保留前2位和后2位，中间用遮蔽字符替换
                    if len(value) <= 4:
                        masked[key] = mask_char * len(value)
                    else:
                        masked[key] = value[:2] + mask_char * (len(value) - 4) + value[-2:]
                else:
                    masked[key] = mask_char * 8
            else:
                masked[key] = value

        return masked

    @staticmethod
    def generate_secure_token(length: int = 32) -> str:
        """生成安全令牌"""
        return secrets.token_urlsafe(length)

    @staticmethod
    def hash_password(password: str, salt: str = None) -> tuple[str, str]:
        """哈希密码"""
        if salt is None:
            salt = secrets.token_hex(16)

        # 使用SHA-256哈希
        hash_obj = hashlib.sha256()
        hash_obj.update((password + salt).encode('utf-8'))
        password_hash = hash_obj.hexdigest()

        return password_hash, salt

    @staticmethod
    def verify_password(password: str, password_hash: str, salt: str) -> bool:
        """验证密码"""
        expected_hash, _ = SecurityValidator.hash_password(password, salt)
        return secrets.compare_digest(expected_hash, password_hash)

    @staticmethod
    def validate_file_path(file_path: str, allowed_dirs: List[str] = None) -> str:
        """验证文件路径安全性"""
        if not file_path:
            raise ValidationError(
                "Empty file path",
                ErrorCodes.VALIDATION_INVALID_FORMAT
            )

        # 规范化路径
        import os
        file_path = os.path.normpath(file_path)

        # 检查路径遍历攻击
        if '..' in file_path or file_path.startswith('/'):
            raise ValidationError(
                f"Unsafe file path: {file_path}",
                ErrorCodes.VALIDATION_INVALID_FORMAT
            )

        # 检查允许的目录
        if allowed_dirs:
            if not any(file_path.startswith(allowed_dir) for allowed_dir in allowed_dirs):
                raise ValidationError(
                    f"File path not in allowed directories: {file_path}",
                    ErrorCodes.VALIDATION_INVALID_FORMAT
                )

        return file_path


class InputValidator:
    """输入验证器"""

    @staticmethod
    def validate_email(email: str) -> bool:
        """验证邮箱格式"""
        if not email:
            return False

        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))

    @staticmethod
    def validate_phone_number(phone: str) -> bool:
        """验证电话号码格式"""
        if not phone:
            return False

        # 移除所有非数字字符
        digits_only = re.sub(r'[^\d]', '', phone)
        return 8 <= len(digits_only) <= 15

    @staticmethod
    def validate_url(url: str) -> bool:
        """验证URL格式"""
        if not url:
            return False

        url_pattern = r'^https?://(?:[-\w.])+(?:[:\d]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:#(?:\w*))?)?$'
        return bool(re.match(url_pattern, url))

    @staticmethod
    def validate_ip_address(ip: str) -> bool:
        """验证IP地址格式"""
        if not ip:
            return False

        # IPv4
        ipv4_pattern = r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
        if re.match(ipv4_pattern, ip):
            return True

        # IPv6 (简化版)
        ipv6_pattern = r'^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$'
        return bool(re.match(ipv6_pattern, ip))

    @staticmethod
    def validate_json_structure(data: Any, required_fields: List[str] = None) -> bool:
        """验证JSON结构"""
        if not isinstance(data, dict):
            return False

        if required_fields:
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                return False

        return True


class SecurityHeaders:
    """安全HTTP头"""

    @staticmethod
    def get_security_headers() -> Dict[str, str]:
        """获取安全HTTP头"""
        return {
            'X-Content-Type-Options': 'nosniff',
            'X-Frame-Options': 'DENY',
            'X-XSS-Protection': '1; mode=block',
            'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
            'Content-Security-Policy': "default-src 'self'",
            'Referrer-Policy': 'strict-origin-when-cross-origin'
        }


class RateLimiter:
    """简单的内存限流器"""

    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = {}

    def is_allowed(self, identifier: str) -> bool:
        """检查是否允许请求"""
        import time

        current_time = time.time()
        window_start = current_time - self.window_seconds

        # 清理过期记录
        if identifier in self.requests:
            self.requests[identifier] = [
                req_time for req_time in self.requests[identifier]
                if req_time > window_start
            ]
        else:
            self.requests[identifier] = []

        # 检查是否超过限制
        if len(self.requests[identifier]) >= self.max_requests:
            return False

        # 记录当前请求
        self.requests[identifier].append(current_time)
        return True

    def get_remaining_requests(self, identifier: str) -> int:
        """获取剩余请求次数"""
        if identifier not in self.requests:
            return self.max_requests

        return max(0, self.max_requests - len(self.requests[identifier]))

    @staticmethod
    def load_secure_config() -> Dict[str, Any]:
        """从环境变量加载安全配置"""
        import os

        secure_config = {}

        # Telegram配置
        telegram_config = {
            'enabled': os.getenv('TELEGRAM_ENABLED', 'false').lower() == 'true',
            'api_id': os.getenv('TELEGRAM_API_ID', ''),
            'api_hash': os.getenv('TELEGRAM_API_HASH', ''),
            'bot_token': os.getenv('TELEGRAM_BOT_TOKEN', ''),
            'chat_id': os.getenv('TELEGRAM_CHAT_ID', '').split(',') if os.getenv('TELEGRAM_CHAT_ID') else [],
            'session_name': os.getenv('TELEGRAM_SESSION_NAME', 'MsgBot')
        }
        secure_config['telegram_config'] = telegram_config

        # Tushare配置
        tushare_config = {
            'enabled': os.getenv('TUSHARE_ENABLED', 'false').lower() == 'true',
            'token': os.getenv('TUSHARE_TOKEN', '')
        }
        secure_config['tushare_config'] = tushare_config

        # API配置
        api_config = {
            'host': os.getenv('API_HOST', '0.0.0.0'),
            'port': int(os.getenv('API_PORT', '8000')),
            'workers': int(os.getenv('API_WORKERS', '1'))
        }
        secure_config['api_config'] = api_config

        # 数据库配置
        database_config = {
            'db_path': os.getenv('DATABASE_PATH', 'data/quotes.db')
        }
        secure_config['database_config'] = database_config

        return secure_config

    @staticmethod
    def redact_sensitive_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """编辑配置中的敏感信息以便记录"""
        import copy

        config_copy = copy.deepcopy(config)

        # 编辑敏感字段
        sensitive_patterns = ['token', 'api_key', 'secret', 'hash', 'password']

        def redact_recursive(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    key_lower = key.lower()
                    if any(pattern in key_lower for pattern in sensitive_patterns):
                        if isinstance(value, str) and value:
                            # 保留前2位和后1位
                            if len(value) <= 3:
                                obj[key] = '*' * len(value)
                            else:
                                obj[key] = value[:2] + '*' * (len(value) - 3) + value[-1]
                    else:
                        redact_recursive(value)
            elif isinstance(obj, list):
                for item in obj:
                    redact_recursive(item)

        redact_recursive(config_copy)
        return config_copy


# 全局限流器实例
api_rate_limiter = RateLimiter(max_requests=100, window_seconds=60)
data_download_limiter = RateLimiter(max_requests=10, window_seconds=60)