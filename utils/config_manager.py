"""
统一的配置管理模块
整合底层配置操作和应用层类型安全访问
"""

import json
import logging
from typing import Any, Optional, Dict, List
from dataclasses import dataclass, field
from pathlib import Path


# 定义自定义异常（避免循环导入）
class ConfigurationError(Exception):
    """配置错误异常"""
    pass

class ErrorCodes:
    """错误代码常量"""
    CONFIG_FILE_NOT_FOUND = "CONFIG_FILE_NOT_FOUND"
    CONFIG_INVALID_FORMAT = "CONFIG_INVALID_FORMAT"
    CONFIG_LOAD_ERROR = "CONFIG_LOAD_ERROR"
    CONFIG_SAVE_ERROR = "CONFIG_SAVE_ERROR"

# 获取配置专用日志器
config_logger = logging.getLogger("Config")

# ============================================================================
# 配置数据类型定义
# ============================================================================

@dataclass
class LoggingModuleConfig:
    """模块日志配置"""
    level: str = "INFO"
    enabled: bool = True

@dataclass
class FileLoggingConfig:
    """文件日志配置"""
    enabled: bool = True
    directory: str = "log"
    filename: str = "sys.log"
    rotation: Optional[Dict[str, Any]] = None

@dataclass
class ConsoleLoggingConfig:
    """控制台日志配置"""
    enabled: bool = True
    colored: bool = True

@dataclass
class PerformanceConfig:
    """性能监控配置"""
    enabled: bool = True
    slow_operation_threshold: float = 1.0

@dataclass
class LoggingConfig:
    """完整日志配置"""
    level: str = "INFO"
    format: str = "[%(levelname)s][%(asctime)s][%(filename)s:%(lineno)d] - %(message)s"
    date_format: str = "%Y-%m-%d %H:%M:%S"
    file_config: FileLoggingConfig = field(default_factory=FileLoggingConfig)
    console_config: ConsoleLoggingConfig = field(default_factory=ConsoleLoggingConfig)
    modules: Dict[str, LoggingModuleConfig] = field(default_factory=dict)
    performance_monitoring: PerformanceConfig = field(default_factory=PerformanceConfig)

@dataclass
class DatabaseConfig:
    """数据库配置"""
    db_path: str = "data/quotes.db"
    backup_enabled: bool = True
    backup_interval_days: int = 7

@dataclass
class ExchangeRules:
    """交易所规则配置"""
    exchange_mapping: Dict[str, str] = field(default_factory=dict)
    symbol_rules: Dict[str, Any] = field(default_factory=dict)
    symbol_start_with: Dict[str, List[str]] = field(default_factory=dict)

@dataclass
class DataSourceConfig:
    """数据源配置"""
    enabled: bool = True
    exchanges_supported: List[str] = field(default_factory=list)
    primary_source_of: List[str] = field(default_factory=list)
    max_requests_per_minute: int = 30
    max_requests_per_hour: int = 500
    max_requests_per_day: int = 5000
    retry_times: int = 3
    retry_interval: float = 2.0

@dataclass
class TelegramConfig:
    """Telegram配置"""
    enabled: bool = False
    api_id: str = ""
    api_hash: str = ""
    bot_token: str = ""
    chat_ids: List[str] = field(default_factory=list)
    session_name: str = "quote_bot"

@dataclass
class DataConfig:
    """数据配置"""
    data_dir: str = "data"
    market_presets: Dict[str, List[str]] = field(default_factory=dict)
    batch_size: int = 50

@dataclass
class SchedulerConfig:
    """调度器配置"""
    enabled: bool = True
    timezone: str = "Asia/Shanghai"
    jobs: Dict[str, Dict[str, Any]] = field(default_factory=dict)

@dataclass
class ApiConfig:
    """API配置"""
    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 8000
    workers: int = 1
    reload: bool = False
    cors_origins: List[str] = field(default_factory=lambda: ["*"])


# ============================================================================
# 统一配置管理器
# ============================================================================

class UnifiedConfigManager:
    """统一配置管理器 - 整合底层操作和应用层抽象"""

    def __init__(self, config_dir: str = "config"):
        self._config_dir = Path(config_dir)
        self._config_data: Dict[str, Any] = {}
        self._config_path = self._config_dir / "config.json"

        # 类型化配置缓存
        self._typed_cache: Dict[str, Any] = {}

        # 初始化配置
        self._load_config()

    def _load_config(self) -> None:
        """加载配置文件"""
        try:
            config_logger.info("Loading configuration file...")

            if not self._config_path.exists():
                # 尝试使用模板文件
                template_path = self._config_dir / "config-template.json"
                if template_path.exists():
                    self._config_path = template_path
                    config_logger.warning(f"Using template config: {template_path}")
                else:
                    raise ConfigurationError(
                        f"Configuration file not found: {self._config_path}",
                        ErrorCodes.CONFIG_FILE_NOT_FOUND
                    )

            with open(self._config_path, 'r', encoding='utf-8') as f:
                self._config_data = json.load(f)

            config_logger.info(f"Configuration loaded from: {self._config_path}")
            # 清除类型化缓存
            self._typed_cache.clear()

        except json.JSONDecodeError as e:
            raise ConfigurationError(
                f"Invalid JSON in configuration file: {e}",
                ErrorCodes.CONFIG_INVALID_FORMAT
            ) from e
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load configuration: {e}",
                ErrorCodes.CONFIG_LOAD_ERROR
            ) from e

    def reload_config(self) -> None:
        """重新加载配置"""
        config_logger.info("Reloading configuration...")
        self._load_config()

    # ========================================================================
    # 底层访问方法
    # ========================================================================

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        return self._config_data.get(key, default)

    def get_nested(self, path: str, default: Any = None) -> Any:
        """获取嵌套配置值，支持点分隔路径"""
        keys = path.split('.')
        current = self._config_data

        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default

        return current

    def set(self, key: str, value: Any) -> None:
        """设置配置值"""
        self._config_data[key] = value
        # 清除相关缓存
        if key in self._typed_cache:
            del self._typed_cache[key]

    def set_nested(self, path: str, value: Any) -> None:
        """设置嵌套配置值"""
        keys = path.split('.')
        current = self._config_data

        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        current[keys[-1]] = value

    def __contains__(self, key: str) -> bool:
        """支持 'in' 操作符"""
        return key in self._config_data

    def __getitem__(self, key: str) -> Any:
        """支持字典式访问"""
        return self._config_data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """支持字典式设置"""
        self.set(key, value)

    # ========================================================================
    # 类型安全访问方法
    # ========================================================================

    def get_logging_config(self) -> LoggingConfig:
        """获取日志配置（类型安全）"""
        if 'logging_config' not in self._typed_cache:
            try:
                logging_data = self.get_nested('logging_config', {})

                # 解析文件日志配置
                file_data = logging_data.get('file_config', {})
                file_config = FileLoggingConfig(
                    enabled=file_data.get('enabled', True),
                    directory=file_data.get('directory', 'log'),
                    filename=file_data.get('filename', 'sys.log'),
                    rotation=file_data.get('rotation')
                )

                # 解析控制台日志配置
                console_data = logging_data.get('console_config', {})
                console_config = ConsoleLoggingConfig(
                    enabled=console_data.get('enabled', True),
                    colored=console_data.get('colored', True)
                )

                # 解析性能监控配置
                perf_data = logging_data.get('performance_monitoring', {})
                perf_config = PerformanceConfig(
                    enabled=perf_data.get('enabled', True),
                    slow_operation_threshold=perf_data.get('slow_operation_threshold', 1.0)
                )

                # 解析模块配置
                modules_data = logging_data.get('modules', {})
                modules = {}
                for module_name, module_data in modules_data.items():
                    modules[module_name] = LoggingModuleConfig(
                        level=module_data.get('level', 'INFO'),
                        enabled=module_data.get('enabled', True)
                    )

                self._typed_cache['logging_config'] = LoggingConfig(
                    level=logging_data.get('level', 'INFO'),
                    file_config=file_config,
                    console_config=console_config,
                    modules=modules,
                    performance_monitoring=perf_config
                )
            except Exception as e:
                config_logger.error(f"Failed to parse logging config: {e}")
                self._typed_cache['logging_config'] = LoggingConfig()

        return self._typed_cache['logging_config']

    def get_database_config(self) -> DatabaseConfig:
        """获取数据库配置（类型安全）"""
        if 'database_config' not in self._typed_cache:
            try:
                db_data = self.get_nested('database_config', {})
                self._typed_cache['database_config'] = DatabaseConfig(
                    db_path=db_data.get('db_path', 'data/quotes.db'),
                    backup_enabled=db_data.get('backup_enabled', True),
                    backup_interval_days=db_data.get('backup_interval_days', 7)
                )
            except Exception as e:
                config_logger.error(f"Failed to parse database config: {e}")
                self._typed_cache['database_config'] = DatabaseConfig()

        return self._typed_cache['database_config']

    def get_telegram_config(self) -> TelegramConfig:
        """获取Telegram配置（类型安全）"""
        if 'telegram_config' not in self._typed_cache:
            try:
                tg_data = self.get_nested('telegram_config', {})
                # 安全处理敏感信息
                api_id = str(tg_data.get('api_id', ''))
                api_hash = tg_data.get('api_hash', '')
                bot_token = tg_data.get('bot_token', '')

                # 记录时遮蔽敏感信息
                config_logger.debug(f"Loading Telegram config - API ID: {'*' * len(api_id) if api_id else 'None'}")
                config_logger.debug(f"API Hash: {'*' * len(api_hash) if api_hash else 'None'}")
                config_logger.debug(f"Bot Token: {'*' * len(bot_token) if bot_token else 'None'}")

                self._typed_cache['telegram_config'] = TelegramConfig(
                    enabled=tg_data.get('enabled', False),
                    api_id=api_id,
                    api_hash=api_hash,
                    bot_token=bot_token,
                    chat_ids=tg_data.get('chat_ids', []),
                    session_name=tg_data.get('session_name', 'quote_bot')
                )
            except Exception as e:
                config_logger.error(f"Failed to parse telegram config: {e}")
                self._typed_cache['telegram_config'] = TelegramConfig()

        return self._typed_cache['telegram_config']

    def get_exchange_rules(self) -> ExchangeRules:
        """获取交易所规则配置（类型安全）"""
        if 'exchange_rules' not in self._typed_cache:
            try:
                rules_data = self.get_nested('exchange_rules', {})
                self._typed_cache['exchange_rules'] = ExchangeRules(
                    exchange_mapping=rules_data.get('exchange_mapping', {}),
                    symbol_rules=rules_data.get('symbol_rules', {}),
                    symbol_start_with=rules_data.get('symbol_start_with', {})
                )
            except Exception as e:
                config_logger.error(f"Failed to parse exchange rules: {e}")
                self._typed_cache['exchange_rules'] = ExchangeRules()

        return self._typed_cache['exchange_rules']

    def get_data_config(self) -> DataConfig:
        """获取数据配置（类型安全）"""
        if 'data_config' not in self._typed_cache:
            try:
                data_data = self.get_nested('data_config', {})
                self._typed_cache['data_config'] = DataConfig(
                    data_dir=data_data.get('data_dir', 'data'),
                    market_presets=data_data.get('market_presets', {}),
                    batch_size=data_data.get('batch_size', 50)
                )
            except Exception as e:
                config_logger.error(f"Failed to parse data config: {e}")
                self._typed_cache['data_config'] = DataConfig()

        return self._typed_cache['data_config']

    def get_scheduler_config(self) -> SchedulerConfig:
        """获取调度器配置（类型安全）"""
        if 'scheduler_config' not in self._typed_cache:
            try:
                scheduler_data = self.get_nested('scheduler_config', {})
                self._typed_cache['scheduler_config'] = SchedulerConfig(
                    enabled=scheduler_data.get('enabled', True),
                    timezone=scheduler_data.get('timezone', 'Asia/Shanghai'),
                    jobs=scheduler_data.get('jobs', {})
                )
            except Exception as e:
                config_logger.error(f"Failed to parse scheduler config: {e}")
                self._typed_cache['scheduler_config'] = SchedulerConfig()

        return self._typed_cache['scheduler_config']

    def get_data_source_config(self, source_name: str) -> Optional[DataSourceConfig]:
        """获取指定数据源的配置"""
        cache_key = f'data_source_config_{source_name}'
        if cache_key not in self._typed_cache:
            try:
                sources_config = self.get_nested('data_sources_config', {})
                source_data = sources_config.get(source_name, {})
                if source_data:
                    self._typed_cache[cache_key] = DataSourceConfig(
                        enabled=source_data.get('enabled', True),
                        exchanges_supported=source_data.get('exchanges_supported', []),
                        primary_source_of=source_data.get('primary_source_of', []),
                        max_requests_per_minute=source_data.get('max_requests_per_minute', 30),
                        max_requests_per_hour=source_data.get('max_requests_per_hour', 500),
                        max_requests_per_day=source_data.get('max_requests_per_day', 5000),
                        retry_times=source_data.get('retry_times', 3),
                        retry_interval=source_data.get('retry_interval', 2.0)
                    )
                else:
                    self._typed_cache[cache_key] = None
            except Exception as e:
                config_logger.error(f"Failed to parse {source_name} config: {e}")
                self._typed_cache[cache_key] = None

        return self._typed_cache[cache_key]
    
    def get_api_config(self) -> ApiConfig:
        """获取API配置（类型安全）"""
        if 'api_config' not in self._typed_cache:
            try:
                api_data = self.get_nested('api_config', {})
                self._typed_cache['api_config'] = ApiConfig(
                    enabled=api_data.get('enabled', True),
                    host=api_data.get('host', '0.0.0.0'),
                    port=api_data.get('port', 8000),
                    workers=api_data.get('workers', 1),
                    reload=api_data.get('reload', False),
                    cors_origins=api_data.get('cors_origins', ['*'])
                )
            except Exception as e:
                config_logger.error(f"Failed to parse api config: {e}")
                self._typed_cache['api_config'] = ApiConfig()

        return self._typed_cache['api_config']
                


    # ========================================================================
    # 便捷方法
    # ========================================================================

    def is_enabled(self, feature_path: str) -> bool:
        """检查功能是否启用"""
        return self.get_nested(f"{feature_path}.enabled", False)

    def get_data_dir(self) -> str:
        """获取数据目录"""
        return self.get_nested('data_config.data_dir', 'data')

    def save_config(self, file_path: Optional[str] = None) -> None:
        """保存配置到文件"""
        save_path = Path(file_path) if file_path else self._config_path

        try:
            # 确保目录存在
            save_path.parent.mkdir(parents=True, exist_ok=True)

            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(self._config_data, f, indent=2, ensure_ascii=False)
            config_logger.info(f"Configuration saved to: {save_path}")
        except Exception as e:
            raise ConfigurationError(
                f"Failed to save configuration: {e}",
                ErrorCodes.CONFIG_SAVE_ERROR
            ) from e

    def to_dict(self) -> Dict[str, Any]:
        """返回配置数据的字典副本"""
        return self._config_data.copy()

    def update_from_dict(self, config_dict: Dict[str, Any]) -> None:
        """从字典更新配置"""
        self._config_data.update(config_dict)
        self._typed_cache.clear()  # 清除缓存
        config_logger.info("Configuration updated from dict")

    def clear_cache(self) -> None:
        """清除类型化配置缓存"""
        self._typed_cache.clear()
        config_logger.debug("Configuration cache cleared")


# ============================================================================
# 全局单例实例
# ============================================================================

# 创建统一配置管理器实例
config_manager = UnifiedConfigManager()

# 为了向后兼容，保留原有的别名
config_instance = config_manager