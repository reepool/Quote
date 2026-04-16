#!/usr/bin/env python3
"""
配置管理器使用示例
演示如何访问和使用系统配置
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_manager import config_manager


def example_typed_config_access():
    """示例1：类型安全的配置访问（推荐）"""
    print("=" * 60)
    print("示例1：类型安全的配置访问")
    print("=" * 60)

    # API配置
    api_config = config_manager.get_api_config()
    print(f"API配置:")
    print(f"  - 启用状态: {api_config.enabled}")
    print(f"  - 监听地址: {api_config.host}")
    print(f"  - 监听端口: {api_config.port}")

    # 数据库配置
    db_config = config_manager.get_database_config()
    print(f"\n数据库配置:")
    print(f"  - 数据库路径: {db_config.db_path}")
    print(f"  - 备份启用: {db_config.backup_enabled}")
    print(f"  - 备份间隔: {db_config.backup_interval_days} 天")

    # 调度器配置
    scheduler_config = config_manager.get_scheduler_config()
    print(f"\n调度器配置:")
    print(f"  - 启用状态: {scheduler_config.enabled}")
    print(f"  - 时区: {scheduler_config.timezone}")

    # Telegram配置（注意：敏感信息会被遮蔽）
    telegram_config = config_manager.get_telegram_config()
    print(f"\nTelegram配置:")
    print(f"  - 启用状态: {telegram_config.enabled}")
    print(f"  - API ID: {'已配置' if telegram_config.api_id else '未配置'}")
    print(f"  - 聊天ID数量: {len(telegram_config.chat_ids)}")

    # 数据配置
    data_config = config_manager.get_data_config()
    print(f"\n数据配置:")
    print(f"  - 数据目录: {data_config.data_dir}")
    print(f"  - 批处理大小: {data_config.batch_size}")
    print(f"  - 市场预设: {list(data_config.market_presets.keys())}")

    # 交易所规则
    exchange_rules = config_manager.get_exchange_rules()
    print(f"\n交易所规则:")
    print(f"  - 交易所映射: {exchange_rules.exchange_mapping}")
    print(f"  - 代码规则: {exchange_rules.symbol_rules}")
    print(f"  - 代码前缀: {exchange_rules.symbol_start_with}")


def example_nested_config_access():
    """示例2：嵌套配置访问"""
    print("\n" + "=" * 60)
    print("示例2：嵌套配置访问")
    print("=" * 60)

    # 使用 get_nested() 方法
    api_port = config_manager.get_nested('api_config.port', 8000)
    api_host = config_manager.get_nested('api_config.host', 'localhost')

    print(f"API服务地址: {api_host}:{api_port}")

    # 获取日志配置
    log_level = config_manager.get_nested('logging_config.level', 'INFO')
    log_file = config_manager.get_nested('logging_config.file_config.filename', 'sys.log')

    print(f"日志级别: {log_level}")
    print(f"日志文件: {log_file}")

    # 获取数据源配置
    baostock_enabled = config_manager.get_nested('data_sources_config.baostock.enabled', False)
    baostock_rpm = config_manager.get_nested('data_sources_config.baostock.max_requests_per_minute', 0)

    print(f"BaoStock启用: {baostock_enabled}")
    print(f"BaoStock请求限制: {baostock_rpm} 次/分钟")


def example_data_source_config():
    """示例3：数据源配置访问"""
    print("\n" + "=" * 60)
    print("示例3：数据源配置访问")
    print("=" * 60)

    # 获取各个数据源的配置
    data_sources = ['baostock', 'akshare', 'yfinance', 'tushare']

    for source_name in data_sources:
        config = config_manager.get_data_source_config(source_name)
        if config:
            print(f"\n{source_name.upper()} 配置:")
            print(f"  - 启用状态: {config.enabled}")
            print(f"  - 支持市场区域: {config.exchanges_supported}")
            print(f"  - 支持品种类型: {config.instrument_types_supported or ['全部']}")
            print(f"  - 请求限制: {config.max_requests_per_minute} 次/分钟")
            print(f"  - 重试次数: {config.retry_times}")
            print(f"  - 重试间隔: {config.retry_interval} 秒")
        else:
            print(f"\n{source_name.upper()}: 配置不存在")


def example_convenience_methods():
    """示例4：便捷方法使用"""
    print("\n" + "=" * 60)
    print("示例4：便捷方法使用")
    print("=" * 60)

    # 检查功能是否启用
    telegram_enabled = config_manager.is_enabled('telegram_config')
    scheduler_enabled = config_manager.is_enabled('scheduler_config')
    api_enabled = config_manager.is_enabled('api_config')

    print(f"功能启用状态:")
    print(f"  - Telegram通知: {'✅' if telegram_enabled else '❌'}")
    print(f"  - 调度器: {'✅' if scheduler_enabled else '❌'}")
    print(f"  - API服务: {'✅' if api_enabled else '❌'}")

    # 获取常用路径
    data_dir = config_manager.get_data_dir()
    print(f"\n数据目录: {data_dir}")

    # 获取完整配置字典
    config_dict = config_manager.to_dict()
    print(f"\n配置包含的主要部分: {list(config_dict.keys())}")


def example_dynamic_config_update():
    """示例5：动态配置更新"""
    print("\n" + "=" * 60)
    print("示例5：动态配置更新")
    print("=" * 60)

    # 修改API端口
    original_port = config_manager.get_nested('api_config.port', 8000)
    print(f"原始API端口: {original_port}")

    # 动态更新配置
    config_manager.set_nested('api_config.port', 9000)
    new_port = config_manager.get_nested('api_config.port', 8000)
    print(f"更新后API端口: {new_port}")

    # 恢复原始配置
    config_manager.set_nested('api_config.port', original_port)
    restored_port = config_manager.get_nested('api_config.port', 8000)
    print(f"恢复后API端口: {restored_port}")

    # 清除缓存测试
    config_manager.clear_cache()
    print(f"\n配置缓存已清除，下次访问将重新解析")


def example_error_handling():
    """示例6：错误处理"""
    print("\n" + "=" * 60)
    print("示例6：错误处理")
    print("=" * 60)

    # 访问不存在的配置
    non_existent = config_manager.get_nested('non_existent.config', '默认值')
    print(f"不存在的配置: {non_existent}")

    # 访问不存在的数据源配置
    non_existent_source = config_manager.get_data_source_config('non_existent_source')
    print(f"不存在的数据源配置: {non_existent_source}")

    # 安全的字典访问
    try:
        config = config_manager['non_existent_key']
    except KeyError:
        print("字典式访问遇到KeyError，这是正常的")

    # 使用in检查
    if 'non_existent_key' not in config_manager:
        print("使用in检查确认配置不存在")


def main():
    """主函数"""
    print("🔧 Quote System 配置管理器使用示例")
    print("本示例展示如何访问和使用系统配置")

    try:
        # 执行各种示例
        example_typed_config_access()
        example_nested_config_access()
        example_data_source_config()
        example_convenience_methods()
        example_dynamic_config_update()
        example_error_handling()

        print("\n" + "=" * 60)
        print("✅ 所有示例执行完成！")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ 执行示例时出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
