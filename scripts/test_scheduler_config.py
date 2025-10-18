#!/usr/bin/env python3
"""
Scheduler配置化功能测试脚本
验证配置文件解析和任务配置功能
"""

import asyncio
import sys
import os

# 添加项目路径
sys.path.append('/home/python/Quote')

from utils import config_manager, scheduler_logger
from scheduler.job_config import JobConfigManager
from scheduler.scheduler import TaskScheduler


async def test_job_config_parsing():
    """测试任务配置解析"""
    print("=" * 60)
    print("测试任务配置解析")
    print("=" * 60)

    try:
        # 创建配置管理器
        config_mgr = config_manager

        # 创建任务配置管理器
        job_config_mgr = JobConfigManager(config_mgr)

        # 加载任务配置
        job_configs = job_config_mgr.load_job_configs()

        print(f"✅ 成功加载 {len(job_configs)} 个任务配置")

        # 显示每个任务的配置信息
        for job_id, job_config in job_configs.items():
            print(f"\n📋 任务: {job_id}")
            print(f"   描述: {job_config.description}")
            print(f"   启用: {job_config.enabled}")
            print(f"   触发器: {type(job_config.trigger).__name__}")
            print(f"   最大实例数: {job_config.max_instances}")
            print(f"   宽限期: {job_config.misfire_grace_time}秒")
            print(f"   合并执行: {job_config.coalesce}")
            print(f"   参数: {job_config.parameters}")

            # 显示下次运行时间
            next_run = job_config_mgr.get_next_run_time(job_id)
            if next_run:
                print(f"   下次运行: {next_run}")

        return True

    except Exception as e:
        print(f"❌ 配置解析失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_scheduler_initialization():
    """测试调度器初始化"""
    print("\n" + "=" * 60)
    print("测试调度器初始化")
    print("=" * 60)

    try:
        # 创建调度器实例
        scheduler = TaskScheduler()

        # 初始化调度器
        await scheduler.initialize()

        print(f"✅ 调度器初始化成功")
        print(f"   调度器运行状态: {scheduler.scheduler.running}")
        print(f"   已配置任务数: {len(scheduler.jobs)}")

        # 显示任务状态
        for job_id, job_info in scheduler.jobs.items():
            job = job_info['job']
            print(f"\n⏰ 任务: {job_id}")
            print(f"   描述: {job_info['description']}")
            print(f"   下次运行: {getattr(job, 'next_run_time', 'N/A')}")
            print(f"   运行次数: {getattr(job, 'executions', 0)}")

        # 测试任务状态查询
        print(f"\n📊 任务状态查询:")
        all_status = scheduler.get_all_jobs_status()
        for job_id, status in all_status.get('jobs', {}).items():
            print(f"   {job_id}: {status.get('enabled', True)} - {status.get('description', 'N/A')}")

        return True

    except Exception as e:
        print(f"❌ 调度器初始化失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_job_parameters():
    """测试任务参数配置"""
    print("\n" + "=" * 60)
    print("测试任务参数配置")
    print("=" * 60)

    try:
        # 创建配置管理器
        config_mgr = config_manager

        # 创建任务配置管理器
        job_config_mgr = JobConfigManager(config_mgr)
        job_configs = job_config_mgr.load_job_configs()

        # 测试每日数据更新任务参数
        daily_job_config = job_configs.get('daily_data_update')
        if daily_job_config:
            params = daily_job_config.parameters
            print(f"📈 每日数据更新任务参数:")
            print(f"   交易所: {params.get('exchanges', [])}")
            print(f"   等待收盘: {params.get('wait_for_market_close', True)}")
            print(f"   收盘延迟: {params.get('market_close_delay_minutes', 15)}分钟")
            print(f"   交易日检查: {params.get('enable_trading_day_check', True)}")

        # 测试系统健康检查任务参数
        health_job_config = job_configs.get('system_health_check')
        if health_job_config:
            params = health_job_config.parameters
            print(f"\n🔍 系统健康检查任务参数:")
            print(f"   检查数据源: {params.get('check_data_sources', True)}")
            print(f"   检查数据库: {params.get('check_database', True)}")
            print(f"   检查磁盘空间: {params.get('check_disk_space', True)}")
            print(f"   磁盘空间阈值: {params.get('disk_space_threshold_mb', 1000)}MB")
            print(f"   内存阈值: {params.get('memory_threshold_percent', 85)}%")

        # 测试周维护任务参数
        weekly_job_config = job_configs.get('weekly_data_maintenance')
        if weekly_job_config:
            params = weekly_job_config.parameters
            print(f"\n🧹 每周数据维护任务参数:")
            print(f"   备份数据库: {params.get('backup_database', True)}")
            print(f"   清理日志: {params.get('cleanup_old_logs', True)}")
            print(f"   日志保留天数: {params.get('log_retention_days', 30)}天")
            print(f"   优化数据库: {params.get('optimize_database', True)}")
            print(f"   数据完整性验证: {params.get('validate_data_integrity', True)}")

        return True

    except Exception as e:
        print(f"❌ 任务参数测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_trigger_parsing():
    """测试触发器解析"""
    print("\n" + "=" * 60)
    print("测试触发器解析")
    print("=" * 60)

    try:
        from scheduler.job_config import JobConfigManager
        from utils import config_manager

        config_mgr = config_manager
        job_config_mgr = JobConfigManager(config_mgr)
        job_configs = job_config_mgr.load_job_configs()

        for job_id, job_config in job_configs.items():
            trigger = job_config.trigger
            print(f"\n⏰ {job_id} 触发器:")
            print(f"   类型: {type(trigger).__name__}")

            if hasattr(trigger, 'fields'):
                # CronTrigger
                print(f"   Cron字段: {trigger.fields}")
                if hasattr(trigger, 'day_of_week'):
                    print(f"   星期: {trigger.day_of_week}")
                if hasattr(trigger, 'hour'):
                    print(f"   小时: {trigger.hour}")
                if hasattr(trigger, 'minute'):
                    print(f"   分钟: {trigger.minute}")
            elif hasattr(trigger, 'interval'):
                # IntervalTrigger
                interval = trigger.interval
                print(f"   间隔: {interval}")
                if hasattr(trigger, 'weeks') and trigger.weeks:
                    print(f"   周: {trigger.weeks}")
                if hasattr(trigger, 'days') and trigger.days:
                    print(f"   天: {trigger.days}")
                if hasattr(trigger, 'hours') and trigger.hours:
                    print(f"   小时: {trigger.hours}")
                if hasattr(trigger, 'minutes') and trigger.minutes:
                    print(f"   分钟: {trigger.minutes}")

        return True

    except Exception as e:
        print(f"❌ 触发器解析测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """主测试函数"""
    print("🚀 开始测试 Scheduler 配置化功能")
    print(f"当前工作目录: {os.getcwd()}")

    tests = [
        ("任务配置解析", test_job_config_parsing),
        ("触发器解析", test_trigger_parsing),
        ("任务参数配置", test_job_parameters),
        ("调度器初始化", test_scheduler_initialization),
    ]

    results = []

    for test_name, test_func in tests:
        print(f"\n🧪 执行测试: {test_name}")
        try:
            result = await test_func()
            results.append((test_name, result))
            if result:
                print(f"✅ {test_name} - 通过")
            else:
                print(f"❌ {test_name} - 失败")
        except Exception as e:
            print(f"❌ {test_name} - 异常: {e}")
            results.append((test_name, False))

    # 显示测试结果汇总
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{test_name:20} - {status}")

    print(f"\n总计: {passed}/{total} 个测试通过")

    if passed == total:
        print("🎉 所有测试通过！配置化功能正常工作。")
        return 0
    else:
        print("⚠️  部分测试失败，请检查配置和代码。")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)