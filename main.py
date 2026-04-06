"""
Main entry point for the Quote System.
Provides command-line interface and system initialization.
"""

import asyncio
import argparse
import sys
from typing import Optional, List
from datetime import date, datetime


from utils import (
    scheduler_logger, dm_logger, tgbot_logger, api_logger, config_manager, get_process_manager
)
# 直接导入代码转换工具，避免依赖问题
from utils.code_utils import convert_to_database_format, is_valid_standard_format

from data_manager import data_manager
from scheduler.scheduler import task_scheduler
from scheduler.job_config import job_config_manager
from api.app import app as api_app
from utils.task_manager.task_manager import TaskManagerBot


class QuoteSystem:
    """行情系统主类"""

    def __init__(self):
        self.config = config_manager
        self.running = False
        self.task_manager_bot = None
        self._telegram_bot = None
        self.process_manager = get_process_manager()
        self.service_name = "QuoteSystem"

    async def initialize(self, include_scheduler: bool = True):
        """初始化系统"""
        try:
            scheduler_logger.info("[Main] Initializing Quote System...")

            # 提前初始化TelegramBot（确保所有组件都能使用）
            await self._initialize_telegram_bot()

            # 初始化数据管理器
            await data_manager.initialize()

            # 初始化调度器监控器 (需要 config_manager)
            from scheduler.monitor import SchedulerMonitor
            self.scheduler_monitor = SchedulerMonitor(self.config)

            if include_scheduler:
                # 初始化任务调度器
                await task_scheduler.initialize()

                # 初始化调度器监控
                await self.scheduler_monitor.initialize()

                # 初始化任务管理机器人
                await self._initialize_task_manager()

            scheduler_logger.info("[Main] Quote System initialized successfully")

        except Exception as e:
            scheduler_logger.error(f"[Main] Failed to initialize system: {e}")
            raise

    async def initialize_lightweight(self):
        """轻量级初始化，只初始化数据管理器（用于不需要调度器的命令）"""
        try:
            scheduler_logger.info("[Main] Initializing Quote System (lightweight mode)...")

            # 只初始化数据管理器，不初始化调度器
            await data_manager.initialize()

            scheduler_logger.info("[Main] Quote System initialized successfully (lightweight mode)")

        except Exception as e:
            scheduler_logger.error(f"[Main] Failed to initialize system: {e}")
            raise

    async def start_scheduler_only(self):
        """仅启动调度器"""
        try:
            scheduler_logger.info("[Main] Starting scheduler mode...")
            self.running = True

            # 设置信号处理
            self._setup_signal_handlers()

            # 保持运行
            while self.running:
                await asyncio.sleep(1)

        except KeyboardInterrupt:
            scheduler_logger.info("[Main] Received keyboard interrupt, shutting down...")
        except Exception as e:
            scheduler_logger.error(f"[Main] Scheduler mode error: {e}")
        finally:
            await self.shutdown()

    async def start_api_server(self, host: str = None, port: int = None):
        """启动API服务器"""
        try:
            # 获取API配置
            api_config = self.config.get_api_config()

            # 使用配置文件的值，如果命令行参数未提供
            final_host = host if host is not None else api_config.host
            final_port = port if port is not None else api_config.port

            api_logger.info(f"[Main] Starting API server on {final_host}:{final_port}...")
            api_logger.info(f"[Main] API config - workers: {api_config.workers}, reload: {api_config.reload}")

            import uvicorn
            config = uvicorn.Config(
                api_app,
                host=final_host,
                port=final_port,
                workers=api_config.workers,
                reload=api_config.reload,
                log_level="info"
            )
            server = uvicorn.Server(config)

            # 启动服务器
            await server.serve()

        except Exception as e:
            api_logger.error(f"[Main] API server error: {e}")
            raise

    async def start_full_system(self, host: str = None, port: int = None):
        """启动完整系统（调度器 + API服务）"""
        try:
            scheduler_logger.info("[Main] Starting full system mode (Scheduler + API Server)...")
            self.running = True

            # 设置信号处理
            self._setup_signal_handlers()

            # 获取API配置
            api_config = self.config.get_api_config()
            final_host = host if host is not None else api_config.host
            final_port = port if port is not None else api_config.port

            # 启动API服务器
            api_logger.info(f"[Main] Starting API server on {final_host}:{final_port}...")
            api_logger.info(f"[Main] API config - workers: {api_config.workers}, reload: {api_config.reload}")

            import uvicorn
            config = uvicorn.Config(
                api_app,
                host=final_host,
                port=final_port,
                workers=api_config.workers,
                reload=api_config.reload,
                log_level="info"
            )
            server = uvicorn.Server(config)

            # 并发运行API服务器和调度器
            api_task = asyncio.create_task(server.serve())
            scheduler_logger.info("[Main] API server started, scheduler is already running in background")

            # 等待信号或异常
            while self.running:
                await asyncio.sleep(1)

                # 检查API服务器是否还在运行
                if api_task.done():
                    try:
                        api_task.result()  # 这会抛出异常如果有错误
                    except Exception as e:
                        api_logger.error(f"[Main] API server crashed: {e}")
                        break

            # 停止API服务器
            if not api_task.done():
                api_logger.info("[Main] Shutting down API server...")
                server.should_exit = True
                try:
                    await asyncio.wait_for(api_task, timeout=10)
                except asyncio.TimeoutError:
                    api_logger.warning("[Main] API server shutdown timeout")
                    api_task.cancel()

        except KeyboardInterrupt:
            scheduler_logger.info("[Main] Received keyboard interrupt, shutting down full system...")
        except Exception as e:
            scheduler_logger.error(f"[Main] Full system mode error: {e}")
        finally:
            await self.shutdown()

    async def download_historical_data(self, exchanges: Optional[list] = None, years: Optional[list] = None,
                                      start_date: Optional[date] = None, end_date: Optional[date] = None,
                                      preset: Optional[str] = None, resume: bool = True,
                                      instrument_types: Optional[list] = None):
        """下载历史数据"""
        try:
            dm_logger.info("[Main] Starting historical data download...")

            # 处理预设组合
            if preset:
                exchanges = self._get_preset_exchanges(preset)
                dm_logger.info(f"[Main] Using preset '{preset}': {exchanges}")
            elif exchanges is None:
                exchanges = ['SSE', 'SZSE', 'BSE']

            # 刷新股票列表以获取最新的上市日期信息
            dm_logger.info("[Main] Using precise download mode (based on listed dates)")
            await self._refresh_instrument_list(exchanges, instrument_types=instrument_types)

            # 处理日期范围参数
            from datetime import date

            # 如果直接提供了日期范围，使用它们
            if start_date or end_date:
                if start_date and end_date:
                    dm_logger.info(f"[Main] Using specified date range: {start_date} to {end_date}")
                elif start_date:
                    dm_logger.info(f"[Main] Using start date: {start_date} to today")
                    end_date = date.today()
                elif end_date:
                    dm_logger.info(f"[Main] Using date range from beginning to {end_date}")
            # 否则使用years参数
            elif years:
                # 计算年份范围
                min_year = min(years)
                max_year = max(years)
                start_date = date(min_year, 1, 1)
                end_date = date(max_year, 12, 31)
                dm_logger.info(f"[Main] Using date range: {start_date} to {end_date} (from years: {years})")
            else:
                # 让data_manager根据配置决定默认年份范围
                dm_logger.info(f"[Main] Using default date range (no date parameters provided)")

            # 执行下载
            # 判断是否需要强制更新交易日历
            force_update_calendar = True  # 默认强制更新（全历史下载）

            # 如果指定了日期范围，则不使用续传模式（重新下载指定范围）
            if start_date or end_date:
                resume = False
                dm_logger.info("[Main] Specified date range, disabling resume mode")
                force_update_calendar = False
                dm_logger.info("[Main] Using cached trading calendar for partial date range download")
            elif resume and data_manager.progress.processed_instruments > 0:
                dm_logger.info("[Main] Resume mode, continuing with download progress")

            await data_manager.download_all_historical_data(
                exchanges, start_date, end_date, resume=resume,
                force_update_calendar=force_update_calendar,
                instrument_types=instrument_types
            )

            dm_logger.info("[Main] Historical data download completed")

        except Exception as e:
            dm_logger.error(f"[Main] Historical data download failed: {e}")
            raise

    async def _refresh_instrument_list(self, exchanges: List[str],
                                       instrument_types: Optional[list] = None):
        """刷新交易品种列表（获取最新的上市日期信息）"""
        try:
            dm_logger.info(f"[Main] Refreshing instrument list for exchanges: {exchanges}")

            from data_sources.source_factory import data_source_factory

            for exchange in exchanges:
                dm_logger.info(f"[Main] Refreshing instruments for {exchange}")

                # 获取最新的交易品种信息（包含上市日期）- 强制刷新
                instruments = await data_source_factory.get_instrument_list(
                    exchange, force_refresh=True, instrument_types=instrument_types
                )

                if instruments:
                    # 保存到数据库
                    from database import db_ops
                    success = await db_ops.save_instrument_list(instruments)

                    if success:
                        dm_logger.info(f"[Main] Successfully refreshed {len(instruments)} instruments for {exchange}")
                    else:
                        dm_logger.error(f"[Main] Failed to save instruments for {exchange}")
                else:
                    dm_logger.warning(f"[Main] No instruments found for {exchange}")

        except Exception as e:
            dm_logger.error(f"[Main] Failed to refresh instrument list: {e}")
            raise

    async def update_daily_data(self, exchanges: Optional[list] = None, target_date=None):
        """更新日线数据"""
        try:
            if target_date:
                dm_logger.info(f"[Main] Starting daily data BACKFILL for {target_date}...")
            else:
                dm_logger.info("[Main] Starting daily data update...")

            if exchanges is None:
                exchanges = ['SSE', 'SZSE', 'BSE']

            from datetime import date
            if target_date is None:
                target_date = date.today()

            # 执行更新
            await data_manager.update_daily_data(exchanges, target_date)

            dm_logger.info("[Main] Daily data update completed")

        except Exception as e:
            dm_logger.error(f"[Main] Daily data update failed: {e}")
            raise

    async def download_single_instrument(self, instrument_id: str, start_date: date = None,
                                       end_date: date = None, resume: bool = False):
        """下载指定股票的历史数据"""
        try:
            dm_logger.info(f"[Main] Starting single instrument download: {instrument_id}")

            # 验证instrument_id格式
            if not self._validate_instrument_id(instrument_id):
                print(f"错误: 无效的股票代码格式 '{instrument_id}'")
                print("正确格式示例: 000001.SZ, 600000.SSE")
                return

            # 设置默认日期范围
            from datetime import date
            if start_date is None:
                start_date = date(1990, 1, 1)  # 默认从1990年开始
            if end_date is None:
                end_date = date.today()

            print(f"\n🔍 开始下载指定股票数据:")
            print(f"   股票代码: {instrument_id}")
            print(f"   日期范围: {start_date} 到 {end_date}")
            print(f"   续传模式: {'开启' if resume else '关闭'}")

            # 验证股票是否存在
            instrument_info = await self._get_instrument_info(instrument_id)
            if not instrument_info:
                print(f"错误: 找不到股票代码 '{instrument_id}'")
                print("提示: 请检查股票代码是否正确，或先下载对应交易所的股票列表")
                return

            print(f"   股票名称: {instrument_info.get('name', 'N/A')}")
            print(f"   交易所: {instrument_info.get('exchange', 'N/A')}")
            print(f"   上市日期: {instrument_info.get('listed_date', 'N/A')}")

            # 执行下载
            success = await data_manager.download_single_instrument_data(
                instrument_id, instrument_info, start_date, end_date, resume
            )

            if success:
                print(f"\n✅ 股票 {instrument_id} 数据下载完成!")

                # 显示下载统计
                stats = await self._get_instrument_download_stats(instrument_id, start_date, end_date)
                if stats:
                    print(f"\n📊 下载统计:")
                    print(f"   总记录数: {stats.get('total_quotes', 0):,}")
                    print(f"   数据范围: {stats.get('first_date', 'N/A')} 到 {stats.get('last_date', 'N/A')}")

                    # 检查是否有缺口
                    gaps = await data_manager.detect_data_gaps(
                        [instrument_info.get('exchange')], start_date, end_date
                    )
                    instrument_gaps = [gap for gap in gaps if gap.instrument_id == instrument_id]

                    if instrument_gaps:
                        print(f"   数据缺口: {len(instrument_gaps)} 个")
                        for gap in instrument_gaps[:3]:  # 只显示前3个
                            print(f"      {gap.gap_start} 到 {gap.gap_end} ({gap.gap_days}天)")
                        if len(instrument_gaps) > 3:
                            print(f"      ... 还有 {len(instrument_gaps) - 3} 个缺口")
                    else:
                        print(f"   数据完整性: ✅ 无缺口")
            else:
                print(f"\n❌ 股票 {instrument_id} 数据下载失败!")
                print("请检查日志文件获取详细错误信息")

            dm_logger.info(f"[Main] Single instrument download completed: {instrument_id}")

        except Exception as e:
            dm_logger.error(f"[Main] Single instrument download failed: {e}")
            print(f"下载失败: {e}")
            raise
        finally:
            # 确保关闭数据源，避免资源泄漏
            try:
                from data_sources.source_factory import data_source_factory
                if data_source_factory:
                    await data_source_factory.close_all()
                    dm_logger.debug("[Main] DataSourceFactory closed successfully")
            except Exception as close_error:
                dm_logger.warning(f"[Main] Failed to close DataSourceFactory: {close_error}")

    def _validate_instrument_id(self, instrument_id: str) -> bool:
        """验证股票代码格式"""
        return is_valid_standard_format(instrument_id)

    async def _get_instrument_info(self, instrument_id: str) -> dict:
        """获取股票信息"""
        try:
            # 转换为数据库格式进行查询
            db_instrument_id = convert_to_database_format(instrument_id)
            dm_logger.info(f"[Main] Converting instrument ID: {instrument_id} -> {db_instrument_id}")
            return await data_manager.db_ops.get_instrument_by_id(db_instrument_id)
        except Exception as e:
            dm_logger.error(f"[Main] Failed to get instrument info: {e}")
            return None

    async def _get_instrument_download_stats(self, instrument_id: str, start_date: date, end_date: date) -> dict:
        """获取股票下载统计信息"""
        try:
            # 转换为数据库格式
            db_instrument_id = convert_to_database_format(instrument_id)

            # 获取数据记录数
            quote_count = await data_manager.db_ops.count_quotes_by_instrument(
                db_instrument_id, start_date, end_date
            )

            # 获取数据日期范围
            date_range = await data_manager.db_ops.get_instrument_date_range(
                db_instrument_id, start_date, end_date
            )

            return {
                'total_quotes': quote_count,
                'first_date': date_range.get('start_date'),
                'last_date': date_range.get('end_date')
            }
        except Exception as e:
            dm_logger.error(f"[Main] Failed to get download stats: {e}")
            return {}

    async def run_job(self, job_id: str, target_date=None):
        """运行指定任务

        Args:
            job_id: 任务ID
            target_date: 可选，指定补数据日期（仅 daily_data_update 有效）
        """
        try:
            scheduler_logger.info(f"[Main] Running job: {job_id}")

            # 直接执行任务，避免调度器复杂性
            from scheduler.tasks import scheduled_tasks

            if job_id == 'trading_calendar_update':
                # 从配置获取参数
                job_config = self.config.get_nested(f'scheduler_config.jobs.{job_id}', {})
                if not job_config:
                    scheduler_logger.error(f"[Main] Job {job_id} configuration not found")
                    return False

                params = job_config.get('parameters', {})
                success = await scheduled_tasks.trading_calendar_update(
                    exchanges=params.get('exchanges'),
                    update_future_months=params.get('update_future_months', 6),
                    force_update=params.get('force_update', False),
                    validate_holidays=params.get('validate_holidays', True)
                )

                if success:
                    scheduler_logger.info(f"[Main] Job {job_id} completed successfully")
                else:
                    scheduler_logger.error(f"[Main] Job {job_id} failed")
                return success

            elif job_id == 'daily_data_update':
                # 从配置获取参数
                job_config = self.config.get_nested(f'scheduler_config.jobs.{job_id}', {})
                if not job_config:
                    scheduler_logger.error(f"[Main] Job {job_id} configuration not found")
                    return False

                params = job_config.get('parameters', {})
                success = await scheduled_tasks.daily_data_update(
                    exchanges=params.get('exchanges'),
                    wait_for_market_close=params.get('wait_for_market_close', True),
                    market_close_delay_minutes=params.get('market_close_delay_minutes', 15),
                    enable_trading_day_check=params.get('enable_trading_day_check', True),
                    instrument_types=params.get('instrument_types'),
                    target_date=target_date
                )

                if success:
                    scheduler_logger.info(f"[Main] Job {job_id} completed successfully")
                else:
                    scheduler_logger.error(f"[Main] Job {job_id} failed")
                return success

            else:
                # 回退到调度器方式
                scheduler_logger.warning(f"[Main] Job {job_id} not implemented directly, falling back to scheduler")
                success = await task_scheduler.run_job_now(job_id)
                if success:
                    scheduler_logger.info(f"[Main] Job {job_id} scheduled successfully")
                else:
                    scheduler_logger.error(f"[Main] Failed to schedule job {job_id}")
                return success

        except Exception as e:
            scheduler_logger.error(f"[Main] Failed to run job {job_id}: {e}")
            import traceback
            scheduler_logger.error(f"[Main] Exception details: {traceback.format_exc()}")
            return False

    async def show_system_status(self):
        """显示系统状态"""
        try:
            status = await data_manager.get_system_status()

            print("\n" + "="*60)
            print("         QUOTE SYSTEM STATUS")
            print("="*60)

            # 数据管理器状态
            dm_status = status.get('data_manager', {})
            print(f"\n📊 Data Manager:")
            print(f"   Running: {dm_status.get('is_running', False)}")
            if dm_status.get('download_progress'):
                progress = dm_status['download_progress']
                print(f"   Download Progress: {progress.get('success_rate', 0):.1f}%")
                print(f"   Processed: {progress.get('processed_instruments', 0)}/{progress.get('total_instruments', 0)}")

            # 数据库状态
            db_status = status.get('database', {})
            print(f"\n💾 Database:")
            print(f"   Instruments: {db_status.get('instruments_count', 0):,}")
            print(f"   Quotes: {db_status.get('quotes_count', 0):,}")
            if db_status.get('quotes_date_range'):
                date_range = db_status['quotes_date_range']
                print(f"   Date Range: {date_range.get('start_date', 'N/A')} to {date_range.get('end_date', 'N/A')}")

            # 缓存状态
            cache_status = status.get('cache', {})
            print(f"\n⚡ Cache:")
            print(f"   Enabled: {cache_status.get('cache_enabled', False)}")
            if cache_status.get('quote_cache'):
                qc = cache_status['quote_cache']
                print(f"   Quote Cache: {qc.get('active_entries', 0)} active entries")

            # 数据源状态
            sources_status = status.get('data_sources', {})
            print(f"\n🔗 Data Sources:")
            for source, is_healthy in sources_status.items():
                status_icon = "✅" if is_healthy else "❌"
                print(f"   {status_icon} {source}: {'Healthy' if is_healthy else 'Unhealthy'}")

            # 调度器状态
            scheduler_status = task_scheduler.get_all_jobs_status()
            print(f"\n⏰ Scheduler:")
            print(f"   Running: {scheduler_status.get('scheduler_running', False)}")
            print(f"   Total Jobs: {scheduler_status.get('total_jobs', 0)}")

            print("\n" + "="*60)

        except Exception as e:
            scheduler_logger.error(f"[Main] Failed to get system status: {e}")
            print(f"Error getting system status: {e}")

    async def _initialize_telegram_bot(self):
        """提前初始化TelegramBot"""
        try:
            tgbot_logger.info("[Main] Initialization of TelegramBot...")

            # 检查Telegram配置
            telegram_config = self.config.get_nested('telegram_config', {})
            if not telegram_config.get('enabled', False):
                tgbot_logger.info("[Main] Telegram disabled, skipping early bot initialization")
                return

            # 创建并初始化TelegramBot
            from utils.tgbot import TelegramBot
            telegram_bot = TelegramBot()
            await telegram_bot.create_bot_instance()

            # 保存引用以便清理
            self._telegram_bot = telegram_bot
            tgbot_logger.info("[Main] TelegramBot initialized successfully")

        except Exception as e:
            tgbot_logger.error(f"[Main] Failed to initialize TelegramBot early: {e}")
            # 不抛出异常，允许系统继续运行

    async def _initialize_task_manager(self):
        """初始化任务管理机器人"""
        try:
            scheduler_logger.info("[Main] Initializing task manager bot...")

            # 使用已初始化的TelegramBot
            if not hasattr(self, '_telegram_bot') or not self._telegram_bot:
                scheduler_logger.warning("[Main] TelegramBot not initialized early, cannot create task manager")
                return

            telegram_bot = self._telegram_bot
            scheduler_logger.debug("[Main] Using pre-initialized TelegramBot for task manager")

            # 创建任务管理机器人实例
            self.task_manager_bot = TaskManagerBot(
                telegram_bot=telegram_bot,
                task_scheduler=task_scheduler,
                job_config_manager=job_config_manager,
                scheduler_monitor=self.scheduler_monitor,
                config_manager=self.config,
                logger=scheduler_logger
            )

            # 初始化任务管理机器人
            await self.task_manager_bot.initialize()

            # 保存TelegramBot引用以便清理
            self._telegram_bot = telegram_bot

            scheduler_logger.info("[Main] Task manager bot initialized successfully")

        except Exception as e:
            scheduler_logger.error(f"[Main] Failed to initialize task manager bot: {e}")
            # 不抛出异常，允许系统继续运行
            self.task_manager_bot = None

    async def shutdown(self):
        """关闭系统"""
        try:
            scheduler_logger.info("[Main] Shutting down Quote System...")

            # 关闭任务管理机器人
            if self.task_manager_bot:
                await self.task_manager_bot.cleanup()
                self.task_manager_bot = None

            # 关闭TelegramBot
            if self._telegram_bot:
                await self._telegram_bot.disconnect()
                self._telegram_bot = None

            # 关闭调度器
            await task_scheduler.shutdown()

            # 关闭数据源工厂
            from data_sources.source_factory import data_source_factory
            if data_source_factory:
                await data_source_factory.close_all()

            # 清理进程管理器
            self.process_manager.cleanup(self.service_name)

            scheduler_logger.info("[Main] Quote System shutdown completed")

        except Exception as e:
            scheduler_logger.error(f"[Main] Error during shutdown: {e}")

    def _get_preset_exchanges(self, preset: str) -> list:
        """获取预设组合对应的交易所列表"""
        try:
            presets = self.config.get_nested('data_config.market_presets', {})
            if preset not in presets:
                dm_logger.error(f"[Main] Unknown preset '{preset}'. Available presets: {list(presets.keys())}")
                raise ValueError(f"Unknown preset: {preset}")

            exchanges = presets[preset]
            dm_logger.info(f"[Main] Preset '{preset}' corresponds to exchanges: {exchanges}")
            return exchanges

        except Exception as e:
            dm_logger.error(f"[Main] Failed to get preset exchanges: {e}")
            raise

    async def list_market_presets(self):
        """显示所有可用的市场预设"""
        try:
            presets = self.config.get_nested('data_config.market_presets', {})

            print("\n" + "="*60)
            print("         可用市场预设组合")
            print("="*60)

            preset_descriptions = {
                "a_shares": "A股市场 (上海+深圳+北京)",
                "hk_stocks": "港股市场",
                "us_stocks": "美股市场 (纳斯达克+纽交所)",
                "mainland": "大陆股市 (上海+深圳+北京)",
                "overseas": "海外市场 (港股+美股)",
                "chinese": "中资股市场 (A股+港股)",
                "global": "全球主要市场 (美股)",
                "all_markets": "全部市场"
            }

            for preset_name, exchanges in presets.items():
                description = preset_descriptions.get(preset_name, "自定义组合")
                exchange_str = ", ".join(exchanges)
                print(f"\n📊 {preset_name:12} - {description}")
                print(f"   交易所: {exchange_str}")
                print(f"   命令: python main.py download --preset {preset_name}")

            print("\n" + "="*60)
            print("使用示例:")
            print("  python main.py download --preset a_shares")
            print("  python main.py download --preset us_stocks --years 2020 2021 2022")
            print("="*60)

        except Exception as e:
            dm_logger.error(f"[Main] Failed to list presets: {e}")
            print(f"Error listing presets: {e}")

    async def interactive_download(self, mode: str = 'both'):
        """交互式下载模式"""
        try:
            print("\n" + "="*60)
            print("         交互式历史数据下载")
            print("="*60)

            exchanges = None
            years = None

            # 交互选择交易所
            if mode in ['market', 'both']:
                print("\n📊 请选择要下载的市场:")
                print("1. A股市场 (SSE+SZSE)")
                print("2. 港股市场 (HKEX)")
                print("3. 美股市场 (NASDAQ+NYSE)")
                print("4. 大陆股市 (SSE+SZSE)")
                print("5. 海外市场 (HKEX+NASDAQ+NYSE)")
                print("6. 中资股市场 (SSE+SZSE+HKEX)")
                print("7. 全球主要市场 (NASDAQ+NYSE)")
                print("8. 全部市场")
                print("9. 自定义交易所")
                print("0. 查看所有预设")

                while True:
                    try:
                        choice = input("\n请输入选择 (0-9): ").strip()
                        if choice == '0':
                            await self.list_market_presets()
                            continue
                        elif choice == '1':
                            exchanges = ['SSE', 'SZSE']
                            break
                        elif choice == '2':
                            exchanges = ['HKEX']
                            break
                        elif choice == '3':
                            exchanges = ['NASDAQ', 'NYSE']
                            break
                        elif choice == '4':
                            exchanges = ['SSE', 'SZSE']
                            break
                        elif choice == '5':
                            exchanges = ['HKEX', 'NASDAQ', 'NYSE']
                            break
                        elif choice == '6':
                            exchanges = ['SSE', 'SZSE', 'HKEX']
                            break
                        elif choice == '7':
                            exchanges = ['NASDAQ', 'NYSE']
                            break
                        elif choice == '8':
                            exchanges = ['SSE', 'SZSE', 'HKEX', 'NASDAQ', 'NYSE']
                            break
                        elif choice == '9':
                            print("\n可用交易所: SSE, SZSE, HKEX, NASDAQ, NYSE")
                            exchanges_input = input("请输入交易所代码 (用空格分隔): ").strip().upper()
                            exchanges = exchanges_input.split()
                            valid_exchanges = {'SSE', 'SZSE', 'HKEX', 'NASDAQ', 'NYSE'}
                            exchanges = [ex for ex in exchanges if ex in valid_exchanges]
                            if not exchanges:
                                print("❌ 无效的交易所代码")
                                continue
                            break
                        else:
                            print("❌ 无效选择，请重新输入")
                    except KeyboardInterrupt:
                        print("\n\n操作已取消")
                        return

                print(f"✅ 已选择: {', '.join(exchanges)}")

            # 交互选择年份
            if mode in ['year', 'both']:
                print("\n📅 请选择年份范围:")
                print("1. 最近3年 (2022-2025)")
                print("2. 最近5年 (2020-2025)")
                print("3. 最近10年 (2015-2025)")
                print("4. 全部历史 (1990-2025)")
                print("5. 自定义年份")

                while True:
                    try:
                        choice = input("\n请输入选择 (1-5): ").strip()
                        current_year = 2025

                        if choice == '1':
                            years = list(range(current_year - 3, current_year + 1))
                            break
                        elif choice == '2':
                            years = list(range(current_year - 5, current_year + 1))
                            break
                        elif choice == '3':
                            years = list(range(current_year - 10, current_year + 1))
                            break
                        elif choice == '4':
                            years = list(range(1990, current_year + 1))
                            break
                        elif choice == '5':
                            years_input = input("请输入年份列表 (用空格分隔，如: 2020 2021 2022): ").strip()
                            try:
                                years = [int(year.strip()) for year in years_input.split()]
                                if not years:
                                    print("❌ 请至少输入一个年份")
                                    continue
                                years = sorted(years)
                                break
                            except ValueError:
                                print("❌ 年份格式错误，请重新输入")
                                continue
                        else:
                            print("❌ 无效选择，请重新输入")
                    except KeyboardInterrupt:
                        print("\n\n操作已取消")
                        return

                print(f"✅ 已选择年份: {min(years)}-{max(years)} ({len(years)}年)")

            # 确认下载
            print(f"\n📋 下载配置:")
            print(f"   交易所: {', '.join(exchanges) if exchanges else '全部'}")
            print(f"   年份范围: {min(years)}-{max(years)} ({len(years)}年)" if years else "   年份: 使用配置默认")

            confirm = input("\n是否开始下载? (y/N): ").strip().lower()
            if confirm not in ['y', 'yes']:
                print("下载已取消")
                return

            # 开始下载
            print("\n🚀 开始下载历史数据...")
            await self.download_historical_data(exchanges, years)

        except KeyboardInterrupt:
            print("\n\n交互式下载已取消")
        except Exception as e:
            dm_logger.error(f"[Main] Interactive download failed: {e}")
            print(f"交互式下载失败: {e}")

    async def detect_data_gaps(self, exchanges: Optional[list] = None, start_date: str = None,
                              end_date: str = None, severity_filter: str = None,
                              output_file: str = None, detailed: bool = False):
        """检测数据缺口"""
        try:
            from datetime import datetime

            dm_logger.info("[Main] Starting data gap detection...")

            # 处理日期参数
            start_date_obj = None
            end_date_obj = None

            if start_date:
                try:
                    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
                except ValueError:
                    print(f"错误: 开始日期格式无效，请使用 YYYY-MM-DD 格式")
                    return

            if end_date:
                try:
                    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
                except ValueError:
                    print(f"错误: 结束日期格式无效，请使用 YYYY-MM-DD 格式")
                    return

            # 检查日期范围
            if start_date_obj and end_date_obj and start_date_obj > end_date_obj:
                print(f"错误: 开始日期 {start_date_obj} 不能晚于结束日期 {end_date_obj}")
                return

            # 设置默认交易所
            if exchanges is None:
                exchanges = ['SSE', 'SZSE', 'BSE']

            # 设置默认日期范围
            if start_date_obj is None:
                start_date_obj = date(1990, 1, 1)  # 从1990年开始
            if end_date_obj is None:
                end_date_obj = date.today()

            print(f"\n🔍 开始检测数据缺口...")
            print(f"   交易所: {', '.join(exchanges)}")
            print(f"   日期范围: {start_date_obj} 到 {end_date_obj}")

            # 执行GAP检测
            gaps = await data_manager.detect_data_gaps(exchanges, start_date_obj, end_date_obj)

            # 按严重程度过滤
            if severity_filter:
                original_count = len(gaps)
                gaps = [gap for gap in gaps if gap.severity == severity_filter]
                print(f"   严重程度过滤: {severity_filter} ({original_count} -> {len(gaps)})")

            # 生成报告
            report = await self._generate_gap_report(gaps, detailed, exchanges, start_date_obj, end_date_obj)

            # 输出结果
            self._display_gap_report(report, detailed)

            # 保存到文件
            if output_file:
                await self._save_gap_report(report, output_file)
                print(f"\n📄 报告已保存到: {output_file}")

            dm_logger.info(f"[Main] Gap detection completed. Found {len(gaps)} gaps.")

        except Exception as e:
            dm_logger.error(f"[Main] Gap detection failed: {e}")
            print(f"缺口检测失败: {e}")

    async def _generate_gap_report(self, gaps, detailed: bool, exchanges: list,
                                 start_date: date, end_date: date) -> dict:
        """生成GAP检测报告"""
        from collections import defaultdict, Counter

        # 基本统计
        total_gaps = len(gaps)
        severity_counts = Counter(gap.severity for gap in gaps)
        exchange_counts = Counter(gap.exchange for gap in gaps)

        # 按股票分组
        gaps_by_stock = defaultdict(list)
        for gap in gaps:
            key = f"{gap.symbol}.{gap.exchange}"
            gaps_by_stock[key].append(gap)

        # 详细信息
        stock_details = {}
        if detailed:
            for stock_key, stock_gaps in gaps_by_stock.items():
                stock_details[stock_key] = {
                    'total_gaps': len(stock_gaps),
                    'total_missing_days': sum(gap.gap_days for gap in stock_gaps),
                    'gaps': [
                        {
                            'start_date': gap.gap_start.isoformat(),
                            'end_date': gap.gap_end.isoformat(),
                            'days': gap.gap_days,
                            'severity': gap.severity,
                            'recommendation': gap.recommendation,
                            'missing_dates': [d.isoformat() for d in gap.missing_dates]
                        }
                        for gap in sorted(stock_gaps, key=lambda x: x.gap_start)
                    ]
                }

        return {
            'detection_info': {
                'exchanges': exchanges,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'detection_time': datetime.now().isoformat()
            },
            'summary': {
                'total_gaps': total_gaps,
                'affected_stocks': len(gaps_by_stock),
                'severity_distribution': dict(severity_counts),
                'exchange_distribution': dict(exchange_counts)
            },
            'stock_details': stock_details if detailed else None,
            'top_affected_stocks': data_manager.get_top_affected_stocks(gaps, 10)
        }

    def _display_gap_report(self, report: dict, detailed: bool):
        """显示GAP检测报告"""
        print("\n" + "="*80)
        print("                     数据缺口检测报告")
        print("="*80)

        # 检测信息
        info = report['detection_info']
        print(f"\n📋 检测信息:")
        print(f"   交易所: {', '.join(info['exchanges'])}")
        print(f"   检测范围: {info['start_date']} 到 {info['end_date']}")
        print(f"   检测时间: {info['detection_time'][:19]}")

        # 摘要统计
        summary = report['summary']
        print(f"\n📊 缺口摘要:")
        print(f"   总缺口数: {summary['total_gaps']:,}")
        print(f"   受影响股票数: {summary['affected_stocks']:,}")

        print(f"\n🎯 严重程度分布:")
        for severity, count in summary['severity_distribution'].items():
            percentage = (count / summary['total_gaps'] * 100) if summary['total_gaps'] > 0 else 0
            print(f"   {severity:8}: {count:6,} ({percentage:5.1f}%)")

        print(f"\n📈 交易所分布:")
        for exchange, count in summary['exchange_distribution'].items():
            percentage = (count / summary['total_gaps'] * 100) if summary['total_gaps'] > 0 else 0
            print(f"   {exchange:6}: {count:6,} ({percentage:5.1f}%)")

        # 最受影响的股票
        if report['top_affected_stocks']:
            print(f"\n🔝 受影响最严重的股票 (前{len(report['top_affected_stocks'])}名):")
            print(f"{'排名':<4} {'股票代码':<12} {'严重分数':<10} {'缺失天数':<8}")
            print("-" * 45) # 调整分隔线长度以匹配新的列宽
            for i, stock in enumerate(report['top_affected_stocks'], 1):
                print(f"{i:<4} {stock['symbol']:<12} {stock['severity_score']:<10.2f} {stock['total_missing_days']:<8}") # 格式化分数和天数

        # 详细信息
        if detailed and report['stock_details']:
            print(f"\n📋 详细股票缺口信息:")
            for stock_key, details in sorted(report['stock_details'].items()):
                if details['total_gaps'] > 0:
                    print(f"\n   📈 {stock_key} (缺口数: {details['total_gaps']}, 缺失天数: {details['total_missing_days']})")

                    # 显示缺失的日期
                    all_missing_dates = []
                    for gap in details['gaps']:
                        all_missing_dates.extend(gap['missing_dates'])

                    if all_missing_dates:
                        # 按日期排序
                        all_missing_dates.sort()

                        # 决定显示哪些日期
                        if len(all_missing_dates) <= 10:
                            # 如果缺失天数在10天以内，显示全部
                            dates_to_show = all_missing_dates
                            show_more = False
                        else:
                            # 如果超过10天，只显示前10天
                            dates_to_show = all_missing_dates[:10]
                            show_more = True

                        # 格式化日期显示
                        date_strs = [d.replace('-', '/') for d in dates_to_show]

                        # 每行显示5个日期
                        for i in range(0, len(date_strs), 5):
                            line_dates = date_strs[i:i+5]
                            print(f"      缺失日期: {', '.join(line_dates)}")

                        if show_more:
                            print(f"      ... 还有 {len(all_missing_dates) - 10} 个缺失日期")

                    # 显示缺口范围信息
                    for gap in details['gaps'][:3]:  # 只显示前3个缺口范围
                        print(f"      缺口范围: {gap['start_date']} 到 {gap['end_date']} "
                              f"({gap['days']}天, {gap['severity']})")
                    if details['total_gaps'] > 3:
                        print(f"      ... 还有 {details['total_gaps'] - 3} 个缺口范围")

        print("\n" + "="*80)

    async def _save_gap_report(self, report: dict, output_file: str):
        """保存GAP报告到文件"""
        import json
        import os

        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)

        except Exception as e:
            dm_logger.error(f"[Main] Failed to save gap report: {e}")
            raise

    def _setup_signal_handlers(self):
        """设置信号处理器"""
        try:
            import signal

            def signal_handler(signum, frame):
                scheduler_logger.info(f"[Main] Received signal {signum}, shutting down...")
                self.running = False

            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)

        except Exception as e:
            scheduler_logger.warning(f"[Main] Failed to setup signal handlers: {e}")


def create_parser():
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description="Quote System - 股票行情数据管理系统",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python main.py scheduler                    # 启动调度器模式
  python main.py api --host 0.0.0.0 --port 8000  # 启动API服务器
  python main.py full --host 0.0.0.0 --port 8000  # 启动完整系统（调度器 + API服务）
  python main.py download --exchanges SSE SZSE --years 2023 2024  # 下载历史数据
  python main.py download --exchanges SZSE --start-date 2024-01-01 --end-date 2024-12-31  # 下载指定日期范围
  python main.py download --instrument-id 000001.SZ --start-date 2024-01-01 --end-date 2024-12-31  # 下载指定股票
  python main.py update --exchanges SSE          # 更新日线数据
  python main.py status                         # 显示系统状态
  python main.py job --job-id daily_data_update  # 运行指定任务
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='可用命令')

    # 调度器模式
    scheduler_parser = subparsers.add_parser('scheduler', help='启动调度器模式')

    # API服务器
    api_parser = subparsers.add_parser('api', help='启动API服务器')
    api_parser.add_argument('--host', default='0.0.0.0', help='监听地址 (默认: 0.0.0.0)')
    api_parser.add_argument('--port', type=int, default=8000, help='监听端口 (默认: 8000)')

    # 完整系统（调度器 + API）
    full_parser = subparsers.add_parser('full', help='启动完整系统（调度器 + API服务）')
    full_parser.add_argument('--host', default='0.0.0.0', help='API监听地址 (默认: 0.0.0.0)')
    full_parser.add_argument('--port', type=int, default=8000, help='API监听端口 (默认: 8000)')

    # 下载历史数据
    download_parser = subparsers.add_parser('download', help='下载历史数据')
    download_parser.add_argument('--exchanges', nargs='+', choices=['SSE', 'SZSE', 'BSE', 'HKEX', 'NASDAQ', 'NYSE'],
                               help='交易所列表')
    download_parser.add_argument('--years', type=int, nargs='+', help='年份列表')
    download_parser.add_argument('--start-date', type=str, help='开始日期 (YYYY-MM-DD)')
    download_parser.add_argument('--end-date', type=str, help='结束日期 (YYYY-MM-DD)')
    download_parser.add_argument('--preset', choices=['a_shares', 'hk_stocks', 'us_stocks', 'mainland', 'overseas', 'chinese', 'global'],
                               help='市场预设组合')
    download_parser.add_argument('--instrument-id', type=str, help='指定股票代码下载 (如: 000001.SZ)')
    download_parser.add_argument('--list-presets', action='store_true', help='显示所有可用的市场预设')
    download_parser.add_argument('--resume', action='store_true', help='续传模式（默认开启，使用--no-resume禁用）')
    download_parser.add_argument('--no-resume', action='store_true', help='重置进度重新下载')
    download_parser.add_argument('--types', nargs='+', choices=['stock', 'index', 'etf'],
                               help='品种类型 (默认使用配置文件 instrument_types)')
    download_parser.set_defaults(resume=True)

    # 更新日线数据
    update_parser = subparsers.add_parser('update', help='更新日线数据')
    update_parser.add_argument('--exchanges', nargs='+', choices=['SSE', 'SZSE', 'BSE', 'HKEX', 'NASDAQ', 'NYSE'],
                             help='交易所列表 (默认: 全部)')
    update_parser.add_argument('--target-date', type=str,
                             help='目标日期 (YYYY-MM-DD)，用于补充历史缺失数据')

    # 显示系统状态
    status_parser = subparsers.add_parser('status', help='显示系统状态')

    # 运行任务
    job_parser = subparsers.add_parser('job', help='运行指定任务')
    job_parser.add_argument('--job-id', required=True, help='任务ID')

    # 交互式下载
    interactive_parser = subparsers.add_parser('interactive', help='交互式下载模式')
    interactive_parser.add_argument('--mode', choices=['market', 'year', 'both'], default='both',
                                  help='交互选择模式')

    # GAP检测
    gap_parser = subparsers.add_parser('gap', help='数据缺口检测')
    gap_parser.add_argument('--exchanges', nargs='+', choices=['SSE', 'SZSE', 'BSE', 'HKEX', 'NASDAQ', 'NYSE'],
                           help='交易所列表 (默认: 全部)')
    gap_parser.add_argument('--start-date', type=str, help='开始日期 (YYYY-MM-DD)')
    gap_parser.add_argument('--end-date', type=str, help='结束日期 (YYYY-MM-DD)')
    gap_parser.add_argument('--severity', choices=['low', 'medium', 'high', 'critical'],
                           help='严重程度过滤')
    gap_parser.add_argument('--output', type=str, help='输出报告文件路径')
    gap_parser.add_argument('--detailed', action='store_true', help='显示详细的股票级别缺口信息')

    return parser


async def main():
    """主函数"""
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        # 创建系统实例
        system = QuoteSystem()

        # 对于需要单实例检查的命令，先检查进程状态
        if args.command in ['full', 'scheduler', 'api']:
            # 在初始化前检查单实例
            if not system.process_manager.check_single_instance("QuoteSystem"):
                scheduler_logger.error(f"[Main] Another instance is already running. Exiting...")
                return

        # 根据命令类型选择初始化方式
        if args.command in ['gap', 'api', 'download', 'update', 'status', 'interactive', 'job']:
            # 轻量级初始化，不需要调度器
            await system.initialize_lightweight()
        else:
            # 完整初始化，包括调度器（scheduler, full命令需要）
            await system.initialize()

        # 执行对应命令
        if args.command == 'scheduler':
            await system.start_scheduler_only()

        elif args.command == 'api':
            await system.start_api_server(
                host=args.host if args.host != "0.0.0.0" else None,
                port=args.port if args.port != 8000 else None
            )

        elif args.command == 'full':
            await system.start_full_system(
                host=args.host if args.host != "0.0.0.0" else None,
                port=args.port if args.port != 8000 else None
            )

        elif args.command == 'download':
            # 处理预设参数
            if args.list_presets:
                await system.list_market_presets()
            else:
                # 处理日期参数
                from datetime import datetime
                start_date = None
                end_date = None

                if args.start_date:
                    try:
                        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
                    except ValueError:
                        print(f"错误: 开始日期格式无效，请使用 YYYY-MM-DD 格式")
                        sys.exit(1)

                if args.end_date:
                    try:
                        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
                    except ValueError:
                        print(f"错误: 结束日期格式无效，请使用 YYYY-MM-DD 格式")
                        sys.exit(1)

                # 检查日期范围是否合理
                if start_date and end_date and start_date > end_date:
                    print(f"错误: 开始日期 {start_date} 不能晚于结束日期 {end_date}")
                    sys.exit(1)

                # 处理续传参数
                resume = args.resume and not args.no_resume

                # 检查是否指定了instrument_id
                if args.instrument_id:
                    # 下载指定股票的数据
                    if not args.start_date or not args.end_date:
                        print("错误: 使用 --instrument-id 时必须指定 --start-date 和 --end-date")
                        sys.exit(1)
                    await system.download_single_instrument(args.instrument_id, start_date, end_date, resume)
                else:
                    # 下载历史数据（原有的逻辑）
                    await system.download_historical_data(args.exchanges, args.years, start_date, end_date, args.preset,
                                                        resume=resume, instrument_types=getattr(args, 'types', None))

        elif args.command == 'update':
            target_date = None
            if hasattr(args, 'target_date') and args.target_date:
                from datetime import datetime as _dt
                try:
                    target_date = _dt.strptime(args.target_date, '%Y-%m-%d').date()
                except ValueError:
                    print(f"错误: 目标日期格式无效 '{args.target_date}'，请使用 YYYY-MM-DD 格式")
                    sys.exit(1)
            await system.update_daily_data(args.exchanges, target_date=target_date)

        elif args.command == 'status':
            await system.show_system_status()

        elif args.command == 'job':
            await system.run_job(args.job_id)

        elif args.command == 'interactive':
            await system.interactive_download(args.mode)

        elif args.command == 'gap':
            await system.detect_data_gaps(args.exchanges, args.start_date, args.end_date,
                                        args.severity, args.output, args.detailed)

        else:
            parser.print_help()

    except KeyboardInterrupt:
        scheduler_logger.info("[Main] Received keyboard interrupt")
    except Exception as e:
        scheduler_logger.error(f"[Main] System error: {e}")
        sys.exit(1)
    finally:
        # 确保在程序退出时关闭所有数据源和连接
        try:
            # 关闭数据源连接
            from data_sources.source_factory import data_source_factory
            if data_source_factory:
                await data_source_factory.close_all()
                scheduler_logger.debug("[Main] All data sources closed on exit")
        except Exception as e:
            scheduler_logger.warning(f"[Main] Failed to close data sources on exit: {e}")

        try:
            # 关闭Telegram连接
            from utils.tgbot import TelegramBot
            # 清理singleton实例
            if hasattr(TelegramBot, '_instance') and TelegramBot._instance:
                try:
                    # 尝试正常关闭连接
                    if hasattr(TelegramBot._instance, 'bot_thon') and TelegramBot._instance.bot_thon:
                        await TelegramBot._instance.bot_thon.disconnect()
                        scheduler_logger.debug("[Main] Telegram connection closed on exit")
                except Exception as e:
                    scheduler_logger.warning(f"[Main] Failed to close Telegram connection: {e}")
                finally:
                    TelegramBot._instance = None
        except Exception as e:
            scheduler_logger.warning(f"[Main] Failed to cleanup Telegram bot: {e}")


if __name__ == "__main__":
    asyncio.run(main())