# encoding:utf-8

from telethon import TelegramClient
from telethon import events
from telethon.errors import FloodError
from telethon.tl.types import MessageMediaWebPage
import asyncio
import io
from typing import List
from utils import tgbot_logger, config_manager
from utils.singleton import singleton

# 读取系统配置
tgbot_logger.info("[tgbot] Reading system configuration...")
config = config_manager
telegram_config = config.get('telegram_config')
intervals = telegram_config.get('intervals', {}) if telegram_config else {}


@singleton
class TelegramBot:
    _instance = None

    def __init__(self):
        self.bot_thon = None


    async def __aenter__(self):
        if not hasattr(self, 'bot_thon') or self.bot_thon is None:
            await self.create_bot_instance()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.bot_thon:
            try:
                tgbot_logger.info("[tgbot] Disconnecting Telegram bot...")
                # 先停止所有事件处理
                if hasattr(self.bot_thon, 'disconnect'):
                    await self.bot_thon.disconnect()
                tgbot_logger.info("[tgbot] Telegram bot disconnected successfully")
            except Exception as e:
                tgbot_logger.error(f"[tgbot] Error during bot disconnect: {e}")
            finally:
                self.bot_thon = None

    async def create_bot_instance(self):
        try:
            tgbot_logger.info("[tgbot] Initializing Telegram-telethon bot with create_bot_instance() method...")

            # 验证配置
            api_id = telegram_config.get('api_id', None)
            api_hash = telegram_config.get('api_hash', None)
            bot_token = telegram_config.get('bot_token', None)
            session_name = telegram_config.get('session_name', 'bot_session')

            # 详细的配置验证日志
            tgbot_logger.debug(f"[tgbot] Telegram configuration validation:")
            tgbot_logger.debug(f"[tgbot]   - api_id: {'configured' if api_id else 'MISSING'}")
            tgbot_logger.debug(f"[tgbot]   - api_hash: {'configured' if api_hash else 'MISSING'}")
            tgbot_logger.debug(f"[tgbot]   - bot_token: {'configured' if bot_token else 'MISSING'}")
            tgbot_logger.debug(f"[tgbot]   - session_name: {session_name}")

            # 检查必要配置
            if not all([api_id, api_hash, bot_token]):
                missing_configs = []
                if not api_id:
                    missing_configs.append("api_id")
                if not api_hash:
                    missing_configs.append("api_hash")
                if not bot_token:
                    missing_configs.append("bot_token")

                error_msg = f"[tgbot] Missing required Telegram configuration: {', '.join(missing_configs)}"
                tgbot_logger.error(error_msg)
                raise ValueError(error_msg)

            tgbot_logger.info(f"[tgbot] Creating TelegramClient with session: {session_name}")

            # 创建客户端实例
            client = TelegramClient(session_name, api_id, api_hash)
            tgbot_logger.debug(f"[tgbot] TelegramClient instance created, attempting to connect...")

            # 连接和认证
            tgbot_logger.info("[tgbot] Connecting to Telegram servers...")
            await client.start(bot_token=bot_token)

            self.bot_thon = client
            tgbot_logger.info(f"[tgbot] Telegram-telethon bot connected and initialized successfully")

            # 测试连接
            try:
                me = await client.get_me()
                tgbot_logger.info(f"[tgbot] Bot identity verified: @{me.username} (ID: {me.id})")
            except Exception as e:
                tgbot_logger.warning(f"[tgbot] Bot identity verification failed: {e}")
                # 连接成功但身份验证失败，这可能是因为使用了错误的token

        except Exception as e:
            tgbot_logger.error(f"[tgbot] Failed to initialize Telegram bot: {e}")
            tgbot_logger.error(f"[tgbot] Error type: {type(e).__name__}")

            # 详细错误分类
            if "Invalid bot token" in str(e):
                tgbot_logger.error("[tgbot] CAUSE: Invalid bot token - please check your bot token configuration")
            elif "flood" in str(e).lower():
                tgbot_logger.error("[tgbot] CAUSE: Flood wait - Telegram rate limit exceeded")
            elif "network" in str(e).lower() or "connection" in str(e).lower():
                tgbot_logger.error("[tgbot] CAUSE: Network connection issue - check internet connectivity")
            elif "timeout" in str(e).lower():
                tgbot_logger.error("[tgbot] CAUSE: Connection timeout - Telegram servers may be unreachable")
            else:
                tgbot_logger.error(f"[tgbot] CAUSE: Unknown error - {str(e)}")

            raise

    def _check_bot_thon(self):
        if not self.bot_thon:
            raise RuntimeError("[tgbot] TelegramClient instance not initialized")

    def on(self, *args, **kwargs):
        self._check_bot_thon()
        return self.bot_thon.on(*args, **kwargs)

    def add_event_handler(self, *args, **kwargs):
        self._check_bot_thon()
        return self.bot_thon.add_event_handler(*args, **kwargs)

    def remove_event_handler(self, *args, **kwargs):
        self._check_bot_thon()
        return self.bot_thon.remove_event_handler(*args, **kwargs)

    def register_command_handler(self, command: str, handler_func):
        """注册命令处理器"""
        self._check_bot_thon()

        @self.bot_thon.on(events.NewMessage(func=lambda e: e.text and e.text.startswith(command)))
        async def command_handler(event):
            try:
                tgbot_logger.debug(f"[tgbot] Received command '{command}' from chat_id: {event.chat_id}")
                await handler_func(event)
                tgbot_logger.debug(f"[tgbot] Command '{command}' processed successfully")
            except Exception as e:
                tgbot_logger.error(f"[tgbot] Error handling command '{command}' from chat_id {event.chat_id}: {e}")
                tgbot_logger.error(f"[tgbot] Command text: {event.text}")
                import traceback
                tgbot_logger.debug(f"[tgbot] Command handler traceback: {traceback.format_exc()}")

        return command_handler

    def register_callback_handler(self, handler_func, pattern: str = None):
        """注册回调查询处理器"""
        self._check_bot_thon()

        if pattern:
            @self.bot_thon.on(events.CallbackQuery(data=pattern))
            async def callback_handler(event):
                try:
                    await handler_func(event)
                except Exception as e:
                    tgbot_logger.error(f"[tgbot] Error handling callback query {pattern}: {e}")
        else:
            @self.bot_thon.on(events.CallbackQuery)
            async def callback_handler(event):
                try:
                    await handler_func(event)
                except Exception as e:
                    tgbot_logger.error(f"[tgbot] Error handling callback query: {e}")

        return callback_handler

    async def send_message_with_keyboard(self, chat_id, message, keyboard=None, parse_mode=None):
        """发送带键盘的消息"""
        self._check_bot_thon()
        tgbot_logger.debug(f"[tgbot] Sending message with keyboard to {chat_id}")

        try:
            # 确保chat_id是整数格式
            if isinstance(chat_id, str):
                if chat_id.startswith('@'):
                    target_chat_id = chat_id
                else:
                    try:
                        target_chat_id = int(chat_id)
                    except ValueError:
                        tgbot_logger.warning(f"[tgbot] Invalid chat_id format: {chat_id}")
                        return
            else:
                target_chat_id = int(chat_id)

            if keyboard:
                return await self.bot_thon.send_message(
                    target_chat_id,
                    message,
                    parse_mode=parse_mode,
                    buttons=keyboard
                )
            else:
                return await self.bot_thon.send_message(
                    target_chat_id,
                    message,
                    parse_mode=parse_mode
                )

        except Exception as e:
            tgbot_logger.error(f"[tgbot] Error sending message with keyboard to {chat_id}: {e}")
            raise

    # 这是自定义的时间控制函数，主要给tg反馈做限时用
    async def wait_for(self, event_type, timeout=None, *args, **kwargs):
        future = asyncio.Future()

        @self.bot_thon.on(event_type(*args, **kwargs))
        async def handler(event):
            future.set_result(event)
            raise events.StopPropagation

        try:
            return await asyncio.wait_for(future, timeout)
        finally:
            self.bot_thon.remove_event_handler(handler)

    async def start(self):
        self._check_bot_thon()
        return await self.bot_thon.start()

    async def disconnect(self):
        """断开Telegram连接并清理资源"""
        if self.bot_thon:
            try:
                tgbot_logger.info("[tgbot] Disconnecting Telegram bot...")
                await self.bot_thon.disconnect()
                tgbot_logger.info("[tgbot] Telegram bot disconnected successfully")
            except Exception as e:
                tgbot_logger.error(f"[tgbot] Error during disconnect: {e}")
            finally:
                self.bot_thon = None
        else:
            tgbot_logger.debug("[tgbot] No active connection to disconnect")

    async def check_connection_health(self) -> bool:
        """检查 TG bot 连接健康状态"""
        try:
            if not self.bot_thon:
                tgbot_logger.warning("[tgbot] Health check: No bot instance available")
                return False

            if not self.bot_thon.is_connected():
                tgbot_logger.warning("[tgbot] Health check: Bot is not connected")
                return False

            # 尝试获取 bot 信息来验证连接
            try:
                me = await self.bot_thon.get_me()
                tgbot_logger.debug(f"[tgbot] Health check: Bot @{me.username} (ID: {me.id}) - connection healthy")
                return True
            except Exception as e:
                tgbot_logger.error(f"[tgbot] Health check: Failed to get bot info: {e}")
                return False

        except Exception as e:
            tgbot_logger.error(f"[tgbot] Health check failed: {e}")
            return False

    async def ensure_connection(self) -> bool:
        """确保 TG bot 连接正常，如果断开则尝试重连"""
        try:
            if await self.check_connection_health():
                return True

            tgbot_logger.info("[tgbot] Connection unhealthy, attempting to reconnect...")

            # 清理现有连接
            if self.bot_thon:
                try:
                    await self.bot_thon.disconnect()
                except:
                    pass
                self.bot_thon = None

            # 重新创建连接
            await self.create_bot_instance()

            # 验证新连接
            return await self.check_connection_health()

        except Exception as e:
            tgbot_logger.error(f"[tgbot] Failed to ensure connection: {e}")
            return False

    async def cleanup(self):
        """清理所有资源"""
        await self.disconnect()

    async def run_until_disconnected(self):
        self._check_bot_thon()
        return await self.bot_thon.run_until_disconnected()

    async def send_message_async(self, chat_id, message):
        self._check_bot_thon()
        tgbot_logger.debug(f"[tgbot] Using send_message_async method to Send message to chat_id: {chat_id}")

        try:
            # 确保chat_id是整数格式
            if isinstance(chat_id, str):
                if chat_id.startswith('@'):
                    # 用户名格式，直接使用
                    target_chat_id = chat_id
                else:
                    # 尝试转换为整数
                    try:
                        target_chat_id = int(chat_id)
                    except ValueError:
                        tgbot_logger.warning(f"[tgbot] Invalid chat_id format: {chat_id}, expected integer or @username")
                        return
            else:
                target_chat_id = int(chat_id)

            await self.bot_thon.send_message(target_chat_id, message)
            tgbot_logger.info(f"[tgbot] Message sent successfully to {target_chat_id}: {message[:30]}...")
        except Exception as e:
            tgbot_logger.error(f"[tgbot] Error sending message to {chat_id}: {e}")
            tgbot_logger.debug(f"[tgbot] Chat_id type: {type(chat_id)}, value: {chat_id}")

    async def get_message_from_channel(self, channel_name, message_count):
        self._check_bot_thon()
        tgbot_logger.debug(f"[tgbot] Using get_message_from_channel method to Fetching message from channel: {channel_name}, the newest {message_count} messages")

        try:
            message = await self.bot_thon.get_messages(channel_name, limit=message_count)
            if message:
                tgbot_logger.info(f"[tgbot] Message fetched successfully: {message.text[:30]}..., sent on: {message.date}")
                return message.text
            else:
                tgbot_logger.warning("[tgbot] Message not found")
                return None
        except Exception as e:
            tgbot_logger.error(f"[tgbot] Error fetching message from channel: {e}")
            return None

    async def respond_message_with_retry(self, event, text_to_send, username):
        self._check_bot_thon()
        tgbot_logger.debug(f"[tgbot] Using respond_message_with_retry method to Respond to {username} with message: {text_to_send}")

        tg_msg_retry_interval = intervals.get('tg_msg_retry_interval', 5)
        tg_msg_retry_times = intervals.get('tg_msg_retry_times', 10)

        for attempt in range(tg_msg_retry_times):
            try:
                tgbot_logger.info(f"[tgbot] Attempting to send message to {username}, attempt {attempt+1}")
                message = await event.respond(text_to_send)
                tgbot_logger.info(f"[tgbot] Message sent successfully on attempt {attempt+1}")
                return message
            except FloodError as e:
                retry_after = e.seconds
                tgbot_logger.warning(f"[tgbot] Flood error! Need to wait {retry_after} seconds. Attempt: {attempt+1}")
                await asyncio.sleep(retry_after)
            except Exception as e:
                tgbot_logger.error(f"[tgbot] Error sending message to {username}: {e}, on attempt {attempt+1}")
                await asyncio.sleep(tg_msg_retry_interval * (attempt+1))
        tgbot_logger.error(f"[tgbot] Failed to send message to {username} after {tg_msg_retry_times} attempts.")
        return None

    async def send_to_channel_with_retry(self, channel_dest_name, msg_media, message):
        self._check_bot_thon()
        tgbot_logger.debug(f"[tgbot] Using send_to_channel_with_retry method to Send message to channel {channel_dest_name}")

        tg_msg_retry_interval = intervals.get('tg_msg_retry_interval', 5)
        tg_msg_retry_times = intervals.get('tg_msg_retry_times', 10)

        for attempt in range(tg_msg_retry_times):
            try:
                if isinstance(msg_media, MessageMediaWebPage):
                    web_page_message = f"{message}\nURL: {msg_media.webpage.url}"
                    await self.bot_thon.send_message(channel_dest_name, web_page_message, parse_mode='md')
                else:
                    await self.bot_thon.send_file(channel_dest_name, file=msg_media, caption=message, parse_mode='md')
                tgbot_logger.info(f"[tgbot] Message sent successfully to channel {channel_dest_name} on attempt {attempt+1}")
                return True
            except FloodError as e:
                retry_after = e.seconds
                tgbot_logger.warning(f"[tgbot] Flood error! Need to wait {retry_after} seconds. Attempt: {attempt+1}")
                await asyncio.sleep(retry_after)
            except Exception as e:
                tgbot_logger.error(f"[tgbot] Error sending message to channel {channel_dest_name}: {e}, on attempt {attempt+1}")
                await asyncio.sleep(tg_msg_retry_interval * (attempt+1))
        tgbot_logger.error(f"[tgbot] Failed to send message to channel {channel_dest_name} after {tg_msg_retry_times} attempts.")
        return False

    async def send_voice_message_with_retry(self, event, response_voice, username):
        self._check_bot_thon()
        tgbot_logger.debug(f"[tgbot] Using send_voice_message_with_retry method to Send voice message to {username}")

        tg_msg_retry_interval = intervals.get('tg_msg_retry_interval', 5)
        tg_msg_retry_times = intervals.get('tg_msg_retry_times', 10)

        for attempt in range(tg_msg_retry_times):
            try:
                with io.BytesIO(response_voice) as audio_file:
                    audio_file.name = 'response.mp3'
                    await self.bot_thon.send_file(event.chat_id, audio_file, voice_note=True)
                tgbot_logger.info(f"[tgbot] Voice message sent successfully to {username} on attempt {attempt+1}")
                return True
            except FloodError as e:
                retry_after = e.seconds
                tgbot_logger.warning(f"[tgbot] Flood error! Need to wait {retry_after} seconds. Attempt: {attempt+1}")
                await asyncio.sleep(retry_after)
            except Exception as e:
                tgbot_logger.error(f"[tgbot] Error sending voice message to {username}: {e}, on attempt {attempt+1}")
                await asyncio.sleep(tg_msg_retry_interval * (attempt+1))
        tgbot_logger.error(f"[tgbot] Failed to send voice message to {username} after {tg_msg_retry_times} attempts.")
        return False

    # ==================== 统一通知方法 ====================

    async def send_notification(self, message: str, prefix: str = None, chat_ids: List[str] = None,
                              level: str = "info", enable_retry: bool = True) -> bool:
        """
        统一的通知发送方法

        Args:
            message: 要发送的消息内容
            prefix: 消息前缀，如 "[定时任务]" 等
            chat_ids: 指定的聊天ID列表，如果不提供则使用配置中的默认列表
            level: 消息级别 (info, warning, error, success)
            enable_retry: 是否启用重试机制

        Returns:
            bool: 是否发送成功
        """
        try:
            # 检查Telegram是否启用
            if not telegram_config.get('enabled', False):
                tgbot_logger.debug("[tgbot] Telegram is disabled, skipping notification")
                return True

            # 添加前缀
            if prefix:
                formatted_message = f"{prefix} {message}"
            else:
                formatted_message = message

            # 添加级别图标
            level_emojis = {
                "info": "ℹ️",
                "warning": "⚠️",
                "error": "❌",
                "success": "✅"
            }
            emoji = level_emojis.get(level, "")
            if emoji:
                formatted_message = f"{emoji} {formatted_message}"

            # 获取聊天ID列表
            if chat_ids is None:
                chat_ids = telegram_config.get('chat_id', [])

            if not chat_ids:
                tgbot_logger.warning("[tgbot] No chat IDs configured for notification")
                return False

            # 发送到所有配置的聊天
            success_count = 0
            failed_count = 0

            for chat_id in chat_ids:
                try:
                    if enable_retry:
                        # 使用重试机制发送
                        if await self._send_with_retry(chat_id, formatted_message):
                            success_count += 1
                        else:
                            failed_count += 1
                    else:
                        # 直接发送
                        await self.send_message_async(chat_id, formatted_message)
                        success_count += 1

                except Exception as e:
                    failed_count += 1
                    tgbot_logger.error(f"[tgbot] Failed to send notification to {chat_id}: {e}")

            # 记录发送结果
            if failed_count == 0:
                tgbot_logger.info(f"[tgbot] Notification sent successfully to {success_count} chats")
                return True
            else:
                tgbot_logger.warning(f"[tgbot] Notification partially sent: {success_count} success, {failed_count} failed")
                return success_count > 0  # 如果有成功的就认为部分成功

        except Exception as e:
            tgbot_logger.error(f"[tgbot] Error in send_notification: {e}")
            return False

    async def send_system_notification(self, message: str, level: str = "info") -> bool:
        """发送系统级通知"""
        return await self.send_notification(
            message=message,
            prefix="[系统通知]",
            level=level
        )

    async def send_task_notification(self, message: str, task_name: str = None, level: str = "info") -> bool:
        """发送任务相关通知"""
        if task_name:
            prefix = f"[定时任务] {task_name}"
        else:
            prefix = "[定时任务]"
        return await self.send_notification(
            message=message,
            prefix=prefix,
            level=level
        )

    async def send_scheduler_notification(self, message: str, level: str = "info") -> bool:
        """发送调度器相关通知"""
        return await self.send_notification(
            message=message,
            prefix="[调度器监控]",
            level=level
        )

    async def send_data_notification(self, message: str, level: str = "info") -> bool:
        """发送数据管理相关通知"""
        return await self.send_notification(
            message=message,
            prefix="[数据管理]",
            level=level
        )

    async def send_report_notification(self, report: dict, report_type: str, level: str = "info") -> bool:
        """
        发送报告类型的通知

        Args:
            report: 报告数据字典
            report_type: 报告类型 ("download_report", "daily_update_report", "system_status", "backup_result")
            level: 消息级别
        """
        try:
            tgbot_logger.debug(f"[tgbot] Sending report notification: type={report_type}, level={level}")

            # 导入新的统一报告系统
            from .report import generate_report

            # 使用统一报告生成器
            message = generate_report(report_type, report, 'telegram')
            tgbot_logger.debug(f"[tgbot] Report formatted successfully, length: {len(message)}")

            result = await self.send_data_notification(message, level)
            if result:
                tgbot_logger.info(f"[tgbot] Report notification sent successfully: type={report_type}")
            return result

        except Exception as e:
            tgbot_logger.error(f"[tgbot] Error formatting report notification: {e}")
            return await self.send_data_notification(f"报告格式化失败: {str(e)}", "error")

    async def _send_with_retry(self, chat_id: str, message: str) -> bool:
        """带重试机制的发送方法"""
        tg_msg_retry_interval = intervals.get('tg_msg_retry_interval', 5)
        tg_msg_retry_times = intervals.get('tg_msg_retry_times', 10)

        for attempt in range(tg_msg_retry_times):
            try:
                await self.send_message_async(chat_id, message)
                return True
            except Exception as e:
                if attempt < tg_msg_retry_times - 1:  # 不是最后一次尝试
                    tgbot_logger.warning(f"[tgbot] Send attempt {attempt+1} failed, retrying in {tg_msg_retry_interval}s: {e}")
                    await asyncio.sleep(tg_msg_retry_interval)
                else:
                    tgbot_logger.error(f"[tgbot] All {tg_msg_retry_times} send attempts failed for {chat_id}: {e}")
                    return False


# 全局辅助函数，避免使用context manager清空单例连接
async def send_report_without_context(report_type: str, report_data: dict, level: str = "info") -> bool:
    """
    发送报告但不使用context manager，避免清空单例的bot_thon连接

    Args:
        report_type: 报告类型
        report_data: 报告数据
        level: 消息级别

    Returns:
        bool: 发送是否成功
    """
    try:
        bot = TelegramBot()
        if not bot.bot_thon:
            await bot.create_bot_instance()
        result = await bot.send_report_notification(report_data, report_type, level)
        return result
    except Exception as e:
        tgbot_logger.error(f"[tgbot] Failed to send report: {e}")
        return False