"""
进程管理器
处理单实例检查和PID管理
"""

import os
import sys
import time
import fcntl
import signal
import logging
from pathlib import Path
from typing import Optional, Dict, Any

from utils import scheduler_logger


class ProcessManager:
    """进程管理器 - 处理单实例检查和PID管理"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """初始化进程管理器"""
        self.config = config or {}
        self.pid_file = self.config.get('pid_file', 'log/process.pid')
        self.lock_file = self.config.get('lock_file', 'log/process.lock')
        self.single_instance = self.config.get('single_instance', True)
        self.cleanup_on_startup = self.config.get('cleanup_on_startup', True)
        self.persistent_pids = self.config.get('persistent_pids', {})

        # 确保日志目录存在
        Path(self.pid_file).parent.mkdir(parents=True, exist_ok=True)
        Path(self.lock_file).parent.mkdir(parents=True, exist_ok=True)

        scheduler_logger.info(f"[ProcessManager] Initialized with config: single_instance={self.single_instance}")

    def _write_pid(self, pid_file: str, service_name: str = None) -> None:
        """写入PID文件"""
        try:
            with open(pid_file, 'w') as f:
                f.write(f"{os.getpid()}\n")
                f.write(f"{service_name}\n")
                f.write(f"{time.time()}\n")
            scheduler_logger.debug(f"[ProcessManager] PID {os.getpid()} written to {pid_file}")
        except Exception as e:
            scheduler_logger.error(f"[ProcessManager] Failed to write PID to {pid_file}: {e}")

    def _read_pid(self, pid_file: str) -> Optional[Dict[str, Any]]:
        """读取PID文件"""
        try:
            if not os.path.exists(pid_file):
                return None

            with open(pid_file, 'r') as f:
                lines = f.read().strip().split('\n')
                if len(lines) >= 3:
                    return {
                        'pid': int(lines[0]),
                        'service': lines[1],
                        'timestamp': float(lines[2])
                    }
                else:
                    return {'pid': int(lines[0]), 'service': 'unknown', 'timestamp': time.time()}
        except Exception as e:
            scheduler_logger.error(f"[ProcessManager] Failed to read PID from {pid_file}: {e}")
            return None

    def _is_process_running(self, pid: int) -> bool:
        """检查进程是否仍在运行"""
        try:
            # 发送信号0检查进程是否存在
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def _cleanup_old_pids(self) -> None:
        """清理无效的PID文件"""
        if not self.cleanup_on_startup:
            return

        pid_files = [self.pid_file] + list(self.persistent_pids.values())
        cleaned_count = 0

        for pid_file in pid_files:
            pid_info = self._read_pid(pid_file)
            if pid_info and not self._is_process_running(pid_info['pid']):
                try:
                    os.remove(pid_file)
                    cleaned_count += 1
                    scheduler_logger.info(f"[ProcessManager] Cleaned up stale PID file: {pid_file}")
                except Exception as e:
                    scheduler_logger.error(f"[ProcessManager] Failed to remove stale PID file {pid_file}: {e}")

        if cleaned_count > 0:
            scheduler_logger.info(f"[ProcessManager] Cleaned up {cleaned_count} stale PID files")

    def check_single_instance(self, service_name: str = "QuoteSystem") -> bool:
        """检查单实例，如果已在运行返回False"""
        if not self.single_instance:
            return True

        # 清理旧的PID文件
        self._cleanup_old_pids()

        # 检查锁文件
        if self._is_locked():
            scheduler_logger.warning(f"[ProcessManager] Another instance is running (lock file: {self.lock_file})")
            return False

        # 检查主PID文件
        pid_info = self._read_pid(self.pid_file)
        if pid_info and self._is_process_running(pid_info['pid']):
            if pid_info['service'] == service_name:
                scheduler_logger.warning(
                    f"[ProcessManager] Another instance of {service_name} is already running (PID: {pid_info['pid']})"
                )
                return False

        # 尝试获取锁
        if not self._acquire_lock():
            scheduler_logger.warning("[ProcessManager] Failed to acquire lock, another instance may be starting")
            return False

        # 写入PID文件
        self._write_pid(self.pid_file, service_name)

        # 如果有持久化PID配置，也写入相应的PID文件
        if service_name in self.persistent_pids:
            pid_file = self.persistent_pids[service_name]
            self._write_pid(pid_file, service_name)

        scheduler_logger.info(f"[ProcessManager] {service_name} started with PID {os.getpid()}")
        return True

    def _is_locked(self) -> bool:
        """检查是否有进程锁"""
        try:
            if not os.path.exists(self.lock_file):
                return False

            with open(self.lock_file, 'r') as f:
                lock_pid = int(f.read().strip())

            # 检查锁文件中的进程是否还在运行
            if self._is_process_running(lock_pid):
                return True
            else:
                # 进程不存在，清理锁文件
                os.remove(self.lock_file)
                return False
        except (OSError, ValueError):
            return False

    def _acquire_lock(self) -> bool:
        """获取进程锁"""
        try:
            with open(self.lock_file, 'w') as f:
                f.write(f"{os.getpid()}\n")

                # 尝试获取文件锁
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                scheduler_logger.debug(f"[ProcessManager] Lock acquired for PID {os.getpid()}")
                return True
        except (OSError, IOError):
            return False

    def _release_lock(self) -> None:
        """释放进程锁"""
        try:
            if os.path.exists(self.lock_file):
                with open(self.lock_file, 'r') as f:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                os.remove(self.lock_file)
                scheduler_logger.debug("[ProcessManager] Lock released")
        except Exception as e:
            scheduler_logger.error(f"[ProcessManager] Error releasing lock: {e}")

    def cleanup(self, service_name: str = None) -> None:
        """清理PID文件和锁"""
        try:
            # 释放锁
            self._release_lock()

            # 清理主PID文件
            if os.path.exists(self.pid_file):
                os.remove(self.pid_file)

            # 清理服务特定的PID文件
            if service_name and service_name in self.persistent_pids:
                pid_file = self.persistent_pids[service_name]
                if os.path.exists(pid_file):
                    os.remove(pid_file)

            scheduler_logger.info(f"[ProcessManager] Process {service_name} cleanup completed")
        except Exception as e:
            scheduler_logger.error(f"[ProcessManager] Error during cleanup: {e}")

    def get_service_status(self, service_name: str = None) -> Dict[str, Any]:
        """获取服务状态"""
        status = {
            'current_pid': os.getpid(),
            'is_locked': self._is_locked(),
            'pid_files': {}
        }

        # 检查主PID文件
        main_pid_info = self._read_pid(self.pid_file)
        if main_pid_info:
            status['pid_files']['main'] = {
                'pid': main_pid_info['pid'],
                'service': main_pid_info['service'],
                'timestamp': main_pid_info['timestamp'],
                'running': self._is_process_running(main_pid_info['pid'])
            }

        # 检查持久化PID文件
        for service, pid_file in self.persistent_pids.items():
            pid_info = self._read_pid(pid_file)
            if pid_info:
                status['pid_files'][service] = {
                    'pid': pid_info['pid'],
                    'service': pid_info['service'],
                    'timestamp': pid_info['timestamp'],
                    'running': self._is_process_running(pid_info['pid'])
                }

        return status


# 全局进程管理器实例
_process_manager = None

def get_process_manager(config: Optional[Dict[str, Any]] = None) -> ProcessManager:
    """获取进程管理器实例"""
    global _process_manager
    if _process_manager is None:
        from utils import config_manager
        _process_manager = ProcessManager(config or config_manager.get_nested('sys_config', {}))
    return _process_manager