#!/usr/bin/env python3
"""
Quote System 优雅关闭脚本
用于 systemd 服务的 ExecStop 指令
"""

import asyncio
import signal
import sys
import os

# 添加项目路径
sys.path.append('/home/python/Quote')

from utils import get_process_manager


def force_kill_processes():
    """强制终止遗留进程"""
    try:
        process_manager = get_process_manager()
        status = process_manager.get_service_status()

        killed_count = 0
        for service_name, info in status['pid_files'].items():
            if info['running']:
                try:
                    os.kill(info['pid'], signal.SIGTERM)
                    print(f"发送SIGTERM到进程 {info['pid']} ({service_name})")
                    killed_count += 1
                except OSError:
                    try:
                        os.kill(info['pid'], signal.SIGKILL)
                        print(f"强制终止进程 {info['pid']} ({service_name})")
                        killed_count += 1
                    except OSError:
                        print(f"无法终止进程 {info['pid']} ({service_name})")

        if killed_count > 0:
            print(f"已发送终止信号到 {killed_count} 个进程")

        # 清理锁文件
        import time
        time.sleep(2)  # 等待进程终止

        # 再次清理PID文件
        process_manager.cleanup()

    except Exception as e:
        print(f"强制终止进程时发生错误: {e}")


async def graceful_shutdown():
    """优雅关闭 Quote System"""
    try:
        print("开始优雅关闭 Quote System...")

        # 直接获取进程管理器，无需初始化整个系统
        process_manager = get_process_manager()
        status = process_manager.get_service_status()

        print(f"当前进程状态: PID={status['current_pid']}, 锁状态={status['is_locked']}")

        # 检查并关闭运行的进程
        closed_count = 0
        for service_name, info in status['pid_files'].items():
            if info['running']:
                print(f"正在关闭进程: {service_name} (PID: {info['pid']})")
                try:
                    # 尝试优雅关闭
                    os.kill(info['pid'], signal.SIGTERM)
                    print(f"已发送SIGTERM信号到进程 {info['pid']} ({service_name})")
                    closed_count += 1

                    # 等待进程优雅退出
                    await asyncio.sleep(2)

                    # 检查是否已经退出
                    if process_manager._is_process_running(info['pid']):
                        print(f"进程 {info['pid']} 仍在运行，尝试强制终止...")
                        os.kill(info['pid'], signal.SIGKILL)
                        print(f"已强制终止进程 {info['pid']} ({service_name})")
                except OSError as e:
                    print(f"无法关闭进程 {info['pid']} ({service_name}): {e}")

        # 清理所有PID文件和锁
        process_manager.cleanup()

        print(f'Quote System 已关闭，处理了 {closed_count} 个进程')
        sys.exit(0)

    except Exception as e:
        print(f'关闭过程中发生错误: {e}')

        # 如果优雅关闭失败，尝试强制终止
        print("尝试强制终止残留进程...")
        force_kill_processes()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(graceful_shutdown())