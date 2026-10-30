#!/usr/bin/env python
"""akshare 受控升级 worker：升级 → 冒烟 → 报告 → 重启/回滚。

由每日 12:00 的 market_dependency_version_check 任务在检测到新版时派生
（独立会话，升级期间不受服务重启影响），也支持人工执行：

    python scripts/upgrade_akshare.py --target latest
    python scripts/upgrade_akshare.py --target 1.21.0
    python scripts/upgrade_akshare.py --dry-run

流程契约：
  1. 升级使用 `pip install --no-deps akshare==<target>`，绝不联动其他依赖；
  2. 升级后必须通过 scripts/akshare_smoke.py 全部用例才算成功；
  3. 成功：更新 requirements.txt 精确 pin，通知成功，重启 quote-system.service
     （无 polkit 授权时提示手动重启）；
  4. 失败：立即 `--force-reinstall` 回滚到原版本并复跑冒烟，通知失败并说明原因，
     仍然重启服务（进程内存已被换包污染，重启是恢复一致的唯一途径）；
  5. 结果快照写入 log/akshare_upgrade_result.json 供审计。

requirements.txt 的 pin 变更不自动 git 提交，由人工确认后提交。
"""

from __future__ import annotations

import argparse
import fcntl
import json
import logging
import subprocess
import sys
import urllib.request
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.akshare_upgrade import (  # noqa: E402
    build_upgrade_report,
    parse_smoke_summary,
    send_upgrade_notification,
    update_requirements_pin,
)

LOG_DIR = PROJECT_ROOT / "log"
LOCK_PATH = LOG_DIR / "akshare_upgrade.lock"
RESULT_PATH = LOG_DIR / "akshare_upgrade_result.json"
REQUIREMENTS_PATH = PROJECT_ROOT / "requirements.txt"
SMOKE_SCRIPT = PROJECT_ROOT / "scripts" / "akshare_smoke.py"
PYPI_URL_TEMPLATE = "https://pypi.org/pypi/akshare/json"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("akshare_upgrade")


def _installed_version() -> str:
    return metadata.version("akshare")


def _latest_version(timeout: float = 15.0) -> str:
    with urllib.request.urlopen(PYPI_URL_TEMPLATE, timeout=timeout) as response:
        payload = json.load(response)
    return str(payload.get("info", {}).get("version") or "")


def _run_command(args, *, timeout: int = 600) -> subprocess.CompletedProcess:
    logger.info("$ %s", " ".join(args))
    return subprocess.run(
        args, capture_output=True, text=True, timeout=timeout, check=False
    )


def _run_smoke(timeout: int = 600) -> Dict[str, Any]:
    process = _run_command(
        [sys.executable, str(SMOKE_SCRIPT)], timeout=timeout
    )
    output = process.stdout or ""
    logger.info("smoke exit=%s output_tail=%s", process.returncode, output[-500:])
    summary = parse_smoke_summary(output)
    summary["exit_code"] = process.returncode
    return summary


def _smoke_passed(summary: Dict[str, Any]) -> bool:
    return summary.get("status") == "success" and not summary.get("failed")


def _install(version: str, *, reinstall: bool = False) -> subprocess.CompletedProcess:
    args = [sys.executable, "-m", "pip", "install", "--no-deps"]
    if reinstall:
        args.append("--force-reinstall")
    args.append(f"akshare=={version}")
    return _run_command(args)


def _restart_service(service: str) -> str:
    """通过本机既有的 sudoers 白名单发起服务重启；被拒时返回 manual_required。

    python 用户被 sudoers 授权 `NOPASSWD: systemctl restart quote-system.service`
    （见 runbook 第 3 节），因此使用 `sudo -n`：白名单内免密直接放行，白名单
    缺失时立即非零返回而不是挂起等密码。授权路径下 systemd 执行 restart 的
    stop 阶段会终止本 worker（control-group 级杀灭），因此超时意味着重启已在
    执行；重启必须是本 worker 的最后一个动作。
    """
    try:
        process = subprocess.run(
            ["sudo", "-n", "systemctl", "restart", service],
            capture_output=True,
            text=True,
            timeout=8,
            check=False,
        )
        if process.returncode != 0:
            logger.warning(
                "service restart rejected: %s", (process.stderr or "").strip()[:200]
            )
            return "manual_required"
        return "issued"
    except subprocess.TimeoutExpired:
        return "issued"
    except Exception as exc:
        logger.warning("service restart failed: %s", exc)
        return "manual_required"


def _write_result(payload: Dict[str, Any]) -> None:
    payload = {"finished_at": datetime.now(timezone.utc).isoformat(), **payload}
    RESULT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", default="latest", help="目标版本号或 latest")
    parser.add_argument("--service", default="quote-system.service")
    parser.add_argument("--skip-restart", action="store_true", help="不重启服务（人工控制重启时使用）")
    parser.add_argument(
        "--notify-skip",
        action="store_true",
        help="无新版本时也发送 skipped 通知（默认静默）",
    )
    parser.add_argument("--dry-run", action="store_true", help="只打印计划，不执行")
    args = parser.parse_args()

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    lock_handle = open(LOCK_PATH, "w")
    try:
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        logger.warning("another akshare upgrade worker is running, exit")
        return 0

    previous_version = _installed_version()
    try:
        target_version = (
            _latest_version() if args.target == "latest" else args.target.strip()
        )
    except Exception as exc:
        logger.error("cannot resolve target version: %s", exc)
        return 1
    if not target_version:
        logger.error("empty target version")
        return 1

    if target_version == previous_version:
        logger.info("akshare already at %s, nothing to do", previous_version)
        if args.notify_skip:
            _write_result({"outcome": "skipped", "previous_version": previous_version, "target_version": target_version})
            send_upgrade_notification(
                build_upgrade_report(
                    outcome="skipped",
                    previous_version=previous_version,
                    target_version=target_version,
                    smoke_summary={"cases": []},
                    restart_status="not_required",
                ),
                level="info",
            )
        return 0

    logger.info("plan: %s -> %s (dry_run=%s)", previous_version, target_version, args.dry_run)
    if args.dry_run:
        return 0

    install = _install(target_version)
    if install.returncode != 0:
        logger.error("pip install failed: %s", (install.stderr or "")[-500:])
        rollback = _install(previous_version, reinstall=True)
        restart_status = (
            "not_issued" if args.skip_restart else _restart_service(args.service)
        )
        smoke_summary = {"status": "install_failed", "passed": 0, "failed": 0, "cases": []}
        report = build_upgrade_report(
            outcome="failed",
            previous_version=previous_version,
            target_version=target_version,
            smoke_summary=smoke_summary,
            restart_status=restart_status,
            error=f"pip install exit={install.returncode}; rollback exit={rollback.returncode}",
        )
        _write_result(
            {
                "outcome": "failed",
                "stage": "install",
                "previous_version": previous_version,
                "target_version": target_version,
                "install_stderr": (install.stderr or "")[-1000:],
                "rollback_exit": rollback.returncode,
                "restart_status": restart_status,
            }
        )
        send_upgrade_notification(report, level="error")
        return 1

    smoke_summary = _run_smoke()
    if _smoke_passed(smoke_summary):
        pin = update_requirements_pin(REQUIREMENTS_PATH, "akshare", target_version)
        installed_now = _installed_version()
        restart_status = "not_required" if args.skip_restart else _restart_service(args.service)
        _write_result(
            {
                "outcome": "success",
                "previous_version": previous_version,
                "target_version": target_version,
                "installed_version": installed_now,
                "smoke": smoke_summary,
                "requirements": pin,
                "restart_status": restart_status,
            }
        )
        send_upgrade_notification(
            build_upgrade_report(
                outcome="success",
                previous_version=previous_version,
                target_version=installed_now or target_version,
                smoke_summary=smoke_summary,
                restart_status=restart_status,
                requirements_action=pin.get("action"),
            ),
            level="success",
        )
        return 0

    logger.error("smoke failed after upgrade, rolling back to %s", previous_version)
    rollback = _install(previous_version, reinstall=True)
    rollback_smoke = _run_smoke() if rollback.returncode == 0 else {"status": "rollback_failed", "cases": []}
    restart_status = "not_issued" if args.skip_restart else _restart_service(args.service)
    _write_result(
        {
            "outcome": "failed",
            "stage": "smoke",
            "previous_version": previous_version,
            "target_version": target_version,
            "rollback_exit": rollback.returncode,
            "installed_version": _installed_version(),
            "smoke": smoke_summary,
            "rollback_smoke": rollback_smoke,
        }
    )
    send_upgrade_notification(
        build_upgrade_report(
            outcome="failed",
            previous_version=previous_version,
            target_version=target_version,
            smoke_summary=smoke_summary,
            restart_status=restart_status,
            rollback_smoke_summary=rollback_smoke,
            error=f"rollback exit={rollback.returncode}",
        ),
        level="error",
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
