"""
akshare 受控升级支撑：requirements pin 更新与升级结果通知。

升级编排（安装、冒烟、重启、回滚）在 scripts/upgrade_akshare.py；本模块只保留
可单元测试的纯逻辑和独立进程可用的 Telegram HTTP 通知（不走 telethon session，
避免与服务进程争用同一 bot session 文件）。
"""

from __future__ import annotations

import json
import re
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

import logging

logger = logging.getLogger("akshare_upgrade")

REQUIREMENTS_LINE_PATTERN = re.compile(
    r"^(?P<name>[A-Za-z0-9_.\-]+)(?P<extra>\[[^\]]*\])?"
    r"(?P<operator>==|>=|<=|~=|!=|>|<)?(?P<version>[^\s,;#]+)?"
    r"(?P<rest>.*)$"
)


def update_requirements_pin(
    requirements_path: Path, distribution: str, version: str
) -> Dict[str, Any]:
    """把 requirements.txt 中某发行版的版本约束更新为 `distribution==version`。

    只修改首个匹配行：裸名行、已有版本约束行都会被替换为精确 pin；文件中没有
    该依赖时追加到文件末尾。其余行保持原样。
    """
    normalized = distribution.lower().replace("-", "_")
    pin_line = f"{distribution}=={version}"
    lines = requirements_path.read_text(encoding="utf-8").splitlines()
    matched_index: Optional[int] = None
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        match = REQUIREMENTS_LINE_PATTERN.match(stripped)
        if not match:
            continue
        candidate = match.group("name").lower().replace("-", "_")
        if candidate == normalized:
            matched_index = index
            break
    if matched_index is None:
        if lines and lines[-1].strip():
            lines.append(pin_line)
        else:
            lines.append(pin_line)
        action = "appended"
    else:
        comment = ""
        old = lines[matched_index]
        if "#" in old:
            comment = "  #" + old.split("#", 1)[1].rstrip()
        lines[matched_index] = pin_line + comment
        action = "replaced"
    requirements_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"action": action, "pin_line": pin_line}


def parse_smoke_summary(payload: Optional[str]) -> Dict[str, Any]:
    """解析 scripts/akshare_smoke.py 的 JSON 输出，容错空输出。"""
    if not payload:
        return {"status": "missing_output", "passed": 0, "failed": 0, "cases": []}
    try:
        data = json.loads(payload.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return {"status": "invalid_output", "passed": 0, "failed": 0, "cases": []}
    if not isinstance(data, dict):
        return {"status": "invalid_output", "passed": 0, "failed": 0, "cases": []}
    return data


def summarize_smoke(summary: Dict[str, Any]) -> str:
    """冒烟结果的单行摘要，供报告与日志使用。"""
    cases = summary.get("cases") or []
    passed = [case for case in cases if case.get("ok")]
    failed = [case for case in cases if not case.get("ok")]
    parts = [f"passed={len(passed)}", f"failed={len(failed)}"]
    for case in failed:
        parts.append(
            f"{case.get('name')}:{str(case.get('error') or 'unknown')[:80]}"
        )
    return ", ".join(parts)


def build_upgrade_report(
    *,
    outcome: str,
    previous_version: str,
    target_version: str,
    smoke_summary: Dict[str, Any],
    restart_status: str,
    requirements_action: Optional[str] = None,
    rollback_smoke_summary: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> str:
    """组装升级报告文案；outcome ∈ {success, failed, skipped}。"""
    lines = [
        f"akshare 自动升级 {'成功' if outcome == 'success' else ('跳过' if outcome == 'skipped' else '失败')}",
        "",
        f"结果: `{outcome}`",
        f"版本: `{previous_version}` → `{target_version}`",
        f"冒烟: `{summarize_smoke(smoke_summary)}`",
    ]
    if requirements_action:
        lines.append(f"requirements: `{requirements_action}`")
    if restart_status:
        lines.append(f"服务重启: `{restart_status}`")
    if rollback_smoke_summary is not None:
        lines.append(f"回滚后冒烟: `{summarize_smoke(rollback_smoke_summary)}`")
    if error:
        lines.append(f"错误: `{str(error)[:200]}`")
    if outcome == "success" and restart_status == "manual_required":
        lines.extend(["", "请手动执行 `sudo systemctl restart quote-system.service` 使新版本生效。"])
    if outcome == "failed":
        lines.extend(["", "已回滚到旧版本；请结合冒烟失败项人工评估后再升级。"])
    return "\n".join(lines)


def send_upgrade_notification(message: str, *, level: str = "info") -> bool:
    """通过 Bot HTTP API 发送升级通知；独立于服务的 telethon session。

    通知失败只记录日志，绝不影响升级流程本身。
    """
    try:
        from utils import config_manager

        telegram = config_manager.get_telegram_config()
        token = telegram.bot_token
        chat_ids: List[str] = list(telegram.chat_ids or [])
        if not token or not chat_ids:
            logger.warning("[akshare-upgrade] telegram bot_token/chat_ids missing, skip notify")
            return False
        prefix = {"success": "✅", "warning": "⚠️", "error": "❌"}.get(level, "ℹ️")
        sent = False
        for chat_id in chat_ids:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = json.dumps(
                {
                    "chat_id": chat_id,
                    "text": f"{prefix} [依赖升级] {message}",
                    "parse_mode": "Markdown",
                }
            ).encode("utf-8")
            request = urllib.request.Request(
                url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=20) as response:
                    sent = sent or response.status == 200
            except Exception as exc:
                logger.warning("[akshare-upgrade] telegram send failed chat=%s error=%s", chat_id, exc)
        return sent
    except Exception as exc:
        logger.warning("[akshare-upgrade] telegram notify unavailable: %s", exc)
        return False
