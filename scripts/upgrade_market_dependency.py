#!/usr/bin/env python
"""行情依赖受控升级：逐包安装 → 冒烟 → 成功 pin / 失败回滚 → 最后重启一次。

    python scripts/upgrade_market_dependency.py --package pytdx --package baostock
    python scripts/upgrade_market_dependency.py --package yfinance==1.8.0
    python scripts/upgrade_akshare.py --target latest

每个包使用 pip install --no-deps。冒烟失败只回滚该包。任一包改过磁盘后，
在全部报告发出之后重启一次 quote-system.service。
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
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.akshare_upgrade import (  # noqa: E402
    build_upgrade_report,
    parse_smoke_summary,
    send_upgrade_notification,
    update_requirements_pin,
)
from utils.dependency_upgrade import (  # noqa: E402
    DEPENDENCY_UPGRADE_SPECS,
    ordered_upgrade_names,
    parse_package_spec,
    upgrade_one_package,
)

LOG_DIR = PROJECT_ROOT / "log"
LOCK_PATH = LOG_DIR / "market_dependency_upgrade.lock"
RESULT_PATH = LOG_DIR / "dependency_upgrade_result.json"
AKSHARE_RESULT_PATH = LOG_DIR / "akshare_upgrade_result.json"
REQUIREMENTS_PATH = PROJECT_ROOT / "requirements.txt"

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("dependency_upgrade")


def _installed_version(distribution: str) -> str:
    return metadata.version(distribution)


def _latest_version(distribution: str, timeout: float = 15.0) -> str:
    url = f"https://pypi.org/pypi/{distribution}/json"
    with urllib.request.urlopen(url, timeout=timeout) as response:
        payload = json.load(response)
    return str(payload.get("info", {}).get("version") or "")


def _run_command(args: List[str], *, timeout: int = 600) -> subprocess.CompletedProcess:
    logger.info("$ %s", " ".join(args))
    return subprocess.run(
        args, capture_output=True, text=True, timeout=timeout, check=False
    )


def _install(distribution: str, version: str, *, reinstall: bool) -> int:
    args = [sys.executable, "-m", "pip", "install", "--no-deps"]
    if reinstall:
        args.append("--force-reinstall")
    args.append(f"{distribution}=={version}")
    process = _run_command(args)
    if process.returncode != 0:
        logger.error("pip failed: %s", (process.stderr or "")[-500:])
    return process.returncode


def _run_smoke(script: Path, extra_args: tuple[str, ...]) -> Dict[str, Any]:
    process = _run_command([sys.executable, str(script), *extra_args])
    output = process.stdout or ""
    logger.info("smoke exit=%s output_tail=%s", process.returncode, output[-500:])
    summary = parse_smoke_summary(output)
    summary["exit_code"] = process.returncode
    return summary


def _restart_service(service: str) -> str:
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


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")


def _notify(package: str, result: Dict[str, Any], restart_status: str) -> None:
    outcome = str(result.get("outcome") or "failed")
    error = None
    if outcome == "failed":
        if result.get("stage") == "install":
            error = f"pip install failed; rollback exit={result.get('rollback_exit')}"
        else:
            error = f"rollback exit={result.get('rollback_exit')}"
    requirements = result.get("requirements") or {}
    report = build_upgrade_report(
        package=package,
        outcome=outcome,
        previous_version=str(result.get("previous_version") or ""),
        target_version=str(result.get("target_version") or ""),
        smoke_summary=result.get("smoke") or {"cases": []},
        restart_status=restart_status,
        requirements_action=requirements.get("action"),
        rollback_smoke_summary=result.get("rollback_smoke"),
        error=error,
    )
    send_upgrade_notification(report, level="success" if outcome == "success" else "error")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--package",
        action="append",
        default=[],
        help="包名，或 name==version；可重复",
    )
    parser.add_argument("--service", default="quote-system.service")
    parser.add_argument("--skip-restart", action="store_true")
    parser.add_argument("--notify-skip", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    requested = [parse_package_spec(item) for item in args.package]
    names = ordered_upgrade_names([name for name, _version in requested if name])
    versions = {name: version for name, version in requested}
    unknown = [name for name in names if name not in DEPENDENCY_UPGRADE_SPECS]
    if unknown or not names:
        logger.error("unknown or empty package list: %s", unknown or args.package)
        return 2

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    lock_handle = open(LOCK_PATH, "w")
    try:
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        logger.warning("another dependency upgrade worker is running, exit")
        return 0

    results: List[Dict[str, Any]] = []
    changed_disk = False
    for name in names:
        spec = DEPENDENCY_UPGRADE_SPECS[name]
        try:
            previous = _installed_version(spec.distribution)
        except metadata.PackageNotFoundError:
            logger.error("%s is not installed, skip upgrade", spec.distribution)
            results.append(
                {
                    "name": name,
                    "outcome": "failed",
                    "stage": "missing",
                    "previous_version": None,
                    "target_version": versions.get(name),
                    "changed_disk": False,
                    "smoke": {"status": "missing", "cases": []},
                }
            )
            continue
        target = versions.get(name)
        if not target:
            try:
                target = _latest_version(spec.distribution)
            except Exception as exc:
                logger.error("cannot resolve %s latest: %s", spec.distribution, exc)
                results.append(
                    {
                        "name": name,
                        "outcome": "failed",
                        "stage": "resolve",
                        "previous_version": previous,
                        "target_version": None,
                        "changed_disk": False,
                        "smoke": {"status": "resolve_failed", "cases": []},
                        "error": str(exc),
                    }
                )
                continue
        if target == previous:
            logger.info("%s already at %s, skip", name, previous)
            skipped = {
                "name": name,
                "outcome": "skipped",
                "previous_version": previous,
                "target_version": target,
                "changed_disk": False,
                "smoke": {"cases": []},
            }
            results.append(skipped)
            if args.notify_skip:
                _notify(name, skipped, "not_required")
            continue

        logger.info("plan: %s %s -> %s (dry_run=%s)", name, previous, target, args.dry_run)
        if args.dry_run:
            results.append(
                {
                    "name": name,
                    "outcome": "dry_run",
                    "previous_version": previous,
                    "target_version": target,
                    "changed_disk": False,
                }
            )
            continue

        smoke_script = PROJECT_ROOT / spec.smoke_script

        def _install_this(version: str, reinstall: bool, distribution=spec.distribution) -> int:
            return _install(distribution, version, reinstall=reinstall)

        def _smoke_this(script=smoke_script, extra=spec.smoke_args) -> Dict[str, Any]:
            return _run_smoke(script, extra)

        def _pin_this(version: str, distribution=spec.distribution) -> Dict[str, Any]:
            return update_requirements_pin(REQUIREMENTS_PATH, distribution, version)

        result = upgrade_one_package(
            name=name,
            distribution=spec.distribution,
            previous_version=previous,
            target_version=target,
            install=_install_this,
            run_smoke=_smoke_this,
            pin=_pin_this,
        )
        try:
            result["installed_version"] = _installed_version(spec.distribution)
        except metadata.PackageNotFoundError:
            result["installed_version"] = None
        results.append(result)
        changed_disk = changed_disk or bool(result.get("changed_disk"))
        logger.info(
            "package %s outcome=%s stage=%s",
            name,
            result.get("outcome"),
            result.get("stage"),
        )

    if args.dry_run:
        print(json.dumps({"packages": results}, ensure_ascii=False))
        return 0

    if not changed_disk:
        restart_status = "not_required"
    elif args.skip_restart:
        restart_status = "not_issued"
    else:
        restart_status = "issued"

    for result in results:
        if result.get("outcome") not in {"success", "failed"}:
            continue
        if result.get("stage") == "missing":
            continue
        if result.get("stage") == "resolve":
            send_upgrade_notification(
                build_upgrade_report(
                    package=str(result.get("name")),
                    outcome="failed",
                    previous_version=str(result.get("previous_version") or ""),
                    target_version="",
                    smoke_summary=result.get("smoke") or {"cases": []},
                    restart_status=restart_status,
                    error=str(result.get("error") or "resolve failed"),
                ),
                level="error",
            )
            continue
        _notify(str(result.get("name")), result, restart_status)

    payload = {
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "restart_status": restart_status,
        "packages": results,
    }
    _write_json(RESULT_PATH, payload)
    akshare_result = next((item for item in results if item.get("name") == "akshare"), None)
    if akshare_result and akshare_result.get("outcome") in {"success", "failed"}:
        _write_json(
            AKSHARE_RESULT_PATH,
            {
                "finished_at": payload["finished_at"],
                "restart_status": restart_status,
                **akshare_result,
            },
        )

    if restart_status == "issued":
        actual = _restart_service(args.service)
        if actual != "issued":
            payload["restart_status"] = actual
            _write_json(RESULT_PATH, payload)
            send_upgrade_notification(
                "服务重启未执行，请手动执行 `sudo systemctl restart quote-system.service`。",
                level="warning",
            )
    failed = [item for item in results if item.get("outcome") == "failed"]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
