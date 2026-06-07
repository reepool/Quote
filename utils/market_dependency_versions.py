"""
Version checks for market-data runtime dependencies.

The checker is intentionally read-only: it compares installed distributions with
PyPI metadata and never installs or upgrades packages.
"""

from __future__ import annotations

import json
import urllib.error
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from importlib import metadata
from typing import Any, Dict, Iterable, List, Optional

from packaging.version import InvalidVersion, Version

from utils.http_transport import HttpTlsConfig, urlopen_bytes


DEFAULT_MARKET_DEPENDENCIES: List[Dict[str, str]] = [
    {"name": "pytdx", "distribution": "pytdx"},
    {"name": "baostock", "distribution": "baostock"},
    {"name": "akshare", "distribution": "akshare"},
    {"name": "akshare_proxy_patch", "distribution": "akshare-proxy-patch"},
    {"name": "yfinance", "distribution": "yfinance"},
    {"name": "curl_cffi", "distribution": "curl_cffi"},
]


@dataclass
class DependencyVersionStatus:
    name: str
    distribution: str
    installed_version: Optional[str]
    latest_version: Optional[str]
    update_available: bool
    package_url: Optional[str] = None
    error: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def check_market_dependency_versions(
    packages: Optional[Iterable[Dict[str, str]]] = None,
    *,
    timeout_sec: float = 10.0,
    pypi_url_template: str = "https://pypi.org/pypi/{distribution}/json",
) -> Dict[str, Any]:
    """Check installed market-data packages against PyPI latest versions."""

    checked_at = datetime.now(timezone.utc).isoformat()
    statuses: List[DependencyVersionStatus] = []

    for item in packages or DEFAULT_MARKET_DEPENDENCIES:
        name = str(item.get("name") or item.get("distribution") or "").strip()
        distribution = str(item.get("distribution") or name).strip()
        if not name or not distribution:
            statuses.append(
                DependencyVersionStatus(
                    name=name or "<invalid>",
                    distribution=distribution or "<invalid>",
                    installed_version=None,
                    latest_version=None,
                    update_available=False,
                    error="invalid dependency config",
                )
            )
            continue

        installed_version = _get_installed_version(distribution)
        try:
            latest_payload = _fetch_pypi_metadata(
                distribution,
                timeout_sec=timeout_sec,
                pypi_url_template=pypi_url_template,
            )
            latest_version = latest_payload.get("version")
            package_url = latest_payload.get("package_url")
            if not latest_version:
                raise ValueError("PyPI metadata missing info.version")

            update_available = (
                installed_version is not None
                and _is_newer_version(latest_version, installed_version)
            )
            error = None if installed_version is not None else "package is not installed"
        except Exception as exc:
            latest_version = None
            package_url = None
            update_available = False
            error = str(exc)

        statuses.append(
            DependencyVersionStatus(
                name=name,
                distribution=distribution,
                installed_version=installed_version,
                latest_version=latest_version,
                update_available=update_available,
                package_url=package_url,
                error=error,
            )
        )

    status_dicts = [status.as_dict() for status in statuses]
    return {
        "checked_at": checked_at,
        "statuses": status_dicts,
        "updates": [item for item in status_dicts if item.get("update_available")],
        "errors": [item for item in status_dicts if item.get("error")],
    }


def format_market_dependency_version_message(result: Dict[str, Any]) -> str:
    """Build a concise Telegram message for updates/errors."""

    updates = result.get("updates") or []
    errors = result.get("errors") or []

    lines: List[str] = []
    if updates:
        lines.append("行情依赖发现可升级版本:")
        for item in updates:
            lines.append(
                f"- {item['name']}: {item.get('installed_version') or '未安装'}"
                f" -> {item.get('latest_version')}"
            )

    if errors:
        if lines:
            lines.append("")
        lines.append("版本检查异常:")
        for item in errors:
            lines.append(f"- {item['name']}: {item.get('error')}")

    if not lines:
        lines.append("行情依赖版本检查完成，未发现可升级模块。")

    lines.append("")
    lines.append("本任务只通知，不会自动升级。")
    return "\n".join(lines)


def _get_installed_version(distribution: str) -> Optional[str]:
    try:
        return metadata.version(distribution)
    except metadata.PackageNotFoundError:
        return None


def _fetch_pypi_metadata(
    distribution: str,
    *,
    timeout_sec: float,
    pypi_url_template: str,
) -> Dict[str, Optional[str]]:
    url = pypi_url_template.format(distribution=distribution)

    try:
        content = urlopen_bytes(
            url,
            timeout_sec=timeout_sec,
            user_agent="QuoteSystem/market-dependency-version-check",
            tls_config=HttpTlsConfig(source_name="pypi"),
        )
        payload = json.loads(content.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"PyPI HTTP {exc.code} for {distribution}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"PyPI request failed for {distribution}: {exc.reason}") from exc

    info = payload.get("info") or {}
    return {
        "version": info.get("version"),
        "package_url": info.get("package_url") or f"https://pypi.org/project/{distribution}/",
    }


def _is_newer_version(latest_version: str, installed_version: str) -> bool:
    try:
        return Version(latest_version) > Version(installed_version)
    except InvalidVersion:
        return latest_version != installed_version
