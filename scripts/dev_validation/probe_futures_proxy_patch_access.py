#!/usr/bin/env python3
"""Probe official futures exchange endpoints with direct and proxy-patch access.

The proxy-patch modes intentionally run in child processes so the patch can be
installed before importing requests. Outputs never include the configured token.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


ROOT_DIR = Path(__file__).resolve().parents[2]
CHILD_ENV = "QUOTE_FUTURES_PROXY_PROBE_MODE"
DEFAULT_DOMAINS = ["www.shfe.com.cn", "www.ine.cn", "www.gfex.com.cn"]
DEFAULT_ENDPOINTS = ["SHFE_SAMPLE", "INE_SAMPLE", "GFEX_DAY_SAMPLE", "GFEX_CALENDAR_SAMPLE"]
SENSITIVE_OUTPUT_KEYS = {"auth_token", "authorization", "cookie", "nid18", "nid18_create_time", "set-cookie", "token"}


GFEX_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    ),
    "Referer": "http://www.gfex.com.cn/gfex/rihq/hqsj_tjsj.shtml",
    "Origin": "http://www.gfex.com.cn",
    "X-Requested-With": "XMLHttpRequest",
}


ENDPOINTS: Dict[str, Dict[str, Any]] = {
    "SHFE_SAMPLE": {
        "method": "GET",
        "url": "https://www.shfe.com.cn/data/tradedata/future/dailydata/kx20170109.dat",
        "headers": {"User-Agent": GFEX_HEADERS["User-Agent"]},
    },
    "INE_SAMPLE": {
        "method": "GET",
        "url": "https://www.ine.cn/data/tradedata/future/dailydata/kx20180326.dat",
        "headers": {"User-Agent": GFEX_HEADERS["User-Agent"]},
    },
    "GFEX_DAY_SAMPLE": {
        "method": "POST",
        "url": "http://www.gfex.com.cn/u/interfacesWebTiDayQuotes/loadList",
        "headers": GFEX_HEADERS,
        "data": {"trade_date": "20240612", "trade_type": "0", "variety": ""},
    },
    "GFEX_CALENDAR_SAMPLE": {
        "method": "POST",
        "url": "http://www.gfex.com.cn/u/interfacesWebTpTradingCalendar/loadList",
        "headers": GFEX_HEADERS,
        "data": {
            "calendar_date_begin": "20240601",
            "calendar_date_end": "20240630",
            "trade_type": "0",
            "variety": "",
        },
    },
}


def _load_proxy_config() -> Dict[str, Any]:
    config_path = ROOT_DIR / "config" / "03_data.json"
    with config_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    return dict(((data.get("data_sources_config") or {}).get("akshare") or {}).get("proxy_patch") or {})


def _mask(value: Any, secrets: Iterable[str]) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, Mapping):
        masked = {}
        for key, item in value.items():
            key_text = str(key)
            if key_text.lower() in SENSITIVE_OUTPUT_KEYS:
                masked[key_text] = "***" if item else ""
            else:
                masked[key_text] = _mask(item, secrets)
        return masked
    if isinstance(value, list):
        return [_mask(item, secrets) for item in value]
    text = _mask_proxy_credentials(str(value))
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return text


def _mask_proxy_credentials(text: str) -> str:
    for scheme in ("http://", "https://", "socks5://"):
        start = text.find(scheme)
        while start >= 0:
            after_scheme = start + len(scheme)
            at_pos = text.find("@", after_scheme)
            if at_pos < 0:
                break
            host_start = at_pos + 1
            end_candidates = [pos for pos in (text.find(",", host_start), text.find('"', host_start), text.find("'", host_start), text.find(" ", host_start)) if pos >= 0]
            end = min(end_candidates) if end_candidates else len(text)
            replacement = f"{scheme}***:***@{text[host_start:end]}"
            text = text[:start] + replacement + text[end:]
            start = text.find(scheme, start + len(replacement))
    return text


def _redacted_error(exc: Exception, token: str) -> str:
    return _mask(f"{type(exc).__name__}: {exc}", [token])


def _gateway_host(gateway: str) -> str:
    parsed = urllib.parse.urlparse(gateway if "://" in gateway else f"http://{gateway}")
    return parsed.hostname or gateway


def _auth_proxy_candidate(config: Mapping[str, Any], *, timeout: float) -> Dict[str, Any]:
    gateway = str(config.get("gateway") or "").strip()
    token = str(config.get("auth_token") or "").strip()
    if not gateway or not token:
        return {"status": "skipped", "reason": "proxy patch gateway/auth_token is not fully configured"}
    host = _gateway_host(gateway)
    params = urllib.parse.urlencode({"token": token, "version": "0.5.0"})
    url = f"http://{host}:47001/api/akshare-auth?{params}"
    try:
        request = urllib.request.Request(url, headers={"User-Agent": GFEX_HEADERS["User-Agent"]})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            text = response.read().decode("utf-8", errors="replace")
        try:
            payload: Any = json.loads(text)
        except json.JSONDecodeError:
            payload = {"raw_text_prefix": text[:500]}
        proxy_url = _find_proxy_url(payload)
        return {
            "status": "success",
            "http_status": 200,
            "response_keys": sorted(payload.keys()) if isinstance(payload, Mapping) else [],
            "response_preview": _mask(payload, [token]),
            "proxy_url": _mask(proxy_url, [token]) if proxy_url else None,
            "_proxy_url_raw": proxy_url,
        }
    except Exception as exc:
        return {"status": "failed", "error": _redacted_error(exc, token)}


def _find_proxy_url(payload: Any) -> Optional[str]:
    if isinstance(payload, str):
        text = payload.strip()
        if text.startswith(("http://", "https://", "socks5://")):
            return text
        return None
    if isinstance(payload, Mapping):
        for key in ("proxy", "proxy_url", "http", "https"):
            value = payload.get(key)
            candidate = _find_proxy_url(value)
            if candidate:
                return candidate
        for value in payload.values():
            candidate = _find_proxy_url(value)
            if candidate:
                return candidate
    if isinstance(payload, list):
        for item in payload:
            candidate = _find_proxy_url(item)
            if candidate:
                return candidate
    return None


def _install_proxy_patch(config: Mapping[str, Any], hook_domains: Sequence[str]) -> Dict[str, Any]:
    token = str(config.get("auth_token") or "").strip()
    gateway = str(config.get("gateway") or "").strip()
    retry = int(config.get("retry", 30))
    domains = [str(item).strip() for item in hook_domains if str(item).strip()]
    try:
        import akshare_proxy_patch

        akshare_proxy_patch.install_patch(
            gateway,
            auth_token=token,
            retry=retry,
            hook_domains=domains,
        )
        return {
            "status": "ready",
            "gateway": gateway,
            "retry": retry,
            "hook_domains": domains,
        }
    except Exception as exc:
        return {"status": "failed", "error": _redacted_error(exc, token), "hook_domains": domains}


def _probe_endpoint(name: str, spec: Mapping[str, Any], *, timeout: float, proxy_url: Optional[str] = None) -> Dict[str, Any]:
    import requests

    method = str(spec["method"]).upper()
    kwargs: Dict[str, Any] = {
        "headers": dict(spec.get("headers") or {}),
        "timeout": timeout,
    }
    if "data" in spec:
        kwargs["data"] = dict(spec["data"])
    if proxy_url:
        kwargs["proxies"] = {"http": proxy_url, "https": proxy_url}
    try:
        response = requests.request(method, str(spec["url"]), **kwargs)
        content_type = response.headers.get("content-type", "")
        text = response.text or ""
        parsed_json = None
        if "json" in content_type.lower() or text.lstrip().startswith(("{", "[")):
            try:
                parsed = response.json()
                parsed_json = {
                    "type": type(parsed).__name__,
                    "keys": sorted(parsed.keys()) if isinstance(parsed, Mapping) else [],
                    "data_len": len(parsed.get("data") or []) if isinstance(parsed, Mapping) else None,
                    "code": parsed.get("code") if isinstance(parsed, Mapping) else None,
                    "msg": parsed.get("msg") if isinstance(parsed, Mapping) else None,
                }
            except Exception:
                parsed_json = None
        return {
            "name": name,
            "url": spec["url"],
            "status": "success" if 200 <= response.status_code < 300 else "http_error",
            "http_status": response.status_code,
            "content_type": content_type,
            "body_prefix": text[:240].replace("\n", " "),
            "json_summary": parsed_json,
        }
    except Exception as exc:
        return {
            "name": name,
            "url": spec["url"],
            "status": "failed",
            "error": f"{type(exc).__name__}: {exc}",
        }


def _run_child(mode: str, args: argparse.Namespace) -> int:
    config = _load_proxy_config()
    token = str(config.get("auth_token") or "").strip()
    domains = _parse_csv(args.hook_domains) or DEFAULT_DOMAINS
    endpoint_names = _parse_csv(args.endpoints) or DEFAULT_ENDPOINTS
    endpoints = {name: ENDPOINTS[name] for name in endpoint_names if name in ENDPOINTS}
    output: Dict[str, Any] = {
        "mode": mode,
        "enabled": bool(config.get("enabled")),
        "gateway": str(config.get("gateway") or ""),
        "hook_domains": domains,
        "endpoints": list(endpoints),
        "proxy_patch": None,
        "auth_probe": None,
        "results": [],
    }

    proxy_url: Optional[str] = None
    if mode == "proxy_patch":
        output["proxy_patch"] = _install_proxy_patch(config, domains)
    elif mode == "manual_proxy":
        auth_probe = _auth_proxy_candidate(config, timeout=args.timeout)
        proxy_url = str(auth_probe.pop("_proxy_url_raw", "") or "") or None
        output["auth_probe"] = auth_probe

    for name, spec in endpoints.items():
        result = _probe_endpoint(name, spec, timeout=args.timeout, proxy_url=proxy_url)
        output["results"].append(_mask(result, [token]))

    print(json.dumps(_mask(output, [token]), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _spawn(mode: str, args: argparse.Namespace) -> Dict[str, Any]:
    env = os.environ.copy()
    env[CHILD_ENV] = mode
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--mode",
        mode,
        "--timeout",
        str(args.timeout),
        "--hook-domains",
        args.hook_domains,
        "--endpoints",
        args.endpoints,
    ]
    completed = subprocess.run(
        command,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    payload: Any
    try:
        payload = json.loads(completed.stdout)
    except Exception:
        payload = completed.stdout
    return {
        "mode": mode,
        "returncode": completed.returncode,
        "stdout": payload,
        "stderr": completed.stderr[-2000:],
    }


def _parse_csv(raw: str) -> List[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["both", "direct", "proxy_patch", "manual_proxy"], default="both")
    parser.add_argument("--timeout", type=float, default=12.0)
    parser.add_argument("--hook-domains", default=",".join(DEFAULT_DOMAINS))
    parser.add_argument("--endpoints", default=",".join(DEFAULT_ENDPOINTS))
    parser.add_argument("--output-path", type=Path, default=None)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    child_mode = os.environ.get(CHILD_ENV)
    if child_mode:
        return _run_child(child_mode, args)

    modes = ["direct", "proxy_patch", "manual_proxy"] if args.mode == "both" else [args.mode]
    result = {"runs": [_spawn(mode, args) for mode in modes]}
    text = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        args.output_path.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if all(item["returncode"] == 0 for item in result["runs"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
