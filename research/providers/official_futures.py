"""Official domestic futures exchange daily-bar provider."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from html.parser import HTMLParser
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import requests

from research.futures_market_data import (
    FuturesBar,
    FuturesContinuousMapping,
    FuturesContract,
    FuturesContractBar,
    FuturesProductSpec,
    FuturesSeries,
    infer_contract_month,
    make_futures_contract_id,
)
from utils.config_manager import ResearchConfig
from utils.http_transport import HttpTlsConfig, create_requests_session, request_get, request_post


OFFICIAL_FUTURES_PARSER_VERSION = "official_futures_daily.v1"
logger = logging.getLogger(__name__)


class OfficialFuturesSourceUnavailable(RuntimeError):
    """Raised when an official source is unsupported, empty, or failed."""


class DceOfficialBrowserClient:
    """Browser-assisted DCE official API client.

    DCE's public JSON endpoints are protected by a JavaScript challenge that
    attaches a dynamic token to in-page fetch calls. Static HTTP clients and
    headless browser modes currently fail in live probes, so this client keeps
    the browser requirement isolated from the normal parser/storage layer.
    """

    base_url = "http://www.dce.com.cn"
    bootstrap_page = "http://www.dce.com.cn/dce/channel/list/168.html"

    def __init__(self, config: Optional[Mapping[str, Any]] = None):
        cfg = dict(config or {})
        self.bootstrap_page = str(cfg.get("bootstrap_page") or self.bootstrap_page)
        self.browser_executable_path = str(
            cfg.get("browser_executable_path")
            or os.environ.get("QUOTE_DCE_CHROME_PATH")
            or _default_dce_chrome_path()
            or ""
        ).strip()
        self.headless = bool(cfg.get("headless", False))
        self.settle_seconds = max(0.0, float(cfg.get("settle_seconds", 9)))
        self.timeout_seconds = max(1.0, float(cfg.get("timeout_seconds", 30)))
        self.retry_attempts = max(1, int(cfg.get("retry_attempts", 3)))
        self.retry_backoff_seconds = max(0.0, float(cfg.get("retry_backoff_seconds", 2)))
        self.virtual_display = cfg.get("virtual_display", "auto")
        self.display_size = tuple(cfg.get("display_size") or (1920, 1080))
        self.browser_args = list(
            cfg.get("browser_args")
            or ["--no-sandbox", "--disable-dev-shm-usage", "--window-size=1920,1080"]
        )
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._browser: Any = None
        self._page: Any = None
        self._display: Any = None

    def fetch_day_quotes_payload(self, trade_date: str) -> Mapping[str, Any]:
        body = {
            "varietyId": "all",
            "tradeDate": _compact_date(trade_date),
            "tradeType": "0",
            "contractId": "",
            "lang": None,
            "optionSeries": "",
            "statisticsType": 0,
        }
        return self._run(self._api("POST", "/dcereport/publicweb/dailystat/dayQuotes", body))

    def fetch_contract_info_payload(self) -> Mapping[str, Any]:
        body = {
            "lang": "zh",
            "tradeType": "1",
            "varietyId": "all",
        }
        return self._run(self._api("POST", "/dcereport/publicweb/tradepara/contractInfo", body))

    def fetch_page_html(self, url: str) -> str:
        """Fetch a DCE official page after the site challenge has run in Chrome."""
        return self._run(self._page_html(url))

    def close(self) -> None:
        if self._loop is None:
            return
        try:
            self._run(self._stop())
        finally:
            self._loop.close()
            self._loop = None
            self._browser = None
            self._page = None
            self._display = None

    def _run(self, coro: Any) -> Any:
        if self._loop is None:
            self._loop = asyncio.new_event_loop()
        return self._loop.run_until_complete(coro)

    async def _ensure_started(self) -> None:
        if self._page is not None:
            return
        try:
            import nodriver as uc
        except Exception as exc:  # pragma: no cover - optional dependency path
            raise OfficialFuturesSourceUnavailable(
                "DCE official browser client requires optional dependency nodriver"
            ) from exc
        self._start_virtual_display_if_needed()
        kwargs: Dict[str, Any] = {
            "headless": self.headless,
            "browser_args": self.browser_args,
            "sandbox": False,
            "no_sandbox": True,
            "lang": "zh-CN",
        }
        if self.browser_executable_path:
            kwargs["browser_executable_path"] = self.browser_executable_path
        try:
            self._browser = await uc.start(**kwargs)
            self._page = await self._browser.get(self.bootstrap_page)
            await self._page.sleep(self.settle_seconds)
            await self._api("GET", "/dcereport/publicweb/maxTradeDate")
        except Exception as exc:
            await self._stop()
            raise OfficialFuturesSourceUnavailable(
                "official DCE browser session failed; install real Chrome or set "
                f"QUOTE_DCE_CHROME_PATH/browser_executable_path: {exc}"
            ) from exc

    async def _api(self, method: str, path: str, body: Optional[Mapping[str, Any]] = None) -> Mapping[str, Any]:
        await self._ensure_started()
        body_js = json.dumps(body, ensure_ascii=False) if body is not None else "null"
        script = f"""
        (async () => {{
          const opt = {{ method: {method!r}, credentials: 'include' }};
          const body = {body_js};
          if (body !== null) {{
            opt.headers = {{ 'Content-Type': 'application/json' }};
            opt.body = JSON.stringify(body);
          }}
          try {{
            const r = await fetch({path!r}, opt);
            const text = await r.text();
            return JSON.stringify({{status: r.status, ok: r.ok, text}});
          }} catch (e) {{
            return JSON.stringify({{status: -1, ok: false, text: String(e)}});
          }}
        }})()
        """
        last_error = ""
        for attempt in range(1, self.retry_attempts + 1):
            raw_result = await self._page.evaluate(script, await_promise=True, return_by_value=True)
            response = json.loads(raw_result if isinstance(raw_result, str) else str(raw_result))
            text = str(response.get("text") or "")
            if int(response.get("status") or -1) == 200:
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError as exc:
                    raise OfficialFuturesSourceUnavailable(
                        f"official DCE {path} returned non-JSON payload: {text[:200]}"
                    ) from exc
                if payload.get("success") is True:
                    return payload
                raise OfficialFuturesSourceUnavailable(
                    f"official DCE {path} business failure: {payload.get('msg') or payload.get('code')}"
                )
            last_error = f"HTTP {response.get('status')}: {text[:200]}"
            if attempt < self.retry_attempts and self.retry_backoff_seconds > 0:
                await self._page.sleep(self.retry_backoff_seconds)
        raise OfficialFuturesSourceUnavailable(f"official DCE {path} request failed: {last_error}")

    async def _page_html(self, url: str) -> str:
        await self._ensure_started()
        last_error = ""
        for attempt in range(1, self.retry_attempts + 1):
            try:
                self._page = await self._browser.get(str(url))
                await self._page.sleep(self.settle_seconds)
                raw_result = await self._page.evaluate(
                    "document.documentElement ? document.documentElement.outerHTML : ''",
                    return_by_value=True,
                )
                html = str(raw_result or "")
                if "<html" in html[:500].lower() or html.strip():
                    return html
                last_error = "empty page html"
            except Exception as exc:
                last_error = str(exc)
            if attempt < self.retry_attempts and self.retry_backoff_seconds > 0:
                await self._page.sleep(self.retry_backoff_seconds)
        raise OfficialFuturesSourceUnavailable(f"official DCE page html request failed url={url}: {last_error}")

    async def _stop(self) -> None:
        if self._browser is not None:
            self._browser.stop()
        self._browser = None
        self._page = None
        if self._display is not None:
            self._display.stop()
        self._display = None

    def _start_virtual_display_if_needed(self) -> None:
        if self._display is not None:
            return
        use_display = self.virtual_display
        if str(use_display).lower() == "auto":
            use_display = not bool(os.environ.get("DISPLAY"))
        if not use_display:
            return
        try:
            from pyvirtualdisplay import Display
        except Exception as exc:  # pragma: no cover - optional dependency path
            raise OfficialFuturesSourceUnavailable(
                "DCE official browser client needs pyvirtualdisplay when no DISPLAY is available"
            ) from exc
        self._display = Display(visible=False, size=self.display_size)
        self._display.start()


@dataclass(frozen=True)
class OfficialFuturesContractBar:
    exchange: str
    trade_date: str
    variety: str
    contract: str
    open: Optional[float]
    high: Optional[float]
    low: Optional[float]
    close: Optional[float]
    settlement: Optional[float]
    volume: Optional[float]
    open_interest: Optional[float]
    amount: Optional[float]
    source_interface: str
    raw_payload: Dict[str, Any] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class OfficialFuturesDailyProbeResult:
    exchange: str
    trade_date: str
    status: str
    is_trading_day: Optional[bool]
    row_count: int
    source_interface: str
    evidence_url: str
    parser_version: str
    payload_hash: str = ""
    failure_reason: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OfficialFuturesFailureClassification:
    category: str
    is_retryable: bool
    suspected_local_ip_risk_control: bool
    summary: str
    evidence: Dict[str, Any] = field(default_factory=dict)


class OfficialFuturesMarketDataProvider:
    """Fetch and normalize first-hand domestic futures exchange daily bars."""

    source_name = "exchange_official"
    parser_version = OFFICIAL_FUTURES_PARSER_VERSION
    supported_exchanges = {"SHFE", "INE", "DCE", "CZCE", "GFEX"}

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
        )
    }
    GFEX_AJAX_HEADERS = {
        **DEFAULT_HEADERS,
        "Referer": "http://www.gfex.com.cn/gfex/rihq/hqsj_tjsj.shtml",
        "Origin": "http://www.gfex.com.cn",
        "X-Requested-With": "XMLHttpRequest",
    }
    GFEX_PRODUCT_PAGE_HEADERS = {
        **DEFAULT_HEADERS,
        "Referer": "http://www.gfex.com.cn/gfex/sspz/redirect_firstChannel.shtml",
    }
    GFEX_PRODUCT_RULE_PAGES = {
        "PT": "http://www.gfex.com.cn/gfex/sspzb/sspz.shtml",
        "PD": "http://www.gfex.com.cn/gfex/sspzp/sspz.shtml",
        "PS": "http://www.gfex.com.cn/gfex/djg/sspz.shtml",
        "LC": "http://www.gfex.com.cn/gfex/tsl/sspz.shtml",
        "SI": "http://www.gfex.com.cn/gfex/gyeg/sspz.shtml",
    }

    def __init__(self, research_config: ResearchConfig):
        self.research_config = research_config
        self.module_cfg = research_config.modules.get("commodity_market_data", {})
        self.source_cfg = self.module_cfg.get("sources", {}).get("exchange_official", {})
        governance_cfg = self.module_cfg.get("trading_day_governance", {})
        backfill_cfg = governance_cfg.get("official_calendar_backfill", {}) if isinstance(governance_cfg, Mapping) else {}
        self.timeout_seconds = float(self.source_cfg.get("timeout_seconds", 20))
        self.retry_attempts = max(1, int(self.source_cfg.get("retry_attempts", 2)))
        self.retry_backoff_seconds = max(0.0, float(self.source_cfg.get("retry_backoff_seconds", 0.5)))
        self.request_interval_seconds = max(0.0, float(self.source_cfg.get("request_interval_seconds", 0.0)))
        interval_by_exchange = self.source_cfg.get("request_interval_seconds_by_exchange", {})
        self.request_interval_seconds_by_exchange = {
            str(exchange).upper(): max(0.0, float(value))
            for exchange, value in interval_by_exchange.items()
            if str(exchange).strip()
        } if isinstance(interval_by_exchange, Mapping) else {}
        challenge_retry_attempts = self.source_cfg.get("challenge_retry_attempts_by_exchange", {})
        self.challenge_retry_attempts_by_exchange = {
            str(exchange).upper(): max(0, int(value))
            for exchange, value in challenge_retry_attempts.items()
            if str(exchange).strip()
        } if isinstance(challenge_retry_attempts, Mapping) else {}
        challenge_backoff = self.source_cfg.get("challenge_backoff_seconds_by_exchange", {})
        self.challenge_backoff_seconds_by_exchange = {
            str(exchange).upper(): max(0.0, float(value))
            for exchange, value in challenge_backoff.items()
            if str(exchange).strip()
        } if isinstance(challenge_backoff, Mapping) else {}
        rate_limit_retry_attempts = self.source_cfg.get("rate_limit_retry_attempts_by_exchange", {})
        self.rate_limit_retry_attempts_by_exchange = {
            str(exchange).upper(): max(0, int(value))
            for exchange, value in rate_limit_retry_attempts.items()
            if str(exchange).strip()
        } if isinstance(rate_limit_retry_attempts, Mapping) else {}
        rate_limit_backoff = self.source_cfg.get("rate_limit_backoff_seconds_by_exchange", {})
        self.rate_limit_backoff_seconds_by_exchange = {
            str(exchange).upper(): max(0.0, float(value))
            for exchange, value in rate_limit_backoff.items()
            if str(exchange).strip()
        } if isinstance(rate_limit_backoff, Mapping) else {}
        batch_pause_every = self.source_cfg.get("batch_pause_every_requests_by_exchange", {})
        self.batch_pause_every_requests_by_exchange = {
            str(exchange).upper(): max(0, int(value))
            for exchange, value in batch_pause_every.items()
            if str(exchange).strip()
        } if isinstance(batch_pause_every, Mapping) else {}
        batch_pause_seconds = self.source_cfg.get("batch_pause_seconds_by_exchange", {})
        self.batch_pause_seconds_by_exchange = {
            str(exchange).upper(): max(0.0, float(value))
            for exchange, value in batch_pause_seconds.items()
            if str(exchange).strip()
        } if isinstance(batch_pause_seconds, Mapping) else {}
        empty_closed_defaults = {
            exchange: "2010-01-01"
            for exchange in self.supported_exchanges
        }
        empty_closed_config = backfill_cfg.get("empty_payload_closed_start_dates", {})
        if isinstance(empty_closed_config, Mapping):
            empty_closed_defaults.update(
                {
                    str(exchange).upper(): _date_key(value)
                    for exchange, value in empty_closed_config.items()
                    if value
                }
            )
        self.empty_payload_closed_start_dates = empty_closed_defaults
        self.dce_browser_cfg = self.source_cfg.get("dce_browser", {}) if isinstance(self.source_cfg.get("dce_browser", {}), dict) else {}
        self.dce_browser_enabled = bool(self.dce_browser_cfg.get("enabled", True))
        self.enabled_exchanges = {
            str(item).upper()
            for item in self.source_cfg.get("enabled_exchanges", sorted(self.supported_exchanges))
        }
        self.tls_config = HttpTlsConfig(source_name=self.source_name)
        self._last_request_started_at = 0.0
        self._request_counts_by_exchange: Dict[str, int] = {}
        self._metrics: Dict[str, Dict[str, float]] = {}
        self._dce_browser_client: Optional[DceOfficialBrowserClient] = None

    def supports_series(self, series: FuturesSeries) -> bool:
        exchange = _series_exchange(series)
        return exchange in self.supported_exchanges and exchange in self.enabled_exchanges

    async def fetch_daily_bars(
        self,
        series: FuturesSeries,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        mode: str = "direct",
    ) -> List[FuturesBar]:
        return await asyncio.to_thread(
            self._fetch_daily_bars_sync,
            series,
            start_date,
            end_date,
            mode,
        )

    async def fetch_daily_artifacts(
        self,
        series: FuturesSeries,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        mode: str = "direct",
    ) -> Dict[str, List[Any]]:
        return await asyncio.to_thread(
            self._fetch_daily_artifacts_sync,
            series,
            start_date,
            end_date,
            mode,
        )

    async def fetch_exchange_contract_bars(
        self,
        exchange: str,
        trade_date: str,
        *,
        mode: str = "direct",
    ) -> List[OfficialFuturesContractBar]:
        """Fetch parsed official contract rows once for an exchange/date."""
        return await asyncio.to_thread(
            self._fetch_exchange_contract_bars_sync,
            exchange,
            trade_date,
            mode,
        )

    def fetch_exchange_contract_bars_sync(
        self,
        exchange: str,
        trade_date: str,
        *,
        mode: str = "direct",
    ) -> List[OfficialFuturesContractBar]:
        """Synchronous variant for CLI diagnostics and backfill preflight."""
        return self._fetch_exchange_contract_bars_sync(exchange, trade_date, mode)

    def build_series_artifacts_from_contract_rows(
        self,
        series: FuturesSeries,
        rows: Sequence[OfficialFuturesContractBar],
        *,
        mode: str = "direct",
    ) -> Dict[str, List[Any]]:
        """Build storage artifacts for one series from pre-fetched exchange rows."""
        if mode != "direct":
            raise OfficialFuturesSourceUnavailable(f"official futures source supports direct mode only: {mode}")
        exchange = _series_exchange(series)
        if not self.supports_series(series):
            raise OfficialFuturesSourceUnavailable(f"official futures source unsupported or disabled for {exchange}")
        variety = _series_variety(series)
        series_rows = [
            row for row in rows
            if row.exchange.upper() == exchange and row.variety.upper() == variety.upper()
        ]
        return self._build_storage_artifacts(series, series_rows, mode=mode)

    def _fetch_exchange_contract_bars_sync(
        self,
        exchange: str,
        trade_date: str,
        mode: str,
    ) -> List[OfficialFuturesContractBar]:
        if mode != "direct":
            raise OfficialFuturesSourceUnavailable(f"official futures source supports direct mode only: {mode}")
        exchange_key = str(exchange or "").upper()
        if exchange_key not in self.supported_exchanges or exchange_key not in self.enabled_exchanges:
            raise OfficialFuturesSourceUnavailable(f"official futures source unsupported or disabled for {exchange_key}")
        session = create_requests_session(tls_config=self.tls_config, headers=self.DEFAULT_HEADERS)
        return self._fetch_exchange_contract_bars(session, exchange_key, _date_key(trade_date))

    def _fetch_daily_bars_sync(
        self,
        series: FuturesSeries,
        start_date: Optional[str],
        end_date: Optional[str],
        mode: str,
    ) -> List[FuturesBar]:
        if mode != "direct":
            raise OfficialFuturesSourceUnavailable(f"official futures source supports direct mode only: {mode}")
        exchange = _series_exchange(series)
        if not self.supports_series(series):
            raise OfficialFuturesSourceUnavailable(f"official futures source unsupported or disabled for {exchange}")
        variety = _series_variety(series)
        start = _date_key(start_date or end_date or _today_key())
        end = _date_key(end_date or start_date or start)
        if start > end:
            raise ValueError("start_date must be earlier than or equal to end_date")
        rows: List[OfficialFuturesContractBar] = []
        session = create_requests_session(tls_config=self.tls_config, headers=self.DEFAULT_HEADERS)
        current = date.fromisoformat(start)
        end_date_obj = date.fromisoformat(end)
        while current <= end_date_obj:
            day_key = current.isoformat()
            rows.extend(self._fetch_exchange_contract_bars(session, exchange, day_key, variety=variety))
            current += timedelta(days=1)
        bars = self._build_storage_artifacts(series, rows, mode=mode)["series_bars"]
        if not bars:
            raise OfficialFuturesSourceUnavailable(
                f"official futures source returned no usable bars for {series.series_id} {start}-{end}"
            )
        return bars

    def _fetch_daily_artifacts_sync(
        self,
        series: FuturesSeries,
        start_date: Optional[str],
        end_date: Optional[str],
        mode: str,
    ) -> Dict[str, List[Any]]:
        if mode != "direct":
            raise OfficialFuturesSourceUnavailable(f"official futures source supports direct mode only: {mode}")
        exchange = _series_exchange(series)
        if not self.supports_series(series):
            raise OfficialFuturesSourceUnavailable(f"official futures source unsupported or disabled for {exchange}")
        variety = _series_variety(series)
        start = _date_key(start_date or end_date or _today_key())
        end = _date_key(end_date or start_date or start)
        if start > end:
            raise ValueError("start_date must be earlier than or equal to end_date")
        rows: List[OfficialFuturesContractBar] = []
        session = create_requests_session(tls_config=self.tls_config, headers=self.DEFAULT_HEADERS)
        current = date.fromisoformat(start)
        end_date_obj = date.fromisoformat(end)
        while current <= end_date_obj:
            day_key = current.isoformat()
            rows.extend(self._fetch_exchange_contract_bars(session, exchange, day_key, variety=variety))
            current += timedelta(days=1)
        artifacts = self._build_storage_artifacts(series, rows, mode=mode)
        if not artifacts["series_bars"]:
            raise OfficialFuturesSourceUnavailable(
                f"official futures source returned no usable bars for {series.series_id} {start}-{end}"
            )
        return artifacts

    def _fetch_exchange_contract_bars(
        self,
        session: requests.Session,
        exchange: str,
        trade_date: str,
        *,
        variety: Optional[str] = None,
    ) -> List[OfficialFuturesContractBar]:
        payload = self._request_exchange_payload(session, exchange, trade_date)
        if exchange == "SHFE":
            rows = self._parse_shfe_payload(payload, trade_date=trade_date, exchange="SHFE")
        elif exchange == "INE":
            rows = self._parse_shfe_payload(payload, trade_date=trade_date, exchange="INE")
        elif exchange == "DCE":
            rows = self._parse_dce_payload(payload, trade_date=trade_date)
        elif exchange == "CZCE":
            rows = self._parse_czce_text(str(payload), trade_date=trade_date)
        elif exchange == "GFEX":
            rows = self._parse_gfex_payload(payload, trade_date=trade_date)
        else:
            raise OfficialFuturesSourceUnavailable(f"unsupported official exchange: {exchange}")
        if variety:
            rows = [row for row in rows if row.variety.upper() == variety.upper()]
        return rows

    def probe_exchange_trading_day(self, exchange: str, trade_date: str) -> OfficialFuturesDailyProbeResult:
        """Classify an exchange/date through the official daily market-data endpoint.

        This is intentionally exchange-level. It lets calendar governance verify
        a trading day once and later reuse the same source interface for all
        varieties without issuing per-series duplicate requests.
        """
        exchange_key = str(exchange or "").upper()
        day_key = _date_key(trade_date)
        if exchange_key not in self.supported_exchanges or exchange_key not in self.enabled_exchanges:
            return OfficialFuturesDailyProbeResult(
                exchange=exchange_key,
                trade_date=day_key,
                status="unresolved",
                is_trading_day=None,
                row_count=0,
                source_interface=_source_interface_for_exchange(exchange_key),
                evidence_url=_official_daily_url(exchange_key, day_key),
                parser_version=self.parser_version,
                failure_reason=f"official futures source unsupported or disabled for {exchange_key}",
            )
        session = create_requests_session(tls_config=self.tls_config, headers=self.DEFAULT_HEADERS)
        try:
            payload = self._request_exchange_payload(session, exchange_key, day_key)
            rows = self._parse_exchange_payload(exchange_key, payload, trade_date=day_key)
            row_count = len(rows)
            if row_count == 0 and not self._can_treat_empty_payload_as_closed(exchange_key, day_key):
                return OfficialFuturesDailyProbeResult(
                    exchange=exchange_key,
                    trade_date=day_key,
                    status="unresolved",
                    is_trading_day=None,
                    row_count=0,
                    source_interface=_source_interface_for_exchange(exchange_key),
                    evidence_url=_official_daily_url(exchange_key, day_key),
                    parser_version=self.parser_version,
                    payload_hash=_hash_payload(payload),
                    failure_reason=(
                        "official empty payload before reliable empty-closed start date "
                        f"{self.empty_payload_closed_start_dates.get(exchange_key)}"
                    ),
                    metadata={
                        "classification_rule": "official_empty_payload_before_reliable_history_start",
                        "empty_payload_closed_start_date": self.empty_payload_closed_start_dates.get(exchange_key),
                    },
                )
            return OfficialFuturesDailyProbeResult(
                exchange=exchange_key,
                trade_date=day_key,
                status="trading" if row_count > 0 else "closed",
                is_trading_day=row_count > 0,
                row_count=row_count,
                source_interface=_source_interface_for_exchange(exchange_key),
                evidence_url=_official_daily_url(exchange_key, day_key),
                parser_version=self.parser_version,
                payload_hash=_hash_payload(payload),
                metadata={"classification_rule": "official_daily_rows" if row_count > 0 else "official_empty_payload"},
            )
        except OfficialFuturesSourceUnavailable as exc:
            classification = classify_official_futures_failure(exc)
            if _is_official_closed_response(exc):
                if not self._can_treat_empty_payload_as_closed(exchange_key, day_key):
                    return OfficialFuturesDailyProbeResult(
                        exchange=exchange_key,
                        trade_date=day_key,
                        status="unresolved",
                        is_trading_day=None,
                        row_count=0,
                        source_interface=_source_interface_for_exchange(exchange_key),
                        evidence_url=_official_daily_url(exchange_key, day_key),
                        parser_version=self.parser_version,
                        failure_reason=(
                            "official no-report response before reliable empty-closed start date "
                            f"{self.empty_payload_closed_start_dates.get(exchange_key)}: {exc}"
                        ),
                        metadata={
                            "classification_rule": "official_no_report_before_reliable_history_start",
                            "empty_payload_closed_start_date": self.empty_payload_closed_start_dates.get(exchange_key),
                            "failure_category": classification.category,
                            "suspected_local_ip_risk_control": classification.suspected_local_ip_risk_control,
                        },
                    )
                return OfficialFuturesDailyProbeResult(
                    exchange=exchange_key,
                    trade_date=day_key,
                    status="closed",
                    is_trading_day=False,
                    row_count=0,
                    source_interface=_source_interface_for_exchange(exchange_key),
                    evidence_url=_official_daily_url(exchange_key, day_key),
                    parser_version=self.parser_version,
                    failure_reason=str(exc),
                    metadata={
                        "classification_rule": "official_no_report_response",
                        "failure_category": classification.category,
                        "suspected_local_ip_risk_control": classification.suspected_local_ip_risk_control,
                    },
                )
            return OfficialFuturesDailyProbeResult(
                exchange=exchange_key,
                trade_date=day_key,
                status="unresolved",
                is_trading_day=None,
                row_count=0,
                source_interface=_source_interface_for_exchange(exchange_key),
                evidence_url=_official_daily_url(exchange_key, day_key),
                parser_version=self.parser_version,
                failure_reason=str(exc),
                metadata={
                    "failure_category": classification.category,
                    "failure_summary": classification.summary,
                    "is_retryable": classification.is_retryable,
                    "suspected_local_ip_risk_control": classification.suspected_local_ip_risk_control,
                },
            )

    def _can_treat_empty_payload_as_closed(self, exchange: str, trade_date: str) -> bool:
        start = self.empty_payload_closed_start_dates.get(str(exchange).upper())
        if not start:
            return True
        return _date_key(trade_date) >= _date_key(start)

    def _parse_exchange_payload(
        self,
        exchange: str,
        payload: Any,
        *,
        trade_date: str,
    ) -> List[OfficialFuturesContractBar]:
        if exchange == "SHFE":
            return self._parse_shfe_payload(payload, trade_date=trade_date, exchange="SHFE")
        if exchange == "INE":
            return self._parse_shfe_payload(payload, trade_date=trade_date, exchange="INE")
        if exchange == "DCE":
            return self._parse_dce_payload(payload, trade_date=trade_date)
        if exchange == "CZCE":
            return self._parse_czce_text(str(payload), trade_date=trade_date)
        if exchange == "GFEX":
            return self._parse_gfex_payload(payload, trade_date=trade_date)
        raise OfficialFuturesSourceUnavailable(f"unsupported official exchange: {exchange}")

    def _request_exchange_payload(
        self,
        session: requests.Session,
        exchange: str,
        trade_date: str,
    ) -> Any:
        last_error: Optional[Exception] = None
        challenge_retry_limit = self._challenge_retry_attempts_for_exchange(exchange)
        rate_limit_retry_limit = self._rate_limit_retry_attempts_for_exchange(exchange)
        max_attempts = self.retry_attempts + challenge_retry_limit + rate_limit_retry_limit + 1
        generic_failures = 0
        challenge_retries = 0
        rate_limit_retries = 0
        for attempt in range(1, max_attempts + 1):
            self._wait_for_request_slot(exchange)
            try:
                if exchange == "SHFE":
                    response = request_get(
                        f"https://www.shfe.com.cn/data/tradedata/future/dailydata/kx{_compact_date(trade_date)}.dat",
                        session=session,
                        tls_config=self.tls_config,
                        timeout=self.timeout_seconds,
                    )
                    response.raise_for_status()
                    return response.json()
                if exchange == "INE":
                    response = request_get(
                        f"https://www.ine.cn/data/tradedata/future/dailydata/kx{_compact_date(trade_date)}.dat",
                        session=session,
                        tls_config=self.tls_config,
                        timeout=self.timeout_seconds,
                    )
                    response.raise_for_status()
                    return response.json()
                if exchange == "DCE":
                    if self.dce_browser_enabled:
                        return self._get_dce_browser_client().fetch_day_quotes_payload(_compact_date(trade_date))
                    return self._request_dce_payload_direct(session, trade_date)
                if exchange == "CZCE":
                    day = _compact_date(trade_date)
                    response = request_get(
                        f"http://www.czce.com.cn/cn/DFSStaticFiles/Future/{day[:4]}/{day}/FutureDataDaily.txt",
                        session=session,
                        tls_config=self.tls_config,
                        timeout=self.timeout_seconds,
                    )
                    response.raise_for_status()
                    response.encoding = response.apparent_encoding or response.encoding
                    return response.text
                if exchange == "GFEX":
                    with create_requests_session(
                        tls_config=self.tls_config,
                        headers=self.GFEX_AJAX_HEADERS,
                    ) as gfex_session:
                        response = request_post(
                            "http://www.gfex.com.cn/u/interfacesWebTiDayQuotes/loadList",
                            session=gfex_session,
                            tls_config=self.tls_config,
                            data={"trade_date": _compact_date(trade_date), "trade_type": "0", "variety": ""},
                            headers=self.GFEX_AJAX_HEADERS,
                            timeout=self.timeout_seconds,
                        )
                    if self._is_challenge_response(response):
                        self._increment_metric(exchange, "challenge_count", 1)
                        logger.warning(
                            "[OfficialFutures] challenge response exchange=%s trade_date=%s attempt=%s http_status=%s content_type=%s",
                            exchange,
                            trade_date,
                            attempt,
                            getattr(response, "status_code", None),
                            (getattr(response, "headers", {}) or {}).get("content-type", ""),
                        )
                        raise OfficialFuturesSourceUnavailable(
                            "gfex_html_challenge "
                            f"http_status={getattr(response, 'status_code', None)} "
                            f"content_type={(getattr(response, 'headers', {}) or {}).get('content-type', '')}"
                        )
                    response.raise_for_status()
                    return response.json()
                raise OfficialFuturesSourceUnavailable(f"unsupported official exchange: {exchange}")
            except Exception as exc:
                last_error = exc
                if self._is_retryable_challenge(exc):
                    if challenge_retries >= challenge_retry_limit:
                        break
                    challenge_retries += 1
                    backoff = self._challenge_backoff_for_exchange(exchange)
                    if backoff > 0:
                        self._increment_metric(exchange, "challenge_backoff_count", 1)
                        self._increment_metric(exchange, "challenge_backoff_seconds", backoff * challenge_retries)
                        logger.warning(
                            "[OfficialFutures] challenge retry backoff exchange=%s trade_date=%s attempt=%s next_attempt=%s sleep_seconds=%s error=%s",
                            exchange,
                            trade_date,
                            attempt,
                            attempt + 1,
                            backoff * challenge_retries,
                            exc,
                        )
                        time.sleep(backoff * challenge_retries)
                    continue
                if self._is_retryable_rate_limit(exc):
                    if rate_limit_retries >= rate_limit_retry_limit:
                        break
                    rate_limit_retries += 1
                    backoff = self._rate_limit_backoff_for_exchange(exchange)
                    if backoff > 0:
                        sleep_seconds = backoff * rate_limit_retries
                        self._increment_metric(exchange, "rate_limit_count", 1)
                        self._increment_metric(exchange, "rate_limit_backoff_seconds", sleep_seconds)
                        logger.warning(
                            "[OfficialFutures] rate-limit retry backoff exchange=%s trade_date=%s attempt=%s next_attempt=%s sleep_seconds=%s error=%s",
                            exchange,
                            trade_date,
                            attempt,
                            attempt + 1,
                            sleep_seconds,
                            exc,
                        )
                        time.sleep(sleep_seconds)
                    continue
                generic_failures += 1
                if generic_failures >= self.retry_attempts:
                    break
                if self.retry_backoff_seconds > 0:
                    self._increment_metric(exchange, "retry_backoff_count", 1)
                    self._increment_metric(exchange, "retry_backoff_seconds", self.retry_backoff_seconds * generic_failures)
                    logger.warning(
                        "[OfficialFutures] request retry backoff exchange=%s trade_date=%s attempt=%s next_attempt=%s sleep_seconds=%s error=%s",
                        exchange,
                        trade_date,
                        attempt,
                        attempt + 1,
                        self.retry_backoff_seconds * generic_failures,
                        exc,
                    )
                    time.sleep(self.retry_backoff_seconds * generic_failures)
        logger.warning(
            "[OfficialFutures] request failed exchange=%s trade_date=%s attempts=%s error=%s",
            exchange,
            trade_date,
            max_attempts,
            last_error,
        )
        raise OfficialFuturesSourceUnavailable(
            f"official {exchange} request failed for {trade_date}: {last_error}"
        )

    def fetch_exchange_product_specs_sync(self, exchange: str) -> Dict[str, FuturesProductSpec]:
        """Fetch exchange-normalized root-product specifications when available."""
        exchange_key = str(exchange or "").upper()
        if exchange_key == "DCE":
            configured = self._configured_product_specs(exchange_key)
            payload = self._get_dce_browser_client().fetch_contract_info_payload()
            contract_specs = self._parse_dce_contract_info_payload(payload)
            try:
                official_pages = self._fetch_dce_product_page_specs()
            except OfficialFuturesSourceUnavailable as exc:
                logger.warning("[OfficialFutures] DCE product page specs unavailable: %s", exc)
                official_pages = {}
            specs = self._merge_product_specs(configured, contract_specs)
            specs = self._merge_product_specs(specs, official_pages)
            if specs:
                return specs
        if exchange_key == "GFEX":
            configured = self._configured_product_specs(exchange_key)
            try:
                official_pages = self._fetch_gfex_product_page_specs()
            except OfficialFuturesSourceUnavailable as exc:
                logger.warning("[OfficialFutures] GFEX product page specs unavailable: %s", exc)
                official_pages = {}
            specs = self._merge_product_specs(configured, official_pages)
            if specs:
                return specs
        raise OfficialFuturesSourceUnavailable(f"unsupported official product spec exchange: {exchange_key}")

    @staticmethod
    def _merge_product_specs(
        base: Mapping[str, FuturesProductSpec],
        override: Mapping[str, FuturesProductSpec],
    ) -> Dict[str, FuturesProductSpec]:
        merged: Dict[str, FuturesProductSpec] = {}
        for symbol in sorted(set(base) | set(override)):
            left = base.get(symbol)
            right = override.get(symbol)
            if left is None and right is not None:
                merged[symbol] = right
                continue
            if right is None and left is not None:
                merged[symbol] = left
                continue
            if left is None or right is None:
                continue
            field_sources: Dict[str, Any] = {}
            for field_name in ("name", "category", "currency", "unit", "contract_multiplier", "tick_size"):
                right_value = getattr(right, field_name)
                left_value = getattr(left, field_name)
                if right_value not in (None, ""):
                    source = (right.field_sources or {}).get(field_name)
                    if source:
                        field_sources[field_name] = source
                elif left_value not in (None, ""):
                    source = (left.field_sources or {}).get(field_name)
                    if source:
                        field_sources[field_name] = source
            evidence = {
                **dict(left.evidence or {}),
                **dict(right.evidence or {}),
                "merged_sources": [
                    item for item in [
                        left.source_profile,
                        right.source_profile,
                    ] if item
                ],
            }
            complete = bool(
                (right.name or left.name)
                and (right.category or left.category)
                and (right.currency or left.currency)
                and (right.unit or left.unit)
            )
            merged[symbol] = FuturesProductSpec(
                exchange=right.exchange or left.exchange,
                symbol=symbol,
                name=right.name or left.name,
                category=right.category or left.category,
                currency=right.currency or left.currency or "CNY",
                unit=right.unit or left.unit,
                contract_multiplier=right.contract_multiplier
                if right.contract_multiplier is not None
                else left.contract_multiplier,
                tick_size=right.tick_size if right.tick_size is not None else left.tick_size,
                source_profile=right.source_profile or left.source_profile,
                source_interface=right.source_interface or left.source_interface,
                source_url=right.source_url or left.source_url,
                quality_flag="official_product_spec_complete" if complete else "official_product_spec_partial",
                evidence=evidence,
                field_sources=field_sources,
            )
        return merged

    def _fetch_gfex_product_page_specs(self) -> Dict[str, FuturesProductSpec]:
        discovery_cfg = self.module_cfg.get("master_data_discovery") or {}
        adapter_cfg = ((discovery_cfg.get("adapters") or {}).get("GFEX") or {})
        configured_pages = adapter_cfg.get("product_rule_pages") or {}
        page_map = {
            **self.GFEX_PRODUCT_RULE_PAGES,
            **{
                str(symbol).upper(): str(url)
                for symbol, url in configured_pages.items()
                if str(symbol).strip() and str(url).strip()
            },
        }
        specs: Dict[str, FuturesProductSpec] = {}
        failures: Dict[str, str] = {}
        with create_requests_session(tls_config=self.tls_config, headers=self.GFEX_PRODUCT_PAGE_HEADERS) as session:
            for symbol, url in sorted(page_map.items()):
                try:
                    html = self._request_gfex_product_rule_page(session, url, symbol)
                    spec = self._parse_gfex_product_page_html(html, symbol=symbol, source_url=url)
                except Exception as exc:
                    failures[symbol] = str(exc)
                    continue
                if spec.symbol:
                    specs[spec.symbol] = spec
        if failures:
            logger.warning("[OfficialFutures] GFEX product page spec partial failures=%s", failures)
        return specs

    def _fetch_dce_product_page_specs(self) -> Dict[str, FuturesProductSpec]:
        discovery_cfg = self.module_cfg.get("master_data_discovery") or {}
        adapter_cfg = ((discovery_cfg.get("adapters") or {}).get("DCE") or {})
        configured_pages = adapter_cfg.get("product_rule_pages") or {}
        if not isinstance(configured_pages, Mapping) or not configured_pages:
            return {}
        specs: Dict[str, FuturesProductSpec] = {}
        failures: Dict[str, str] = {}
        client = self._get_dce_browser_client()
        for symbol, url in sorted(configured_pages.items()):
            symbol_key = str(symbol or "").upper().strip()
            if not symbol_key or not str(url or "").strip():
                continue
            try:
                html = client.fetch_page_html(str(url))
                spec = self._parse_dce_product_page_html(html, symbol=symbol_key, source_url=str(url))
            except Exception as exc:
                failures[symbol_key] = str(exc)
                continue
            if spec.symbol:
                specs[spec.symbol] = spec
        if failures:
            logger.warning("[OfficialFutures] DCE product page spec partial failures=%s", failures)
        return specs

    def _request_gfex_product_rule_page(self, session: requests.Session, url: str, symbol: str) -> str:
        last_error: Optional[Exception] = None
        challenge_retry_limit = self._challenge_retry_attempts_for_exchange("GFEX")
        max_attempts = self.retry_attempts + challenge_retry_limit + 1
        generic_failures = 0
        challenge_retries = 0
        for attempt in range(1, max_attempts + 1):
            self._wait_for_request_slot("GFEX")
            try:
                response = request_get(
                    url,
                    session=session,
                    tls_config=self.tls_config,
                    headers=self.GFEX_PRODUCT_PAGE_HEADERS,
                    timeout=self.timeout_seconds,
                )
                if getattr(response, "status_code", None) == 567:
                    self._increment_metric("GFEX", "challenge_count", 1)
                    raise OfficialFuturesSourceUnavailable(
                        f"gfex_product_page_challenge symbol={symbol} http_status=567"
                    )
                response.raise_for_status()
                response.encoding = response.apparent_encoding or response.encoding
                return response.text
            except Exception as exc:
                last_error = exc
                if "gfex_product_page_challenge" in str(exc).lower():
                    if challenge_retries >= challenge_retry_limit:
                        break
                    challenge_retries += 1
                    backoff = self._challenge_backoff_for_exchange("GFEX")
                    if backoff > 0:
                        sleep_seconds = backoff * challenge_retries
                        self._increment_metric("GFEX", "challenge_backoff_count", 1)
                        self._increment_metric("GFEX", "challenge_backoff_seconds", sleep_seconds)
                        time.sleep(sleep_seconds)
                    continue
                generic_failures += 1
                if generic_failures >= self.retry_attempts:
                    break
                if self.retry_backoff_seconds > 0:
                    sleep_seconds = self.retry_backoff_seconds * generic_failures
                    self._increment_metric("GFEX", "retry_backoff_count", 1)
                    self._increment_metric("GFEX", "retry_backoff_seconds", sleep_seconds)
                    time.sleep(sleep_seconds)
        raise OfficialFuturesSourceUnavailable(
            f"official GFEX product page request failed symbol={symbol} url={url}: {last_error}"
        )

    def _parse_dce_product_page_html(self, html: str, *, symbol: str, source_url: str) -> FuturesProductSpec:
        texts = _html_text_chunks(html)
        field_map = {
            "product_name": _field_after_label(texts, ("交易品种", "品种名称")),
            "trade_unit": _field_after_label(texts, ("交易单位", "合约单位")),
            "quote_unit": _field_after_label(texts, ("报价单位",)),
            "tick_size": _field_after_label(texts, ("最小变动价位", "最小变动单位")),
            "trade_code": _field_after_label(texts, ("交易代码", "合约代码")),
        }
        product_symbol = str(field_map.get("trade_code") or symbol or "").strip().upper()
        product_symbol = re.sub(r"[^A-Z]", "", product_symbol) or str(symbol or "").upper()
        name = str(field_map.get("product_name") or "").strip()
        quote_unit_text = str(field_map.get("quote_unit") or "").strip()
        unit = _normalize_domestic_quote_unit(quote_unit_text)
        currency = "CNY" if unit.startswith("CNY/") or "人民币" in quote_unit_text or "元" in quote_unit_text else ""
        multiplier = _first_number(field_map.get("trade_unit"))
        tick_size = _first_number(field_map.get("tick_size"))
        field_sources = {
            field_name: {
                "source_type": "official_product_rule_page",
                "source_ref": source_url,
                "quality_flag": "official_product_rule_page",
            }
            for field_name, value in (
                ("name", name),
                ("currency", currency),
                ("unit", unit),
                ("contract_multiplier", multiplier),
                ("tick_size", tick_size),
            )
            if value not in (None, "")
        }
        return FuturesProductSpec(
            exchange="DCE",
            symbol=product_symbol,
            name=name,
            currency=currency or "CNY",
            unit=unit,
            contract_multiplier=multiplier,
            tick_size=tick_size,
            source_profile="exchange_official_product_rule_page",
            source_interface="official_dce_product_page",
            source_url=source_url,
            quality_flag="official_product_spec_partial",
            evidence={
                "field_map": {key: value for key, value in field_map.items() if value},
                "source_limitations": [
                    "dce_product_page_does_not_provide_project_category",
                ],
            },
            field_sources=field_sources,
        )

    def _parse_gfex_product_page_html(self, html: str, *, symbol: str, source_url: str) -> FuturesProductSpec:
        texts = _html_text_chunks(html)
        field_map = {
            "product_name": _field_after_label(texts, ("交易品种",)) or _gfex_meta_content(html, "ColumnName"),
            "trade_unit": _field_after_label(texts, ("交易单位",)),
            "quote_unit": _field_after_label(texts, ("报价单位",)),
            "tick_size": _field_after_label(texts, ("最小变动价位",)),
            "trade_code": _field_after_label(texts, ("交易代码",)),
        }
        product_symbol = str(field_map.get("trade_code") or symbol or "").strip().upper()
        product_symbol = re.sub(r"[^A-Z]", "", product_symbol) or str(symbol or "").upper()
        name = str(field_map.get("product_name") or "").strip()
        quote_unit_text = str(field_map.get("quote_unit") or "").strip()
        unit = _normalize_domestic_quote_unit(quote_unit_text)
        currency = "CNY" if unit.startswith("CNY/") or "人民币" in quote_unit_text or "元" in quote_unit_text else ""
        multiplier = _first_number(field_map.get("trade_unit"))
        tick_size = _first_number(field_map.get("tick_size"))
        source_ref = source_url
        field_sources = {
            field_name: {
                "source_type": "official_product_rule_page",
                "source_ref": source_ref,
                "quality_flag": "official_product_rule_page",
            }
            for field_name, value in (
                ("name", name),
                ("currency", currency),
                ("unit", unit),
                ("contract_multiplier", multiplier),
                ("tick_size", tick_size),
            )
            if value not in (None, "")
        }
        return FuturesProductSpec(
            exchange="GFEX",
            symbol=product_symbol,
            name=name,
            currency=currency or "CNY",
            unit=unit,
            contract_multiplier=multiplier,
            tick_size=tick_size,
            source_profile="exchange_official_product_rule_page",
            source_interface="official_gfex_product_page",
            source_url=source_url,
            quality_flag="official_product_spec_partial",
            evidence={
                "field_map": {key: value for key, value in field_map.items() if value},
                "source_limitations": [
                    "gfex_product_page_does_not_provide_project_category",
                ],
            },
            field_sources=field_sources,
        )

    def _configured_product_specs(self, exchange: str) -> Dict[str, FuturesProductSpec]:
        """Return governed product-rule metadata as normalized spec seed evidence."""
        exchange_key = str(exchange or "").upper()
        discovery_cfg = self.module_cfg.get("master_data_discovery") or {}
        adapter_cfg = ((discovery_cfg.get("adapters") or {}).get(exchange_key) or {})
        known_products = adapter_cfg.get("known_products") or {}
        if not isinstance(known_products, Mapping):
            return {}
        specs: Dict[str, FuturesProductSpec] = {}
        for symbol, payload in known_products.items():
            if not isinstance(payload, Mapping):
                continue
            symbol_key = str(symbol or "").upper()
            if not symbol_key:
                continue
            source_ref = str(payload.get("source_url") or "config/11_futures.json")
            field_sources = {
                field_name: {
                    "source_type": "governed_rule_metadata",
                    "source_ref": source_ref,
                    "quality_flag": "governed_rule_verified",
                }
                for field_name in ("name", "category", "currency", "unit")
                if payload.get(field_name) not in (None, "")
            }
            specs[symbol_key] = FuturesProductSpec(
                exchange=exchange_key,
                symbol=symbol_key,
                name=str(payload.get("name") or ""),
                category=str(payload.get("category") or ""),
                currency=str(payload.get("currency") or "CNY"),
                unit=str(payload.get("unit") or ""),
                contract_multiplier=_number(payload.get("contract_multiplier")),
                tick_size=_number(payload.get("tick_size")),
                source_profile="governed_product_rule_metadata",
                source_interface="config_11_futures_master_data_discovery",
                source_url=source_ref,
                quality_flag="governed_rule_verified",
                evidence={
                    "source_limitations": [
                        "governed_local_rule_metadata_not_raw_official_payload",
                    ],
                },
                field_sources=field_sources,
            )
        return specs

    def _parse_dce_contract_info_payload(self, payload: Mapping[str, Any]) -> Dict[str, FuturesProductSpec]:
        rows = payload.get("data") or []
        specs: Dict[str, FuturesProductSpec] = {}
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            contract = str(row.get("contractId") or "").strip().upper()
            symbol = str(row.get("varietyOrder") or "").strip().upper()
            if not symbol:
                symbol = _contract_variety(contract)
            if not symbol:
                continue
            name = str(row.get("variety") or "").strip()
            if "小计" in name or "总计" in name:
                continue
            multiplier = _number(row.get("unit"))
            tick = _number(row.get("tick"))
            existing = specs.get(symbol)
            evidence_rows = list((existing.evidence or {}).get("sample_rows") or []) if existing else []
            if len(evidence_rows) < 3:
                evidence_rows.append({
                    key: row.get(key)
                    for key in (
                        "contractId",
                        "variety",
                        "varietyOrder",
                        "unit",
                        "tick",
                        "startTradeDate",
                        "endTradeDate",
                        "endDeliveryDate",
                    )
                    if key in row
                })
            specs[symbol] = FuturesProductSpec(
                exchange="DCE",
                symbol=symbol,
                name=name or (existing.name if existing else ""),
                currency="CNY",
                contract_multiplier=(
                    multiplier
                    if multiplier is not None
                    else (existing.contract_multiplier if existing else None)
                ),
                tick_size=tick if tick is not None else (existing.tick_size if existing else None),
                source_profile="exchange_official_product_spec",
                source_interface="official_dce_contract_info",
                source_url="http://www.dce.com.cn/dcereport/publicweb/tradepara/contractInfo",
                quality_flag="official_product_spec_partial",
                evidence={
                    "source_limitations": [
                        "dce_contract_info_unit_is_contract_trading_unit_not_quote_unit",
                        "dce_contract_info_does_not_provide_project_category",
                    ],
                    "sample_rows": evidence_rows,
                },
            )
        return specs

    def _request_dce_payload_direct(self, session: requests.Session, trade_date: str) -> Any:
        response = request_post(
            "http://www.dce.com.cn/dcereport/publicweb/dailystat/dayQuotes",
            session=session,
            tls_config=self.tls_config,
            json={
                "contractId": "",
                "lang": None,
                "optionSeries": "",
                "statisticsType": 0,
                "tradeDate": _compact_date(trade_date),
                "tradeType": "0",
                "varietyId": "all",
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, Mapping) and payload.get("success") is False:
            raise OfficialFuturesSourceUnavailable(
                "official DCE /dcereport/publicweb/dailystat/dayQuotes "
                f"business failure: {payload.get('msg') or payload.get('code')}"
            )
        return payload

    def _get_dce_browser_client(self) -> "DceOfficialBrowserClient":
        if self._dce_browser_client is None:
            self._dce_browser_client = DceOfficialBrowserClient(self.dce_browser_cfg)
        return self._dce_browser_client

    def close(self) -> None:
        if self._dce_browser_client is not None:
            self._dce_browser_client.close()
            self._dce_browser_client = None

    def snapshot_metrics(self) -> Dict[str, Dict[str, float]]:
        return {exchange: dict(values) for exchange, values in self._metrics.items()}

    def _increment_metric(self, exchange: str, key: str, value: float) -> None:
        exchange_key = str(exchange or "").upper()
        metrics = self._metrics.setdefault(exchange_key, {})
        metrics[key] = metrics.get(key, 0.0) + value

    def _construct_main_series_bars(
        self,
        series: FuturesSeries,
        rows: Sequence[OfficialFuturesContractBar],
        *,
        mode: str,
    ) -> List[FuturesBar]:
        return self._build_storage_artifacts(series, rows, mode=mode)["series_bars"]

    def _build_storage_artifacts(
        self,
        series: FuturesSeries,
        rows: Sequence[OfficialFuturesContractBar],
        *,
        mode: str,
    ) -> Dict[str, List[Any]]:
        by_date: Dict[str, List[OfficialFuturesContractBar]] = {}
        for row in rows:
            by_date.setdefault(row.trade_date, []).append(row)
        bars: List[FuturesBar] = []
        contracts_by_id: Dict[str, FuturesContract] = {}
        contract_bars: List[FuturesContractBar] = []
        mappings: List[FuturesContinuousMapping] = []
        for row in rows:
            contract_id = make_futures_contract_id(series.instrument_id, row.contract)
            quality = "ok" if not row.warnings else "partial"
            contracts_by_id[contract_id] = FuturesContract(
                contract_id=contract_id,
                instrument_id=series.instrument_id,
                exchange=row.exchange,
                exchange_contract_code=row.contract,
                contract_month=infer_contract_month(row.contract),
                delivery_month=infer_contract_month(row.contract),
                currency=series.currency or "CNY",
                unit=series.unit,
                active=True,
                source=self.source_name,
                quality_flag=quality,
                metadata={"variety": row.variety, "warnings": row.warnings},
            )
            contract_bars.append(
                FuturesContractBar(
                    contract_id=contract_id,
                    instrument_id=series.instrument_id,
                    trade_date=row.trade_date,
                    open=row.open,
                    high=row.high,
                    low=row.low,
                    close=row.close,
                    settlement=row.settlement,
                    volume=row.volume,
                    open_interest=row.open_interest,
                    amount=row.amount,
                    currency=series.currency or "CNY",
                    unit=series.unit,
                    source=self.source_name,
                    source_mode=mode,
                    source_profile="exchange_official",
                    source_interface=row.source_interface,
                    parser_version=self.parser_version,
                    quality_flag=quality,
                    raw_payload_hash=_hash_payload(
                        {
                            "contract_id": contract_id,
                            "trade_date": row.trade_date,
                            "raw": row.raw_payload,
                        }
                    ),
                    metadata={"exchange_contract_code": row.contract, "variety": row.variety, "warnings": row.warnings},
                )
            )
        for trade_date, date_rows in sorted(by_date.items()):
            selected = _select_main_contract(date_rows)
            selected_contract_id = make_futures_contract_id(series.instrument_id, selected.contract)
            metadata = {
                "underlying_contract": selected.contract,
                "underlying_contract_id": selected_contract_id,
                "variety": selected.variety,
                "exchange": selected.exchange,
                "construction_method": "official_open_interest_main",
                "construction_version": "futures_continuous_mapping.v1",
                "selection_open_interest": selected.open_interest,
                "selection_volume": selected.volume,
                "warnings": selected.warnings,
                "raw_payload": selected.raw_payload,
            }
            quality = "ok" if not selected.warnings else "partial"
            bars.append(
                FuturesBar(
                    series_id=series.series_id,
                    trade_date=trade_date,
                    open=selected.open,
                    high=selected.high,
                    low=selected.low,
                    close=selected.close,
                    settlement=selected.settlement,
                    volume=selected.volume,
                    open_interest=selected.open_interest,
                    amount=selected.amount,
                    currency=series.currency or "CNY",
                    unit=series.unit,
                    source=self.source_name,
                    source_mode=mode,
                    source_profile="exchange_official",
                    source_interface=selected.source_interface,
                    parser_version=self.parser_version,
                    quality_flag=quality,
                    raw_payload_hash=_hash_payload(
                        {
                            "series_id": series.series_id,
                            "trade_date": trade_date,
                            "selected_contract": selected.contract,
                            "raw": selected.raw_payload,
                        }
                    ),
                    metadata=metadata,
                )
            )
            mappings.append(
                FuturesContinuousMapping(
                    series_id=series.series_id,
                    trade_date=trade_date,
                    contract_id=selected_contract_id,
                    exchange_contract_code=selected.contract,
                    instrument_id=series.instrument_id,
                    construction_method="official_open_interest_main",
                    construction_version="futures_continuous_mapping.v1",
                    selection_open_interest=selected.open_interest,
                    selection_volume=selected.volume,
                    source_profile="exchange_official",
                    quality_flag=quality,
                    metadata={"exchange": selected.exchange, "variety": selected.variety},
                )
            )
        return {
            "contracts": list(contracts_by_id.values()),
            "contract_bars": contract_bars,
            "mappings": mappings,
            "series_bars": bars,
        }

    def _parse_shfe_payload(
        self,
        payload: Mapping[str, Any],
        *,
        trade_date: str,
        exchange: str,
    ) -> List[OfficialFuturesContractBar]:
        rows = payload.get("o_curinstrument") or []
        interface = "official_ine_daily_kx_dat" if exchange == "INE" else "official_shfe_daily_kx_dat"
        parsed: List[OfficialFuturesContractBar] = []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            delivery = str(row.get("DELIVERYMONTH") or "").strip()
            product_name = str(row.get("PRODUCTNAME") or "")
            if delivery in {"", "小计", "合计"} or "总计" in product_name:
                continue
            variety = _first_text(row, ("PRODUCTGROUPID", "PRODUCTID")).split("_")[0].upper()
            contract = f"{variety}{delivery}".upper()
            parsed.append(
                OfficialFuturesContractBar(
                    exchange=exchange,
                    trade_date=_date_key(trade_date),
                    variety=variety,
                    contract=contract,
                    open=_number(row.get("OPENPRICE")),
                    high=_number(row.get("HIGHESTPRICE")),
                    low=_number(row.get("LOWESTPRICE")),
                    close=_number(row.get("CLOSEPRICE")),
                    settlement=_number(row.get("SETTLEMENTPRICE")),
                    volume=_number(row.get("VOLUME")),
                    open_interest=_number(row.get("OPENINTEREST")),
                    amount=_number(row.get("TURNOVER")),
                    source_interface=interface,
                    raw_payload=dict(row),
                    warnings=_quality_warnings(row, amount_unit="exchange_reported"),
                )
            )
        return parsed

    def _parse_dce_payload(
        self,
        payload: Mapping[str, Any],
        *,
        trade_date: str,
    ) -> List[OfficialFuturesContractBar]:
        rows = payload.get("data") or []
        parsed: List[OfficialFuturesContractBar] = []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            variety_name = str(row.get("variety") or "")
            contract = str(row.get("contractId") or "").strip().upper()
            if not contract or "小计" in variety_name or "总计" in variety_name:
                continue
            parsed.append(
                OfficialFuturesContractBar(
                    exchange="DCE",
                    trade_date=_date_key(trade_date),
                    variety=_contract_variety(contract),
                    contract=contract,
                    open=_number(row.get("open")),
                    high=_number(row.get("high")),
                    low=_number(row.get("low")),
                    close=_number(row.get("close")),
                    settlement=_number(row.get("clearPrice")),
                    volume=_number(row.get("volumn")),
                    open_interest=_number(row.get("openInterest")),
                    amount=_number(row.get("turnover")),
                    source_interface="official_dce_day_quotes",
                    raw_payload=dict(row),
                    warnings=_quality_warnings(row, amount_unit="exchange_reported"),
                )
            )
        return parsed

    def _parse_gfex_payload(
        self,
        payload: Mapping[str, Any],
        *,
        trade_date: str,
    ) -> List[OfficialFuturesContractBar]:
        rows = payload.get("data") or []
        parsed: List[OfficialFuturesContractBar] = []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            variety_text = str(row.get("variety") or "")
            variety = str(row.get("varietyOrder") or "").strip().upper()
            delivery = str(row.get("delivMonth") or "").strip()
            if not variety or not delivery or "小计" in variety_text or "总计" in variety_text:
                continue
            parsed.append(
                OfficialFuturesContractBar(
                    exchange="GFEX",
                    trade_date=_date_key(trade_date),
                    variety=variety,
                    contract=f"{variety}{delivery}",
                    open=_number(row.get("open")),
                    high=_number(row.get("high")),
                    low=_number(row.get("low")),
                    close=_number(row.get("close")),
                    settlement=_number(row.get("clearPrice")),
                    volume=_number(row.get("volumn")),
                    open_interest=_number(row.get("openInterest")),
                    amount=_number(row.get("turnover")),
                    source_interface="official_gfex_ti_day_quotes",
                    raw_payload=dict(row),
                    warnings=_quality_warnings(row, amount_unit="exchange_reported"),
                )
            )
        return parsed

    def _parse_czce_text(self, text: str, *, trade_date: str) -> List[OfficialFuturesContractBar]:
        parsed: List[OfficialFuturesContractBar] = []
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("小") or "合约" in line or "品种" in line:
                continue
            delimiter = "|" if "|" in line else ","
            cells = [item.strip().replace(",", "") for item in line.split(delimiter)]
            if len(cells) < 11:
                continue
            contract = cells[0].upper()
            if not re.match(r"^[A-Z]{1,3}[0-9]{3,4}$", contract):
                continue
            parsed.append(
                OfficialFuturesContractBar(
                    exchange="CZCE",
                    trade_date=_date_key(trade_date),
                    variety=_contract_variety(contract),
                    contract=contract,
                    open=_number(cells[2]),
                    high=_number(cells[3]),
                    low=_number(cells[4]),
                    close=_number(cells[5]),
                    settlement=_number(cells[6]),
                    volume=_number(cells[9]),
                    open_interest=_number(cells[10]),
                    amount=_number(cells[12] if len(cells) > 12 else None),
                    source_interface="official_czce_future_data_daily_txt",
                    raw_payload={"cells": cells},
                    warnings=["amount_unit_exchange_reported"],
                )
            )
        return parsed

    def _request_interval_for_exchange(self, exchange: str) -> float:
        return self.request_interval_seconds_by_exchange.get(
            str(exchange or "").upper(),
            self.request_interval_seconds,
        )

    def _challenge_retry_attempts_for_exchange(self, exchange: str) -> int:
        return self.challenge_retry_attempts_by_exchange.get(str(exchange or "").upper(), 0)

    def _challenge_backoff_for_exchange(self, exchange: str) -> float:
        return self.challenge_backoff_seconds_by_exchange.get(str(exchange or "").upper(), 0.0)

    def _rate_limit_retry_attempts_for_exchange(self, exchange: str) -> int:
        return self.rate_limit_retry_attempts_by_exchange.get(str(exchange or "").upper(), 0)

    def _rate_limit_backoff_for_exchange(self, exchange: str) -> float:
        return self.rate_limit_backoff_seconds_by_exchange.get(str(exchange or "").upper(), 0.0)

    def _wait_for_request_slot(self, exchange: str) -> None:
        exchange_key = str(exchange or "").upper()
        interval = self._request_interval_for_exchange(exchange_key)
        if interval <= 0:
            self._last_request_started_at = time.monotonic()
        else:
            now = time.monotonic()
            elapsed = now - self._last_request_started_at
            if self._last_request_started_at > 0 and elapsed < interval:
                time.sleep(interval - elapsed)
            self._last_request_started_at = time.monotonic()
        count = self._request_counts_by_exchange.get(exchange_key, 0) + 1
        self._request_counts_by_exchange[exchange_key] = count
        pause_every = self.batch_pause_every_requests_by_exchange.get(exchange_key, 0)
        pause_seconds = self.batch_pause_seconds_by_exchange.get(exchange_key, 0.0)
        if pause_every > 0 and pause_seconds > 0 and count > 1 and (count - 1) % pause_every == 0:
            self._increment_metric(exchange_key, "batch_pause_count", 1)
            self._increment_metric(exchange_key, "batch_pause_seconds", pause_seconds)
            logger.info(
                "[OfficialFutures] batch pause exchange=%s request_count=%s pause_seconds=%s",
                exchange_key,
                count - 1,
                pause_seconds,
            )
            time.sleep(pause_seconds)
            self._last_request_started_at = time.monotonic()

    @staticmethod
    def _is_challenge_response(response: requests.Response) -> bool:
        headers = getattr(response, "headers", {}) or {}
        content_type = headers.get("content-type", "").lower()
        if getattr(response, "status_code", None) == 567:
            return True
        if "html" not in content_type:
            return False
        text = getattr(response, "text", "") or ""
        return text.lstrip().startswith("<!doctype html") or "<html" in text[:200].lower()

    @staticmethod
    def _is_retryable_challenge(exc: Exception) -> bool:
        text = str(exc).lower()
        return "gfex_html_challenge" in text or "http_status=567" in text

    @staticmethod
    def _is_retryable_rate_limit(exc: Exception) -> bool:
        text = str(exc).lower()
        return "访问过于频繁" in text or "too frequent" in text or "rate limit" in text


def _select_main_contract(rows: Sequence[OfficialFuturesContractBar]) -> OfficialFuturesContractBar:
    if not rows:
        raise ValueError("cannot select main contract from empty rows")
    return sorted(
        rows,
        key=lambda row: (
            -1.0 * (row.open_interest if row.open_interest is not None else -1.0),
            -1.0 * (row.volume if row.volume is not None else -1.0),
            row.contract,
        ),
    )[0]


def _series_exchange(series: FuturesSeries) -> str:
    parts = str(series.instrument_id or "").split(".")
    return parts[-1].upper() if parts else ""


def _series_variety(series: FuturesSeries) -> str:
    parts = str(series.instrument_id or "").split(".")
    if len(parts) >= 2:
        return parts[1].upper()
    return _contract_variety(series.symbol)


def _contract_variety(value: str) -> str:
    match = re.match(r"^([A-Za-z]+)", str(value or "").strip())
    return match.group(1).upper() if match else str(value or "").upper()


def _official_daily_url(exchange: str, trade_date: str) -> str:
    day = _compact_date(trade_date)
    if exchange == "SHFE":
        return f"https://www.shfe.com.cn/data/tradedata/future/dailydata/kx{day}.dat"
    if exchange == "INE":
        return f"https://www.ine.cn/data/tradedata/future/dailydata/kx{day}.dat"
    if exchange == "DCE":
        return "http://www.dce.com.cn/dcereport/publicweb/dailystat/dayQuotes"
    if exchange == "CZCE":
        return f"http://www.czce.com.cn/cn/DFSStaticFiles/Future/{day[:4]}/{day}/FutureDataDaily.txt"
    if exchange == "GFEX":
        return "http://www.gfex.com.cn/u/interfacesWebTiDayQuotes/loadList"
    return ""


def classify_official_futures_failure(error: Any, *, payload_text: str = "") -> OfficialFuturesFailureClassification:
    """Classify official-source failures for operator diagnostics."""
    text = f"{error or ''} {payload_text or ''}".strip()
    lowered = text.lower()
    evidence: Dict[str, Any] = {"raw": text[:1000]}
    if "network is unreachable" in lowered or "errno 101" in lowered:
        return OfficialFuturesFailureClassification(
            category="network_unreachable",
            is_retryable=True,
            suspected_local_ip_risk_control=True,
            summary="local host cannot route to official endpoint; cross-IP success indicates possible local-IP block or network policy",
            evidence=evidence,
        )
    if "name or service not known" in lowered or "temporary failure in name resolution" in lowered or "gaierror" in lowered:
        return OfficialFuturesFailureClassification(
            category="dns_failure",
            is_retryable=True,
            suspected_local_ip_risk_control=False,
            summary="DNS resolution failed",
            evidence=evidence,
        )
    if "timed out" in lowered or "timeout" in lowered or "read timed out" in lowered:
        return OfficialFuturesFailureClassification(
            category="timeout",
            is_retryable=True,
            suspected_local_ip_risk_control=True,
            summary="official endpoint request timed out; repeated timeout from this host can indicate local-IP throttling",
            evidence=evidence,
        )
    if "ssl" in lowered or "certificate" in lowered or "tls" in lowered:
        return OfficialFuturesFailureClassification(
            category="tls_failure",
            is_retryable=True,
            suspected_local_ip_risk_control=False,
            summary="TLS/certificate negotiation failed",
            evidence=evidence,
        )
    anti_bot_markers = (
        "captcha",
        "access denied",
        "forbidden",
        "risk",
        "waf",
        "riversafe",
        "瑞数",
        "安全验证",
        "人机",
        "challenge",
    )
    if any(marker in lowered for marker in anti_bot_markers):
        return OfficialFuturesFailureClassification(
            category="possible_anti_bot_or_ip_risk_control",
            is_retryable=True,
            suspected_local_ip_risk_control=True,
            summary="official endpoint returned anti-bot/risk-control evidence",
            evidence=evidence,
        )
    if "567 server error" in lowered or "unknown status" in lowered:
        return OfficialFuturesFailureClassification(
            category="possible_anti_bot_or_ip_risk_control",
            is_retryable=True,
            suspected_local_ip_risk_control=True,
            summary="official endpoint returned non-standard HTTP 567/unknown status, likely a risk-control or challenge page",
            evidence=evidence,
        )
    if "http" in lowered and re.search(r"\b(403|429)\b", lowered):
        return OfficialFuturesFailureClassification(
            category="possible_anti_bot_or_ip_risk_control",
            is_retryable=True,
            suspected_local_ip_risk_control=True,
            summary="official endpoint returned HTTP 403/429 style refusal",
            evidence=evidence,
        )
    if "404" in lowered or "not found" in lowered:
        return OfficialFuturesFailureClassification(
            category="official_not_found_or_no_report",
            is_retryable=False,
            suspected_local_ip_risk_control=False,
            summary="official endpoint reports missing daily file or no report",
            evidence=evidence,
        )
    if payload_text and "<html" in payload_text.lower():
        return OfficialFuturesFailureClassification(
            category="unexpected_html_payload",
            is_retryable=True,
            suspected_local_ip_risk_control=True,
            summary="official endpoint returned HTML instead of expected data payload",
            evidence=evidence,
        )
    return OfficialFuturesFailureClassification(
        category="unknown_failure",
        is_retryable=True,
        suspected_local_ip_risk_control=False,
        summary="unclassified official-source failure",
        evidence=evidence,
    )


def _source_interface_for_exchange(exchange: str) -> str:
    return {
        "SHFE": "official_shfe_daily_kx_dat",
        "INE": "official_ine_daily_kx_dat",
        "DCE": "official_dce_day_quotes",
        "CZCE": "official_czce_future_data_daily_txt",
        "GFEX": "official_gfex_ti_day_quotes",
    }.get(exchange, "official_unknown_daily")


def _default_dce_chrome_path() -> str:
    for path in (
        "/opt/google/chrome/chrome",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    ):
        if os.path.exists(path):
            return path
    return ""


def _is_official_closed_response(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(
        marker in text
        for marker in (
            "404",
            "not found",
            "futuredataDaily.txt".lower(),
            "no report",
            "non-trading",
        )
    )


def _today_key() -> str:
    return date.today().isoformat()


def _date_key(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value or "").strip()
    if not text:
        raise ValueError("date value is empty")
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return date.fromisoformat(text[:10]).isoformat()


def _compact_date(value: Any) -> str:
    return _date_key(value).replace("-", "")


def _number(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in {"", "-", "--"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


class _HtmlTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.texts: List[str] = []

    def handle_data(self, data: str) -> None:
        text = re.sub(r"\s+", " ", str(data or "")).strip()
        if text:
            self.texts.append(text)


def _html_text_chunks(html: str) -> List[str]:
    parser = _HtmlTextExtractor()
    parser.feed(str(html or ""))
    return parser.texts


def _normalize_label(value: Any) -> str:
    return re.sub(r"[\s：:]+", "", str(value or "").strip())


def _field_after_label(texts: Sequence[str], labels: Sequence[str]) -> str:
    normalized_labels = {_normalize_label(item) for item in labels}
    for index, text in enumerate(texts):
        normalized = _normalize_label(text)
        for label in normalized_labels:
            if normalized == label:
                for value in texts[index + 1:]:
                    candidate = str(value or "").strip()
                    if candidate and _normalize_label(candidate) not in normalized_labels:
                        return candidate
            if normalized.startswith(label) and len(normalized) > len(label):
                return normalized[len(label):].strip()
    return ""


def _gfex_meta_content(html: str, name: str) -> str:
    pattern = (
        r"<meta[^>]+name=[\"']"
        + re.escape(name)
        + r"[\"'][^>]+content=[\"']([^\"']+)[\"'][^>]*>"
    )
    match = re.search(pattern, str(html or ""), flags=re.IGNORECASE)
    return match.group(1).strip() if match else ""


def _normalize_domestic_quote_unit(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if "美元" in text or "usd" in text:
        currency = "USD"
    elif "元" in text or "人民币" in text or "cny" in text:
        currency = "CNY"
    else:
        currency = "CNY"
    if "千克" in text or "kg" in text:
        unit = "kg"
    elif "克" in text or "gram" in text or "/g" in text:
        unit = "gram"
    elif "立方米" in text:
        unit = "cubic_meter"
    elif "张" in text or "sheet" in text:
        unit = "sheet"
    elif "吨" in text or "ton" in text:
        unit = "ton"
    else:
        return ""
    return f"{currency}/{unit}"


def _first_number(value: Any) -> Optional[float]:
    text = str(value or "").replace(",", "")
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return None
    return _number(match.group(0))


def _first_text(row: Mapping[str, Any], keys: Iterable[str]) -> str:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def _quality_warnings(row: Mapping[str, Any], *, amount_unit: str) -> List[str]:
    warnings: List[str] = []
    if amount_unit == "exchange_reported":
        warnings.append("amount_unit_exchange_reported")
    for field in ("OPENPRICE", "open", "HIGHESTPRICE", "high", "LOWESTPRICE", "low", "CLOSEPRICE", "close"):
        if field in row and str(row.get(field)).strip() in {"", "-", "--"}:
            warnings.append("missing_price_field")
            break
    return sorted(set(warnings))


def _hash_payload(value: Any) -> str:
    payload = json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
