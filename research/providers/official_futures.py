"""Official domestic futures exchange daily-bar provider."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import requests

from research.futures_market_data import (
    FuturesBar,
    FuturesSeries,
)
from utils.config_manager import ResearchConfig
from utils.http_transport import HttpTlsConfig, create_requests_session, request_get, request_post


OFFICIAL_FUTURES_PARSER_VERSION = "official_futures_daily.v1"


class OfficialFuturesSourceUnavailable(RuntimeError):
    """Raised when an official source is unsupported, empty, or failed."""


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

    def __init__(self, research_config: ResearchConfig):
        self.research_config = research_config
        self.module_cfg = research_config.modules.get("commodity_market_data", {})
        self.source_cfg = self.module_cfg.get("sources", {}).get("exchange_official", {})
        self.timeout_seconds = float(self.source_cfg.get("timeout_seconds", 20))
        self.retry_attempts = max(1, int(self.source_cfg.get("retry_attempts", 2)))
        self.retry_backoff_seconds = max(0.0, float(self.source_cfg.get("retry_backoff_seconds", 0.5)))
        self.request_interval_seconds = max(0.0, float(self.source_cfg.get("request_interval_seconds", 0.0)))
        self.enabled_exchanges = {
            str(item).upper()
            for item in self.source_cfg.get("enabled_exchanges", sorted(self.supported_exchanges))
        }
        self.tls_config = HttpTlsConfig(source_name=self.source_name)
        self._last_request_started_at = 0.0

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
        bars = self._construct_main_series_bars(series, rows, mode=mode)
        if not bars:
            raise OfficialFuturesSourceUnavailable(
                f"official futures source returned no usable bars for {series.series_id} {start}-{end}"
            )
        return bars

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

    def _request_exchange_payload(
        self,
        session: requests.Session,
        exchange: str,
        trade_date: str,
    ) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.retry_attempts + 1):
            self._wait_for_request_slot()
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
                    response = request_post(
                        "http://www.dce.com.cn/dcereport/publicweb/dailystat/dayQuotes",
                        session=session,
                        tls_config=self.tls_config,
                        json={
                            "contractId": "",
                            "lang": "zh",
                            "optionSeries": "",
                            "statisticsType": "0",
                            "tradeDate": _compact_date(trade_date),
                            "tradeType": "1",
                            "varietyId": "all",
                        },
                        timeout=self.timeout_seconds,
                    )
                    response.raise_for_status()
                    return response.json()
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
                    response = request_post(
                        "http://www.gfex.com.cn/u/interfacesWebTiDayQuotes/loadList",
                        session=session,
                        tls_config=self.tls_config,
                        data={"trade_date": _compact_date(trade_date), "trade_type": "0"},
                        timeout=self.timeout_seconds,
                    )
                    response.raise_for_status()
                    return response.json()
                raise OfficialFuturesSourceUnavailable(f"unsupported official exchange: {exchange}")
            except Exception as exc:
                last_error = exc
                if attempt >= self.retry_attempts:
                    break
                if self.retry_backoff_seconds > 0:
                    time.sleep(self.retry_backoff_seconds * attempt)
        raise OfficialFuturesSourceUnavailable(
            f"official {exchange} request failed for {trade_date}: {last_error}"
        )

    def _construct_main_series_bars(
        self,
        series: FuturesSeries,
        rows: Sequence[OfficialFuturesContractBar],
        *,
        mode: str,
    ) -> List[FuturesBar]:
        by_date: Dict[str, List[OfficialFuturesContractBar]] = {}
        for row in rows:
            by_date.setdefault(row.trade_date, []).append(row)
        bars: List[FuturesBar] = []
        for trade_date, date_rows in sorted(by_date.items()):
            selected = _select_main_contract(date_rows)
            metadata = {
                "underlying_contract": selected.contract,
                "variety": selected.variety,
                "exchange": selected.exchange,
                "construction_method": "official_open_interest_main",
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
        return bars

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

    def _wait_for_request_slot(self) -> None:
        if self.request_interval_seconds <= 0:
            self._last_request_started_at = time.monotonic()
            return
        now = time.monotonic()
        elapsed = now - self._last_request_started_at
        if self._last_request_started_at > 0 and elapsed < self.request_interval_seconds:
            time.sleep(self.request_interval_seconds - elapsed)
        self._last_request_started_at = time.monotonic()


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
