"""Official A-share stock master sources.

The adapters in this module fetch and normalize scriptable public exchange
artifacts for A-share stock master data. They intentionally do not call
AkShare/BaoStock; those remain downstream fallback sources in the shared route.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import aiohttp
import pandas as pd

from .base_source import BaseDataSource, RateLimitConfig
from utils import ds_logger, get_shanghai_time


PARSER_VERSION = "a_share_exchange_official_stock_master.v1"


class AShareOfficialStockMasterSource(BaseDataSource):
    """Official exchange stock master source for SSE/SZSE/BSE."""

    is_official_instrument_master_source = True
    source_authority = "official"

    def __init__(
        self,
        name: str,
        rate_limit_config: RateLimitConfig = None,
        *,
        config: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(name, rate_limit_config)
        self.config = dict(config or {})
        self.supported_exchanges = ["SSE", "SZSE", "BSE"]
        self.parser_version = self.config.get("parser_version") or PARSER_VERSION
        self.timeout_sec = float(self.config.get("timeout_sec", 30) or 30)
        self.user_agent = self.config.get(
            "user_agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        )

    async def _initialize_impl(self):
        timeout = aiohttp.ClientTimeout(total=self.timeout_sec)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"User-Agent": self.user_agent},
        )

    async def get_instrument_list(
        self,
        exchange: str = None,
        instrument_types: List[str] = None,
    ) -> List[Dict[str, Any]]:
        if instrument_types and "stock" not in {str(item).lower() for item in instrument_types}:
            return []
        exchange = str(exchange or "").upper()
        await self.rate_limiter.acquire()

        if exchange == "SSE":
            return await self._get_sse_stock_list()
        if exchange == "SZSE":
            return await self._get_szse_stock_list()
        if exchange == "BSE":
            return await self._get_bse_stock_list()
        ds_logger.warning("[A-share official master] Unsupported exchange: %s", exchange)
        return []

    async def get_daily_data(self, *args, **kwargs) -> List[Dict[str, Any]]:
        return []

    async def get_latest_daily_data(self, instrument_id: str, symbol: str) -> Dict[str, Any]:
        return {}

    async def _get_sse_stock_list(self) -> List[Dict[str, Any]]:
        cfg = _market_cfg(self.config, "sse")
        frames: List[pd.DataFrame] = []
        source_urls: List[str] = []
        raw_hashes: List[str] = []

        stock_types = cfg.get("stock_types") or [
            {"label": "main_board_a", "stock_type": "1", "board": "main_board"},
            {"label": "star_market", "stock_type": "8", "board": "star_market"},
        ]
        for item in stock_types:
            raw, source_url = await self._load_sse_payload(cfg, str(item.get("stock_type") or "1"))
            if raw is None:
                continue
            source_urls.append(source_url)
            raw_hashes.append(_sha256_bytes(raw))
            frame = _read_any_table(raw, source_url=source_url)
            if frame.empty:
                continue
            frame["__board"] = item.get("board") or item.get("label") or ""
            frames.append(frame)

        if not frames:
            return []
        df = pd.concat(frames, ignore_index=True)
        return self._normalize_records(
            _records(df),
            exchange="SSE",
            source_urls=source_urls,
            raw_hashes=raw_hashes,
        )

    async def _get_szse_stock_list(self) -> List[Dict[str, Any]]:
        cfg = _market_cfg(self.config, "szse")
        raw, source_url = await self._load_szse_payload(cfg)
        if raw is None:
            return []
        raw_hash = _sha256_bytes(raw)
        df = _read_any_table(raw, source_url=source_url)
        return self._normalize_records(
            _records(df),
            exchange="SZSE",
            source_urls=[source_url],
            raw_hashes=[raw_hash],
        )

    async def _get_bse_stock_list(self) -> List[Dict[str, Any]]:
        cfg = _market_cfg(self.config, "bse")
        raw, source_url = await self._load_bse_payload(cfg)
        if raw is None:
            return []
        raw_hash = _sha256_bytes(raw)
        df = _read_any_table(raw, source_url=source_url)
        if not df.empty:
            df = _normalize_bse_frame(df)
        return self._normalize_records(
            _records(df),
            exchange="BSE",
            source_urls=[source_url],
            raw_hashes=[raw_hash],
        )

    async def _load_sse_payload(self, cfg: Dict[str, Any], stock_type: str) -> tuple[Optional[bytes], str]:
        fixture = cfg.get("current_list_file")
        if fixture:
            return _read_file_bytes(fixture), str(fixture)

        url = cfg.get("current_list_url") or "https://query.sse.com.cn/sseQuery/commonQuery.do"
        params = {
            "STOCK_TYPE": stock_type,
            "REG_PROVINCE": "",
            "CSRC_CODE": "",
            "STOCK_CODE": "",
            "sqlId": cfg.get("sql_id") or "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L",
            "COMPANY_STATUS": cfg.get("company_status") or "2,4,5,7,8",
            "type": "inParams",
            "isPagination": "true",
            "pageHelp.cacheSize": "1",
            "pageHelp.beginPage": "1",
            "pageHelp.pageSize": str(cfg.get("page_size") or 10000),
            "pageHelp.pageNo": "1",
            "pageHelp.endPage": "1",
        }
        headers = {
            "Referer": cfg.get("referer") or "https://www.sse.com.cn/assortment/stock/list/share/",
            "Host": "query.sse.com.cn",
        }
        return await self._http_get(url, params=params, headers=headers), url

    async def _load_szse_payload(self, cfg: Dict[str, Any]) -> tuple[Optional[bytes], str]:
        fixture = cfg.get("current_list_file")
        if fixture:
            return _read_file_bytes(fixture), str(fixture)
        url = cfg.get("current_list_url") or "https://www.szse.cn/api/report/ShowReport"
        params = {
            "SHOWTYPE": cfg.get("show_type") or "xlsx",
            "CATALOGID": cfg.get("catalog_id") or "1110",
            "TABKEY": cfg.get("tab_key") or "tab1",
            "random": cfg.get("random") or "0.6935816432433362",
        }
        return await self._http_get(url, params=params), url

    async def _load_bse_payload(self, cfg: Dict[str, Any]) -> tuple[Optional[bytes], str]:
        fixture = cfg.get("current_list_file")
        if fixture:
            return _read_file_bytes(fixture), str(fixture)
        url = cfg.get("current_list_url") or "https://www.bse.cn/nqxxController/nqxxCnzq.do"
        payload = {
            "page": "0",
            "typejb": cfg.get("typejb") or "T",
            "xxfcbj[]": cfg.get("xxfcbj") or "2",
            "xxzqdm": "",
            "sortfield": cfg.get("sortfield") or "xxzqdm",
            "sorttype": cfg.get("sorttype") or "asc",
        }
        raw = await self._http_post(url, data=payload)
        if raw is None:
            return None, url
        pages = _extract_bse_pages(raw)
        if pages <= 1:
            return raw, url
        payloads = [raw]
        for page in range(1, pages):
            page_payload = dict(payload)
            page_payload["page"] = str(page)
            page_raw = await self._http_post(url, data=page_payload)
            if page_raw:
                payloads.append(page_raw)
        merged_content = []
        for item in payloads:
            merged_content.extend(_extract_bse_content(item))
        return json.dumps(merged_content, ensure_ascii=False).encode("utf-8"), url

    async def _http_get(
        self,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Optional[bytes]:
        try:
            async with self.session.get(url, params=params, headers=headers) as response:
                response.raise_for_status()
                return await response.read()
        except Exception as exc:
            ds_logger.warning("[A-share official master] GET failed for %s: %s", url, exc)
            return None

    async def _http_post(self, url: str, *, data: Dict[str, Any]) -> Optional[bytes]:
        try:
            async with self.session.post(url, data=data) as response:
                response.raise_for_status()
                return await response.read()
        except Exception as exc:
            ds_logger.warning("[A-share official master] POST failed for %s: %s", url, exc)
            return None

    def _normalize_stock_row(
        self,
        row: Dict[str, Any],
        *,
        exchange: str,
        source_urls: List[str],
        raw_hashes: List[str],
    ) -> Optional[Dict[str, Any]]:
        symbol = _first_text(row, _symbol_keys(exchange))
        if not symbol:
            return None
        symbol = symbol.split(".")[0].strip().zfill(6)
        if not symbol.isdigit() or len(symbol) != 6:
            return None

        name = _first_text(row, _name_keys(exchange))
        if not name:
            return None
        listed_date = _parse_date(_first_text(row, _listed_date_keys(exchange)))
        industry = _first_text(row, ["所属行业", "CSRC_CODE_DESC", "所属行业", "industry"])
        sector = _first_text(row, ["地区", "REG_PROVINCE", "area", "__board"])
        market = _first_text(row, ["板块", "BOARD_NAME", "__board", "market"])
        instrument_id = _instrument_id(symbol, exchange)
        now = get_shanghai_time()
        source_name = f"{exchange.lower()}_official"

        return {
            "instrument_id": instrument_id,
            "symbol": symbol,
            "name": name,
            "exchange": exchange,
            "type": "stock",
            "currency": "CNY",
            "listed_date": listed_date,
            "delisted_date": None,
            "industry": industry,
            "sector": sector,
            "market": market,
            "status": "active",
            "is_active": True,
            "is_st": "ST" in name.upper(),
            "trading_status": 1,
            "source": source_name,
            "source_symbol": symbol,
            "source_authority": "official",
            "official_lifecycle_source": source_name,
            "source_url": ";".join(source_urls),
            "raw_snapshot_hash": _combine_hashes(raw_hashes),
            "parser_version": self.parser_version,
            "created_at": now,
            "updated_at": now,
            "data_version": 1,
            "metadata": {
                "source_authority": "official",
                "source_urls": source_urls,
                "market": market,
            },
        }

    def _normalize_records(
        self,
        records: Iterable[Dict[str, Any]],
        *,
        exchange: str,
        source_urls: List[str],
        raw_hashes: List[str],
    ) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        seen = set()
        for row in records:
            item = self._normalize_stock_row(
                row,
                exchange=exchange,
                source_urls=source_urls,
                raw_hashes=raw_hashes,
            )
            if not item:
                continue
            instrument_id = item["instrument_id"]
            if instrument_id in seen:
                continue
            seen.add(instrument_id)
            normalized.append(item)
        return normalized


def _market_cfg(config: Dict[str, Any], market: str) -> Dict[str, Any]:
    value = config.get(market) or config.get(market.upper()) or {}
    return value if isinstance(value, dict) else {}


def _read_file_bytes(path_text: str) -> bytes:
    return Path(path_text).expanduser().read_bytes()


def _read_any_table(raw: bytes, *, source_url: str = "") -> pd.DataFrame:
    if raw is None:
        return pd.DataFrame()
    text = _decode(raw)
    stripped = text.strip()
    suffix = Path(str(source_url)).suffix.lower()
    try:
        if suffix in {".xlsx", ".xls"} or raw[:2] == b"PK":
            return pd.read_excel(BytesIO(raw))
        if stripped.startswith("{") or stripped.startswith("["):
            payload = json.loads(stripped)
            rows = _json_rows(payload)
            return pd.DataFrame(rows)
        if suffix == ".csv" or "," in stripped.splitlines()[0]:
            return pd.read_csv(StringIO(text))
        tables = pd.read_html(StringIO(text))
        return tables[0] if tables else pd.DataFrame()
    except Exception as exc:
        ds_logger.warning("[A-share official master] Failed to parse table %s: %s", source_url, exc)
        return pd.DataFrame()


def _json_rows(payload: Any) -> List[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in ("result", "data", "rows", "content"):
        value = payload.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = _json_rows(value)
            if nested:
                return nested
    return []


def _extract_bse_pages(raw: bytes) -> int:
    try:
        payload = json.loads(_decode(raw)[_decode(raw).find("[") : -1])
        if payload and isinstance(payload[0], dict):
            return int(payload[0].get("totalPages") or 1)
    except Exception:
        return 1
    return 1


def _extract_bse_content(raw: bytes) -> List[Any]:
    try:
        text = _decode(raw)
        payload = json.loads(text[text.find("[") : -1])
        if payload and isinstance(payload[0], dict):
            return list(payload[0].get("content") or [])
        if isinstance(payload, list):
            return payload
    except Exception:
        return []
    return []


def _records(df: pd.DataFrame) -> Iterable[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    return df.where(pd.notna(df), None).to_dict(orient="records")


def _decode(raw: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def _first_text(row: Dict[str, Any], keys: Iterable[str]) -> str:
    for key in keys:
        if key in row and row[key] is not None:
            text = str(row[key]).strip()
            if text and text.lower() not in {"nan", "none", "null"}:
                return text
    return ""


def _parse_date(value: str) -> Optional[str]:
    if not value:
        return None
    try:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.date().isoformat()
    except Exception:
        return None


def _normalize_bse_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize BSE live JSON-array columns or already named tables."""
    if {"证券代码", "证券简称"}.issubset(set(str(c) for c in df.columns)):
        return df
    if df.shape[1] <= 40:
        return df
    renamed = pd.DataFrame()
    renamed["上市日期"] = df.iloc[:, 0]
    renamed["所属行业"] = df.iloc[:, 17]
    renamed["地区"] = df.iloc[:, 29]
    renamed["证券代码"] = df.iloc[:, 38]
    renamed["证券简称"] = df.iloc[:, 40]
    if df.shape[1] > 36:
        renamed["总股本"] = df.iloc[:, 36]
    if df.shape[1] > 11:
        renamed["流通股本"] = df.iloc[:, 11]
    return renamed


def _symbol_keys(exchange: str) -> List[str]:
    if exchange == "SSE":
        return ["证券代码", "A_STOCK_CODE", "SECURITY_CODE_A", "SECURITY_CODE", "code", "symbol"]
    if exchange == "SZSE":
        return ["A股代码", "证券代码", "secucode", "zqdm", "code", "symbol"]
    return ["证券代码", "xxzqdm", "zqdm", "code", "symbol"]


def _name_keys(exchange: str) -> List[str]:
    if exchange == "SSE":
        return ["证券简称", "SEC_NAME_CN", "SECURITY_ABBR_A", "SECURITY_ABBR", "name"]
    if exchange == "SZSE":
        return ["A股简称", "证券简称", "agjc", "zqjc", "name"]
    return ["证券简称", "xxzqjc", "zqjc", "name"]


def _listed_date_keys(exchange: str) -> List[str]:
    if exchange == "SSE":
        return ["上市日期", "LIST_DATE", "上市时间", "listed_date"]
    if exchange == "SZSE":
        return ["A股上市日期", "上市日期", "agssrq", "listed_date"]
    return ["上市日期", "xxssrq", "listed_date"]


def _instrument_id(symbol: str, exchange: str) -> str:
    suffix = {"SSE": "SH", "SZSE": "SZ", "BSE": "BJ"}[exchange]
    return f"{symbol}.{suffix}"


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw or b"").hexdigest()


def _combine_hashes(values: List[str]) -> str:
    values = [item for item in values if item]
    if not values:
        return ""
    if len(values) == 1:
        return values[0]
    return hashlib.sha256("|".join(values).encode("utf-8")).hexdigest()
