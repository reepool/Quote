"""Official A-share index source adapters.

The module keeps publisher-specific parsing isolated from daily update logic.
CNIndex and CSIndex are treated as official evidence sources for index master
metadata and lifecycle decisions; BaoStock/AkShare remain fallback quote
sources through the normal routing chain.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, date
from io import BytesIO
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from pypdf import PdfReader

from utils import ds_logger
from utils.http_transport import HttpTlsConfig, urlopen_bytes

from .base_source import BaseDataSource, RateLimitConfig


OFFICIAL_INDEX_PARSER_VERSION = "official-index-source-v1"


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _date_value(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    try:
        parsed = pd.to_datetime(value)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def _snapshot_hash(raw: bytes | str) -> str:
    if isinstance(raw, str):
        raw = raw.encode("utf-8")
    return hashlib.sha256(raw or b"").hexdigest()


def normalize_index_code(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    if "." in text:
        text = text.split(".", 1)[0]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else ""


def cnindex_metadata_instrument_id(code: Any, *, cni_code: Any = "") -> str:
    cni_text = _text(cni_code).upper()
    if cni_text:
        return cni_text
    normalized = normalize_index_code(code)
    return f"CNI{normalized}.SZ" if normalized else ""


def cnindex_instrument_id(code: Any, *, quote_code: Any = "", cni_code: Any = "") -> str:
    quote_symbol = normalize_index_code(quote_code)
    if quote_symbol:
        return f"{quote_symbol}.SZ"
    return cnindex_metadata_instrument_id(code, cni_code=cni_code)


def csindex_instrument_id(code: Any) -> str:
    normalized = normalize_index_code(code)
    if not normalized:
        return ""
    # CSIndex publishes many cross-market/SSE quote codes under 000/93 prefixes.
    # Keep the existing local convention for these benchmark rows.
    return f"{normalized}.SH"


@dataclass
class OfficialIndexSnapshot:
    source: str
    source_url: str
    parser_version: str
    raw_snapshot_hash: str
    rows: List[Dict[str, Any]]
    diagnostics: Dict[str, Any]


class OfficialIndexLifecycleParser:
    """Conservative parser for official index lifecycle announcements."""

    TERMINATION_PATTERNS = (
        "终止计算发布",
        "终止发布",
        "停止计算发布",
        "停止发布",
    )

    @classmethod
    def extract_pdf_text(cls, pdf_bytes: bytes) -> str:
        reader = PdfReader(BytesIO(pdf_bytes))
        parts = []
        for page in reader.pages:
            text = page.extract_text() or ""
            if text:
                parts.append(text)
        return "\n".join(parts)

    @classmethod
    def parse_termination_announcement(
        cls,
        *,
        text: str,
        title: str,
        announcement_date: Optional[date],
        source_url: str,
    ) -> List[Dict[str, Any]]:
        normalized_text = re.sub(r"\s+", " ", text or "")
        title_text = title or ""
        combined = f"{title_text} {normalized_text}"
        if not any(pattern in combined for pattern in cls.TERMINATION_PATTERNS):
            return []

        effective_date = None
        date_match = re.search(
            r"自\s*(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*起",
            normalized_text,
        )
        if date_match:
            year, month, day = (int(part) for part in date_match.groups())
            effective_date = date(year, month, day)

        codes = sorted(set(re.findall(r"(?<!\d)([0-9]{6})(?!\d)", combined)))
        rows: List[Dict[str, Any]] = []
        for code in codes:
            rows.append(
                {
                    "instrument_id": cnindex_instrument_id(code),
                    "symbol": code,
                    "exchange": "SZSE",
                    "lifecycle_state": "calculation_terminated",
                    "event_type": "calculation_terminated",
                    "effective_date": effective_date,
                    "announcement_date": announcement_date,
                    "announcement_title": title_text,
                    "evidence_url": source_url,
                    "matched_code": code,
                    "confidence": "direct",
                    "source": "cnindex_announcement",
                    "parser_version": OFFICIAL_INDEX_PARSER_VERSION,
                    "diagnostics": {"matched_codes": codes},
                }
            )
        return rows


class CNIndexSource(BaseDataSource):
    """Official CNIndex source for CNI/SZSE index metadata and daily quotes."""

    DEFAULT_INDEX_LIST_URL = (
        "https://www.cnindex.com.cn/index_1020/brochures_1593/201912/"
        "P020260506563681367298.xlsx"
    )
    DEFAULT_NOTICE_ROOT_URL = "https://www.cnindex.com.cn/zh_information/notices_news/?act_menu=2"
    DEFAULT_DAILY_URL = (
        "https://hq.cnindex.com.cn/market/market/getIndexDailyDataWithDataFormat"
    )

    def __init__(
        self,
        name: str,
        rate_limit_config: RateLimitConfig = None,
        *,
        config: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(name, rate_limit_config)
        self.source_config = config or {}
        self.supported_exchanges = ["SZSE"]
        self.instrument_types_supported = ["index"]
        self.user_agent = self.source_config.get(
            "user_agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )
        self.timeout_sec = float(self.source_config.get("timeout_sec", 30))
        self.index_list_url = self.source_config.get("index_list_url") or self.DEFAULT_INDEX_LIST_URL
        self.notice_root_url = self.source_config.get("notice_root_url") or self.DEFAULT_NOTICE_ROOT_URL
        self.daily_url = self.source_config.get("daily_url") or self.DEFAULT_DAILY_URL
        self.termination_announcement_urls = list(
            self.source_config.get("termination_announcement_urls") or []
        )

    async def _initialize_impl(self):
        self.is_initialized = True

    def _fetch_bytes(self, url: str) -> bytes:
        return urlopen_bytes(
            url,
            timeout_sec=self.timeout_sec,
            user_agent=self.user_agent,
            tls_config=HttpTlsConfig(source_name="cnindex"),
        )

    @staticmethod
    def parse_index_list_excel(raw_bytes: bytes, *, source_url: str) -> OfficialIndexSnapshot:
        frame = pd.read_excel(BytesIO(raw_bytes), dtype=str)
        rows: List[Dict[str, Any]] = []
        for _, row in frame.iterrows():
            code = normalize_index_code(row.get("指数代码"))
            if not code:
                continue
            full_name = _text(row.get("指数全称"))
            short_name = _text(row.get("指数简称")) or full_name or code
            price_return_type = _text(row.get("价格收益"))
            szse_quote_code = normalize_index_code(row.get("深交所行情代码"))
            cni_code = _text(row.get(".CNI"))
            metadata_only = not bool(szse_quote_code)
            rows.append(
                {
                    "instrument_id": cnindex_instrument_id(
                        code,
                        quote_code=szse_quote_code,
                        cni_code=cni_code,
                    ),
                    "symbol": szse_quote_code or code,
                    "name": short_name,
                    "exchange": "SZSE",
                    "type": "index",
                    "currency": "CNY",
                    "listed_date": _date_value(row.get("发布日期")),
                    "delisted_date": None,
                    "status": "metadata_only" if metadata_only else "active",
                    "is_active": not metadata_only,
                    "is_st": False,
                    "trading_status": 0 if metadata_only else 1,
                    "source": "cnindex",
                    "source_symbol": code,
                    "market": _text(row.get("指数系列")),
                    "industry": _text(row.get("指数类别")),
                    "sector": _text(row.get("资产类别")),
                    "official_lifecycle_source": "cnindex_index_list",
                    "source_url": source_url,
                    "parser_version": OFFICIAL_INDEX_PARSER_VERSION,
                    "metadata": {
                        "full_name": full_name,
                        "english_name": _text(row.get("英文名称")),
                        "publisher": _text(row.get("发布渠道")),
                        "szse_quote_code": szse_quote_code,
                        "ric": _text(row.get("RIC")),
                        "bloomberg": _text(row.get("BLOOMBERG")),
                        "cni_code": _text(row.get(".CNI")),
                        "base_date": _text(row.get("基日")),
                        "base_point": _text(row.get("基点")),
                        "price_return_type": price_return_type,
                        "index_family": _text(row.get("指数系列")),
                        "index_category": _text(row.get("指数类别")),
                        "calculation_system": _text(row.get("指数计算系统")),
                        "coverage_scope": _text(row.get("覆盖范围")),
                    },
                }
            )
        return OfficialIndexSnapshot(
            source="cnindex_index_list",
            source_url=source_url,
            parser_version=OFFICIAL_INDEX_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash(raw_bytes),
            rows=rows,
            diagnostics={"row_count": len(rows)},
        )

    async def get_index_master_snapshot(self) -> OfficialIndexSnapshot:
        await self.rate_limiter.acquire()
        raw = await asyncio.to_thread(self._fetch_bytes, self.index_list_url)
        return self.parse_index_list_excel(raw, source_url=self.index_list_url)

    async def get_instrument_list(
        self,
        exchange: str = None,
        instrument_types: List[str] = None,
    ) -> List[Dict[str, Any]]:
        if exchange and exchange.upper() not in ("SZSE", "SSE"):
            return []
        if instrument_types and "index" not in {str(item).lower() for item in instrument_types}:
            return []
        snapshot = await self.get_index_master_snapshot()
        return snapshot.rows

    def parse_notice_page(self, html: str) -> List[Dict[str, Any]]:
        notices: List[Dict[str, Any]] = []
        for match in re.finditer(
            r'href="(?P<href>\./[^"]+?\.pdf)\?act_menu=2".{0,500}?<span>(?P<title>.*?)</span>.{0,200}?<i class="news-time">\s*(?P<date>\d{4}-\d{2}-\d{2})</i>',
            html or "",
            flags=re.S,
        ):
            href = match.group("href").replace("./", "")
            url = "https://www.cnindex.com.cn/zh_information/notices_news/" + href
            title = re.sub(r"<.*?>", "", match.group("title")).strip()
            notice_date = datetime.fromisoformat(match.group("date")).date()
            notices.append({"title": title, "date": notice_date, "url": url})
        return notices

    async def discover_lifecycle_announcements(self) -> List[Dict[str, Any]]:
        notices: List[Dict[str, Any]] = []
        try:
            await self.rate_limiter.acquire()
            raw = await asyncio.to_thread(self._fetch_bytes, self.notice_root_url)
            notices.extend(self.parse_notice_page(raw.decode("utf-8", errors="ignore")))
        except Exception as exc:
            ds_logger.warning("[cnindex] notice page fetch/parse failed: %s", exc)

        for url in self.termination_announcement_urls:
            if url and not any(item.get("url") == url for item in notices):
                notices.append({"title": "", "date": None, "url": url})
        return notices

    async def get_lifecycle_evidence(self) -> List[Dict[str, Any]]:
        evidence: List[Dict[str, Any]] = []
        for notice in await self.discover_lifecycle_announcements():
            title = notice.get("title") or ""
            if title and not any(
                pattern in title for pattern in OfficialIndexLifecycleParser.TERMINATION_PATTERNS
            ):
                continue
            url = notice.get("url")
            if not url:
                continue
            try:
                await self.rate_limiter.acquire()
                raw_pdf = await asyncio.to_thread(self._fetch_bytes, url)
                pdf_text = OfficialIndexLifecycleParser.extract_pdf_text(raw_pdf)
                rows = OfficialIndexLifecycleParser.parse_termination_announcement(
                    text=pdf_text,
                    title=title or self._title_from_pdf_text(pdf_text),
                    announcement_date=notice.get("date"),
                    source_url=url,
                )
                for row in rows:
                    row["raw_snapshot_hash"] = _snapshot_hash(raw_pdf)
                evidence.extend(rows)
            except Exception as exc:
                ds_logger.warning("[cnindex] lifecycle announcement parse failed %s: %s", url, exc)
        return evidence

    @staticmethod
    def _title_from_pdf_text(text: str) -> str:
        for line in (text or "").splitlines():
            line = line.strip()
            if "公告" in line and len(line) > 4:
                return line
        return ""

    @staticmethod
    def parse_daily_response(
        payload: Dict[str, Any],
        *,
        instrument_id: str,
        source_symbol: str,
    ) -> List[Dict[str, Any]]:
        data_block = payload.get("data") if isinstance(payload, dict) else {}
        rows = data_block.get("data") if isinstance(data_block, dict) else []
        normalized: List[Dict[str, Any]] = []
        for row in rows or []:
            if not isinstance(row, list) or len(row) < 10:
                continue
            try:
                pct_text = _text(row[7]).replace("%", "")
                normalized.append(
                    {
                        "instrument_id": instrument_id,
                        "time": pd.to_datetime(row[0]).to_pydatetime(),
                        "open": float(row[3]),
                        "high": float(row[2]),
                        "low": float(row[4]),
                        "close": float(row[5]),
                        "volume": int(float(row[9] or 0) * 10000),
                        "amount": float(row[8] or 0) * 100000000,
                        "change": float(row[6] or 0),
                        "pct_change": float(pct_text or 0),
                        "tradestatus": 1,
                        "factor": 1.0,
                        "adjustment_type": "none",
                        "is_complete": True,
                        "quality_score": 0.95,
                        "source": "cnindex",
                        "batch_id": f"cnindex_{source_symbol}",
                    }
                )
            except Exception:
                continue
        return sorted(normalized, key=lambda item: item["time"])

    async def get_daily_data(
        self,
        instrument_id: str,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        instrument_type: str = "stock",
        source_symbol: str = "",
    ) -> List[Dict[str, Any]]:
        if (instrument_type or "stock").lower() != "index":
            return []
        code = normalize_index_code(source_symbol or symbol or instrument_id)
        if not code:
            return []
        url = (
            f"{self.daily_url}?indexCode={code}"
            f"&startDate={start_date.date().isoformat()}"
            f"&endDate={end_date.date().isoformat()}"
            "&frequency=day"
        )
        await self.rate_limiter.acquire()
        raw = await asyncio.to_thread(self._fetch_bytes, url)
        import json

        payload = json.loads(raw.decode("utf-8", errors="ignore"))
        return self.parse_daily_response(payload, instrument_id=instrument_id, source_symbol=code)

    async def get_latest_daily_data(self, instrument_id: str, symbol: str) -> Dict[str, Any]:
        rows = await self.get_daily_data(
            instrument_id,
            symbol,
            datetime.combine(date.today(), datetime.min.time()),
            datetime.combine(date.today(), datetime.max.time()),
            instrument_type="index",
        )
        return rows[-1] if rows else {}


class CSIndexSource(BaseDataSource):
    """Official CSIndex source for CSI/SSE index metadata and daily quotes."""

    DEFAULT_BASIC_INFO_URL = "https://www.csindex.com.cn/csindex-home/indexInfo/index-basic-info/{code}"
    DEFAULT_FUZZY_SEARCH_URL = "https://www.csindex.com.cn/csindex-home/indexInfo/index-fuzzy-search"
    DEFAULT_DAILY_URL = "https://www.csindex.com.cn/csindex-home/perf/index-perf"

    def __init__(
        self,
        name: str,
        rate_limit_config: RateLimitConfig = None,
        *,
        config: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(name, rate_limit_config)
        self.source_config = config or {}
        self.supported_exchanges = ["SSE", "SZSE"]
        self.instrument_types_supported = ["index"]
        self.timeout_sec = float(self.source_config.get("timeout_sec", 30))
        self.user_agent = self.source_config.get(
            "user_agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )
        self.basic_info_url = self.source_config.get("basic_info_url") or self.DEFAULT_BASIC_INFO_URL
        self.fuzzy_search_url = self.source_config.get("fuzzy_search_url") or self.DEFAULT_FUZZY_SEARCH_URL
        self.daily_url = self.source_config.get("daily_url") or self.DEFAULT_DAILY_URL
        self.list_page_size = int(self.source_config.get("list_page_size", 1000))

    async def _initialize_impl(self):
        self.is_initialized = True

    def _fetch_bytes(self, url: str) -> bytes:
        return urlopen_bytes(
            url,
            timeout_sec=self.timeout_sec,
            user_agent=self.user_agent,
            tls_config=HttpTlsConfig(source_name="csindex"),
        )

    @staticmethod
    def parse_basic_info(payload: Dict[str, Any], *, source_url: str) -> Dict[str, Any]:
        data = payload.get("data") if isinstance(payload, dict) else {}
        if not isinstance(data, dict) or not data:
            return {}
        code = normalize_index_code(data.get("indexCode"))
        if not code:
            return {}
        return {
            "instrument_id": csindex_instrument_id(code),
            "symbol": code,
            "name": _text(data.get("indexShortNameCn") or data.get("indexFullNameCn") or code),
            "exchange": "SSE",
            "type": "index",
            "currency": "CNY",
            "listed_date": _date_value(data.get("publishDate")),
            "delisted_date": None,
            "status": "active",
            "is_active": True,
            "is_st": False,
            "trading_status": 1,
            "source": "csindex",
            "source_symbol": code,
            "official_lifecycle_source": "csindex_basic_info",
            "source_url": source_url,
            "parser_version": OFFICIAL_INDEX_PARSER_VERSION,
            "metadata": {
                "full_name": _text(data.get("indexFullNameCn")),
                "english_name": _text(data.get("indexFullNameEn")),
                "publisher": "CSIndex",
                "publish_channel": _text(data.get("publishChannelCn")),
                "base_date": _text(data.get("basicDate")),
                "base_point": _text(data.get("basicIndex")),
                "index_type": _text(data.get("indexType")),
                "ric": _text(data.get("ric")),
                "bloomberg": _text(data.get("bloombergid")),
                "description": _text(data.get("indexCnDesc")),
            },
        }

    async def get_basic_info(self, code: str) -> Dict[str, Any]:
        normalized = normalize_index_code(code)
        if not normalized:
            return {}
        url = self.basic_info_url.format(code=normalized)
        await self.rate_limiter.acquire()
        raw = await asyncio.to_thread(self._fetch_bytes, url)

        payload = json.loads(raw.decode("utf-8", errors="ignore"))
        return self.parse_basic_info(payload, source_url=url)

    @staticmethod
    def parse_fuzzy_search_response(payload: Dict[str, Any], *, source_url: str) -> OfficialIndexSnapshot:
        data = payload.get("data") if isinstance(payload, dict) else []
        rows: List[Dict[str, Any]] = []
        for item in data or []:
            if not isinstance(item, dict):
                continue
            code = normalize_index_code(item.get("indexCode"))
            if not code:
                continue
            rows.append(
                {
                    "instrument_id": csindex_instrument_id(code),
                    "symbol": code,
                    "name": _text(item.get("indexName") or code),
                    "exchange": "SSE",
                    "type": "index",
                    "currency": "CNY",
                    "listed_date": None,
                    "delisted_date": None,
                    "status": "active",
                    "is_active": True,
                    "is_st": False,
                    "trading_status": 1,
                    "source": "csindex",
                    "source_symbol": code,
                    "official_lifecycle_source": "csindex_fuzzy_search",
                    "source_url": source_url,
                    "parser_version": OFFICIAL_INDEX_PARSER_VERSION,
                    "metadata": {
                        "english_name": _text(item.get("indexNameEn")),
                        "publisher": "CSIndex",
                    },
                }
            )
        return OfficialIndexSnapshot(
            source="csindex_fuzzy_search",
            source_url=source_url,
            parser_version=OFFICIAL_INDEX_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash(json.dumps(payload, ensure_ascii=False, sort_keys=True)),
            rows=rows,
            diagnostics={
                "row_count": len(rows),
                "total": payload.get("total") if isinstance(payload, dict) else None,
                "page_size": payload.get("pageSize") if isinstance(payload, dict) else None,
                "current_page": payload.get("currentPage") if isinstance(payload, dict) else None,
            },
        )

    async def get_index_master_snapshot(self) -> OfficialIndexSnapshot:
        page_size = max(1, min(self.list_page_size, 1000))
        page_num = 1
        rows_by_id: Dict[str, Dict[str, Any]] = {}
        diagnostics: Dict[str, Any] = {"pages": 0, "total": None}
        payload_hash_parts: List[str] = []

        while True:
            url = f"{self.fuzzy_search_url}?searchInput=&pageNum={page_num}&pageSize={page_size}"
            await self.rate_limiter.acquire()
            raw = await asyncio.to_thread(self._fetch_bytes, url)
            payload = json.loads(raw.decode("utf-8", errors="ignore"))
            snapshot = self.parse_fuzzy_search_response(payload, source_url=url)
            added = 0
            for row in snapshot.rows:
                key = row.get("instrument_id")
                if key and key not in rows_by_id:
                    rows_by_id[key] = row
                    added += 1
            payload_hash_parts.append(snapshot.raw_snapshot_hash)
            diagnostics["pages"] = page_num
            diagnostics["total"] = snapshot.diagnostics.get("total")
            total = snapshot.diagnostics.get("total")
            if not snapshot.rows:
                break
            if total is None or len(rows_by_id) >= int(total):
                break
            if added == 0:
                break
            page_num += 1

        rows = list(rows_by_id.values())
        return OfficialIndexSnapshot(
            source="csindex_fuzzy_search",
            source_url=self.fuzzy_search_url,
            parser_version=OFFICIAL_INDEX_PARSER_VERSION,
            raw_snapshot_hash=_snapshot_hash("|".join(payload_hash_parts)),
            rows=rows,
            diagnostics={**diagnostics, "row_count": len(rows)},
        )

    async def get_instrument_list(
        self,
        exchange: str = None,
        instrument_types: List[str] = None,
    ) -> List[Dict[str, Any]]:
        if exchange and exchange.upper() not in ("SSE", "SZSE"):
            return []
        if instrument_types and "index" not in {str(item).lower() for item in instrument_types}:
            return []
        snapshot = await self.get_index_master_snapshot()
        return snapshot.rows

    @staticmethod
    def parse_daily_response(
        payload: Dict[str, Any],
        *,
        instrument_id: str,
        source_symbol: str,
    ) -> List[Dict[str, Any]]:
        rows = payload.get("data") if isinstance(payload, dict) else []
        normalized: List[Dict[str, Any]] = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            try:
                normalized.append(
                    {
                        "instrument_id": instrument_id,
                        "time": pd.to_datetime(row.get("tradeDate"), format="%Y%m%d").to_pydatetime(),
                        "open": float(row.get("open")),
                        "high": float(row.get("high")),
                        "low": float(row.get("low")),
                        "close": float(row.get("close")),
                        "volume": int(float(row.get("tradingVol") or 0)),
                        "amount": float(row.get("tradingValue") or 0) * 100000000,
                        "change": float(row.get("change") or 0),
                        "pct_change": float(row.get("changePct") or 0),
                        "tradestatus": 1,
                        "factor": 1.0,
                        "adjustment_type": "none",
                        "is_complete": True,
                        "quality_score": 0.97,
                        "source": "csindex",
                        "batch_id": f"csindex_{source_symbol}",
                    }
                )
            except Exception:
                continue
        return sorted(normalized, key=lambda item: item["time"])

    async def get_daily_data(
        self,
        instrument_id: str,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        instrument_type: str = "stock",
        source_symbol: str = "",
    ) -> List[Dict[str, Any]]:
        if (instrument_type or "stock").lower() != "index":
            return []
        code = normalize_index_code(source_symbol or symbol or instrument_id)
        if not code:
            return []
        url = (
            f"{self.daily_url}?indexCode={code}"
            f"&startDate={start_date.strftime('%Y%m%d')}"
            f"&endDate={end_date.strftime('%Y%m%d')}"
        )
        await self.rate_limiter.acquire()
        raw = await asyncio.to_thread(self._fetch_bytes, url)
        payload = json.loads(raw.decode("utf-8", errors="ignore"))
        return self.parse_daily_response(payload, instrument_id=instrument_id, source_symbol=code)

    async def get_latest_daily_data(self, instrument_id: str, symbol: str) -> Dict[str, Any]:
        rows = await self.get_daily_data(
            instrument_id,
            symbol,
            datetime.combine(date.today(), datetime.min.time()),
            datetime.combine(date.today(), datetime.max.time()),
            instrument_type="index",
        )
        return rows[-1] if rows else {}
