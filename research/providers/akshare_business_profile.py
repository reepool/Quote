"""Free structured A-share business-profile sources exposed through AkShare."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any, Callable, Mapping, Optional, Sequence

from utils.date_utils import get_shanghai_time
from utils.http_transport import HttpTlsConfig, create_requests_session

from .akshare_support import load_akshare


COMPOSITION_SOURCE = "eastmoney_main_composition"
INTRODUCTION_SOURCE = "ths_main_business_intro"
STRUCTURED_BUSINESS_PROFILE_SOURCES = {
    COMPOSITION_SOURCE,
    INTRODUCTION_SOURCE,
}
COMPOSITION_CLASSIFICATIONS = {
    "按产品分类": "product",
    "按行业分类": "industry",
    "按地区分类": "geography",
}
EASTMONEY_COMPOSITION_ENDPOINT = (
    "https://emweb.securities.eastmoney.com/PC_HSF10/BusinessAnalysis/PageAjax"
)
THS_INTRODUCTION_URL_TEMPLATE = "https://basic.10jqka.com.cn/new/{symbol}/operate.html"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


@dataclass(frozen=True)
class BusinessCompositionRow:
    """One source-reported business-composition row without semantic inference."""

    instrument_id: str
    report_period: str
    classification_type: str
    item_name: str
    revenue: Optional[float]
    revenue_ratio: Optional[float]
    cost: Optional[float]
    cost_ratio: Optional[float]
    profit: Optional[float]
    profit_ratio: Optional[float]
    gross_margin: Optional[float]
    source_row_hash: str


@dataclass(frozen=True)
class BusinessIntroduction:
    """Current source-reported company introduction fields."""

    instrument_id: str
    main_business: Optional[str]
    product_types: Optional[str]
    product_names: Optional[str]
    business_scope: Optional[str]
    source_row_hash: str


@dataclass(frozen=True)
class StructuredSourceResult:
    source: str
    status: str
    payload_hash: Optional[str]
    rows: tuple[BusinessCompositionRow, ...] = ()
    introduction: Optional[BusinessIntroduction] = None
    raw_payload: tuple[dict[str, Any], ...] = ()
    diagnostics: tuple[str, ...] = ()
    elapsed_seconds: float = 0.0


@dataclass(frozen=True)
class StructuredBusinessProfileSnapshot:
    instrument_id: str
    observed_at: str
    composition: StructuredSourceResult
    introduction: StructuredSourceResult

    @property
    def status(self) -> str:
        statuses = {
            status
            for status in (self.composition.status, self.introduction.status)
            if status != "skipped"
        }
        if not statuses:
            return "skipped"
        if statuses == {"success"}:
            return "success"
        if statuses == {"empty"}:
            return "empty"
        if statuses & {"success", "empty"}:
            return "degraded"
        return "failed"


class AkshareStructuredBusinessProfileProvider:
    """Read free structured labels and numeric composition from public aggregators.

    The provider deliberately does not infer products, value-chain positions,
    customers, suppliers, or commodity directions. Those are separate governed
    review decisions.
    """

    def __init__(
        self,
        *,
        akshare_module: Any = None,
        mode: str = "direct",
        possible_row_cap: int = 200,
        request_timeout_seconds: float = 20.0,
        request_interval_seconds: float = 0.5,
        retry_attempts: int = 2,
        retry_backoff_seconds: float = 1.0,
        session: Any = None,
    ):
        self._akshare_module = akshare_module
        self.mode = str(mode or "direct").strip().lower()
        if self.mode not in {"direct", "proxy_patch"}:
            raise ValueError(f"unsupported AkShare business-profile mode: {mode}")
        self.possible_row_cap = max(1, int(possible_row_cap))
        self.request_timeout_seconds = max(1.0, float(request_timeout_seconds))
        self.request_interval_seconds = max(0.0, float(request_interval_seconds))
        self.retry_attempts = max(1, int(retry_attempts))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self._session = session
        self._last_request_started_at = 0.0
        self._tls_config = HttpTlsConfig(source_name="business_profile_structured")

    async def fetch(
        self,
        instrument_id: str,
        *,
        observed_at: Optional[str] = None,
        sources: Optional[Sequence[str]] = None,
        deadline_monotonic: Optional[float] = None,
    ) -> StructuredBusinessProfileSnapshot:
        selected_sources = _normalize_source_scope(sources)
        return await asyncio.to_thread(
            self._fetch_sync,
            _normalize_instrument_id(instrument_id),
            observed_at or get_shanghai_time().isoformat(),
            selected_sources,
            deadline_monotonic,
        )

    def _fetch_sync(
        self,
        instrument_id: str,
        observed_at: str,
        sources: frozenset[str],
        deadline_monotonic: Optional[float],
    ) -> StructuredBusinessProfileSnapshot:
        owned_session = None
        if self._akshare_module is not None:
            composition_loader = lambda _timeout: self._akshare_module.stock_zygc_em(
                symbol=_eastmoney_symbol(instrument_id)
            )
            introduction_loader = lambda _timeout: self._akshare_module.stock_zyjs_ths(
                symbol=instrument_id.split(".", 1)[0]
            )
        else:
            if self.mode == "proxy_patch":
                load_akshare(self.mode)
            session = self._session
            if session is None:
                session = create_requests_session(tls_config=self._tls_config)
                owned_session = session
            composition_loader = lambda timeout: self._request_eastmoney_composition(
                session,
                instrument_id,
                timeout_seconds=timeout,
            )
            introduction_loader = lambda timeout: self._request_ths_introduction(
                session,
                instrument_id,
                timeout_seconds=timeout,
            )
        try:
            composition = (
                self._fetch_composition(
                    instrument_id,
                    composition_loader,
                    deadline_monotonic=deadline_monotonic,
                )
                if COMPOSITION_SOURCE in sources
                else _skipped_source(COMPOSITION_SOURCE)
            )
            introduction = (
                self._fetch_introduction(
                    instrument_id,
                    introduction_loader,
                    deadline_monotonic=deadline_monotonic,
                )
                if INTRODUCTION_SOURCE in sources
                else _skipped_source(INTRODUCTION_SOURCE)
            )
        finally:
            if owned_session is not None:
                owned_session.close()
        return StructuredBusinessProfileSnapshot(
            instrument_id=instrument_id,
            observed_at=_normalize_datetime(observed_at),
            composition=composition,
            introduction=introduction,
        )

    def _fetch_composition(
        self,
        instrument_id: str,
        loader: Callable[[float], Any],
        *,
        deadline_monotonic: Optional[float],
    ) -> StructuredSourceResult:
        started_at = time.perf_counter()
        try:
            frame = self._call_with_retry(
                loader,
                deadline_monotonic=deadline_monotonic,
            )
            raw_rows = _frame_records(frame)
            rows = normalize_composition_rows(raw_rows, instrument_id=instrument_id)
            diagnostics: list[str] = []
            if len(raw_rows) >= self.possible_row_cap:
                diagnostics.append("possible_source_row_cap")
            if not rows:
                diagnostics.append("empty_normalized_composition")
            return StructuredSourceResult(
                source=COMPOSITION_SOURCE,
                status="success" if rows else "empty",
                payload_hash=_payload_hash(raw_rows),
                rows=rows,
                raw_payload=tuple(raw_rows),
                diagnostics=tuple(diagnostics),
                elapsed_seconds=time.perf_counter() - started_at,
            )
        except Exception as exc:
            return StructuredSourceResult(
                source=COMPOSITION_SOURCE,
                status="failed",
                payload_hash=None,
                diagnostics=(f"{type(exc).__name__}:{exc}",),
                elapsed_seconds=time.perf_counter() - started_at,
            )

    def _fetch_introduction(
        self,
        instrument_id: str,
        loader: Callable[[float], Any],
        *,
        deadline_monotonic: Optional[float],
    ) -> StructuredSourceResult:
        started_at = time.perf_counter()
        try:
            frame = self._call_with_retry(
                loader,
                deadline_monotonic=deadline_monotonic,
            )
            raw_rows = _frame_records(frame)
            introduction = normalize_introduction_rows(
                raw_rows,
                instrument_id=instrument_id,
            )
            diagnostics = () if introduction else ("empty_normalized_introduction",)
            return StructuredSourceResult(
                source=INTRODUCTION_SOURCE,
                status="success" if introduction else "empty",
                payload_hash=_payload_hash(raw_rows),
                introduction=introduction,
                raw_payload=tuple(raw_rows),
                diagnostics=diagnostics,
                elapsed_seconds=time.perf_counter() - started_at,
            )
        except Exception as exc:
            return StructuredSourceResult(
                source=INTRODUCTION_SOURCE,
                status="failed",
                payload_hash=None,
                diagnostics=(f"{type(exc).__name__}:{exc}",),
                elapsed_seconds=time.perf_counter() - started_at,
            )

    def _call_with_retry(
        self,
        loader: Callable[[float], Any],
        *,
        deadline_monotonic: Optional[float],
    ) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.retry_attempts + 1):
            self._wait_for_request_slot(deadline_monotonic=deadline_monotonic)
            request_timeout = self.request_timeout_seconds
            if deadline_monotonic is not None:
                remaining = deadline_monotonic - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("structured business-profile deadline exhausted")
                request_timeout = min(request_timeout, remaining)
            try:
                result = loader(max(0.001, request_timeout))
                if (
                    deadline_monotonic is not None
                    and time.monotonic() >= deadline_monotonic
                ):
                    raise TimeoutError("structured business-profile deadline exhausted")
                return result
            except Exception as exc:
                last_error = exc
                if attempt >= self.retry_attempts:
                    break
                backoff = self.retry_backoff_seconds * attempt
                if deadline_monotonic is not None:
                    remaining = deadline_monotonic - time.monotonic()
                    if remaining <= backoff:
                        raise TimeoutError(
                            "structured business-profile deadline exhausted"
                        ) from exc
                if backoff > 0:
                    time.sleep(backoff)
        raise RuntimeError(
            f"structured business-profile request failed after "
            f"{self.retry_attempts} attempts: {last_error}"
        ) from last_error

    def _wait_for_request_slot(
        self,
        *,
        deadline_monotonic: Optional[float],
    ) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_started_at
        if (
            self._last_request_started_at > 0
            and elapsed < self.request_interval_seconds
        ):
            wait_seconds = self.request_interval_seconds - elapsed
            if (
                deadline_monotonic is not None
                and deadline_monotonic - now <= wait_seconds
            ):
                raise TimeoutError("structured business-profile deadline exhausted")
            time.sleep(wait_seconds)
        self._last_request_started_at = time.monotonic()

    def _request_eastmoney_composition(
        self,
        session: Any,
        instrument_id: str,
        *,
        timeout_seconds: float,
    ) -> list[dict[str, Any]]:
        response = session.get(
            EASTMONEY_COMPOSITION_ENDPOINT,
            params={"code": _eastmoney_symbol(instrument_id)},
            headers={"User-Agent": USER_AGENT},
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, Mapping):
            raise ValueError("Eastmoney composition response root must be an object")
        rows = payload.get("zygcfx")
        if not isinstance(rows, list):
            raise ValueError("Eastmoney composition response is missing zygcfx rows")
        return [
            _normalize_eastmoney_api_row(row)
            for row in rows
            if isinstance(row, Mapping)
        ]

    def _request_ths_introduction(
        self,
        session: Any,
        instrument_id: str,
        *,
        timeout_seconds: float,
    ) -> list[dict[str, Any]]:
        from bs4 import BeautifulSoup

        symbol = instrument_id.split(".", 1)[0]
        response = session.get(
            THS_INTRODUCTION_URL_TEMPLATE.format(symbol=symbol),
            headers={"User-Agent": USER_AGENT},
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        response.encoding = "gb2312"
        soup = BeautifulSoup(response.text, "lxml")
        container = soup.find("ul", attrs={"class": "main_intro_list"})
        if container is None:
            raise ValueError("THS introduction response is missing main_intro_list")
        values: dict[str, Any] = {"股票代码": symbol}
        for item in container.find_all("li"):
            text = item.get_text(strip=True)
            if "：" not in text:
                continue
            key, value = text.split("：", maxsplit=1)
            normalized_value = "".join(value.split())
            if key.strip() and normalized_value:
                values[key.strip()] = normalized_value
        return [values] if len(values) > 1 else []


def _normalize_eastmoney_api_row(row: Mapping[str, Any]) -> dict[str, Any]:
    if "报告日期" in row:
        return dict(row)
    classification = {
        "1": "按行业分类",
        "2": "按产品分类",
        "3": "按地区分类",
    }.get(str(row.get("MAINOP_TYPE") or "").strip())
    return {
        "股票代码": row.get("SECURITY_CODE"),
        "报告日期": row.get("REPORT_DATE"),
        "分类类型": classification,
        "主营构成": row.get("ITEM_NAME"),
        "主营收入": row.get("MAIN_BUSINESS_INCOME"),
        "收入比例": row.get("MBI_RATIO"),
        "主营成本": row.get("MAIN_BUSINESS_COST"),
        "成本比例": row.get("MBC_RATIO"),
        "主营利润": row.get("MAIN_BUSINESS_RPOFIT"),
        "利润比例": row.get("MBR_RATIO"),
        "毛利率": row.get("GROSS_RPOFIT_RATIO"),
    }


def normalize_composition_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    instrument_id: str,
) -> tuple[BusinessCompositionRow, ...]:
    """Normalize the documented ``stock_zygc_em`` columns."""
    normalized_instrument = _normalize_instrument_id(instrument_id)
    output: list[BusinessCompositionRow] = []
    for row in rows:
        _validate_source_security_code(
            row.get("股票代码"),
            instrument_id=normalized_instrument,
        )
        report_period = _normalize_date(row.get("报告日期"))
        classification = COMPOSITION_CLASSIFICATIONS.get(
            _optional_text(row.get("分类类型")) or ""
        )
        item_name = _optional_text(row.get("主营构成"))
        if not report_period or not classification or not item_name:
            continue
        semantic_row = {
            "instrument_id": normalized_instrument,
            "report_period": report_period,
            "classification_type": classification,
            "item_name": item_name,
            "revenue": _optional_float(row.get("主营收入")),
            "revenue_ratio": _optional_float(row.get("收入比例")),
            "cost": _optional_float(row.get("主营成本")),
            "cost_ratio": _optional_float(row.get("成本比例")),
            "profit": _optional_float(row.get("主营利润")),
            "profit_ratio": _optional_float(row.get("利润比例")),
            "gross_margin": _optional_float(row.get("毛利率")),
        }
        output.append(
            BusinessCompositionRow(
                **semantic_row,
                source_row_hash=_payload_hash(semantic_row),
            )
        )
    output.sort(
        key=lambda item: (
            item.report_period,
            item.classification_type,
            item.item_name,
            item.source_row_hash,
        )
    )
    return tuple(output)


def normalize_introduction_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    instrument_id: str,
) -> Optional[BusinessIntroduction]:
    """Normalize current THS introduction fields as raw review context."""
    if not rows:
        return None
    row = rows[0]
    normalized_instrument = _normalize_instrument_id(instrument_id)
    _validate_source_security_code(
        row.get("股票代码"),
        instrument_id=normalized_instrument,
    )
    values = {
        "instrument_id": normalized_instrument,
        "main_business": _first_text(row, ("主营业务", "主营介绍")),
        "product_types": _first_text(row, ("产品类型",)),
        "product_names": _first_text(row, ("产品名称", "主要产品")),
        "business_scope": _first_text(row, ("经营范围",)),
    }
    if not any(value for key, value in values.items() if key != "instrument_id"):
        return None
    return BusinessIntroduction(
        **values,
        source_row_hash=_payload_hash(values),
    )


def _frame_records(frame: Any) -> list[dict[str, Any]]:
    if frame is None:
        return []
    if hasattr(frame, "to_dict"):
        records = frame.to_dict(orient="records")
    elif isinstance(frame, Sequence) and not isinstance(frame, (str, bytes)):
        records = list(frame)
    else:
        raise TypeError("structured business-profile source returned unsupported data")
    return [
        {str(key): _json_value(value) for key, value in dict(row).items()}
        for row in records
        if isinstance(row, Mapping)
    ]


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    try:
        if math.isnan(float(value)):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    return value


def _payload_hash(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalize_source_scope(
    sources: Optional[Sequence[str]],
) -> frozenset[str]:
    if sources is None:
        return frozenset(STRUCTURED_BUSINESS_PROFILE_SOURCES)
    selected = frozenset(
        str(source or "").strip() for source in sources if str(source or "").strip()
    )
    if not selected:
        raise ValueError("structured business-profile source scope must not be empty")
    unsupported = selected - STRUCTURED_BUSINESS_PROFILE_SOURCES
    if unsupported:
        raise ValueError(
            f"unsupported structured business-profile sources: "
            f"{sorted(unsupported)}"
        )
    return selected


def _skipped_source(source: str) -> StructuredSourceResult:
    return StructuredSourceResult(
        source=source,
        status="skipped",
        payload_hash=None,
        diagnostics=("not_in_source_scope",),
    )


def _normalize_instrument_id(value: str) -> str:
    text = str(value or "").strip().upper()
    if len(text) != 9 or text[6:] not in {".SH", ".SZ", ".BJ"}:
        raise ValueError(f"unsupported A-share instrument_id: {value}")
    if not text[:6].isdigit():
        raise ValueError(f"unsupported A-share instrument_id: {value}")
    return text


def _validate_source_security_code(
    value: Any,
    *,
    instrument_id: str,
) -> None:
    if value is None or str(value).strip() in {"", "None", "nan", "--"}:
        return
    text = str(value).strip().upper()
    expected_code = instrument_id[:6]
    source_code: Optional[str] = None
    if text.isdigit():
        source_code = text.zfill(6)
    elif text.endswith(".0") and text[:-2].isdigit():
        source_code = text[:-2].zfill(6)
    elif len(text) == 8 and text[:2] in {"SH", "SZ", "BJ"}:
        source_code = text[2:]
    elif len(text) == 9 and text[6:] in {".SH", ".SZ", ".BJ"}:
        source_code = text[:6]
    if source_code is None or not source_code.isdigit():
        raise ValueError(f"unsupported source security code: {value}")
    if source_code != expected_code:
        raise ValueError(
            "structured business-profile source security mismatch: "
            f"expected={expected_code}, actual={source_code}"
        )


def _eastmoney_symbol(instrument_id: str) -> str:
    code, suffix = instrument_id.split(".", 1)
    return f"{suffix}{code}"


def _normalize_datetime(value: Any) -> str:
    text = str(value or "").strip()
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).isoformat()
    except ValueError as exc:
        raise ValueError(f"observed_at must be an ISO datetime: {value}") from exc


def _normalize_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        return None


def _optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return None if text in {"", "None", "nan", "--"} else text


def _first_text(row: Mapping[str, Any], aliases: Sequence[str]) -> Optional[str]:
    for alias in aliases:
        if alias in row:
            value = _optional_text(row.get(alias))
            if value:
                return value
    return None


def _optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).replace(",", "").replace("%", "").strip()
    if text in {"", "None", "nan", "--"}:
        return None
    try:
        parsed = float(text)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def source_result_to_dict(result: StructuredSourceResult) -> dict[str, Any]:
    """Serialize a source result for raw-payload storage and diagnostics."""
    return asdict(result)
