"""
SWS Research direct industry index-analysis provider.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from utils import dm_logger
from utils.http_transport import (
    HttpTlsConfig,
    create_requests_session,
    resolve_requests_verify,
)

from .base import BaseIndustryIndexAnalysisProvider, IndustryIndexAnalysisSnapshot


class SWSResearchIndexAnalysisProvider(BaseIndustryIndexAnalysisProvider):
    """Fetch official Shenwan index-level market metrics.

    The web frontend currently serves latest daily rows through
    ``day_week_month_report``. Date filters are not trusted here because the
    observed endpoint echoes the latest trade date even when historical date
    parameters are supplied.
    """

    source_name = "swsresearch_index_analysis_direct"
    supported_modes = {"direct"}

    REQUIRED_FIELDS = ("swindexcode", "swindexname", "bargaindate")
    NUMERIC_FIELDS = {
        "closeindex": "close_index",
        "bargainamount": "bargain_volume",
        "markup": "markup",
        "turnoverrate": "turnover_rate",
        "pe": "pe",
        "pb": "pb",
        "meanprice": "mean_price",
        "bargainsumrate": "bargain_sum_rate",
        "negotiablessharesum1": "negotiable_share_sum",
        "negotiablessharesum2": "average_negotiable_share_sum",
        "dp": "dividend_yield",
    }

    def __init__(
        self,
        *,
        endpoint: str = (
            "https://www.swsresearch.com/institute-sw/api/index_analysis/"
            "day_week_month_report/"
        ),
        taxonomy_system: str = "sw",
        taxonomy_version: str = "sw_2021",
        index_types: Optional[List[str]] = None,
        request_timeout_seconds: float = 20.0,
        retry_attempts: int = 2,
        retry_backoff_seconds: float = 0.5,
        page_size: int = 200,
        max_pages_per_type: int = 10,
        extra_ca_cert_path: Optional[str] = None,
    ):
        self.endpoint = endpoint
        self.taxonomy_system = taxonomy_system
        self.taxonomy_version = taxonomy_version
        self.index_types = list(
            index_types
            or ["市场表征", "一级行业", "二级行业", "三级行业", "风格指数"]
        )
        self.request_timeout_seconds = max(1.0, float(request_timeout_seconds))
        self.retry_attempts = max(1, int(retry_attempts))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self.page_size = max(1, int(page_size))
        self.max_pages_per_type = max(1, int(max_pages_per_type))
        self.tls_config = HttpTlsConfig(
            source_name=self.source_name,
            extra_ca_cert_path=extra_ca_cert_path,
        )
        self.request_verify = resolve_requests_verify(self.tls_config)

    async def fetch_latest_index_analysis(
        self,
        *,
        mode: str = "direct",
        index_types: Optional[List[str]] = None,
        limit_per_type: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        latest_date: Optional[str] = None,
    ) -> List[IndustryIndexAnalysisSnapshot]:
        if not self.supports_mode(mode):
            raise ValueError(f"Unsupported SWS index-analysis mode: {mode}")
        return await asyncio.to_thread(
            self._fetch_latest_index_analysis_sync,
            list(index_types or self.index_types),
            limit_per_type,
            start_date,
            end_date,
            latest_date,
        )

    def _fetch_latest_index_analysis_sync(
        self,
        index_types: List[str],
        limit_per_type: Optional[int],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        latest_date: Optional[str] = None,
    ) -> List[IndustryIndexAnalysisSnapshot]:
        session = create_requests_session(tls_config=self.tls_config)
        rows: List[IndustryIndexAnalysisSnapshot] = []
        requested_start = _normalize_optional_date(start_date)
        requested_end = _normalize_optional_date(end_date)
        requested_latest = _normalize_optional_date(latest_date)
        for index_type in index_types:
            dm_logger.info(
                "[SWSResearchIndexAnalysis] Fetching index_type=%s "
                "(limit=%s, start_date=%s, end_date=%s, latest_date=%s)",
                index_type,
                limit_per_type,
                requested_start,
                requested_end,
                requested_latest,
            )
            before_count = len(rows)
            rows.extend(
                self._fetch_index_type(
                    session,
                    index_type=index_type,
                    limit=limit_per_type,
                    start_date=requested_start,
                    end_date=requested_end,
                    latest_date=requested_latest,
                )
            )
            dm_logger.info(
                "[SWSResearchIndexAnalysis] Finished index_type=%s (rows=%s)",
                index_type,
                len(rows) - before_count,
            )
        return rows

    def _fetch_index_type(
        self,
        session: requests.Session,
        *,
        index_type: str,
        limit: Optional[int],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        latest_date: Optional[str] = None,
    ) -> List[IndustryIndexAnalysisSnapshot]:
        headers = {
            "Accept": "application/json",
            "Referer": "https://www.swsresearch.com/institute_sw/home",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
            ),
        }
        snapshots: List[IndustryIndexAnalysisSnapshot] = []
        page = 1
        while page <= self.max_pages_per_type:
            params = {
                "type": "DAY",
                "index_type": index_type,
                "page": page,
                "page_size": self.page_size,
            }
            if start_date:
                params["start_date"] = start_date
                params["startDate"] = start_date
            if end_date:
                params["end_date"] = end_date
                params["endDate"] = end_date
            if latest_date:
                params["bargaindate"] = latest_date
            payload = self._request_json(session, params=params, headers=headers)
            if str(payload.get("code")) != "200":
                raise ValueError(
                    f"SWS index-analysis returned non-ok code for {index_type}: {payload}"
                )
            data = payload.get("data") or {}
            result_rows = data.get("results") or []
            dm_logger.info(
                "[SWSResearchIndexAnalysis] Page fetched "
                "(index_type=%s, page=%s, result_rows=%s, has_next=%s)",
                index_type,
                page,
                len(result_rows),
                bool(data.get("next")),
            )
            for row in result_rows:
                snapshot = self._parse_row(row, index_type=index_type)
                if not _date_in_requested_range(
                    snapshot.trade_date,
                    start_date=start_date,
                    end_date=end_date,
                    latest_date=latest_date,
                ):
                    continue
                snapshots.append(snapshot)
                if limit is not None and len(snapshots) >= int(limit):
                    return snapshots
            if not data.get("next") or not result_rows:
                break
            page += 1
        return snapshots

    def _request_json(
        self,
        session: requests.Session,
        *,
        params: Dict[str, Any],
        headers: Dict[str, str],
    ) -> Dict[str, Any]:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.retry_attempts + 1):
            try:
                response = session.get(
                    self.endpoint,
                    params=params,
                    headers=headers,
                    timeout=self.request_timeout_seconds,
                    verify=self.request_verify,
                )
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt >= self.retry_attempts:
                    break
                if self.retry_backoff_seconds > 0:
                    time.sleep(self.retry_backoff_seconds * attempt)
        raise RuntimeError(
            f"SWS index-analysis request failed after {self.retry_attempts} attempts: "
            f"{last_error}"
        )

    def _parse_row(
        self,
        row: Dict[str, Any],
        *,
        index_type: str,
    ) -> IndustryIndexAnalysisSnapshot:
        missing = [
            field
            for field in self.REQUIRED_FIELDS
            if not str(row.get(field) or "").strip()
        ]
        if missing:
            raise ValueError(f"SWS index-analysis row missing fields: {missing}")

        metrics = {
            target: _to_float(row.get(source))
            for source, target in self.NUMERIC_FIELDS.items()
        }
        return IndustryIndexAnalysisSnapshot(
            taxonomy_system=self.taxonomy_system,
            taxonomy_version=self.taxonomy_version,
            sw_index_code=str(row.get("swindexcode")).strip(),
            sw_index_name=str(row.get("swindexname")).strip(),
            trade_date=_to_trade_date(row.get("bargaindate")),
            index_type=index_type,
            source=self.source_name,
            source_mode="direct",
            raw_payload=dict(row),
            **metrics,
        )


def _to_float(value: Any) -> Optional[float]:
    text = str(value or "").strip().replace(",", "")
    if text in {"", "-", "--", "nan", "None"}:
        return None
    return float(text)


def _to_trade_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("SWS index-analysis row has empty bargaindate")
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).date().isoformat()
    except ValueError:
        return text[:10]


def _normalize_optional_date(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return _to_trade_date(text)


def _date_in_requested_range(
    trade_date: str,
    *,
    start_date: Optional[str],
    end_date: Optional[str],
    latest_date: Optional[str],
) -> bool:
    if latest_date and trade_date != latest_date:
        return False
    if start_date and trade_date < start_date:
        return False
    if end_date and trade_date > end_date:
        return False
    return True
