"""
AkShare-backed authoritative Shenwan industry provider.
"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field, replace
from io import StringIO
from typing import Any, Dict, List, Optional

import pandas as pd

from utils import dm_logger
from utils.http_transport import HttpTlsConfig, request_get

from .akshare_support import load_akshare
from .base import (
    BaseIndustryStandardProvider,
    IndustrySnapshot,
    IndustryTaxonomySnapshot,
    get_leaf_taxonomy_nodes,
)


@dataclass(frozen=True)
class _ShenwanTaxonomyCache:
    """In-memory Shenwan taxonomy cache built from AkShare."""

    taxonomy_nodes: List[IndustryTaxonomySnapshot]
    l1_by_code: Dict[str, IndustryTaxonomySnapshot]
    l1_by_name: Dict[str, IndustryTaxonomySnapshot]
    l2_by_code: Dict[str, IndustryTaxonomySnapshot]
    l2_by_name: Dict[str, IndustryTaxonomySnapshot]
    l3_by_code: Dict[str, IndustryTaxonomySnapshot]
    l3_by_name: Dict[str, IndustryTaxonomySnapshot]


@dataclass(frozen=True)
class _ShenwanFetchDiagnostics:
    """Diagnostics for the latest Shenwan membership fetch."""

    exchange: str
    requested_instruments: int = 0
    attempted_third_codes: int = 0
    successful_third_codes: int = 0
    failed_third_codes: int = 0
    matched_instruments: int = 0
    missing_instruments: int = 0
    aborted_early: bool = False
    abort_reason: Optional[str] = None
    failed_code_samples: List[Dict[str, str]] = field(default_factory=list)
    missing_instrument_ids: List[str] = field(default_factory=list)


class AkshareShenwanIndustryProvider(BaseIndustryStandardProvider):
    """Fetch authoritative Shenwan taxonomy and memberships through AkShare."""

    source_name = "akshare"
    supported_modes = {"direct", "proxy_patch"}

    def __init__(
        self,
        *,
        taxonomy_system: str = "sw",
        taxonomy_version: str = "sw_2021",
        constituent_base_url: str = "https://legulegu.com/stockdata/index-composition",
        constituent_request_timeout_seconds: float = 8.0,
        max_constituent_fetch_seconds: Optional[float] = None,
        max_failed_constituent_pages: Optional[int] = 25,
        failed_code_sample_limit: int = 10,
    ):
        self.taxonomy_system = taxonomy_system
        self.taxonomy_version = taxonomy_version
        self.constituent_base_url = constituent_base_url
        self.constituent_request_timeout_seconds = max(
            1.0,
            float(constituent_request_timeout_seconds),
        )
        self.max_constituent_fetch_seconds = (
            None
            if max_constituent_fetch_seconds is None
            else max(1.0, float(max_constituent_fetch_seconds))
        )
        self.max_failed_constituent_pages = max_failed_constituent_pages
        self.failed_code_sample_limit = max(1, failed_code_sample_limit)
        self.tls_config = HttpTlsConfig(source_name=self.source_name)
        self._taxonomy_cache: Optional[_ShenwanTaxonomyCache] = None
        self._last_fetch_metadata: Dict[str, Any] = {}

    async def fetch_taxonomy(
        self,
        *,
        mode: str = "direct",
    ) -> List[IndustryTaxonomySnapshot]:
        if not self.supports_mode(mode):
            return []

        return await asyncio.to_thread(self._fetch_taxonomy_sync, mode)

    async def fetch_industries(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[IndustrySnapshot]:
        if not self.supports_mode(mode):
            return []

        target_instruments = [
            instrument
            for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("exchange") == exchange
        ]
        if limit is not None:
            target_instruments = target_instruments[:limit]

        if not target_instruments:
            return []

        return await asyncio.to_thread(
            self._fetch_industries_sync,
            target_instruments,
            mode,
        )

    async def fetch_component_sets(
        self,
        *,
        taxonomy_nodes: Optional[List[IndustryTaxonomySnapshot]] = None,
        mode: str = "direct",
    ) -> Dict[str, set[str]]:
        if not self.supports_mode(mode):
            return {}

        return await asyncio.to_thread(
            self._fetch_component_sets_sync,
            taxonomy_nodes,
            mode,
        )

    def _fetch_taxonomy_sync(self, mode: str) -> List[IndustryTaxonomySnapshot]:
        cache = self._get_taxonomy_cache()
        if mode == "direct":
            return list(cache.taxonomy_nodes)

        return [replace(node, source_mode=mode) for node in cache.taxonomy_nodes]

    def _fetch_industries_sync(
        self,
        target_instruments: List[Dict[str, Any]],
        mode: str,
    ) -> List[IndustrySnapshot]:
        cache = self._get_taxonomy_cache()
        leaf_nodes = get_leaf_taxonomy_nodes(list(cache.taxonomy_nodes), minimum_level=2)
        target_lookup = self._build_target_lookup(target_instruments)
        remaining_ids = {instrument["instrument_id"] for instrument in target_instruments}
        snapshots: List[IndustrySnapshot] = []
        failed_code_samples: List[Dict[str, str]] = []
        diagnostics = _ShenwanFetchDiagnostics(
            exchange=str(target_instruments[0].get("exchange", "")) if target_instruments else "",
            requested_instruments=len(target_instruments),
        )
        load_akshare(mode)
        started_at = time.monotonic()

        for leaf_node in leaf_nodes:
            leaf_code = leaf_node.industry_code
            if (
                self.max_constituent_fetch_seconds is not None
                and not snapshots
                and (time.monotonic() - started_at) >= self.max_constituent_fetch_seconds
            ):
                diagnostics = replace(
                    diagnostics,
                    aborted_early=True,
                    abort_reason=(
                        "constituent fetch time budget exceeded before any target "
                        "instrument was matched"
                    ),
                )
                break
            diagnostics = replace(
                diagnostics,
                attempted_third_codes=diagnostics.attempted_third_codes + 1,
            )
            try:
                constituent_df = self._fetch_constituent_frame(leaf_code, mode=mode)
            except Exception as exc:
                if len(failed_code_samples) < self.failed_code_sample_limit:
                    failed_code_samples.append(
                        {
                            "third_code": leaf_code,
                            "error": str(exc),
                        }
                    )
                diagnostics = replace(
                    diagnostics,
                    failed_third_codes=diagnostics.failed_third_codes + 1,
                    failed_code_samples=list(failed_code_samples),
                )
                if (
                    self.max_failed_constituent_pages is not None
                    and diagnostics.failed_third_codes >= self.max_failed_constituent_pages
                    and not snapshots
                ):
                    diagnostics = replace(
                        diagnostics,
                        aborted_early=True,
                        abort_reason=(
                            "failed constituent-page threshold reached before any target "
                            "instrument was matched"
                        ),
                    )
                    break
                continue

            diagnostics = replace(
                diagnostics,
                successful_third_codes=diagnostics.successful_third_codes + 1,
            )
            if constituent_df is None or constituent_df.empty:
                continue

            for row in constituent_df.to_dict("records"):
                instrument_id = self._to_instrument_id(str(row.get("股票代码") or "").strip())
                if not instrument_id:
                    continue

                instrument = target_lookup.get(instrument_id)
                if instrument is None:
                    continue

                l3_name = self._clean_text(row.get("申万3级"))
                l2_name = self._clean_text(row.get("申万2级"))
                l1_name = self._clean_text(row.get("申万1级"))

                matched_node = None
                if l3_name:
                    matched_node = cache.l3_by_name.get(l3_name)
                if matched_node is None and l2_name:
                    matched_node = cache.l2_by_name.get(l2_name)
                if matched_node is None:
                    matched_node = leaf_node

                l1_node = None
                l2_node = None
                l3_node = None
                if int(matched_node.industry_level) == 1:
                    l1_node = matched_node
                elif int(matched_node.industry_level) == 2:
                    l2_node = matched_node
                    if matched_node.parent_code:
                        l1_node = cache.l1_by_code.get(str(matched_node.parent_code))
                else:
                    l3_node = matched_node
                    if matched_node.parent_code:
                        l2_node = cache.l2_by_code.get(str(matched_node.parent_code))
                    if l2_node is not None and l2_node.parent_code:
                        l1_node = cache.l1_by_code.get(str(l2_node.parent_code))

                if l2_node is None and l2_name:
                    l2_node = cache.l2_by_name.get(l2_name)
                if l1_node is None and l1_name:
                    l1_node = cache.l1_by_name.get(l1_name)

                normalized_instrument_id = instrument["instrument_id"]
                remaining_ids.discard(normalized_instrument_id)
                snapshots.append(
                    IndustrySnapshot(
                        instrument_id=normalized_instrument_id,
                        symbol=instrument.get("symbol", normalized_instrument_id.split(".", 1)[0]),
                        exchange=instrument.get("exchange", ""),
                        taxonomy_system=self.taxonomy_system,
                        taxonomy_version=self.taxonomy_version,
                        industry_code=matched_node.industry_code,
                        industry_name=matched_node.industry_name,
                        industry_level=matched_node.industry_level,
                        parent_code=matched_node.parent_code,
                        mapping_status="authoritative",
                        effective_date=self._clean_text(row.get("纳入时间")),
                        source_classification="申万标准行业",
                        source_industry_name=matched_node.industry_name,
                        sw_l1_code=l1_node.industry_code if l1_node else None,
                        sw_l1_name=l1_node.industry_name if l1_node else None,
                        sw_l2_code=l2_node.industry_code if l2_node else None,
                        sw_l2_name=l2_node.industry_name if l2_node else None,
                        sw_l3_code=l3_node.industry_code if l3_node else None,
                        sw_l3_name=l3_node.industry_name if l3_node else None,
                        source=self.source_name,
                        source_mode=mode,
                        membership_json={
                            "levels": {
                                "sw_l1": self._node_payload(l1_node),
                                "sw_l2": self._node_payload(l2_node),
                                "sw_l3": self._node_payload(l3_node),
                            },
                            "constituent": row,
                        },
                        raw_payload={"constituent": row, "leaf_level_code": leaf_code},
                    )
                )

                if not remaining_ids:
                    break
            if not remaining_ids:
                break

        self._last_fetch_metadata = replace(
            diagnostics,
            matched_instruments=len(target_lookup) - len(remaining_ids),
            missing_instruments=len(remaining_ids),
            failed_code_samples=list(failed_code_samples),
            missing_instrument_ids=sorted(remaining_ids),
        ).__dict__
        return snapshots

    def _fetch_component_sets_sync(
        self,
        taxonomy_nodes: Optional[List[IndustryTaxonomySnapshot]],
        mode: str,
    ) -> Dict[str, set[str]]:
        target_nodes = taxonomy_nodes or list(self._get_taxonomy_cache().taxonomy_nodes)
        leaf_nodes = get_leaf_taxonomy_nodes(target_nodes, minimum_level=2)
        if not leaf_nodes:
            return {}

        akshare_module = self._akshare(mode)
        component_sets: Dict[str, set[str]] = {}
        for node in leaf_nodes:
            try:
                frame = self._fetch_component_frame(akshare_module, node)
                component_sets[node.industry_code] = self._extract_component_symbols(frame)
            except Exception as exc:
                dm_logger.warning(
                    "[AkshareShenwanIndustry] Failed to fetch component set for %s (%s): %s",
                    node.industry_code,
                    node.industry_name,
                    exc,
                )

        return component_sets

    @staticmethod
    def _fetch_component_frame(
        akshare_module: Any,
        node: IndustryTaxonomySnapshot,
    ) -> pd.DataFrame:
        """Fetch current Shenwan leaf-node constituents.

        `sw_index_third_cons` is preferred when available. `index_component_sw`
        remains a compatibility fallback for older AkShare builds, second-level
        leaf nodes, or transient upstream failures.
        """
        primary_error: Optional[Exception] = None
        primary_frame: Optional[pd.DataFrame] = None
        if hasattr(akshare_module, "sw_index_third_cons"):
            try:
                frame = akshare_module.sw_index_third_cons(symbol=str(node.industry_code))
                if frame is not None and not frame.empty:
                    return frame
                primary_frame = frame
            except Exception as exc:
                primary_error = exc

        if hasattr(akshare_module, "index_component_sw"):
            index_symbol = str(node.industry_code).split(".", 1)[0]
            return akshare_module.index_component_sw(symbol=index_symbol)

        if primary_frame is not None:
            return primary_frame
        if primary_error is not None:
            raise primary_error
        raise AttributeError("AkShare module does not expose Shenwan component functions")

    def get_last_fetch_metadata(self) -> Dict[str, Any]:
        return dict(self._last_fetch_metadata)

    def _fetch_constituent_frame(
        self,
        third_code: str,
        *,
        mode: str,
    ) -> pd.DataFrame:
        load_akshare(mode)
        response = request_get(
            self.constituent_base_url,
            tls_config=self.tls_config,
            params={"industryCode": third_code},
            headers=self._request_headers(),
            timeout=self.constituent_request_timeout_seconds,
        )
        response.raise_for_status()
        frame = pd.read_html(StringIO(response.text))[0]
        frame.columns = [
            "序号",
            "股票代码",
            "股票简称",
            "纳入时间",
            "申万1级",
            "申万2级",
            "申万3级",
            "价格",
            "市盈率",
            "市盈率ttm",
            "市净率",
            "股息率",
            "市值",
            "归母净利润同比增长(09-30)",
            "归母净利润同比增长(06-30)",
            "营业收入同比增长(09-30)",
            "营业收入同比增长(06-30)",
        ]
        for column in ("价格", "市盈率", "市盈率ttm", "市净率", "市值"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        frame["股息率"] = pd.to_numeric(
            frame["股息率"].astype(str).str.strip("%"),
            errors="coerce",
        )
        for column in (
            "归母净利润同比增长(09-30)",
            "归母净利润同比增长(06-30)",
            "营业收入同比增长(09-30)",
            "营业收入同比增长(06-30)",
        ):
            frame[column] = pd.to_numeric(
                frame[column].astype(str).str.strip("%"),
                errors="coerce",
            )
        return frame

    def _get_taxonomy_cache(self) -> _ShenwanTaxonomyCache:
        if self._taxonomy_cache is None:
            self._taxonomy_cache = self._build_taxonomy_cache()
        return self._taxonomy_cache

    def _build_taxonomy_cache(self) -> _ShenwanTaxonomyCache:
        akshare_module = self._akshare("direct")
        first_df = akshare_module.sw_index_first_info()
        second_df = akshare_module.sw_index_second_info()
        third_df = akshare_module.sw_index_third_info()

        taxonomy_nodes: List[IndustryTaxonomySnapshot] = []
        l1_by_code: Dict[str, IndustryTaxonomySnapshot] = {}
        l1_by_name: Dict[str, IndustryTaxonomySnapshot] = {}
        l2_by_code: Dict[str, IndustryTaxonomySnapshot] = {}
        l2_by_name: Dict[str, IndustryTaxonomySnapshot] = {}
        l3_by_code: Dict[str, IndustryTaxonomySnapshot] = {}
        l3_by_name: Dict[str, IndustryTaxonomySnapshot] = {}

        for row in first_df.to_dict("records"):
            node = self._build_taxonomy_node(
                row=row,
                level=1,
                classification="申万一级",
            )
            if node is None:
                continue
            taxonomy_nodes.append(node)
            l1_by_code[node.industry_code] = node
            l1_by_name[node.industry_name] = node

        for row in second_df.to_dict("records"):
            parent_name = self._clean_text(row.get("上级行业"))
            parent_node = l1_by_name.get(parent_name or "")
            node = self._build_taxonomy_node(
                row=row,
                level=2,
                classification="申万二级",
                parent_code=parent_node.industry_code if parent_node else None,
            )
            if node is None:
                continue
            taxonomy_nodes.append(node)
            l2_by_code[node.industry_code] = node
            l2_by_name[node.industry_name] = node

        for row in third_df.to_dict("records"):
            parent_name = self._clean_text(row.get("上级行业"))
            parent_node = l2_by_name.get(parent_name or "")
            node = self._build_taxonomy_node(
                row=row,
                level=3,
                classification="申万三级",
                parent_code=parent_node.industry_code if parent_node else None,
            )
            if node is None:
                continue
            taxonomy_nodes.append(node)
            l3_by_code[node.industry_code] = node
            l3_by_name[node.industry_name] = node

        return _ShenwanTaxonomyCache(
            taxonomy_nodes=taxonomy_nodes,
            l1_by_code=l1_by_code,
            l1_by_name=l1_by_name,
            l2_by_code=l2_by_code,
            l2_by_name=l2_by_name,
            l3_by_code=l3_by_code,
            l3_by_name=l3_by_name,
        )

    def _build_taxonomy_node(
        self,
        *,
        row: Dict[str, Any],
        level: int,
        classification: str,
        parent_code: Optional[str] = None,
    ) -> Optional[IndustryTaxonomySnapshot]:
        industry_code = self._clean_text(row.get("行业代码"))
        industry_name = self._clean_text(row.get("行业名称"))
        if not industry_code or not industry_name:
            return None

        return IndustryTaxonomySnapshot(
            taxonomy_system=self.taxonomy_system,
            taxonomy_version=self.taxonomy_version,
            industry_code=industry_code,
            industry_name=industry_name,
            industry_level=level,
            parent_code=parent_code,
            source_classification=classification,
            source=self.source_name,
            source_mode="direct",
            raw_payload=row,
        )

    @staticmethod
    def _build_target_lookup(
        target_instruments: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        lookup: Dict[str, Dict[str, Any]] = {}
        for instrument in target_instruments:
            instrument_id = str(instrument["instrument_id"])
            lookup[instrument_id] = instrument
            if instrument_id.endswith(".BSE"):
                lookup[instrument_id.replace(".BSE", ".BJ")] = instrument
            if instrument_id.endswith(".BJ"):
                lookup[instrument_id.replace(".BJ", ".BSE")] = instrument
        return lookup

    @staticmethod
    def _node_payload(node: Optional[IndustryTaxonomySnapshot]) -> Optional[Dict[str, Any]]:
        if node is None:
            return None

        return {
            "industry_code": node.industry_code,
            "industry_name": node.industry_name,
            "industry_level": node.industry_level,
            "parent_code": node.parent_code,
        }

    @staticmethod
    def _clean_text(value: Any) -> Optional[str]:
        if value is None or pd.isna(value):
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _to_instrument_id(symbol: str) -> Optional[str]:
        if not symbol:
            return None

        if symbol.startswith(("600", "601", "603", "605", "688", "689", "730")):
            return f"{symbol}.SH"
        if symbol.startswith(("000", "001", "002", "003", "300", "301")):
            return f"{symbol}.SZ"
        if symbol.startswith(("430", "831", "832", "833", "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", "874", "875", "876", "877", "878", "879")):
            return f"{symbol}.BJ"
        return None

    @staticmethod
    def _akshare(mode: str = "direct"):
        return load_akshare(mode)

    @staticmethod
    def _request_headers() -> Dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            )
        }

    @classmethod
    def _extract_component_symbols(cls, frame: pd.DataFrame) -> set[str]:
        if frame is None or frame.empty:
            return set()

        normalized_columns = {str(column).strip(): column for column in frame.columns}
        symbol_column = None
        for candidate in (
            "证券代码",
            "股票代码",
            "成分券代码",
            "品种代码",
            "代码",
        ):
            if candidate in normalized_columns:
                symbol_column = normalized_columns[candidate]
                break

        if symbol_column is None:
            for normalized_name, original_name in normalized_columns.items():
                if "代码" in normalized_name and "行业" not in normalized_name and "指数" not in normalized_name:
                    symbol_column = original_name
                    break

        if symbol_column is None:
            raise ValueError("Unable to identify component symbol column from Shenwan component frame")

        symbols: set[str] = set()
        for raw_value in frame[symbol_column].tolist():
            symbol = cls._normalize_component_symbol(raw_value)
            if symbol:
                symbols.add(symbol)

        return symbols

    @staticmethod
    def _normalize_component_symbol(value: Any) -> Optional[str]:
        if value is None or pd.isna(value):
            return None

        text = str(value).strip()
        if not text:
            return None

        match = re.search(r"(\d{6})", text)
        if match:
            return match.group(1)
        return None
