"""
SWS Research direct official Shenwan classification provider.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import re
from dataclasses import dataclass, field, replace
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from utils import dm_logger

from .tls_support import build_ca_bundle_with_extra_certificate
from .base import (
    BaseIndustryStandardProvider,
    BaseOfficialIndustryHistoryProvider,
    IndustryClassificationHistorySnapshot,
    IndustrySnapshot,
    IndustrySourceFileSnapshot,
    IndustryTaxonomySnapshot,
    OfficialIndustryHistorySnapshot,
)


@dataclass(frozen=True)
class SWSResearchClassificationBundle:
    """Parsed official classification files and normalized rows."""

    taxonomy_nodes: List[IndustryTaxonomySnapshot]
    history_rows: List[IndustryClassificationHistorySnapshot]
    latest_classifications: List[OfficialIndustryHistorySnapshot]
    source_files: List[IndustrySourceFileSnapshot]
    changed: bool = True
    unchanged_artifacts: List[str] = field(default_factory=list)
    diagnostics: Dict[str, Any] = field(default_factory=dict)


class SWSResearchShenwanClassificationProvider(
    BaseIndustryStandardProvider,
    BaseOfficialIndustryHistoryProvider,
):
    """Fetch official Shenwan classification taxonomy and stock history files."""

    source_name = "swsresearch"
    supported_modes = {"direct"}

    STOCK_HISTORY_ARTIFACT = "shenwan_stock_classification_history"
    CODE_TABLE_ARTIFACT = "shenwan_classification_code_table"

    REQUIRED_STOCK_COLUMNS = ("股票代码", "计入日期", "行业代码", "更新日期")
    REQUIRED_CODE_COLUMNS = ("行业代码", "一级行业名称", "二级行业名称", "三级行业名称")

    def __init__(
        self,
        *,
        stock_history_url: str = (
            "https://www.swsresearch.com/swindex/pdf/SwClass2021/"
            "StockClassifyUse_stock.xls"
        ),
        code_table_url: str = (
            "https://www.swsresearch.com/swindex/pdf/SwClass2021/"
            "SwClassCode_2021.xls"
        ),
        request_timeout_seconds: float = 30.0,
        parser_version: str = "swsresearch_shenwan_classification.v1",
        taxonomy_system: str = "sw",
        taxonomy_version: str = "sw_2021",
        minimum_stock_history_rows: int = 5000,
        minimum_code_rows: int = 400,
        symbol_aliases: Optional[List[Dict[str, Any]]] = None,
        extra_ca_cert_path: Optional[str] = None,
    ):
        self.stock_history_url = stock_history_url
        self.code_table_url = code_table_url
        self.request_timeout_seconds = max(1.0, float(request_timeout_seconds))
        self.parser_version = parser_version
        self.taxonomy_system = taxonomy_system
        self.taxonomy_version = taxonomy_version
        self.minimum_stock_history_rows = max(1, int(minimum_stock_history_rows))
        self.minimum_code_rows = max(1, int(minimum_code_rows))
        self.symbol_aliases = list(symbol_aliases or [])
        self.request_verify = build_ca_bundle_with_extra_certificate(extra_ca_cert_path)
        self._bundle_cache: Optional[SWSResearchClassificationBundle] = None
        self._last_fetch_metadata: Dict[str, Any] = {}

    async def fetch_official_classification_bundle(
        self,
        *,
        mode: str = "direct",
        previous_source_files: Optional[Dict[str, Dict[str, Any]]] = None,
        force_refresh: bool = False,
    ) -> SWSResearchClassificationBundle:
        """Fetch and parse both official classification files once."""
        if not self.supports_mode(mode):
            return SWSResearchClassificationBundle(
                taxonomy_nodes=[],
                history_rows=[],
                latest_classifications=[],
                source_files=[],
                changed=False,
                diagnostics={"error": f"unsupported_mode:{mode}"},
            )

        return await asyncio.to_thread(
            self._fetch_official_classification_bundle_sync,
            mode,
            previous_source_files or {},
            force_refresh,
        )

    async def fetch_taxonomy(
        self,
        *,
        mode: str = "direct",
    ) -> List[IndustryTaxonomySnapshot]:
        bundle = await self.fetch_official_classification_bundle(mode=mode)
        return bundle.taxonomy_nodes

    async def fetch_industries(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[IndustrySnapshot]:
        bundle = await self.fetch_official_classification_bundle(mode=mode)
        target_instruments = [
            instrument
            for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("exchange") == exchange
        ]
        if limit is not None:
            target_instruments = target_instruments[:limit]
        return self.build_latest_memberships(
            instruments=target_instruments,
            taxonomy_nodes=bundle.taxonomy_nodes,
            latest_classifications=bundle.latest_classifications,
            source_mode=mode,
        )

    async def fetch_latest_classifications(
        self,
        *,
        instruments: List[Dict[str, Any]],
        exchange: str,
        mode: str = "direct",
        limit: Optional[int] = None,
    ) -> List[OfficialIndustryHistorySnapshot]:
        bundle = await self.fetch_official_classification_bundle(mode=mode)
        target_symbols = {
            str(instrument.get("symbol") or "").strip()
            for instrument in instruments
            if instrument.get("type") == "stock" and instrument.get("exchange") == exchange
        }
        if limit is not None:
            target_symbols = set(list(target_symbols)[:limit])
        return [
            item
            for item in bundle.latest_classifications
            if item.symbol in target_symbols and item.exchange == exchange
        ]

    async def fetch_all_latest_classifications(
        self,
        *,
        mode: str = "direct",
    ) -> List[OfficialIndustryHistorySnapshot]:
        bundle = await self.fetch_official_classification_bundle(mode=mode)
        return bundle.latest_classifications

    def get_last_fetch_metadata(self) -> Dict[str, Any]:
        return dict(self._last_fetch_metadata)

    def _fetch_official_classification_bundle_sync(
        self,
        mode: str,
        previous_source_files: Dict[str, Dict[str, Any]],
        force_refresh: bool,
    ) -> SWSResearchClassificationBundle:
        if self._bundle_cache is not None and not force_refresh and not previous_source_files:
            return self._bundle_cache

        session = requests.Session()
        stock_artifact = self._fetch_artifact(
            session,
            artifact_kind=self.STOCK_HISTORY_ARTIFACT,
            url=self.stock_history_url,
            previous=previous_source_files.get(self.STOCK_HISTORY_ARTIFACT),
            force_refresh=force_refresh,
        )
        code_artifact = self._fetch_artifact(
            session,
            artifact_kind=self.CODE_TABLE_ARTIFACT,
            url=self.code_table_url,
            previous=previous_source_files.get(self.CODE_TABLE_ARTIFACT),
            force_refresh=force_refresh,
        )

        source_files = [stock_artifact["snapshot"], code_artifact["snapshot"]]
        unchanged_artifacts = [
            item["snapshot"].artifact_kind
            for item in (stock_artifact, code_artifact)
            if item["status"] == "unchanged"
        ]
        changed = any(item["status"] != "unchanged" for item in (stock_artifact, code_artifact))

        if not changed:
            bundle = SWSResearchClassificationBundle(
                taxonomy_nodes=[],
                history_rows=[],
                latest_classifications=[],
                source_files=source_files,
                changed=False,
                unchanged_artifacts=unchanged_artifacts,
                diagnostics={"short_circuit": "all_artifacts_unchanged"},
            )
            self._last_fetch_metadata = dict(bundle.diagnostics)
            return bundle

        if code_artifact.get("content") is None:
            raise ValueError("Official Shenwan code-table artifact unchanged but reload is required")
        if stock_artifact.get("content") is None:
            raise ValueError("Official Shenwan stock-history artifact unchanged but reload is required")

        code_frame = self._read_single_sheet_excel(
            code_artifact["content"],
            expected_columns=self.REQUIRED_CODE_COLUMNS,
            artifact_kind=self.CODE_TABLE_ARTIFACT,
        )
        stock_frame = self._read_single_sheet_excel(
            stock_artifact["content"],
            expected_columns=self.REQUIRED_STOCK_COLUMNS,
            artifact_kind=self.STOCK_HISTORY_ARTIFACT,
        )

        taxonomy_nodes = self._parse_taxonomy_nodes(code_frame)
        history_rows = self._parse_history_rows(stock_frame)
        latest_classifications = self._latest_classifications_from_history(history_rows)
        latest_classifications = self._apply_symbol_aliases(latest_classifications)
        source_files = [
            self._with_file_row_metadata(
                stock_artifact["snapshot"],
                row_count=len(stock_frame),
                max_source_update_time=self._max_datetime_string(stock_frame["更新日期"]),
            ),
            self._with_file_row_metadata(
                code_artifact["snapshot"],
                row_count=len(code_frame),
                max_source_update_time=None,
            ),
        ]

        validation_diagnostics = self._validate_bundle(
            taxonomy_nodes=taxonomy_nodes,
            history_rows=history_rows,
            latest_classifications=latest_classifications,
        )
        bundle = SWSResearchClassificationBundle(
            taxonomy_nodes=taxonomy_nodes,
            history_rows=history_rows,
            latest_classifications=latest_classifications,
            source_files=source_files,
            changed=True,
            unchanged_artifacts=unchanged_artifacts,
            diagnostics={
                "taxonomy_nodes": len(taxonomy_nodes),
                "history_rows": len(history_rows),
                "latest_classifications": len(latest_classifications),
                "unchanged_artifacts": unchanged_artifacts,
                **validation_diagnostics,
            },
        )
        self._bundle_cache = bundle
        self._last_fetch_metadata = dict(bundle.diagnostics)
        return bundle

    def _fetch_artifact(
        self,
        session: requests.Session,
        *,
        artifact_kind: str,
        url: str,
        previous: Optional[Dict[str, Any]],
        force_refresh: bool,
    ) -> Dict[str, Any]:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
        }
        if previous and not force_refresh:
            etag = str(previous.get("etag") or "").strip()
            last_modified = str(previous.get("last_modified") or "").strip()
            if etag:
                headers["If-None-Match"] = etag
            if last_modified:
                headers["If-Modified-Since"] = last_modified

        response = session.get(
            url,
            headers=headers,
            timeout=self.request_timeout_seconds,
            verify=self.request_verify,
        )
        raw_headers = {key: value for key, value in response.headers.items()}
        if response.status_code == 304 and previous:
            snapshot = IndustrySourceFileSnapshot(
                source=self.source_name,
                source_mode="direct",
                artifact_kind=artifact_kind,
                url=url,
                parser_version=self.parser_version,
                status="unchanged",
                etag=previous.get("etag"),
                last_modified=previous.get("last_modified"),
                content_length=_safe_int(previous.get("content_length")),
                sha256=previous.get("sha256"),
                row_count=int(previous.get("row_count") or 0),
                max_source_update_time=previous.get("max_source_update_time"),
                raw_headers=raw_headers,
                metadata_json={"previous_source_file": previous},
            )
            return {"status": "unchanged", "snapshot": snapshot, "content": None}

        response.raise_for_status()
        content = response.content
        digest = hashlib.sha256(content).hexdigest()
        if previous and not force_refresh and digest == str(previous.get("sha256") or ""):
            snapshot = IndustrySourceFileSnapshot(
                source=self.source_name,
                source_mode="direct",
                artifact_kind=artifact_kind,
                url=url,
                parser_version=self.parser_version,
                status="unchanged",
                etag=response.headers.get("ETag") or previous.get("etag"),
                last_modified=response.headers.get("Last-Modified")
                or previous.get("last_modified"),
                content_length=len(content),
                sha256=digest,
                row_count=int(previous.get("row_count") or 0),
                max_source_update_time=previous.get("max_source_update_time"),
                raw_headers=raw_headers,
                metadata_json={"unchanged_by": "sha256"},
            )
            return {"status": "unchanged", "snapshot": snapshot, "content": None}

        snapshot = IndustrySourceFileSnapshot(
            source=self.source_name,
            source_mode="direct",
            artifact_kind=artifact_kind,
            url=url,
            parser_version=self.parser_version,
            status="downloaded",
            etag=response.headers.get("ETag"),
            last_modified=response.headers.get("Last-Modified"),
            content_length=len(content),
            sha256=digest,
            raw_headers=raw_headers,
        )
        return {"status": "downloaded", "snapshot": snapshot, "content": content}

    def _read_single_sheet_excel(
        self,
        content: bytes,
        *,
        expected_columns: tuple[str, ...],
        artifact_kind: str,
    ) -> pd.DataFrame:
        workbook = pd.ExcelFile(io.BytesIO(content))
        if len(workbook.sheet_names) != 1:
            raise ValueError(
                f"{artifact_kind} expected exactly one sheet, got {workbook.sheet_names}"
            )
        frame = pd.read_excel(workbook, sheet_name=workbook.sheet_names[0], dtype=str)
        frame = frame.rename(columns={column: str(column).strip() for column in frame.columns})
        missing_columns = [column for column in expected_columns if column not in frame.columns]
        if missing_columns:
            raise ValueError(f"{artifact_kind} missing required columns: {missing_columns}")
        return frame.loc[:, list(expected_columns)].copy()

    def _parse_taxonomy_nodes(self, frame: pd.DataFrame) -> List[IndustryTaxonomySnapshot]:
        nodes: List[IndustryTaxonomySnapshot] = []
        seen_codes: set[str] = set()
        for row in frame.to_dict("records"):
            code = _clean_code(row.get("行业代码"))
            if not _is_six_digit_code(code):
                continue
            l1_name = _clean_text(row.get("一级行业名称"))
            l2_name = _clean_text(row.get("二级行业名称"))
            l3_name = _clean_text(row.get("三级行业名称"))
            if not l1_name:
                continue

            if not l2_name and not l3_name:
                level = 1
                name = l1_name
                parent_code = None
            elif l2_name and not l3_name:
                level = 2
                name = l2_name
                parent_code = f"{code[:2]}0000"
            elif l2_name and l3_name:
                level = 3
                name = l3_name
                parent_code = f"{code[:4]}00"
            else:
                continue

            if code in seen_codes:
                continue
            seen_codes.add(code)
            nodes.append(
                IndustryTaxonomySnapshot(
                    taxonomy_system=self.taxonomy_system,
                    taxonomy_version=self.taxonomy_version,
                    industry_code=code,
                    industry_name=name,
                    industry_level=level,
                    parent_code=parent_code,
                    source_classification="申万官方行业分类代码表",
                    source=self.source_name,
                    source_mode="direct",
                    raw_payload={
                        "official_industry_code": code,
                        "level_1_name": l1_name,
                        "level_2_name": l2_name,
                        "level_3_name": l3_name,
                    },
                )
            )
        return nodes

    def _parse_history_rows(self, frame: pd.DataFrame) -> List[IndustryClassificationHistorySnapshot]:
        rows: List[IndustryClassificationHistorySnapshot] = []
        for record in frame.to_dict("records"):
            symbol = _clean_symbol(record.get("股票代码"))
            industry_code = _clean_code(record.get("行业代码"))
            if not symbol or not _is_six_digit_code(industry_code):
                continue
            instrument_id = self._to_instrument_id(symbol)
            exchange = self._to_exchange(symbol)
            if not instrument_id or not exchange:
                continue

            start_date = _format_timestamp(record.get("计入日期"))
            update_time = _format_timestamp(record.get("更新日期"))
            raw_payload = {
                "股票代码": symbol,
                "计入日期": start_date,
                "行业代码": industry_code,
                "更新日期": update_time,
            }
            row_hash = hashlib.sha256(
                "|".join(
                    [
                        symbol,
                        industry_code,
                        start_date or "",
                        update_time or "",
                    ]
                ).encode("utf-8")
            ).hexdigest()
            rows.append(
                IndustryClassificationHistorySnapshot(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    exchange=exchange,
                    taxonomy_system=self.taxonomy_system,
                    taxonomy_version=self.taxonomy_version,
                    official_industry_code=industry_code,
                    official_start_date=start_date,
                    official_update_time=update_time,
                    row_hash=row_hash,
                    source=self.source_name,
                    source_mode="direct",
                    classification_json=raw_payload,
                )
            )
        return rows

    def _latest_classifications_from_history(
        self,
        history_rows: List[IndustryClassificationHistorySnapshot],
    ) -> List[OfficialIndustryHistorySnapshot]:
        latest_by_symbol: Dict[str, IndustryClassificationHistorySnapshot] = {}
        for row in sorted(
            history_rows,
            key=lambda item: (
                item.symbol,
                item.official_start_date or "",
                item.official_update_time or "",
            ),
        ):
            latest_by_symbol[row.symbol] = row

        return [
            OfficialIndustryHistorySnapshot(
                instrument_id=row.instrument_id,
                symbol=row.symbol,
                exchange=row.exchange,
                official_industry_code=row.official_industry_code,
                start_date=row.official_start_date,
                update_time=row.official_update_time,
                source=row.source,
                source_mode=row.source_mode,
                raw_payload=row.classification_json,
            )
            for row in latest_by_symbol.values()
        ]

    def _apply_symbol_aliases(
        self,
        latest_classifications: List[OfficialIndustryHistorySnapshot],
    ) -> List[OfficialIndustryHistorySnapshot]:
        """Duplicate official rows for configured stock-code changes.

        SWS Research classification files may lag rare exchange-side code
        changes. Aliases must be explicit and auditable in config so they do
        not mask broad universe-quality issues.
        """
        if not self.symbol_aliases:
            return latest_classifications

        latest_by_symbol = {
            str(item.symbol).strip(): item for item in latest_classifications
        }
        result = list(latest_classifications)
        for alias in self.symbol_aliases:
            current_symbol = _clean_symbol(
                alias.get("current_symbol")
                or alias.get("symbol")
                or alias.get("target_symbol")
            )
            official_symbol = _clean_symbol(
                alias.get("official_symbol")
                or alias.get("source_symbol")
                or alias.get("legacy_symbol")
            )
            if not current_symbol or not official_symbol:
                continue
            if current_symbol in latest_by_symbol:
                continue
            official = latest_by_symbol.get(official_symbol)
            if official is None:
                continue

            instrument_id = str(alias.get("instrument_id") or "").strip()
            if not instrument_id:
                instrument_id = self._to_instrument_id(current_symbol) or current_symbol
            exchange = str(alias.get("exchange") or "").strip()
            if not exchange:
                exchange = self._to_exchange(current_symbol) or official.exchange
            raw_payload = dict(official.raw_payload or {})
            raw_payload["symbol_alias"] = {
                "current_symbol": current_symbol,
                "official_symbol": official_symbol,
                "instrument_id": instrument_id,
                "exchange": exchange,
                "reason": alias.get("reason"),
                "evidence": alias.get("evidence"),
            }
            alias_snapshot = replace(
                official,
                instrument_id=instrument_id,
                symbol=current_symbol,
                exchange=exchange,
                raw_payload=raw_payload,
            )
            latest_by_symbol[current_symbol] = alias_snapshot
            result.append(alias_snapshot)
        return result

    def build_latest_memberships(
        self,
        *,
        instruments: List[Dict[str, Any]],
        taxonomy_nodes: List[IndustryTaxonomySnapshot],
        latest_classifications: List[OfficialIndustryHistorySnapshot],
        source_mode: str = "direct",
    ) -> List[IndustrySnapshot]:
        taxonomy_by_code = {node.industry_code: node for node in taxonomy_nodes}
        latest_by_symbol = {
            str(snapshot.symbol).strip(): snapshot for snapshot in latest_classifications
        }
        snapshots: List[IndustrySnapshot] = []
        for instrument in instruments:
            symbol = str(instrument.get("symbol") or "").strip()
            if not symbol:
                symbol = str(instrument.get("instrument_id") or "").split(".", 1)[0]
            official = latest_by_symbol.get(symbol)
            if official is None:
                continue
            leaf_node = taxonomy_by_code.get(official.official_industry_code)
            if leaf_node is None:
                dm_logger.warning(
                    "[SWSResearchClassification] Missing taxonomy node for official code %s",
                    official.official_industry_code,
                )
                continue
            l1_node, l2_node, l3_node = _resolve_taxonomy_levels(
                node=leaf_node,
                taxonomy_by_code=taxonomy_by_code,
            )
            levels_payload = {
                "sw_l1": _node_payload(l1_node),
                "sw_l2": _node_payload(l2_node),
                "sw_l3": _node_payload(l3_node),
            }
            raw_payload = {
                "official_classification": official.raw_payload,
                "levels": levels_payload,
                "identifier_namespace": "swsresearch_official_classification_code",
            }
            snapshots.append(
                IndustrySnapshot(
                    instrument_id=str(instrument.get("instrument_id") or official.instrument_id),
                    symbol=symbol,
                    exchange=str(instrument.get("exchange") or official.exchange),
                    taxonomy_system=leaf_node.taxonomy_system,
                    taxonomy_version=leaf_node.taxonomy_version,
                    industry_code=leaf_node.industry_code,
                    industry_name=leaf_node.industry_name,
                    industry_level=leaf_node.industry_level,
                    parent_code=leaf_node.parent_code,
                    mapping_status="authoritative",
                    effective_date=official.start_date,
                    source_classification="申万官方股票行业分类文件",
                    source_industry_name=leaf_node.industry_name,
                    sw_l1_code=None if l1_node is None else l1_node.industry_code,
                    sw_l1_name=None if l1_node is None else l1_node.industry_name,
                    sw_l2_code=None if l2_node is None else l2_node.industry_code,
                    sw_l2_name=None if l2_node is None else l2_node.industry_name,
                    sw_l3_code=None if l3_node is None else l3_node.industry_code,
                    sw_l3_name=None if l3_node is None else l3_node.industry_name,
                    source=self.source_name,
                    source_mode=source_mode,
                    membership_json=raw_payload,
                    raw_payload=raw_payload,
                )
            )
        return snapshots

    def _validate_bundle(
        self,
        *,
        taxonomy_nodes: List[IndustryTaxonomySnapshot],
        history_rows: List[IndustryClassificationHistorySnapshot],
        latest_classifications: List[OfficialIndustryHistorySnapshot],
    ) -> Dict[str, Any]:
        if len(history_rows) < self.minimum_stock_history_rows:
            raise ValueError(
                "Official Shenwan stock history row count below minimum: "
                f"{len(history_rows)} < {self.minimum_stock_history_rows}"
            )
        if len(taxonomy_nodes) < self.minimum_code_rows:
            raise ValueError(
                "Official Shenwan code table row count below minimum: "
                f"{len(taxonomy_nodes)} < {self.minimum_code_rows}"
            )
        level_counts: Dict[int, int] = {}
        for node in taxonomy_nodes:
            level_counts[int(node.industry_level)] = level_counts.get(int(node.industry_level), 0) + 1
        if level_counts.get(1, 0) < 25 or level_counts.get(2, 0) < 100 or level_counts.get(3, 0) < 300:
            raise ValueError(f"Unexpected official Shenwan taxonomy level counts: {level_counts}")
        taxonomy_codes = {node.industry_code for node in taxonomy_nodes}
        missing_codes = sorted(
            {
                snapshot.official_industry_code
                for snapshot in latest_classifications
                if snapshot.official_industry_code not in taxonomy_codes
            }
        )
        if missing_codes:
            dm_logger.warning(
                "[SWSResearchClassification] Latest official classification rows reference "
                "taxonomy codes absent from the current code table; these rows will be skipped "
                "when building memberships. count=%s sample=%s",
                len(missing_codes),
                missing_codes[:20],
            )
        return {
            "taxonomy_level_counts": level_counts,
            "latest_missing_taxonomy_code_count": len(missing_codes),
            "latest_missing_taxonomy_code_sample": missing_codes[:20],
        }

    def _with_file_row_metadata(
        self,
        snapshot: IndustrySourceFileSnapshot,
        *,
        row_count: int,
        max_source_update_time: Optional[str],
    ) -> IndustrySourceFileSnapshot:
        return replace(
            snapshot,
            row_count=row_count,
            max_source_update_time=max_source_update_time,
        )

    @staticmethod
    def _max_datetime_string(values: Any) -> Optional[str]:
        parsed = pd.to_datetime(values, errors="coerce")
        if parsed.isna().all():
            return None
        return pd.Timestamp(parsed.max()).isoformat()

    @staticmethod
    def _to_instrument_id(symbol: str) -> Optional[str]:
        if symbol.startswith(("600", "601", "603", "605", "688", "689", "730")):
            return f"{symbol}.SH"
        if symbol.startswith(("000", "001", "002", "003", "300", "301")):
            return f"{symbol}.SZ"
        if symbol.startswith(
            (
                "920",
                "430",
                "831",
                "832",
                "833",
                "834",
                "835",
                "836",
                "837",
                "838",
                "839",
                "870",
                "871",
                "872",
                "873",
                "874",
                "875",
                "876",
                "877",
                "878",
                "879",
            )
        ):
            return f"{symbol}.BJ"
        return None

    @classmethod
    def _to_exchange(cls, symbol: str) -> Optional[str]:
        instrument_id = cls._to_instrument_id(symbol)
        if instrument_id is None:
            return None
        suffix = instrument_id.rsplit(".", 1)[-1]
        if suffix == "SH":
            return "SSE"
        if suffix == "SZ":
            return "SZSE"
        if suffix == "BJ":
            return "BSE"
        return None


def _clean_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return re.sub(r"\s+", " ", text)


def _clean_code(value: Any) -> str:
    text = _clean_text(value)
    text = text.split(".", 1)[0]
    return re.sub(r"\D", "", text)


def _clean_symbol(value: Any) -> str:
    text = _clean_text(value)
    match = re.search(r"\d{6}", text)
    return "" if match is None else match.group(0)


def _is_six_digit_code(value: str) -> bool:
    return bool(re.fullmatch(r"\d{6}", value or ""))


def _format_timestamp(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return None
    return pd.Timestamp(timestamp).date().isoformat()


def _resolve_taxonomy_levels(
    *,
    node: Optional[IndustryTaxonomySnapshot],
    taxonomy_by_code: Dict[str, IndustryTaxonomySnapshot],
) -> tuple[
    Optional[IndustryTaxonomySnapshot],
    Optional[IndustryTaxonomySnapshot],
    Optional[IndustryTaxonomySnapshot],
]:
    if node is None:
        return None, None, None
    if int(node.industry_level) == 1:
        return node, None, None
    if int(node.industry_level) == 2:
        return taxonomy_by_code.get(str(node.parent_code or "")), node, None
    l3_node = node
    l2_node = taxonomy_by_code.get(str(node.parent_code or ""))
    l1_node = None if l2_node is None else taxonomy_by_code.get(str(l2_node.parent_code or ""))
    return l1_node, l2_node, l3_node


def _node_payload(node: Optional[IndustryTaxonomySnapshot]) -> Optional[Dict[str, Any]]:
    if node is None:
        return None
    return {
        "industry_code": node.industry_code,
        "industry_name": node.industry_name,
        "industry_level": node.industry_level,
        "parent_code": node.parent_code,
    }


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
