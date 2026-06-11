"""
Broker risk-control report parsing and financial fact integration helpers.
"""

from __future__ import annotations

import hashlib
import io
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

from research.financial_fact_aliases import describe_financial_numeric_fact_name
from research.providers.base import (
    FinancialFilingPayload,
    FinancialNumericFactSnapshot,
    FinancialSourceFileManifest,
)
from research.providers.cninfo_announcements import (
    CninfoAnnouncementRecord,
    CninfoAnnouncementScanConfig,
    CninfoAnnouncementScanner,
)
from research.listed_broker_dealer_scope import (
    enrich_instrument_with_broker_scope,
    is_confirmed_listed_broker_dealer,
    resolve_listed_broker_dealer_scope,
)
from utils.date_utils import get_shanghai_time
from utils.http_transport import HttpTlsConfig, request_get


BROKER_RISK_CONTROL_SOURCE_PROFILE = "broker_risk_control_report"
BROKER_RISK_CONTROL_ARTIFACT_KIND = "broker_risk_control_pdf"
BROKER_RISK_CONTROL_PARSER_VERSION = "broker_risk_control_pdf.v1"
BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE = "broker_annual_report_embedded_risk_control"
BROKER_ANNUAL_REPORT_RISK_CONTROL_ARTIFACT_KIND = "broker_annual_or_semiannual_report_pdf"
BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION = "broker_annual_report_embedded_risk_control_pdf.v1"
BROKER_RISK_CONTROL_STATEMENT_FAMILY = "regulatory_risk_control"
LOGGER = logging.getLogger(__name__)


BROKER_RISK_CONTROL_REQUIRED_FACTS = ("net_capital",)


BROKER_RISK_CONTROL_CANONICAL_FACTS: Dict[str, Dict[str, Any]] = {
    "net_capital": {
        "semantic": "regulatory_net_capital",
        "unit": "CNY",
        "aliases": ["净资本"],
    },
    "core_net_capital": {
        "semantic": "regulatory_core_net_capital",
        "unit": "CNY",
        "aliases": ["核心净资本"],
    },
    "subordinated_net_capital": {
        "semantic": "regulatory_subordinated_net_capital",
        "unit": "CNY",
        "aliases": ["附属净资本"],
    },
    "regulatory_net_assets": {
        "semantic": "regulatory_net_assets",
        "unit": "CNY",
        "aliases": ["净资产"],
    },
    "risk_capital_reserve_total": {
        "semantic": "total_risk_capital_reserve",
        "unit": "CNY",
        "aliases": ["各项风险资本准备之和", "风险资本准备之和"],
    },
    "market_risk_capital_reserve": {
        "semantic": "market_risk_capital_reserve",
        "unit": "CNY",
        "aliases": ["市场风险资本准备"],
    },
    "credit_risk_capital_reserve": {
        "semantic": "credit_risk_capital_reserve",
        "unit": "CNY",
        "aliases": ["信用风险资本准备"],
    },
    "operational_risk_capital_reserve": {
        "semantic": "operational_risk_capital_reserve",
        "unit": "CNY",
        "aliases": ["操作风险资本准备"],
    },
    "balance_sheet_assets_total": {
        "semantic": "regulatory_balance_sheet_assets_total",
        "unit": "CNY",
        "aliases": ["表内外资产总额", "表内资产总额"],
    },
    "off_balance_sheet_assets_total": {
        "semantic": "regulatory_off_balance_sheet_assets_total",
        "unit": "CNY",
        "aliases": ["表外资产总额"],
    },
    "risk_coverage_ratio": {
        "semantic": "risk_coverage_ratio",
        "unit": "ratio",
        "aliases": ["风险覆盖率"],
    },
    "capital_leverage_ratio": {
        "semantic": "capital_leverage_ratio",
        "unit": "ratio",
        "aliases": ["资本杠杆率"],
    },
    "liquidity_coverage_ratio": {
        "semantic": "liquidity_coverage_ratio",
        "unit": "ratio",
        "aliases": ["流动性覆盖率"],
    },
    "net_stable_funding_ratio": {
        "semantic": "net_stable_funding_ratio",
        "unit": "ratio",
        "aliases": ["净稳定资金率"],
    },
    "net_capital_to_net_assets": {
        "semantic": "net_capital_to_net_assets",
        "unit": "ratio",
        "aliases": ["净资本/净资产", "净资本与净资产的比例"],
    },
    "net_capital_to_liabilities": {
        "semantic": "net_capital_to_liabilities",
        "unit": "ratio",
        "aliases": ["净资本/负债", "净资本与负债的比例"],
    },
    "net_assets_to_liabilities": {
        "semantic": "net_assets_to_liabilities",
        "unit": "ratio",
        "aliases": ["净资产/负债", "净资产与负债的比例"],
    },
    "proprietary_equity_securities_to_net_capital": {
        "semantic": "proprietary_equity_securities_and_derivatives_to_net_capital",
        "unit": "ratio",
        "aliases": ["自营权益类证券及其衍生品/净资本", "自营权益类证券及证券衍生品/净资本"],
    },
    "proprietary_non_equity_securities_to_net_capital": {
        "semantic": "proprietary_non_equity_securities_and_derivatives_to_net_capital",
        "unit": "ratio",
        "aliases": ["自营非权益类证券及其衍生品/净资本", "自营非权益类证券及证券衍生品/净资本"],
    },
    "margin_financing_to_net_capital": {
        "semantic": "margin_financing_including_securities_lending_to_net_capital",
        "unit": "ratio",
        "aliases": ["融资（含融券）的金额/净资本", "融资含融券的金额/净资本", "融资融券金额/净资本"],
    },
    "high_quality_liquid_assets": {
        "semantic": "high_quality_liquid_assets",
        "unit": "CNY",
        "aliases": ["优质流动性资产"],
    },
    "net_cash_outflow": {
        "semantic": "net_cash_outflow",
        "unit": "CNY",
        "aliases": ["未来30日现金净流出量", "现金净流出量"],
    },
    "available_stable_funding": {
        "semantic": "available_stable_funding",
        "unit": "CNY",
        "aliases": ["可用稳定资金"],
    },
    "required_stable_funding": {
        "semantic": "required_stable_funding",
        "unit": "CNY",
        "aliases": ["所需稳定资金"],
    },
    "single_client_concentration_ratio": {
        "semantic": "single_client_concentration_ratio",
        "unit": "ratio",
        "aliases": ["单一客户相关风险占净资本比例"],
    },
    "single_security_concentration_ratio": {
        "semantic": "single_security_concentration_ratio",
        "unit": "ratio",
        "aliases": ["单一证券相关风险占净资本比例"],
    },
    "broker_operational_risk_brokerage_net_revenue": {
        "semantic": "regulatory_operational_risk_brokerage_net_revenue",
        "unit": "CNY",
        "aliases": ["经纪业务净收入"],
    },
    "broker_operational_risk_investment_banking_net_revenue": {
        "semantic": "regulatory_operational_risk_investment_banking_net_revenue",
        "unit": "CNY",
        "aliases": ["投资银行业务净收入"],
    },
    "broker_operational_risk_asset_management_net_revenue": {
        "semantic": "regulatory_operational_risk_asset_management_net_revenue",
        "unit": "CNY",
        "aliases": ["资产管理业务净收入"],
    },
    "broker_operational_risk_proprietary_net_revenue": {
        "semantic": "regulatory_operational_risk_proprietary_net_revenue",
        "unit": "CNY",
        "aliases": ["证券自营业务净收入", "自营业务净收入"],
    },
}


_LABEL_OVERRIDES = {
    alias: canonical
    for canonical, spec in BROKER_RISK_CONTROL_CANONICAL_FACTS.items()
    for alias in spec.get("aliases", [])
}


def broker_risk_control_catalog_entries() -> Dict[str, Dict[str, Any]]:
    """Return canonical catalog entries for broker risk-control numeric facts."""
    return {
        canonical: {
            "statement_family": BROKER_RISK_CONTROL_STATEMENT_FAMILY,
            "semantic": str(spec["semantic"]),
            "unit": str(spec["unit"]),
            "aliases": list(dict.fromkeys([canonical, *spec.get("aliases", [])])),
        }
        for canonical, spec in BROKER_RISK_CONTROL_CANONICAL_FACTS.items()
    }


def is_broker_risk_control_title(
    title: str,
    *,
    title_patterns: Optional[Sequence[str]] = None,
) -> bool:
    """Return whether an announcement title is a broker risk-control report."""
    text = re.sub(r"<[^>]+>", "", str(title or ""))
    text = text.replace("&nbsp;", "").replace("&amp;", "&")
    text = re.sub(r"\s+", "", text)
    if not text:
        return False
    if "管理办法" in text:
        return False
    patterns = list(
        title_patterns
        or (
            "风险控制指标相关情况报告",
            "风险控制指标报告",
            "风险控制指标",
        )
    )
    return any(str(pattern) and str(pattern).replace(" ", "") in text for pattern in patterns)


def is_formal_broker_annual_or_semiannual_report_title(title: str) -> bool:
    """Return whether a CNInfo title is a formal annual/semiannual report."""
    text = _normalize_announcement_title(title)
    if not text:
        return False
    if not ("年度报告" in text or "半年度报告" in text):
        return False
    if not re.search(r"(20\d{2}|19\d{2})", text):
        return False
    excluded_tokens = (
        "H股公告",
        "摘要",
        "审计报告",
        "审阅报告",
        "法律意见",
        "鉴证报告",
        "问询函",
        "回复",
        "意见函",
        "监管工作函",
        "持续督导",
        "保荐",
        "业绩说明会",
        "说明会",
        "投资者关系",
        "社会责任",
        "环境、社会及治理",
        "环境社会及治理",
        "ESG",
        "英文",
        "可视版",
        "提质增效",
        "重回报",
        "行动方案",
        "落实情况",
        "披露时间",
        "提示性公告",
        "关于变更",
        "修订说明",
        "更正",
        "取消",
    )
    if any(token in text for token in excluded_tokens):
        return False
    return True


def infer_broker_annual_report_period(record: CninfoAnnouncementRecord) -> str:
    """Infer report period from a formal annual/semiannual report title."""
    title = _normalize_announcement_title(record.title)
    year_match = re.search(r"(20\d{2}|19\d{2})", title)
    if year_match:
        year = year_match.group(1)
        if "半年度报告" in title:
            return f"{year}-06-30"
        return f"{year}-12-31"
    if record.announcement_time and re.match(r"^\d{4}", record.announcement_time):
        year = int(record.announcement_time[:4]) - 1
        return f"{year}-12-31"
    return ""


def infer_broker_risk_control_report_period(record: CninfoAnnouncementRecord) -> str:
    """Infer report period from a broker risk-control announcement title."""
    title = re.sub(r"<[^>]+>", "", str(record.title or ""))
    title = title.replace("&nbsp;", "").replace("&amp;", "&")
    title = re.sub(r"\s+", "", title)
    year_match = re.search(r"(20\d{2}|19\d{2})", title)
    if year_match:
        year = year_match.group(1)
        if any(token in title for token in ("第一季度", "一季度", "1季度")):
            return f"{year}-03-31"
        if any(token in title for token in ("半年度", "上半年", "中期")):
            return f"{year}-06-30"
        if any(token in title for token in ("第三季度", "三季度", "3季度")):
            return f"{year}-09-30"
        return f"{year}-12-31"
    if record.announcement_time and re.match(r"^\d{4}", record.announcement_time):
        year = int(record.announcement_time[:4]) - 1
        return f"{year}-12-31"
    return ""


def is_broker_risk_control_instrument(
    instrument: Dict[str, Any],
    *,
    allow_validation_override: bool = False,
) -> bool:
    """Return whether an instrument is in the securities-company scope."""
    if allow_validation_override:
        return True
    return is_confirmed_listed_broker_dealer(instrument)


def classify_broker_risk_control_artifact(title: str, *, adjunct_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Classify matching announcements as broker risk-control PDF artifacts."""
    if not is_broker_risk_control_title(title):
        return None
    type_text = str(adjunct_type or "").lower()
    if type_text and "pdf" not in type_text:
        return None
    return {
        "artifact_kind": BROKER_RISK_CONTROL_ARTIFACT_KIND,
        "parser_candidate": BROKER_RISK_CONTROL_PARSER_VERSION,
        "source_profile": BROKER_RISK_CONTROL_SOURCE_PROFILE,
    }


def classify_broker_annual_report_risk_control_artifact(
    title: str,
    *,
    adjunct_type: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Classify formal annual/semiannual report PDFs for embedded risk-control parsing."""
    if not is_formal_broker_annual_or_semiannual_report_title(title):
        return None
    type_text = str(adjunct_type or "").lower()
    if type_text and "pdf" not in type_text:
        return None
    report_type = "semiannual" if "半年度报告" in _normalize_announcement_title(title) else "annual"
    return {
        "artifact_kind": BROKER_ANNUAL_REPORT_RISK_CONTROL_ARTIFACT_KIND,
        "parser_candidate": BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION,
        "source_profile": BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
        "report_type": report_type,
        "source_priority": "primary",
    }


def _normalize_announcement_title(title: str) -> str:
    text = re.sub(r"<[^>]+>", "", str(title or ""))
    text = text.replace("&nbsp;", "").replace("&amp;", "&")
    return re.sub(r"\s+", "", text)


@dataclass(frozen=True)
class BrokerRiskControlParseResult:
    numeric_facts: List[FinancialNumericFactSnapshot]
    diagnostics: Dict[str, Any] = field(default_factory=dict)


class BrokerRiskControlPdfFactParser:
    """Parse broker regulatory risk-control metrics from PDF text."""

    _FIXED_ORDER_EMBEDDED_FACTS = (
        "core_net_capital",
        "subordinated_net_capital",
        "net_capital",
        "regulatory_net_assets",
        "risk_capital_reserve_total",
        "balance_sheet_assets_total",
        "risk_coverage_ratio",
        "capital_leverage_ratio",
        "liquidity_coverage_ratio",
        "net_stable_funding_ratio",
        "net_capital_to_net_assets",
        "net_capital_to_liabilities",
        "net_assets_to_liabilities",
        "proprietary_equity_securities_to_net_capital",
        "proprietary_non_equity_securities_to_net_capital",
    )

    _MONEY_UNITS = {
        "元": 1.0,
        "千元": 1_000.0,
        "万元": 10_000.0,
        "百万元": 1_000_000.0,
        "亿元": 100_000_000.0,
    }
    _MONEY_UNIT_PATTERN = re.compile(
        r"单位\s*[:：]?\s*(?:人民币)?\s*(百万元|千元|万元|亿元|元)"
    )
    _PAREN_MONEY_UNIT_PATTERN = re.compile(
        r"[（(]\s*(?:人民币)?\s*(百万元|千元|万元|亿元|元)\s*[)）]"
    )
    _NUMBER_PATTERN = re.compile(
        r"(?P<value>[+-]?(?:(?:\d{1,3}(?:\s*,\s*\d{3})+|\d+)"
        r"(?:\s*\.\s*\d+)?|\.\s*\d+))\s*(?P<percent>%|％)?"
    )

    def __init__(self, *, parser_version: str = BROKER_RISK_CONTROL_PARSER_VERSION):
        self.parser_version = parser_version
        self._label_patterns = self._compile_label_patterns()

    def parse(
        self,
        payload: bytes | str,
        *,
        source_file_id: str,
        instrument_id: str,
        symbol: str,
        exchange: str,
        report_period: str,
        source: str,
        source_mode: str = "direct",
        report_type: Optional[str] = None,
        source_profile: str = BROKER_RISK_CONTROL_SOURCE_PROFILE,
        artifact_kind: str = BROKER_RISK_CONTROL_ARTIFACT_KIND,
        licensed_broker_name: Optional[str] = None,
        listed_broker_scope: Optional[Dict[str, Any]] = None,
    ) -> BrokerRiskControlParseResult:
        text, text_diagnostics = self._extract_text(payload)
        source_unit, unit_scale = self._detect_money_unit(text)
        report_scope = self._detect_report_scope(text)
        rows = self._candidate_rows(text)
        facts: List[FinancialNumericFactSnapshot] = []
        ambiguous_rows: List[Dict[str, Any]] = []
        matched_rows: Dict[str, Dict[str, Any]] = {}

        for line_index, line in enumerate(rows):
            canonical_matches = self._match_canonical_labels(line)
            if not canonical_matches:
                continue
            if len(canonical_matches) > 1:
                ambiguous_rows.append(
                    {"line_index": line_index, "line": line, "matches": canonical_matches}
                )
                continue
            canonical_name = canonical_matches[0]
            if canonical_name in matched_rows:
                ambiguous_rows.append(
                    {
                        "line_index": line_index,
                        "line": line,
                        "matches": [canonical_name],
                        "reason": "duplicate_canonical_fact",
                    }
                )
                continue
            value = self._extract_numeric_value(line, canonical_name=canonical_name)
            if value is None:
                ambiguous_rows.append(
                    {
                        "line_index": line_index,
                        "line": line,
                        "matches": [canonical_name],
                        "reason": "numeric_value_missing",
                    }
                )
                continue
            unit = self._unit_for(canonical_name)
            effective_source_unit, effective_unit_scale, unit_detection = self._effective_money_unit(
                canonical_name,
                value["value"],
                source_unit=source_unit,
                unit_scale=unit_scale,
                rows=rows,
                line_index=line_index,
                line=line,
            )
            canonical_value = self._normalize_value(
                value,
                unit=unit,
                source_unit=effective_source_unit,
                unit_scale=effective_unit_scale,
            )
            if canonical_value is None:
                ambiguous_rows.append(
                    {
                        "line_index": line_index,
                        "line": line,
                        "matches": [canonical_name],
                        "reason": "source_unit_unknown",
                    }
                )
                continue
            quality_issue = self._fact_quality_issue(
                canonical_name,
                canonical_value,
                source_unit=effective_source_unit,
                value=value,
            )
            if quality_issue:
                ambiguous_rows.append(
                    {
                        "line_index": line_index,
                        "line": line,
                        "matches": [canonical_name],
                        "reason": quality_issue,
                        "value": canonical_value,
                        "value_text": value["value_text"],
                        "source_unit": effective_source_unit,
                        "unit_detection": unit_detection,
                    }
                )
                continue

            fact_name = self._source_fact_name(canonical_name)
            standard_metadata = describe_financial_numeric_fact_name(fact_name)
            if (
                standard_metadata.get("canonical_fact_name") != canonical_name
                or standard_metadata.get("canonical_statement_family")
                != BROKER_RISK_CONTROL_STATEMENT_FAMILY
            ):
                standard_metadata = describe_financial_numeric_fact_name(canonical_name)
            matched_rows[canonical_name] = {
                "line_index": line_index,
                "line": line,
                "value": canonical_value,
            }
            facts.append(
                self._build_fact_snapshot(
                    source_file_id=source_file_id,
                    instrument_id=instrument_id,
                    symbol=symbol,
                    exchange=exchange,
                    report_period=report_period,
                    report_type=report_type,
                    source=source,
                    source_mode=source_mode,
                    source_profile=source_profile,
                    artifact_kind=artifact_kind,
                    licensed_broker_name=licensed_broker_name,
                    listed_broker_scope=listed_broker_scope,
                    report_scope=report_scope,
                    canonical_name=canonical_name,
                    canonical_value=canonical_value,
                    value=value,
                    source_unit=effective_source_unit,
                    unit_scale=effective_unit_scale,
                    unit_detection=unit_detection,
                    line=line,
                    line_index=line_index,
                    extraction_strategy="label_line",
                )
            )

        fixed_order_fallback_rows: List[Dict[str, Any]] = []
        if (
            source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
            and "net_capital" not in matched_rows
        ):
            fixed_order_fallback_rows = self._extract_fixed_order_embedded_rows(
                rows,
                source_unit=source_unit,
                unit_scale=unit_scale,
            )
            for item in fixed_order_fallback_rows:
                canonical_name = str(item["canonical_name"])
                if canonical_name in matched_rows:
                    continue
                value = {
                    "value": item["value"],
                    "value_text": item["value_text"],
                    "percent": item["percent"],
                }
                fallback_source_unit = item.get("source_unit") or source_unit
                fallback_unit_scale = item.get("unit_scale") or unit_scale
                unit_detection = str(item.get("unit_detection") or "global_source_unit")
                unit = self._unit_for(canonical_name)
                canonical_value = self._normalize_value(
                    value,
                    unit=unit,
                    source_unit=fallback_source_unit,
                    unit_scale=fallback_unit_scale,
                )
                if canonical_value is None:
                    ambiguous_rows.append(
                        {
                            "line_index": item["line_index"],
                            "line": item["line"],
                            "matches": [canonical_name],
                            "reason": "fixed_order_source_unit_unknown",
                        }
                    )
                    continue
                quality_issue = self._fact_quality_issue(
                    canonical_name,
                    canonical_value,
                    source_unit=fallback_source_unit,
                    value=value,
                )
                if quality_issue:
                    ambiguous_rows.append(
                        {
                            "line_index": item["line_index"],
                            "line": item["line"],
                            "matches": [canonical_name],
                            "reason": quality_issue,
                            "value": canonical_value,
                            "value_text": value["value_text"],
                            "source_unit": fallback_source_unit,
                            "unit_detection": unit_detection,
                        }
                    )
                    continue
                matched_rows[canonical_name] = {
                    "line_index": item["line_index"],
                    "line": item["line"],
                    "value": canonical_value,
                    "extraction_strategy": "fixed_order_embedded_table",
                }
                facts.append(
                    self._build_fact_snapshot(
                        source_file_id=source_file_id,
                        instrument_id=instrument_id,
                        symbol=symbol,
                        exchange=exchange,
                        report_period=report_period,
                        report_type=report_type,
                        source=source,
                        source_mode=source_mode,
                        source_profile=source_profile,
                        artifact_kind=artifact_kind,
                        licensed_broker_name=licensed_broker_name,
                        listed_broker_scope=listed_broker_scope,
                        report_scope=report_scope,
                        canonical_name=canonical_name,
                        canonical_value=canonical_value,
                        value=value,
                        source_unit=fallback_source_unit,
                        unit_scale=fallback_unit_scale,
                        unit_detection=unit_detection,
                        line=item["line"],
                        line_index=item["line_index"],
                        extraction_strategy="fixed_order_embedded_table",
                    )
                )

        missing_required = [
            name for name in BROKER_RISK_CONTROL_REQUIRED_FACTS if name not in matched_rows
        ]
        diagnostics = {
            **text_diagnostics,
            "numeric_fact_count": len(facts),
            "parser_version": self.parser_version,
            "source_profile": source_profile,
            "artifact_kind": artifact_kind,
            "source_unit": source_unit,
            "source_unit_scale": unit_scale,
            "report_scope": report_scope,
            "licensed_broker_name": licensed_broker_name,
            "listed_broker_scope": listed_broker_scope,
            "report_scope_uncertain": report_scope == "unknown",
            "candidate_row_count": len(rows),
            "matched_canonical_facts": sorted(matched_rows),
            "missing_required_facts": missing_required,
            "ambiguous_rows": ambiguous_rows,
            "fixed_order_fallback_rows": fixed_order_fallback_rows,
            "parse_status": "parsed" if facts else "no_numeric_facts",
        }
        if source_unit is None:
            diagnostics["unknown_units"] = True
        return BrokerRiskControlParseResult(numeric_facts=facts, diagnostics=diagnostics)

    def _build_fact_snapshot(
        self,
        *,
        source_file_id: str,
        instrument_id: str,
        symbol: str,
        exchange: str,
        report_period: str,
        report_type: Optional[str],
        source: str,
        source_mode: str,
        source_profile: str,
        artifact_kind: str,
        licensed_broker_name: Optional[str],
        listed_broker_scope: Optional[Dict[str, Any]],
        report_scope: str,
        canonical_name: str,
        canonical_value: float,
        value: Dict[str, Any],
        source_unit: Optional[str],
        unit_scale: Optional[float],
        unit_detection: str,
        line: str,
        line_index: int,
        extraction_strategy: str,
    ) -> FinancialNumericFactSnapshot:
        unit = self._unit_for(canonical_name)
        fact_name = self._source_fact_name(canonical_name)
        standard_metadata = describe_financial_numeric_fact_name(fact_name)
        if (
            standard_metadata.get("canonical_fact_name") != canonical_name
            or standard_metadata.get("canonical_statement_family")
            != BROKER_RISK_CONTROL_STATEMENT_FAMILY
        ):
            standard_metadata = describe_financial_numeric_fact_name(canonical_name)
        return FinancialNumericFactSnapshot(
            source_file_id=source_file_id,
            instrument_id=instrument_id,
            symbol=symbol,
            exchange=exchange,
            report_period=report_period,
            report_type=report_type or "annual_risk_control",
            statement_family=BROKER_RISK_CONTROL_STATEMENT_FAMILY,
            fact_name=fact_name,
            canonical_fact_name=standard_metadata.get("canonical_fact_name") or canonical_name,
            canonical_statement_family=standard_metadata.get("canonical_statement_family")
            or BROKER_RISK_CONTROL_STATEMENT_FAMILY,
            canonical_semantic=standard_metadata.get("canonical_semantic")
            or BROKER_RISK_CONTROL_CANONICAL_FACTS[canonical_name]["semantic"],
            canonical_unit=standard_metadata.get("canonical_unit") or unit,
            canonical_version=standard_metadata.get("canonical_version"),
            taxonomy_namespace=f"cninfo:{source_profile}",
            context_id=f"broker_risk_control:{report_period}:{canonical_name}",
            unit=source_unit if unit == "CNY" else "percent" if value.get("percent") else "ratio",
            period_end=report_period,
            instant=report_period,
            fact_value=canonical_value,
            value_text=value["value_text"],
            dimensions_json={
                "report_scope": report_scope,
                "licensed_broker_name": licensed_broker_name,
                "listed_broker_scope": listed_broker_scope,
            },
            raw_fact_json={
                "source_profile": source_profile,
                "artifact_kind": artifact_kind,
                "parser_candidate": self.parser_version,
                "source_unit": source_unit,
                "source_unit_scale": unit_scale,
                "unit_detection": unit_detection,
                "report_scope": report_scope,
                "licensed_broker_name": licensed_broker_name,
                "listed_broker_scope": listed_broker_scope,
                "raw_line": line,
                "line_index": line_index,
                "extraction_strategy": extraction_strategy,
                "standardized_fact": standard_metadata,
                "risk_control_field_requiredness": (
                    "required" if canonical_name in BROKER_RISK_CONTROL_REQUIRED_FACTS else "optional"
                ),
            },
            source=source,
            source_mode=source_mode,
            parser_version=self.parser_version,
        )

    def _extract_text(self, payload: bytes | str) -> tuple[str, Dict[str, Any]]:
        if isinstance(payload, str):
            return payload, {"text_extraction": "provided_text", "unparseable_pages": []}
        try:
            from pypdf import PdfReader
        except Exception as exc:  # pragma: no cover - exercised only when dependency is absent
            return "", {
                "text_extraction": "failed",
                "unparseable_pages": ["pypdf_unavailable"],
                "error_message": str(exc),
            }
        unparseable: List[int] = []
        pages: List[str] = []
        try:
            reader = PdfReader(io.BytesIO(payload))
            for page_index, page in enumerate(reader.pages):
                try:
                    pages.append(page.extract_text() or "")
                except Exception:
                    unparseable.append(page_index)
        except Exception as exc:
            return "", {
                "text_extraction": "failed",
                "unparseable_pages": ["document"],
                "error_message": str(exc),
            }
        return "\n".join(pages), {
            "text_extraction": "pypdf",
            "page_count": len(pages),
            "unparseable_pages": unparseable,
            "text_length": sum(len(page) for page in pages),
        }

    def _candidate_rows(self, text: str) -> List[str]:
        normalized = re.sub(r"[ \t]+", " ", str(text or ""))
        rows = []
        for line in normalized.splitlines():
            row = line.strip()
            if row:
                rows.append(row)
        return rows

    def _extract_fixed_order_embedded_rows(
        self,
        rows: Sequence[str],
        *,
        source_unit: Optional[str],
        unit_scale: Optional[float],
    ) -> List[Dict[str, Any]]:
        """Recover annual-report risk-control tables when PDF fonts garble labels.

        The fallback is deliberately conservative: it only accepts a block whose
        first rows satisfy core net capital + subordinated net capital ~= net
        capital and whose following rows look like common regulatory ratios.
        """
        numeric_rows: List[Dict[str, Any]] = []
        for line_index, line in enumerate(rows):
            numbers = self._numbers_in_line(line)
            if len(numbers) >= 2:
                numeric_rows.append(
                    {
                        "line_index": line_index,
                        "line": line,
                        "numbers": numbers,
                    }
                )
        minimum_rows = 10
        for start in range(0, max(0, len(numeric_rows) - minimum_rows + 1)):
            window = numeric_rows[start : start + len(self._FIXED_ORDER_EMBEDDED_FACTS)]
            if len(window) < minimum_rows:
                continue
            if not self._looks_like_fixed_order_risk_control_block(window, source_unit=source_unit):
                continue
            result: List[Dict[str, Any]] = []
            for canonical_name, row in zip(self._FIXED_ORDER_EMBEDDED_FACTS, window):
                first = row["numbers"][0]
                row_source_unit, row_unit_scale, unit_detection = self._effective_money_unit(
                    canonical_name,
                    first["value"],
                    source_unit=source_unit,
                    unit_scale=unit_scale,
                    rows=rows,
                    line_index=int(row["line_index"]),
                    line=str(row["line"]),
                )
                result.append(
                    {
                        "canonical_name": canonical_name,
                        "line_index": row["line_index"],
                        "line": row["line"],
                        "value": first["value"],
                        "value_text": first["value_text"],
                        "percent": first["percent"],
                        "source_unit": row_source_unit,
                        "unit_scale": row_unit_scale,
                        "unit_detection": unit_detection,
                    }
                )
            return result
        return []

    def _numbers_in_line(self, line: str) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        for match in self._NUMBER_PATTERN.finditer(line):
            value_text = match.group("value")
            try:
                value = float(self._normalize_number_text(value_text))
            except ValueError:
                continue
            result.append(
                {
                    "value": value,
                    "value_text": value_text,
                    "percent": bool(match.group("percent")),
                }
            )
        return result

    def _looks_like_fixed_order_risk_control_block(
        self,
        window: Sequence[Dict[str, Any]],
        *,
        source_unit: Optional[str],
    ) -> bool:
        if len(window) < 10:
            return False
        gaps = [
            int(window[index + 1]["line_index"]) - int(window[index]["line_index"])
            for index in range(min(9, len(window) - 1))
        ]
        if any(gap <= 0 or gap > 4 for gap in gaps):
            return False
        first_values = [abs(float(row["numbers"][0]["value"])) for row in window[:10]]
        core, subordinated, net_capital = first_values[0], first_values[1], first_values[2]
        if net_capital <= 0:
            return False
        if abs((core + subordinated) - net_capital) / net_capital > 0.08:
            return False
        source_unit_known = source_unit in self._MONEY_UNITS
        raw_amounts_look_absolute = all(value >= 100_000_000 for value in first_values[:6])
        if not source_unit_known and not raw_amounts_look_absolute:
            return False
        risk_coverage, capital_leverage, lcr, nsfr = first_values[6:10]
        return (
            50.0 <= risk_coverage <= 1000.0
            and 3.0 <= capital_leverage <= 100.0
            and 50.0 <= lcr <= 1000.0
            and 50.0 <= nsfr <= 500.0
        )

    def _effective_money_unit(
        self,
        canonical_name: str,
        raw_value: float,
        *,
        source_unit: Optional[str],
        unit_scale: Optional[float],
        rows: Sequence[str],
        line_index: int,
        line: str,
    ) -> tuple[Optional[str], Optional[float], str]:
        if self._unit_for(canonical_name) != "CNY":
            return source_unit, unit_scale, "not_money_fact"
        if abs(float(raw_value)) >= 100_000_000:
            return "元", 1.0, "absolute_yuan_value"
        local_unit = self._detect_local_money_unit(
            rows,
            line_index=line_index,
            line=line,
        )
        if local_unit:
            return local_unit, self._MONEY_UNITS[local_unit], "local_context_unit"
        if source_unit in self._MONEY_UNITS:
            return source_unit, unit_scale or self._MONEY_UNITS[source_unit], "document_unit"
        return source_unit, unit_scale, "unknown_unit"

    def _detect_local_money_unit(
        self,
        rows: Sequence[str],
        *,
        line_index: int,
        line: str,
    ) -> Optional[str]:
        """Find the nearest table/header money unit instead of trusting whole-document unit."""
        same_line = self._money_unit_from_line(line)
        if same_line:
            return same_line
        start = max(0, int(line_index) - 8)
        for candidate in reversed(rows[start:int(line_index)]):
            unit = self._money_unit_from_line(candidate)
            if unit:
                return unit
            if re.search(r"第[一二三四五六七八九十]+[章节]|释义|目录", candidate):
                break
        return None

    def _money_unit_from_line(self, line: str) -> Optional[str]:
        text = str(line or "")
        unit_match = self._MONEY_UNIT_PATTERN.search(text)
        if unit_match:
            return unit_match.group(1)
        paren_match = self._PAREN_MONEY_UNIT_PATTERN.search(text)
        if paren_match and ("项目" in text or "指标" in text or "金额" in text):
            return paren_match.group(1)
        return None

    def _fact_quality_issue(
        self,
        canonical_name: str,
        canonical_value: float,
        *,
        source_unit: Optional[str],
        value: Dict[str, Any],
    ) -> Optional[str]:
        if canonical_value != canonical_value:
            return "numeric_value_nan"
        unit = self._unit_for(canonical_name)
        if unit == "CNY":
            if source_unit not in self._MONEY_UNITS:
                return "source_unit_unknown"
            if abs(canonical_value) > 10_000_000_000_000:
                return "money_value_out_of_plausible_range"
            if canonical_name in {
                "net_capital",
                "core_net_capital",
                "regulatory_net_assets",
                "risk_capital_reserve_total",
                "balance_sheet_assets_total",
            } and canonical_value < 100_000_000:
                return "money_value_out_of_plausible_range"
        elif unit == "ratio":
            if canonical_value < 0:
                return "ratio_value_out_of_plausible_range"
            if canonical_value > 100:
                return "ratio_value_out_of_plausible_range"
            raw = abs(float(value.get("value") or 0.0))
            if raw > 10_000 and not value.get("percent"):
                return "ratio_value_out_of_plausible_range"
        return None

    def _detect_money_unit(self, text: str) -> tuple[Optional[str], Optional[float]]:
        payload = str(text or "")
        unit_match = self._MONEY_UNIT_PATTERN.search(payload)
        if unit_match:
            unit = unit_match.group(1)
            return unit, self._MONEY_UNITS[unit]
        head = payload[:5000]
        for unit in ("百万元", "亿元", "万元", "千元", "元"):
            if f"单位：{unit}" in head or f"单位:{unit}" in head:
                return unit, self._MONEY_UNITS[unit]
        return None, None

    def _detect_report_scope(self, text: str) -> str:
        payload = str(text or "")
        if re.search(r"母公司|公司本部|母公司的净资本及风险控制指标", payload):
            return "parent_company"
        if re.search(r"合并口径|合并报表", payload):
            return "consolidated"
        if re.search(r"监管口径|风险控制指标监管报表", payload):
            return "regulatory"
        return "unknown"

    def _match_canonical_labels(self, line: str) -> List[str]:
        compact = re.sub(r"[\s　]+", "", line)
        matches = [
            canonical
            for canonical, pattern in self._label_patterns
            if pattern.search(compact)
        ]
        if "core_net_capital" in matches and "net_capital" in matches:
            matches.remove("net_capital")
        if "subordinated_net_capital" in matches and "net_capital" in matches:
            matches.remove("net_capital")
        if "net_capital_to_net_assets" in matches:
            for generic in ("net_capital", "regulatory_net_assets"):
                if generic in matches:
                    matches.remove(generic)
        if "net_capital_to_liabilities" in matches and "net_capital" in matches:
            matches.remove("net_capital")
        if "margin_financing_to_net_capital" in matches and "net_capital" in matches:
            matches.remove("net_capital")
        if (
            "proprietary_equity_securities_to_net_capital" in matches
            and "net_capital" in matches
        ):
            matches.remove("net_capital")
        if (
            "proprietary_non_equity_securities_to_net_capital" in matches
            and "net_capital" in matches
        ):
            matches.remove("net_capital")
        if "regulatory_net_assets" in matches and "net_assets_to_liabilities" in matches:
            matches.remove("regulatory_net_assets")
        return matches

    def _extract_numeric_value(self, line: str, *, canonical_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        search_text = line
        label_end = self._label_match_end(line, canonical_name) if canonical_name else 0
        matches = [match for match in self._NUMBER_PATTERN.finditer(search_text) if match.start() >= label_end]
        if not matches:
            matches = list(self._NUMBER_PATTERN.finditer(line))
        if not matches:
            return None
        match = matches[0]
        value_text = match.group("value")
        try:
            value = float(self._normalize_number_text(value_text))
        except ValueError:
            return None
        return {
            "value": value,
            "value_text": value_text,
            "percent": bool(match.group("percent")),
        }

    def _normalize_number_text(self, value_text: str) -> str:
        return re.sub(r"\s+", "", str(value_text or "")).replace(",", "")

    def _label_match_end(self, line: str, canonical_name: Optional[str]) -> int:
        if not canonical_name:
            return 0
        labels = BROKER_RISK_CONTROL_CANONICAL_FACTS.get(canonical_name, {}).get("aliases", [])
        candidates = [canonical_name, *labels]
        compact_line = re.sub(r"[\s　]+", "", line)
        best_end = 0
        for label in candidates:
            compact_label = str(label).replace(" ", "")
            if not compact_label:
                continue
            index = compact_line.find(compact_label)
            if index >= 0:
                best_end = max(best_end, index + len(compact_label))
        return best_end

    def _normalize_value(
        self,
        value: Dict[str, Any],
        *,
        unit: str,
        source_unit: Optional[str],
        unit_scale: Optional[float],
    ) -> Optional[float]:
        raw = float(value["value"])
        if unit == "ratio":
            return raw / 100.0 if value.get("percent") or raw > 10.0 else raw
        if unit == "CNY":
            if unit_scale is None:
                return None
            return raw * unit_scale
        return raw

    def _source_fact_name(self, canonical_name: str) -> str:
        aliases = BROKER_RISK_CONTROL_CANONICAL_FACTS[canonical_name].get("aliases", [])
        return str(aliases[0] if aliases else canonical_name)

    def _unit_for(self, canonical_name: str) -> str:
        return str(BROKER_RISK_CONTROL_CANONICAL_FACTS[canonical_name]["unit"])

    def _compile_label_patterns(self) -> List[tuple[str, re.Pattern[str]]]:
        entries = []
        for canonical, spec in BROKER_RISK_CONTROL_CANONICAL_FACTS.items():
            labels = [canonical, *spec.get("aliases", [])]
            escaped = [re.escape(str(label).replace(" ", "")) for label in labels if str(label)]
            entries.append((canonical, re.compile("|".join(escaped))))
        return sorted(entries, key=lambda item: len(item[1].pattern), reverse=True)


@dataclass
class BrokerRiskControlSyncResult:
    status: str
    mode: str
    target_instruments: int = 0
    target_periods: int = 0
    reports_discovered: int = 0
    reports_parsed: int = 0
    facts_parsed: int = 0
    facts_written: int = 0
    unchanged_reports: int = 0
    missing_reports: int = 0
    parse_failures: int = 0
    announcements_scanned: int = 0
    matching_announcements: int = 0
    retryable_pending_reports: int = 0
    filtered_announcements: int = 0
    errors: List[str] = field(default_factory=list)
    parse_failure_details: List[Dict[str, Any]] = field(default_factory=list)
    report_summaries: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "mode": self.mode,
            "target_instruments": self.target_instruments,
            "target_periods": self.target_periods,
            "reports_discovered": self.reports_discovered,
            "reports_parsed": self.reports_parsed,
            "facts_parsed": self.facts_parsed,
            "facts_written": self.facts_written,
            "unchanged_reports": self.unchanged_reports,
            "missing_reports": self.missing_reports,
            "parse_failures": self.parse_failures,
            "announcements_scanned": self.announcements_scanned,
            "matching_announcements": self.matching_announcements,
            "retryable_pending_reports": self.retryable_pending_reports,
            "filtered_announcements": self.filtered_announcements,
            "errors": list(self.errors),
            "parse_failure_details": list(self.parse_failure_details),
            "report_summaries": list(self.report_summaries),
        }


PayloadFetcher = Callable[[CninfoAnnouncementRecord], Optional[bytes | str]]


class BrokerRiskControlReportSyncService:
    """Backfill and incremental sync for broker risk-control report facts."""

    def __init__(
        self,
        *,
        storage: Any,
        scanner: Optional[CninfoAnnouncementScanner] = None,
        parser: Optional[BrokerRiskControlPdfFactParser] = None,
        payload_fetcher: Optional[PayloadFetcher] = None,
        title_patterns: Optional[Sequence[str]] = None,
        source_profile: str = BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
        allow_non_broker_validation_override: bool = False,
        archive_root: Optional[str | Path] = None,
        force_reparse_existing: bool = False,
        replace_existing_facts: bool = False,
    ) -> None:
        self.storage = storage
        self.scanner = scanner or CninfoAnnouncementScanner()
        if parser is None and source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE:
            parser = BrokerRiskControlPdfFactParser(
                parser_version=BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION
            )
        self.parser = parser or BrokerRiskControlPdfFactParser()
        self.payload_fetcher = payload_fetcher or self._download_payload
        self.title_patterns = list(title_patterns or [])
        self.source_profile = source_profile
        self.allow_non_broker_validation_override = allow_non_broker_validation_override
        self.archive_root = None if archive_root is None else Path(archive_root)
        self.force_reparse_existing = bool(force_reparse_existing)
        self.replace_existing_facts = bool(replace_existing_facts)

    def backfill(
        self,
        *,
        instruments: Sequence[Dict[str, Any]],
        report_periods: Sequence[str],
        announcement_records: Optional[Iterable[CninfoAnnouncementRecord]] = None,
        ingestion_run_id: Optional[int] = None,
        tier: str = "history",
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        result = BrokerRiskControlSyncResult(
            status="success",
            mode="historical_backfill",
            target_instruments=len(instruments),
            target_periods=len(report_periods),
        )
        instrument_by_symbol = {
            str(item.get("symbol") or "").strip(): item
            for item in instruments
            if item.get("symbol")
        }
        records = list(announcement_records or [])
        for record in records:
            instrument = self._resolve_record_instrument(record, instrument_by_symbol)
            if instrument is None or not self._instrument_in_scope(instrument) or not self._record_matches(record):
                result.filtered_announcements += 1
                continue
            if not self._record_period(record) in set(str(period) for period in report_periods):
                result.filtered_announcements += 1
                continue
            self._process_record(
                record,
                instrument,
                result,
                ingestion_run_id=ingestion_run_id,
                tier=tier,
                dry_run=dry_run,
            )
        result.reports_discovered = result.matching_announcements
        result.missing_reports = max(0, result.target_instruments * result.target_periods - result.reports_discovered)
        if result.parse_failures or result.retryable_pending_reports:
            result.status = "partial"
        return result.to_dict()

    def incremental_update(
        self,
        *,
        market: str,
        column: str,
        instruments: Sequence[Dict[str, Any]],
        ingestion_run_id: Optional[int] = None,
        tier: str = "hot",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        page_size: int = 30,
        max_pages: int = 20,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        purpose_key = self.source_profile
        state = self.storage.get_cninfo_announcement_scan_state(
            purpose_key=purpose_key,
            market=market,
            column=column,
        )
        scan_result = self.scanner.scan(
            CninfoAnnouncementScanConfig(
                purpose_key=purpose_key,
                market=market,
                column=column,
                search_key="年度报告"
                if self.source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
                else "风险控制指标",
                start_date=start_date,
                end_date=end_date,
                page_size=page_size,
                max_pages=max_pages,
                stop_at_watermark=None if state is None else state.get("last_watermark"),
            ),
            filters=[self._record_filter],
        )
        result = BrokerRiskControlSyncResult(
            status="success",
            mode="incremental_update",
            target_instruments=len(instruments),
            announcements_scanned=scan_result.announcements_seen,
            matching_announcements=0,
            filtered_announcements=scan_result.announcements_seen - len(scan_result.selected_records),
            errors=list(scan_result.errors),
        )
        instrument_by_symbol = {
            str(item.get("symbol") or "").strip(): item
            for item in instruments
            if item.get("symbol")
        }
        for record in scan_result.selected_records:
            instrument = self._resolve_record_instrument(record, instrument_by_symbol)
            if instrument is None or not self._instrument_in_scope(instrument):
                result.filtered_announcements += 1
                continue
            self._process_record(
                record,
                instrument,
                result,
                ingestion_run_id=ingestion_run_id,
                tier=tier,
                dry_run=dry_run,
            )
        if result.parse_failures or result.retryable_pending_reports or result.errors:
            result.status = "partial"
        if not dry_run:
            self.storage.upsert_cninfo_announcement_scan_state(
                purpose_key=purpose_key,
                market=market,
                column=column,
                last_watermark=scan_result.max_announcement_time,
                last_scan_started_at=get_shanghai_time().isoformat(),
                last_scan_completed_at=get_shanghai_time().isoformat(),
                pages_scanned=scan_result.pages_scanned,
                announcements_seen=scan_result.announcements_seen,
                selected_announcements=len(scan_result.selected_records),
                status=result.status,
                metadata={
                    "source_profile": self.source_profile,
                    "retryable_pending_reports": result.retryable_pending_reports,
                    "stopped_at_watermark": scan_result.stopped_at_watermark,
                },
            )
        return result.to_dict()

    def _record_filter(self, record: CninfoAnnouncementRecord) -> List[str]:
        if not self._record_matches(record):
            return []
        if self.source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE:
            return ["formal_annual_or_semiannual_report"]
        return ["broker_risk_control_title"]

    def _record_matches(self, record: CninfoAnnouncementRecord) -> bool:
        if self.source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE:
            return is_formal_broker_annual_or_semiannual_report_title(record.title)
        return is_broker_risk_control_title(record.title, title_patterns=self.title_patterns)

    def _instrument_in_scope(self, instrument: Dict[str, Any]) -> bool:
        return is_broker_risk_control_instrument(
            instrument,
            allow_validation_override=self.allow_non_broker_validation_override,
        )

    def _process_record(
        self,
        record: CninfoAnnouncementRecord,
        instrument: Dict[str, Any],
        result: BrokerRiskControlSyncResult,
        *,
        ingestion_run_id: Optional[int],
        tier: str,
        dry_run: bool,
    ) -> None:
        result.matching_announcements += 1
        report_period = self._record_period(record)
        LOGGER.info(
            "broker risk-control report start: instrument_id=%s symbol=%s exchange=%s report_period=%s announcement_id=%s title=%s dry_run=%s",
            instrument.get("instrument_id"),
            instrument.get("symbol"),
            instrument.get("exchange"),
            report_period,
            record.announcement_id,
            record.title,
            dry_run,
        )
        try:
            payload = self.payload_fetcher(record)
        except Exception as exc:
            result.retryable_pending_reports += 1
            result.errors.append(str(exc))
            LOGGER.warning(
                "broker risk-control report payload failed: instrument_id=%s symbol=%s report_period=%s announcement_id=%s error=%s",
                instrument.get("instrument_id"),
                instrument.get("symbol"),
                report_period,
                record.announcement_id,
                exc,
            )
            return
        if not payload:
            result.retryable_pending_reports += 1
            LOGGER.warning(
                "broker risk-control report payload empty: instrument_id=%s symbol=%s report_period=%s announcement_id=%s",
                instrument.get("instrument_id"),
                instrument.get("symbol"),
                report_period,
                record.announcement_id,
            )
            return
        hash_payload = payload.encode("utf-8") if isinstance(payload, str) else payload
        content_hash = hashlib.sha256(hash_payload).hexdigest()
        LOGGER.info(
            "broker risk-control report payload loaded: instrument_id=%s symbol=%s report_period=%s announcement_id=%s bytes=%s",
            instrument.get("instrument_id"),
            instrument.get("symbol"),
            report_period,
            record.announcement_id,
            len(hash_payload),
        )
        archive_path = None if dry_run else self._archive_payload(record, instrument, report_period, hash_payload)
        manifest = self._build_manifest(
            record,
            instrument,
            report_period,
            payload,
            content_hash,
            archive_path=archive_path,
        )
        if not self.force_reparse_existing and self._unchanged_manifest_exists(manifest, content_hash):
            result.unchanged_reports += 1
            LOGGER.info(
                "broker risk-control report unchanged: instrument_id=%s symbol=%s report_period=%s announcement_id=%s content_hash=%s",
                instrument.get("instrument_id"),
                instrument.get("symbol"),
                report_period,
                record.announcement_id,
                content_hash[:12],
            )
            return
        source_file_id = (
            f"dryrun:{record.announcement_id}:{content_hash[:12]}"
            if dry_run
            else self.storage.upsert_financial_source_file_manifest(
                manifest,
                ingestion_run_id=ingestion_run_id,
            )
        )
        try:
            scope_resolution = resolve_listed_broker_dealer_scope(instrument)
            listed_scope = scope_resolution.to_dict()
            licensed_broker_name = (
                scope_resolution.entry.licensed_broker_name if scope_resolution.entry else None
            )
            parsed = self.parser.parse(
                payload,
                source_file_id=source_file_id,
                instrument_id=str(instrument.get("instrument_id") or ""),
                symbol=str(instrument.get("symbol") or ""),
                exchange=str(instrument.get("exchange") or ""),
                report_period=report_period,
                report_type=self._record_report_type(record),
                source="cninfo",
                source_mode="direct",
                source_profile=self.source_profile,
                artifact_kind=self._artifact_kind(),
                licensed_broker_name=licensed_broker_name,
                listed_broker_scope=listed_scope,
            )
        except Exception as exc:
            result.parse_failures += 1
            result.errors.append(str(exc))
            result.parse_failure_details.append(
                self._parse_failure_detail(
                    record,
                    instrument,
                    report_period,
                    reason="parser_exception",
                    error=str(exc),
                )
            )
            LOGGER.warning(
                "broker risk-control report parse failed: instrument_id=%s symbol=%s report_period=%s announcement_id=%s error=%s",
                instrument.get("instrument_id"),
                instrument.get("symbol"),
                report_period,
                record.announcement_id,
                exc,
            )
            return
        result.reports_parsed += 1
        result.facts_parsed += len(parsed.numeric_facts)
        result.report_summaries.append(
            self._parsed_report_summary(
                record,
                instrument,
                report_period,
                parsed,
            )
        )
        LOGGER.info(
            "broker risk-control report parsed: instrument_id=%s symbol=%s report_period=%s announcement_id=%s facts=%s diagnostics_keys=%s",
            instrument.get("instrument_id"),
            instrument.get("symbol"),
            report_period,
            record.announcement_id,
            len(parsed.numeric_facts),
            ",".join(sorted(parsed.diagnostics.keys())) if parsed.diagnostics else "",
        )
        if dry_run:
            if not parsed.numeric_facts:
                result.parse_failures += 1
                result.parse_failure_details.append(
                    self._parse_failure_detail(
                        record,
                        instrument,
                        report_period,
                        reason="no_numeric_facts",
                        diagnostics=parsed.diagnostics,
                    )
                )
                LOGGER.warning(
                    "broker risk-control report dry-run parsed no facts: instrument_id=%s symbol=%s report_period=%s announcement_id=%s",
                    instrument.get("instrument_id"),
                    instrument.get("symbol"),
                    report_period,
                    record.announcement_id,
                )
            return
        parsed_manifest = FinancialSourceFileManifest(
            **{
                **manifest.__dict__,
                "source_file_id": source_file_id,
                "status": "parsed" if parsed.numeric_facts else "parse_failed",
                "parser_diagnostics": parsed.diagnostics,
            }
        )
        self.storage.upsert_financial_source_file_manifest(
            parsed_manifest,
            ingestion_run_id=ingestion_run_id,
        )
        if self.replace_existing_facts and hasattr(
            self.storage,
            "replace_financial_numeric_facts_for_source_file",
        ):
            replace_result = self.storage.replace_financial_numeric_facts_for_source_file(
                source_file_id,
                parsed.numeric_facts,
                ingestion_run_id=ingestion_run_id,
                tier=tier,
                parser_version=self.parser.parser_version,
                statement_family=BROKER_RISK_CONTROL_STATEMENT_FAMILY,
            )
            result.facts_written += int(replace_result.get("inserted") or 0)
            LOGGER.info(
                "broker risk-control report facts replaced: instrument_id=%s symbol=%s report_period=%s announcement_id=%s deleted=%s inserted=%s tier=%s",
                instrument.get("instrument_id"),
                instrument.get("symbol"),
                report_period,
                record.announcement_id,
                replace_result.get("deleted"),
                replace_result.get("inserted"),
                tier,
            )
        else:
            result.facts_written += self.storage.upsert_financial_numeric_facts(
                parsed.numeric_facts,
                ingestion_run_id=ingestion_run_id,
                tier=tier,
            )
        LOGGER.info(
            "broker risk-control report write done: instrument_id=%s symbol=%s report_period=%s announcement_id=%s facts=%s tier=%s",
            instrument.get("instrument_id"),
            instrument.get("symbol"),
            report_period,
            record.announcement_id,
            len(parsed.numeric_facts),
            tier,
        )
        if not parsed.numeric_facts:
            result.parse_failures += 1
            result.parse_failure_details.append(
                self._parse_failure_detail(
                    record,
                    instrument,
                    report_period,
                    reason="no_numeric_facts",
                    diagnostics=parsed.diagnostics,
                )
            )
            LOGGER.warning(
                "broker risk-control report write produced no facts: instrument_id=%s symbol=%s report_period=%s announcement_id=%s",
                instrument.get("instrument_id"),
                instrument.get("symbol"),
                report_period,
                record.announcement_id,
            )

    def _parse_failure_detail(
        self,
        record: CninfoAnnouncementRecord,
        instrument: Dict[str, Any],
        report_period: str,
        *,
        reason: str,
        error: str = "",
        diagnostics: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return {
            "reason": reason,
            "error": error,
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "announcement_id": record.announcement_id,
            "title": record.title,
            "report_period": report_period,
            "announcement_time": record.announcement_time,
            "adjunct_url": record.adjunct_url,
            "diagnostics": diagnostics or {},
        }

    def _parsed_report_summary(
        self,
        record: CninfoAnnouncementRecord,
        instrument: Dict[str, Any],
        report_period: str,
        parsed: BrokerRiskControlParseResult,
    ) -> Dict[str, Any]:
        facts_by_canonical = {
            str(fact.canonical_fact_name or fact.fact_name): fact
            for fact in parsed.numeric_facts
        }
        net_capital = facts_by_canonical.get("net_capital")
        return {
            "instrument_id": instrument.get("instrument_id"),
            "symbol": instrument.get("symbol"),
            "exchange": instrument.get("exchange"),
            "report_period": report_period,
            "announcement_id": record.announcement_id,
            "title": record.title,
            "fact_count": len(parsed.numeric_facts),
            "parse_status": parsed.diagnostics.get("parse_status"),
            "matched_canonical_facts": parsed.diagnostics.get("matched_canonical_facts")
            or [],
            "missing_required_facts": parsed.diagnostics.get("missing_required_facts")
            or [],
            "net_capital": None
            if net_capital is None
            else {
                "fact_value": net_capital.fact_value,
                "value_text": net_capital.value_text,
                "unit": net_capital.unit,
                "unit_detection": net_capital.raw_fact_json.get("unit_detection"),
                "line_index": net_capital.raw_fact_json.get("line_index"),
            },
        }

    def _build_manifest(
        self,
        record: CninfoAnnouncementRecord,
        instrument: Dict[str, Any],
        report_period: str,
        payload: bytes | str,
        content_hash: str,
        archive_path: Optional[str] = None,
    ) -> FinancialSourceFileManifest:
        classification = self._classify_record(record)
        scope_resolution = resolve_listed_broker_dealer_scope(instrument)
        return FinancialSourceFileManifest(
            source="cninfo",
            source_mode="direct",
            instrument_id=str(instrument.get("instrument_id") or ""),
            symbol=str(instrument.get("symbol") or ""),
            exchange=str(instrument.get("exchange") or ""),
            report_period=report_period,
            report_type=self._record_report_type(record),
            filing_id=record.announcement_id,
            source_url=record.adjunct_url,
            archive_path=archive_path,
            content_hash=content_hash,
            content_length=len(payload),
            published_at=record.announcement_time,
            downloaded_at=get_shanghai_time().isoformat(),
            parser_version=self.parser.parser_version,
            status="downloaded",
            metadata_json={
                **classification,
                "announcement_title": record.title,
                "source_profile": self.source_profile,
                "listed_broker_dealer_scope": scope_resolution.to_dict(),
                "announcement_record": {
                    "market": record.market,
                    "column": record.column,
                    "symbols": record.symbols,
                    "selection_reasons": record.selection_reasons,
                },
            },
        )

    def _archive_payload(
        self,
        record: CninfoAnnouncementRecord,
        instrument: Dict[str, Any],
        report_period: str,
        payload: bytes,
    ) -> Optional[str]:
        if self.archive_root is None:
            return None
        exchange = str(instrument.get("exchange") or "UNKNOWN")
        symbol = str(instrument.get("symbol") or (record.symbols[0] if record.symbols else "UNKNOWN"))
        filename = f"{symbol}_{report_period}_{record.announcement_id}.pdf"
        safe_filename = re.sub(r"[^0-9A-Za-z_.-]+", "_", filename)
        path = self.archive_root / exchange / symbol / safe_filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return str(path)

    def _unchanged_manifest_exists(
        self,
        manifest: FinancialSourceFileManifest,
        content_hash: str,
    ) -> bool:
        rows = self.storage.get_financial_source_file_manifests(
            instrument_id=manifest.instrument_id,
            report_period=manifest.report_period,
            source=manifest.source,
        )
        return any(
            row.get("content_hash") == content_hash
            and row.get("parser_version") == manifest.parser_version
            and row.get("status") in {"downloaded", "parsed"}
            for row in rows
        )

    def _record_period(self, record: CninfoAnnouncementRecord) -> str:
        if self.source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE:
            return infer_broker_annual_report_period(record)
        return infer_broker_risk_control_report_period(record)

    def _record_report_type(self, record: CninfoAnnouncementRecord) -> str:
        if self.source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE:
            title = _normalize_announcement_title(record.title)
            return "semiannual" if "半年度报告" in title else "annual"
        period = self._record_period(record)
        if period.endswith("06-30"):
            return "semiannual_risk_control"
        if period.endswith(("03-31", "09-30")):
            return "quarterly_risk_control"
        return "annual_risk_control"

    def _artifact_kind(self) -> str:
        if self.source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE:
            return BROKER_ANNUAL_REPORT_RISK_CONTROL_ARTIFACT_KIND
        return BROKER_RISK_CONTROL_ARTIFACT_KIND

    def _classify_record(self, record: CninfoAnnouncementRecord) -> Dict[str, Any]:
        if self.source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE:
            return classify_broker_annual_report_risk_control_artifact(
                record.title,
                adjunct_type=record.adjunct_type,
            ) or {}
        payload = classify_broker_risk_control_artifact(
            record.title,
            adjunct_type=record.adjunct_type,
        ) or {}
        if payload:
            payload["source_priority"] = "supplementary"
        return payload

    def _resolve_record_instrument(
        self,
        record: CninfoAnnouncementRecord,
        instrument_by_symbol: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        for symbol in record.symbols:
            clean = str(symbol).strip()
            if clean in instrument_by_symbol:
                return instrument_by_symbol[clean]
        return None

    def _download_payload(self, record: CninfoAnnouncementRecord) -> Optional[bytes]:
        if not record.adjunct_url:
            return None
        url = record.adjunct_url
        if not url.startswith(("http://", "https://")):
            url = f"https://static.cninfo.com.cn/{url.lstrip('/')}"
        response = request_get(
            url,
            tls_config=HttpTlsConfig(source_name="cninfo"),
            timeout=20,
        )
        response.raise_for_status()
        return response.content
