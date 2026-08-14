"""
Broker risk-control report parsing and financial fact integration helpers.
"""

from __future__ import annotations

import hashlib
import io
import logging
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from research.announcements import (
    AnnouncementAcquisitionService,
    AnnouncementAttachment,
    AnnouncementAttachmentRetriever,
    AnnouncementQuery,
    AnnouncementRecord,
    AnnouncementScope,
    ProviderCursor,
    build_announcement_key,
    load_announcement_acquisition_config,
)
from research.financial_fact_aliases import describe_financial_numeric_fact_name
from research.providers.base import (
    FinancialFilingPayload,
    FinancialNumericFactSnapshot,
    FinancialSourceFileManifest,
)
from research.providers.registry import OfficialAnnouncementProviderRegistry
from research.listed_broker_dealer_scope import (
    enrich_instrument_with_broker_scope,
    is_confirmed_listed_broker_dealer,
    resolve_listed_broker_dealer_scope,
)
from utils.date_utils import get_shanghai_time
from utils.config_manager import ResearchConfig, config_manager


BROKER_RISK_CONTROL_SOURCE_PROFILE = "broker_risk_control_report"
BROKER_RISK_CONTROL_ARTIFACT_KIND = "broker_risk_control_pdf"
BROKER_RISK_CONTROL_PARSER_VERSION = "broker_risk_control_pdf.v1"
BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE = "broker_annual_report_embedded_risk_control"
BROKER_ANNUAL_REPORT_RISK_CONTROL_ARTIFACT_KIND = "broker_annual_or_semiannual_report_pdf"
BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION = "broker_annual_report_embedded_risk_control_pdf.v1"
BROKER_RISK_CONTROL_STATEMENT_FAMILY = "regulatory_risk_control"
LOGGER = logging.getLogger(__name__)


def validate_broker_shared_asset_processing(
    storage: Any,
    asset: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate persisted broker facts against one exact shared asset binding."""

    binding = _normalize_bound_shared_annual_report_asset(asset)
    manifests = storage.get_financial_source_file_manifests(
        instrument_id=binding["instrument_id"],
        report_period=binding["report_period"],
        source=binding["source"],
        filing_id=binding["source_announcement_id"],
        statuses=("parsed",),
    )
    matching_manifests: list[Mapping[str, Any]] = []
    for manifest in manifests:
        metadata = manifest.get("metadata") or {}
        lineage = (
            metadata.get("shared_annual_report_asset")
            if isinstance(metadata, Mapping)
            else None
        )
        if not isinstance(lineage, Mapping):
            continue
        if (
            manifest.get("parser_version")
            == BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION
            and manifest.get("source_mode") == "shared_announcement_asset"
            and str(manifest.get("content_hash") or "").lower()
            == binding["content_hash"]
            and all(
                str(lineage.get(field) or "").strip().lower()
                == str(binding[field]).strip().lower()
                for field in ("asset_id", "observation_version", "content_hash")
            )
        ):
            matching_manifests.append(manifest)
    if len(matching_manifests) != 1:
        return {
            "ready": False,
            "reason_code": "shared_broker_manifest_not_unique",
            "matching_manifest_count": len(matching_manifests),
            "required_facts": list(BROKER_RISK_CONTROL_REQUIRED_FACTS),
        }

    source_file_id = str(matching_manifests[0].get("source_file_id") or "")
    facts = storage.get_financial_numeric_facts(
        binding["instrument_id"],
        include_history=True,
        report_period=binding["report_period"],
    )
    matching_facts: list[Mapping[str, Any]] = []
    invalid_lineage_count = 0
    for fact in facts:
        if str(fact.get("source_file_id") or "") != source_file_id:
            continue
        if (
            fact.get("parser_version")
            != BROKER_ANNUAL_REPORT_RISK_CONTROL_PARSER_VERSION
            or fact.get("source_mode") != "shared_announcement_asset"
        ):
            invalid_lineage_count += 1
            continue
        raw_fact = fact.get("raw_fact") or {}
        dimensions = fact.get("dimensions") or {}
        lineages = (
            raw_fact.get("source_asset_lineage")
            if isinstance(raw_fact, Mapping)
            else None,
            dimensions.get("source_asset_lineage")
            if isinstance(dimensions, Mapping)
            else None,
        )
        if not all(
            isinstance(lineage, Mapping)
            and all(
                str(lineage.get(field) or "").strip().lower()
                == str(binding[field]).strip().lower()
                for field in ("asset_id", "observation_version", "content_hash")
            )
            for lineage in lineages
        ):
            invalid_lineage_count += 1
            continue
        matching_facts.append(fact)
    canonical_facts = {
        str(fact.get("canonical_fact_name") or fact.get("fact_name") or "")
        for fact in matching_facts
    }
    missing_required = sorted(
        set(BROKER_RISK_CONTROL_REQUIRED_FACTS) - canonical_facts
    )
    ready = bool(matching_facts) and invalid_lineage_count == 0
    return {
        "ready": ready,
        "reason_code": (
            None
            if ready
            else "broker_fact_lineage_invalid"
            if invalid_lineage_count
            else "broker_fact_output_empty"
        ),
        "source_file_id": source_file_id,
        "fact_count": len(matching_facts),
        "invalid_lineage_count": invalid_lineage_count,
        "required_facts": list(BROKER_RISK_CONTROL_REQUIRED_FACTS),
        "missing_required_facts": missing_required,
        "business_fact_complete": not missing_required,
    }


def _normalize_bound_shared_annual_report_asset(
    asset: Mapping[str, Any],
) -> dict[str, Any]:
    binding = {
        **dict(asset),
        "asset_id": str(asset.get("asset_id") or "").strip(),
        "instrument_id": str(asset.get("instrument_id") or "").strip(),
        "fiscal_year": int(asset.get("fiscal_year") or 0),
        "source": str(asset.get("source") or "").strip().lower(),
        "source_announcement_id": str(
            asset.get("source_announcement_id") or ""
        ).strip(),
        "attachment_id": str(asset.get("attachment_id") or "").strip(),
        "observation_version": str(
            asset.get("observation_version") or ""
        ).strip(),
        "content_hash": str(asset.get("content_hash") or "").strip().lower(),
        "report_period": str(asset.get("report_period") or "").strip(),
    }
    missing = sorted(
        key
        for key in (
            "asset_id",
            "instrument_id",
            "source",
            "source_announcement_id",
            "attachment_id",
            "observation_version",
            "content_hash",
            "report_period",
        )
        if not binding[key]
    )
    if missing:
        raise ValueError(
            "bound broker shared annual-report asset is incomplete: "
            + ",".join(missing)
        )
    if len(binding["content_hash"]) != 64 or any(
        character not in "0123456789abcdef"
        for character in binding["content_hash"]
    ):
        raise ValueError("bound broker shared annual-report content_hash is invalid")
    if (
        binding["fiscal_year"] < 1990
        or len(binding["report_period"]) < 4
        or int(binding["report_period"][:4]) != binding["fiscal_year"]
    ):
        raise ValueError("bound broker shared annual-report fiscal year is invalid")
    return binding


def _build_shared_annual_report_access(research_config: ResearchConfig) -> Any | None:
    """Bind broker annual-report reads to the independent asset access facade."""

    storage_config = getattr(research_config, "storage", None)
    db_path = getattr(storage_config, "db_path", None)
    if not db_path:
        return None
    from research.announcement_assets import (
        AnnouncementAssetAccess,
        AnnouncementAssetConfig,
        AnnouncementAssetRepository,
        AnnouncementAssetService,
    )

    asset_config = AnnouncementAssetConfig.from_research_config(
        research_config,
        project_root=Path.cwd(),
    )
    path = Path(str(db_path))
    if not path.is_absolute():
        path = Path.cwd() / path
    repository = AnnouncementAssetRepository(path)
    acquisition_config = load_announcement_acquisition_config(research_config)
    retriever = AnnouncementAttachmentRetriever.from_provider_configs(
        acquisition_config.provider_configs
    )
    service = AnnouncementAssetService(
        repository=repository,
        config=asset_config,
        attachment_retriever=retriever,
    )
    return AnnouncementAssetAccess(
        repository=repository,
        config=asset_config,
        service=service,
    )


def _announcement_id(record: AnnouncementRecord) -> str:
    return record.source_announcement_id


def _announcement_published_at(record: AnnouncementRecord) -> Optional[str]:
    return record.published_at


def _announcement_source(record: AnnouncementRecord) -> str:
    return record.source


def _announcement_attachment(record: AnnouncementRecord) -> Optional[AnnouncementAttachment]:
    attachments = record.attachments
    if attachments:
        return next(
            (
                item
                for item in attachments
                if "pdf" in str(item.media_type or item.file_extension or "").lower()
            ),
            attachments[0],
        )
    return None


def _announcement_attachment_type(record: AnnouncementRecord) -> Optional[str]:
    attachment = _announcement_attachment(record)
    if attachment is None:
        return None
    return attachment.media_type or attachment.file_extension


def _announcement_source_url(record: AnnouncementRecord) -> Optional[str]:
    attachment = _announcement_attachment(record)
    if attachment is None:
        return None
    return attachment.resolved_url or attachment.source_url


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


def infer_broker_annual_report_period(record: AnnouncementRecord) -> str:
    """Infer report period from a formal annual/semiannual report title."""
    title = _normalize_announcement_title(record.title)
    year_match = re.search(r"(20\d{2}|19\d{2})", title)
    if year_match:
        year = year_match.group(1)
        if "半年度报告" in title:
            return f"{year}-06-30"
        return f"{year}-12-31"
    published_at = _announcement_published_at(record)
    if published_at and re.match(r"^\d{4}", published_at):
        year = int(published_at[:4]) - 1
        return f"{year}-12-31"
    return ""


def infer_broker_risk_control_report_period(record: AnnouncementRecord) -> str:
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
    published_at = _announcement_published_at(record)
    if published_at and re.match(r"^\d{4}", published_at):
        year = int(published_at[:4]) - 1
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
        source_asset_lineage: Mapping[str, Any] | None = None,
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
                    source_asset_lineage=source_asset_lineage,
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
                        source_asset_lineage=source_asset_lineage,
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
            "source_asset_lineage": (
                None
                if source_asset_lineage is None
                else dict(source_asset_lineage)
            ),
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
        source_asset_lineage: Mapping[str, Any] | None,
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
                "annual_report_asset_id": (
                    None
                    if source_asset_lineage is None
                    else source_asset_lineage.get("asset_id")
                ),
                "annual_report_observation_version": (
                    None
                    if source_asset_lineage is None
                    else source_asset_lineage.get("observation_version")
                ),
                "annual_report_content_hash": (
                    None
                    if source_asset_lineage is None
                    else source_asset_lineage.get("content_hash")
                ),
                "annual_report_effective_decision_state": (
                    None
                    if source_asset_lineage is None
                    else source_asset_lineage.get("effective_decision_state")
                ),
                "source_asset_lineage": (
                    None
                    if source_asset_lineage is None
                    else dict(source_asset_lineage)
                ),
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
                "annual_report_asset_id": (
                    None
                    if source_asset_lineage is None
                    else source_asset_lineage.get("asset_id")
                ),
                "annual_report_observation_version": (
                    None
                    if source_asset_lineage is None
                    else source_asset_lineage.get("observation_version")
                ),
                "annual_report_content_hash": (
                    None
                    if source_asset_lineage is None
                    else source_asset_lineage.get("content_hash")
                ),
                "annual_report_effective_decision_state": (
                    None
                    if source_asset_lineage is None
                    else source_asset_lineage.get("effective_decision_state")
                ),
                "source_asset_lineage": (
                    None
                    if source_asset_lineage is None
                    else dict(source_asset_lineage)
                ),
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


PayloadFetcher = Callable[[AnnouncementRecord], Optional[bytes | str]]


class BrokerRiskControlReportSyncService:
    """Backfill and incremental sync for broker risk-control report facts."""

    def __init__(
        self,
        *,
        storage: Any,
        announcement_service: Optional[AnnouncementAcquisitionService] = None,
        attachment_retriever: Optional[AnnouncementAttachmentRetriever] = None,
        research_config: Optional[ResearchConfig] = None,
        parser: Optional[BrokerRiskControlPdfFactParser] = None,
        payload_fetcher: Optional[PayloadFetcher] = None,
        title_patterns: Optional[Sequence[str]] = None,
        source_profile: str = BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE,
        allow_non_broker_validation_override: bool = False,
        archive_root: Optional[str | Path] = None,
        force_reparse_existing: bool = False,
        replace_existing_facts: bool = False,
        shared_asset_access: Any | None = None,
        shared_asset_service: Any | None = None,
        shared_annual_report_enabled: bool | None = None,
        legacy_semiannual_enabled: bool | None = None,
        annual_report_asset_mode: str | None = None,
    ) -> None:
        self.storage = storage
        self.research_config = research_config or config_manager.get_research_config()
        explicit_shared_dependency = (
            shared_asset_access is not None or shared_asset_service is not None
        )
        if shared_asset_access is not None and shared_asset_service is not None:
            raise ValueError(
                "provide shared_asset_access or shared_asset_service, not both"
            )
        modules = getattr(self.research_config, "modules", {}) or {}
        financial_cfg = (
            modules.get("financial_statements", {})
            if isinstance(modules, Mapping)
            else {}
        )
        broker_cfg = (
            modules.get(
                "broker_risk_control_reports",
                financial_cfg.get("broker_risk_control_reports", {}),
            )
            if isinstance(modules, Mapping)
            else {}
        )
        dependency_cfg = (
            broker_cfg.get("annual_report_asset_dependency", {})
            if isinstance(broker_cfg, Mapping)
            else {}
        )
        if not isinstance(dependency_cfg, Mapping):
            raise ValueError(
                "broker_risk_control_reports.annual_report_asset_dependency "
                "must be a mapping"
            )
        explicit_mode = annual_report_asset_mode is not None
        dependency_mode = str(
            annual_report_asset_mode
            or (
                "shared_only"
                if explicit_shared_dependency
                else dependency_cfg.get(
                    "mode",
                    "shared_only" if dependency_cfg.get("enabled", False) else "legacy",
                )
            )
        ).strip().lower()
        if dependency_mode not in {"legacy", "dual_read", "shared_only"}:
            raise ValueError("invalid broker annual-report asset mode")
        dependency_enabled = bool(
            shared_annual_report_enabled
            if shared_annual_report_enabled is not None
            else dependency_mode in {"dual_read", "shared_only"}
            if explicit_mode or explicit_shared_dependency
            else dependency_cfg.get("enabled", False)
        )
        if (
            not explicit_shared_dependency
            and dependency_mode == "shared_only"
            and (
                not str(
                    dependency_cfg.get("reconciliation_evidence_id") or ""
                ).strip()
                or dependency_cfg.get("legacy_writer_disabled") is not True
            )
        ):
            raise ValueError(
                "broker shared-only cutover requires reconciliation evidence "
                "and legacy writer disablement"
            )
        if dependency_enabled != (dependency_mode in {"dual_read", "shared_only"}):
            raise ValueError("broker annual-report mode conflicts with enabled flag")
        legacy_annual_fallback = bool(
            dependency_mode in {"legacy", "dual_read"}
            if explicit_mode or explicit_shared_dependency
            else dependency_cfg.get(
                "legacy_fallback_enabled", dependency_mode != "shared_only"
            )
        )
        if legacy_annual_fallback != (dependency_mode in {"legacy", "dual_read"}):
            raise ValueError(
                "broker annual-report mode conflicts with legacy fallback"
            )
        if shared_asset_access is None and shared_asset_service is not None:
            from research.announcement_assets import AnnouncementAssetAccess

            shared_asset_access = AnnouncementAssetAccess(
                repository=shared_asset_service.repository,
                config=shared_asset_service.config,
                service=shared_asset_service,
            )
        if shared_asset_access is None and dependency_enabled:
            shared_asset_access = _build_shared_annual_report_access(
                self.research_config
            )
        self.shared_asset_access = shared_asset_access
        self.shared_annual_report_enabled = bool(dependency_enabled)
        self.annual_report_asset_mode = dependency_mode
        self.legacy_annual_report_fallback_enabled = legacy_annual_fallback
        self.legacy_semiannual_enabled = (
            bool(dependency_cfg.get("legacy_semiannual_enabled", True))
            if legacy_semiannual_enabled is None
            else bool(legacy_semiannual_enabled)
        )
        if self.shared_annual_report_enabled and self.shared_asset_access is None:
            raise ValueError(
                "shared annual-report dependency requires shared asset access"
            )
        acquisition_config = load_announcement_acquisition_config(self.research_config)
        self.announcement_service = announcement_service or AnnouncementAcquisitionService(
            registry=OfficialAnnouncementProviderRegistry(
                research_config=self.research_config
            ),
            config=acquisition_config,
        )
        self.attachment_retriever = attachment_retriever or (
            None
            if payload_fetcher is not None
            else AnnouncementAttachmentRetriever.from_provider_configs(
                acquisition_config.provider_configs
            )
        )
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
        announcement_records: Optional[Iterable[AnnouncementRecord]] = None,
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
        if (
            self.source_profile
            == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
            and self.annual_report_asset_mode == "shared_only"
        ):
            LOGGER.info(
                "broker formal annual-report incremental scan skipped: "
                "shared asset event is required"
            )
            return BrokerRiskControlSyncResult(
                status="success",
                mode="shared_asset_event_required",
                target_instruments=len(instruments),
            ).to_dict()
        purpose_key = self.source_profile
        exchange = self._exchange_from_market_column(market, column)
        scope = AnnouncementScope(
            exchange=exchange,
            market=market,
            keyword=(
                "年度报告"
                if self.source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
                else "风险控制指标"
            ),
            start_date=start_date,
            end_date=end_date,
            page_size=page_size,
            max_pages=max_pages,
        )
        state = self.storage.get_announcement_scan_state(
            purpose_key=purpose_key,
            source="cninfo",
            scope_key=scope.scope_key,
        )
        if state and state.get("committed_cursor"):
            cursor = state["committed_cursor"]
            scope = AnnouncementScope(
                **{
                    **scope.__dict__,
                    "cursor": ProviderCursor(
                        kind=str(cursor["kind"]),
                        value=str(cursor["value"]),
                    ),
                }
            )
        route_result = self.announcement_service.acquire(
            AnnouncementQuery(purpose_key=purpose_key, scope=scope),
            selectors=[self._record_filter],
        )
        scan_result = route_result.scan_result
        if scan_result is None:
            raise RuntimeError("announcement route returned no scan result")
        result = BrokerRiskControlSyncResult(
            status="success",
            mode="incremental_update",
            target_instruments=len(instruments),
            announcements_scanned=scan_result.announcements_seen,
            matching_announcements=0,
            filtered_announcements=(
                scan_result.announcements_seen - len(scan_result.selected_records)
            ),
            errors=list(scan_result.errors),
        )
        instrument_by_symbol = {
            str(item.get("symbol") or "").strip(): item
            for item in instruments
            if item.get("symbol")
        }
        selected_records: List[tuple[AnnouncementRecord, Optional[Dict[str, Any]]]] = []
        for record in scan_result.selected_records:
            instrument = self._resolve_record_instrument(record, instrument_by_symbol)
            selected_records.append((record, instrument))
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
            self.storage.upsert_announcement_scan_state(
                scan_result=scan_result,
                selected_announcements=len(scan_result.selected_records),
                attempts=[asdict(item) for item in route_result.attempts],
                metadata={
                    "source_profile": self.source_profile,
                    "retryable_pending_reports": result.retryable_pending_reports,
                },
            )
            for record, instrument in selected_records:
                self.storage.store_announcement_audit(
                    purpose_key=purpose_key,
                    record=record,
                    instrument_id=(
                        None
                        if instrument is None
                        else str(instrument.get("instrument_id") or "")
                    ),
                    symbol=(
                        None
                        if instrument is None
                        else str(instrument.get("symbol") or "")
                    ),
                    ingestion_run_id=ingestion_run_id,
                )
        return result.to_dict()

    def process_shared_asset_event(
        self,
        event: Mapping[str, Any],
        *,
        instrument: Mapping[str, Any],
        ingestion_run_id: int | None = None,
        tier: str = "hot",
        dry_run: bool = False,
        bound_asset: Mapping[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """Process one qualifying shared annual-report event through normal parsing."""

        if not self.shared_annual_report_enabled or self.shared_asset_access is None:
            raise RuntimeError("shared broker annual-report dependency is disabled")
        event_type = str(event.get("event_type") or "").strip().lower()
        if event_type not in {"added", "replaced", "repaired"}:
            return BrokerRiskControlSyncResult(
                status="success",
                mode="shared_asset_event_ignored",
                target_instruments=1,
            ).to_dict()
        instrument_id = str(instrument.get("instrument_id") or "").strip()
        fiscal_year = int(event.get("fiscal_year") or 0)
        if not instrument_id or fiscal_year < 1990:
            raise ValueError("shared broker asset event scope is incomplete")
        asset = (
            _normalize_bound_shared_annual_report_asset(bound_asset)
            if bound_asset is not None
            else self.shared_asset_access.get_effective_asset(
                instrument_id,
                fiscal_year=fiscal_year,
            )
        )
        if (
            not asset
            or (
                bound_asset is None
                and asset.get("availability") != "local_valid"
            )
            or str(asset.get("asset_id") or "")
            != str(event.get("asset_id") or "")
            or str(asset.get("instrument_id") or "") != instrument_id
            or int(asset.get("fiscal_year") or 0) != fiscal_year
        ):
            return BrokerRiskControlSyncResult(
                status="partial",
                mode="shared_asset_event_stale",
                target_instruments=1,
                retryable_pending_reports=1,
                errors=["shared asset event does not match the current local asset"],
            ).to_dict()
        source = str(asset.get("source") or "").strip().lower()
        source_announcement_id = str(
            asset.get("source_announcement_id") or ""
        ).strip()
        attachment_id = str(asset.get("attachment_id") or asset["asset_id"])
        record = AnnouncementRecord(
            source=source,
            source_announcement_id=source_announcement_id,
            announcement_key=build_announcement_key(
                source, source_announcement_id
            ),
            title=(
                f"{fiscal_year}年年度报告"
                + ("（修订版）" if asset.get("is_correction") else "")
            ),
            published_at=asset.get("published_at"),
            exchange=str(instrument.get("exchange") or "").upper() or None,
            market=str(instrument.get("exchange") or "").upper() or None,
            symbols=(str(instrument.get("symbol") or "").strip(),),
            attachments=(
                AnnouncementAttachment(
                    source_url=f"shared-asset://{asset['asset_id']}",
                    attachment_id=attachment_id,
                    media_type="application/pdf",
                    file_extension="pdf",
                    raw_metadata={
                        "shared_asset_id": asset["asset_id"],
                        "observation_version": asset.get("observation_version"),
                        "content_hash": asset.get("content_hash"),
                    },
                ),
            ),
            raw_payload={
                "shared_asset_event": dict(event),
                "shared_asset_projection": dict(asset),
                "shared_asset_binding_mode": (
                    "exact_observation"
                    if bound_asset is not None
                    else "effective_projection"
                ),
            },
            selection_reasons=("shared_annual_report_asset_event",),
        )
        return self.backfill(
            instruments=[dict(instrument)],
            report_periods=[str(asset.get("report_period") or f"{fiscal_year}-12-31")],
            announcement_records=[record],
            ingestion_run_id=ingestion_run_id,
            tier=tier,
            dry_run=dry_run,
        )

    def _shared_annual_report_asset(
        self,
        record: AnnouncementRecord,
        instrument: Mapping[str, Any],
    ) -> tuple[bytes, dict[str, Any], dict[str, Any]] | None:
        """Read a formal annual report from shared custody, if bound."""

        if (
            not self.shared_annual_report_enabled
            or self.shared_asset_access is None
            or self.source_profile
            != BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
            or "半年度报告" in _normalize_announcement_title(record.title)
        ):
            return None
        instrument_id = str(instrument.get("instrument_id") or "").strip()
        if not instrument_id:
            raise ValueError("shared broker annual-report identity is incomplete")
        from research.announcement_assets import EnsureRequest

        bound_asset_raw = dict(
            (record.raw_payload or {}).get("shared_asset_projection") or {}
        )
        if (
            bound_asset_raw
            and (record.raw_payload or {}).get("shared_asset_binding_mode")
            == "exact_observation"
        ):
            asset = _normalize_bound_shared_annual_report_asset(bound_asset_raw)
            if (
                asset["instrument_id"] != instrument_id
                or asset["source"] != _announcement_source(record)
                or asset["source_announcement_id"] != _announcement_id(record)
            ):
                raise RuntimeError(
                    "bound broker shared asset does not match parser record identity"
                )
            content = self.shared_asset_access.exact_observation_handle(
                EnsureRequest(
                    instrument_id=instrument_id,
                    source=asset["source"],
                    source_announcement_id=asset["source_announcement_id"],
                    attachment_id=asset["attachment_id"],
                    observation_version=asset["observation_version"],
                    expected_content_hash=asset["content_hash"],
                    allow_network=False,
                    principal="broker-risk-control",
                ),
                authorized=True,
            )
            handle = content["file_handle"]
            try:
                payload = handle.read()
            finally:
                handle.close()
            if (
                str(content.get("observation_version") or "")
                != asset["observation_version"]
                or str(content.get("content_hash") or "").lower()
                != asset["content_hash"]
                or hashlib.sha256(payload).hexdigest() != asset["content_hash"]
                or len(payload) != int(content["content_length"])
            ):
                raise RuntimeError(
                    "bound broker shared asset observation integrity mismatch"
                )
            return payload, asset, dict(content)

        access_config = getattr(self.shared_asset_access, "config", None)
        wait_seconds = float(
            getattr(access_config, "wait_seconds_maximum", 30.0)
        )
        ensured = self.shared_asset_access.ensure(
            EnsureRequest(
                instrument_id=instrument_id,
                source=_announcement_source(record),
                source_announcement_id=_announcement_id(record),
                allow_network=True,
                wait_seconds=wait_seconds,
                consumer="broker_risk_control",
                principal="broker-risk-control",
            )
        )
        asset = ensured.get("asset")
        if not asset or ensured.get("availability") != "local_valid":
            return None
        if (
            str(asset.get("source") or "").strip().lower()
            != _announcement_source(record)
            or str(asset.get("source_announcement_id") or "")
            != _announcement_id(record)
        ):
            raise RuntimeError(
                "shared broker annual-report selector resolved a different legal filing"
            )
        content = self.shared_asset_access.content_handle(str(asset["asset_id"]))
        handle = content["file_handle"]
        try:
            payload = handle.read()
        finally:
            handle.close()
        if hashlib.sha256(payload).hexdigest() != str(content["content_hash"]).lower():
            raise RuntimeError("shared broker annual-report hash validation failed")
        if len(payload) != int(content["content_length"]):
            raise RuntimeError("shared broker annual-report length validation failed")
        return payload, dict(asset), dict(content)

    def _formal_annual_uses_shared_assets(self, record: AnnouncementRecord) -> bool:
        return (
            self.shared_annual_report_enabled
            and self.source_profile
            == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
            and "半年度报告" not in _normalize_announcement_title(record.title)
        )

    def _record_filter(self, record: AnnouncementRecord) -> List[str]:
        if not self._record_matches(record):
            return []
        if self.source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE:
            return ["formal_annual_or_semiannual_report"]
        return ["broker_risk_control_title"]

    def _record_matches(self, record: AnnouncementRecord) -> bool:
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
        record: AnnouncementRecord,
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
            _announcement_id(record),
            record.title,
            dry_run,
        )
        try:
            shared_asset = self._shared_annual_report_asset(record, instrument)
            if shared_asset is not None:
                payload, shared_asset_lineage, shared_content = shared_asset
            else:
                if (
                    self._formal_annual_uses_shared_assets(record)
                    and not self.legacy_annual_report_fallback_enabled
                ):
                    raise RuntimeError(
                        "shared broker annual-report asset is not locally ready"
                    )
                if (
                    self.source_profile
                    == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE
                    and "半年度报告" in _normalize_announcement_title(record.title)
                    and not self.legacy_semiannual_enabled
                ):
                    raise RuntimeError("legacy broker semiannual acquisition is disabled")
                existing_manifest = self._existing_direct_filing_manifest(
                    record,
                    instrument,
                    report_period,
                )
                if existing_manifest is not None:
                    result.unchanged_reports += 1
                    LOGGER.info(
                        "broker risk-control report unchanged before download: "
                        "instrument_id=%s symbol=%s report_period=%s "
                        "announcement_id=%s source_file_id=%s",
                        instrument.get("instrument_id"),
                        instrument.get("symbol"),
                        report_period,
                        _announcement_id(record),
                        existing_manifest.get("source_file_id"),
                    )
                    return
                payload = self.payload_fetcher(record)
                shared_asset_lineage = None
                shared_content = None
        except Exception as exc:
            result.retryable_pending_reports += 1
            result.errors.append(str(exc))
            LOGGER.warning(
                "broker risk-control report payload failed: instrument_id=%s symbol=%s report_period=%s announcement_id=%s error=%s",
                instrument.get("instrument_id"),
                instrument.get("symbol"),
                report_period,
                _announcement_id(record),
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
                _announcement_id(record),
            )
            return
        hash_payload = payload.encode("utf-8") if isinstance(payload, str) else payload
        content_hash = hashlib.sha256(hash_payload).hexdigest()
        LOGGER.info(
            "broker risk-control report payload loaded: instrument_id=%s symbol=%s report_period=%s announcement_id=%s bytes=%s",
            instrument.get("instrument_id"),
            instrument.get("symbol"),
            report_period,
            _announcement_id(record),
            len(hash_payload),
        )
        archive_path = (
            None
            if dry_run
            else (
                None
                if shared_asset_lineage is not None
                else self._archive_payload(
                    record, instrument, report_period, hash_payload
                )
            )
        )
        manifest = self._build_manifest(
            record,
            instrument,
            report_period,
            payload,
            content_hash,
            archive_path=archive_path,
            shared_asset_lineage=shared_asset_lineage,
            shared_content=shared_content,
        )
        if not self.force_reparse_existing and self._unchanged_manifest_exists(manifest, content_hash):
            result.unchanged_reports += 1
            LOGGER.info(
                "broker risk-control report unchanged: instrument_id=%s symbol=%s report_period=%s announcement_id=%s content_hash=%s",
                instrument.get("instrument_id"),
                instrument.get("symbol"),
                report_period,
                _announcement_id(record),
                content_hash[:12],
            )
            return
        source_file_id = (
            f"dryrun:{_announcement_id(record)}:{content_hash[:12]}"
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
                source=_announcement_source(record),
                source_mode=(
                    "shared_announcement_asset"
                    if shared_asset_lineage is not None
                    else "direct"
                ),
                source_profile=self.source_profile,
                artifact_kind=self._artifact_kind(),
                licensed_broker_name=licensed_broker_name,
                listed_broker_scope=listed_scope,
                source_asset_lineage=shared_asset_lineage,
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
                _announcement_id(record),
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
            _announcement_id(record),
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
                    _announcement_id(record),
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
                _announcement_id(record),
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
            _announcement_id(record),
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
                _announcement_id(record),
            )

    def _parse_failure_detail(
        self,
        record: AnnouncementRecord,
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
            "announcement_id": _announcement_id(record),
            "title": record.title,
            "report_period": report_period,
            "announcement_time": _announcement_published_at(record),
            "adjunct_url": _announcement_source_url(record),
            "diagnostics": diagnostics or {},
        }

    def _parsed_report_summary(
        self,
        record: AnnouncementRecord,
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
            "announcement_id": _announcement_id(record),
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
        record: AnnouncementRecord,
        instrument: Dict[str, Any],
        report_period: str,
        payload: bytes | str,
        content_hash: str,
        archive_path: Optional[str] = None,
        shared_asset_lineage: Mapping[str, Any] | None = None,
        shared_content: Mapping[str, Any] | None = None,
    ) -> FinancialSourceFileManifest:
        classification = self._classify_record(record)
        scope_resolution = resolve_listed_broker_dealer_scope(instrument)
        return FinancialSourceFileManifest(
            source=_announcement_source(record),
            source_mode=(
                "shared_announcement_asset"
                if shared_asset_lineage is not None
                else "direct"
            ),
            instrument_id=str(instrument.get("instrument_id") or ""),
            symbol=str(instrument.get("symbol") or ""),
            exchange=str(instrument.get("exchange") or ""),
            report_period=report_period,
            report_type=self._record_report_type(record),
            filing_id=_announcement_id(record),
            source_url=_announcement_source_url(record),
            archive_path=archive_path,
            content_hash=content_hash,
            content_length=len(payload),
            published_at=_announcement_published_at(record),
            downloaded_at=get_shanghai_time().isoformat(),
            parser_version=self.parser.parser_version,
            status="downloaded",
            metadata_json={
                **classification,
                "announcement_title": record.title,
                "source_profile": self.source_profile,
                "listed_broker_dealer_scope": scope_resolution.to_dict(),
                "announcement_record": {
                    "source": _announcement_source(record),
                    "market": record.market,
                    "symbols": record.symbols,
                    "selection_reasons": record.selection_reasons,
                },
                "shared_annual_report_asset": (
                    None
                    if shared_asset_lineage is None
                    else {
                        "asset_id": shared_asset_lineage.get("asset_id"),
                        "source": shared_asset_lineage.get("source"),
                        "source_announcement_id": shared_asset_lineage.get(
                            "source_announcement_id"
                        ),
                        "attachment_id": shared_asset_lineage.get("attachment_id"),
                        "observation_version": shared_asset_lineage.get(
                            "observation_version"
                        ),
                        "content_hash": shared_asset_lineage.get("content_hash"),
                        "variant": shared_asset_lineage.get("variant"),
                        "effective_decision_state": shared_asset_lineage.get(
                            "effective_decision_state"
                        ),
                        "content_length": (
                            None
                            if shared_content is None
                            else shared_content.get("content_length")
                        ),
                    }
                ),
            },
        )

    def _archive_payload(
        self,
        record: AnnouncementRecord,
        instrument: Dict[str, Any],
        report_period: str,
        payload: bytes,
    ) -> Optional[str]:
        if self.archive_root is None:
            return None
        exchange = str(instrument.get("exchange") or "UNKNOWN")
        symbol = str(instrument.get("symbol") or (record.symbols[0] if record.symbols else "UNKNOWN"))
        filename = f"{symbol}_{report_period}_{_announcement_id(record)}.pdf"
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
        matching_rows = [
            row
            for row in rows
            if row.get("content_hash") == content_hash
            and row.get("parser_version") == manifest.parser_version
        ]
        if manifest.source_mode != "shared_announcement_asset":
            return any(
                row.get("status") in {"downloaded", "parsed"}
                for row in matching_rows
            )

        expected_lineage = (
            manifest.metadata_json.get("shared_annual_report_asset") or {}
        )
        expected_identity = tuple(
            str(expected_lineage.get(field) or "").strip().lower()
            for field in ("asset_id", "observation_version", "content_hash")
        )
        if not all(expected_identity):
            return False
        for row in matching_rows:
            if (
                row.get("source_mode") != "shared_announcement_asset"
                or row.get("status") != "parsed"
            ):
                continue
            metadata = row.get("metadata") or {}
            if not isinstance(metadata, Mapping):
                continue
            shared_lineage = metadata.get("shared_annual_report_asset") or {}
            if not isinstance(shared_lineage, Mapping):
                continue
            row_identity = tuple(
                str(shared_lineage.get(field) or "").strip().lower()
                for field in ("asset_id", "observation_version", "content_hash")
            )
            if row_identity == expected_identity:
                return True
        return False

    def _existing_direct_filing_manifest(
        self,
        record: AnnouncementRecord,
        instrument: Mapping[str, Any],
        report_period: str,
    ) -> Mapping[str, Any] | None:
        """Return an already parsed direct filing that needs no new download."""
        if self.force_reparse_existing:
            return None
        filing_id = _announcement_id(record)
        instrument_id = str(instrument.get("instrument_id") or "").strip()
        source = _announcement_source(record)
        report_type = self._record_report_type(record)
        if not filing_id or not instrument_id or not report_period or not source:
            return None
        rows = self.storage.get_financial_source_file_manifests(
            instrument_id=instrument_id,
            report_period=report_period,
            source=source,
            report_types=(report_type,),
            filing_id=filing_id,
            statuses=("parsed",),
        )
        source_url = str(_announcement_source_url(record) or "").strip()
        for row in rows:
            if (
                str(row.get("instrument_id") or "").strip() != instrument_id
                or str(row.get("report_period") or "").strip() != report_period
                or str(row.get("report_type") or "").strip() != report_type
                or str(row.get("source") or "").strip().lower() != source
                or str(row.get("filing_id") or "").strip() != filing_id
                or row.get("source_mode") != "direct"
                or row.get("status") != "parsed"
                or row.get("parser_version") != self.parser.parser_version
            ):
                continue
            manifest_url = str(row.get("source_url") or "").strip()
            if source_url and manifest_url and source_url != manifest_url:
                continue
            if self.archive_root is not None and not self._manifest_archive_is_valid(row):
                continue
            return row
        return None

    @staticmethod
    def _manifest_archive_is_valid(manifest: Mapping[str, Any]) -> bool:
        archive_path = str(manifest.get("archive_path") or "").strip()
        expected_hash = str(manifest.get("content_hash") or "").strip().lower()
        if not archive_path or not expected_hash:
            return False
        path = Path(archive_path)
        if not path.is_file():
            return False
        try:
            expected_length = manifest.get("content_length")
            if expected_length is not None and path.stat().st_size != int(expected_length):
                return False
            digest = hashlib.sha256()
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            return digest.hexdigest() == expected_hash
        except (OSError, TypeError, ValueError):
            return False

    def _record_period(self, record: AnnouncementRecord) -> str:
        if self.source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE:
            return infer_broker_annual_report_period(record)
        return infer_broker_risk_control_report_period(record)

    def _record_report_type(self, record: AnnouncementRecord) -> str:
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

    def _classify_record(self, record: AnnouncementRecord) -> Dict[str, Any]:
        if self.source_profile == BROKER_ANNUAL_REPORT_RISK_CONTROL_SOURCE_PROFILE:
            return classify_broker_annual_report_risk_control_artifact(
                record.title,
                adjunct_type=_announcement_attachment_type(record),
            ) or {}
        payload = classify_broker_risk_control_artifact(
            record.title,
            adjunct_type=_announcement_attachment_type(record),
        ) or {}
        if payload:
            payload["source_priority"] = "supplementary"
        return payload

    def _resolve_record_instrument(
        self,
        record: AnnouncementRecord,
        instrument_by_symbol: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        for symbol in record.symbols:
            clean = str(symbol).strip()
            if clean in instrument_by_symbol:
                return instrument_by_symbol[clean]
        return None

    @staticmethod
    def _exchange_from_market_column(market: str, column: str) -> str:
        normalized = str(column or "").strip().lower()
        if normalized in {"sse", "sh"}:
            return "SSE"
        if normalized in {"szse", "sz"}:
            return "SZSE"
        if normalized in {"bse", "bj"}:
            return "BSE"
        market_text = str(market or "").strip().upper()
        return {
            "沪市": "SSE",
            "深市": "SZSE",
            "北交所": "BSE",
        }.get(market_text, market_text)

    def _download_payload(self, record: AnnouncementRecord) -> Optional[bytes]:
        attachment = _announcement_attachment(record)
        if attachment is None:
            return None
        if self.attachment_retriever is None:
            raise RuntimeError("announcement attachment retriever is not configured")
        result = self.attachment_retriever.retrieve(
            _announcement_source(record),
            attachment,
            require_pdf=True,
        )
        if result.status != "success":
            raise RuntimeError(
                "broker risk-control attachment retrieval failed: "
                + "; ".join(result.errors)
            )
        return result.content
