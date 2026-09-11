"""Automatic six-chapter Evidence planning for the frozen shadow cohort."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping, Sequence
from enum import Enum
from itertools import pairwise
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal

from pydantic import ConfigDict, Field, model_validator

from research.business_profile_disclosure_templates import (
    ResolvedDisclosureTemplate,
    load_disclosure_template_catalog,
)
from research.business_profile_pdf_artifacts import (
    BusinessProfilePdfArtifactExtractor,
    BusinessProfilePdfArtifactStore,
    ensure_archived_pdf_page_artifact,
)
from research.business_profile_section_selection import (
    ANNUAL_REPORT_SEMANTIC_BUNDLE_FAMILY,
    BusinessProfileSectionSelector,
)
from research.business_profile_semantic_runtime import (
    _recover_business_profile_document,
)

from .models import PRODUCTION_AUTHORIZATION, ChapterTask
from .shadow_batch import (
    SHADOW_REPORT_COUNT,
    ShadowSampleManifest,
    _payload_hash,
    _StrictModel,
    _utc_now,
)
from .stage5 import (
    EvidenceReportPlan,
    EvidenceScopePlan,
    EvidenceTaskPlan,
    PreparedRequestScope,
    Stage5EvidencePreparer,
)
from .stage5_service import stage5_field_ids

SHADOW_EVIDENCE_PLAN_SCHEMA = "company_profile_shadow_evidence_plan.v1"
SHADOW_EVIDENCE_PLAN_VERSION = "manufacturing_materials_shadow.2026-09-10.5"
SHADOW_OPERATING_OWNERSHIP_PLAN_VERSION = (
    "manufacturing_materials_shadow.2026-09-11.6"
)
SHADOW_PREPARATION_AUDIT_SCHEMA = "company_profile_shadow_preparation_audit.v1"
SHADOW_SCOPE_REFINEMENT_AUDIT_SCHEMA = (
    "company_profile_shadow_scope_refinement_audit.v1"
)
SHADOW_ROUTING_CONTINUATION_AUDIT_SCHEMA = (
    "company_profile_shadow_routing_continuation_audit.v1"
)
_BSE_INDUSTRY_DISCLOSURE_OPT_OUT_PATTERN = (
    r"第九节行业信息.{0,240}是否自愿披露.{0,20}[√☑]否"
)
_OPERATING_QUANTITY_LEGAL_EMPTY_PATTERN = (
    r"(?:公司实物销售收入是否大于劳务收入|产销量情况分析表)"
    r".{0,80}(?:[√☑](?:否|不适用)|不适用)"
)
_BUSINESS_OVERVIEW_CROSS_REFERENCE_PAGE_PATTERN = re.compile(
    r"(?:主营业务分析.{0,30}概述|概述)"
    r"(?:具体(?:内容|情况))?(?:参见|详见|见)(?:本报告|报告)?"
    r".{0,120}(?:主要业务|主营业务).{0,30}(?:相关)?(?:内容|情况)?"
)
_SUBSIDIARY_FINANCIAL_TABLE_PATTERN = re.compile(
    r"(?:主要控股参股公司分析|主要子公司及.{0,30}参股公司情况)"
    r".{0,500}公司名称.{0,80}公司类型.{0,80}主要业务"
    r".{0,160}(?:注册资本|总资产).{0,240}营业收入.{0,240}净利润"
)
_ISSUER_OVERVIEW_SUBSTANCE_PATTERN = re.compile(
    r"(?:公司|本公司).{0,40}(?:主营业务|主要业务)(?:主要)?"
    r"(?:包括|为|是).{1,120}(?:。|；|$)"
    r"|(?:公司|本公司).{0,40}(?:主要|专业|重点)?"
    r"(?:从事|经营|开展|提供|采用).{0,120}"
    r"(?:业务|产品|服务|生产|制造|加工|销售|研发|开发|经营模式)"
    r"|(?:主要业务|主营业务|经营模式)(?:主要)?(?:包括|为|是|采用)"
    r".{0,120}(?:业务|产品|服务|生产|制造|加工|销售|研发|开发)"
)
_BUSINESS_OVERVIEW_CHECKLIST_PATTERN = re.compile(
    r"公司是否需(?:通过互联网渠道)?开展业务.{0,80}(?:是|否|适用|不适用)"
)


def _business_overview_statistical_table_only(text: str) -> bool:
    compact = re.sub(r"\s+", "", text)
    without_checklist = _BUSINESS_OVERVIEW_CHECKLIST_PATTERN.sub("", compact)
    has_segment_table = (
        any(term in compact for term in ("分行业", "分产品", "分地区"))
        and "营业收入" in compact
        and any(term in compact for term in ("营业成本", "毛利率"))
    )
    return bool(
        has_segment_table
        and not _ISSUER_OVERVIEW_SUBSTANCE_PATTERN.search(without_checklist)
    )


def _business_overview_cross_reference_only(text: str) -> bool:
    compact = re.sub(r"\s+", "", text)
    without_checklist = _BUSINESS_OVERVIEW_CHECKLIST_PATTERN.sub("", compact)
    without_reference = _BUSINESS_OVERVIEW_CROSS_REFERENCE_PAGE_PATTERN.sub(
        "", without_checklist
    )
    return bool(
        _BUSINESS_OVERVIEW_CROSS_REFERENCE_PAGE_PATTERN.search(compact)
        and not _ISSUER_OVERVIEW_SUBSTANCE_PATTERN.search(without_reference)
    )


_MATERIAL_CONTINUATION_OWNER_PATTERN = re.compile(
    r"(?:公司|本公司).{0,120}(?:原料|原材料|耐火材料)"
    r".{0,40}(?:替代|再生|循环利用|回收|消耗|储备)"
    r"|(?:原料|原材料|耐火材料).{0,40}(?:替代|再生|循环利用|回收|消耗|储备)"
)
_MATERIAL_PAGE_BREAK_TAIL_PATTERN = re.compile(
    r"(?:原料|原材料|耐火材料).{0,60}(?:再生|循环|回收|替代|消耗|储备)[^。；！？]{0,30}$"
)
_PROHIBITED_KEYS = frozenset(
    {
        "activity_actor",
        "expected_value",
        "gold",
        "semantic",
        "source_actor",
        "source_verb",
        "subject_basis",
        "subject_scope",
    }
)
_CHAPTER_CONFIG: dict[
    ChapterTask,
    tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]],
] = {
    ChapterTask.EXTRACT_BUSINESS_OVERVIEW: (
        ("business_overview_source", "explicit_activity"),
        ("principal_business", "products_and_applications", "business_model"),
        (
            "公司从事的主要业务",
            "报告期内公司从事的主要业务",
            "主要业务",
            "主要产品",
            "经营模式",
        ),
    ),
    ChapterTask.EXTRACT_SEGMENT_FINANCIALS: (
        (
            "segment_dimension",
            "operating_revenue",
            "operating_cost",
            "gross_margin_reported",
        ),
        ("segment_information", "revenue_cost_analysis"),
        ("分部信息", "分部报告", "分行业", "分产品", "分地区", "主营业务分"),
    ),
    ChapterTask.EXTRACT_OPERATING_QUANTITIES: (
        (
            "production_capacity",
            "capacity_under_construction",
            "capacity_utilization",
            "production_volume",
            "sales_volume",
            "inventory_volume",
            "processing_volume",
        ),
        (
            "production_sales_inventory",
            "resources_and_reserves",
            "major_projects",
            "coal_operations",
            "coal_resources",
        ),
        (
            "产销量",
            "生产量",
            "销售量",
            "库存量",
            "公司实物销售收入是否大于劳务收入",
            "实际产量",
            "产能",
            "产能情况",
            "在建产能",
            "是否自愿披露",
        ),
    ),
    ChapterTask.EXTRACT_MATERIAL_INPUTS: (
        ("material_input",),
        (
            "procurement_and_costs",
            "cost_composition",
            "principal_business",
            "business_model",
        ),
        (
            "主要原材料",
            "原材料及能源",
            "原材料采购",
            "采购模式",
            "成本构成",
            "为原料",
            "原油采购",
            "铁矿石",
            "煤炭",
        ),
    ),
    ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION: (
        (
            "counterparty_relationship",
            "customer_concentration",
            "supplier_concentration",
        ),
        ("major_customers_suppliers", "orders"),
        (
            "前五名客户",
            "前五名供应商",
            "主要销售客户",
            "主要供应商",
            "客户集中度",
            "供应商集中度",
        ),
    ),
    ChapterTask.EXTRACT_BUSINESS_REGIME: (
        ("business_regime",),
        ("principal_business", "business_model", "major_projects"),
        (
            "业务、产品或服务发生重大变化",
            "合并报表范围的变化情况",
            "合并范围发生变化",
            "报告期主要子公司股权变动",
            "重大资产重组",
            "股权过户",
        ),
    ),
}
_CHAPTER_MAX_SCOPES = {
    ChapterTask.EXTRACT_BUSINESS_OVERVIEW: 1,
    ChapterTask.EXTRACT_SEGMENT_FINANCIALS: 2,
    ChapterTask.EXTRACT_OPERATING_QUANTITIES: 2,
    ChapterTask.EXTRACT_MATERIAL_INPUTS: 1,
    ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION: 2,
    ChapterTask.EXTRACT_BUSINESS_REGIME: 1,
}
_REQUIRED_CHAPTER_FIELDS: dict[ChapterTask, tuple[str, ...]] = {
    ChapterTask.EXTRACT_BUSINESS_OVERVIEW: ("business_overview_source",),
    ChapterTask.EXTRACT_SEGMENT_FINANCIALS: ("segment_dimension",),
    ChapterTask.EXTRACT_BUSINESS_REGIME: ("business_regime",),
}
_FIELD_TEXT_PATTERNS: dict[ChapterTask, dict[str, tuple[str, ...]]] = {
    ChapterTask.EXTRACT_BUSINESS_OVERVIEW: {
        "business_overview_source": (
            r"主营(?:业务|范围)",
            r"主要(?:业务|产品)",
            r"经营范围",
            r"业务概述",
            r"(?:专业|主要)从事",
        ),
        "explicit_activity": (
            r"(?:生产|制造|加工|销售|研发|开发|采购|开采|冶炼|提供.{0,8}服务)",
        ),
    },
    ChapterTask.EXTRACT_SEGMENT_FINANCIALS: {
        "segment_dimension": (
            r"分(?:行业|产品|地区|部|销售模式)",
            r"(?:业务|报告)分部",
            r"主营业务分",
        ),
        "operating_revenue": (r"(?:营业|主营业务|销售)收入",),
        "operating_cost": (r"(?:营业|主营业务|销售)成本",),
        "gross_margin_reported": (r"毛利率",),
    },
    ChapterTask.EXTRACT_OPERATING_QUANTITIES: {
        "production_capacity": (
            r"(?:设计|现有|核定|实际|总|年)产能",
            r"产能(?:规模|为|达到)",
            r"产能(?:与开工)?情况.{0,60}(?:[√☑]不适用|不适用)",
            _BSE_INDUSTRY_DISCLOSURE_OPT_OUT_PATTERN,
        ),
        "capacity_under_construction": (
            r"在建产能",
            r"在建(?:项目|工程).{0,40}(?:产能|生产线|装置|吨|台|套|GWh|MWh|㎡)",
            r"新增.{0,20}产能",
            r"建设.{0,20}产能",
            r"产能.{0,12}建设项目",
        ),
        "capacity_utilization": (r"产能利用率",),
        "production_volume": (r"(?:生产量|产量)",),
        "sales_volume": (r"(?:销售量|销量)",),
        "inventory_volume": (r"(?:库存量|期末库存)",),
        "processing_volume": (r"(?:加工量|处理量|吞吐量)",),
    },
    ChapterTask.EXTRACT_MATERIAL_INPUTS: {
        "material_input": (
            r"(?:原材料|原料|原燃料|燃料|能源|铁矿石|矿石|煤炭|焦炭|耐火材料|采购模式)",
        ),
    },
    ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION: {
        "counterparty_relationship": (r"(?:客户|供应商|经销商|关联方)",),
        "customer_concentration": (
            r"前五名客户",
            r"客户集中度",
            r"客户.{0,20}(?:销售额|营业收入).{0,20}(?:比例|占比)",
        ),
        "supplier_concentration": (
            r"前五名供应商",
            r"供应商集中度",
            r"供应商.{0,20}(?:采购额|采购总额).{0,20}(?:比例|占比)",
        ),
    },
    ChapterTask.EXTRACT_BUSINESS_REGIME: {
        "business_regime": (
            r"经营模式",
            r"(?:主营业务|业务|经营).{0,20}(?:重大变化|未发生重大变化|发生变化)",
            r"合并(?:报表)?范围.{0,20}(?:变化|变动)",
            r"(?:重大资产重组|资产重组|收购|股权过户|完成过户)",
        ),
    },
}
_FIELD_SECTION_KEYS: dict[ChapterTask, dict[str, tuple[str, ...]]] = {
    ChapterTask.EXTRACT_BUSINESS_OVERVIEW: {
        "business_overview_source": ("principal_business", "business_model"),
        "explicit_activity": ("principal_business", "business_model"),
    },
    ChapterTask.EXTRACT_SEGMENT_FINANCIALS: {
        "segment_dimension": ("segment_information",),
    },
    ChapterTask.EXTRACT_OPERATING_QUANTITIES: {
        "production_volume": ("production_sales_inventory", "coal_operations"),
        "sales_volume": ("production_sales_inventory", "coal_operations"),
        "inventory_volume": ("production_sales_inventory",),
        "capacity_under_construction": ("major_projects",),
    },
    ChapterTask.EXTRACT_MATERIAL_INPUTS: {
        "material_input": (
            "procurement_and_costs",
            "cost_composition",
            "principal_business",
            "business_model",
        ),
    },
    ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION: {
        "counterparty_relationship": ("major_customers_suppliers", "orders"),
    },
    ChapterTask.EXTRACT_BUSINESS_REGIME: {
        "business_regime": ("principal_business", "business_model", "major_projects"),
    },
}
_FIELD_REASON_TERMS: dict[ChapterTask, dict[str, tuple[str, ...]]] = {
    ChapterTask.EXTRACT_BUSINESS_OVERVIEW: {
        "business_overview_source": ("主营业务", "主要业务", "主要产品", "经营模式"),
        "explicit_activity": ("主营业务", "主要业务", "主要产品"),
    },
    ChapterTask.EXTRACT_SEGMENT_FINANCIALS: {
        "segment_dimension": ("分部信息", "分行业", "分产品"),
        "operating_revenue": ("营业收入",),
        "operating_cost": ("营业成本", "成本构成"),
        "gross_margin_reported": ("毛利率",),
    },
    ChapterTask.EXTRACT_OPERATING_QUANTITIES: {
        "production_capacity": ("产能",),
        "capacity_under_construction": ("在建",),
        "production_volume": (
            "产销量",
            "生产量",
            "实际产量",
            "公司实物销售收入是否大于劳务收入",
        ),
        "sales_volume": (
            "产销量",
            "销售量",
            "公司实物销售收入是否大于劳务收入",
        ),
        "inventory_volume": (
            "产销量",
            "库存量",
            "公司实物销售收入是否大于劳务收入",
        ),
        "processing_volume": ("加工量",),
    },
    ChapterTask.EXTRACT_MATERIAL_INPUTS: {
        "material_input": ("原材料", "能源", "采购", "成本构成", "铁矿石", "煤炭"),
    },
    ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION: {
        "counterparty_relationship": ("客户", "供应商"),
        "customer_concentration": ("前五名客户", "客户集中度"),
        "supplier_concentration": ("前五名供应商", "供应商集中度"),
    },
    ChapterTask.EXTRACT_BUSINESS_REGIME: {
        "business_regime": ("经营模式", "重大变化", "业务变化", "合并范围", "重组"),
    },
}


_FINANCIAL_NOTE_MARKERS = (
    r"(?:母公司|合并)?利润表",
    r"资产负债表",
    r"利润表及现金流量表相关科目",
    r"营业收入和营业成本",
    r"未分配利润",
)
_CHAPTER_OWNER_PATTERNS: dict[ChapterTask, tuple[str, ...]] = {
    ChapterTask.EXTRACT_BUSINESS_OVERVIEW: (
        r"(?:公司|本公司).{0,30}(?:主要从事|主营业务|主要业务|经营范围)",
        r"(?:主要业务|主营业务|经营模式).{0,30}(?:包括|为|是|主要采用)",
        r"(?:专业|主要)从事.{0,60}(?:生产|制造|加工|销售|研发|开发|服务)",
        (
            r"(?:公司|本公司).{0,80}(?:系统|产品|业务|服务|产业)"
            r".{0,80}(?:适用于|覆盖|包括|主要采用|提供|生产|制造|销售|研发)"
        ),
    ),
    ChapterTask.EXTRACT_SEGMENT_FINANCIALS: (
        r"分(?:行业|产品|地区|销售模式)",
        r"(?:业务|报告)分部",
        r"分部(?:收入|利润|资产|信息)",
        r"主营业务分",
    ),
    ChapterTask.EXTRACT_OPERATING_QUANTITIES: (
        r"(?:产销量|生产量|销售量|库存量|加工量|处理量|吞吐量)",
        r"公司实物销售收入是否大于劳务收入",
        _OPERATING_QUANTITY_LEGAL_EMPTY_PATTERN,
        r"实际产量",
        r"(?:设计|现有|核定|实际|总|年)产能",
        r"产能(?:规模|为|达到|利用率)",
        r"产能(?:与开工)?情况.{0,60}(?:[√☑]不适用|不适用)",
        _BSE_INDUSTRY_DISCLOSURE_OPT_OUT_PATTERN,
        r"在建(?:产能|项目|工程).{0,40}(?:产能|生产线|装置|吨|台|套|GWh|MWh|㎡)",
    ),
    ChapterTask.EXTRACT_MATERIAL_INPUTS: (
        r"主要原材料",
        r"原材料及能源",
        r"(?:原料|原材料)(?:的)?(?:名称|供应)",
        r"原材料(?:采购模式|采购情况)",
        r"(?:采购模式|采购情况)",
        r"(?:成本分析|营业成本构成|成本构成).{0,300}(?:原材料|原燃料|燃料及动力)",
        r"生产所需.{0,30}(?:原料|原材料|能源)",
        r"(?:公司|本公司).{0,100}(?:采购|购入).{0,80}(?:原料|原材料|精矿|矿石|煤炭|焦炭)",
        r"原材料价格波动风险",
        r"(?:原料|原材料)主要(?:为|包括)",
        r"以.{1,80}为原料",
        r"(?:原油|天然气).{0,35}(?:供应|采购|原料)",
        r"(?:铁矿石|煤炭|焦炭).{0,20}(?:采购|原料|燃料|供应)",
        r"(?:原料|原材料|耐火材料).{0,30}(?:替代|再生|循环利用|回收|消耗|储备)",
    ),
    ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION: (
        r"前五名(?:客户|供应商)",
        r"主要(?:销售)?客户(?:情况)?",
        r"主要供应商(?:情况)?",
        r"(?:客户|供应商)集中度",
        r"客户.{0,30}(?:销售额|营业收入).{0,20}(?:比例|占比)",
        r"供应商.{0,30}(?:采购额|采购总额).{0,20}(?:比例|占比)",
    ),
    ChapterTask.EXTRACT_BUSINESS_REGIME: (
        r"业务、产品或服务发生重大变化",
        r"(?:主营业务|主要业务|经营模式).{0,25}(?:重大变化|未发生重大变化|发生变化)",
        r"(?:报告期)?主要子公司股权变动导致合并范围变化",
        r"合并报表范围的变化情况",
        r"合并范围.{0,25}(?:变化|变动)",
        r"(?:重大资产重组|资产重组|股权过户|完成过户)",
        r"纳入.{0,15}合并报表范围",
        r"(?:新设|设立).{0,15}子公司",
        r"收购.{0,30}股权",
    ),
}
_NARROW_STATISTICAL_CALIBRE_PATTERN = re.compile(
    r"公司主营业务数据统计口径.{0,80}(?:适用|不适用)"
)
_EXPLICIT_ISSUER_CAPACITY_PATTERN = re.compile(
    r"(?:(?:公司|本公司).{0,80}|(?:现有|拥有|下辖).{0,50})"
    r"(?:核定年产能|设计产能|现有产能|总产能|年产能|产能规模)"
    r".{0,30}\d[\d,]*(?:\.\d+)?(?:万吨|吨|GWh|MWh|万㎡|亿㎡|㎡|台|套)"
    r"|主要产品的产能情况.{0,300}(?:设计产能|产能利用率|在建产能)"
)
_SEGMENT_OWNER_PATTERN = re.compile(
    r"分(?:行业|地区|销售模式)|(?<!部)分产品|(?:业务|报告)分部|"
    r"分部(?:收入|利润|资产|信息)|"
    r"主营业务分(?:行业|地区|部)|主营业务(?<!部)分产品|营业收入构成"
)
_STRONG_SEGMENT_OWNER_PATTERN = re.compile(
    r"报告分部的财务信息|(?:本集团|公司|本公司).{0,40}只有一个报告分部|"
    r"细分行业.{0,40}(?:不适用|[√☑]不适用)|"
    r"分(?:行业|产品|地区|销售模式).{0,800}"
    r"(?:营业收入|营业成本|毛利率|成本构成)|"
    r"主营业务分(?:行业|产品|地区|部)"
)
_INCIDENTAL_SEGMENT_CONTEXT_PATTERN = re.compile(
    r"关键审计事项|审计应对|行业政策|产业政策|行业发展情况"
)


def _has_governed_segment_owner(compact: str, keys: set[str]) -> bool:
    if not _SEGMENT_OWNER_PATTERN.search(compact):
        return False
    strong_owner = bool(_STRONG_SEGMENT_OWNER_PATTERN.search(compact))
    if "industry_context" in keys and not strong_owner:
        return False
    return not (
        _INCIDENTAL_SEGMENT_CONTEXT_PATTERN.search(compact) and not strong_owner
    )


def _has_independent_issuer_overview_sentence(text: str) -> bool:
    for sentence in re.split(r"[\n。；！？]+", text):
        compact = re.sub(r"\s+", "", sentence)
        if (
            re.match(r"^(?:公司|本公司)", compact)
            and _ISSUER_OVERVIEW_SUBSTANCE_PATTERN.search(compact)
        ):
            return True
    return False


def _subsidiary_financial_table_without_issuer_overview(text: str) -> bool:
    compact = re.sub(r"\s+", "", text)
    return bool(
        _SUBSIDIARY_FINANCIAL_TABLE_PATTERN.search(compact)
        and not _has_independent_issuer_overview_sentence(text)
    )


def _chapter_owner_score(
    chapter_task: ChapterTask,
    sections: Sequence[Any],
    *,
    selector_reasons: Sequence[str] = (),
    supplemental_terms: Sequence[str] = (),
) -> int:
    # Closed owner score for one chapter Evidence range.
    compact = re.sub(r"\s+", "", "\n".join(str(item.text) for item in sections))
    patterns = _CHAPTER_OWNER_PATTERNS[chapter_task]
    matches = sum(bool(re.search(pattern, compact)) for pattern in patterns)
    if not matches:
        return 0
    reasons = {
        str(reason)
        for item in sections
        for reason in getattr(item, "selector_reasons", ())
    } | {str(reason) for reason in selector_reasons}
    keys = {str(getattr(item, "section_key", "")) for item in sections} | {
        str(term) for term in supplemental_terms
    }
    if chapter_task == ChapterTask.EXTRACT_OPERATING_QUANTITIES:
        explicit_legal_empty = bool(
            re.search(_OPERATING_QUANTITY_LEGAL_EMPTY_PATTERN, compact)
            or re.search(
                r"产能(?:与开工)?情况"
                r".{0,80}(?:[√☑](?:否|不适用)|不适用)",
                compact,
            )
            or re.search(_BSE_INDUSTRY_DISCLOSURE_OPT_OUT_PATTERN, compact)
        )
        physical_unit = r"(?:万吨|吨|GWh|MWh|万㎡|亿㎡|㎡|台|套)"
        quantity_term = (
            r"(?:生产量|销售量|库存量|实际产量|加工量|处理量|吞吐量|"
            r"设计产能|现有产能|核定年产能|总产能|年产能|产能规模)"
        )
        has_quantity_substance = any(
            re.search(pattern, compact)
            for pattern in (
                rf"{quantity_term}.{{0,160}}\d[\d,]*(?:\.\d+)?{physical_unit}",
                rf"{quantity_term}.{{0,160}}{physical_unit}.{{0,80}}\d",
                rf"{physical_unit}.{{0,800}}{quantity_term}.{{0,160}}\d",
                rf"\d[\d,]*(?:\.\d+)?{physical_unit}.{{0,160}}{quantity_term}",
                r"产能利用率.{0,120}\d[\d,]*(?:\.\d+)?(?:%|％)",
                rf"年产\d[\d,]*(?:\.\d+)?{physical_unit}",
            )
        )
        if not explicit_legal_empty and not has_quantity_substance:
            return 0
    if chapter_task == ChapterTask.EXTRACT_BUSINESS_OVERVIEW:
        overview_compact = _BUSINESS_OVERVIEW_CHECKLIST_PATTERN.sub("", compact)
        if not _ISSUER_OVERVIEW_SUBSTANCE_PATTERN.search(overview_compact):
            return 0
    if (
        chapter_task == ChapterTask.EXTRACT_OPERATING_QUANTITIES
        and "industry_context" in keys
        and not _EXPLICIT_ISSUER_CAPACITY_PATTERN.search(compact)
    ):
        return 0
    if chapter_task == ChapterTask.EXTRACT_SEGMENT_FINANCIALS and not (
        _has_governed_segment_owner(compact, keys)
    ):
        return 0
    if (
        chapter_task == ChapterTask.EXTRACT_SEGMENT_FINANCIALS
        and any(re.search(pattern, compact) for pattern in _FINANCIAL_NOTE_MARKERS[:2])
        and not re.search(
            r"分(?:行业|产品|地区|销售模式)|(?:业务|报告)分部|主营业务分",
            compact,
        )
    ):
        return 0
    if (
        chapter_task == ChapterTask.EXTRACT_BUSINESS_OVERVIEW
        and _subsidiary_financial_table_without_issuer_overview(compact)
    ):
        return 0
    if (
        chapter_task == ChapterTask.EXTRACT_BUSINESS_OVERVIEW
        and any(re.search(pattern, compact) for pattern in _FINANCIAL_NOTE_MARKERS)
        and not re.search(
            r"(?:公司|本公司).{0,30}(?:主要从事|主营业务|主要业务|经营范围)",
            compact,
        )
    ):
        return 0
    if (
        chapter_task == ChapterTask.EXTRACT_OPERATING_QUANTITIES
        and "在建工程" in compact
        and not re.search(
            r"(?:产能|生产线|装置|生产量|销售量|库存量|吨|台|套|GWh|MWh|㎡)",
            compact,
        )
    ):
        return 0
    if chapter_task == ChapterTask.EXTRACT_OPERATING_QUANTITIES and len(sections) == 1:
        planning_only = "生产计划" in compact and any(
            marker in compact for marker in ("销售预测量", "往年同期", "目前库存量")
        )
        disclosed_quantity = any(
            re.search(pattern, compact)
            for pattern in (
                r"公司实物销售收入是否大于劳务收入.{0,60}(?:[√☑]否|不适用)",
                r"(?:生产量|销售量|库存量|实际产量).{0,100}\d[\d,]*(?:\.\d+)?",
                r"\d[\d,]*(?:\.\d+)?(?:万吨|吨|台|套|GWh|MWh|万㎡|亿㎡|㎡)",
            )
        )
        if planning_only and not disclosed_quantity:
            return 0
    if (
        chapter_task == ChapterTask.EXTRACT_MATERIAL_INPUTS
        and len(sections) == 1
        and re.search(
            r"(?:控股股东|实际控制人).{0,120}(?:主要经营业务|主营业务|经营范围)",
            compact,
        )
        and not re.search(
            r"(?:公司|本公司).{0,50}(?:采购|购入|消耗|生产所需|主要原材料|原材料供应)|"
            r"(?:营业成本构成|主营业务成本构成|成本构成项目)",
            compact,
        )
    ):
        return 0
    if chapter_task == ChapterTask.EXTRACT_BUSINESS_REGIME:
        without_statistical = _NARROW_STATISTICAL_CALIBRE_PATTERN.sub("", compact)
        if not any(re.search(pattern, without_statistical) for pattern in patterns):
            return 0
    key_bonus = sum(
        reason.startswith(("heading_alias:", "table_signature:")) for reason in reasons
    ) + len(keys & set(_CHAPTER_CONFIG[chapter_task][1]))
    return matches * 100 + key_bonus


def _with_material_continuation_sections(
    sections: Sequence[Any],
    artifact: Any,
) -> tuple[Any, ...]:
    """Add only source-bound adjacent material continuation pages."""

    by_page = {int(item.page_number): item for item in sections}
    artifact_pages = {
        int(page.page_number): page for page in getattr(artifact, "pages", ())
    }
    required = set(by_page)
    for section in tuple(sections):
        page_number = int(section.page_number)
        if not (
            _chapter_owner_score(ChapterTask.EXTRACT_MATERIAL_INPUTS, (section,)) > 0
            or _section_matches(
                section,
                _CHAPTER_CONFIG[ChapterTask.EXTRACT_MATERIAL_INPUTS][1],
                _CHAPTER_CONFIG[ChapterTask.EXTRACT_MATERIAL_INPUTS][2],
            )
        ):
            continue
        next_page = artifact_pages.get(page_number + 1)
        if next_page is None:
            continue
        next_compact = re.sub(r"\s+", "", str(next_page.text or ""))
        if not _MATERIAL_CONTINUATION_OWNER_PATTERN.search(next_compact):
            continue
        required.add(page_number + 1)
        following = artifact_pages.get(page_number + 2)
        if (
            following is not None
            and _MATERIAL_PAGE_BREAK_TAIL_PATTERN.search(next_compact)
            and str(following.text or "").strip()
        ):
            required.add(page_number + 2)

    for page_number in sorted(required - set(by_page)):
        page = artifact_pages[page_number]
        text = str(page.text or "")
        method = str(getattr(page, "extraction_method", "native_text") or "")
        quality = (
            "governed_ocr"
            if method == "ocr" and text.strip()
            else "native"
            if text.strip() and method in {"native_text", "alternate_native"}
            else "unsupported"
        )
        page_hash = str(
            getattr(page, "page_artifact_hash", "")
            or getattr(page, "text_hash", "")
            or _payload_hash(text)
        )
        section_hash = _payload_hash(
            {
                "page_number": page_number,
                "page_hash": page_hash,
                "text": re.sub(r"\s+", " ", text).strip(),
                "reason": "material_page_continuation",
            }
        )
        by_page[page_number] = SimpleNamespace(
            section_id=f"material-continuation:{page_number}:{section_hash[:16]}",
            page_number=page_number,
            section_key="context",
            text=text,
            normalized_text=re.sub(r"\s+", " ", text).strip(),
            normalized_start=0,
            normalized_end=len(text),
            page_hash=page_hash,
            section_hash=section_hash,
            selector_reasons=("material_page_continuation",),
            quality=quality,
        )
    return tuple(by_page[page] for page in sorted(by_page))


def _chapter_owner_page_scope(
    artifact: Any,
    chapter_task: ChapterTask,
) -> tuple[int, ...]:
    pages = {
        int(page.page_number)
        for page in getattr(artifact, "pages", ())
        if _chapter_owner_score(
            chapter_task,
            (
                SimpleNamespace(
                    page_number=int(page.page_number),
                    section_key="",
                    selector_reasons=(),
                    text=str(page.text or ""),
                ),
            ),
        )
        > 0
    }
    available = {int(page.page_number) for page in getattr(artifact, "pages", ())}
    return tuple(
        sorted(
            page
            for owner_page in pages
            for page in (owner_page - 1, owner_page, owner_page + 1)
            if page in available
        )
    )


class ShadowPlanningFailureCode(str, Enum):
    PDF_ARTIFACT_FAILED = "pdf_artifact_failed"
    PDF_IDENTITY_MISMATCH = "pdf_identity_mismatch"
    CHAPTER_EVIDENCE_NOT_FOUND = "chapter_evidence_not_found"
    PAGE_UNREADABLE = "page_unreadable"
    TABLE_CONTEXT_INCOMPLETE = "table_context_incomplete"
    FIELD_CONTRACT_INVALID = "field_contract_invalid"
    PLAN_INVALID = "plan_invalid"
    PLAN_IDENTITY_MISMATCH = "plan_identity_mismatch"


class ShadowEvidencePlanningError(RuntimeError):
    def __init__(
        self,
        code: ShadowPlanningFailureCode,
        message: str,
        *,
        sample_id: str | None = None,
        chapter_task: ChapterTask | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.sample_id = sample_id
        self.chapter_task = chapter_task


class ShadowEvidenceScopeSelection(_StrictModel):
    chapter_task: ChapterTask
    scope_id: str = Field(min_length=1)
    pages: tuple[int, ...] = Field(min_length=1, max_length=3)
    page_hashes: dict[str, str]
    section_hashes: tuple[str, ...] = Field(min_length=1)
    selector_reasons: tuple[str, ...] = Field(min_length=1)
    quality: Literal["native", "governed_ocr"]

    @model_validator(mode="after")
    def _selection_is_bound_to_scope(self) -> ShadowEvidenceScopeSelection:
        if tuple(sorted(set(self.pages))) != self.pages:
            raise ValueError(
                "shadow Evidence selection pages must be sorted and unique"
            )
        if set(self.page_hashes) != {str(page) for page in self.pages}:
            raise ValueError("shadow Evidence selection page hashes mismatch")
        if any(
            not re.fullmatch(r"[0-9a-f]{64}", value)
            for value in self.page_hashes.values()
        ):
            raise ValueError("shadow Evidence selection requires valid page hashes")
        if any(
            not re.fullmatch(r"[0-9a-f]{64}", value) for value in self.section_hashes
        ):
            raise ValueError("shadow Evidence selection requires valid section hashes")
        return self


class ShadowEvidencePlan(_StrictModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["company_profile_shadow_evidence_plan.v1"] = (
        SHADOW_EVIDENCE_PLAN_SCHEMA
    )
    plan_version: str = Field(min_length=1)
    sample_manifest_revision: str = Field(min_length=1)
    sample_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    page_coordinate_system: Literal["one_based_pdf_physical_page"] = (
        "one_based_pdf_physical_page"
    )
    reports: tuple[EvidenceReportPlan, ...] = Field(
        min_length=SHADOW_REPORT_COUNT,
        max_length=SHADOW_REPORT_COUNT,
    )
    pdf_artifact_hashes: dict[str, str]
    selected_section_hashes: dict[str, tuple[str, ...]]
    scope_selections: dict[str, tuple[ShadowEvidenceScopeSelection, ...]]
    recovery_states: dict[str, str]
    recovered_page_numbers: dict[str, tuple[int, ...]]
    created_at: str = Field(min_length=1)
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _plan_is_frozen(self) -> ShadowEvidencePlan:
        sample_ids = [item.sample_id for item in self.reports]
        if len(sample_ids) != len(set(sample_ids)):
            raise ValueError("shadow Evidence plan contains duplicate reports")
        if set(sample_ids) != set(self.pdf_artifact_hashes):
            raise ValueError("shadow Evidence plan PDF artifact identities mismatch")
        if set(sample_ids) != set(self.selected_section_hashes):
            raise ValueError("shadow Evidence plan section identities mismatch")
        if set(sample_ids) != set(self.scope_selections):
            raise ValueError("shadow Evidence plan scope-selection identities mismatch")
        if set(sample_ids) != set(self.recovery_states):
            raise ValueError("shadow Evidence plan recovery-state identities mismatch")
        if set(sample_ids) != set(self.recovered_page_numbers):
            raise ValueError("shadow Evidence plan recovered-page identities mismatch")
        if any(not values for values in self.selected_section_hashes.values()):
            raise ValueError("shadow Evidence plan requires selected section hashes")
        reports_by_id = {item.sample_id: item for item in self.reports}
        for sample_id, selections in self.scope_selections.items():
            report = reports_by_id[sample_id]
            planned = {
                (task.chapter_task, scope.scope_id): scope.pages
                for task in report.tasks
                for scope in task.request_scopes
            }
            selected = {
                (item.chapter_task, item.scope_id): item.pages for item in selections
            }
            if selected != planned:
                raise ValueError(
                    "shadow Evidence scope selections mismatch report plan"
                )
        if self.plan_hash != _payload_hash(self, omit={"plan_hash"}):
            raise ValueError("shadow Evidence plan hash mismatch")
        return self

    def report_by_id(self, sample_id: str) -> EvidenceReportPlan:
        for report in self.reports:
            if report.sample_id == sample_id:
                return report
        raise ShadowEvidencePlanningError(
            ShadowPlanningFailureCode.PLAN_IDENTITY_MISMATCH,
            f"sample is outside the frozen shadow Evidence plan: {sample_id}",
            sample_id=sample_id,
        )


class ShadowPreparationReport(_StrictModel):
    sample_id: str = Field(min_length=1)
    status: Literal["prepared"] = "prepared"
    scope_count: int = Field(ge=6, le=9)
    selected_page_count: int = Field(ge=1)
    evidence_count: int = Field(ge=6)
    field_contract_passed: Literal[True] = True
    pdf_artifact_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    recovery_state: str = Field(min_length=1)
    recovered_page_numbers: tuple[int, ...] = ()


class ShadowEvidencePreparationAudit(_StrictModel):
    schema_version: Literal["company_profile_shadow_preparation_audit.v1"] = (
        SHADOW_PREPARATION_AUDIT_SCHEMA
    )
    audit_id: str = Field(min_length=1)
    sample_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    evidence_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    report_count: int = Field(ge=SHADOW_REPORT_COUNT, le=SHADOW_REPORT_COUNT)
    planned_report_count: int = Field(ge=SHADOW_REPORT_COUNT, le=SHADOW_REPORT_COUNT)
    total_scope_count: int = Field(ge=SHADOW_REPORT_COUNT * 6)
    total_evidence_count: int = Field(ge=SHADOW_REPORT_COUNT * 6)
    evidence_traceability_rate: Literal[1.0] = 1.0
    recovered_report_count: int = Field(ge=0, le=SHADOW_REPORT_COUNT)
    reports: tuple[ShadowPreparationReport, ...] = Field(
        min_length=SHADOW_REPORT_COUNT,
        max_length=SHADOW_REPORT_COUNT,
    )
    created_at: str = Field(min_length=1)
    audit_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _audit_is_frozen(self) -> ShadowEvidencePreparationAudit:
        if len({item.sample_id for item in self.reports}) != SHADOW_REPORT_COUNT:
            raise ValueError("shadow preparation audit report identities mismatch")
        if self.total_scope_count != sum(item.scope_count for item in self.reports):
            raise ValueError("shadow preparation audit scope count mismatch")
        if self.total_evidence_count != sum(
            item.evidence_count for item in self.reports
        ):
            raise ValueError("shadow preparation audit Evidence count mismatch")
        if self.recovered_report_count != sum(
            bool(item.recovered_page_numbers) for item in self.reports
        ):
            raise ValueError("shadow preparation audit recovery count mismatch")
        if self.audit_hash != _payload_hash(self, omit={"audit_hash"}):
            raise ValueError("shadow preparation audit hash mismatch")
        return self


class ShadowOperatingEvidenceOwnershipAudit(_StrictModel):
    schema_version: Literal[
        "company_profile_shadow_operating_evidence_ownership_audit.v1"
    ] = "company_profile_shadow_operating_evidence_ownership_audit.v1"
    audit_id: str = Field(min_length=1)
    sample_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    baseline_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    corrected_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    affected_scope_count: int = Field(ge=1)
    affected_scopes: tuple[dict[str, Any], ...] = Field(min_length=1)
    provider_calls: Literal[0] = 0
    historical_artifacts_mutated: Literal[False] = False
    production_paths_opened: tuple[str, ...] = ()
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION
    created_at: str = Field(min_length=1)
    audit_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _audit_is_closed(self) -> ShadowOperatingEvidenceOwnershipAudit:
        if self.affected_scope_count != len(self.affected_scopes):
            raise ValueError("ownership audit affected scope count mismatch")
        if self.audit_hash != _payload_hash(self, omit={"audit_hash"}):
            raise ValueError("ownership audit hash mismatch")
        return self


class ShadowOwnerClosureResult(_StrictModel):
    review_row_id: str = Field(min_length=1)
    sample_id: str = Field(min_length=1)
    chapter_task: ChapterTask
    field_id: str = Field(min_length=1)
    physical_page: int = Field(ge=1)
    expected_result: Literal[
        "reject_legal_empty",
        "reject_cross_reference_only",
        "preserve_legal_empty",
    ]
    matching_scope_ids: tuple[str, ...]
    status: Literal["resolved", "preserved"]


class ShadowOwnerClosureCorrectionAudit(_StrictModel):
    schema_version: Literal["company_profile_shadow_evidence_correction_audit.v1"] = (
        "company_profile_shadow_evidence_correction_audit.v1"
    )
    audit_id: str = Field(min_length=1)
    sample_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    baseline_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    corrected_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    preparation_audit_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    owner_regression_fixture_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    owner_regression_closure_audit_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    results: tuple[ShadowOwnerClosureResult, ...] = Field(min_length=8, max_length=8)
    unresolved_finding_ids: tuple[str, ...] = ()
    provider_calls: Literal[0] = 0
    created_at: str = Field(min_length=1)
    audit_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _audit_is_closed(self) -> ShadowOwnerClosureCorrectionAudit:
        if len({item.review_row_id for item in self.results}) != len(self.results):
            raise ValueError("shadow owner-closure results must be unique")
        if self.unresolved_finding_ids:
            raise ValueError("shadow owner-closure correction audit is unresolved")
        if self.audit_hash != _payload_hash(self, omit={"audit_hash"}):
            raise ValueError("shadow owner-closure correction audit hash mismatch")
        return self


class ShadowRoutingContinuationResult(_StrictModel):
    review_row_id: str = Field(min_length=1)
    sample_id: str = Field(min_length=1)
    chapter_task: ChapterTask
    field_id: str = Field(min_length=1)
    expected_result: Literal[
        "reject_assignment",
        "require_pages",
        "preserve_assignment",
    ]
    physical_pages: tuple[int, ...] = Field(min_length=1, max_length=3)
    baseline_scope_ids: tuple[str, ...]
    corrected_scope_ids: tuple[str, ...]
    corrected_scope_pages: tuple[tuple[int, ...], ...]
    status: Literal["resolved", "preserved"]


class ShadowRoutingContinuationCorrectionAudit(_StrictModel):
    schema_version: Literal[
        "company_profile_shadow_routing_continuation_audit.v1"
    ] = SHADOW_ROUTING_CONTINUATION_AUDIT_SCHEMA
    audit_id: str = Field(min_length=1)
    sample_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    baseline_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    corrected_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    preparation_audit_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    fixture_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_review_package_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_review_outcomes_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    report_count: Literal[20] = SHADOW_REPORT_COUNT
    results: tuple[ShadowRoutingContinuationResult, ...] = Field(
        min_length=6, max_length=6
    )
    unresolved_finding_ids: tuple[str, ...] = ()
    provider_calls: Literal[0] = 0
    cohort_replay_performed: Literal[False] = False
    historical_artifacts_mutated: Literal[False] = False
    production_paths_opened: tuple[str, ...] = ()
    created_at: str = Field(min_length=1)
    audit_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _audit_is_closed(self) -> ShadowRoutingContinuationCorrectionAudit:
        if len({item.review_row_id for item in self.results}) != len(self.results):
            raise ValueError("routing/continuation audit results must be unique")
        if self.unresolved_finding_ids:
            raise ValueError("routing/continuation correction audit is unresolved")
        if self.production_paths_opened:
            raise ValueError("routing/continuation audit cannot open production paths")
        if self.audit_hash != _payload_hash(self, omit={"audit_hash"}):
            raise ValueError("routing/continuation audit hash mismatch")
        return self


class ShadowScopeRefinementAudit(_StrictModel):
    schema_version: Literal["company_profile_shadow_scope_refinement_audit.v1"] = (
        SHADOW_SCOPE_REFINEMENT_AUDIT_SCHEMA
    )
    audit_id: str = Field(min_length=1)
    sample_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    baseline_plan_version: str = Field(min_length=1)
    baseline_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    refined_plan_version: str = Field(min_length=1)
    refined_plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    report_count: int = Field(ge=SHADOW_REPORT_COUNT, le=SHADOW_REPORT_COUNT)
    baseline_scope_count: int = Field(ge=SHADOW_REPORT_COUNT * 6)
    refined_scope_count: int = Field(ge=SHADOW_REPORT_COUNT * 6)
    baseline_field_scope_pair_count: int = Field(gt=0)
    refined_field_scope_pair_count: int = Field(gt=0)
    baseline_field_bound_evidence_copy_count: int = Field(gt=0)
    refined_field_bound_evidence_copy_count: int = Field(gt=0)
    baseline_field_bound_evidence_character_count: int = Field(gt=0)
    refined_field_bound_evidence_character_count: int = Field(gt=0)
    field_bound_evidence_copy_reduction_rate: float = Field(ge=0.0, le=1.0)
    field_bound_evidence_character_reduction_rate: float = Field(ge=0.0, le=1.0)
    unsupported_assignment_count: Literal[0] = 0
    missing_required_owner_count: Literal[0] = 0
    table_context_incomplete_count: Literal[0] = 0
    evidence_traceability_rate: Literal[1.0] = 1.0
    provider_calls: Literal[0] = 0
    chapter_metrics: dict[str, dict[str, int]]
    created_at: str = Field(min_length=1)
    audit_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    production_authorization: Literal["not_authorized"] = PRODUCTION_AUTHORIZATION

    @model_validator(mode="after")
    def _audit_is_consistent(self) -> ShadowScopeRefinementAudit:
        if self.refined_field_scope_pair_count >= self.baseline_field_scope_pair_count:
            raise ValueError("refined scope plan must reduce field/scope assignments")
        if (
            self.refined_field_bound_evidence_copy_count
            >= self.baseline_field_bound_evidence_copy_count
        ):
            raise ValueError(
                "refined scope plan must reduce field-bound Evidence copies"
            )
        if (
            self.refined_field_bound_evidence_character_count
            >= self.baseline_field_bound_evidence_character_count
        ):
            raise ValueError(
                "refined scope plan must reduce field-bound Evidence characters"
            )
        if self.audit_hash != _payload_hash(self, omit={"audit_hash"}):
            raise ValueError("shadow scope refinement audit hash mismatch")
        return self


class ShadowEvidencePlanner:
    """Translate governed PDF sections into existing Stage 5 plan objects."""

    def __init__(
        self,
        *,
        extractor: BusinessProfilePdfArtifactExtractor | None = None,
        selector: BusinessProfileSectionSelector | None = None,
        plan_version: str = SHADOW_EVIDENCE_PLAN_VERSION,
    ) -> None:
        self._uses_custom_extractor = extractor is not None
        self._extractor = extractor or BusinessProfilePdfArtifactExtractor(
            engine_profile="pypdf_native"
        )
        self._selector = selector or BusinessProfileSectionSelector(
            context_pages=1,
            max_pages=6,
        )
        self._plan_version = plan_version
        self._catalog = load_disclosure_template_catalog()

    def build(
        self,
        manifest: ShadowSampleManifest,
        *,
        expected_artifact_hashes: Mapping[str, str] | None = None,
    ) -> ShadowEvidencePlan:
        reports: list[EvidenceReportPlan] = []
        artifact_hashes: dict[str, str] = {}
        section_hashes: dict[str, tuple[str, ...]] = {}
        scope_selections: dict[str, tuple[ShadowEvidenceScopeSelection, ...]] = {}
        recovery_states: dict[str, str] = {}
        recovered_page_numbers: dict[str, tuple[int, ...]] = {}
        for report in manifest.reports:
            artifact, recovery_state, recovered_pages = self.load_artifact(
                report,
                expected_artifact_hash=(expected_artifact_hashes or {}).get(
                    report.sample_id
                ),
            )
            if artifact.status == "parse_failed":
                failure_class = artifact.diagnostics.get("failure_class")
                raise ShadowEvidencePlanningError(
                    ShadowPlanningFailureCode.PDF_ARTIFACT_FAILED,
                    f"PDF artifact failed: {failure_class}",
                    sample_id=report.sample_id,
                )
            if (
                artifact.source_content_hash != report.content_hash
                or artifact.page_count != report.page_count
            ):
                raise ShadowEvidencePlanningError(
                    ShadowPlanningFailureCode.PDF_IDENTITY_MISMATCH,
                    "PDF artifact identity does not match the frozen manifest",
                    sample_id=report.sample_id,
                )
            templates = self._catalog.select(
                document_date=report.report.published_at[:10],
                exchange=report.exchange,
                board=report.board,
                document_type="annual_report",
                industry_group=report.industry_group,
            )
            tasks: list[EvidenceTaskPlan] = []
            hashes: list[str] = []
            selections: list[ShadowEvidenceScopeSelection] = []
            for chapter_task in ChapterTask:
                task, selected_hashes, selected_scopes = self._plan_task(
                    report=report,
                    artifact=artifact,
                    templates=templates,
                    chapter_task=chapter_task,
                )
                tasks.append(task)
                hashes.extend(selected_hashes)
                selections.extend(selected_scopes)
            reports.append(
                EvidenceReportPlan(
                    sample_id=report.sample_id,
                    content_hash=report.content_hash,
                    plan_version=self._plan_version,
                    tasks=tuple(tasks),
                )
            )
            artifact_hashes[report.sample_id] = artifact.artifact_hash
            section_hashes[report.sample_id] = tuple(hashes)
            scope_selections[report.sample_id] = tuple(selections)
            recovery_states[report.sample_id] = recovery_state
            recovered_page_numbers[report.sample_id] = recovered_pages
        payload = {
            "schema_version": SHADOW_EVIDENCE_PLAN_SCHEMA,
            "plan_version": self._plan_version,
            "sample_manifest_revision": manifest.manifest_revision,
            "sample_manifest_hash": manifest.manifest_hash,
            "page_coordinate_system": "one_based_pdf_physical_page",
            "reports": tuple(reports),
            "pdf_artifact_hashes": artifact_hashes,
            "selected_section_hashes": section_hashes,
            "scope_selections": scope_selections,
            "recovery_states": recovery_states,
            "recovered_page_numbers": recovered_page_numbers,
            "created_at": _utc_now(),
            "production_authorization": PRODUCTION_AUTHORIZATION,
        }
        return ShadowEvidencePlan(**payload, plan_hash=_payload_hash(payload))

    def load_artifact(
        self,
        report: Any,
        *,
        expected_artifact_hash: str | None = None,
    ) -> tuple[Any, str, tuple[int, ...]]:
        if self._uses_custom_extractor:
            artifact = self._extractor.extract_file(
                report.local_path,
                source_file_id=report.asset_id,
            )
            return (
                artifact,
                str(getattr(artifact, "recovery_state", "native_ready")),
                (),
            )
        document = {
            "archive_path": str(report.local_path),
            "content_hash": report.content_hash,
            "source_file_id": report.asset_id,
        }
        try:
            native_result = ensure_archived_pdf_page_artifact(
                document,
                extractor=self._extractor,
            )
            artifact = native_result["artifact"]
            if expected_artifact_hash:
                frozen = _load_frozen_artifact(
                    report=report,
                    native_artifact_path=Path(str(native_result["artifact_path"])),
                    expected_artifact_hash=expected_artifact_hash,
                )
                if frozen is not None:
                    recovered_pages = tuple(
                        sorted(
                            int(page)
                            for page in frozen.diagnostics.get(
                                "recovered_page_numbers", ()
                            )
                        )
                    )
                    state = (
                        "partial_ocr"
                        if recovered_pages
                        else str(frozen.recovery_state or "native_ready")
                    )
                    return frozen, state, recovered_pages
            result, _outline, recovery = _recover_business_profile_document(
                document,
                {
                    "artifact": artifact,
                    "artifact_hash": artifact.artifact_hash,
                    "status": artifact.status,
                },
            )
        except (OSError, RuntimeError, ValueError) as exc:
            raise ShadowEvidencePlanningError(
                ShadowPlanningFailureCode.PDF_ARTIFACT_FAILED,
                f"bounded PDF recovery failed: {exc}",
                sample_id=report.sample_id,
            ) from exc
        state = str(recovery.get("recovery_state") or "native_ready")
        resolved_artifact = result["artifact"]
        recovered_pages = tuple(
            sorted(
                int(page)
                for page in resolved_artifact.diagnostics.get(
                    "recovered_page_numbers", ()
                )
            )
        )
        return resolved_artifact, state, recovered_pages

    def _plan_task(
        self,
        *,
        report: Any,
        artifact: Any,
        templates: tuple[ResolvedDisclosureTemplate, ...],
        chapter_task: ChapterTask,
    ) -> tuple[
        EvidenceTaskPlan,
        tuple[str, ...],
        tuple[ShadowEvidenceScopeSelection, ...],
    ]:
        ownership_aware = self._plan_version == SHADOW_OPERATING_OWNERSHIP_PLAN_VERSION
        field_ids, allowed_keys, hint_terms = _CHAPTER_CONFIG[chapter_task]
        unknown_fields = sorted(set(field_ids) - stage5_field_ids())
        if unknown_fields:
            raise ShadowEvidencePlanningError(
                ShadowPlanningFailureCode.FIELD_CONTRACT_INVALID,
                f"unknown Stage 5 checklist fields: {unknown_fields}",
                sample_id=report.sample_id,
                chapter_task=chapter_task,
            )
        try:
            selected = self._selector.select(
                artifact=artifact,
                instrument_id=report.report.instrument_id,
                source_document_id=report.asset_id,
                field_family=ANNUAL_REPORT_SEMANTIC_BUNDLE_FAMILY,
                templates=templates,
                hint_terms=hint_terms,
                page_scope=_chapter_owner_page_scope(artifact, chapter_task),
                max_pages_override=6,
            )
        except ValueError as exc:
            unreadable = str(getattr(artifact, "recovery_state", "")) in {
                "partial_ocr",
                "section_ocr_required",
                "source_unrecoverable",
                "toc_unresolved",
                "toc_probe_required",
            }
            raise ShadowEvidencePlanningError(
                (
                    ShadowPlanningFailureCode.PAGE_UNREADABLE
                    if unreadable
                    else ShadowPlanningFailureCode.CHAPTER_EVIDENCE_NOT_FOUND
                ),
                str(exc),
                sample_id=report.sample_id,
                chapter_task=chapter_task,
            ) from exc
        selected_sections = (
            _with_material_continuation_sections(selected.sections, artifact)
            if chapter_task == ChapterTask.EXTRACT_MATERIAL_INPUTS
            else selected.sections
        )
        unreadable = [
            item.page_number
            for item in selected_sections
            if not item.text.strip() or item.quality not in {"native", "governed_ocr"}
        ]
        unreadable_pages = set(unreadable)
        readable_sections = tuple(
            item
            for item in selected_sections
            if item.page_number not in unreadable_pages
        )
        direct_pages = {
            section.page_number
            for section in readable_sections
            if (
                _section_matches(section, allowed_keys, hint_terms)
                or (
                    chapter_task == ChapterTask.EXTRACT_MATERIAL_INPUTS
                    and _MATERIAL_CONTINUATION_OWNER_PATTERN.search(
                        re.sub(r"\s+", "", str(section.text or ""))
                    )
                )
            )
            and _chapter_owner_score(chapter_task, (section,)) > 0
        }
        if not direct_pages:
            only_unreadable = bool(unreadable) and not readable_sections
            raise ShadowEvidencePlanningError(
                (
                    ShadowPlanningFailureCode.PAGE_UNREADABLE
                    if only_unreadable
                    else ShadowPlanningFailureCode.CHAPTER_EVIDENCE_NOT_FOUND
                ),
                (
                    f"selected Evidence pages are unreadable: {sorted(unreadable_pages)}"
                    if only_unreadable
                    else f"no governed Evidence selected for {chapter_task.value}"
                ),
                sample_id=report.sample_id,
                chapter_task=chapter_task,
            )
        available = {section.page_number for section in readable_sections}
        bounded = sorted(
            page
            for page in available
            if page in direct_pages
            or page - 1 in direct_pages
            or page + 1 in direct_pages
        )
        scopes: list[EvidenceScopePlan] = []
        scope_selections: list[ShadowEvidenceScopeSelection] = []
        chapter_name = chapter_task.value.removeprefix("extract_")
        ranges = _select_scope_ranges(
            readable_sections,
            direct_pages=direct_pages,
            bounded_pages=bounded,
            maximum_scopes=_CHAPTER_MAX_SCOPES[chapter_task],
            chapter_task=chapter_task,
            ownership_aware=ownership_aware,
        )
        for index, pages in enumerate(ranges, start=1):
            pages = _bind_table_context_range(
                pages,
                readable_sections,
                sample_id=report.sample_id,
                chapter_task=chapter_task,
            )
            page_set = set(pages)
            sections = [
                item for item in readable_sections if item.page_number in page_set
            ]
            combined = "\n".join(item.text for item in sections)
            support_sections = (
                [item for item in sections if item.page_number in direct_pages]
                if ownership_aware
                and chapter_task == ChapterTask.EXTRACT_OPERATING_QUANTITIES
                else sections
            )
            scope_field_ids = _scope_field_ids(
                chapter_task,
                support_sections,
                ownership_aware=ownership_aware,
            )
            if not scope_field_ids:
                continue
            scopes.append(
                EvidenceScopePlan(
                    scope_id=f"{chapter_name}-{index:02d}",
                    field_ids=scope_field_ids,
                    pages=pages,
                    section_titles=tuple(
                        dict.fromkeys(
                            item.section_key
                            for item in sections
                            if item.section_key != "context"
                        )
                    )
                    or (chapter_task.value,),
                    anchor_terms=(_anchor_term(sections, allowed_keys, hint_terms),),
                    required_headers=_matched_headers(sections, templates, combined),
                    required_units=_source_units(combined),
                    required_footnotes=_source_footnotes(combined),
                    continuation_required=len(pages) > 1,
                    printed_page_labels=_printed_labels(artifact, pages),
                    candidate_pages=tuple(
                        page for page in pages if page in direct_pages
                    ),
                )
            )
            scope_selections.append(
                ShadowEvidenceScopeSelection(
                    chapter_task=chapter_task,
                    scope_id=f"{chapter_name}-{index:02d}",
                    pages=pages,
                    page_hashes={
                        str(item.page_number): item.page_hash for item in sections
                    },
                    section_hashes=tuple(item.section_hash for item in sections),
                    selector_reasons=tuple(
                        sorted(
                            {
                                reason
                                for item in sections
                                for reason in item.selector_reasons
                            }
                        )
                    ),
                    quality=(
                        "governed_ocr"
                        if any(item.quality == "governed_ocr" for item in sections)
                        else "native"
                    ),
                )
            )
        if not scopes:
            raise ShadowEvidencePlanningError(
                ShadowPlanningFailureCode.CHAPTER_EVIDENCE_NOT_FOUND,
                f"no source-supported fields selected for {chapter_task.value}",
                sample_id=report.sample_id,
                chapter_task=chapter_task,
            )
        planned_fields = {field_id for scope in scopes for field_id in scope.field_ids}
        missing_required = sorted(
            set(_REQUIRED_CHAPTER_FIELDS.get(chapter_task, ())) - planned_fields
        )
        if missing_required:
            raise ShadowEvidencePlanningError(
                ShadowPlanningFailureCode.CHAPTER_EVIDENCE_NOT_FOUND,
                "required chapter fields have no source-supported owning scope: "
                f"{missing_required}",
                sample_id=report.sample_id,
                chapter_task=chapter_task,
            )
        return (
            EvidenceTaskPlan(
                chapter_task=chapter_task,
                request_scopes=tuple(scopes),
            ),
            tuple(item.section_hash for item in selected.sections),
            tuple(scope_selections),
        )


class ShadowEvidencePreparer:
    """Prepare a frozen shadow plan through the existing Stage 5 owner."""

    def __init__(
        self,
        *,
        planner: ShadowEvidencePlanner | None = None,
        stage5_preparer: Stage5EvidencePreparer | None = None,
    ) -> None:
        self._planner = planner or ShadowEvidencePlanner()
        self._stage5_preparer = stage5_preparer or Stage5EvidencePreparer()

    def prepare(
        self,
        *,
        manifest: ShadowSampleManifest,
        plan: ShadowEvidencePlan,
    ) -> dict[str, tuple[PreparedRequestScope, ...]]:
        if (
            plan.sample_manifest_revision != manifest.manifest_revision
            or plan.sample_manifest_hash != manifest.manifest_hash
        ):
            raise ShadowEvidencePlanningError(
                ShadowPlanningFailureCode.PLAN_IDENTITY_MISMATCH,
                "shadow Evidence plan does not match the active manifest",
            )
        prepared: dict[str, tuple[PreparedRequestScope, ...]] = {}
        for asset in manifest.reports:
            report_plan = plan.report_by_id(asset.sample_id)
            artifact, recovery_state, recovered_pages = self._planner.load_artifact(
                asset,
                expected_artifact_hash=plan.pdf_artifact_hashes[asset.sample_id],
            )
            if (
                artifact.source_content_hash != asset.content_hash
                or artifact.page_count != asset.page_count
                or artifact.artifact_hash != plan.pdf_artifact_hashes[asset.sample_id]
                or recovery_state != plan.recovery_states[asset.sample_id]
                or recovered_pages != plan.recovered_page_numbers[asset.sample_id]
            ):
                raise ShadowEvidencePlanningError(
                    ShadowPlanningFailureCode.PDF_IDENTITY_MISMATCH,
                    "prepared PDF artifact differs from the frozen Evidence plan",
                    sample_id=asset.sample_id,
                )
            artifact_pages = {item.page_number: item for item in artifact.pages}
            for selection in plan.scope_selections[asset.sample_id]:
                for page_number, expected_hash in selection.page_hashes.items():
                    page = artifact_pages.get(int(page_number))
                    if page is None or page.page_artifact_hash != expected_hash:
                        raise ShadowEvidencePlanningError(
                            ShadowPlanningFailureCode.PDF_IDENTITY_MISMATCH,
                            "selected page hash differs from the frozen Evidence plan",
                            sample_id=asset.sample_id,
                            chapter_task=selection.chapter_task,
                        )
            unknown = sorted(
                {
                    field_id
                    for task in report_plan.tasks
                    for scope in task.request_scopes
                    for field_id in scope.field_ids
                }
                - stage5_field_ids()
            )
            if unknown:
                raise ShadowEvidencePlanningError(
                    ShadowPlanningFailureCode.FIELD_CONTRACT_INVALID,
                    f"unknown Stage 5 checklist fields: {unknown}",
                    sample_id=asset.sample_id,
                )
            if plan.plan_version in {
                SHADOW_EVIDENCE_PLAN_VERSION,
                SHADOW_OPERATING_OWNERSHIP_PLAN_VERSION,
            }:
                _validate_refined_report_plan(
                    report_plan,
                    artifact_pages=artifact_pages,
                    sample_id=asset.sample_id,
                    scope_reasons={
                        (selection.chapter_task, selection.scope_id): (
                            selection.selector_reasons
                        )
                        for selection in plan.scope_selections[asset.sample_id]
                    },
                    ownership_aware=(
                        plan.plan_version == SHADOW_OPERATING_OWNERSHIP_PLAN_VERSION
                    ),
                )
            page_results = {
                number: _artifact_page_result(page)
                for number, page in artifact_pages.items()
            }
            prepared_scopes = self._stage5_preparer.prepare_asset_plan(
                asset=asset,
                plan=report_plan,
                plan_version=plan.plan_version,
                page_results=page_results,
            )
            prepared[asset.sample_id] = tuple(
                _bind_shadow_operating_evidence(scope)
                if plan.plan_version == SHADOW_OPERATING_OWNERSHIP_PLAN_VERSION
                else scope
                for scope in prepared_scopes
            )
        return prepared


def _validate_refined_report_plan(
    report_plan: EvidenceReportPlan,
    *,
    artifact_pages: dict[int, Any],
    sample_id: str,
    scope_reasons: dict[tuple[ChapterTask, str], tuple[str, ...]],
    ownership_aware: bool = False,
) -> None:
    for task in report_plan.tasks:
        planned_fields: set[str] = set()
        for scope in task.request_scopes:
            try:
                sections = tuple(
                    SimpleNamespace(
                        page_number=page,
                        text=str(artifact_pages[page].text or ""),
                    )
                    for page in scope.pages
                )
            except KeyError as exc:
                raise ShadowEvidencePlanningError(
                    ShadowPlanningFailureCode.PDF_IDENTITY_MISMATCH,
                    f"refined scope page is missing from the PDF artifact: {exc}",
                    sample_id=sample_id,
                    chapter_task=task.chapter_task,
                ) from exc
            support_sections = (
                tuple(
                    section
                    for section in sections
                    if section.page_number in scope.candidate_pages
                )
                if ownership_aware
                and task.chapter_task == ChapterTask.EXTRACT_OPERATING_QUANTITIES
                else sections
            )
            supported = set(
                _scope_field_ids(
                    task.chapter_task,
                    support_sections,
                    ownership_aware=ownership_aware,
                    selector_reasons=scope_reasons.get(
                        (task.chapter_task, scope.scope_id), ()
                    ),
                    supplemental_terms=(*scope.section_titles, *scope.anchor_terms),
                )
            )
            unsupported = sorted(set(scope.field_ids) - supported)
            if unsupported:
                raise ShadowEvidencePlanningError(
                    ShadowPlanningFailureCode.FIELD_CONTRACT_INVALID,
                    "refined scope assigns fields without a positive source signal: "
                    f"{scope.scope_id}:{unsupported}",
                    sample_id=sample_id,
                    chapter_task=task.chapter_task,
                )
            if (
                _chapter_owner_score(
                    task.chapter_task,
                    sections,
                    selector_reasons=scope_reasons.get(
                        (task.chapter_task, scope.scope_id), ()
                    ),
                    supplemental_terms=(*scope.section_titles, *scope.anchor_terms),
                )
                <= 0
            ):
                raise ShadowEvidencePlanningError(
                    ShadowPlanningFailureCode.FIELD_CONTRACT_INVALID,
                    f"refined scope has no chapter-owning Evidence: {scope.scope_id}",
                    sample_id=sample_id,
                    chapter_task=task.chapter_task,
                )
            _validate_table_context(
                sections,
                sample_id=sample_id,
                chapter_task=task.chapter_task,
            )
            planned_fields.update(scope.field_ids)
        missing = sorted(
            set(_REQUIRED_CHAPTER_FIELDS.get(task.chapter_task, ())) - planned_fields
        )
        if missing:
            raise ShadowEvidencePlanningError(
                ShadowPlanningFailureCode.FIELD_CONTRACT_INVALID,
                f"refined chapter has no owner for required fields: {missing}",
                sample_id=sample_id,
                chapter_task=task.chapter_task,
            )


def build_shadow_preparation_audit(
    plan: ShadowEvidencePlan,
    *,
    audit_id: str,
    prepared: dict[str, tuple[PreparedRequestScope, ...]],
) -> ShadowEvidencePreparationAudit:
    reports: list[ShadowPreparationReport] = []
    for report in plan.reports:
        selections = plan.scope_selections[report.sample_id]
        scopes = prepared.get(report.sample_id)
        if scopes is None or len(scopes) != len(selections):
            raise ValueError("shadow preparation audit requires every prepared scope")
        evidence_count = sum(len(scope.evidence_bundle) for scope in scopes)
        if any(
            evidence.evidence.report != scope.report
            or evidence.evidence.page not in {page.page for page in scope.page_contexts}
            for scope in scopes
            for evidence in scope.evidence_bundle
        ):
            raise ValueError("shadow prepared Evidence traceability mismatch")
        reports.append(
            ShadowPreparationReport(
                sample_id=report.sample_id,
                scope_count=len(selections),
                selected_page_count=len(
                    {page for selection in selections for page in selection.pages}
                ),
                evidence_count=evidence_count,
                pdf_artifact_hash=plan.pdf_artifact_hashes[report.sample_id],
                recovery_state=plan.recovery_states[report.sample_id],
                recovered_page_numbers=plan.recovered_page_numbers[report.sample_id],
            )
        )
    payload = {
        "schema_version": SHADOW_PREPARATION_AUDIT_SCHEMA,
        "audit_id": audit_id,
        "sample_manifest_hash": plan.sample_manifest_hash,
        "evidence_plan_hash": plan.plan_hash,
        "report_count": len(reports),
        "planned_report_count": len(reports),
        "total_scope_count": sum(item.scope_count for item in reports),
        "total_evidence_count": sum(item.evidence_count for item in reports),
        "evidence_traceability_rate": 1.0,
        "recovered_report_count": sum(
            bool(item.recovered_page_numbers) for item in reports
        ),
        "reports": tuple(reports),
        "created_at": _utc_now(),
        "production_authorization": PRODUCTION_AUTHORIZATION,
    }
    return ShadowEvidencePreparationAudit(
        **payload,
        audit_hash=_payload_hash(payload),
    )


def build_shadow_operating_evidence_ownership_audit(
    *,
    audit_id: str,
    baseline_plan: ShadowEvidencePlan,
    corrected_plan: ShadowEvidencePlan,
    corrected_prepared: Mapping[str, tuple[PreparedRequestScope, ...]],
    affected_scope_ids: set[tuple[str, str]],
) -> ShadowOperatingEvidenceOwnershipAudit:
    """Build a provider-free, hash-bound comparison of operating ownership."""

    if baseline_plan.sample_manifest_hash != corrected_plan.sample_manifest_hash:
        raise ValueError("ownership audit plans use different sample manifests")
    baseline_by_key = {
        (report.sample_id, scope.scope_id): scope
        for report in baseline_plan.reports
        for task in report.tasks
        if task.chapter_task == ChapterTask.EXTRACT_OPERATING_QUANTITIES
        for scope in task.request_scopes
    }
    corrected_by_key = {
        (scope.sample_id, scope.scope_id): scope
        for scopes in corrected_prepared.values()
        for scope in scopes
        if scope.chapter_task == ChapterTask.EXTRACT_OPERATING_QUANTITIES
    }
    missing = sorted(set(affected_scope_ids) - set(baseline_by_key))
    if missing:
        raise ValueError(f"ownership audit scope is missing from baseline: {missing}")
    rows: list[dict[str, Any]] = []
    for key in sorted(affected_scope_ids):
        baseline = baseline_by_key[key]
        corrected = corrected_by_key.get(key)
        if corrected is None:
            raise ValueError(
                f"ownership audit scope is missing from corrected preparation: {key}"
            )
        corrected_bindings = [
            {
                "evidence_id": item.evidence.evidence_id,
                "page": item.evidence.page,
                "field_id": item.field_id,
            }
            for item in corrected.evidence_bundle
        ]
        rows.append(
            {
                "sample_id": key[0],
                "scope_id": key[1],
                "baseline_field_ids": list(baseline.field_ids),
                "corrected_field_ids": list(corrected.field_ids),
                "baseline_pages": list(baseline.pages),
                "corrected_candidate_pages": list(corrected.candidate_pages),
                "evidence_ids": [
                    item.evidence.evidence_id for item in corrected.evidence_bundle
                ],
                "baseline_legacy_binding_count": len(baseline.field_ids)
                * len(corrected.evidence_bundle),
                "corrected_explicit_binding_count": sum(
                    item.field_id is not None for item in corrected.evidence_bundle
                ),
                "corrected_bindings": corrected_bindings,
                "field_set_changed": baseline.field_ids != corrected.field_ids,
            }
        )
    payload = {
        "schema_version": "company_profile_shadow_operating_evidence_ownership_audit.v1",
        "audit_id": audit_id,
        "sample_manifest_hash": baseline_plan.sample_manifest_hash,
        "baseline_plan_hash": baseline_plan.plan_hash,
        "corrected_plan_hash": corrected_plan.plan_hash,
        "affected_scope_count": len(rows),
        "affected_scopes": tuple(rows),
        "provider_calls": 0,
        "historical_artifacts_mutated": False,
        "production_paths_opened": (),
        "production_authorization": PRODUCTION_AUTHORIZATION,
        "created_at": _utc_now(),
    }
    return ShadowOperatingEvidenceOwnershipAudit(
        **payload,
        audit_hash=_payload_hash(payload),
    )


def build_shadow_owner_closure_correction_audit(
    *,
    audit_id: str,
    baseline_plan: ShadowEvidencePlan,
    corrected_plan: ShadowEvidencePlan,
    preparation_audit: ShadowEvidencePreparationAudit,
    regression_cases: Mapping[str, Any],
    owner_regression_closure_audit_hash: str,
) -> ShadowOwnerClosureCorrectionAudit:
    """Bind the current plan to the reviewed owner regressions without a provider."""

    fixture_payload = dict(regression_cases)
    fixture_hash = fixture_payload.pop("fixture_hash", None)
    if fixture_hash != _payload_hash(fixture_payload):
        raise ValueError("shadow owner-regression fixture hash mismatch")
    if baseline_plan.sample_manifest_hash != corrected_plan.sample_manifest_hash:
        raise ValueError("shadow owner-closure plans use different cohorts")
    if baseline_plan.pdf_artifact_hashes != corrected_plan.pdf_artifact_hashes:
        raise ValueError("shadow owner-closure plans use different PDF artifacts")
    if preparation_audit.evidence_plan_hash != corrected_plan.plan_hash:
        raise ValueError("shadow owner-closure preparation audit does not match the plan")
    if not re.fullmatch(r"[0-9a-f]{64}", owner_regression_closure_audit_hash):
        raise ValueError("shadow owner-regression closure audit hash is invalid")

    cases = regression_cases.get("cases")
    if not isinstance(cases, list) or len(cases) != 8:
        raise ValueError("shadow owner-closure audit requires eight reviewed cases")
    results: list[ShadowOwnerClosureResult] = []
    unresolved: list[str] = []
    for raw_case in cases:
        if not isinstance(raw_case, Mapping):
            raise TypeError("shadow owner-closure case must be an object")
        review_row_id = str(raw_case.get("review_row_id") or "")
        sample_id = str(raw_case.get("sample_id") or "")
        chapter_task = ChapterTask(str(raw_case.get("chapter_task") or ""))
        field_id = str(raw_case.get("field_id") or "")
        physical_page = int(raw_case.get("physical_page") or 0)
        expected_result = str(raw_case.get("expected_result") or "")
        report_plan = corrected_plan.report_by_id(sample_id)
        matching_scope_ids = tuple(
            scope.scope_id
            for task in report_plan.tasks
            if task.chapter_task == chapter_task
            for scope in task.request_scopes
            if field_id in scope.field_ids and physical_page in scope.pages
        )
        if expected_result in {"reject_legal_empty", "reject_cross_reference_only"}:
            passed = not matching_scope_ids
            status = "resolved"
        elif expected_result == "preserve_legal_empty":
            passed = bool(matching_scope_ids)
            status = "preserved"
        else:
            raise ValueError(
                f"unsupported shadow owner-closure expectation: {expected_result}"
            )
        if not passed:
            unresolved.append(review_row_id)
        results.append(
            ShadowOwnerClosureResult(
                review_row_id=review_row_id,
                sample_id=sample_id,
                chapter_task=chapter_task,
                field_id=field_id,
                physical_page=physical_page,
                expected_result=expected_result,
                matching_scope_ids=matching_scope_ids,
                status=status,
            )
        )
    if unresolved:
        raise ValueError(f"shadow owner-closure cases remain unresolved: {unresolved}")
    payload = {
        "schema_version": "company_profile_shadow_evidence_correction_audit.v1",
        "audit_id": audit_id,
        "sample_manifest_hash": corrected_plan.sample_manifest_hash,
        "baseline_plan_hash": baseline_plan.plan_hash,
        "corrected_plan_hash": corrected_plan.plan_hash,
        "preparation_audit_hash": preparation_audit.audit_hash,
        "owner_regression_fixture_hash": str(fixture_hash),
        "owner_regression_closure_audit_hash": owner_regression_closure_audit_hash,
        "results": tuple(results),
        "unresolved_finding_ids": (),
        "provider_calls": 0,
        "created_at": _utc_now(),
        "production_authorization": PRODUCTION_AUTHORIZATION,
    }
    return ShadowOwnerClosureCorrectionAudit(
        **payload,
        audit_hash=_payload_hash(payload),
    )


def build_shadow_routing_continuation_correction_audit(
    *,
    audit_id: str,
    baseline_plan: ShadowEvidencePlan,
    corrected_plan: ShadowEvidencePlan,
    preparation_audit: ShadowEvidencePreparationAudit,
    regression_cases: Mapping[str, Any],
    source_review_package_hash: str,
    source_review_outcomes_hash: str,
) -> ShadowRoutingContinuationCorrectionAudit:
    """Prove the three reviewed routing defects closed without a provider."""

    fixture_payload = dict(regression_cases)
    fixture_hash = fixture_payload.pop("fixture_hash", None)
    if fixture_hash != _payload_hash(fixture_payload):
        raise ValueError("routing/continuation fixture hash mismatch")
    if baseline_plan.sample_manifest_hash != corrected_plan.sample_manifest_hash:
        raise ValueError("routing/continuation plans use different cohorts")
    if baseline_plan.pdf_artifact_hashes != corrected_plan.pdf_artifact_hashes:
        raise ValueError("routing/continuation plans use different PDF artifacts")
    if preparation_audit.evidence_plan_hash != corrected_plan.plan_hash:
        raise ValueError("routing/continuation preparation audit plan mismatch")
    for value in (source_review_package_hash, source_review_outcomes_hash):
        if not re.fullmatch(r"[0-9a-f]{64}", value):
            raise ValueError("routing/continuation source review hash is invalid")
    inputs = regression_cases.get("inputs")
    if not isinstance(inputs, Mapping):
        raise TypeError("routing/continuation fixture inputs must be an object")
    expected_bindings = {
        "sample_manifest": corrected_plan.sample_manifest_hash,
        "baseline_plan": baseline_plan.plan_hash,
        "source_review_package": source_review_package_hash,
        "source_review_outcomes": source_review_outcomes_hash,
    }
    for name, expected_hash in expected_bindings.items():
        binding = inputs.get(name)
        if not isinstance(binding, Mapping):
            raise TypeError(f"routing/continuation fixture binding is invalid: {name}")
        key = "identity_hash" if name in {"sample_manifest", "baseline_plan"} else "sha256"
        if binding.get(key) != expected_hash:
            raise ValueError(f"routing/continuation fixture binding drift: {name}")

    cases = regression_cases.get("cases")
    if not isinstance(cases, list) or len(cases) != 6:
        raise ValueError("routing/continuation audit requires six closed cases")
    results: list[ShadowRoutingContinuationResult] = []
    unresolved: list[str] = []
    for raw_case in cases:
        if not isinstance(raw_case, Mapping):
            raise TypeError("routing/continuation case must be an object")
        review_row_id = str(raw_case.get("review_row_id") or "")
        sample_id = str(raw_case.get("sample_id") or "")
        chapter_task = ChapterTask(str(raw_case.get("chapter_task") or ""))
        field_id = str(raw_case.get("field_id") or "")
        expected_result = str(raw_case.get("expected_result") or "")
        physical_pages = tuple(int(page) for page in raw_case.get("physical_pages", ()))
        expected_page_hashes = {
            int(page): str(value)
            for page, value in dict(raw_case.get("page_hashes") or {}).items()
        }
        if set(expected_page_hashes) != set(physical_pages):
            raise ValueError(f"routing/continuation page hashes mismatch: {review_row_id}")

        def matching(
            plan: ShadowEvidencePlan,
            *,
            bound_sample_id: str = sample_id,
            bound_chapter_task: ChapterTask = chapter_task,
            bound_field_id: str = field_id,
            bound_physical_pages: tuple[int, ...] = physical_pages,
            bound_expected_result: str = expected_result,
        ) -> tuple[Any, ...]:
            report = plan.report_by_id(bound_sample_id)
            return tuple(
                scope
                for task in report.tasks
                if task.chapter_task == bound_chapter_task
                for scope in task.request_scopes
                if bound_field_id in scope.field_ids
                and (
                    any(page in scope.pages for page in bound_physical_pages)
                    if bound_expected_result == "reject_assignment"
                    else set(bound_physical_pages).issubset(scope.pages)
                )
            )

        baseline_matches = matching(baseline_plan)
        corrected_matches = matching(corrected_plan)
        observed_hashes = {
            int(page): page_hash
            for plan in (baseline_plan, corrected_plan)
            for selection in plan.scope_selections.get(sample_id, ())
            for page, page_hash in selection.page_hashes.items()
            if int(page) in physical_pages
        }
        if observed_hashes != expected_page_hashes:
            raise ValueError(f"routing/continuation source page drift: {review_row_id}")
        if expected_result == "reject_assignment":
            passed = bool(baseline_matches) and not corrected_matches
            status = "resolved"
        elif expected_result == "require_pages":
            passed = bool(corrected_matches)
            status = "resolved"
        elif expected_result == "preserve_assignment":
            passed = bool(baseline_matches) and bool(corrected_matches)
            status = "preserved"
        else:
            raise ValueError(
                f"unsupported routing/continuation expectation: {expected_result}"
            )
        if not passed:
            unresolved.append(review_row_id)
        results.append(
            ShadowRoutingContinuationResult(
                review_row_id=review_row_id,
                sample_id=sample_id,
                chapter_task=chapter_task,
                field_id=field_id,
                expected_result=expected_result,
                physical_pages=physical_pages,
                baseline_scope_ids=tuple(scope.scope_id for scope in baseline_matches),
                corrected_scope_ids=tuple(scope.scope_id for scope in corrected_matches),
                corrected_scope_pages=tuple(scope.pages for scope in corrected_matches),
                status=status,
            )
        )
    if unresolved:
        raise ValueError(f"routing/continuation cases remain unresolved: {unresolved}")
    payload = {
        "schema_version": SHADOW_ROUTING_CONTINUATION_AUDIT_SCHEMA,
        "audit_id": audit_id,
        "sample_manifest_hash": corrected_plan.sample_manifest_hash,
        "baseline_plan_hash": baseline_plan.plan_hash,
        "corrected_plan_hash": corrected_plan.plan_hash,
        "preparation_audit_hash": preparation_audit.audit_hash,
        "fixture_hash": str(fixture_hash),
        "source_review_package_hash": source_review_package_hash,
        "source_review_outcomes_hash": source_review_outcomes_hash,
        "report_count": preparation_audit.report_count,
        "results": tuple(results),
        "unresolved_finding_ids": (),
        "provider_calls": 0,
        "cohort_replay_performed": False,
        "historical_artifacts_mutated": False,
        "production_paths_opened": (),
        "created_at": _utc_now(),
        "production_authorization": PRODUCTION_AUTHORIZATION,
    }
    return ShadowRoutingContinuationCorrectionAudit(
        **payload,
        audit_hash=_payload_hash(payload),
    )


def build_shadow_scope_refinement_audit(
    *,
    audit_id: str,
    baseline_plan: ShadowEvidencePlan,
    refined_plan: ShadowEvidencePlan,
    baseline_prepared: dict[str, tuple[PreparedRequestScope, ...]],
    refined_prepared: dict[str, tuple[PreparedRequestScope, ...]],
) -> ShadowScopeRefinementAudit:
    """Compare two hash-bound plans without invoking or emulating a provider."""

    if baseline_plan.sample_manifest_hash != refined_plan.sample_manifest_hash:
        raise ValueError("scope refinement plans use different sample manifests")
    if baseline_plan.pdf_artifact_hashes != refined_plan.pdf_artifact_hashes:
        raise ValueError("scope refinement plans use different PDF artifacts")
    if baseline_plan.selected_section_hashes != refined_plan.selected_section_hashes:
        raise ValueError("scope refinement plans use different governed sections")
    if baseline_plan.recovery_states != refined_plan.recovery_states or (
        baseline_plan.recovered_page_numbers != refined_plan.recovered_page_numbers
    ):
        raise ValueError("scope refinement plans use different PDF recovery states")
    expected_ids = {report.sample_id for report in baseline_plan.reports}
    if (
        expected_ids != {report.sample_id for report in refined_plan.reports}
        or expected_ids != set(baseline_prepared)
        or expected_ids != set(refined_prepared)
    ):
        raise ValueError("scope refinement report identities mismatch")

    baseline_metrics = _prepared_scope_metrics(baseline_prepared)
    refined_metrics = _prepared_scope_metrics(refined_prepared)
    unsupported = _unsupported_prepared_assignments(refined_plan, refined_prepared)
    missing_required = _missing_required_field_owners(refined_plan)
    if unsupported:
        raise ValueError(
            f"refined plan has unsupported field assignments: {unsupported}"
        )
    if missing_required:
        raise ValueError(
            f"refined plan has missing required field owners: {missing_required}"
        )
    _validate_prepared_table_contexts(refined_prepared)

    chapter_metrics: dict[str, dict[str, int]] = {}
    for chapter in ChapterTask:
        baseline_chapter = _prepared_scope_metrics(
            _prepared_for_chapter(baseline_prepared, chapter)
        )
        refined_chapter = _prepared_scope_metrics(
            _prepared_for_chapter(refined_prepared, chapter)
        )
        chapter_metrics[chapter.value] = {
            "baseline_scope_count": baseline_chapter["scope_count"],
            "refined_scope_count": refined_chapter["scope_count"],
            "baseline_field_scope_pair_count": baseline_chapter[
                "field_scope_pair_count"
            ],
            "refined_field_scope_pair_count": refined_chapter["field_scope_pair_count"],
            "baseline_field_bound_evidence_copy_count": baseline_chapter[
                "field_bound_evidence_copy_count"
            ],
            "refined_field_bound_evidence_copy_count": refined_chapter[
                "field_bound_evidence_copy_count"
            ],
        }
    payload = {
        "schema_version": SHADOW_SCOPE_REFINEMENT_AUDIT_SCHEMA,
        "audit_id": audit_id,
        "sample_manifest_hash": baseline_plan.sample_manifest_hash,
        "baseline_plan_version": baseline_plan.plan_version,
        "baseline_plan_hash": baseline_plan.plan_hash,
        "refined_plan_version": refined_plan.plan_version,
        "refined_plan_hash": refined_plan.plan_hash,
        "report_count": len(expected_ids),
        "baseline_scope_count": baseline_metrics["scope_count"],
        "refined_scope_count": refined_metrics["scope_count"],
        "baseline_field_scope_pair_count": baseline_metrics["field_scope_pair_count"],
        "refined_field_scope_pair_count": refined_metrics["field_scope_pair_count"],
        "baseline_field_bound_evidence_copy_count": baseline_metrics[
            "field_bound_evidence_copy_count"
        ],
        "refined_field_bound_evidence_copy_count": refined_metrics[
            "field_bound_evidence_copy_count"
        ],
        "baseline_field_bound_evidence_character_count": baseline_metrics[
            "field_bound_evidence_character_count"
        ],
        "refined_field_bound_evidence_character_count": refined_metrics[
            "field_bound_evidence_character_count"
        ],
        "field_bound_evidence_copy_reduction_rate": _reduction_rate(
            baseline_metrics["field_bound_evidence_copy_count"],
            refined_metrics["field_bound_evidence_copy_count"],
        ),
        "field_bound_evidence_character_reduction_rate": _reduction_rate(
            baseline_metrics["field_bound_evidence_character_count"],
            refined_metrics["field_bound_evidence_character_count"],
        ),
        "unsupported_assignment_count": 0,
        "missing_required_owner_count": 0,
        "table_context_incomplete_count": 0,
        "evidence_traceability_rate": 1.0,
        "provider_calls": 0,
        "chapter_metrics": chapter_metrics,
        "created_at": _utc_now(),
        "production_authorization": PRODUCTION_AUTHORIZATION,
    }
    return ShadowScopeRefinementAudit(
        **payload,
        audit_hash=_payload_hash(payload),
    )


def _prepared_scope_metrics(
    prepared: dict[str, tuple[PreparedRequestScope, ...]],
) -> dict[str, int]:
    scopes = [scope for values in prepared.values() for scope in values]
    evidence_copies = 0
    evidence_characters = 0
    for scope in scopes:
        for field_id in scope.field_ids:
            for item in scope.evidence_bundle:
                bound = item.model_copy(update={"field_id": field_id})
                evidence_copies += 1
                evidence_characters += len(bound.model_dump_json())
    return {
        "scope_count": len(scopes),
        "field_scope_pair_count": sum(len(scope.field_ids) for scope in scopes),
        "field_bound_evidence_copy_count": evidence_copies,
        "field_bound_evidence_character_count": evidence_characters,
    }


def _prepared_for_chapter(
    prepared: dict[str, tuple[PreparedRequestScope, ...]],
    chapter: ChapterTask,
) -> dict[str, tuple[PreparedRequestScope, ...]]:
    return {
        sample_id: tuple(scope for scope in scopes if scope.chapter_task == chapter)
        for sample_id, scopes in prepared.items()
    }


def _unsupported_prepared_assignments(
    plan: ShadowEvidencePlan,
    prepared: dict[str, tuple[PreparedRequestScope, ...]],
) -> tuple[str, ...]:
    unsupported: list[str] = []
    for sample_id, scopes in prepared.items():
        report_plan = plan.report_by_id(sample_id)
        scope_plans = {
            (task.chapter_task, scope.scope_id): scope
            for task in report_plan.tasks
            for scope in task.request_scopes
        }
        scope_reasons = {
            (selection.chapter_task, selection.scope_id): selection.selector_reasons
            for selection in plan.scope_selections[sample_id]
        }
        for scope in scopes:
            scope_plan = scope_plans[(scope.chapter_task, scope.scope_id)]
            sections = tuple(
                SimpleNamespace(page_number=page.page, text=page.text)
                for page in scope.page_contexts
            )
            supported = set(
                _scope_field_ids(
                    scope.chapter_task,
                    sections,
                    selector_reasons=scope_reasons.get(
                        (scope.chapter_task, scope.scope_id), ()
                    ),
                    supplemental_terms=(
                        *scope_plan.section_titles,
                        *scope_plan.anchor_terms,
                    ),
                )
            )
            for field_id in scope.field_ids:
                if field_id not in supported:
                    unsupported.append(f"{sample_id}:{scope.scope_id}:{field_id}")
    return tuple(unsupported)


def _missing_required_field_owners(
    plan: ShadowEvidencePlan,
) -> tuple[str, ...]:
    missing: list[str] = []
    for report in plan.reports:
        for task in report.tasks:
            planned = {
                field_id
                for scope in task.request_scopes
                for field_id in scope.field_ids
            }
            for field_id in _REQUIRED_CHAPTER_FIELDS.get(task.chapter_task, ()):
                if field_id not in planned:
                    missing.append(
                        f"{report.sample_id}:{task.chapter_task.value}:{field_id}"
                    )
    return tuple(missing)


def _validate_prepared_table_contexts(
    prepared: dict[str, tuple[PreparedRequestScope, ...]],
) -> None:
    for sample_id, scopes in prepared.items():
        for scope in scopes:
            _validate_table_context(
                tuple(
                    SimpleNamespace(page_number=page.page, text=page.text)
                    for page in scope.page_contexts
                ),
                sample_id=sample_id,
                chapter_task=scope.chapter_task,
            )


def _reduction_rate(baseline: int, refined: int) -> float:
    return round((baseline - refined) / baseline, 6)


def _artifact_page_result(page: Any) -> SimpleNamespace:
    text = str(page.text or "")
    method = str(page.extraction_method or "none")
    usable = bool(text.strip()) and method in {"native_text", "alternate_native", "ocr"}
    quality = "governed_ocr" if method == "ocr" else "native"
    return SimpleNamespace(
        page_number=page.page_number,
        selected_usable_for_semantic=usable,
        selected_text=text,
        selected_method=method,
        quality_status=quality,
    )


def _load_frozen_artifact(
    *,
    report: Any,
    native_artifact_path: Path,
    expected_artifact_hash: str,
) -> Any | None:
    store = BusinessProfilePdfArtifactStore()
    pattern = f"{report.content_hash}_*.json.gz"
    for candidate in sorted(native_artifact_path.parent.glob(pattern)):
        try:
            payload = store.read(candidate)
            if payload.get("artifact_hash") != expected_artifact_hash:
                continue
            return store.read_artifact(
                candidate,
                source_file_id=report.asset_id,
                source_pdf_path=str(report.local_path),
                expected_content_hash=report.content_hash,
            )
        except (OSError, RuntimeError, ValueError):
            continue
    return None


def write_shadow_evidence_artifact(
    path: str | Path,
    value: (
        ShadowEvidencePlan
        | ShadowEvidencePreparationAudit
        | ShadowOperatingEvidenceOwnershipAudit
        | ShadowScopeRefinementAudit
        | ShadowOwnerClosureCorrectionAudit
        | ShadowRoutingContinuationCorrectionAudit
    ),
) -> None:
    destination = Path(path)
    content = (
        json.dumps(
            value.model_dump(mode="json"),
            ensure_ascii=False,
            indent=2,
        )
        + "\n"
    )
    if destination.exists():
        if destination.read_text(encoding="utf-8") != content:
            raise RuntimeError(
                f"immutable shadow Evidence artifact mismatch: {destination}"
            )
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.part")
    temporary.write_text(content, encoding="utf-8")
    try:
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def load_shadow_evidence_plan(path: str | Path) -> ShadowEvidencePlan:
    source = Path(path).read_text(encoding="utf-8")
    raw = json.loads(source)
    prohibited = _find_keys(raw)
    if prohibited:
        raise ShadowEvidencePlanningError(
            ShadowPlanningFailureCode.PLAN_INVALID,
            f"shadow Evidence plan contains prohibited keys: {sorted(prohibited)}",
        )
    try:
        return ShadowEvidencePlan.model_validate_json(source)
    except ValueError as exc:
        raise ShadowEvidencePlanningError(
            ShadowPlanningFailureCode.PLAN_INVALID,
            f"shadow Evidence plan is invalid: {exc}",
        ) from exc


def load_shadow_preparation_audit(
    path: str | Path,
) -> ShadowEvidencePreparationAudit:
    try:
        return ShadowEvidencePreparationAudit.model_validate_json(
            Path(path).read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise ShadowEvidencePlanningError(
            ShadowPlanningFailureCode.PLAN_INVALID,
            f"shadow preparation audit is invalid: {exc}",
        ) from exc


def load_shadow_scope_refinement_audit(
    path: str | Path,
) -> ShadowScopeRefinementAudit:
    try:
        return ShadowScopeRefinementAudit.model_validate_json(
            Path(path).read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise ShadowEvidencePlanningError(
            ShadowPlanningFailureCode.PLAN_INVALID,
            f"shadow scope refinement audit is invalid: {exc}",
        ) from exc


def _section_matches(
    section: Any,
    allowed_keys: tuple[str, ...],
    hint_terms: tuple[str, ...],
) -> bool:
    if section.section_key in allowed_keys:
        return True
    for reason in section.selector_reasons:
        if reason.startswith("heading_alias:"):
            key = reason.split(":", 2)[1]
            if key in allowed_keys:
                return True
        if reason.startswith("structured_hint:"):
            term = reason.split(":", 1)[1]
            if term in hint_terms:
                return True
    return False


def _validate_table_context(
    sections: Sequence[Any],
    *,
    sample_id: str,
    chapter_task: ChapterTask,
) -> None:
    pages = {item.page_number for item in sections}
    for section in sections:
        normalized = section.text.replace(" ", "")
        missing_previous = "续表" in normalized and section.page_number - 1 not in pages
        requires_next = any(term in normalized for term in ("续下表", "接下页"))
        missing_next = requires_next and section.page_number + 1 not in pages
        if missing_previous or missing_next:
            raise ShadowEvidencePlanningError(
                ShadowPlanningFailureCode.TABLE_CONTEXT_INCOMPLETE,
                f"selected table continuation context is incomplete at page {section.page_number}",
                sample_id=sample_id,
                chapter_task=chapter_task,
            )


def _continuous_ranges(
    pages: Sequence[int],
    *,
    maximum: int,
) -> tuple[tuple[int, ...], ...]:
    ranges: list[list[int]] = []
    for page in pages:
        if not ranges or page != ranges[-1][-1] + 1 or len(ranges[-1]) >= maximum:
            ranges.append([page])
        else:
            ranges[-1].append(page)
    return tuple(tuple(values) for values in ranges)


def _select_scope_ranges(
    sections: Sequence[Any],
    *,
    direct_pages: set[int],
    bounded_pages: Sequence[int],
    maximum_scopes: int,
    chapter_task: ChapterTask,
    ownership_aware: bool = False,
) -> tuple[tuple[int, ...], ...]:
    sections_by_page = {item.page_number: item for item in sections}
    if chapter_task == ChapterTask.EXTRACT_SEGMENT_FINANCIALS:
        # Segment tables are row-owned by the selector's direct pages. Do not
        # inherit the generic bounded ±1-page context: the following page is
        # often a different table and can materially confuse a high-cardinality
        # partition request. Explicit continuation markers are bound below by
        # _bind_table_context_range.
        governed_pages = tuple(sorted(direct_pages))
    else:
        governed_pages = tuple(
            page
            for page in bounded_pages
            if not (
                chapter_task == ChapterTask.EXTRACT_BUSINESS_OVERVIEW
                and (
                    _business_overview_cross_reference_only(
                        str(sections_by_page[page].text)
                    )
                    or _subsidiary_financial_table_without_issuer_overview(
                        str(sections_by_page[page].text)
                    )
                    or _business_overview_statistical_table_only(
                        str(sections_by_page[page].text)
                    )
                )
            )
        )

    ranges = _continuous_ranges(governed_pages, maximum=3)

    def score(pages: tuple[int, ...]) -> tuple[int, int, int]:
        scoped_sections = [sections_by_page[page] for page in pages]
        support_sections = (
            [item for item in scoped_sections if item.page_number in direct_pages]
            if ownership_aware
            and chapter_task == ChapterTask.EXTRACT_OPERATING_QUANTITIES
            else scoped_sections
        )
        supported_fields = _scope_field_ids(
            chapter_task, support_sections, ownership_aware=ownership_aware
        )
        reasons = {
            reason
            for page in pages
            for reason in sections_by_page[page].selector_reasons
        }
        value = sum(
            100
            if reason.startswith("table_signature:")
            else 40
            if reason.startswith("structured_hint:")
            else 20
            if reason.startswith("heading_alias:")
            else 1
            for reason in reasons
        )
        value += 10 * sum(page in direct_pages for page in pages)
        value += _chapter_owner_score(chapter_task, scoped_sections)
        return len(supported_fields), value, -pages[0]

    supported = [pages for pages in ranges if score(pages)[0] > 0]
    chosen = sorted(supported, key=score, reverse=True)[:maximum_scopes]
    return tuple(sorted(chosen, key=lambda pages: pages[0]))


def _scope_field_ids(
    chapter_task: ChapterTask,
    sections: Sequence[Any],
    *,
    selector_reasons: Sequence[str] = (),
    supplemental_terms: Sequence[str] = (),
    ownership_aware: bool = False,
) -> tuple[str, ...]:
    """Return the stable existing field subset supported by this source scope."""

    chapter_fields = _CHAPTER_CONFIG[chapter_task][0]
    compact = re.sub(r"\s+", "", "\n".join(str(item.text) for item in sections))
    if ownership_aware and chapter_task == ChapterTask.EXTRACT_OPERATING_QUANTITIES:
        return _operating_field_ids_from_text(compact)

    section_keys = {str(getattr(item, "section_key", "")) for item in sections} | {
        str(term) for term in supplemental_terms
    }
    reasons = {
        str(reason)
        for item in sections
        for reason in getattr(item, "selector_reasons", ())
    } | {str(reason) for reason in selector_reasons}
    matched: set[str] = set()
    for field_id in chapter_fields:
        patterns = _FIELD_TEXT_PATTERNS[chapter_task].get(field_id, ())
        key_signals = _FIELD_SECTION_KEYS.get(chapter_task, {}).get(field_id, ())
        reason_terms = _FIELD_REASON_TERMS.get(chapter_task, {}).get(field_id, ())
        if (
            any(re.search(pattern, compact) for pattern in patterns)
            or bool(section_keys & set(key_signals))
            or any(term in reason for reason in reasons for term in reason_terms)
        ):
            matched.add(field_id)
    if chapter_task == ChapterTask.EXTRACT_SEGMENT_FINANCIALS:
        if not _has_governed_segment_owner(compact, section_keys):
            matched.clear()
        elif "segment_dimension" not in matched:
            matched -= {
                "operating_revenue",
                "operating_cost",
                "gross_margin_reported",
            }
    if (
        chapter_task == ChapterTask.EXTRACT_OPERATING_QUANTITIES
        and (
            re.search(
                r"产能(?:与开工)?情况.{0,60}(?:[√☑]不适用|不适用)",
                compact,
            )
            or re.search(_BSE_INDUSTRY_DISCLOSURE_OPT_OUT_PATTERN, compact)
        )
        and not re.search(
            r"(?:生产量|销售量|库存量|实际产量|加工量|处理量|吞吐量)"
            r".{0,100}\d[\d,]*(?:\.\d+)?",
            compact,
        )
    ):
        matched &= {"production_capacity"}
    return tuple(field_id for field_id in chapter_fields if field_id in matched)


def _operating_field_ids_from_text(compact: str) -> tuple[str, ...]:
    """Return operating fields supported by source text, without selector metadata."""

    chapter_task = ChapterTask.EXTRACT_OPERATING_QUANTITIES
    chapter_fields = _CHAPTER_CONFIG[chapter_task][0]
    matched = {
        field_id
        for field_id in chapter_fields
        if any(
            re.search(pattern, compact)
            for pattern in _FIELD_TEXT_PATTERNS[chapter_task].get(field_id, ())
        )
    }
    if re.search(_OPERATING_QUANTITY_LEGAL_EMPTY_PATTERN, compact):
        matched.update({"production_volume", "sales_volume", "inventory_volume"})
    capacity_legal_empty = bool(
        re.search(
            r"产能(?:与开工)?情况.{0,80}(?:[√☑](?:否|不适用)|不适用)",
            compact,
        )
        or re.search(_BSE_INDUSTRY_DISCLOSURE_OPT_OUT_PATTERN, compact)
    )
    if capacity_legal_empty:
        matched.add("production_capacity")
    if capacity_legal_empty and not re.search(
        r"(?:生产量|销售量|库存量|实际产量|加工量|处理量|吞吐量)"
        r".{0,100}\d[\d,]*(?:\.\d+)?",
        compact,
    ):
        matched &= {"production_capacity"}
    return tuple(field_id for field_id in chapter_fields if field_id in matched)


def _bind_shadow_operating_evidence(
    scope: PreparedRequestScope,
) -> PreparedRequestScope:
    """Bind direct operating pages while retaining adjacent pages as context only."""

    if scope.chapter_task != ChapterTask.EXTRACT_OPERATING_QUANTITIES:
        return scope
    contexts = {item.page: item for item in scope.page_contexts}
    bound: list[Any] = []
    for item in scope.evidence_bundle:
        page = item.evidence.page
        if page not in scope.candidate_pages:
            bound.append(item.model_copy(update={"field_id": None}))
            continue
        context = contexts.get(page)
        supported = (
            set(_operating_field_ids_from_text(re.sub(r"\s+", "", context.text)))
            if context is not None
            else set()
        ) & set(scope.field_ids)
        if not supported:
            bound.append(item.model_copy(update={"field_id": None}))
            continue
        bound.extend(
            item.model_copy(update={"field_id": field_id})
            for field_id in scope.field_ids
            if field_id in supported
        )
    return scope.model_copy(update={"evidence_bundle": tuple(bound)})


def _bind_table_context_range(
    pages: tuple[int, ...],
    sections: Sequence[Any],
    *,
    sample_id: str,
    chapter_task: ChapterTask,
) -> tuple[int, ...]:
    sections_by_page = {item.page_number: item for item in sections}
    available = set(sections_by_page)
    required = set(pages)
    changed = True
    while changed:
        changed = False
        for page in tuple(sorted(required)):
            normalized = re.sub(r"\s+", "", sections_by_page[page].text)
            if "续表" in normalized and page - 1 not in required:
                if page - 1 not in available:
                    _raise_table_context_incomplete(
                        sample_id=sample_id,
                        chapter_task=chapter_task,
                        page=page,
                    )
                required.add(page - 1)
                changed = True
            if any(term in normalized for term in ("续下表", "接下页")) and (
                page + 1 not in required
            ):
                if page + 1 not in available:
                    _raise_table_context_incomplete(
                        sample_id=sample_id,
                        chapter_task=chapter_task,
                        page=page,
                    )
                required.add(page + 1)
                changed = True
    bounded = tuple(sorted(required))
    if len(bounded) > 3 or any(right != left + 1 for left, right in pairwise(bounded)):
        _raise_table_context_incomplete(
            sample_id=sample_id,
            chapter_task=chapter_task,
            page=min(bounded),
        )
    _validate_table_context(
        [sections_by_page[page] for page in bounded],
        sample_id=sample_id,
        chapter_task=chapter_task,
    )
    return bounded


def _raise_table_context_incomplete(
    *,
    sample_id: str,
    chapter_task: ChapterTask,
    page: int,
) -> None:
    raise ShadowEvidencePlanningError(
        ShadowPlanningFailureCode.TABLE_CONTEXT_INCOMPLETE,
        f"selected table continuation context is incomplete at page {page}",
        sample_id=sample_id,
        chapter_task=chapter_task,
    )


def _anchor_term(
    sections: Sequence[Any],
    allowed_keys: tuple[str, ...],
    hint_terms: tuple[str, ...],
) -> str:
    combined = "\n".join(item.text for item in sections)
    for section in sections:
        for reason in section.selector_reasons:
            if reason.startswith("heading_alias:"):
                _, key, term = reason.split(":", 2)
                if key in allowed_keys and term in combined:
                    return term
            if reason.startswith("structured_hint:"):
                term = reason.split(":", 1)[1]
                if term in hint_terms and term in combined:
                    return term
    for term in hint_terms:
        if term in combined:
            return term
    for line in combined.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped[:120]
    raise ValueError("selected chapter Evidence contains no anchor text")


def _matched_headers(
    sections: Sequence[Any],
    templates: Sequence[ResolvedDisclosureTemplate],
    combined: str,
) -> tuple[str, ...]:
    signature_ids = {
        reason.split(":", 1)[1]
        for section in sections
        for reason in section.selector_reasons
        if reason.startswith("table_signature:")
    }
    headers: list[str] = []
    for resolved in templates:
        for signature in resolved.template.table_signatures:
            if signature.signature_id not in signature_ids:
                continue
            for header in signature.required_headers:
                if header in combined and header not in headers:
                    headers.append(header)
    return tuple(headers)


def _source_units(text: str) -> tuple[str, ...]:
    matches = re.findall(r"单位\s*[:：]\s*([^\s，。；|]{1,16})", text)
    return tuple(dict.fromkeys(item.strip() for item in matches if item.strip()))


def _source_footnotes(text: str) -> tuple[str, ...]:
    matches = re.findall(r"(?:^|\n)(注[:：][^\n]{1,100})", text)
    return tuple(dict.fromkeys(item.strip() for item in matches if item.strip()))


def _printed_labels(artifact: Any, pages: Sequence[int]) -> dict[str, str]:
    wanted = set(pages)
    return {
        str(page.page_number): str(page.printed_page_label)
        for page in artifact.pages
        if page.page_number in wanted and page.printed_page_label
    }


def _find_keys(value: Any) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            if key in _PROHIBITED_KEYS:
                found.add(key)
            found.update(_find_keys(item))
    elif isinstance(value, list):
        for item in value:
            found.update(_find_keys(item))
    return found
