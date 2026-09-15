from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from pydantic import TypeAdapter

from research.company_profile import ChapterTask
from research.company_profile.contracts import (
    CompanyProfileTaskResult,
    Disposition,
    DispositionStatus,
)
from research.company_profile.core_evidence_selection import select_core_evidence
from research.company_profile.core_skeleton import (
    COMMON_CORE_CHAPTERS,
    form_core_skeleton,
    select_activated_chapters,
)
from research.company_profile.models import ReportIdentity, SemanticRecord

ROOT = Path(__file__).resolve().parents[3]
REFERENCE_INPUT = (
    ROOT / "tests/fixtures/company_profile_stage4/reference_profile_input.json"
)
RECORD_ADAPTER = TypeAdapter(SemanticRecord)

MANUFACTURING_OVERVIEW = (
    "公司主要从事动力电池、储能电池的研发、生产和销售，"
    "主要产品为动力电池系统，通过向客户销售电池系统取得货款。"
)
SERVICE_OVERVIEW = (
    "公司主要从事软件开发和信息技术服务。"
    "主要服务包括系统集成、运维和技术咨询，"
    "通过向客户提供技术服务收取服务费。"
)
BANK_OVERVIEW = (
    "公司主营商业银行业务。"
    "主要服务包括公司金融、零售银行和金融市场业务。"
    "营业收入主要来源于利息净收入和手续费及佣金收入。"
)


def _reference_payload():
    return json.loads(REFERENCE_INPUT.read_text(encoding="utf-8"))


def _record(payload):
    return RECORD_ADAPTER.validate_json(json.dumps(payload, ensure_ascii=False))


def _report(**overrides) -> ReportIdentity:
    payload = _reference_payload()["report"]
    payload.update(overrides)
    return ReportIdentity.model_validate(payload)


def _overview_record(report: ReportIdentity, source_text: str, record_id: str):
    item = deepcopy(
        next(
            row
            for row in _reference_payload()["records"]
            if row["record_id"] == "cp-300750-overview"
        )
    )
    item["record_id"] = record_id
    item["report"] = json.loads(report.model_dump_json())
    item["source_text"] = source_text
    item["evidence"][0]["report"] = item["report"]
    item["evidence"][0]["anchor"]["bounded_quote"] = source_text
    return _record(item)


def _revenue_row(report: ReportIdentity, record_id: str, label: str):
    item = deepcopy(
        next(
            row
            for row in _reference_payload()["records"]
            if row["record_id"] == "cp-300750-revenue"
        )
    )
    item["record_id"] = record_id
    item["report"] = json.loads(report.model_dump_json())
    item["measured_object"] = label
    item["segment_dimension"] = None
    item["segment_label"] = None
    item["source_native"] = {
        "name": label,
        "value": "1",
        "unit": "千元",
        "header": "余额",
    }
    item["evidence"][0]["report"] = item["report"]
    item["evidence"][0]["anchor"] = {
        "anchor_type": "table",
        "row_label": label,
        "column_header": "余额",
        "cell_locator": f"page80/{label}/余额",
    }
    return _record(item)


def _accepted(report: ReportIdentity, records):
    return CompanyProfileTaskResult(
        request_id="core-skeleton",
        records=tuple(records),
        dispositions=tuple(
            Disposition(
                target_id=record.record_id,
                field_id=record.field_id,
                status=DispositionStatus.ACCEPTED_FOR_REVIEW,
            )
            for record in records
        ),
        coverage=(),
        human_review_items=(),
        task_complete=True,
    )


def _overview_page(text: str, page: int = 14) -> dict[str, object]:
    return {
        "page": page,
        "text": f"报告期内公司从事的主要业务\n{text}\n二、风险因素\n宏观经济波动。",
    }


def test_common_core_does_not_add_tasks_or_require_manufacturing_quantities():
    assert COMMON_CORE_CHAPTERS == (
        ChapterTask.EXTRACT_BUSINESS_OVERVIEW,
        ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
    )
    assert [item.value for item in ChapterTask] == [
        "extract_business_overview",
        "extract_segment_financials",
        "extract_operating_quantities",
        "extract_material_inputs",
        "extract_counterparties_and_concentration",
        "extract_business_regime",
    ]


def test_manufacturing_disclosure_forms_core_and_keeps_quantities_as_enhancement():
    report = _report(report_id="asset-manufacturing-skeleton")
    overview = _overview_record(report, MANUFACTURING_OVERVIEW, "mfg-overview")
    result = form_core_skeleton(
        report=report,
        pages=(
            _overview_page(MANUFACTURING_OVERVIEW),
            {
                "page": 25,
                "text": (
                    "占公司营业收入或营业利润10%以上\n"
                    "分产品\n动力电池系统 营业收入 316506369 千元\n"
                    "三、主要销售客户"
                ),
            },
            {
                "page": 49,
                "text": (
                    "主要产品的产销量情况\n"
                    "产品 生产量 销售量 库存量\n"
                    "动力电池 100 90 10"
                ),
            },
        ),
        task_results=(_accepted(report, [overview]),),
    )

    assert result.assessment.core_complete is True
    assert result.assessment.principal_business.answered is True
    assert result.assessment.products_services.answered is True
    assert result.assessment.revenue_model.answered is True
    assert {item.chapter_task for item in result.evidence.spans} <= {
        "extract_business_overview",
        "extract_segment_financials",
    }
    quantity = next(
        item
        for item in result.activated_chapters
        if item.chapter_task == ChapterTask.EXTRACT_OPERATING_QUANTITIES
    )
    assert quantity.role == "enhancement"
    assert quantity.status == "activated"
    overview_task = next(
        item
        for item in result.activated_chapters
        if item.chapter_task == ChapterTask.EXTRACT_BUSINESS_OVERVIEW
    )
    assert overview_task.role == "common_core"
    assert overview_task.status == "activated"


def test_service_disclosure_forms_core_without_production_inventory():
    report = _report(report_id="asset-service-skeleton")
    overview = _overview_record(report, SERVICE_OVERVIEW, "svc-overview")
    result = form_core_skeleton(
        report=report,
        pages=(_overview_page(SERVICE_OVERVIEW),),
        task_results=(_accepted(report, [overview]),),
    )

    assert result.assessment.core_complete is True
    quantity = next(
        item
        for item in result.activated_chapters
        if item.chapter_task == ChapterTask.EXTRACT_OPERATING_QUANTITIES
    )
    assert quantity.status == "not_applicable"
    assert {item.chapter_task for item in result.evidence.spans} == {
        "extract_business_overview"
    }


def test_bank_disclosure_forms_core_and_does_not_activate_manufacturing_quantities():
    report = _report(report_id="asset-bank-skeleton")
    overview = _overview_record(report, BANK_OVERVIEW, "bank-overview")
    result = form_core_skeleton(
        report=report,
        pages=(
            _overview_page(BANK_OVERVIEW),
            {
                "page": 80,
                "text": (
                    "贷款按行业分布\n"
                    "制造业 100000\n批发和零售业 80000\n"
                    "房地产业 60000\n租赁和商务服务业 40000"
                ),
            },
        ),
        task_results=(_accepted(report, [overview]),),
    )

    assert result.assessment.core_complete is True
    quantity = next(
        item
        for item in result.activated_chapters
        if item.chapter_task == ChapterTask.EXTRACT_OPERATING_QUANTITIES
    )
    assert quantity.role == "enhancement"
    assert quantity.status == "not_applicable"
    assert "extract_operating_quantities" not in {
        item.chapter_task for item in result.evidence.spans
    }


def test_many_bank_table_rows_are_not_a_complete_principal_business():
    report = _report(report_id="asset-bank-rows")
    rows = [
        _revenue_row(report, f"loan-{index}", f"行业{index}")
        for index in range(30)
    ]
    result = form_core_skeleton(
        report=report,
        pages=(
            {
                "page": 80,
                "text": "贷款按行业分布\n" + "\n".join(
                    f"行业{index} 余额 {index}" for index in range(30)
                ),
            },
        ),
        task_results=(_accepted(report, rows),),
    )

    assert result.assessment.principal_business.answered is False
    assert result.assessment.core_complete is False
    quantity = next(
        item
        for item in result.activated_chapters
        if item.chapter_task == ChapterTask.EXTRACT_OPERATING_QUANTITIES
    )
    assert quantity.status == "not_applicable"


def _chapter(chapters, task: ChapterTask):
    return next(item for item in chapters if item.chapter_task == task)


def test_toc_does_not_activate_segment_or_quantity_against_evidence():
    pages = (
        {
            "page": 3,
            "text": "目录：“分部信息120、主要产品的产销量情况35”",
        },
    )
    selected = select_core_evidence(report=_report(), pages=pages)
    chapters = select_activated_chapters(pages)

    assert selected.spans == ()
    assert _chapter(chapters, ChapterTask.EXTRACT_SEGMENT_FINANCIALS).status == (
        "not_applicable"
    )
    assert _chapter(chapters, ChapterTask.EXTRACT_OPERATING_QUANTITIES).status == (
        "not_applicable"
    )


def test_risk_body_and_explicit_negation_do_not_activate_quantities():
    risk = select_activated_chapters(
        (
            {
                "page": 40,
                "text": "主要产品产销量下降可能影响收入",
            },
        )
    )
    negated = select_activated_chapters(
        (
            {
                "page": 49,
                "text": "公司实物销售收入是否大于劳务收入：否",
            },
        )
    )
    negated_section = select_activated_chapters(
        (
            {
                "page": 49,
                "text": "公司实物销售收入是否大于劳务收入\n否",
            },
        )
    )

    assert _chapter(risk, ChapterTask.EXTRACT_OPERATING_QUANTITIES).status == (
        "not_applicable"
    )
    assert _chapter(negated, ChapterTask.EXTRACT_OPERATING_QUANTITIES).status == (
        "not_applicable"
    )
    assert _chapter(negated, ChapterTask.EXTRACT_OPERATING_QUANTITIES).reason == (
        "explicit_negation"
    )
    assert _chapter(
        negated_section, ChapterTask.EXTRACT_OPERATING_QUANTITIES
    ).status == "not_applicable"
    assert _chapter(
        negated_section, ChapterTask.EXTRACT_OPERATING_QUANTITIES
    ).reason == "explicit_negation"


def test_disabled_enhancements_are_not_activated_even_when_disclosed():
    chapters = select_activated_chapters(
        (
            {
                "page": 30,
                "text": (
                    "主要原材料及能源\n正极材料采购\n"
                    "前五名客户的销售情况\n客户A\n"
                    "报告期内主营业务未发生重大变化"
                ),
            },
        )
    )

    for task in (
        ChapterTask.EXTRACT_MATERIAL_INPUTS,
        ChapterTask.EXTRACT_COUNTERPARTIES_AND_CONCENTRATION,
        ChapterTask.EXTRACT_BUSINESS_REGIME,
    ):
        item = _chapter(chapters, task)
        assert item.status == "not_activated"
        assert item.reason == "not_enabled_in_this_slice"


def test_checked_yes_is_not_quantity_negation():
    checked_yes = select_activated_chapters(
        (
            {
                "page": 49,
                "text": "公司实物销售收入是否大于劳务收入\n☑是 □否",
            },
        )
    )
    item = _chapter(checked_yes, ChapterTask.EXTRACT_OPERATING_QUANTITIES)
    assert item.status == "activated"
    assert item.reason == "owned_heading"

    later_heading = select_activated_chapters(
        (
            {
                "page": 49,
                "text": "公司实物销售收入是否大于劳务收入\n☑是 □否",
            },
            {
                "page": 50,
                "text": "主要产品的产销量情况\n产品 生产量 销售量 库存量",
            },
        )
    )
    later = _chapter(later_heading, ChapterTask.EXTRACT_OPERATING_QUANTITIES)
    assert later.status == "activated"
    assert later.reason == "owned_heading"


def test_checked_no_remains_negation_unless_another_page_owns_quantity():
    checked_no = _chapter(
        select_activated_chapters(
            (
                {
                    "page": 49,
                    "text": "公司实物销售收入是否大于劳务收入\n□是 ☑否",
                },
            )
        ),
        ChapterTask.EXTRACT_OPERATING_QUANTITIES,
    )
    assert checked_no.status == "not_applicable"
    assert checked_no.reason == "explicit_negation"

    later_heading = _chapter(
        select_activated_chapters(
            (
                {
                    "page": 49,
                    "text": "公司实物销售收入是否大于劳务收入\n□是 ☑否",
                },
                {
                    "page": 50,
                    "text": "主要产品的产销量情况\n产品 生产量 销售量 库存量",
                },
            )
        ),
        ChapterTask.EXTRACT_OPERATING_QUANTITIES,
    )
    assert later_heading.status == "activated"
    assert later_heading.reason == "owned_heading"
