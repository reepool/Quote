from __future__ import annotations

import asyncio
import json

from research.business_profile_async_production import (
    WORK_STAGES,
    BusinessProfileAsyncProductionService,
    BusinessProfileWorkRepository,
    StageBudget,
    get_business_profile_write_coordinator,
)
from research.company_profile.contracts import (
    CompanyProfileTaskResult,
    ContractErrorCode,
    Disposition,
    DispositionStatus,
    PreparedEvidence,
)
from research.company_profile.core_assessment_projection import (
    overview_dimension_hits,
    project_core_assessment,
)
from research.company_profile.core_evidence_selection import (
    project_owned_page_facts,
    select_core_evidence,
)
from research.company_profile.execution import (
    EMPTY_DELIVERY_PROCESSING_IDENTITY,
    OWNED_PAGE_FACTS_V1_IDENTITY,
    OWNED_PAGE_FACTS_V2_IDENTITY,
    OWNED_PAGE_FACTS_V3_IDENTITY,
    OWNED_PAGE_FACTS_V4_IDENTITY,
    OWNED_PAGE_FACTS_V5_IDENTITY,
    default_processing_identity,
)
from research.company_profile.models import (
    PRODUCTION_AUTHORIZATION,
    ChapterTask,
    SubjectBasis,
    SubjectScope,
)
from research.company_profile.operations import CompanyProfileTaskService
from research.company_profile.runtime import (
    CompanyProfileResearchWriter,
    CompanyProfileStageRuntime,
    _semantic_request,
)
from research.company_profile.workflow import _candidate_issue
from tests.unit.test_research.test_business_profile_async_production import _frontier
from tests.unit.test_research.test_business_profile_exposure_components import _storage
from tests.unit.test_research.test_business_profile_production_operations import (
    _announcement,
)
from tests.unit.test_research.test_company_profile_runtime import (
    _drive,
    _item,
    _report,
)

AVIC_OVERVIEW = (
    "一、报告期内公司从事的主要业务\n"
    "（一）主要业务、主要产品及其用途\r\n"
    "报告期内，公司主营业务为航空产品研发、制造、销售、维修与服务保障，主要产品包括航空防务\r\n"
    "装备、民用航空产品和智能测控产品。\r\n"
    "与依法取得军品出口经营权、并在核定的经营范围内从事军品出口经营活动的军\r\n"
    "贸公司共同合作，公司进行产品的研发、生产、技术服务等。\n"
    "二、风险因素\n宏观风险。"
)
AVIC_SEGMENT = (
    "（1） 营业收入构成\r\n"
    "单位：元\r\n"
    "分行业\r\n"
    "航空制造业 73,551,376,777.\r\n"
    "21 97.60% 63,195,455,418.\r\n"
    "89 97.14% 16.39%\r\n"
    "分产品\r\n"
    "航空产品 73,551,376,777.\r\n"
    "21 97.60% 63,195,455,418.\r\n"
    "89 97.14% 16.39%\r\n"
    "分地区\r\n"
    "国内 74,034,112,060.\r\n"
    "93 98.24%\r\n"
    "分销售模式\r\n"
    "直销 75,358,958,001.\r\n"
    "86 100.00%\r\n"
    "营业收入合计 75,358,958,001.\r\n"
    "86 100%\r\n"
    "三、主要销售客户"
)
SPDB_TITLE_WRAPPED = (
    "3.6 公司主要业务情况\r\n"
    "公司致力于为客户提\r\n"
    "供全面而专业的金融服务，涵盖商业信贷、交易银行、投资银行、电子银行、跨境业务、离岸业务等多\r\n"
    "个领域。"
)
SPDB_SCOPE = (
    "经营范围 银行业务；证券投资基金托管；"
    "公募证券投资基金销售；经批准的其它业务。\n"
    "注册资本 293.52亿元"
)
SPDB_TITLE = (
    "3.6 公司主要业务情况\n"
    "公司致力于为客户提供全面而专业的金融服务，"
    "涵盖商业信贷、交易银行、投资银行、电子银行、"
    "跨境业务、离岸业务等多个领域。\n"
    "二、风险因素\n宏观风险。"
)
SPDB_INCOME = (
    "3.7 利润表分析\r\n"
    "报告期内，本集团各项业务持续发展，实现营业收入 1,739.64 亿元。\r\n"
    "单位：人民币百万元\r\n"
    "项目 报告期 上年同期\r\n"
    "营业收入 173,964 170,748\r\n"
    "利息净收入 120,483 114,717\r\n"
    "3.7.1 营业收入\r\n"
    "下表列出本集团近三年营业收入构成的占比情况：\r\n"
    "单位：%\r\n"
    "项目 2025 年 2024 年 2023 年\r\n"
    "利息净收入 69.26 67.18 68.29\r\n"
    "下表列示出本集团业务总收入变动情况：\r\n"
    "单位：人民币百万元\r\n"
    "贷款利息收入 186,233 57.26\r\n"
    "合计 325,269 100.00\r\n"
    "3.7.2 利息净收入\r\n"
    "净息差 1.37\r\n"
    "成本收入比 28.50\r\n"
)
SPDB_HIGHLIGHTS = (
    "2.7 主要会计数据和财务指标\r\n"
    "单位：人民币百万元\r\n"
    "营业收入 173,964 170,748\r\n"
    "占营业收入百分比（%）\r\n"
    "利息净收入比营业收入 69.26 67.18\r\n"
)
COMPANY_TOTAL_ONLY = (
    "（1） 营业收入构成\r\n"
    "单位：元\r\n"
    "营业收入合计 75,358,958,001.86\r\n"
)


def _avic_pages():
    return (
        {"page": 11, "text": AVIC_OVERVIEW, "readable": True},
        {"page": 14, "text": AVIC_SEGMENT, "readable": True},
    )


def _spdb_pages():
    return (
        {"page": 22, "text": SPDB_SCOPE, "readable": True},
        {"page": 62, "text": SPDB_TITLE, "readable": True},
    )


def test_default_identity_is_distinct_from_empty_delivery():
    identity = default_processing_identity()
    assert identity != EMPTY_DELIVERY_PROCESSING_IDENTITY
    assert identity["rules"] == "company_profile_common_core.v1"
    assert identity["owned_page_facts"] == "v6"
    assert identity != OWNED_PAGE_FACTS_V1_IDENTITY
    assert identity != OWNED_PAGE_FACTS_V2_IDENTITY
    assert identity != OWNED_PAGE_FACTS_V3_IDENTITY
    assert identity != OWNED_PAGE_FACTS_V4_IDENTITY
    assert identity != OWNED_PAGE_FACTS_V5_IDENTITY


def test_avic_official_excerpts_project_core_facts_without_provider():
    report = _report(instrument_id="302132.SZ", report_id="asset-avic-owned")
    selected = select_core_evidence(report=report, pages=_avic_pages())
    projected = project_owned_page_facts(selected)
    texts = " ".join(
        getattr(record, "source_text", "")
        + " "
        + getattr(record, "object_name", "")
        + " "
        + getattr(record, "label", "")
        + " "
        + str(record.source_native.value or "")
        for record in projected
    )
    assert "航空产品" in texts
    assert "航空防务装备" in texts
    assert "航空制造业" in texts
    assert "73,551,376,777.21" in texts
    assert all(record.data_status == "research_fixture" for record in projected)
    assert not any("\n" in getattr(record, "object_name", "") for record in projected)
    assert not any("军贸" in getattr(record, "object_name", "") for record in projected)
    revenues = [
        record
        for record in projected
        if getattr(record, "field_id", "") == "operating_revenue"
    ]
    assert revenues
    assert all(record.source_native.unit == "元" for record in revenues)
    by_label = {
        (getattr(record, "label", "") or getattr(record, "segment_label", ""),
         getattr(record, "dimension", None) or getattr(record, "segment_dimension", None))
        for record in projected
        if getattr(record, "label", "") or getattr(record, "segment_label", "")
    }
    assert ("航空制造业", "industry") in by_label
    assert ("航空产品", "product") in by_label
    assert ("国内", "region") in by_label
    assert ("直销", "sales_mode") in by_label
    totals = [
        record
        for record in projected
        if getattr(record, "measured_object", "") == "营业收入合计"
    ]
    assert len(totals) == 1
    assert totals[0].segment_dimension is None
    assert totals[0].segment_label is None
    assert totals[0].source_native.value == "75,358,958,001.86"
    zhi_xiao = [
        record
        for record in projected
        if getattr(record, "label", "") == "直销"
        or getattr(record, "segment_label", "") == "直销"
    ]
    assert zhi_xiao
    assert all(
        (getattr(record, "dimension", None) or getattr(record, "segment_dimension", None))
        == "sales_mode"
        for record in zhi_xiao
    )
    assert {record.record_id for record in totals} != {
        record.record_id for record in zhi_xiao
    }


def test_official_avic_does_not_project_third_party_business_scope():
    report = _report(instrument_id="302132.SZ", report_id="asset-avic-scope-boundary")
    selected = select_core_evidence(report=report, pages=_avic_pages())
    projected = project_owned_page_facts(selected)
    objects = [getattr(record, "object_name", "") for record in projected]
    joined = " ".join(objects)
    assert "军贸公司" not in joined
    assert "经营范围内" not in joined
    assert all("\r" not in item and "\n" not in item for item in objects if item)


def test_wrapped_line_starting_with_business_scope_suffix_is_not_a_field_label():
    report = _report(instrument_id="302132.SZ", report_id="asset-avic-scope-wrap")
    selected = select_core_evidence(
        report=report,
        pages=(
            {
                "page": 11,
                "text": (
                    "与依法取得军品出口经营权、并在核定的\r\n"
                    "经营范围内从事军品出口经营活动的军贸公司共同合作，"
                    "公司进行产品的研发、生产、技术服务等。\n"
                    "二、风险因素\n宏观风险。"
                ),
                "readable": True,
            },
        ),
    )
    projected = project_owned_page_facts(selected)
    objects = [getattr(record, "object_name", "") for record in projected]
    titles = [span.section_title for span in selected.spans]
    assert "经营范围" not in titles
    assert not any("军贸" in item for item in objects)


def test_official_spdb_soft_wrap_does_not_pollute_activity_objects():
    report = _report(instrument_id="600000.SH", report_id="asset-spdb-wrap")
    selected = select_core_evidence(
        report=report,
        pages=({"page": 62, "text": SPDB_TITLE_WRAPPED, "readable": True},),
    )
    projected = project_owned_page_facts(selected)
    objects = [getattr(record, "object_name", "") for record in projected]
    assert "商业信贷" in objects
    assert "离岸业务" in objects
    assert all("等多" not in item for item in objects)
    assert all("\n" not in item and "\r" not in item for item in objects if item)


def test_wrapped_segment_row_without_unit_is_not_invented():
    report = _report(instrument_id="302132.SZ", report_id="asset-avic-nounit")
    selected = select_core_evidence(
        report=report,
        pages=(
            {
                "page": 14,
                "text": (
                    "分行业\r\n"
                    "航空制造业 73,551,376,777.\r\n"
                    "21 97.60%\r\n"
                    "三、主要销售客户"
                ),
                "readable": True,
            },
        ),
    )
    projected = project_owned_page_facts(selected)
    assert any(getattr(record, "label", "") == "航空制造业" for record in projected)
    assert not any(
        getattr(record, "field_id", "") == "operating_revenue" for record in projected
    )


def test_runtime_accepts_avic_facts_with_provider_none(tmp_path):
    report = _report(instrument_id="302132.SZ", report_id="asset-avic-runtime")
    writer = CompanyProfileResearchWriter(tmp_path)
    runtime = CompanyProfileStageRuntime(writer=writer, provider=None)
    published = asyncio.run(
        _drive(runtime, _item(report, _avic_pages(), work_id="work-avic-none"))
    )
    assessment = published["assessment"]

    assert published["production_authorization"] == PRODUCTION_AUTHORIZATION
    assert published["provider_calls"] == []
    assert assessment["principal_business"]["answered"] is True
    assert assessment["products_services"]["answered"] is True
    assert assessment["revenue_model"]["answered"] is True
    assert "航空" in assessment["principal_business"]["excerpt"]
    assert "航空制造业" in json.dumps(assessment, ensure_ascii=False)
    products_ids = assessment["products_services"]["supporting_record_ids"]
    assert not any("直销" in item for item in products_ids)
    assert not any("国内" in item for item in products_ids)
    assert any("航空制造业" in item or "航空产品" in item for item in products_ids)
    disputes = published["execution"]["semantic_disputes"]
    assert not any(
        "provider-unavailable" in item.get("reason_codes", [])
        or "required_coverage_missing" in item.get("reason_codes", [])
        for item in disputes
        if item.get("field_id")
        in {"business_overview_source", "explicit_activity", "operating_revenue"}
    )


def test_runtime_accepts_open_spdb_title_without_section_boundary(tmp_path):
    pages = [
        {
            "page": 62,
            "text": (
                "3.6 公司主要业务情况\n"
                "公司致力于为客户提供全面而专业的金融服务，"
                "涵盖商业信贷、交易银行、投资银行、电子银行、"
                "跨境业务、离岸业务等多个领域。"
            ),
            "readable": True,
        }
    ]
    pages.extend(
        {"page": page, "text": f"续表正文第{page}页。", "readable": True}
        for page in range(63, 71)
    )
    report = _report(instrument_id="600000.SH", report_id="asset-spdb-open")
    published = asyncio.run(
        _drive(
            CompanyProfileStageRuntime(
                writer=CompanyProfileResearchWriter(tmp_path),
                provider=None,
            ),
            _item(report, pages, work_id="work-spdb-open"),
        )
    )
    assessment = published["assessment"]

    assert published["provider_calls"] == []
    assert "table_context_incomplete" not in published["evidence_gap_codes"]
    assert assessment["principal_business"]["answered"] is True
    assert assessment["products_services"]["answered"] is True
    assert "商业信贷" in json.dumps(assessment, ensure_ascii=False)
    assert "金融服务" in assessment["principal_business"]["excerpt"]
    assert "小额信贷经营模式" not in assessment["principal_business"]["excerpt"]
    assert "续表正文" not in (assessment["principal_business"]["excerpt"] or "")


def test_runtime_accepts_spdb_scope_without_chapter_missing(tmp_path):
    report = _report(instrument_id="600000.SH", report_id="asset-spdb-runtime")
    writer = CompanyProfileResearchWriter(tmp_path)
    runtime = CompanyProfileStageRuntime(writer=writer, provider=None)
    published = asyncio.run(
        _drive(
            runtime,
            _item(report, ({"page": 22, "text": SPDB_SCOPE, "readable": True},),
                  work_id="work-spdb-scope"),
        )
    )

    assert "chapter_missing" not in published["evidence_gap_codes"]
    assert published["assessment"]["principal_business"]["answered"] is True
    assert "银行业务" in published["assessment"]["principal_business"]["excerpt"]
    assert published["provider_calls"] == []


def test_empty_scope_receipt_is_not_reused_by_successor(tmp_path):
    report = _report(instrument_id="302132.SZ", report_id="asset-empty-scope")
    writer = CompanyProfileResearchWriter(tmp_path)
    first = asyncio.run(
        _drive(
            CompanyProfileStageRuntime(writer=writer, provider=None),
            _item(report, _avic_pages(), work_id="work-scope-first"),
        )
    )
    scope = next(
        path
        for path in (writer.output_root / "scopes").rglob("*.json")
        if path.name == "extract_business_overview.json"
    )
    payload = json.loads(scope.read_text(encoding="utf-8"))
    payload["task_result"]["records"] = []
    payload["task_result"]["dispositions"] = []
    payload["task_result"]["coverage"] = []
    scope.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    successor = asyncio.run(
        _drive(
            CompanyProfileStageRuntime(writer=writer, provider=None),
            _item(report, _avic_pages(), work_id="work-scope-successor"),
        )
    )

    assert first["assessment"]["principal_business"]["answered"] is True
    assert successor["assessment"]["principal_business"]["answered"] is True
    assert ChapterTask.EXTRACT_BUSINESS_OVERVIEW.value not in successor.get(
        "reused_scope_ids", []
    )


def test_new_identity_enqueues_successor_instead_of_reusing_empty(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    first = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=EMPTY_DELIVERY_PROCESSING_IDENTITY,
        instrument_ids=["600000.SH"],
    )
    report = _report(instrument_id="600000.SH", report_id="asset-identity-empty")

    def load_empty(_item):
        return {
            "report": report,
            "pages": ({"page": 1, "text": "目录\n无业务章节", "readable": True},),
        }

    runtime = CompanyProfileStageRuntime(
        writer=CompanyProfileResearchWriter(tmp_path),
        provider=None,
        page_source=load_empty,
    )
    service = BusinessProfileAsyncProductionService(
        repository=queue,
        discovery_runner=lambda **_kwargs: None,
        stage_runner=runtime,
        write_coordinator=get_business_profile_write_coordinator(storage),
        lease_seconds=30,
    )
    budget = StageBudget(max_items=1, max_concurrency=1, max_elapsed_seconds=15)

    async def drain_all():
        return {stage: await service._drain_stage(stage, budget) for stage in WORK_STAGES}

    asyncio.run(drain_all())
    successor = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=default_processing_identity(),
        instrument_ids=["600000.SH"],
    )

    assert first["inserted"] == 1
    assert successor["inserted"] == 1
    assert successor["reused"] == 0


def test_v2_identity_enqueues_successor_instead_of_reusing_v1(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    first = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=OWNED_PAGE_FACTS_V1_IDENTITY,
        instrument_ids=["600000.SH"],
    )
    successor = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=OWNED_PAGE_FACTS_V2_IDENTITY,
        instrument_ids=["600000.SH"],
    )
    assert first["inserted"] == 1
    assert successor["inserted"] == 1
    assert successor["reused"] == 0
    assert first["work_ids"] != successor["work_ids"]


def test_company_total_alone_keeps_revenue_model_numeric_total_only():
    report = _report(instrument_id="302132.SZ", report_id="asset-total-only")
    selected = select_core_evidence(
        report=report,
        pages=({"page": 14, "text": COMPANY_TOTAL_ONLY, "readable": True},),
    )
    projected = project_owned_page_facts(selected)
    assert any(
        getattr(record, "measured_object", "") == "营业收入合计"
        and getattr(record, "segment_dimension", None) is None
        for record in projected
    )
    result = CompanyProfileTaskResult(
        request_id="total-only",
        records=tuple(projected),
        dispositions=tuple(
            Disposition(
                target_id=record.record_id,
                field_id=record.field_id,
                status=DispositionStatus.ACCEPTED_FOR_REVIEW,
            )
            for record in projected
        ),
        coverage=(),
        human_review_items=(),
        task_complete=True,
    )
    assessment = project_core_assessment(report=report, task_results=(result,))
    assert assessment.revenue_model.answered is False
    assert assessment.revenue_model.missing_reason == "numeric_total_only"


def test_spdb_income_analysis_projects_group_mix_and_skips_business_total():
    report = _report(instrument_id="600000.SH", report_id="asset-spdb-income")
    selected = select_core_evidence(
        report=report,
        pages=({"page": 71, "text": SPDB_INCOME, "readable": True},),
    )
    assert any(span.section_title == "利润表分析" for span in selected.spans)
    projected = project_owned_page_facts(selected)
    totals = [
        record
        for record in projected
        if getattr(record, "measured_object", "") == "营业收入"
        and getattr(record, "segment_dimension", None) is None
    ]
    assert len(totals) == 1
    assert totals[0].source_native.value == "173,964"
    assert totals[0].source_native.unit == "百万元"
    assert totals[0].subject_scope.value == "consolidated_group"
    assert totals[0].subject_basis.value == "direct_source_wording"
    interest = [
        record
        for record in projected
        if getattr(record, "measured_object", "") == "利息净收入"
        and getattr(record, "metric_type", None)
        and record.metric_type.value == "operating_revenue"
    ]
    assert len(interest) == 1
    assert interest[0].source_native.value == "120,483"
    assert interest[0].segment_dimension == "income_item"
    shares = [
        record
        for record in projected
        if getattr(record, "metric_type", None)
        and record.metric_type.value == "disclosed_share"
    ]
    assert len(shares) == 1
    assert shares[0].source_native.value == "69.26"
    assert shares[0].relationship_context == "营业收入"
    values = {
        getattr(record.source_native, "value", None)
        for record in projected
        if getattr(record, "source_native", None) is not None
    }
    assert "325,269" not in values
    assert "186,233" not in values
    texts = json.dumps(
        [record.model_dump(mode="json") for record in projected],
        ensure_ascii=False,
    )
    assert "净息差" not in texts
    assert "成本收入比" not in texts
    result = CompanyProfileTaskResult(
        request_id="spdb-income",
        records=tuple(projected),
        dispositions=tuple(
            Disposition(
                target_id=record.record_id,
                field_id=record.field_id,
                status=DispositionStatus.ACCEPTED_FOR_REVIEW,
            )
            for record in projected
        ),
        coverage=(),
        human_review_items=(),
        task_complete=True,
    )
    assessment = project_core_assessment(report=report, task_results=(result,))
    assert assessment.revenue_model.answered is True
    assert any("利息净收入" in item for item in assessment.revenue_model.supporting_record_ids)


def _runtime_issue(record):
    bundle = tuple(
        PreparedEvidence(
            evidence=item,
            field_id=record.field_id,
            source_native=record.source_native,
        )
        for item in record.evidence
    )
    request = _semantic_request(
        work_id="accept-boundary",
        report=record.report,
        chapter=ChapterTask.EXTRACT_SEGMENT_FINANCIALS,
        evidence_bundle=bundle,
        unresolved_field_ids=(),
        deterministic_candidates=(record,),
    )
    return _candidate_issue(record, request)


def _spdb_mix_records():
    report = _report(instrument_id="600000.SH", report_id="asset-spdb-accept")
    selected = select_core_evidence(
        report=report,
        pages=({"page": 71, "text": SPDB_INCOME, "readable": True},),
    )
    projected = project_owned_page_facts(selected)
    total = next(
        record
        for record in projected
        if getattr(record, "measured_object", "") == "营业收入"
        and getattr(record, "segment_dimension", None) is None
    )
    share = next(
        record
        for record in projected
        if getattr(record, "metric_type", None)
        and record.metric_type.value == "disclosed_share"
    )
    return total, share


def test_acceptance_keeps_projected_group_mix():
    total, share = _spdb_mix_records()
    assert _runtime_issue(total) is None
    assert _runtime_issue(share) is None


def test_acceptance_blocks_share_bound_to_business_total():
    _total, share = _spdb_mix_records()
    mutated = share.model_copy(update={"relationship_context": "业务总收入"})
    assert mutated.relationship_context == "业务总收入"
    assert _runtime_issue(mutated) == ContractErrorCode.METRIC_NOT_ALLOWED


def test_acceptance_blocks_group_revenue_without_direct_group_subject():
    total, _share = _spdb_mix_records()
    issuer = total.model_copy(
        update={
            "subject_scope": SubjectScope.ISSUER,
            "subject_basis": SubjectBasis.DIRECT_SOURCE_WORDING,
        }
    )
    unclear = total.model_copy(
        update={"subject_scope": SubjectScope.UNCLEAR, "subject_basis": None}
    )
    assert _runtime_issue(issuer) == ContractErrorCode.SUBJECT_UNSUPPORTED
    assert _runtime_issue(unclear) == ContractErrorCode.SUBJECT_UNSUPPORTED


def test_acceptance_keeps_manufacturing_total_without_group_wording():
    report = _report(instrument_id="302132.SZ", report_id="asset-avic-accept")
    selected = select_core_evidence(
        report=report,
        pages=({"page": 14, "text": COMPANY_TOTAL_ONLY, "readable": True},),
    )
    total = next(
        record
        for record in project_owned_page_facts(selected)
        if getattr(record, "measured_object", "") == "营业收入合计"
    )
    assert total.subject_scope == SubjectScope.UNCLEAR
    assert _runtime_issue(total) is None


def test_income_analysis_ready_excerpt_is_complete_after_clip():
    report = _report(instrument_id="600000.SH", report_id="asset-spdb-income-closed")
    selected = select_core_evidence(
        report=report,
        pages=({"page": 71, "text": SPDB_INCOME, "readable": True},),
    )
    income = next(
        span for span in selected.spans if span.section_title == "利润表分析"
    )
    assert income.context_complete is True
    assert all(
        item.context_complete and item.continuation_complete
        for item in selected.prepared_evidence
        if item.evidence.section_title == "利润表分析"
    )


def test_runtime_keeps_income_mix_when_incomplete_segment_sibling_exists(tmp_path):
    pages = (
        {"page": 71, "text": SPDB_INCOME, "readable": True},
        {
            "page": 196,
            "text": "分部报告\n本行按地区披露分部信息。\n",
            "readable": True,
        },
    )
    report = _report(instrument_id="600000.SH", report_id="asset-spdb-sibling")
    selected = select_core_evidence(report=report, pages=pages)
    assert any(span.section_title == "利润表分析" for span in selected.spans)
    assert any(span.section_title == "分部报告" for span in selected.spans)
    assert any(
        span.section_title == "分部报告" and span.context_complete is False
        for span in selected.spans
    )
    assert all(
        item.evidence.section_title != "分部报告"
        for item in selected.prepared_evidence
    )
    writer = CompanyProfileResearchWriter(tmp_path)
    published = asyncio.run(
        _drive(
            CompanyProfileStageRuntime(writer=writer, provider=None),
            _item(report, pages, work_id="work-spdb-sibling"),
        )
    )
    scope = next(
        path
        for path in (writer.output_root / "scopes").rglob("*.json")
        if path.name == "extract_segment_financials.json"
    )
    payload = json.loads(scope.read_text(encoding="utf-8"))
    result = payload.get("task_result") or {}
    records = result.get("records") or []
    dispositions = {
        item.get("target_id"): item.get("status")
        for item in result.get("dispositions") or []
    }
    values = {
        ((item.get("source_native") or {}).get("value"), item.get("measured_object"))
        for item in records
    }
    assessment = published["assessment"]
    assert ("173,964", "营业收入") in values
    assert ("120,483", "利息净收入") in values
    assert ("69.26", "利息净收入") in values
    assert not any(value == "325,269" for value, _ in values)
    assert all(
        dispositions.get(item.get("record_id")) == "accepted_for_review"
        for item in records
        if (item.get("source_native") or {}).get("value")
        in {"173,964", "120,483", "69.26"}
    )
    excerpt = next(
        span.excerpt
        for span in selected.spans
        if span.section_title == "利润表分析"
    )
    assert "325,269" not in excerpt
    assert "净息差" not in excerpt
    assert assessment["revenue_model"]["answered"] is True
    assert any(
        "利息净收入" in item
        for item in assessment["revenue_model"]["supporting_record_ids"]
    )


def test_financial_highlights_are_not_owned_as_income_analysis():
    report = _report(instrument_id="600000.SH", report_id="asset-spdb-highlights")
    selected = select_core_evidence(
        report=report,
        pages=({"page": 28, "text": SPDB_HIGHLIGHTS, "readable": True},),
    )
    assert not any(
        span.section_title in {"主要会计数据和财务指标", "利润表分析"}
        for span in selected.spans
    )
    projected = project_owned_page_facts(selected)
    assert not any(
        getattr(record, "source_native", None) is not None
        and record.source_native.value in {"173,964", "69.26"}
        for record in projected
    )


def test_v3_identity_enqueues_successor_instead_of_reusing_v2(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    first = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=OWNED_PAGE_FACTS_V2_IDENTITY,
        instrument_ids=["600000.SH"],
    )
    successor = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=OWNED_PAGE_FACTS_V3_IDENTITY,
        instrument_ids=["600000.SH"],
    )
    assert first["inserted"] == 1
    assert successor["inserted"] == 1
    assert successor["reused"] == 0
    assert first["work_ids"] != successor["work_ids"]


def test_v4_identity_enqueues_successor_instead_of_reusing_v3(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    first = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=OWNED_PAGE_FACTS_V3_IDENTITY,
        instrument_ids=["600000.SH"],
    )
    successor = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=default_processing_identity(),
        instrument_ids=["600000.SH"],
    )
    assert first["inserted"] == 1
    assert successor["inserted"] == 1
    assert successor["reused"] == 0
    assert first["work_ids"] != successor["work_ids"]


def test_query_prefers_current_identity_when_predecessor_work_id_sorts_later(
    tmp_path,
):
    report = _report(instrument_id="600000.SH", report_id="asset-query-identity")
    writer = CompanyProfileResearchWriter(tmp_path)
    predecessor_item = _item(
        report,
        ({"page": 1, "text": "目录\n无业务章节", "readable": True},),
        work_id="zz-empty-predecessor",
    )
    predecessor_item["processing_identity"] = EMPTY_DELIVERY_PROCESSING_IDENTITY
    successor_item = _item(
        report,
        ({"page": 22, "text": SPDB_SCOPE, "readable": True},),
        work_id="aa-repaired-successor",
    )
    successor_item["processing_identity"] = default_processing_identity()
    asyncio.run(
        _drive(CompanyProfileStageRuntime(writer=writer, provider=None), predecessor_item)
    )
    asyncio.run(
        _drive(CompanyProfileStageRuntime(writer=writer, provider=None), successor_item)
    )

    storage = _storage(tmp_path)
    _frontier(storage)
    result = asyncio.run(
        CompanyProfileTaskService(
            storage=storage,
            output_root=tmp_path,
            checkpoint_root=tmp_path / "checkpoints",
        ).execute("query", instrument_ids=["600000.SH"])
    )
    profile = result["profiles"][0]
    assert profile["work_id"] == "aa-repaired-successor"
    assert profile["accepted_facts"]
    assert any(
        item["dimension_id"] == "principal_business" and item["answered"]
        for item in profile["dimensions"]
    )


def _two_company_frontier(storage):
    repository, first = _frontier(storage)
    second = {
        "instrument_id": "302132.SZ",
        "symbol": "302132",
        "exchange": "SZSE",
    }
    repository.upsert_record(
        instrument=second,
        record=_announcement(
            "annual-2025-302132",
            "中航成飞2025年年度报告",
            published_at="2026-03-21T08:00:00+08:00",
        ),
    )
    return repository, first, second


def test_published_path_replay_returns_accepted_facts_for_both_companies(tmp_path):
    storage = _storage(tmp_path)
    _two_company_frontier(storage)
    pages = {
        "600000.SH": {
            "report": _report(instrument_id="600000.SH", report_id="asset-spdb-pub"),
            "pages": _spdb_pages(),
        },
        "302132.SZ": {
            "report": _report(instrument_id="302132.SZ", report_id="asset-avic-pub"),
            "pages": _avic_pages(),
        },
    }

    def load_pages(item):
        key = str(item.get("instrument_id") or "")
        return pages[key]

    service = CompanyProfileTaskService(
        storage=storage,
        output_root=tmp_path / "output",
        checkpoint_root=tmp_path / "checkpoints",
        provider=None,
        page_source=load_pages,
    )
    run = asyncio.run(
        service.execute(
            "run",
            knowledge_cutoff="2026-08-30",
            instrument_ids=["302132.SZ", "600000.SH"],
            max_items=2,
        )
    )
    query = asyncio.run(
        service.execute("query", instrument_ids=["302132.SZ", "600000.SH"])
    )
    by_id = {item["instrument_id"]: item for item in query["profiles"]}

    assert run["production_authorization"] == PRODUCTION_AUTHORIZATION
    assert run["enqueue"]["inserted"] >= 2
    assert query["delivered"] == 2
    assert by_id["302132.SZ"]["accepted_facts"]
    assert by_id["600000.SH"]["accepted_facts"]
    assert any(
        item["answered"]
        for item in by_id["302132.SZ"]["dimensions"]
        if item["dimension_id"] == "principal_business"
    )
    assert any(
        item["answered"]
        for item in by_id["600000.SH"]["dimensions"]
        if item["dimension_id"] == "principal_business"
    )


def test_same_identity_repeat_still_reuses_completed_work(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    identity = default_processing_identity()
    first = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=identity,
    )
    second = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=identity,
    )
    assert first["inserted"] == 1
    assert second["reused"] == 1


OWNED_PRINCIPAL = (
    "（一）主营业务\n"
    "公司主要从事汽车、发动机的开发、设计、生产和销售业务。\n"
    "二、报告期内公司所处行业情况\n"
)
LOOSE_PRINCIPAL = "公司主要从事汽车、发动机的开发、设计、生产和销售业务。\n"
MDA_REVENUE = (
    "（一）主营业务分析\n"
    "2、收入和成本分析\n"
    "单位：元 币种：人民币\n"
    "主营业务分行业情况\n"
    "分行业 营业收入 营业成本 毛利率（%）\n"
    "航空服务业 7955002081.35 5940772752.65 25.32\n"
    "6、分部信息\n"
    "分行业\n"
    "航空服务业 7955002081.35 1.00 1.00\n"
)
UNTITLED_REVENUE = "航空服务业 7955002081.35 5940772752.65 25.32\n"
SEGMENT_TEMPLATE = (
    "分部信息\n"
    "分行业\n"
    "单位：元\n"
    "航空地面服务 1361162400.00 10.00\n"
)


BUSINESS_SITUATION = (
    "一、报告期内公司从事的业务情况\n"
    "公司为机场的管理和运营机构，公司以该机场为经营载体，主要从事航空服务业务，"
    "以及商业场地租赁服务等航空性延伸服务业务。\n"
    "二、报告期内公司所处行业情况\n"
)


def test_business_situation_heading_projects_principal_business():
    report = _report(instrument_id="SHAPE.SH", report_id="asset-business-situation")
    selected = select_core_evidence(
        report=report,
        pages=({"page": 8, "text": BUSINESS_SITUATION, "readable": True},),
    )
    records = project_owned_page_facts(selected)
    overview = next(item for item in records if item.field_id == "business_overview_source")
    assert overview.evidence[0].section_title == "报告期内公司从事的业务情况"
    assert "主要从事" in overview.source_text
    assert "为经营载体" in overview.source_text
    assert "principal_business" in overview_dimension_hits(overview.source_text)


def test_owned_heading_projects_principally_engaged_wording():
    report = _report(instrument_id="600006.SH", report_id="asset-principal")
    selected = select_core_evidence(
        report=report,
        pages=({"page": 8, "text": OWNED_PRINCIPAL, "readable": True},),
    )
    records = project_owned_page_facts(selected)
    overview = next(item for item in records if item.field_id == "business_overview_source")
    activities = [item for item in records if item.field_id == "explicit_activity"]
    assert "主要从事" in overview.source_text
    assert activities
    assert any("汽车" in item.object_name for item in activities)
    assert overview.evidence[0].page == 8


def test_principally_engaged_wording_outside_owned_heading_is_refused():
    report = _report(instrument_id="600006.SH", report_id="asset-loose-principal")
    selected = select_core_evidence(
        report=report,
        pages=({"page": 8, "text": LOOSE_PRINCIPAL, "readable": True},),
    )
    assert not any(
        span.chapter_task == ChapterTask.EXTRACT_BUSINESS_OVERVIEW.value
        for span in selected.spans
    )
    assert not any(
        item.field_id in {"business_overview_source", "explicit_activity"}
        for item in project_owned_page_facts(selected)
    )


def test_mda_revenue_table_keeps_source_binding_and_ignores_later_equal_amount():
    report = _report(instrument_id="600004.SH", report_id="asset-mda-revenue")
    selected = select_core_evidence(
        report=report,
        pages=(
            {"page": 12, "text": MDA_REVENUE, "readable": True},
            {"page": 198, "text": SEGMENT_TEMPLATE, "readable": True},
        ),
    )
    revenue_span = next(
        span for span in selected.spans if span.section_title == "收入和成本分析"
    )
    assert "航空服务业 7955002081.35" in revenue_span.excerpt
    assert "航空地面服务" not in revenue_span.excerpt
    records = [
        item
        for item in project_owned_page_facts(selected)
        if item.field_id in {"segment_dimension", "operating_revenue"}
        and getattr(item, "segment_label", None) == "航空服务业"
        or (
            item.field_id == "operating_revenue"
            and getattr(getattr(item, "source_native", None), "value", None)
            == "7955002081.35"
        )
    ]
    assert records
    assert all(item.evidence[0].page == 12 for item in records)
    assert all(item.evidence[0].section_title == "收入和成本分析" for item in records)
    assert any(getattr(item, "segment_dimension", None) == "industry" for item in records)
    assert any(
        getattr(getattr(item, "source_native", None), "unit", None) == "元"
        for item in records
    )
    assert not any(
        getattr(getattr(item, "source_native", None), "value", None) == "1.00"
        for item in project_owned_page_facts(selected)
    )


AIRPORT_REVENUE_SHAPE = (
    "（一）主营业务分析\n"
    "2、收入和成本分析\n"
    "单位：万元 币种：人民币\n"
    "一、航空性收入 325,184.77 40.88 295,218.84 39.77 10.15\n"
    "1、飞机起降相关收入 114,986.00 14.45 113,873.61 15.34 0.98\n"
    "二、非航空性收入 470,315.44 59.12 447,140.89 60.23 5.18\n"
    "单位：元 币种：人民币\n"
    "主营业务分行业情况\n"
    "分行业 营业收入 营业成本 毛利率（%）\n"
    "航空服务业 7,955,002,081.35 5,940,772,752.65 25.32 7.16 9.69 -1.73\n"
    "(3). 成本分析表\n"
    "货币资金 65,885,912.67 65,885,912.67\n"
)
VEHICLE_REVENUE_SHAPE = (
    "2、收入和成本分析\n"
    "单位：元 币种：人民币\n"
    "主营业务分行业情况\n"
    "分行业 营业收入 营业成本\n"
    "汽车制\n"
    "造业\n"
    "8,809,766,813.05 8,930,817,225.01 -1.37 -19.12 -16.75\n"
    "主营业务分产品情况\n"
    "分产品 营业收入 营业成本\n"
    "整车 7,303,679,564.58 7,579,828,607.20 -3.78\n"
    "非整车 1,506,087,248.47 1,350,988,617.81 10.30\n"
    "主营业务分地区情况\n"
    "分地区 营业收入 营业成本\n"
    "境内 8,267,285,112.72 8,329,539,226.59 -0.75\n"
    "个百分点\n"
    "境外 542,481,700.33 601,277,998.42 -10.84\n"
    "主营业务分销售模式情况\n"
    "销售模\n"
    "式\n"
    "营业收入 营业成本\n"
    "代理销\n"
    "售模式\n"
    "7,497,059,102.65 7,620,318,256.67 -1.64\n"
    "个百分点\n"
    "订单销\n"
    "售模式\n"
    "1,312,707,710.40 1,310,498,968.34 0.17\n"
    "主营产销量情况分析表\n"
    "客车 辆 3,751 4,567 242\n"
    "(3). 成本分析表\n"
    "整车 7,579,828,607.20 84.87 9,870,621,824.11\n"
    "货币资金 65,885,912.67 65,885,912.67\n"
    "应付账款 1,139,889,932.98 35.99\n"
)
SAME_PAGE_INVALID_MDA = (
    "2、收入和成本分析\n"
    "报告期内收入结构未发生变化。\n"
    "分部信息\n"
    "分行业\n"
    "单位：元\n"
    "航空地面服务 1361162400.00 10.00\n"
)


def _labels(records):
    return {getattr(item, "segment_label", None) for item in records}


def test_enumerated_income_classes_use_revenue_composition_dimension():
    report = _report(instrument_id="SHAPE.SH", report_id="asset-airport-shape")
    selected = select_core_evidence(
        report=report,
        pages=({"page": 12, "text": AIRPORT_REVENUE_SHAPE, "readable": True},),
    )
    records = project_owned_page_facts(selected)
    classes = {
        item.label: item.dimension
        for item in records
        if item.field_id == "segment_dimension"
    }
    assert classes["航空性收入"] == "revenue_composition"
    assert classes["非航空性收入"] == "revenue_composition"
    assert classes["航空服务业"] == "industry"
    assert "货币资金" not in classes
    units = {
        item.measured_object: item.source_native.unit
        for item in records
        if item.field_id == "operating_revenue"
    }
    assert units["航空性收入"] == "万元"
    assert units["非航空性收入"] == "万元"
    assert units["航空服务业"] == "元"
    assert all(item.evidence[0].section_title == "收入和成本分析" for item in records if item.field_id == "segment_dimension")


def test_wrapped_industry_and_product_rows_stop_before_later_tables():
    report = _report(instrument_id="SHAPE.SH", report_id="asset-vehicle-shape")
    selected = select_core_evidence(
        report=report,
        pages=({"page": 11, "text": VEHICLE_REVENUE_SHAPE, "readable": True},),
    )
    records = project_owned_page_facts(selected)
    classes = {
        item.label: item.dimension
        for item in records
        if item.field_id == "segment_dimension"
    }
    assert classes["汽车制造业"] == "industry"
    assert classes["整车"] == "product"
    assert classes["非整车"] == "product"
    assert classes["境内"] == "region"
    assert classes["境外"] == "region"
    assert classes["代理销售模式"] == "sales_mode"
    assert classes["订单销售模式"] == "sales_mode"
    assert all(
        item.source_native.unit == "元"
        for item in records
        if item.field_id == "operating_revenue"
    )
    assert "个百分点订单销售模式" not in classes
    assert not any("个百分点" in label for label in classes)
    values = {
        getattr(getattr(item, "source_native", None), "value", None) for item in records
    }
    assert "7,303,679,564.58" in values
    assert "7,579,828,607.20" not in values
    assert "65,885,912.67" not in values
    assert "客车" not in classes
    assert "货币资金" not in classes
    assert "应付账款" not in classes


def test_invalid_mda_falls_through_to_segment_template_on_the_same_page():
    report = _report(instrument_id="SHAPE.SH", report_id="asset-same-page-fallback")
    selected = select_core_evidence(
        report=report,
        pages=({"page": 12, "text": SAME_PAGE_INVALID_MDA, "readable": True},),
    )
    assert any(span.section_title == "分部信息" and span.page == 12 for span in selected.spans)
    records = project_owned_page_facts(selected)
    assert any(getattr(item, "segment_label", None) == "航空地面服务" for item in records)


def test_untitled_revenue_row_is_refused():
    report = _report(instrument_id="600004.SH", report_id="asset-untitled")
    selected = select_core_evidence(
        report=report,
        pages=({"page": 12, "text": UNTITLED_REVENUE, "readable": True},),
    )
    assert not any(
        span.chapter_task == ChapterTask.EXTRACT_SEGMENT_FINANCIALS.value
        for span in selected.spans
    )


INVALID_MDA_PROSE = (
    "2、收入和成本分析\n"
    "报告期内公司收入结构未发生变化，详见后附财务报表。\n"
)
INVALID_MDA_UNTITLED = (
    "2、收入和成本分析\n"
    "单位：元\n"
    "航空服务业 7955002081.35 5940772752.65 25.32\n"
)


def test_invalid_mda_section_falls_through_to_later_segment_template():
    report = _report(instrument_id="600004.SH", report_id="asset-mda-fallback")
    selected = select_core_evidence(
        report=report,
        pages=(
            {"page": 12, "text": INVALID_MDA_PROSE, "readable": True},
            {"page": 198, "text": SEGMENT_TEMPLATE, "readable": True},
        ),
    )
    assert any(span.section_title == "分部信息" and span.page == 198 for span in selected.spans)
    assert not any(span.section_title == "收入和成本分析" for span in selected.spans)
    records = project_owned_page_facts(selected)
    assert any(getattr(item, "segment_label", None) == "航空地面服务" for item in records)
    assert all(item.evidence[0].page == 198 for item in records if item.field_id == "segment_dimension")


def test_untitled_amount_row_does_not_block_later_segment_template():
    report = _report(instrument_id="600004.SH", report_id="asset-untitled-fallback")
    selected = select_core_evidence(
        report=report,
        pages=(
            {"page": 12, "text": INVALID_MDA_UNTITLED, "readable": True},
            {"page": 198, "text": SEGMENT_TEMPLATE, "readable": True},
        ),
    )
    assert any(span.section_title == "分部信息" and span.page == 198 for span in selected.spans)
    records = project_owned_page_facts(selected)
    assert any(getattr(item, "segment_label", None) == "航空地面服务" for item in records)
    assert not any(
        getattr(getattr(item, "source_native", None), "value", None) == "7955002081.35"
        for item in records
    )


def test_segment_information_template_remains_usable():
    report = _report(instrument_id="600004.SH", report_id="asset-segment-template")
    selected = select_core_evidence(
        report=report,
        pages=({"page": 198, "text": SEGMENT_TEMPLATE, "readable": True},),
    )
    assert any(span.section_title == "分部信息" for span in selected.spans)


def test_owned_disclosure_projects_without_provider(tmp_path):
    report = _report(instrument_id="600006.SH", report_id="asset-no-provider")
    pages = ({"page": 8, "text": OWNED_PRINCIPAL, "readable": True},)
    writer = CompanyProfileResearchWriter(tmp_path)
    published = asyncio.run(
        _drive(
            CompanyProfileStageRuntime(writer=writer, provider=None),
            _item(report, pages, work_id="work-no-provider"),
        )
    )
    assert published["provider_calls"] == []
    assert published["assessment"]["principal_business"]["answered"] is True
    assert published["assessment"]["products_services"]["answered"] is True


def test_v5_enqueues_successor_and_query_prefers_it_over_later_v4_work_id(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    first = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=OWNED_PAGE_FACTS_V4_IDENTITY,
        instrument_ids=["600000.SH"],
    )
    successor = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=default_processing_identity(),
        instrument_ids=["600000.SH"],
    )
    assert default_processing_identity()["owned_page_facts"] == "v6"
    assert default_processing_identity() != OWNED_PAGE_FACTS_V5_IDENTITY
    assert first["inserted"] == 1
    assert successor["inserted"] == 1
    assert successor["reused"] == 0
    assert first["work_ids"] != successor["work_ids"]

    report = _report(instrument_id="600000.SH", report_id="asset-v5-query")
    writer = CompanyProfileResearchWriter(tmp_path / "output")
    predecessor = _item(
        report,
        ({"page": 1, "text": "目录\n无业务章节", "readable": True},),
        work_id="zz-v4-empty",
    )
    predecessor["processing_identity"] = OWNED_PAGE_FACTS_V4_IDENTITY
    current = _item(
        report,
        ({"page": 8, "text": OWNED_PRINCIPAL, "readable": True},),
        work_id="aa-v5-successor",
    )
    current["processing_identity"] = default_processing_identity()
    asyncio.run(
        _drive(CompanyProfileStageRuntime(writer=writer, provider=None), predecessor)
    )
    asyncio.run(
        _drive(CompanyProfileStageRuntime(writer=writer, provider=None), current)
    )
    queried = asyncio.run(
        CompanyProfileTaskService(
            storage=_storage(tmp_path / "query"),
            output_root=tmp_path / "output",
            checkpoint_root=tmp_path / "checkpoints",
        ).execute("query", instrument_ids=["600000.SH"])
    )
    profile = queried["profiles"][0]
    assert profile["work_id"] == "aa-v5-successor"
    assert profile["accepted_facts"]


def test_repair_does_not_rewrite_sw_l1_or_start_m4():
    from pathlib import Path

    execution = Path("research/company_profile/execution.py").read_text(encoding="utf-8")
    runtime = Path("research/company_profile/runtime.py").read_text(encoding="utf-8")
    assert "sw_l1_name" not in execution
    assert "sw_l1_name" not in runtime
    assert "industry_package" not in default_processing_identity()
