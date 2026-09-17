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
from research.company_profile.core_evidence_selection import (
    project_owned_page_facts,
    select_core_evidence,
)
from research.company_profile.execution import (
    EMPTY_DELIVERY_PROCESSING_IDENTITY,
    OWNED_PAGE_FACTS_V1_IDENTITY,
    default_processing_identity,
)
from research.company_profile.models import PRODUCTION_AUTHORIZATION, ChapterTask
from research.company_profile.operations import CompanyProfileTaskService
from research.company_profile.runtime import (
    CompanyProfileResearchWriter,
    CompanyProfileStageRuntime,
)
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
    assert identity["owned_page_facts"] == "v2"
    assert identity != OWNED_PAGE_FACTS_V1_IDENTITY


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


def test_repair_does_not_rewrite_sw_l1_or_start_m4():
    from pathlib import Path

    execution = Path("research/company_profile/execution.py").read_text(encoding="utf-8")
    runtime = Path("research/company_profile/runtime.py").read_text(encoding="utf-8")
    assert "sw_l1_name" not in execution
    assert "sw_l1_name" not in runtime
    assert "industry_package" not in default_processing_identity()
