from __future__ import annotations

import asyncio
import hashlib
import json
from types import SimpleNamespace

from research.business_profile_async_production import BusinessProfileWorkRepository
from research.company_profile import ExtractResponse
from research.company_profile.commodity_exposure import (
    derive_commodity_role,
    project_commodity_exposures,
)
from research.company_profile.core_evidence_selection import (
    project_owned_page_facts,
    select_core_evidence,
)
from research.company_profile.core_skeleton import select_activated_chapters
from research.company_profile.execution import (
    OWNED_PAGE_FACTS_V8_IDENTITY,
    default_processing_identity,
)
from research.company_profile.models import ChapterTask
from research.company_profile.operations import CompanyProfileTaskService
from research.company_profile.runtime import (
    COMMON_CORE_STORAGE_NAMESPACE,
    CompanyProfileResearchWriter,
    CompanyProfileStageRuntime,
)
from tests.unit.test_research.test_business_profile_async_production import _frontier
from tests.unit.test_research.test_business_profile_exposure_components import _storage
from tests.unit.test_research.test_company_profile_commodity_exposure import _activity
from tests.unit.test_research.test_company_profile_core_skeleton import (
    MANUFACTURING_OVERVIEW,
    SERVICE_OVERVIEW,
)
from tests.unit.test_research.test_company_profile_runtime import _drive, _item, _report

NAMED_INPUT = (
    "3、原材料价格波动风险\n"
    "自 2025 年下半年以来，钢材、铝材、碳酸锂及镍等主要原材料价格持续走高，预计 2026 年\n"
    "可能直接推高整车制造成本。\n"
)
PRICE_ONLY = "原材料价格持续上涨，公司盈利可能承压。\n"
GENERIC_COST = "直接材料成本 1200 万元\n原材料存货 800\n原材料 65885912.67 65885912.67\n"
SALES_ONLY = "公司主要产品为钢材，通过向客户销售钢材取得货款。\n"
ENERGY_INPUT = "公司采购电力用于生产。\n"


def _materials(text: str, *, instrument_id: str = "SHAPE.SH"):
    report = _report(instrument_id=instrument_id, report_id=f"asset-{instrument_id}")
    selected = select_core_evidence(
        report=report,
        pages=({"page": 24, "text": text, "readable": True},),
    )
    records = [
        item
        for item in project_owned_page_facts(selected)
        if item.field_id == "material_input"
    ]
    return report, selected, records


def _roles(records, catalog=None):
    exposures = project_commodity_exposures(records, catalog=catalog)
    return {item.source_native_name: item for item in exposures}


def test_named_production_inputs_deliver_raw_material_role_without_quantity():
    _report_obj, selected, records = _materials(NAMED_INPUT)
    assert {item.object_name for item in records} == {"钢材", "铝材", "碳酸锂", "镍"}
    assert all(item.evidence[0].page == 24 for item in records)
    assert all(item.evidence[0].section_title == "3、原材料价格波动风险" for item in records)
    assert all("碳酸锂" in item.evidence[0].anchor.bounded_quote for item in records)
    assert all("制造" in item.evidence[0].anchor.bounded_quote for item in records)
    roles = _roles(records, catalog=_Catalog("pending"))
    assert set(roles) == {"钢材", "铝材", "碳酸锂", "镍"}
    assert all(item.role == "raw_material_input" for item in roles.values())
    assert all(item.mapping_status == "pending" for item in roles.values())
    assert all(item.commodity_id is None for item in roles.values())
    assert all(item.market_series_id is None for item in roles.values())
    assert any(
        item.chapter_task == ChapterTask.EXTRACT_MATERIAL_INPUTS
        and item.status == "activated"
        for item in select_activated_chapters(
            ({"page": 24, "text": NAMED_INPUT, "readable": True},)
        )
    )
    assert selected.spans


def test_existing_manufacturing_fixture_uses_the_same_input_path():
    text = f"{MANUFACTURING_OVERVIEW}\n公司采购正极材料用于生产动力电池。\n"
    _report_obj, _selected, records = _materials(text, instrument_id="BATTERY.SZ")
    assert [item.object_name for item in records] == ["正极材料"]
    assert _roles(records, catalog=_Catalog("pending"))["正极材料"].role == (
        "raw_material_input"
    )


def test_service_report_without_an_explicit_input_has_no_material_role():
    _report_obj, selected, records = _materials(SERVICE_OVERVIEW, instrument_id="SOFT.SH")
    assert records == []
    assert not any(
        span.chapter_task == "extract_material_inputs" for span in selected.spans
    )
    chapters = select_activated_chapters(
        ({"page": 8, "text": SERVICE_OVERVIEW, "readable": True},)
    )
    material = next(
        item for item in chapters if item.chapter_task == ChapterTask.EXTRACT_MATERIAL_INPUTS
    )
    assert material.status == "not_applicable"


def test_price_risk_generic_cost_and_inventory_do_not_create_inputs():
    for text in (PRICE_ONLY, GENERIC_COST):
        _report_obj, _selected, records = _materials(text)
        assert records == []


def test_sales_evidence_alone_does_not_create_an_input_role():
    report, _selected, records = _materials(SALES_ONLY)
    assert records == []
    exposures = project_commodity_exposures(
        (_activity("钢材", report=report, record_id="sales-steel"),),
        catalog=_Catalog("pending"),
    )
    assert [item.role for item in exposures] == ["product_sales"]


def test_independent_sales_and_input_evidence_keep_both_roles():
    report, _selected, records = _materials(NAMED_INPUT)
    steel = next(item for item in records if item.object_name == "钢材")
    exposures = project_commodity_exposures(
        (steel, _activity("钢材", report=report, record_id="sales-steel")),
        catalog=_Catalog("pending"),
    )
    assert {item.role for item in exposures} == {"raw_material_input", "product_sales"}
    assert len(exposures) == 2


def test_energy_input_keeps_energy_consumption():
    _report_obj, _selected, records = _materials(ENERGY_INPUT)
    assert [item.object_name for item in records] == ["电力"]
    assert derive_commodity_role(records[0]) == "energy_consumption"


def test_ambiguous_catalog_keeps_the_input_without_guessing_an_id():
    _report_obj, _selected, records = _materials("公司采购正极材料用于生产。\n")
    exposure = _roles(records, catalog=_Catalog("ambiguous"))["正极材料"]
    assert exposure.role == "raw_material_input"
    assert exposure.mapping_status == "ambiguous"
    assert exposure.commodity_id is None
    assert exposure.source_native_name == "正极材料"


def test_mapped_catalog_keeps_one_commodity_id():
    _report_obj, _selected, records = _materials("公司采购正极材料用于生产。\n")
    exposure = _roles(records, catalog=_Catalog("mapped"))["正极材料"]
    assert exposure.mapping_status == "mapped"
    assert exposure.commodity_id == "commodity-1"


def test_runtime_delivers_input_role_and_query_prefers_the_new_identity(tmp_path):
    report = _report(instrument_id="600000.SH", report_id="asset-material-query")
    writer = CompanyProfileResearchWriter(tmp_path)
    predecessor = _item(
        report,
        (
            {
                "page": 8,
                "text": f"报告期内公司从事的主要业务\n{SERVICE_OVERVIEW}\n二、风险因素\n",
                "readable": True,
            },
        ),
        work_id="zz-v8-predecessor",
    )
    predecessor["processing_identity"] = dict(OWNED_PAGE_FACTS_V8_IDENTITY)
    asyncio.run(
        _drive(CompanyProfileStageRuntime(writer=writer, provider=None), predecessor)
    )
    v8_paths = _v8_work_files(tmp_path)
    v8_hashes = {path: _sha256(path) for path in v8_paths}
    assert all(path.is_file() for path in v8_paths)
    assert "material_input" not in _stored_field_ids(v8_paths)

    spy = _MaterialProviderSpy()
    successor = _item(
        report,
        (
            {
                "page": 8,
                "text": (
                    "报告期内公司从事的主要业务\n"
                    f"{MANUFACTURING_OVERVIEW}\n"
                    "二、风险因素\n"
                ),
                "readable": True,
            },
            {"page": 24, "text": NAMED_INPUT, "readable": True},
        ),
        work_id="aa-material-successor",
    )
    asyncio.run(
        _drive(
            CompanyProfileStageRuntime(writer=writer, provider=spy),
            successor,
        )
    )
    assert ChapterTask.EXTRACT_MATERIAL_INPUTS not in spy.chapters
    assert {path: _sha256(path) for path in v8_paths} == v8_hashes

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
    assert profile["work_id"] == "aa-material-successor"
    exposures = profile["commodity_exposure"]["assessment"]["exposures"]
    roles = {
        item["source_native_name"]: item["role"]
        for item in exposures
    }
    assert roles["钢材"] == "raw_material_input"
    assert roles["镍"] == "raw_material_input"
    assert all(item["market_series_id"] is None for item in exposures)


def test_material_identity_enqueues_successor_without_replaying_v8(tmp_path):
    storage = _storage(tmp_path)
    _frontier(storage)
    queue = BusinessProfileWorkRepository(
        storage, checkpoint_root=tmp_path / "checkpoints"
    )
    predecessor = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=OWNED_PAGE_FACTS_V8_IDENTITY,
        instrument_ids=["600000.SH"],
    )
    successor = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=default_processing_identity(),
        instrument_ids=["600000.SH"],
    )
    assert predecessor["inserted"] == 1
    assert successor["inserted"] == 1
    assert successor["reused"] == 0
    assert predecessor["work_ids"] != successor["work_ids"]
    repeat = queue.enqueue_latest_annual(
        knowledge_cutoff="2026-08-30",
        processing_identity=OWNED_PAGE_FACTS_V8_IDENTITY,
        instrument_ids=["600000.SH"],
    )
    assert repeat["reused"] == 1
    assert repeat["work_ids"] == predecessor["work_ids"]


class _MaterialProviderSpy:
    def __init__(self) -> None:
        self.chapters: list[ChapterTask] = []

    def apply_output_token_budget(self, **kwargs) -> None:
        del kwargs

    def extract(self, request):
        self.chapters.append(request.chapter_task)
        if request.chapter_task == ChapterTask.EXTRACT_MATERIAL_INPUTS:
            raise AssertionError("material chapter must stay on the deterministic path")
        return ExtractResponse(request_id=request.request_id)

    def repair(self, request):
        raise AssertionError("repair is outside the material-input slice")

    def verify(self, request):
        from research.company_profile import VerifyCheck, VerifyResponse, VerifyStatus

        return VerifyResponse(
            request_id=request.request_id,
            checks=tuple(
                VerifyCheck(
                    target_type="candidate",
                    target_id=record.record_id,
                    status=VerifyStatus.PASS,
                )
                for record in request.candidates
            ),
        )


def _stored_field_ids(paths) -> set[str]:
    found: set[str] = set()

    def walk(value) -> None:
        if isinstance(value, dict):
            field_id = value.get("field_id")
            if isinstance(field_id, str):
                found.add(field_id)
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    for path in paths:
        walk(json.loads(path.read_text(encoding="utf-8")))
    return found


def _v8_work_files(root):
    namespace = root / COMMON_CORE_STORAGE_NAMESPACE
    return (
        namespace / "zz-v8-predecessor.json",
        namespace / "checkpoints" / "zz-v8-predecessor.json",
    )


def _sha256(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class _Catalog:
    catalog_version = "fixture-catalog"

    def __init__(self, mode: str) -> None:
        self.mode = mode

    def resolve_alias(self, name: str):
        del name
        if self.mode == "ambiguous":
            return SimpleNamespace(product_ids=("p1", "p2"))
        if self.mode == "mapped":
            return SimpleNamespace(product_ids=("p1",))
        return SimpleNamespace(product_ids=())

    def commodity_candidates(self, product_id: str):
        if self.mode == "mapped":
            return (SimpleNamespace(commodity_id="commodity-1"),)
        if self.mode == "ambiguous":
            return (SimpleNamespace(commodity_id=f"c-{product_id}"),)
        return ()
