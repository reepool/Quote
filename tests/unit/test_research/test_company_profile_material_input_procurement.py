"""Procurement-row and materials-table entrances stay on the research plan."""

from __future__ import annotations

from types import SimpleNamespace

from research.company_profile.commodity_exposure import project_commodity_exposures
from research.company_profile.core_evidence_selection import (
    explicit_material_input_names,
)
from research.company_profile.material_input_research import (
    MATERIAL_INPUT_PROCUREMENT_PLAN_VERSION,
    MATERIAL_INPUT_RESEARCH_PLAN_VERSION,
    _relationship,
    research_material_hits,
)
from research.company_profile.models import (
    Evidence,
    ReportIdentity,
    TextAnchor,
)

_PLAN = MATERIAL_INPUT_PROCUREMENT_PLAN_VERSION
_COMPOUND = (
    "购销商品、提供和接受劳务的关联交易\n"
    "向关联方采购商品及提供服务采购原材料（磷酸铁锂）及委托加工原材料\n"
    "向关联方采购商品采购原材料（锂盐及氢氧化锂）\n"
    "向关联方销售商品及提供服务提供租赁、咨询服务及销售电芯\n"
)
_TABLE = (
    "三、主要原材料及能源采购"
    "原材料及能源名称耗用情况"
    "丁酮肟合理范围分散采购"
    "蒸汽合理范围定向采购"
    "电合理范围定向采购"
)
_NAMED = "公司生产经营所需主要原材料包括正极材料、负极材料、隔膜和电解液等。"


def _report() -> ReportIdentity:
    return ReportIdentity(
        instrument_id="FIXTURE.SH",
        report_id="asset-fixture",
        document_version="ver-fixture",
        report_period="2025-12-31",
        published_at="2026-03-01T00:00:00+00:00",
    )


def _relationships(kind: str, text: str, plan: str = _PLAN):
    report = _report()
    hits = research_material_hits(kind, text, plan)
    records = []
    for hit in hits:
        assert hit.section_title in hit.quote
        assert hit.direction in hit.quote
        assert hit.name in hit.quote
        evidence = Evidence(
            evidence_id=f"evidence-{hit.name}",
            report=report,
            page=1,
            section_title=hit.section_title,
            anchor=TextAnchor(bounded_quote=hit.quote),
        )
        records.append(_relationship(report, evidence, hit.name, "fixture"))
    return records


def test_purchase_rows_in_a_service_heading_deliver_each_named_material():
    records = _relationships("company_purchase", _COMPOUND)
    assert {item.object_name for item in records} == {"磷酸铁锂", "锂盐", "氢氧化锂"}
    assert all(item.relation_type.value == "material_input" for item in records)
    assert all(item.source_native.value is None for item in records)
    assert "电芯" not in {item.object_name for item in records}
    assert explicit_material_input_names(_COMPOUND) == ()
    assert (
        research_material_hits(
            "company_purchase", _COMPOUND, MATERIAL_INPUT_RESEARCH_PLAN_VERSION
        )
        == ()
    )


def test_materials_table_delivers_raw_material_rows_without_quantity():
    records = _relationships("materials_energy_table", _TABLE)
    assert [item.object_name for item in records] == ["丁酮肟"]
    assert records[0].source_native.value is None
    assert "蒸汽" not in {item.object_name for item in records}
    assert "电" not in {item.object_name for item in records}


def test_pending_and_ambiguous_mappings_keep_the_relationship():
    records = _relationships("company_purchase", _COMPOUND)

    class _Catalog:
        catalog_version = "fixture"

        def resolve_alias(self, name: str):
            if name == "锂盐":
                return SimpleNamespace(product_ids=("left", "right"))
            return SimpleNamespace(product_ids=())

        def commodity_candidates(self, product_id: str):
            return (SimpleNamespace(commodity_id=f"commodity-{product_id}"),)

    exposures = {
        item.source_native_name: item
        for item in project_commodity_exposures(records, catalog=_Catalog())
    }
    assert set(exposures) == {"磷酸铁锂", "锂盐", "氢氧化锂"}
    assert exposures["磷酸铁锂"].mapping_status == "pending"
    assert exposures["锂盐"].mapping_status == "ambiguous"
    assert all(item.commodity_id is None for item in exposures.values())


def test_refused_rows_create_no_input_relationship():
    refused = (
        "购销商品、提供和接受劳务向关联方销售商品及提供服务提供租赁、咨询服务及销售电芯",
        "购销商品、提供和接受劳务公司提供租赁、咨询服务",
        "关联交易公司向关联方采购商品金额1000000元",
        "购销商品供应商采购原材料（钢材）",
        "三、主要原材料及能源蒸汽合理范围定向采购电合理范围定向采购",
        "公司委托外部厂商加工丁酮肟的情况。委外加工是由公司提供主要原料并支付固定加工费。",
        "直接材料221,152,510千元",
        "原材料18,723,415.97元",
    )
    for text in refused:
        assert _relationships("company_purchase", text) == []
        assert _relationships("materials_energy_table", text) == []


def test_multiple_scopes_aggregate_sentence_and_purchase_inputs():
    sentence_names = explicit_material_input_names(_NAMED)
    purchase_names = tuple(
        hit.name for hit in research_material_hits("company_purchase", _COMPOUND, _PLAN)
    )
    aggregated = tuple(dict.fromkeys((*sentence_names, *purchase_names)))
    assert "正极材料" in aggregated
    assert "磷酸铁锂" in aggregated
    assert len(aggregated) == len(set(aggregated))
