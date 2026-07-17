import copy
import json

import pytest

from research.business_profile_disclosure_templates import (
    DEFAULT_DISCLOSURE_TEMPLATE_CATALOG_PATH,
    load_disclosure_template_catalog,
    normalize_board,
    parse_disclosure_template_catalog,
)
from research.business_profile_fact_catalog import load_business_fact_catalog


def _default_payload():
    return json.loads(
        DEFAULT_DISCLOSURE_TEMPLATE_CATALOG_PATH.read_text(encoding="utf-8")
    )


def _fact_field_ids():
    return {
        definition.field_id for definition in load_business_fact_catalog().definitions
    }


def test_default_catalog_covers_common_and_six_industry_templates():
    catalog = load_disclosure_template_catalog()

    assert catalog.catalog_version == "business_profile_disclosure_templates.2026.1"
    assert catalog.fact_catalog_version == "business_profile_facts.2026.1"
    assert len(catalog.templates) == 7
    assert {
        industry
        for template in catalog.templates
        for industry in template.industry_groups
    } == {
        "all",
        "coal",
        "nonferrous_and_solid_mineral",
        "steel",
        "petrochemical",
        "basic_chemical",
        "building_material",
    }


def test_selection_returns_common_then_matching_industry_only():
    catalog = load_disclosure_template_catalog()

    selected = catalog.select(
        document_date="2025-12-31",
        exchange="SH",
        board="main",
        document_type="annual_report",
        industry_group="coal",
    )

    assert [item.template_id for item in selected] == [
        "common_periodic_report.v1",
        "coal_industry.v1",
    ]
    assert [item.scope.exchange for item in selected] == ["SSE", "SSE"]
    assert selected[0].authority_type == "csrc_common_rule"
    assert selected[0].rule_version == "csrc_rule_no_2_2025"
    assert selected[1].authority_type == "exchange_industry_rule"
    assert selected[1].rule_version == "sse_guideline_3_2022_no_2_coal"


def test_selection_without_industry_returns_only_common_template():
    catalog = load_disclosure_template_catalog()

    selected = catalog.select(
        document_date="2025-06-30",
        exchange="SZSE",
        board="chinext",
        document_type="semiannual_report",
    )

    assert [item.template_id for item in selected] == ["common_periodic_report.v1"]


def test_merged_aliases_include_common_and_industry_sections():
    catalog = load_disclosure_template_catalog()

    aliases = catalog.merged_section_aliases(
        document_date="2025-12-31",
        exchange="BSE",
        board="bse",
        document_type="annual_report_correction",
        industry_group="steel",
    )

    assert "主营业务" in aliases["principal_business"]
    assert "钢材产销量" in aliases["steel_operations"]


@pytest.mark.parametrize(
    ("raw_board", "expected"),
    [
        ("main_board", "main"),
        ("star_market", "star"),
        ("创业板", "chinext"),
        ("北交所", "bse"),
    ],
)
def test_board_aliases_cover_stock_master_values(raw_board, expected):
    assert normalize_board(raw_board) == expected


def test_selection_accepts_stock_master_board_values():
    catalog = load_disclosure_template_catalog()

    main = catalog.select(
        document_date="2025-12-31",
        exchange="SSE",
        board="main_board",
        document_type="annual_report",
        industry_group="coal",
    )
    star = catalog.select(
        document_date="2025-12-31",
        exchange="SSE",
        board="star_market",
        document_type="annual_report",
        industry_group="coal",
    )

    assert main[1].authority_type == "exchange_industry_rule"
    assert star[1].authority_type == "observed_parser_pattern"


def test_industry_authority_matches_exact_market_scope():
    catalog = load_disclosure_template_catalog()

    szse_coal = catalog.select(
        document_date="2025-12-31",
        exchange="SZSE",
        board="main_board",
        document_type="annual_report",
        industry_group="coal",
    )
    szse_chemical = catalog.select(
        document_date="2025-12-31",
        exchange="SZSE",
        board="创业板",
        document_type="annual_report",
        industry_group="basic_chemical",
    )
    sse_coal_semiannual = catalog.select(
        document_date="2025-12-31",
        exchange="SSE",
        board="main_board",
        document_type="semiannual_report",
        industry_group="coal",
    )
    bse_building_material = catalog.select(
        document_date="2025-12-31",
        exchange="BSE",
        board="北交所",
        document_type="annual_report",
        industry_group="building_material",
    )

    assert szse_coal[1].authority_type == "observed_parser_pattern"
    assert szse_chemical[1].rule_version == "szse_guideline_3_2023_chemical"
    assert szse_chemical[1].authority_type == "exchange_industry_rule"
    assert sse_coal_semiannual[1].authority_type == "observed_parser_pattern"
    assert bse_building_material[1].authority_type == "observed_parser_pattern"


def test_row_role_markers_preserve_reconciliation_rows():
    catalog = load_disclosure_template_catalog()
    common = next(
        template
        for template in catalog.templates
        if template.template_id == "common_periodic_report.v1"
    )
    segment_signature = next(
        signature
        for signature in common.table_signatures
        if signature.signature_id == "common.segment_revenue_cost.v1"
    )

    assert dict(segment_signature.row_role_markers) == {
        "elimination": ("分部间抵销", "内部抵销"),
        "subtotal": ("小计",),
        "total": ("合计",),
    }
    assert "row_exclusions" not in segment_signature.to_dict()


def test_parser_rejects_unknown_fact_field_reference():
    payload = _default_payload()
    payload["templates"][0]["table_signatures"][0]["field_ids"].append(
        "operating.unknown"
    )

    with pytest.raises(ValueError, match="unknown business fact fields"):
        parse_disclosure_template_catalog(
            payload,
            fact_field_ids=_fact_field_ids(),
        )


def test_parser_rejects_unknown_section_reference():
    payload = _default_payload()
    payload["templates"][0]["table_signatures"][0]["section_keys"] = ["missing_section"]

    with pytest.raises(ValueError, match="unknown section keys"):
        parse_disclosure_template_catalog(
            payload,
            fact_field_ids=_fact_field_ids(),
        )


def test_parser_rejects_invalid_header_threshold():
    payload = _default_payload()
    signature = payload["templates"][0]["table_signatures"][0]
    signature["min_required_header_matches"] = len(signature["required_headers"]) + 1

    with pytest.raises(ValueError, match="invalid min_required_header_matches"):
        parse_disclosure_template_catalog(
            payload,
            fact_field_ids=_fact_field_ids(),
        )


def test_parser_rejects_duplicate_signature_ids_across_templates():
    payload = _default_payload()
    duplicate = copy.deepcopy(payload["templates"][1]["table_signatures"][0])
    duplicate["signature_id"] = payload["templates"][0]["table_signatures"][0][
        "signature_id"
    ]
    payload["templates"][1]["table_signatures"].append(duplicate)

    with pytest.raises(ValueError, match="duplicate disclosure table signature_id"):
        parse_disclosure_template_catalog(
            payload,
            fact_field_ids=_fact_field_ids(),
        )


def test_parser_rejects_duplicate_scope_ids():
    payload = _default_payload()
    payload["templates"][1]["scopes"][0]["scope_id"] = payload["templates"][0][
        "scopes"
    ][0]["scope_id"]

    with pytest.raises(ValueError, match="duplicate disclosure rule scope_id"):
        parse_disclosure_template_catalog(
            payload,
            fact_field_ids=_fact_field_ids(),
        )


def test_selection_rejects_overlapping_scopes():
    payload = _default_payload()
    overlapping = copy.deepcopy(payload["templates"][1]["scopes"][1])
    overlapping["scope_id"] = "coal.sse_main.overlap"
    payload["templates"][1]["scopes"].append(overlapping)
    catalog = parse_disclosure_template_catalog(
        payload,
        fact_field_ids=_fact_field_ids(),
    )

    with pytest.raises(ValueError, match="overlapping scopes"):
        catalog.select(
            document_date="2025-12-31",
            exchange="SSE",
            board="main",
            document_type="annual_report",
            industry_group="coal",
        )


def test_parser_rejects_unknown_row_role():
    payload = _default_payload()
    payload["templates"][0]["table_signatures"][0]["row_role_markers"]["drop"] = [
        "合计"
    ]

    with pytest.raises(ValueError, match="unsupported row role"):
        parse_disclosure_template_catalog(
            payload,
            fact_field_ids=_fact_field_ids(),
        )


def test_selection_rejects_unknown_market_dimensions():
    catalog = load_disclosure_template_catalog()

    with pytest.raises(ValueError, match="unsupported disclosure template board"):
        catalog.select(
            document_date="2025-12-31",
            exchange="SSE",
            board="unknown",
            document_type="annual_report",
            industry_group="coal",
        )


def test_selection_rejects_invalid_exchange_board_combination():
    catalog = load_disclosure_template_catalog()

    with pytest.raises(ValueError, match="invalid disclosure template exchange"):
        catalog.select(
            document_date="2025-12-31",
            exchange="BSE",
            board="main",
            document_type="annual_report",
            industry_group="coal",
        )


def test_selection_fails_when_no_template_is_effective():
    catalog = load_disclosure_template_catalog()

    with pytest.raises(ValueError, match="no effective disclosure template"):
        catalog.select(
            document_date="2020-12-31",
            exchange="SSE",
            board="main",
            document_type="annual_report",
            industry_group="coal",
        )


def test_selection_fails_when_matching_industry_template_is_missing():
    payload = _default_payload()
    payload["templates"] = [
        template
        for template in payload["templates"]
        if "coal" not in template["industry_groups"]
    ]
    catalog = parse_disclosure_template_catalog(
        payload,
        fact_field_ids=_fact_field_ids(),
    )

    with pytest.raises(ValueError, match="exactly one industry template"):
        catalog.select(
            document_date="2025-12-31",
            exchange="SSE",
            board="main",
            document_type="annual_report",
            industry_group="coal",
        )


def test_loader_rejects_fact_catalog_version_mismatch(tmp_path):
    payload = _default_payload()
    payload["fact_catalog_version"] = "business_profile_facts.2099.1"
    path = tmp_path / "templates.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    load_disclosure_template_catalog.cache_clear()

    with pytest.raises(ValueError, match="fact catalog version mismatch"):
        load_disclosure_template_catalog(path)
