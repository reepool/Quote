import copy
import json

import pytest

from research.business_profile_product_catalog import (
    DEFAULT_PRODUCT_CATALOG_PATH,
    INDUSTRY_GROUPS,
    load_business_product_catalog,
    load_known_commodity_references,
    load_known_commodity_price_series,
    parse_business_product_catalog,
)


def _payload():
    return json.loads(DEFAULT_PRODUCT_CATALOG_PATH.read_text(encoding="utf-8"))


def _known_references():
    return load_known_commodity_references()


def _known_price_series():
    return load_known_commodity_price_series()


def test_default_product_catalog_covers_first_wave_industries():
    catalog = load_business_product_catalog()

    assert catalog.catalog_version == "business_profile_products.2026.4"
    assert len(catalog.products) == 41
    assert len(catalog.aliases) == 49
    assert len(catalog.commodity_mappings) == 45
    assert {
        industry for product in catalog.products for industry in product.industry_groups
    } == INDUSTRY_GROUPS


def test_unique_and_ambiguous_aliases_have_different_review_outcomes():
    catalog = load_business_product_catalog()

    copper = catalog.resolve_alias(
        " 阴极铜 ",
        industry_group="nonferrous_and_solid_mineral",
    )
    coal = catalog.resolve_alias("煤炭", industry_group="coal")

    assert copper.product_ids == ("metal.refined_copper",)
    assert copper.review_required is False
    assert coal.product_ids == ("coal.coking_coal", "coal.thermal_coal")
    assert coal.review_required is True
    assert "ambiguous_product_alias" in coal.diagnostics


def test_alias_resolution_uses_exact_source_label_without_prose_analysis():
    catalog = load_business_product_catalog()

    generic = catalog.resolve_alias(
        "玻璃",
        industry_group="building_material",
    )
    exact = catalog.resolve_alias(
        "平板玻璃",
        industry_group="building_material",
    )
    embedded = catalog.resolve_alias(
        "玻璃纤维",
        industry_group="building_material",
    )

    assert generic.product_ids == ("building.flat_glass",)
    assert generic.review_required is True
    assert exact.product_ids == ("building.flat_glass",)
    assert exact.review_required is False
    assert embedded.product_ids == ()
    assert "alias_not_found" in embedded.diagnostics


def test_product_without_market_series_is_preserved_without_mapping():
    catalog = load_business_product_catalog()

    cement = catalog.require_product("building.cement")
    mappings = catalog.commodity_candidates("building.cement")

    assert cement.label_zh == "水泥"
    assert mappings == ()


def test_generic_polyethylene_is_distinct_from_lldpe_price_series():
    catalog = load_business_product_catalog()

    resolution = catalog.resolve_alias("聚乙烯", industry_group="petrochemical")
    mapping = catalog.commodity_candidates(
        "polymer.polyethylene",
        exposure_role="revenue",
        evidence_requirement="explicit_product",
    )

    assert resolution.product_ids == ("polymer.polyethylene",)
    assert resolution.review_required is False
    assert len(mapping) == 1
    assert mapping[0].commodity_id == "COMMODITY.polymer.polyethylene"
    assert mapping[0].candidate_only is True


def test_same_product_supports_role_specific_revenue_and_cost_candidates():
    catalog = load_business_product_catalog()

    iron_ore = catalog.commodity_candidates("mineral.iron_ore")
    crude_oil = catalog.commodity_candidates("energy.crude_oil")

    assert {mapping.exposure_role for mapping in iron_ore} == {
        "feedstock_cost",
        "revenue",
    }
    assert {mapping.exposure_role for mapping in crude_oil} == {
        "feedstock_cost",
        "revenue",
    }
    assert {
        mapping.exposure_role
        for mapping in catalog.commodity_candidates(
            "energy.crude_oil",
            evidence_requirement="explicit_product",
        )
    } == {"revenue"}


def test_all_mapping_targets_exist_and_starter_cohort_is_bounded():
    catalog = load_business_product_catalog()
    references = _known_references()

    promoted = [
        mapping for mapping in catalog.commodity_mappings if not mapping.candidate_only
    ]
    assert 1 <= len(promoted) <= 6
    assert all(mapping.promotion_evidence for mapping in promoted)
    for mapping in catalog.commodity_mappings:
        assert mapping.commodity_id != mapping.product_id
        for target in mapping.targets:
            assert target.reference_id in references[target.reference_type]
    for mapping in promoted:
        assert len(mapping.targets) == 1
        assert (
            mapping.targets[0].price_series_id
            in _known_price_series()[mapping.targets[0].reference_id]
        )


def test_loader_accepts_historical_supported_document_date():
    load_business_product_catalog.cache_clear()

    catalog = load_business_product_catalog(document_date="2025-12-31")
    assert catalog.document_applicable_from == "2021-01-01"
    with pytest.raises(ValueError, match="not applicable to document_date"):
        load_business_product_catalog(document_date="2020-12-31")


def test_parser_rejects_unknown_commodity_reference():
    payload = _payload()
    payload["commodity_mappings"][0]["targets"][0]["reference_id"] = "CNF.UNKNOWN.TEST"

    with pytest.raises(ValueError, match="unknown futures_instrument"):
        parse_business_product_catalog(
            payload,
            known_references=_known_references(),
            known_price_series=_known_price_series(),
        )


def test_parser_rejects_ambiguous_alias_without_review():
    payload = _payload()
    alias = next(item for item in payload["aliases"] if len(item["product_ids"]) > 1)
    alias["review_policy"] = "auto_candidate_if_unique"

    with pytest.raises(ValueError, match="ambiguous alias requires"):
        parse_business_product_catalog(
            payload,
            known_references=_known_references(),
            known_price_series=_known_price_series(),
        )


def test_parser_rejects_promoted_mapping_without_evidence():
    payload = _payload()
    mapping = next(
        item for item in payload["commodity_mappings"] if item["candidate_only"]
    )
    mapping["candidate_only"] = False

    with pytest.raises(
        ValueError, match="one single target|price_series_id|promotion_evidence"
    ):
        parse_business_product_catalog(
            payload,
            known_references=_known_references(),
            known_price_series=_known_price_series(),
        )


def test_parser_rejects_duplicate_product():
    payload = _payload()
    payload["products"].append(copy.deepcopy(payload["products"][0]))

    with pytest.raises(ValueError, match="duplicate business product_id"):
        parse_business_product_catalog(
            payload,
            known_references=_known_references(),
            known_price_series=_known_price_series(),
        )
