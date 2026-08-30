import copy
import json
from decimal import Decimal

import pytest

from research.business_profile_unit_conversions import (
    DEFAULT_UNIT_CONVERSION_CATALOG_PATH,
    UnitResolution,
    load_unit_conversion_catalog,
    normalize_unit_lexeme,
    parse_unit_conversion_catalog,
)
from research.business_profile_fact_catalog import load_business_fact_catalog


def _payload():
    return json.loads(DEFAULT_UNIT_CONVERSION_CATALOG_PATH.read_text(encoding="utf-8"))


def test_default_unit_catalog_has_canonical_unit_for_each_dimension():
    catalog = load_unit_conversion_catalog()

    dimensions = {unit.dimension for unit in catalog.units}
    canonical_dimensions = {
        unit.dimension for unit in catalog.units if unit.canonical_for_dimension
    }

    assert catalog.catalog_version == "business_profile_units.2026.7"
    assert catalog.fact_catalog_version == "business_profile_facts.2026.3"
    assert dimensions == canonical_dimensions
    assert len(catalog.units) == 44
    assert len(catalog.conversions) == 23


def test_unit_catalog_covers_all_business_fact_canonical_units():
    catalog = load_unit_conversion_catalog()
    fact_catalog = load_business_fact_catalog()

    configured_units = {unit.unit_id for unit in catalog.units}
    required_units = {
        unit
        for definition in fact_catalog.definitions
        for unit in definition.canonical_units
    }

    assert required_units <= configured_units


def test_unit_alias_resolution_is_deterministic():
    catalog = load_unit_conversion_catalog()

    assert catalog.resolve_unit("万吨").unit_id == "10k_tonne"
    assert catalog.resolve_unit("万元").unit_id == "10k_CNY"
    assert catalog.resolve_unit("CNY/ton").unit_id == "CNY/tonne"
    assert catalog.resolve_unit("吨每年").unit_id == "tonne/year"


def test_fixed_conversion_preserves_raw_and_basis_lineage():
    catalog = load_unit_conversion_catalog()

    result = catalog.convert(
        "125.5",
        from_unit="万吨",
        to_unit="tonne",
        period_basis="full_year",
        equity_basis="consolidated_100_percent",
    )

    assert result.raw_value == Decimal("125.5")
    assert result.raw_unit == "10k_tonne"
    assert result.normalized_value == Decimal("1255000.0")
    assert result.multiplier == Decimal("10000")
    assert result.period_basis == "full_year"
    assert result.equity_basis == "consolidated_100_percent"


def test_inverse_fixed_conversion_is_explicit_in_lineage():
    catalog = load_unit_conversion_catalog()

    result = catalog.convert(
        "1000",
        from_unit="tonne",
        to_unit="kg",
        period_basis="period_total",
        equity_basis="project_100_percent",
    )

    assert result.normalized_value == Decimal("1000000")
    assert result.conversion_rule_id == "mass.kg_to_tonne:inverse"


def test_ratio_and_price_conversions_use_exact_decimal():
    catalog = load_unit_conversion_catalog()

    ratio = catalog.convert(
        "12.5",
        from_unit="percent",
        to_unit="fraction",
        period_basis="instant",
        equity_basis="unknown",
    )
    price = catalog.convert(
        "1.25",
        from_unit="CNY/kg",
        to_unit="CNY/tonne",
        period_basis="instant",
        equity_basis="unknown",
    )

    assert ratio.normalized_value == Decimal("0.125")
    assert price.normalized_value == Decimal("1250.00")


def test_currency_conversion_requires_external_lineage():
    catalog = load_unit_conversion_catalog()

    with pytest.raises(ValueError, match="requires external lineage"):
        catalog.convert(
            "100",
            from_unit="CNY",
            to_unit="USD",
            period_basis="instant",
            equity_basis="unknown",
        )


def test_dimension_period_and_equity_mismatches_fail_closed():
    catalog = load_unit_conversion_catalog()

    with pytest.raises(ValueError, match="dimension mismatch"):
        catalog.convert(
            "1",
            from_unit="tonne",
            to_unit="barrel",
            period_basis="period_total",
            equity_basis="unknown",
        )
    with pytest.raises(ValueError, match="unsupported period_basis"):
        catalog.convert(
            "1",
            from_unit="tonne",
            to_unit="kg",
            period_basis="annualized_guess",
            equity_basis="unknown",
        )
    with pytest.raises(ValueError, match="unsupported equity_basis"):
        catalog.convert(
            "1",
            from_unit="tonne",
            to_unit="kg",
            period_basis="period_total",
            equity_basis="assumed_equity",
        )


def test_parser_rejects_nonpositive_fixed_multiplier():
    payload = _payload()
    fixed = next(
        item
        for item in payload["conversions"]
        if item["conversion_type"] == "fixed_multiplier"
    )
    fixed["multiplier"] = "0"

    with pytest.raises(ValueError, match="requires positive multiplier"):
        parse_unit_conversion_catalog(payload)


def test_parser_rejects_alias_collision():
    payload = _payload()
    payload["units"][1]["aliases"].append(payload["units"][0]["aliases"][0])

    with pytest.raises(ValueError, match="unit alias collision"):
        parse_unit_conversion_catalog(payload)


def test_parser_rejects_duplicate_unit():
    payload = _payload()
    payload["units"].append(copy.deepcopy(payload["units"][0]))

    with pytest.raises(ValueError, match="duplicate unit_id"):
        parse_unit_conversion_catalog(payload)


def test_loader_rejects_fact_catalog_version_mismatch(tmp_path):
    payload = _payload()
    payload["fact_catalog_version"] = "business_profile_facts.2099.1"
    path = tmp_path / "units.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    load_unit_conversion_catalog.cache_clear()

    with pytest.raises(ValueError, match="fact catalog version mismatch"):
        load_unit_conversion_catalog(path)


@pytest.mark.parametrize(
    ("raw_unit", "dimension", "canonical", "multiplier"),
    [
        ("千只", "count", "unit", Decimal("1000")),
        ("万台（套）", "count", "unit", Decimal("10000")),
        ("亿千瓦时", "energy", "kwh", Decimal("100000000")),
        ("kW", "power", "watt", Decimal("1000")),
        ("元/吨", "price_per_mass", "CNY/tonne", Decimal("1")),
        ("元/公斤", "price_per_mass", "CNY/tonne", Decimal("1000")),
        ("吨每年", "mass_capacity", "tonne/year", Decimal("1")),
        ("CNY hundred million", "currency", "CNY", Decimal("100000000")),
        ("人民币元", "currency", "CNY", Decimal("1")),
        ("万粒", "count", "unit", Decimal("10000")),
        ("万羽", "count", "unit", Decimal("10000")),
        ("个/片/套/只", "count", "unit", Decimal("1")),
        ("瓶/支/盒/袋/板", "count", "unit", Decimal("1")),
        ("瓶/袋/支", "count", "unit", Decimal("1")),
        ("万Ah", "electric_charge", "Ah", Decimal("10000")),
        ("万张", "count", "unit", Decimal("10000")),
        ("项", "count", "unit", Decimal("1")),
        ("艘", "count", "unit", Decimal("1")),
        ("套/项", "count", "unit", Decimal("1")),
        ("万重箱", "mass", "tonne", Decimal("500")),
        ("万重量箱", "mass", "tonne", Decimal("500")),
        ("重箱", "mass", "tonne", Decimal("0.05")),
        ("重量箱", "mass", "tonne", Decimal("0.05")),
        ("点", "count", "unit", Decimal("1")),
        ("万粒/万瓶", "count", "unit", Decimal("10000")),
        ("PCS", "count", "unit", Decimal("1")),
        ("pcs", "count", "unit", Decimal("1")),
        ("piece", "count", "unit", Decimal("1")),
        ("pieces", "count", "unit", Decimal("1")),
        ("平方", "area", "square_meter", Decimal("1")),
        ("立方", "volume", "cubic_meter", Decimal("1")),
        ("吨千米", "freight_turnover", "tonne_km", Decimal("1")),
        ("亿吨千米", "freight_turnover", "tonne_km", Decimal("100000000")),
        ("元币种：人民币", "currency", "CNY", Decimal("1")),
        ("单位：元 币种：人民币", "currency", "CNY", Decimal("1")),
        ("mAh", "electric_charge", "Ah", Decimal("0.001")),
        ("kAh", "electric_charge", "Ah", Decimal("1000")),
    ],
)
def test_compositional_resolution_handles_chinese_and_si_units(
    raw_unit, dimension, canonical, multiplier
):
    catalog = load_unit_conversion_catalog()
    resolution = catalog.resolve(raw_unit)
    assert isinstance(resolution, UnitResolution)
    assert resolution.status == "resolved"
    assert resolution.dimension == dimension
    assert resolution.canonical_unit == canonical
    assert resolution.multiplier == multiplier


@pytest.mark.parametrize(
    ("raw_unit", "dimension", "canonical", "multiplier"),
    [
        ("m", "length", "meter", Decimal("1")),
        ("g", "mass", "tonne", Decimal("0.000001")),
        ("mm", "length", "meter", Decimal("0.001")),
        ("mg", "mass", "tonne", Decimal("0.000000001")),
        ("Mt", "mass", "tonne", Decimal("1000000")),
        ("kt", "mass", "tonne", Decimal("1000")),
    ],
)
def test_si_prefix_case_and_complete_tokens(raw_unit, dimension, canonical, multiplier):
    resolution = load_unit_conversion_catalog().resolve(raw_unit)
    assert resolution.status == "resolved"
    assert resolution.dimension == dimension
    assert resolution.canonical_unit == canonical
    assert resolution.multiplier == multiplier


@pytest.mark.parametrize("raw_unit", ["M", "G", "k"])
def test_bare_si_prefix_is_pending(raw_unit):
    resolution = load_unit_conversion_catalog().resolve(raw_unit)
    assert resolution.status == "unit_resolution_pending"


def test_unknown_unit_is_pending_and_does_not_raise():
    resolution = load_unit_conversion_catalog().resolve("每百枚神秘单位")
    assert resolution.status == "unit_resolution_pending"
    assert resolution.publishable is False
    assert resolution.reason in {"unknown_unit_token", "unsupported_compound_unit"}


def test_pcs_alias_is_exact_and_does_not_rewrite_product_text():
    resolution = load_unit_conversion_catalog().resolve("储能PCS")

    assert resolution.status == "unit_resolution_pending"
    assert resolution.reason == "unknown_unit_token"


def test_currency_header_alias_is_exact_and_does_not_rewrite_prose():
    resolution = load_unit_conversion_catalog().resolve("本表单位为元币种人民币")

    assert resolution.status == "unit_resolution_pending"


def test_cross_dimension_parenthesized_unit_remains_pending():
    resolution = load_unit_conversion_catalog().resolve("万台（万千瓦时）")

    assert resolution.status == "unit_resolution_pending"
    assert resolution.reason == "cross_dimension_alternative"


def test_unit_normalization_is_unicode_and_punctuation_stable():
    assert normalize_unit_lexeme("  万台（套） ") == "万台(套)"
    assert normalize_unit_lexeme("吨每年") == "吨/年"
    assert normalize_unit_lexeme("（%）") == "%"
    assert normalize_unit_lexeme("[(元/吨)]") == "元/吨"


def test_enclosed_ratio_unit_resolves_without_changing_source_lineage():
    resolution = load_unit_conversion_catalog().resolve(
        "（%）", required_dimension="ratio"
    )

    assert resolution.status == "resolved"
    assert resolution.source_unit == "（%）"
    assert resolution.normalized_lexeme == "%"
    assert resolution.canonical_unit == "fraction"
    assert resolution.multiplier == Decimal("0.01")


def test_ampere_hour_does_not_implicitly_convert_to_energy():
    resolution = load_unit_conversion_catalog().resolve(
        "万Ah", required_dimension="energy"
    )

    assert resolution.status == "unit_resolution_pending"
    assert resolution.dimension == "electric_charge"
    assert resolution.reason == "dimension_mismatch"


def test_runtime_auto_approved_overlay_is_publishable_and_shadow_is_opt_in():
    catalog = load_unit_conversion_catalog()
    rules = [
        {
            "rule_id": "runtime:箱",
            "normalized_lexeme": "箱",
            "dimension": "count",
            "canonical_unit": "unit",
            "multiplier": "24",
            "status": "auto_approved",
        },
        {
            "rule_id": "runtime:托",
            "normalized_lexeme": "托",
            "dimension": "count",
            "canonical_unit": "unit",
            "multiplier": "10",
            "status": "shadow_active",
        },
    ]
    assert catalog.resolve("箱", runtime_rules=rules).publishable
    assert catalog.resolve("托", runtime_rules=rules).status == "unit_resolution_pending"
    assert (
        catalog.resolve("托", runtime_rules=rules, allow_shadow=True).status
        == "shadow_active"
    )
