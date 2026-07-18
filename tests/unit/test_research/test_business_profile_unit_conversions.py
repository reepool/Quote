import copy
import json
from decimal import Decimal

import pytest

from research.business_profile_unit_conversions import (
    DEFAULT_UNIT_CONVERSION_CATALOG_PATH,
    load_unit_conversion_catalog,
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

    assert catalog.catalog_version == "business_profile_units.2026.1"
    assert catalog.fact_catalog_version == "business_profile_facts.2026.1"
    assert dimensions == canonical_dimensions
    assert len(catalog.units) == 36
    assert len(catalog.conversions) == 17


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
