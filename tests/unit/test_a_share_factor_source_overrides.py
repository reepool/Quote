import json

import pytest

from data_sources.a_share_factor_source_overrides import (
    CATALOG_PATH,
    FactorSourceOverrideCatalogError,
    load_factor_source_override_catalog,
)


def test_reviewed_factor_source_catalog_contains_operator_decisions():
    entries = load_factor_source_override_catalog(CATALOG_PATH)

    assert sorted(entries) == ["000004.SZ", "600455.SH"]
    assert {entry.selected_source for entry in entries.values()} == {"tdx"}
    assert {entry.scope for entry in entries.values()} == {"whole_lifecycle"}


def test_reviewed_factor_source_catalog_rejects_unknown_source(tmp_path):
    path = tmp_path / "overrides.json"
    path.write_text(
        json.dumps({
            "catalog_version": "unit-v1",
            "reviewed_at": "2026-07-31",
            "instruments": {
                "000004.SZ": {
                    "instrument_id": "000004.SZ",
                    "selected_source": "unknown",
                    "scope": "whole_lifecycle",
                    "reason": "invalid-test",
                }
            },
        }),
        encoding="utf-8",
    )

    with pytest.raises(
        FactorSourceOverrideCatalogError,
        match="unsupported selected_source",
    ):
        load_factor_source_override_catalog(path)


def test_reviewed_factor_source_catalog_rejects_invalid_instrument_id(
    tmp_path,
):
    path = tmp_path / "overrides.json"
    path.write_text(
        json.dumps({
            "catalog_version": "unit-v1",
            "reviewed_at": "2026-07-31",
            "instruments": {
                "4.SZ": {
                    "instrument_id": "4.SZ",
                    "selected_source": "tdx",
                    "scope": "whole_lifecycle",
                    "reason": "invalid-test",
                }
            },
        }),
        encoding="utf-8",
    )

    with pytest.raises(
        FactorSourceOverrideCatalogError,
        match="invalid instrument_id",
    ):
        load_factor_source_override_catalog(path)


def test_reviewed_factor_source_catalog_allows_empty_decision_set(tmp_path):
    path = tmp_path / "overrides.json"
    path.write_text(
        json.dumps({
            "catalog_version": "unit-v1",
            "reviewed_at": "2026-07-31",
            "instruments": {},
        }),
        encoding="utf-8",
    )

    assert load_factor_source_override_catalog(path) == {}


@pytest.mark.parametrize(
    "duplicate_key",
    ["000004.SZ", "000004.sz"],
)
def test_reviewed_factor_source_catalog_rejects_duplicate_normalized_key(
    tmp_path,
    duplicate_key,
):
    path = tmp_path / "overrides.json"
    entry = (
        '{"instrument_id":"000004.SZ","selected_source":"tdx",'
        '"scope":"whole_lifecycle","reason":"reviewed"}'
    )
    path.write_text(
        "{"
        '"catalog_version":"unit-v1",'
        '"reviewed_at":"2026-07-31",'
        f'"instruments":{{"000004.SZ":{entry},"{duplicate_key}":{entry}}}'
        "}",
        encoding="utf-8",
    )

    with pytest.raises(
        FactorSourceOverrideCatalogError,
        match="duplicate normalized catalog key",
    ):
        load_factor_source_override_catalog(path)
