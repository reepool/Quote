import json

import pytest

import research.business_profile_catalog_governance as governance
from research.business_profile_catalog_governance import (
    audit_product_label_resolutions,
    build_product_alias_promotion,
    write_product_alias_promotion,
)
from research.business_profile_product_catalog import (
    DEFAULT_PRODUCT_CATALOG_PATH,
    parse_business_product_catalog,
)


def _segment(
    *,
    record_id,
    raw_label,
    diagnostics,
    product_ids=(),
    version=1,
    instrument_id="600001.SH",
    industry_group="coal",
):
    return {
        "record_id": record_id,
        "instrument_id": instrument_id,
        "report_period": "2025-12-31",
        "segment_type": "product",
        "segment_name_raw": raw_label,
        "review_status": "candidate",
        "version": version,
        "updated_at": f"2026-07-{17 + version:02d}T00:00:00+08:00",
        "metadata": {
            "source_name": "eastmoney_main_composition",
            "source_row_key": f"row:{raw_label}",
            "industry_group": industry_group,
            "product_catalog_version": f"catalog.v{version}",
            "product_resolution": {
                "normalized_alias": raw_label,
                "product_ids": list(product_ids),
                "matched_alias_ids": [],
                "review_required": True,
                "diagnostics": list(diagnostics),
            },
        },
    }


def _catalog_payload():
    return json.loads(DEFAULT_PRODUCT_CATALOG_PATH.read_text(encoding="utf-8"))


def test_label_audit_groups_issues_and_keeps_latest_source_row_version():
    old = _segment(
        record_id="old",
        raw_label="煤制品",
        diagnostics=("alias_not_found",),
    )
    current = _segment(
        record_id="current",
        raw_label="煤制品",
        diagnostics=(),
        product_ids=("coal.thermal_coal",),
        version=2,
    )
    unknown_a = _segment(
        record_id="unknown-a",
        raw_label="新产品",
        diagnostics=("alias_not_found",),
    )
    unknown_b = _segment(
        record_id="unknown-b",
        raw_label="新产品",
        diagnostics=("alias_not_found",),
        instrument_id="600002.SH",
    )
    unknown_b["metadata"]["source_row_key"] = "row:new-product:600002"
    ambiguous = _segment(
        record_id="ambiguous",
        raw_label="煤炭",
        diagnostics=("ambiguous_product_alias",),
        product_ids=("coal.thermal_coal", "coal.coking_coal"),
    )

    result = audit_product_label_resolutions(
        [old, current, unknown_a, unknown_b, ambiguous],
        sample_limit=1,
    )

    assert result["latest_product_rows_examined"] == 4
    assert result["resolved_product_rows"] == 1
    assert result["unmatched_product_rows"] == 2
    assert result["ambiguous_product_rows"] == 1
    unknown = next(
        item for item in result["issues"] if item["normalized_alias"] == "新产品"
    )
    assert unknown["row_count"] == 2
    assert unknown["instrument_count"] == 2
    assert len(unknown["sample_instrument_ids"]) == 1
    assert all(item["normalized_alias"] != "煤制品" for item in result["issues"])


def test_alias_promotion_builds_valid_new_catalog_and_manifest():
    output, manifest = build_product_alias_promotion(
        _catalog_payload(),
        expected_catalog_version="business_profile_products.2026.2",
        new_catalog_version="business_profile_products.2026.3",
        released_on="2026-07-19",
        alias="优质动力煤",
        product_ids=["coal.thermal_coal"],
        industry_groups=["coal"],
        operator="reviewer",
        reason="Official annual report uses this exact product label",
        evidence_references=["cninfo:600001:2025:page-12"],
        promoted_at="2026-07-19T00:00:00+00:00",
    )

    catalog = parse_business_product_catalog(output)
    resolution = catalog.resolve_alias("优质动力煤", industry_group="coal")
    assert resolution.product_ids == ("coal.thermal_coal",)
    assert manifest["source_catalog_version"] == ("business_profile_products.2026.2")
    assert manifest["output_catalog_hash"]
    assert manifest["semantic_inference_performed"] is False


def test_alias_promotion_fails_on_stale_version_or_overlapping_alias():
    payload = _catalog_payload()
    common = {
        "new_catalog_version": "business_profile_products.2026.3",
        "released_on": "2026-07-19",
        "product_ids": ["coal.thermal_coal"],
        "industry_groups": ["coal"],
        "operator": "reviewer",
        "reason": "reviewed",
        "evidence_references": ["official:1"],
    }
    with pytest.raises(ValueError, match="catalog version changed"):
        build_product_alias_promotion(
            payload,
            expected_catalog_version="business_profile_products.stale",
            alias="优质动力煤",
            **common,
        )
    with pytest.raises(ValueError, match="exact alias already exists"):
        build_product_alias_promotion(
            payload,
            expected_catalog_version="business_profile_products.2026.2",
            alias="动力煤",
            **common,
        )


def test_promotion_writer_does_not_overwrite_source_or_existing_output(tmp_path):
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(_catalog_payload(), ensure_ascii=False),
        encoding="utf-8",
    )
    output = tmp_path / "next.json"
    manifest = tmp_path / "promotion.json"
    promotion = {
        "expected_catalog_version": "business_profile_products.2026.2",
        "new_catalog_version": "business_profile_products.2026.3",
        "released_on": "2026-07-19",
        "alias": "优质动力煤",
        "product_ids": ["coal.thermal_coal"],
        "industry_groups": ["coal"],
        "operator": "reviewer",
        "reason": "reviewed",
        "evidence_references": ["official:1"],
        "promoted_at": "2026-07-19T00:00:00+00:00",
    }

    written = write_product_alias_promotion(
        source_path=source,
        output_path=output,
        manifest_path=manifest,
        **promotion,
    )

    assert output.exists()
    assert manifest.exists()
    assert written["output_catalog_version"] == ("business_profile_products.2026.3")
    with pytest.raises(FileExistsError):
        write_product_alias_promotion(
            source_path=source,
            output_path=output,
            manifest_path=manifest,
            **promotion,
        )
    with pytest.raises(ValueError, match="paths must be distinct"):
        write_product_alias_promotion(
            source_path=source,
            output_path=source,
            manifest_path=manifest,
            **promotion,
        )


def test_promotion_writer_rolls_back_catalog_when_manifest_publish_fails(
    tmp_path,
    monkeypatch,
):
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(_catalog_payload(), ensure_ascii=False),
        encoding="utf-8",
    )
    output = tmp_path / "next.json"
    manifest = tmp_path / "promotion.json"
    original_replace = governance.os.replace

    def _replace(source_path, destination_path):
        if destination_path == manifest:
            raise OSError("manifest publish failed")
        return original_replace(source_path, destination_path)

    monkeypatch.setattr(governance.os, "replace", _replace)

    with pytest.raises(OSError, match="manifest publish failed"):
        write_product_alias_promotion(
            source_path=source,
            output_path=output,
            manifest_path=manifest,
            expected_catalog_version="business_profile_products.2026.2",
            new_catalog_version="business_profile_products.2026.3",
            released_on="2026-07-19",
            alias="优质动力煤",
            product_ids=["coal.thermal_coal"],
            industry_groups=["coal"],
            operator="reviewer",
            reason="reviewed",
            evidence_references=["official:1"],
        )

    assert source.exists()
    assert not output.exists()
    assert not manifest.exists()
