import asyncio
from dataclasses import replace

import pandas as pd
import pytest

from research.business_profile_product_catalog import load_business_product_catalog
from research.business_profile_governance import BusinessProfileRepository
from research.business_profile_structured_ingestion import (
    StructuredBusinessProfileCandidateWriter,
)
from research.providers.akshare_business_profile import (
    AkshareStructuredBusinessProfileProvider,
    BusinessCompositionRow,
)
from research.storage import ResearchStorageManager
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)


def _repository(tmp_path):
    config = ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(tmp_path / "quotes.db"),
            quotes_db_alias="quotes",
            financials_db_path=str(tmp_path / "financials.db"),
            valuation_db_path=str(tmp_path / "valuation.db"),
            interests_db_path=str(tmp_path / "interests.db"),
        ),
        budget=ResearchBudgetConfig(),
    )
    storage = ResearchStorageManager(config)
    storage.initialize()
    return BusinessProfileRepository(storage)


def _snapshot():
    class _Akshare:
        @staticmethod
        def stock_zygc_em(symbol):
            return pd.DataFrame(
                [
                    {
                        "报告日期": "2025-12-31",
                        "分类类型": "按产品分类",
                        "主营构成": "动力煤",
                        "主营收入": 1000,
                        "收入比例": 0.8,
                        "主营成本": 600,
                        "成本比例": 0.75,
                        "主营利润": 400,
                        "利润比例": 0.9,
                        "毛利率": 0.4,
                    },
                    {
                        "报告日期": "2025-12-31",
                        "分类类型": "按产品分类",
                        "主营构成": "无法映射的新产品",
                        "主营收入": 250,
                        "收入比例": 0.2,
                    },
                ]
            )

        @staticmethod
        def stock_zyjs_ths(symbol):
            return pd.DataFrame([{"主营业务": "煤炭业务", "产品名称": "动力煤"}])

    provider = AkshareStructuredBusinessProfileProvider(
        akshare_module=_Akshare(),
        request_interval_seconds=0,
        retry_backoff_seconds=0,
    )
    return asyncio.run(
        provider.fetch("601088.SH", observed_at="2026-07-18T10:00:00+08:00")
    )


def test_writer_creates_candidate_evidence_and_segments_without_exposures(tmp_path):
    repository = _repository(tmp_path)
    writer = StructuredBusinessProfileCandidateWriter(repository)

    result = writer.write(_snapshot(), industry_group="coal")
    history = repository.get_profile_history("601088.SH")

    assert result["evidence_written"] == 2
    assert result["segment_candidates_written"] == 2
    assert {item["review_status"] for item in history["evidence"]} == {"candidate"}
    assert {item["review_status"] for item in history["segments"]} == {"candidate"}
    assert history["value_chain_roles"] == []
    assert history["exposures"] == []
    mapped = next(
        item for item in history["segments"] if item["segment_name_raw"] == "动力煤"
    )
    unknown = next(
        item
        for item in history["segments"]
        if item["segment_name_raw"] == "无法映射的新产品"
    )
    assert mapped["segment_name_normalized"] == "coal.thermal_coal"
    assert mapped["revenue_share"] == 0.8
    assert mapped["segment_cost"] == 600
    assert mapped["cost_share"] == 0.75
    assert mapped["profit_share"] == 0.9
    assert mapped["gross_margin"] == 0.4
    assert mapped["metadata"]["fact_catalog_version"] == (
        "business_profile_facts.2026.2"
    )
    assert "cost" not in mapped["metadata"]
    assert mapped["metadata"]["commodity_mapping_candidates"]
    assert unknown["segment_name_normalized"] is None
    assert unknown["metadata"]["product_resolution"]["diagnostics"] == [
        "alias_not_found"
    ]


def test_writer_preserves_negative_gross_margin_and_rejects_unknown_segment_type(
    tmp_path,
):
    repository = _repository(tmp_path)
    writer = StructuredBusinessProfileCandidateWriter(repository)
    snapshot = _snapshot()
    negative_margin = replace(
        snapshot.composition.rows[0],
        gross_margin=-0.25,
        source_row_hash="negative-margin",
    )
    writer.write(
        replace(
            snapshot,
            composition=replace(
                snapshot.composition,
                payload_hash="negative-margin-payload",
                rows=(negative_margin,),
            ),
        ),
        industry_group="coal",
    )
    segment = repository.get_profile_history("601088.SH")["segments"][0]
    assert segment["gross_margin"] == -0.25

    invalid = replace(
        negative_margin,
        classification_type="model_invented_dimension",
        source_row_hash="invalid-dimension",
    )
    with pytest.raises(ValueError, match="not supported by fact catalog"):
        writer.write(
            replace(
                snapshot,
                composition=replace(
                    snapshot.composition,
                    payload_hash="invalid-dimension-payload",
                    rows=(invalid,),
                ),
            )
        )
def test_writer_is_idempotent_for_unchanged_source_payload(tmp_path):
    repository = _repository(tmp_path)
    writer = StructuredBusinessProfileCandidateWriter(repository)
    snapshot = _snapshot()

    first = writer.write(snapshot, industry_group="coal")
    second = writer.write(snapshot, industry_group="coal")

    assert first["segment_candidates_written"] == 2
    assert second["evidence_written"] == 0
    assert set(second["unchanged_sources"]) == {
        "eastmoney_main_composition",
        "ths_main_business_intro",
    }
    assert len(repository.get_profile_history("601088.SH")["segments"]) == 2


def test_changed_snapshot_only_writes_new_or_changed_source_rows(tmp_path):
    repository = _repository(tmp_path)
    writer = StructuredBusinessProfileCandidateWriter(repository)
    original = _snapshot()
    writer.write(original, industry_group="coal")
    new_row = BusinessCompositionRow(
        instrument_id="601088.SH",
        report_period="2026-06-30",
        classification_type="product",
        item_name="动力煤",
        revenue=1100,
        revenue_ratio=0.82,
        cost=620,
        cost_ratio=0.76,
        profit=480,
        profit_ratio=0.91,
        gross_margin=0.436,
        source_row_hash="new-period-row",
    )
    changed_composition = replace(
        original.composition,
        payload_hash="changed-payload",
        rows=(*original.composition.rows, new_row),
    )
    changed_snapshot = replace(
        original,
        observed_at="2026-07-19T10:00:00+08:00",
        composition=changed_composition,
    )

    result = writer.write(changed_snapshot, industry_group="coal")
    history = repository.get_profile_history("601088.SH")

    assert result["evidence_written"] == 1
    assert result["segment_candidates_written"] == 1
    assert len(history["segments"]) == 3
    original_rows = [
        item for item in history["segments"] if item["report_period"] == "2025-12-31"
    ]
    assert len(original_rows) == 2
    assert {item["data_available_date"] for item in original_rows} == {"2026-07-18"}


def test_catalog_version_replays_derived_rows_with_supersession(tmp_path):
    repository = _repository(tmp_path)
    original_catalog = load_business_product_catalog()
    original_writer = StructuredBusinessProfileCandidateWriter(
        repository,
        product_catalog=original_catalog,
    )
    snapshot = _snapshot()
    original_writer.write(snapshot, industry_group="coal")
    upgraded_catalog = replace(
        original_catalog,
        catalog_version="business_profile_products.test-upgrade",
    )

    result = StructuredBusinessProfileCandidateWriter(
        repository,
        product_catalog=upgraded_catalog,
    ).write(snapshot, industry_group="coal")
    segments = repository.get_profile_history("601088.SH")["segments"]

    assert result["evidence_written"] == 0
    assert result["segment_candidates_written"] == 2
    assert len(segments) == 4
    upgraded = [
        item
        for item in segments
        if item["metadata"]["product_catalog_version"]
        == "business_profile_products.test-upgrade"
    ]
    assert {item["version"] for item in upgraded} == {2}
    assert all(item["supersedes_record_id"] for item in upgraded)
    assert {item["data_available_date"] for item in upgraded} == {"2026-07-18"}


def test_writer_does_not_turn_empty_source_response_into_business_evidence(tmp_path):
    class _Akshare:
        @staticmethod
        def stock_zygc_em(symbol):
            return pd.DataFrame()

        @staticmethod
        def stock_zyjs_ths(symbol):
            return pd.DataFrame()

    repository = _repository(tmp_path)
    provider = AkshareStructuredBusinessProfileProvider(
        akshare_module=_Akshare(),
        request_interval_seconds=0,
        retry_backoff_seconds=0,
    )
    snapshot = asyncio.run(
        provider.fetch("601088.SH", observed_at="2026-07-18T10:00:00+08:00")
    )
    result = StructuredBusinessProfileCandidateWriter(repository).write(snapshot)

    assert result["evidence_written"] == 0
    assert repository.get_profile_history("601088.SH")["evidence"] == []
