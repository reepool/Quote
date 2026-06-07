import io
from pathlib import Path

import pandas as pd

from research.providers.tls_support import build_ca_bundle_with_extra_certificate
from research.providers.swsresearch_shenwan_classification import (
    SWSResearchShenwanClassificationProvider,
)


def test_swsresearch_provider_reads_official_excel_artifacts():
    provider = SWSResearchShenwanClassificationProvider(
        minimum_stock_history_rows=1,
        minimum_code_rows=1,
    )
    code_buffer = io.BytesIO()
    with pd.ExcelWriter(code_buffer, engine="openpyxl") as writer:
        pd.DataFrame(
            [
                {
                    "行业代码": "340000",
                    "一级行业名称": "食品饮料",
                    "二级行业名称": None,
                    "三级行业名称": None,
                }
            ]
        ).to_excel(writer, index=False)
    stock_buffer = io.BytesIO()
    with pd.ExcelWriter(stock_buffer, engine="openpyxl") as writer:
        pd.DataFrame(
            [
                {
                    "股票代码": "600519",
                    "计入日期": "2024-01-02",
                    "行业代码": "340501",
                    "更新日期": "2024-01-03",
                }
            ]
        ).to_excel(writer, index=False)

    code_frame = provider._read_single_sheet_excel(
        code_buffer.getvalue(),
        expected_columns=provider.REQUIRED_CODE_COLUMNS,
        artifact_kind=provider.CODE_TABLE_ARTIFACT,
    )
    stock_frame = provider._read_single_sheet_excel(
        stock_buffer.getvalue(),
        expected_columns=provider.REQUIRED_STOCK_COLUMNS,
        artifact_kind=provider.STOCK_HISTORY_ARTIFACT,
    )

    assert list(code_frame.columns) == list(provider.REQUIRED_CODE_COLUMNS)
    assert list(stock_frame.columns) == list(provider.REQUIRED_STOCK_COLUMNS)
    assert code_frame.iloc[0]["行业代码"] == "340000"
    assert stock_frame.iloc[0]["股票代码"] == "600519"


def test_swsresearch_extra_ca_bundle_keeps_tls_verification_enabled():
    cert_path = "config/certs/geotrust_g2_tls_cn_rsa4096_sha256_2022_ca1.crt"
    bundle = build_ca_bundle_with_extra_certificate(cert_path)
    provider = SWSResearchShenwanClassificationProvider(extra_ca_cert_path=cert_path)

    assert bundle is not True
    assert Path(str(bundle)).exists()
    assert provider.request_verify == bundle
    assert Path(cert_path).read_text().strip() in Path(str(bundle)).read_text()


def test_swsresearch_provider_uses_known_extra_ca_by_default():
    provider = SWSResearchShenwanClassificationProvider()

    assert provider.request_verify is not True
    assert Path(str(provider.request_verify)).exists()


def test_swsresearch_extra_ca_bundle_resolves_project_relative_path_from_other_cwd(
    monkeypatch,
    tmp_path,
):
    cert_path = "config/certs/geotrust_g2_tls_cn_rsa4096_sha256_2022_ca1.crt"
    monkeypatch.chdir(tmp_path)

    bundle = build_ca_bundle_with_extra_certificate(cert_path)

    assert bundle is not True
    assert Path(str(bundle)).exists()
    project_cert = Path(__file__).resolve().parents[3] / cert_path
    assert project_cert.read_text().strip() in Path(str(bundle)).read_text()


def test_swsresearch_provider_parses_official_taxonomy_levels():
    provider = SWSResearchShenwanClassificationProvider(
        minimum_stock_history_rows=1,
        minimum_code_rows=1,
    )
    frame = pd.DataFrame(
        [
            {
                "行业代码": "340000",
                "一级行业名称": "食品饮料",
                "二级行业名称": None,
                "三级行业名称": None,
            },
            {
                "行业代码": "340500",
                "一级行业名称": "食品饮料",
                "二级行业名称": "白酒Ⅱ",
                "三级行业名称": None,
            },
            {
                "行业代码": "340501",
                "一级行业名称": "食品饮料",
                "二级行业名称": "白酒Ⅱ",
                "三级行业名称": "白酒Ⅲ",
            },
        ]
    )

    nodes = provider._parse_taxonomy_nodes(frame)

    assert [(node.industry_code, node.industry_level, node.parent_code) for node in nodes] == [
        ("340000", 1, None),
        ("340500", 2, "340000"),
        ("340501", 3, "340500"),
    ]
    assert nodes[0].source == "swsresearch"
    assert nodes[2].raw_payload["level_3_name"] == "白酒Ⅲ"


def test_swsresearch_provider_builds_latest_membership_from_history():
    provider = SWSResearchShenwanClassificationProvider(
        minimum_stock_history_rows=1,
        minimum_code_rows=1,
    )
    taxonomy_nodes = provider._parse_taxonomy_nodes(
        pd.DataFrame(
            [
                {
                    "行业代码": "340000",
                    "一级行业名称": "食品饮料",
                    "二级行业名称": None,
                    "三级行业名称": None,
                },
                {
                    "行业代码": "340500",
                    "一级行业名称": "食品饮料",
                    "二级行业名称": "白酒Ⅱ",
                    "三级行业名称": None,
                },
                {
                    "行业代码": "340501",
                    "一级行业名称": "食品饮料",
                    "二级行业名称": "白酒Ⅱ",
                    "三级行业名称": "白酒Ⅲ",
                },
            ]
        )
    )
    history_rows = provider._parse_history_rows(
        pd.DataFrame(
            [
                {
                    "股票代码": "600519",
                    "计入日期": "2022-01-01",
                    "行业代码": "340500",
                    "更新日期": "2022-01-02",
                },
                {
                    "股票代码": "600519.SH",
                    "计入日期": "2024-01-01",
                    "行业代码": "340501",
                    "更新日期": "2024-01-02",
                },
                {
                    "股票代码": "600519.SH",
                    "计入日期": "2021-01-01",
                    "行业代码": "999999",
                    "更新日期": "2026-01-02",
                },
                {
                    "股票代码": "920001",
                    "计入日期": "2024-02-01",
                    "行业代码": "340501",
                    "更新日期": "2024-02-02",
                },
            ]
        )
    )
    latest = provider._latest_classifications_from_history(history_rows)

    memberships = provider.build_latest_memberships(
        instruments=[
            {
                "instrument_id": "600519.SH",
                "symbol": "600519",
                "exchange": "SSE",
                "type": "stock",
            },
            {
                "instrument_id": "920001.BJ",
                "symbol": "920001",
                "exchange": "BSE",
                "type": "stock",
            },
        ],
        taxonomy_nodes=taxonomy_nodes,
        latest_classifications=latest,
    )

    assert [row.instrument_id for row in memberships] == ["600519.SH", "920001.BJ"]
    assert memberships[0].industry_code == "340501"
    assert memberships[0].sw_l1_code == "340000"
    assert memberships[0].sw_l2_code == "340500"
    assert memberships[0].sw_l3_code == "340501"
    assert memberships[0].mapping_status == "authoritative"
    assert history_rows[-1].exchange == "BSE"
    assert history_rows[-1].instrument_id == "920001.BJ"


def test_swsresearch_provider_applies_configured_symbol_alias():
    provider = SWSResearchShenwanClassificationProvider(
        minimum_stock_history_rows=1,
        minimum_code_rows=1,
        symbol_aliases=[
            {
                "instrument_id": "302132.SZ",
                "current_symbol": "302132",
                "official_symbol": "300114",
                "exchange": "SZSE",
                "reason": "code change",
            }
        ],
    )
    history_rows = provider._parse_history_rows(
        pd.DataFrame(
            [
                {
                    "股票代码": "300114",
                    "计入日期": "2021-07-30",
                    "行业代码": "650501",
                    "更新日期": "2025-01-24",
                }
            ]
        )
    )

    latest = provider._apply_symbol_aliases(
        provider._latest_classifications_from_history(history_rows)
    )

    alias_rows = [row for row in latest if row.symbol == "302132"]
    assert len(alias_rows) == 1
    assert alias_rows[0].instrument_id == "302132.SZ"
    assert alias_rows[0].official_industry_code == "650501"
    assert alias_rows[0].raw_payload["symbol_alias"]["official_symbol"] == "300114"
