from pathlib import Path
from io import BytesIO

import pandas as pd

from data_sources.hkex_instrument_master import (
    HKEXLifecyclePolicy,
    HKEXManualReviewProvider,
    HKEXNewsStockListProvider,
    HKEXSecuritiesListProvider,
    HKEXSourceEvidencePolicy,
    HKEXSuspensionReportProvider,
    HKEXSupplementalAdapter,
    build_quote_availability_diagnostics,
    build_dual_counter_map,
    classify_hkex_product,
    hkex_instrument_id,
    normalize_hkex_code,
)


FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "hkex_instrument_master"


def _fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


def test_normalizes_hkex_codes_to_five_digit_instrument_ids():
    assert normalize_hkex_code("5") == "00005"
    assert normalize_hkex_code("09988.HK") == "09988"
    assert hkex_instrument_id("823") == "00823.HK"


def test_official_hkex_securities_list_parser_adds_lineage_and_classification():
    snapshot = HKEXSecuritiesListProvider(
        source_url="fixture://hkex_securities_list.csv"
    ).parse_csv(_fixture("hkex_securities_list.csv"))

    assert snapshot.source == "hkex_securities_list"
    assert snapshot.parser_version
    assert snapshot.raw_snapshot_hash
    assert snapshot.diagnostics["row_count"] == 8

    by_id = {row["instrument_id"]: row for row in snapshot.rows}
    assert by_id["00005.HK"]["product_type"] == "ordinary_equity"
    assert by_id["00005.HK"]["is_research_equity"] is True
    assert by_id["02800.HK"]["product_type"] == "etf"
    assert by_id["00823.HK"]["product_type"] == "reit"
    assert by_id["50000.HK"]["product_type"] == "cbbc"
    assert by_id["22000.HK"]["product_type"] == "warrant"
    assert by_id["02929.HK"]["product_type"] == "temporary_counter"
    assert by_id["00005.HK"]["official_lifecycle_source"] == "hkex_securities_list"


def test_official_hkex_securities_list_parser_supports_live_excel_layout():
    frame = pd.DataFrame([
        ["List of Securities", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["Updated as at 03/06/2026", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        [
            "Stock Code",
            "Name of Securities",
            "Category",
            "Sub-Category",
            "Board Lot",
            "ISIN",
            "Expiry Date",
            "Subject to Stamp Duty",
            "Shortsell Eligible",
            "CAS Eligible",
            "VCM Eligible",
            "Admitted to CCASS",
            "Debt Securities Board Lot (Nominal)",
            "Debt Securities Investor Type",
            "POS Eligible",
            "Spread Table\n1 = Part A\n3 = Part B",
            "Trading Currency",
            "RMB Counter",
        ],
        ["00005", "HSBC HOLDINGS", "Equity", "Equity Securities (Main Board)", "400", "GB0005405286", "", "Y", "Y", "Y", "Y", "Y", "", "", "Y", "1", "HKD", ""],
        ["89988", "BABA-WR", "Equity", "Equity Securities (Main Board)", "100", "KYG017191142", "", "Y", "Y", "Y", "Y", "Y", "", "", "Y", "1", "CNY", "Y"],
    ])
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, header=False)

    snapshot = HKEXSecuritiesListProvider().parse_excel(buffer.getvalue())

    by_id = {row["instrument_id"]: row for row in snapshot.rows}
    assert snapshot.diagnostics["format"] == "excel"
    assert by_id["00005.HK"]["currency"] == "HKD"
    assert by_id["89988.HK"]["currency"] == "CNY"
    assert by_id["89988.HK"]["rmb_counter"] == "Y"


def test_hkexnews_active_and_delisted_lists_carry_lifecycle_evidence():
    provider = HKEXNewsStockListProvider(source_url="fixture://hkexnews")

    active = provider.parse_html(_fixture("hkexnews_active_list.html"), lifecycle_status="active")
    delisted = provider.parse_html(
        _fixture("hkexnews_delisted_list.html"),
        lifecycle_status="delisted",
    )

    assert active.diagnostics["row_count"] == 4
    assert active.rows[0]["status"] == "active"
    assert active.rows[0]["is_active"] is True
    assert active.rows[0]["lifecycle_evidence"]["source"] == "hkexnews_active_list"

    by_id = {row["instrument_id"]: row for row in delisted.rows}
    assert by_id["02929.HK"]["status"] == "delisted"
    assert by_id["02929.HK"]["delisted_date"] == "2026-05-24"
    assert by_id["02929.HK"]["lifecycle_evidence"]["source"] == "hkexnews_delisted_list"


def test_hkexnews_json_lists_are_lifecycle_evidence():
    active = HKEXNewsStockListProvider(
        source_url="https://www.hkexnews.hk/ncms/script/eds/activestock_sehk_e.json"
    ).parse_json('[{"i":1,"c":"00005","n":"HSBC HOLDINGS","s":7224}]', lifecycle_status="active")
    delisted = HKEXNewsStockListProvider(
        source_url="https://www.hkexnews.hk/ncms/script/eds/inactivestock_sehk_e.json"
    ).parse_json('[{"i":12,"c":"00008","n":"CWHKT","s":215}]', lifecycle_status="delisted")

    assert active.rows[0]["instrument_id"] == "00005.HK"
    assert active.rows[0]["status"] == "active"
    assert active.rows[0]["lifecycle_evidence"]["format"] == "json"
    assert delisted.rows[0]["instrument_id"] == "00008.HK"
    assert delisted.rows[0]["status"] == "delisted"


def test_suspension_report_text_parser_emits_official_suspended_rows():
    snapshot = HKEXSuspensionReportProvider(
        source_url="fixture://psuspenrep_mb.pdf",
        market="Main Board",
    ).parse_text(
        """
        1. This report summarises the status of companies.
        11  Renco Holdings
        Group Limited (In
        Liquidation) (2323)
        20-Jan-2025 19-Jul-2026 1. Conduct an independent forensic investigation
        Link to HKEXnews
        12  Lufax Holding Ltd
        (6623)
        28-Jan-2025 27-Jul-2026 1. Conduct an independent forensic investigation
        Link to HKEXnews
        4.  Greentech Technology
        International Limited (195)
        2-Sep-2024 1-Mar-2026 1. Publish outstanding financial results
        Link to HKEXnews
        """
    )

    by_id = {row["instrument_id"]: row for row in snapshot.rows}
    assert snapshot.source == "hkexnews_suspension_report"
    assert "00001.HK" not in by_id
    assert by_id["02323.HK"]["status"] == "suspended"
    assert by_id["02323.HK"]["trading_status"] == 0
    assert by_id["06623.HK"]["official_lifecycle_source"] == "hkexnews_suspension_report"
    assert by_id["00195.HK"]["status"] == "suspended"


def test_manual_review_provider_turns_operator_conclusions_into_lifecycle_evidence():
    snapshot = HKEXManualReviewProvider(source_url="fixture://manual_review.json").parse_json(
        """
        [
          {
            "instrument_id": "02934.HK",
            "action": "delisted",
            "effective_date": "2026-05-30",
            "reason": "manual official review",
            "evidence_url": "https://www.hkexnews.hk/"
          },
          {"code": "00005", "action": "suspended"}
        ]
        """
    )

    by_id = {row["instrument_id"]: row for row in snapshot.rows}
    assert by_id["02934.HK"]["status"] == "delisted"
    assert by_id["02934.HK"]["delisted_date"] == "2026-05-30"
    assert by_id["00005.HK"]["status"] == "suspended"
    assert by_id["00005.HK"]["source"] == "hkex_manual_review"


def test_source_evidence_policy_blocks_safe_write_when_primary_active_source_is_missing():
    active = HKEXNewsStockListProvider().parse_json(
        '[{"c":"00005","n":"HSBC"}]',
        lifecycle_status="active",
    )

    policy = HKEXSourceEvidencePolicy.assess(
        snapshots=[active],
        errors=[],
        official_active_rows=active.rows,
        official_delisted_rows=[],
    )

    assert policy["active_fallback_used"] is True
    assert policy["safe_write_allowed"] is False
    assert policy["reactivation_write_allowed"] is False


def test_supplemental_adapters_are_non_authoritative_for_lifecycle():
    akshare = HKEXSupplementalAdapter.parse_akshare_spot_csv(
        _fixture("akshare_hk_spot_em.csv"),
        source_url="fixture://akshare",
    )
    eastmoney = HKEXSupplementalAdapter.parse_eastmoney_profile_csv(
        _fixture("eastmoney_hk_profile_rows.csv"),
        source_url="fixture://eastmoney",
    )

    assert akshare.diagnostics["row_count"] == 5
    assert eastmoney.diagnostics["row_count"] == 4
    assert all(row["lifecycle_authoritative"] is False for row in akshare.rows)
    assert all(row["lifecycle_authoritative"] is False for row in eastmoney.rows)


def test_quote_availability_diagnostics_do_not_emit_lifecycle_mutations():
    diagnostics = build_quote_availability_diagnostics(
        local_rows=[
            {"instrument_id": "00005.HK", "last_quote": "2026-06-02", "quote_stale": False},
            {"instrument_id": "00907.HK", "last_quote": None, "quote_stale": True},
        ],
        yfinance_rows=[
            {"instrument_id": "00005.HK", "last_quote": "2026-06-02"},
            {"instrument_id": "08888.HK", "last_quote": "2026-06-02"},
        ],
    )

    assert diagnostics["lifecycle_authoritative"] is False
    assert diagnostics["no_local_quote_samples"] == ["00907.HK"]
    assert diagnostics["stale_local_quote_samples"] == ["00907.HK"]
    assert diagnostics["yfinance_only_quote_samples"] == ["08888.HK"]
    assert diagnostics["mutation_candidates"] == []


def test_dual_counter_mapping_selects_hkd_leg_as_canonical():
    official = HKEXSecuritiesListProvider().parse_csv(_fixture("hkex_securities_list.csv"))

    mapping = build_dual_counter_map(official.rows)

    assert mapping["09988.HK"]["canonical_instrument_id"] == "09988.HK"
    assert mapping["89988.HK"]["canonical_instrument_id"] == "09988.HK"
    assert mapping["09988.HK"]["is_canonical"] is True
    assert mapping["89988.HK"]["is_canonical"] is False


def test_product_classifier_separates_derivatives_debt_funds_and_equity():
    assert classify_hkex_product({"category": "Equity", "sub_category": "Ordinary Shares"})["product_type"] == "ordinary_equity"
    assert classify_hkex_product({"sub_category": "Exchange Traded Fund"})["product_type"] == "etf"
    assert classify_hkex_product({"sub_category": "Real Estate Investment Trust"})["product_type"] == "reit"
    assert classify_hkex_product({"sub_category": "Debt Securities"})["product_type"] == "debt"
    assert classify_hkex_product({"sub_category": "Inline Warrant"})["product_type"] == "inline_warrant"
    assert classify_hkex_product({"sub_category": "Callable Bull/Bear Contract"})["product_type"] == "cbbc"
    temporary = classify_hkex_product({"instrument_id": "02955.HK", "name": "GOFINTECH-2000"})
    assert temporary["product_type"] == "temporary_counter"
    assert temporary["research_scope"] == "exclude"


def test_product_classifier_excludes_hkex_official_special_code_ranges():
    cases = {
        "00290.HK": "ordinary_equity",
        "08239.HK": "ordinary_equity",
        "04332.HK": "trading_only",
        "10000.HK": "warrant",
        "47000.HK": "inline_warrant",
        "50000.HK": "cbbc",
        "30000.HK": "stock_connect_special_counter",
        "80016.HK": "rmb_counter",
        "89988.HK": "rmb_counter",
        "90000.HK": "stock_connect_special_counter",
        "07200.HK": "leveraged_inverse_product",
        "06300.HK": "restricted_security",
    }

    for instrument_id, product_type in cases.items():
        result = classify_hkex_product({
            "instrument_id": instrument_id,
            "category": "Equity",
            "sub_category": "Equity Securities (Main Board)",
        })
        assert result["product_type"] == product_type
        if instrument_id not in {"00290.HK", "08239.HK"}:
            assert result["research_scope"] == "exclude"


def test_product_classifier_excludes_rmb_and_trading_only_metadata():
    rmb = classify_hkex_product({
        "instrument_id": "09988.HK",
        "currency": "CNY",
        "rmb_counter": "Y",
        "category": "Equity",
        "sub_category": "Equity Securities (Main Board)",
    })
    trading_only = classify_hkex_product({
        "instrument_id": "04335.HK",
        "category": "Equity",
        "sub_category": "Trading Only Securities",
    })

    assert rmb["product_type"] == "rmb_counter"
    assert rmb["research_scope"] == "exclude"
    assert trading_only["product_type"] == "trading_only"
    assert trading_only["research_scope"] == "exclude"
    rights = classify_hkex_product({"instrument_id": "08556.HK", "name": "NIUHOLDINGS RTS"})
    assert rights["product_type"] == "temporary_counter"
    assert rights["research_scope"] == "exclude"
    named_not_rights = classify_hkex_product({"instrument_id": "01234.HK", "name": "SAMPLE RTS"})
    assert named_not_rights["product_type"] == "ordinary_equity"
    unit_suffix = classify_hkex_product({"instrument_id": "00290.HK", "name": "GOFINTECH-500"})
    assert unit_suffix["product_type"] == "ordinary_equity"
    assert classify_hkex_product({"instrument_id": "82901.HK", "name": "TEMP RMB"})["product_type"] == "temporary_counter"
    gem_normal = classify_hkex_product({"instrument_id": "08619.HK", "name": "NIU HOLDINGS"})
    assert gem_normal["product_type"] == "ordinary_equity"


def test_lifecycle_policy_requires_official_evidence_for_reactivation_and_delisting():
    official = HKEXSecuritiesListProvider().parse_csv(_fixture("hkex_securities_list.csv"))
    delisted = HKEXNewsStockListProvider().parse_html(
        _fixture("hkexnews_delisted_list.html"),
        lifecycle_status="delisted",
    )
    supplemental = HKEXSupplementalAdapter.parse_akshare_spot_csv(
        _fixture("akshare_hk_spot_em.csv")
    )
    local_rows = [
        {"instrument_id": "00005.HK", "status": "active", "is_active": True},
        {"instrument_id": "00907.HK", "status": "active", "is_active": True},
        {"instrument_id": "02929.HK", "status": "active", "is_active": True},
        {"instrument_id": "09988.HK", "status": "auto_deactivated_zombie", "is_active": False},
    ]

    decisions = HKEXLifecyclePolicy.build_decisions(
        local_rows=local_rows,
        official_active_rows=official.rows,
        official_delisted_rows=delisted.rows,
        supplemental_rows=supplemental.rows,
    )

    reactivation_ids = {row["instrument_id"] for row in decisions["reactivation_candidates"]}
    delisting_ids = {row["instrument_id"] for row in decisions["delisting_candidates"]}
    review_ids = {row["instrument_id"] for row in decisions["review_required"]}

    assert "09988.HK" in reactivation_ids
    assert "02929.HK" not in delisting_ids
    assert "02929.HK" not in review_ids
    assert "00907.HK" in delisting_ids
    assert "00907.HK" not in reactivation_ids
    assert "00907.HK" not in review_ids


def test_lifecycle_policy_keeps_supplemental_only_rows_in_review():
    supplemental = [{"instrument_id": "08888.HK", "source": "akshare_hk_spot_em"}]

    decisions = HKEXLifecyclePolicy.build_decisions(
        local_rows=[],
        official_active_rows=[],
        official_delisted_rows=[],
        supplemental_rows=supplemental,
    )

    assert decisions["review_required"][0]["instrument_id"] == "08888.HK"
    assert decisions["review_required"][0]["reason"] == "supplemental_only_candidate_requires_official_confirmation"


def test_lifecycle_policy_requires_official_evidence_for_suspension():
    decisions = HKEXLifecyclePolicy.build_decisions(
        local_rows=[{"instrument_id": "00005.HK", "status": "active", "is_active": True}],
        official_active_rows=[
            {
                "instrument_id": "00005.HK",
                "status": "suspended",
                "source": "hkexnews_active_list",
            }
        ],
        official_delisted_rows=[],
        supplemental_rows=[{"instrument_id": "00005.HK", "source": "akshare_hk_spot_em"}],
    )

    assert decisions["suspension_candidates"][0]["instrument_id"] == "00005.HK"
    assert decisions["counts"]["suspension_candidates"] == 1
