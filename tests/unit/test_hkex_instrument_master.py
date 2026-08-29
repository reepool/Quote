from pathlib import Path
from io import BytesIO

import pandas as pd

from data_sources.hkex_instrument_master import (
    HKEXLifecyclePolicy,
    HKEXManualReviewProvider,
    HKEXNewsStockListProvider,
    HKEXProviderSnapshot,
    HKEXSecuritiesListProvider,
    HKEXSourceEvidencePolicy,
    HKEXSuspensionReportProvider,
    HKEXSupplementalAdapter,
    build_quote_availability_diagnostics,
    build_dual_counter_map,
    classify_hkex_product,
    hkex_instrument_id,
    normalize_hkex_code,
    hkex_source_usage_key,
    should_skip_prolonged_suspension_reactivation,
    should_write_hkex_reactivation,
)
from research.announcements import AnnouncementRecord, build_announcement_key


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


def test_suspension_report_text_parser_accepts_pdfium_single_space_company_rows():
    snapshot = HKEXSuspensionReportProvider(
        source_url="fixture://psuspenrep_mb.pdfium.txt",
        market="Main Board",
    ).parse_text(
        """
        1. This report summarises the status of companies which have been suspended for
        three months or more.
        1. China Longevity
        Group Company
        Limited (formerly
        known as Sijia
        Group Company
        Limited) (1863) ^
        14-Feb-2013 31-Jul-2019 1. Approval of resumption by Securities and
        Futures Commission (SFC)
        2. Inform market of material information
        Link to HKEXnews
        2. Wisdom Wealth
        Resources
        Investment
        Holding Group Limited (7) *
        2-Apr-2024 1-Oct-2025 1. Publish all outstanding financial results
        Link to HKEXnews
        """
    )

    by_id = {row["instrument_id"]: row for row in snapshot.rows}
    assert set(by_id) == {"01863.HK", "00007.HK"}
    assert by_id["01863.HK"]["status"] == "suspended"
    assert by_id["00007.HK"]["trading_status"] == 0
    assert "00001.HK" not in by_id


def test_suspension_parse_pdf_uses_shared_profile_router(monkeypatch):
    from research.document_processing.pdf.profiles import DEFAULT_PROFILES

    captured = {}

    class _Page:
        text = (
            "11  Renco Holdings\n"
            "(2323)\n"
            "20-Jan-2025 19-Jul-2026 1. Conduct an investigation\n"
            "Link to HKEXnews\n"
        )

    class _Result:
        status = "success"
        page_count = 1
        pages = (_Page(),)

    class _Router:
        def parse(self, request):
            captured["request_profile"] = request.profile.name
            return _Result()

    def fake_resolve(name=None):
        captured["requested_profile"] = name
        return DEFAULT_PROFILES["pypdf_native"]

    def fake_build(profile, **kwargs):
        captured["built_profile"] = profile.name
        return _Router()

    monkeypatch.setattr("research.document_processing.pdf.resolve_profile", fake_resolve)
    monkeypatch.setattr("research.document_processing.pdf.build_router", fake_build)

    snapshot = HKEXSuspensionReportProvider(
        source_url="fixture://psuspenrep_mb.pdf",
        market="Main Board",
        profile_name="pypdf_native",
    ).parse_pdf(b"%PDF-1.4 test")

    assert captured["requested_profile"] == "pypdf_native"
    assert captured["built_profile"] == "pypdf_native"
    assert captured["request_profile"] == "pypdf_native"
    assert snapshot.diagnostics["pdf_profile"] == "pypdf_native"
    assert snapshot.rows[0]["instrument_id"] == "02323.HK"


def test_suspension_native_pdf_with_approved_gpu_profile_bypasses_unavailable_worker(monkeypatch, tmp_path):
    from data_sources.hkex_instrument_master import HKEXSuspensionReportProvider
    from tests.unit.pdf_gpu_caller_test_support import (
        configure_approved_gpu_profile_with_unavailable_worker,
        text_pdf_bytes,
    )

    worker_calls = configure_approved_gpu_profile_with_unavailable_worker(monkeypatch, tmp_path)
    snapshot = HKEXSuspensionReportProvider(
        source_url="fixture://psuspenrep_mb.pdf",
        market="Main Board",
        profile_name="pdfium_paddleocr_gpu",
    ).parse_pdf(
        text_pdf_bytes(
            "11 Renco Holdings",
            "(2323)",
            "20-Jan-2025 19-Jul-2026 1. Conduct an investigation",
            "Link to HKEXnews",
        )
    )

    assert snapshot.rows[0]["instrument_id"] == "02323.HK"
    assert snapshot.diagnostics["pdf_profile"] == "pdfium_paddleocr_gpu"
    assert worker_calls == []


def test_source_evidence_policy_treats_empty_suspension_snapshot_as_unavailable():
    official = HKEXSecuritiesListProvider(
        source_url="fixture://hkex_securities_list.csv"
    ).parse_csv(_fixture("hkex_securities_list.csv"))
    empty = HKEXSuspensionReportProvider(
        source_url="fixture://psuspenrep_mb.pdf",
        market="Main Board",
    ).parse_text("1. This report summarises the status of companies.")

    assert empty.rows == []
    policy = HKEXSourceEvidencePolicy.assess(
        snapshots=[official, empty],
        errors=[],
        official_active_rows=official.rows,
        official_delisted_rows=[],
    )

    assert policy["suspension_source_available"] is False
    assert policy["suspension_write_allowed"] is False
    assert policy["reactivation_write_allowed"] is True


def test_source_evidence_policy_accepts_nonempty_suspension_snapshot():
    official = HKEXSecuritiesListProvider(
        source_url="fixture://hkex_securities_list.csv"
    ).parse_csv(_fixture("hkex_securities_list.csv"))
    suspension = HKEXSuspensionReportProvider(
        source_url="fixture://psuspenrep_mb.pdf",
        market="Main Board",
    ).parse_text(
        """
        11  Renco Holdings
        Group Limited (2323)
        20-Jan-2025 19-Jul-2026 1. Conduct an independent forensic investigation
        Link to HKEXnews
        """
    )

    policy = HKEXSourceEvidencePolicy.assess(
        snapshots=[official, suspension],
        errors=[],
        official_active_rows=official.rows,
        official_delisted_rows=[],
    )

    assert policy["suspension_source_available"] is True
    assert policy["suspension_write_allowed"] is True


def test_source_evidence_policy_accounts_prolonged_suspension_markets_separately():
    official = HKEXSecuritiesListProvider(
        source_url="fixture://hkex_securities_list.csv"
    ).parse_csv(_fixture("hkex_securities_list.csv"))
    gem = HKEXSuspensionReportProvider(
        source_url="fixture://psuspenrep_gem.pdf",
        market="GEM",
    ).parse_text(
        """
        1  LINK REIT
        (823)
        20-Jan-2025 19-Jul-2026 1. Conduct an independent forensic investigation
        Link to HKEXnews
        """
    )

    policy = HKEXSourceEvidencePolicy.assess(
        snapshots=[official, gem],
        errors=[],
        official_active_rows=official.rows,
        official_delisted_rows=[],
        prolonged_suspension_markets={
            "Main Board": {"status": "failed", "row_count": 0},
            "GEM": {"status": "success", "row_count": 1},
        },
    )

    assert policy["prolonged_suspension_source_available"] is True
    assert policy["prolonged_suspension_available_markets"] == ["GEM"]
    assert policy["prolonged_suspension_all_configured_available"] is False
    assert policy["prolonged_suspension_markets"]["Main Board"]["status"] == "failed"
    assert policy["prolonged_suspension_markets"]["GEM"]["status"] == "success"


def test_listing_presence_cannot_clear_pdf_suspension_from_failed_market():
    policy = {
        "prolonged_suspension_available_markets": ["GEM"],
        "prolonged_suspension_all_configured_available": False,
    }
    listing = {"source": "hkex_securities_list", "status": "active"}
    resumption = {"source": "hkexnews_trading_resumption", "status": "active"}

    assert should_skip_prolonged_suspension_reactivation(
        {"status": "suspended", "source": "hkexnews_suspension_report"},
        listing,
        policy,
    )
    assert should_skip_prolonged_suspension_reactivation(
        {
            "status": "suspended",
            "source": "hkexnews_suspension_report",
            "market": "Main Board",
        },
        listing,
        policy,
    )
    assert not should_skip_prolonged_suspension_reactivation(
        {
            "status": "suspended",
            "source": "hkexnews_suspension_report",
            "market": "GEM",
        },
        listing,
        policy,
    )
    assert not should_skip_prolonged_suspension_reactivation(
        {"status": "suspended", "source": "hkexnews_suspension_report"},
        resumption,
        policy,
    )


def test_source_usage_key_keeps_main_board_and_gem_separate():
    main_board = HKEXSuspensionReportProvider(
        source_url="fixture://psuspenrep_mb.pdf",
        market="Main Board",
    ).parse_text(
        """
        1  HSBC HOLDINGS
        (5)
        20-Jan-2025 19-Jul-2026 1. Conduct an independent forensic investigation
        Link to HKEXnews
        """
    )
    gem = HKEXSuspensionReportProvider(
        source_url="fixture://psuspenrep_gem.pdf",
        market="GEM",
    ).parse_text(
        """
        1  LINK REIT
        (823)
        20-Jan-2025 19-Jul-2026 1. Conduct an independent forensic investigation
        Link to HKEXnews
        """
    )
    listing = HKEXProviderSnapshot(
        source="hkex_securities_list",
        source_url="fixture://list",
        parser_version="test",
        raw_snapshot_hash="list",
        rows=[],
        diagnostics={"row_count": 8},
    )

    assert hkex_source_usage_key(main_board) == "hkexnews_suspension_report:Main Board"
    assert hkex_source_usage_key(gem) == "hkexnews_suspension_report:GEM"
    assert hkex_source_usage_key(listing) == "hkex_securities_list"


def test_should_write_hkex_reactivation_matches_write_gates():
    policy = {
        "reactivation_write_allowed": True,
        "suspension_source_available": True,
        "prolonged_suspension_available_markets": ["GEM"],
        "prolonged_suspension_all_configured_available": False,
    }
    listing = {"source": "hkex_securities_list", "status": "active"}
    skipped = {
        "instrument_id": "00005.HK",
        "local": {
            "status": "suspended",
            "source": "hkexnews_suspension_report",
            "market": "Main Board",
        },
        "official": listing,
    }
    allowed = {
        "instrument_id": "00823.HK",
        "local": {
            "status": "suspended",
            "source": "hkexnews_suspension_report",
            "market": "GEM",
        },
        "official": listing,
    }
    allowed_ids = {"00005.HK", "00823.HK"}

    assert should_write_hkex_reactivation(skipped, policy, allowed_ids) is False
    assert should_write_hkex_reactivation(allowed, policy, allowed_ids) is True
    assert should_write_hkex_reactivation(
        allowed,
        {**policy, "reactivation_write_allowed": False},
        allowed_ids,
    ) is False


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
          {"code": "00005", "action": "suspended"},
          {
            "instrument_id": "03038.HK",
            "action": "active",
            "effective_date": "2026-08-20",
            "reason": "operator restored misclassified cessation"
          }
        ]
        """
    )

    by_id = {row["instrument_id"]: row for row in snapshot.rows}
    assert by_id["02934.HK"]["status"] == "delisted"
    assert by_id["02934.HK"]["delisted_date"] == "2026-05-30"
    assert by_id["00005.HK"]["status"] == "suspended"
    assert by_id["00005.HK"]["source"] == "hkex_manual_review"
    assert by_id["03038.HK"]["status"] == "active"
    assert by_id["03038.HK"]["trading_status"] == 1
    assert str(by_id["03038.HK"]["lifecycle_evidence_at"]).startswith("2026-08-20")


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


def test_lifecycle_policy_delisted_overrides_stale_scan_active_row():
    decisions = HKEXLifecyclePolicy.build_decisions(
        local_rows=[
            {
                "instrument_id": "00907.HK",
                "status": "active",
                "is_active": True,
            }
        ],
        official_active_rows=[
            {
                "instrument_id": "00907.HK",
                "status": "active",
                "trading_status": 1,
                "source": "hkexnews_trading_resumption",
            }
        ],
        official_delisted_rows=[
            {
                "instrument_id": "00907.HK",
                "status": "delisted",
                "source": "hkexnews_delisted_list",
                "delisted_date": "2026-05-24",
            }
        ],
    )

    assert [row["instrument_id"] for row in decisions["delisting_candidates"]] == [
        "00907.HK"
    ]
    assert decisions["reactivation_candidates"] == []
    assert decisions["metadata_update_candidates"] == []
    assert decisions["review_required"] == []


def test_lifecycle_policy_listing_and_delisted_conflict_goes_to_review():
    decisions = HKEXLifecyclePolicy.build_decisions(
        local_rows=[
            {
                "instrument_id": "00005.HK",
                "status": "active",
                "is_active": True,
            }
        ],
        official_active_rows=[
            {
                "instrument_id": "00005.HK",
                "status": "active",
                "source": "hkex_securities_list",
                "listing_source_present": True,
            }
        ],
        official_delisted_rows=[
            {
                "instrument_id": "00005.HK",
                "status": "delisted",
                "source": "hkexnews_delisted_list",
            }
        ],
    )

    assert decisions["delisting_candidates"] == []
    assert decisions["metadata_update_candidates"] == []
    assert decisions["review_required"][0]["instrument_id"] == "00005.HK"
    assert decisions["review_required"][0]["reason"] == (
        "official_active_and_delisted_evidence_conflict"
    )


def test_product_cessation_without_resume_date_is_sticky_local_state():
    from datetime import date

    from data_sources.hkex_instrument_master import is_hkex_sticky_untradable_local

    local = {
        "instrument_id": "03038.HK",
        "trading_status": 0,
        "source": "hkexnews_product_cessation",
        "lifecycle_evidence_at": "2026-07-28T04:00:00+00:00",
    }
    assert is_hkex_sticky_untradable_local(local)
    assert is_hkex_sticky_untradable_local(
        {
            **local,
            "expected_resume_date": "2026-09-02",
        },
        as_of=date(2026, 9, 1),
    )
    assert is_hkex_sticky_untradable_local(
        local,
        official={
            "instrument_id": "03038.HK",
            "status": "active",
            "trading_status": 1,
            "source": "hkex_securities_list",
        },
        as_of=date(2026, 8, 28),
    )
    assert not is_hkex_sticky_untradable_local(
        {
            "instrument_id": "01712.HK",
            "trading_status": 0,
            "source": "hkexnews_trading_arrangement",
        }
    )
    assert not is_hkex_sticky_untradable_local(
        {
            "instrument_id": "03038.HK",
            "trading_status": 1,
            "source": "hkexnews_product_cessation",
        }
    )
    assert not is_hkex_sticky_untradable_local(
        {
            **local,
            "expected_resume_date": "2026-09-02",
        },
        as_of=date(2026, 9, 2),
    )
    assert not is_hkex_sticky_untradable_local(
        local,
        official={
            "instrument_id": "03038.HK",
            "status": "active",
            "trading_status": 1,
            "source": "hkex_manual_review",
            "lifecycle_evidence_at": "2026-08-20",
        },
    )
    assert not is_hkex_sticky_untradable_local(
        local,
        official={
            "instrument_id": "03038.HK",
            "status": "active",
            "trading_status": 1,
            "source": "hkexnews_trading_resumption",
            "lifecycle_evidence_at": "2026-08-17T00:46:00+00:00",
        },
    )


def test_source_evidence_policy_does_not_treat_missing_scan_as_complete():
    official = HKEXSecuritiesListProvider(
        source_url="fixture://hkex_securities_list.csv"
    ).parse_csv(_fixture("hkex_securities_list.csv"))

    policy = HKEXSourceEvidencePolicy.assess(
        snapshots=[official],
        errors=[],
        official_active_rows=official.rows,
        official_delisted_rows=[],
        trading_status_scan=None,
    )

    assert policy["trading_status_scan_complete"] is False
    assert policy["untradable_restore_allowed"] is False


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


def _hkexnews_record(
    *,
    announcement_id: str,
    title: str,
    published_at: str,
    symbols: tuple[str, ...],
    headline_category: str | None = None,
    short_text: str = "",
    long_text: str = "",
) -> AnnouncementRecord:
    payload = {
        "TITLE": title,
        "SHORT_TEXT": short_text,
        "LONG_TEXT": long_text,
    }
    if headline_category is not None:
        payload["headline_category"] = headline_category
    return AnnouncementRecord(
        source="hkexnews",
        source_announcement_id=announcement_id,
        announcement_key=build_announcement_key("hkexnews", announcement_id),
        title=title,
        published_at=published_at,
        exchange="HKEX",
        market="SEHK",
        symbols=symbols,
        raw_payload=payload,
    )


def test_trading_status_classifier_uses_headline_category_not_title():
    from data_sources.hkex_instrument_master import classify_hkex_trading_status_headline

    record = _hkexnews_record(
        announcement_id="1803-resume",
        title="TRADING HALT AND RESUMPTION OF TRADING",
        published_at="2026-05-12T04:00:00+00:00",
        symbols=("01803",),
        headline_category="trading_resumption",
        short_text="[Resumption]",
    )

    assert classify_hkex_trading_status_headline(record) == "trading_resumption"


def test_trading_status_snapshot_later_event_overrides_earlier_halt_or_resumption():
    from data_sources.hkex_instrument_master import (
        HKEX_TRADING_HALT_SOURCE,
        HKEX_TRADING_RESUMPTION_SOURCE,
        build_hkex_trading_status_snapshot,
    )

    snapshot = build_hkex_trading_status_snapshot(
        [
            _hkexnews_record(
                announcement_id="1632-resume",
                title="RESUMPTION OF TRADING",
                published_at="2026-06-29T01:00:00+00:00",
                symbols=("01632",),
                headline_category="trading_resumption",
                short_text="[Resumption]",
            ),
            _hkexnews_record(
                announcement_id="1831-halt",
                title="TRADING HALT",
                published_at="2026-08-24T04:21:00+00:00",
                symbols=("01831",),
                headline_category="trading_halt",
                short_text="[Trading Halt]",
            ),
            _hkexnews_record(
                announcement_id="1632-halt-older",
                title="TRADING HALT",
                published_at="2026-03-01T01:00:00+00:00",
                symbols=("01632",),
                headline_category="trading_halt",
                short_text="[Trading Halt]",
            ),
            _hkexnews_record(
                announcement_id="1566-continued",
                title="CONTINUED SUSPENSION OF TRADING",
                published_at="2026-08-11T14:53:00+00:00",
                symbols=("01566",),
                headline_category="trading_suspension",
            ),
            _hkexnews_record(
                announcement_id="1566-resume",
                title="EXCHANGE NOTICE - RESUMPTION OF TRADING",
                published_at="2026-08-17T00:46:00+00:00",
                symbols=("01566",),
                headline_category="trading_resumption",
                short_text="[Resumption]",
            ),
        ]
    )

    by_id = {row["instrument_id"]: row for row in snapshot.rows}
    assert snapshot.source == HKEX_TRADING_HALT_SOURCE
    assert by_id["01831.HK"]["status"] == "suspended"
    assert by_id["01831.HK"]["trading_status"] == 0
    assert by_id["01831.HK"]["source"] == HKEX_TRADING_HALT_SOURCE
    assert by_id["01632.HK"]["status"] == "active"
    assert by_id["01632.HK"]["trading_status"] == 1
    assert by_id["01632.HK"]["source"] == HKEX_TRADING_RESUMPTION_SOURCE
    assert by_id["01566.HK"]["status"] == "active"
    assert by_id["01566.HK"]["trading_status"] == 1
    assert by_id["01566.HK"]["source"] == HKEX_TRADING_RESUMPTION_SOURCE
    assert str(by_id["01566.HK"]["lifecycle_evidence_at"]).startswith("2026-08-17")


def test_trading_status_snapshot_rejects_continued_suspension_stamped_as_resumption():
    from data_sources.hkex_instrument_master import (
        HKEX_TRADING_HALT_SOURCE,
        build_hkex_trading_status_snapshot,
    )

    snapshot = build_hkex_trading_status_snapshot(
        [
            _hkexnews_record(
                announcement_id="1566-continued",
                title="JOINT ANNOUNCEMENT - CONTINUED SUSPENSION OF TRADING",
                published_at="2026-06-30T12:32:00+00:00",
                symbols=("01566",),
                headline_category="trading_resumption",
            )
        ]
    )

    row = snapshot.rows[0]
    assert row["instrument_id"] == "01566.HK"
    assert row["status"] == "suspended"
    assert row["source"] == HKEX_TRADING_HALT_SOURCE


def test_trading_status_snapshot_keeps_future_resumption_untradable_until_resume_date():
    from datetime import date

    from data_sources.hkex_instrument_master import (
        HKEX_TRADING_RESUMPTION_SOURCE,
        build_hkex_trading_status_snapshot,
    )

    records = [
        _hkexnews_record(
            announcement_id="1831-future-resume",
            title="TRADING WILL RESUME ON 2 SEPTEMBER 2026",
            published_at="2026-08-27T04:00:00+00:00",
            symbols=("01831",),
            headline_category="trading_resumption",
            short_text="[Resumption]",
        )
    ]

    before = build_hkex_trading_status_snapshot(records, as_of=date(2026, 8, 27))
    after = build_hkex_trading_status_snapshot(records, as_of=date(2026, 9, 2))

    pending = before.rows[0]
    assert pending["instrument_id"] == "01831.HK"
    assert pending["status"] == "suspended"
    assert pending["trading_status"] == 0
    assert pending["source"] == HKEX_TRADING_RESUMPTION_SOURCE
    assert pending["lifecycle_evidence"]["expected_resume_date"] == "2026-09-02"

    resumed = after.rows[0]
    assert resumed["status"] == "active"
    assert resumed["trading_status"] == 1
    assert resumed["source"] == HKEX_TRADING_RESUMPTION_SOURCE


def test_trading_status_snapshot_later_halt_overrides_earlier_resumption():
    from data_sources.hkex_instrument_master import (
        HKEX_TRADING_HALT_SOURCE,
        build_hkex_trading_status_snapshot,
    )

    snapshot = build_hkex_trading_status_snapshot(
        [
            _hkexnews_record(
                announcement_id="5-resume",
                title="RESUMPTION OF TRADING",
                published_at="2026-08-10T01:00:00+00:00",
                symbols=("00005",),
                headline_category="trading_resumption",
                short_text="[Resumption]",
            ),
            _hkexnews_record(
                announcement_id="5-halt",
                title="TRADING HALT",
                published_at="2026-08-12T04:00:00+00:00",
                symbols=("00005",),
                headline_category="trading_halt",
                short_text="[Trading Halt]",
            ),
        ]
    )

    row = snapshot.rows[0]
    assert row["instrument_id"] == "00005.HK"
    assert row["status"] == "suspended"
    assert row["source"] == HKEX_TRADING_HALT_SOURCE


def test_suspension_report_stamps_report_as_of_as_lifecycle_evidence():
    snapshot = HKEXSuspensionReportProvider(
        source_url="fixture://psuspenrep_mb.pdf",
        market="Main Board",
    ).parse_text(
        """
        (Posted on 31 July 2026)
        MONTHLY PROLONGED SUSPENSION STATUS REPORT (MAIN BOARD)
        (as at 31 July 2026)
        6. CA Cultural Technology Group Limited (1566)
        21-Nov-2024 20-May-2026 1. Publish all outstanding financial results
        Link to HKEXnews
        """
    )

    row = snapshot.rows[0]
    assert row["instrument_id"] == "01566.HK"
    assert str(row["lifecycle_evidence_at"]).startswith("2026-07-31")


def test_later_resumption_overrides_earlier_prolonged_suspension_evidence():
    from data_sources.hkex_instrument_master import overlay_hkex_lifecycle_fields

    merged = overlay_hkex_lifecycle_fields(
        {
            "instrument_id": "01566.HK",
            "status": "suspended",
            "trading_status": 0,
            "source": "hkexnews_suspension_report",
            "lifecycle_evidence_at": "2026-07-31",
        },
        {
            "instrument_id": "01566.HK",
            "status": "active",
            "trading_status": 1,
            "source": "hkexnews_trading_resumption",
            "lifecycle_evidence_at": "2026-08-17T00:46:00+00:00",
        },
    )

    assert merged["status"] == "active"
    assert merged["trading_status"] == 1
    assert merged["source"] == "hkexnews_trading_resumption"


def test_earlier_prolonged_suspension_does_not_override_later_resumption():
    from data_sources.hkex_instrument_master import overlay_hkex_lifecycle_fields

    merged = overlay_hkex_lifecycle_fields(
        {
            "instrument_id": "01566.HK",
            "status": "active",
            "trading_status": 1,
            "source": "hkexnews_trading_resumption",
            "lifecycle_evidence_at": "2026-08-17T00:46:00+00:00",
        },
        {
            "instrument_id": "01566.HK",
            "status": "suspended",
            "trading_status": 0,
            "source": "hkexnews_suspension_report",
            "lifecycle_evidence_at": "2026-07-31",
        },
    )

    assert merged["status"] == "active"
    assert merged["source"] == "hkexnews_trading_resumption"


def test_current_product_cessation_is_not_cleared_by_earlier_resumption_or_listing_row():
    from data_sources.hkex_instrument_master import overlay_hkex_lifecycle_fields

    cessation = {
        "instrument_id": "03038.HK",
        "status": "active",
        "trading_status": 0,
        "source": "hkexnews_product_cessation",
        "lifecycle_evidence_at": "2026-07-28T04:00:00+00:00",
    }
    earlier_resume = overlay_hkex_lifecycle_fields(
        cessation,
        {
            "instrument_id": "03038.HK",
            "status": "active",
            "trading_status": 1,
            "source": "hkexnews_trading_resumption",
            "lifecycle_evidence_at": "2026-06-29T01:00:00+00:00",
        },
    )
    listing_row = overlay_hkex_lifecycle_fields(
        cessation,
        {
            "instrument_id": "03038.HK",
            "status": "active",
            "trading_status": 1,
            "source": "hkex_securities_list",
        },
    )

    assert earlier_resume["trading_status"] == 0
    assert earlier_resume["source"] == "hkexnews_product_cessation"
    assert listing_row["trading_status"] == 0
    assert listing_row["source"] == "hkexnews_product_cessation"


def test_later_official_resumption_clears_product_cessation():
    from data_sources.hkex_instrument_master import overlay_hkex_lifecycle_fields

    merged = overlay_hkex_lifecycle_fields(
        {
            "instrument_id": "03038.HK",
            "status": "active",
            "trading_status": 0,
            "source": "hkexnews_product_cessation",
            "lifecycle_evidence_at": "2026-07-28T04:00:00+00:00",
        },
        {
            "instrument_id": "03038.HK",
            "status": "active",
            "trading_status": 1,
            "source": "hkexnews_trading_resumption",
            "lifecycle_evidence_at": "2026-08-17T00:46:00+00:00",
        },
    )

    assert merged["trading_status"] == 1
    assert merged["source"] == "hkexnews_trading_resumption"


def test_manual_review_active_clears_product_cessation():
    from data_sources.hkex_instrument_master import overlay_hkex_lifecycle_fields

    merged = overlay_hkex_lifecycle_fields(
        {
            "instrument_id": "03038.HK",
            "status": "active",
            "trading_status": 0,
            "source": "hkexnews_product_cessation",
            "lifecycle_evidence_at": "2026-07-28T04:00:00+00:00",
        },
        {
            "instrument_id": "03038.HK",
            "status": "active",
            "trading_status": 1,
            "source": "hkex_manual_review",
            "lifecycle_evidence_at": "2026-08-20",
        },
    )

    assert merged["trading_status"] == 1
    assert merged["source"] == "hkex_manual_review"


def test_manual_review_active_is_not_cleared_by_later_product_cessation():
    from data_sources.hkex_instrument_master import overlay_hkex_lifecycle_fields

    merged = overlay_hkex_lifecycle_fields(
        {
            "instrument_id": "03038.HK",
            "status": "active",
            "trading_status": 1,
            "source": "hkex_manual_review",
            "lifecycle_evidence_at": "2026-08-20",
        },
        {
            "instrument_id": "03038.HK",
            "status": "active",
            "trading_status": 0,
            "source": "hkexnews_product_cessation",
            "lifecycle_evidence_at": "2026-08-21T04:00:00+00:00",
        },
    )

    assert merged["trading_status"] == 1
    assert merged["source"] == "hkex_manual_review"


def test_reached_expected_resume_date_allows_listing_row_to_clear_cessation():
    from datetime import date

    from data_sources.hkex_instrument_master import overlay_hkex_lifecycle_fields

    merged = overlay_hkex_lifecycle_fields(
        {
            "instrument_id": "03038.HK",
            "status": "active",
            "trading_status": 0,
            "source": "hkexnews_product_cessation",
            "lifecycle_evidence_at": "2026-07-28T04:00:00+00:00",
            "expected_resume_date": "2026-09-02",
        },
        {
            "instrument_id": "03038.HK",
            "status": "active",
            "trading_status": 1,
            "source": "hkex_securities_list",
        },
        as_of=date(2026, 9, 2),
    )

    assert merged["trading_status"] == 1
    assert merged["source"] == "hkex_securities_list"


def test_source_evidence_policy_accepts_trading_halt_snapshot():
    official = HKEXSecuritiesListProvider(
        source_url="fixture://hkex_securities_list.csv"
    ).parse_csv(_fixture("hkex_securities_list.csv"))
    halt = HKEXProviderSnapshot(
        source="hkexnews_trading_halt",
        source_url="https://www1.hkexnews.hk/search/titlesearch.xhtml",
        parser_version="test",
        raw_snapshot_hash="halt",
        rows=[
            {
                "instrument_id": "01831.HK",
                "status": "suspended",
                "trading_status": 0,
                "source": "hkexnews_trading_halt",
            }
        ],
        diagnostics={"row_count": 1},
    )

    policy = HKEXSourceEvidencePolicy.assess(
        snapshots=[official, halt],
        errors=[],
        official_active_rows=official.rows,
        official_delisted_rows=[],
    )

    assert policy["suspension_source_available"] is True
    assert policy["suspension_write_allowed"] is True


def test_eligibility_snapshot_marks_scheme_gap_but_not_completed_consolidation():
    from datetime import date

    from data_sources.hkex_instrument_master import (
        build_hkex_trading_eligibility_snapshot,
    )

    snapshot = build_hkex_trading_eligibility_snapshot(
        [
            _hkexnews_record(
                announcement_id="1712-intro",
                title="DEALINGS IN THE NEW SHARES ARE EXPECTED TO COMMENCE ON 2 SEPTEMBER 2026",
                published_at="2026-08-13T04:00:00+00:00",
                symbols=("01712",),
                headline_category="listing_by_introduction",
            ),
            _hkexnews_record(
                announcement_id="1777-reorg",
                title="SHARE CONSOLIDATION BECAME EFFECTIVE ON 12 AUGUST 2026",
                published_at="2026-08-12T04:00:00+00:00",
                symbols=("01777",),
                headline_category="capital_reorganisation",
            ),
            _hkexnews_record(
                announcement_id="3038-cessation",
                title="VOLUNTARY CESSATION OF TRADING AND TERMINATION OF THE SUB-FUND",
                published_at="2026-07-28T04:00:00+00:00",
                symbols=("03038",),
                headline_category="cis_matters",
            ),
            _hkexnews_record(
                announcement_id="3038-monthly",
                title="MONTHLY RETURN OF THE ETF",
                published_at="2026-08-10T04:00:00+00:00",
                symbols=("03038",),
                headline_category="cis_matters",
            ),
        ],
        as_of=date(2026, 8, 27),
    )

    by_id = {row["instrument_id"]: row for row in snapshot.rows}
    assert snapshot.source == "hkexnews_trading_eligibility"
    assert by_id["01712.HK"]["status"] == "active"
    assert by_id["01712.HK"]["trading_status"] == 0
    assert by_id["01712.HK"]["source"] == "hkexnews_trading_arrangement"
    assert str(by_id["01712.HK"]["lifecycle_evidence_at"]).startswith("2026-08-13")
    assert by_id["01712.HK"]["expected_resume_date"] == "2026-09-02"
    assert by_id["01712.HK"]["lifecycle_evidence"]["expected_resume_date"] == "2026-09-02"
    assert "01777.HK" not in by_id
    assert by_id["03038.HK"]["source"] == "hkexnews_product_cessation"
    assert by_id["03038.HK"]["trading_status"] == 0


def test_eligibility_window_reopens_after_expected_resume_date():
    from datetime import date

    from data_sources.hkex_instrument_master import (
        build_hkex_trading_eligibility_snapshot,
    )

    records = [
        _hkexnews_record(
            announcement_id="1712-intro",
            title="DEALINGS IN THE NEW SHARES ARE EXPECTED TO COMMENCE ON 2 SEPTEMBER 2026",
            published_at="2026-08-13T04:00:00+00:00",
            symbols=("01712",),
            headline_category="listing_by_introduction",
        )
    ]

    before = build_hkex_trading_eligibility_snapshot(records, as_of=date(2026, 9, 1))
    after = build_hkex_trading_eligibility_snapshot(records, as_of=date(2026, 9, 2))

    assert [row["instrument_id"] for row in before.rows] == ["01712.HK"]
    assert after.rows == []


def test_eligibility_snapshot_does_not_mark_ordinary_share_from_note_or_class_codes():
    from datetime import date

    from data_sources.hkex_instrument_master import (
        build_hkex_trading_eligibility_snapshot,
        select_hkex_eligibility_symbols,
    )

    note_withdrawal = _hkexnews_record(
        announcement_id="2688-note",
        title="LAST DAY OF DEALINGS IN THE NOTES",
        published_at="2026-08-20T04:00:00+00:00",
        symbols=("02688", "05270"),
        headline_category="withdrawal_of_listing",
    )
    cis_mixed = _hkexnews_record(
        announcement_id="2688-cis",
        title="CESSATION OF TRADING AND TERMINATION OF THE SUB-FUND",
        published_at="2026-08-21T04:00:00+00:00",
        symbols=("02688", "03038"),
        headline_category="cis_matters",
    )

    assert select_hkex_eligibility_symbols(note_withdrawal, "withdrawal_of_listing") == (
        "05270",
    )
    assert select_hkex_eligibility_symbols(cis_mixed, "cis_matters") == ("03038",)

    snapshot = build_hkex_trading_eligibility_snapshot(
        [note_withdrawal, cis_mixed],
        as_of=date(2026, 8, 27),
    )
    by_id = {row["instrument_id"]: row for row in snapshot.rows}
    assert "02688.HK" not in by_id
    assert by_id["03038.HK"]["source"] == "hkexnews_product_cessation"


def test_eligibility_snapshot_ignores_proposed_privatisation_and_category_labels():
    from datetime import date

    from data_sources.hkex_instrument_master import (
        build_hkex_trading_eligibility_snapshot,
        classify_hkex_trading_eligibility_headline,
    )

    category_label = (
        "Privatisation/Withdrawal or Cancellation of Listing of Securities"
    )
    proposed = _hkexnews_record(
        announcement_id="2688-monthly",
        title=(
            "PRE-CONDITIONAL PROPOSAL TO PRIVATIZE ENN ENERGY HOLDINGS "
            "LIMITED AND WITHDRAW ITS LISTING - MONTHLY UPDATE"
        ),
        published_at="2026-05-15T09:56:00+00:00",
        symbols=("02688",),
        headline_category="withdrawal_of_listing",
        short_text=category_label,
        long_text=category_label,
    )
    profit_warning = _hkexnews_record(
        announcement_id="232-warning",
        title="PROFIT WARNING",
        published_at="2026-08-07T11:00:00+00:00",
        symbols=("00232",),
        headline_category="withdrawal_of_listing",
        short_text=category_label,
    )
    lapsed = _hkexnews_record(
        announcement_id="1793-lapse",
        title="JOINT ANNOUNCEMENT RESULTS OF THE COURT MEETING AND LAPSE OF THE PROPOSAL",
        published_at="2026-06-24T10:19:00+00:00",
        symbols=("01793",),
        headline_category="withdrawal_of_listing",
        short_text=category_label,
    )
    proposed_withdrawal = _hkexnews_record(
        announcement_id="751-scheme",
        title=(
            "MONTHLY UPDATE IN RELATION TO PROPOSED PRE-CONDITIONAL SHARE "
            "BUY-BACK AND PROPOSED WITHDRAWAL OF LISTING"
        ),
        published_at="2026-08-17T08:52:00+00:00",
        symbols=("00751",),
        headline_category="withdrawal_of_listing",
        short_text=category_label,
    )
    cancellation = _hkexnews_record(
        announcement_id="8047-cancel",
        title=(
            "DECISION OF THE LISTING COMMITTEE ON THE CANCELLATION OF LISTING "
            "AND CONTINUED SUSPENSION OF TRADING"
        ),
        published_at="2026-08-04T13:42:00+00:00",
        symbols=("08047",),
        headline_category="withdrawal_of_listing",
        short_text=category_label,
    )
    last_day = _hkexnews_record(
        announcement_id="1712-last",
        title="LAST DAY OF DEALINGS IN THE SHARES ON 13 AUGUST 2026",
        published_at="2026-08-06T12:09:00+00:00",
        symbols=("01712",),
        headline_category="withdrawal_of_listing",
    )

    assert classify_hkex_trading_eligibility_headline(proposed) is None
    assert classify_hkex_trading_eligibility_headline(profit_warning) is None
    assert classify_hkex_trading_eligibility_headline(lapsed) is None
    assert classify_hkex_trading_eligibility_headline(proposed_withdrawal) is None
    assert classify_hkex_trading_eligibility_headline(cancellation) == (
        "withdrawal_of_listing"
    )
    assert classify_hkex_trading_eligibility_headline(last_day) == (
        "withdrawal_of_listing"
    )

    snapshot = build_hkex_trading_eligibility_snapshot(
        [proposed, profit_warning, lapsed, proposed_withdrawal, cancellation, last_day],
        as_of=date(2026, 8, 27),
    )
    by_id = {row["instrument_id"]: row for row in snapshot.rows}
    assert "02688.HK" not in by_id
    assert "00232.HK" not in by_id
    assert "01793.HK" not in by_id
    assert "00751.HK" not in by_id
    assert by_id["08047.HK"]["source"] == "hkexnews_product_cessation"
    assert by_id["01712.HK"]["source"] == "hkexnews_product_cessation"


def test_eligibility_snapshot_does_not_mark_remaining_hkd_counter():
    from datetime import date

    from data_sources.hkex_instrument_master import (
        build_hkex_trading_eligibility_snapshot,
        select_hkex_eligibility_symbols,
    )

    usd_rmb_counters = _hkexnews_record(
        announcement_id="3483-counter",
        title=(
            "Announcement - Termination of USD Trading Counter and RMB "
            "Trading Counter of each Sub-Fund"
        ),
        published_at="2026-08-24T10:50:00+00:00",
        symbols=("03483", "03489", "09483", "09489", "83483", "83489"),
        headline_category="cis_matters",
    )
    hkd_counter = _hkexnews_record(
        announcement_id="3011-counter",
        title="Termination of HKD Trading Counter",
        published_at="2026-07-14T09:43:00+00:00",
        symbols=("03011", "09011"),
        headline_category="cis_matters",
    )

    assert select_hkex_eligibility_symbols(usd_rmb_counters, "cis_matters") == ()
    assert select_hkex_eligibility_symbols(hkd_counter, "cis_matters") == ("03011",)

    snapshot = build_hkex_trading_eligibility_snapshot(
        [usd_rmb_counters, hkd_counter],
        as_of=date(2026, 8, 27),
    )
    assert snapshot.rows == []
