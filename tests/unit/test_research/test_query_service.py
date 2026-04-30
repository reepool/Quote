from research.query_service import ResearchQueryService


class _FakeStorage:
    def __init__(self, *, profile=None, industry=None, financial_summary=None):
        self.profile = profile
        self.industry = industry
        self.financial_summary = financial_summary
        self.calls = []

    def get_company_profile(self, instrument_id, include_snapshot=True):
        self.calls.append(("company_profile", instrument_id, include_snapshot))
        return self.profile

    def get_financial_summary(self, instrument_id, include_snapshot=True):
        self.calls.append(("financial_summary", instrument_id, include_snapshot))
        return self.financial_summary

    def get_industry_membership(self, instrument_id, include_snapshot=True):
        self.calls.append(("industry", instrument_id, include_snapshot))
        return self.industry


def test_company_overview_merges_available_sections():
    storage = _FakeStorage(
        profile={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "company_name": "浦发银行",
            "short_name": "浦发银行",
            "exchange": "SSE",
            "market": "1",
            "listed_date": "1999-11-10",
            "industry_raw": "银行",
            "sector_raw": "申万一级",
            "status": "active",
            "source": "baostock",
            "source_mode": "direct",
            "data_as_of": "2026-04-17T18:00:00",
            "updated_at": "2026-04-17T18:05:00",
            "profile": {"instrument_id": "600000.SH"},
        },
        industry={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "taxonomy_system": "sw",
            "taxonomy_version": "sw_2021",
            "industry_code": "850111.SI",
            "industry_name": "白酒",
            "industry_level": 3,
            "effective_date": "2024-01-02",
            "mapping_status": "authoritative",
            "source_classification": "申万标准行业",
            "source_industry_name": "白酒",
            "sw_l1_code": "801120.SI",
            "sw_l1_name": "食品饮料",
            "sw_l2_code": "801124.SI",
            "sw_l2_name": "饮料乳品",
            "sw_l3_code": "850111.SI",
            "sw_l3_name": "白酒",
            "source": "akshare",
            "source_mode": "direct",
            "data_as_of": "2026-04-17T18:30:00",
            "updated_at": "2026-04-17T18:35:00",
            "membership": {"normalized": {"industry_name": "白酒"}},
        },
        financial_summary={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "exchange": "SSE",
            "report_date": "2025-12-31",
            "pub_date": "2026-03-30",
            "fiscal_year": 2025,
            "fiscal_quarter": 4,
            "currency": "CNY",
            "schema_version": "financial_summary.v1",
            "roe": 12.5,
            "net_margin": 18.8,
            "current_ratio": 1.7,
            "liability_to_asset": 0.55,
            "eps": 3.2,
            "source": "pytdx",
            "source_mode": "direct",
            "data_as_of": "2026-04-17T19:00:00",
            "updated_at": "2026-04-17T19:05:00",
            "summary": {"normalized": {"roe": 12.5}},
        },
    )

    service = ResearchQueryService(storage)

    overview = service.get_company_overview(
        "600000.SH",
        include_profile_snapshot=True,
        include_industry_snapshot=True,
        include_financial_snapshot=True,
    )

    assert overview is not None
    assert overview["instrument_id"] == "600000.SH"
    assert overview["company_name"] == "浦发银行"
    assert overview["industry_name"] == "白酒"
    assert overview["industry_taxonomy_version"] == "sw_2021"
    assert overview["industry_mapping_status"] == "authoritative"
    assert overview["sw_l2_code"] == "801124.SI"
    assert overview["sw_l3_name"] == "白酒"
    assert overview["roe"] == 12.5
    assert overview["data_as_of"] == "2026-04-17T19:00:00"
    assert overview["missing_sections"] == []
    assert overview["source_summary"]["company_profile"]["available"] is True
    assert overview["source_summary"]["industry"]["source"] == "akshare"
    assert overview["source_summary"]["financial_summary"]["source"] == "pytdx"
    assert overview["company_profile"]["profile"]["instrument_id"] == "600000.SH"
    assert overview["industry"]["membership"]["normalized"]["industry_name"] == "白酒"
    assert overview["financial_summary"]["summary"]["normalized"]["roe"] == 12.5
    assert storage.calls == [
        ("company_profile", "600000.SH", True),
        ("industry", "600000.SH", True),
        ("financial_summary", "600000.SH", True),
    ]


def test_company_overview_marks_missing_sections():
    storage = _FakeStorage(
        profile={
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "company_name": "浦发银行",
            "exchange": "SSE",
            "source": "baostock",
            "source_mode": "direct",
            "data_as_of": "2026-04-17T18:00:00",
        },
        financial_summary=None,
    )

    service = ResearchQueryService(storage)

    overview = service.get_company_overview("600000.SH")

    assert overview is not None
    assert overview["instrument_id"] == "600000.SH"
    assert overview["missing_sections"] == ["industry", "financial_summary"]
    assert overview["source_summary"]["industry"]["available"] is False
    assert overview["source_summary"]["financial_summary"]["available"] is False
    assert (
        overview["source_summary"]["financial_summary"]["missing_reason"]
        == "snapshot_not_available"
    )
    assert "company_profile" not in overview
    assert "financial_summary" not in overview


def test_company_overview_returns_none_when_all_sections_missing():
    service = ResearchQueryService(_FakeStorage())

    assert service.get_company_overview("600000.SH") is None
