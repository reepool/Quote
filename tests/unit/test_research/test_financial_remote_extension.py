import pytest

from research.financial_remote_extension import FinancialRemoteExtensionService
from research.providers.base import (
    BaseFinancialStatementsProvider,
    FinancialStatementBundle,
    FinancialStatementRawSnapshot,
)


class _FakeEastmoneyProvider(BaseFinancialStatementsProvider):
    source_name = "akshare"

    async def fetch_financial_statement_bundles(self, **kwargs):
        instrument = kwargs["instruments"][0]
        return [
            FinancialStatementBundle(
                instrument_id=instrument["instrument_id"],
                symbol=instrument["symbol"],
                exchange=kwargs["exchange"],
                report_period="2025-12-31",
                source="akshare",
                source_mode=kwargs.get("mode", "direct"),
                raw_statements=[
                    FinancialStatementRawSnapshot(
                        instrument_id=instrument["instrument_id"],
                        symbol=instrument["symbol"],
                        exchange=kwargs["exchange"],
                        statement_type="balance_sheet",
                        report_period="2025-12-31",
                        source="akshare",
                        source_mode=kwargs.get("mode", "direct"),
                        statement_json={
                            "TOTAL_ASSETS": 1200.0,
                            "TOTAL_LIABILITIES": 420.0,
                        },
                    ),
                    FinancialStatementRawSnapshot(
                        instrument_id=instrument["instrument_id"],
                        symbol=instrument["symbol"],
                        exchange=kwargs["exchange"],
                        statement_type="profit_sheet",
                        report_period="2025-12-31",
                        source="akshare",
                        source_mode=kwargs.get("mode", "direct"),
                        statement_json={"TOTAL_OPERATE_INCOME": 1000.0},
                    ),
                ],
                raw_payload={"akshare_statement_interface": "eastmoney_report"},
            )
        ]


@pytest.mark.asyncio
async def test_financial_remote_extension_requires_explicit_opt_in():
    service = FinancialRemoteExtensionService(provider=_FakeEastmoneyProvider())

    result = await service.fetch_facts(
        instrument={"instrument_id": "600000.SH", "symbol": "600000"},
        exchange="SSE",
        requested_canonical_facts=["revenue"],
        allow_remote_extension=False,
    )

    assert result["status"] == "remote_extension_not_allowed"
    assert result["facts"] == []
    assert result["missing_fields"] == [
        {
            "canonical_fact": "revenue",
            "reason": "remote_extension_requires_explicit_opt_in",
        }
    ]


@pytest.mark.asyncio
async def test_financial_remote_extension_returns_canonical_remote_facts():
    service = FinancialRemoteExtensionService(provider=_FakeEastmoneyProvider())

    result = await service.fetch_facts(
        instrument={"instrument_id": "600000.SH", "symbol": "600000"},
        exchange="SSE",
        requested_canonical_facts=["revenue", "total_assets", "equity_parent"],
        report_periods=["2025-12-31"],
        allow_remote_extension=True,
    )

    facts_by_name = {fact["canonical_fact_name"]: fact for fact in result["facts"]}
    assert result["status"] == "partial"
    assert facts_by_name["revenue"]["fact_value"] == 1000.0
    assert facts_by_name["revenue"]["is_remote"] is True
    assert facts_by_name["revenue"]["cache_status"] == "not_cached"
    assert facts_by_name["revenue"]["source_field"] == "TOTAL_OPERATE_INCOME"
    assert facts_by_name["revenue"]["canonical_unit"] == "CNY"
    assert result["missing_fields"] == [
        {
            "canonical_fact": "equity_parent",
            "reason": "remote_extension_fact_not_returned",
        }
    ]
