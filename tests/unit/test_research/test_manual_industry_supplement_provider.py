import asyncio

from research.providers.manual_industry_supplement import (
    ManualIndustryNameSupplementProvider,
)


def test_manual_industry_name_supplement_matches_instrument_id():
    provider = ManualIndustryNameSupplementProvider(
        entries=[
            {
                "instrument_id": "688781.SH",
                "industry_name": "面板",
                "industry_code": "850831.SI",
                "reason": "Manual validation",
            }
        ]
    )

    result = asyncio.run(
        provider.fetch_industry_name_hints(
            instruments=[
                {
                    "instrument_id": "688781.SH",
                    "symbol": "688781",
                    "exchange": "SSE",
                    "type": "stock",
                }
            ],
            exchange="SSE",
            mode="configured",
        )
    )

    assert len(result) == 1
    assert result[0].instrument_id == "688781.SH"
    assert result[0].industry_name == "面板"
    assert result[0].source == "manual"
    assert result[0].source_mode == "configured"
    assert result[0].raw_payload["manual_entry"]["industry_code"] == "850831.SI"


def test_manual_industry_name_supplement_matches_symbol_exchange_when_id_missing():
    provider = ManualIndustryNameSupplementProvider(
        entries=[
            {
                "symbol": "600117",
                "exchange": "SSE",
                "industry_name": "特钢Ⅱ",
            }
        ]
    )

    result = asyncio.run(
        provider.fetch_industry_name_hints(
            instruments=[
                {
                    "instrument_id": "600117.SH",
                    "symbol": "600117",
                    "exchange": "SSE",
                    "type": "stock",
                }
            ],
            exchange="SSE",
            mode="configured",
        )
    )

    assert len(result) == 1
    assert result[0].instrument_id == "600117.SH"
    assert result[0].industry_name == "特钢Ⅱ"
