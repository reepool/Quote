import asyncio

from research.providers.pytdx_industry import PytdxIndustryProvider


def test_pytdx_industry_provider_builds_membership_from_instrument_metadata():
    provider = PytdxIndustryProvider()

    loop = asyncio.new_event_loop()
    try:
        snapshots = loop.run_until_complete(
            provider.fetch_industries(
                instruments=[
                    {
                        "instrument_id": "600000.SH",
                        "symbol": "600000",
                        "name": "浦发银行",
                        "exchange": "SSE",
                        "type": "stock",
                        "industry": "银行",
                        "sector": "申万一级",
                    }
                ],
                exchange="SSE",
            )
        )
    finally:
        loop.close()

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.instrument_id == "600000.SH"
    assert snapshot.taxonomy_system == "sw_l1"
    assert snapshot.taxonomy_version is None
    assert snapshot.industry_name == "银行"
    assert snapshot.mapping_status == "reference_only"
    assert snapshot.source == "pytdx"
    assert snapshot.raw_payload["instrument"]["sector"] == "申万一级"
