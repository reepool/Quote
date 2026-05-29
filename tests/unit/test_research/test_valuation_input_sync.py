from dataclasses import dataclass

import pytest

from research.providers.akshare_valuation_inputs import AkshareCninfoValuationInputProvider
from research.providers.base import ValuationInputSnapshot
from research.storage import ResearchStorageManager
from research.valuation_input_sync import ValuationInputSyncService
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


@dataclass
class _MockDbOps:
    instruments: list[dict]

    async def get_instruments_by_exchange(self, exchange: str):
        return [item for item in self.instruments if item["exchange"] == exchange]


class _FakeValuationInputProvider:
    source_name = "cninfo"

    def supports_mode(self, mode: str) -> bool:
        return mode == "direct"

    async def fetch_valuation_inputs(self, **kwargs):
        return [
            ValuationInputSnapshot(
                instrument_id="600519.SH",
                symbol="600519",
                exchange="SSE",
                as_of_date="2026-04-15",
                shares_outstanding=100.0,
                float_shares=60.0,
                source="cninfo",
                source_mode=kwargs["mode"],
                input_kind="capital_snapshot",
                unit="share",
                data_as_of="2026-04-16",
                diagnostics_json={"source_unit": "10k_share"},
            )
        ]


def _build_research_config(tmp_path) -> ResearchConfig:
    return ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(tmp_path / "quotes.db"),
            quotes_db_alias="quotes",
            financials_db_path=str(tmp_path / "financials.db"),
            valuation_db_path=str(tmp_path / "valuation.db"),
        ),
        budget=ResearchBudgetConfig(default_mode="balanced", allow_paid_proxy=False),
        markets=["SSE"],
        modules={
            "valuation": {
                "enabled": False,
                "input_sync": {
                    "primary_source": "cninfo",
                    "source_mode": "direct",
                },
            }
        },
    )


def _run(coro):
    import asyncio

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_cninfo_valuation_input_provider_converts_10k_shares_to_shares():
    provider = AkshareCninfoValuationInputProvider()

    snapshot = provider._build_snapshot(
        {
            "证券代码": "600519",
            "证券简称": "贵州茅台",
            "公告日期": "2026-04-16",
            "变动日期": "2026-04-15",
            "总股本": 125619.78,
            "已流通股份": 125619.78,
            "变动原因": "定期报告",
        },
        instrument={
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "exchange": "SSE",
        },
        exchange="SSE",
        mode="direct",
        input_kind="capital_change_event",
        start_date=None,
        end_date=None,
    )

    assert snapshot is not None
    assert snapshot.shares_outstanding == pytest.approx(1256197800.0)
    assert snapshot.float_shares == pytest.approx(1256197800.0)
    assert snapshot.unit == "share"
    assert snapshot.data_as_of == "2026-04-16"
    assert snapshot.diagnostics_json["source_unit"] == "10k_share"


def test_valuation_input_sync_writes_to_valuation_db(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = ValuationInputSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                },
                {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                },
            ]
        ),
        storage=storage,
        research_config=research_config,
        provider=_FakeValuationInputProvider(),
    )

    result = _run(service.sync(exchanges=["SSE"], sync_mode="incremental"))

    assert result["status"] == "success"
    assert result["total_snapshots_written"] == 1
    assert result["total_missing_instruments"] == 1

    latest = storage.get_latest_valuation_input(
        "600519.SH",
        as_of_date="2026-04-20",
    )
    assert latest is not None
    assert latest["shares_outstanding"] == 100.0
    assert latest["float_shares"] == 60.0
    assert latest["source"] == "cninfo"
