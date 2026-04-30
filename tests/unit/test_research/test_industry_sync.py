from dataclasses import dataclass

import pytest

from research.industry_sync import IndustryShadowSyncService
from research.providers.base import BaseIndustryProvider, IndustrySnapshot
from research.providers.registry import IndustryProviderRegistry
from research.source_policy import ResearchSourcePolicyResolver
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


@dataclass
class _MockDbOps:
    instruments: list[dict]

    async def get_instruments_by_exchange(self, exchange: str):
        return [item for item in self.instruments if item["exchange"] == exchange]


class _EmptyIndustryProvider(BaseIndustryProvider):
    source_name = "pytdx"

    async def fetch_industries(self, **kwargs):
        return []


class _BaoIndustryProvider(BaseIndustryProvider):
    source_name = "baostock"

    async def fetch_industries(self, *, instruments, exchange, mode="direct", limit=None):
        selected = instruments[:limit] if limit is not None else instruments
        return [
            IndustrySnapshot(
                instrument_id=selected[0]["instrument_id"],
                symbol=selected[0]["symbol"],
                exchange=exchange,
                taxonomy_system="sw_l1",
                taxonomy_version=None,
                industry_code="银行",
                industry_name="银行",
                industry_level=1,
                mapping_status="reference_only",
                source_classification="申万一级",
                source_industry_name="银行",
                source="baostock",
                source_mode=mode,
                membership_json={"normalized": {"industry_name": "银行"}},
                raw_payload={"provider": "baostock"},
            )
        ]


def _build_research_config(
    tmp_path,
    *,
    markets=None,
    industry_module=None,
) -> ResearchConfig:
    return ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(tmp_path / "quotes.db"),
            quotes_db_alias="quotes",
        ),
        budget=ResearchBudgetConfig(default_mode="balanced", allow_paid_proxy=False),
        markets=markets or ["SSE"],
        modules={"industry": industry_module or {"enabled": True}},
        routing={
            "industry": {
                "free_chain": [
                    {"source": "pytdx", "mode": "direct"},
                    {"source": "baostock", "mode": "direct"},
                ],
                "fallback_chain": [],
                "paid_chain": [],
            }
        },
        sources={
            "pytdx": {"enabled": True, "supports_proxy_patch": False, "cost_tier": "free"},
            "baostock": {"enabled": True, "supports_proxy_patch": False, "cost_tier": "free"},
        },
    )


@pytest.mark.asyncio
async def test_industry_sync_falls_back_to_next_provider_and_writes_rows(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = IndustryShadowSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "name": "浦发银行",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryProviderRegistry(
            {
                "pytdx": _EmptyIndustryProvider(),
                "baostock": _BaoIndustryProvider(),
            }
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["total_memberships_written"] == 1
    exchange_result = result["exchanges"][0]
    assert exchange_result["source"] == "baostock"
    assert exchange_result["attempted_sources"] == [
        "pytdx:direct",
        "baostock:direct",
    ]

    with storage.get_connection() as conn:
        membership_count = conn.execute("SELECT COUNT(*) FROM industry_memberships").fetchone()[0]
        audit_count = conn.execute("SELECT COUNT(*) FROM raw_payload_audit").fetchone()[0]

    assert membership_count == 1
    assert audit_count == 1


@pytest.mark.asyncio
async def test_industry_sync_degrades_when_no_provider_returns_memberships(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = IndustryShadowSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "name": "浦发银行",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryProviderRegistry({"pytdx": _EmptyIndustryProvider()}),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "degraded"
    assert result["total_memberships_written"] == 0
    assert result["exchanges"][0]["status"] == "degraded"


@pytest.mark.asyncio
async def test_industry_sync_allows_optional_empty_bse(tmp_path):
    research_config = _build_research_config(
        tmp_path,
        markets=["BSE"],
        industry_module={
            "enabled": True,
            "optional_empty_exchanges": ["BSE"],
        },
    )
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = IndustryShadowSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "430001.BJ",
                    "symbol": "430001",
                    "name": "北交样本",
                    "exchange": "BSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=IndustryProviderRegistry({"pytdx": _EmptyIndustryProvider()}),
    )

    result = await service.sync(exchanges=["BSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["total_memberships_written"] == 0
    assert result["exchanges"][0]["status"] == "success"
