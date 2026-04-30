from dataclasses import dataclass

import pytest

from research.company_profile_sync import CompanyProfileShadowSyncService
from research.providers.base import BaseCompanyProfileProvider, CompanyProfileSnapshot
from research.providers.registry import CompanyProfileProviderRegistry
from research.source_policy import ResearchSourcePolicyResolver
from research.storage import ResearchStorageManager
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)


@dataclass
class _MockDbOps:
    instruments: list[dict]

    async def get_instruments_by_exchange(self, exchange: str):
        return [item for item in self.instruments if item["exchange"] == exchange]


class _EmptyProvider(BaseCompanyProfileProvider):
    source_name = "efinance"

    async def fetch_company_profiles(self, **kwargs):
        return []


class _BaoProvider(BaseCompanyProfileProvider):
    source_name = "baostock"

    async def fetch_company_profiles(self, *, instruments, exchange, mode="direct", limit=None):
        selected = instruments[:limit] if limit is not None else instruments
        return [
            CompanyProfileSnapshot(
                instrument_id=selected[0]["instrument_id"],
                symbol=selected[0]["symbol"],
                company_name=selected[0]["name"],
                short_name=selected[0]["name"],
                exchange=exchange,
                industry_raw="银行",
                sector_raw="金融",
                status="active",
                source="baostock",
                source_mode=mode,
                raw_payload={"provider": "baostock"},
            )
        ]


def _build_research_config(tmp_path) -> ResearchConfig:
    return ResearchConfig(
        enabled=True,
        modules={"company_profile": {"enabled": True}},
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(tmp_path / "quotes.db"),
            quotes_db_alias="quotes",
        ),
        budget=ResearchBudgetConfig(default_mode="balanced", allow_paid_proxy=False),
        markets=["SSE"],
        routing={
            "company_profile": {
                "free_chain": [
                    {"source": "efinance", "mode": "direct"},
                    {"source": "baostock", "mode": "direct"},
                ],
                "fallback_chain": [],
                "paid_chain": [],
            }
        },
        sources={
            "efinance": {"enabled": True, "supports_proxy_patch": True, "cost_tier": "free"},
            "baostock": {"enabled": True, "supports_proxy_patch": False, "cost_tier": "free"},
        },
    )


@pytest.mark.asyncio
async def test_company_profile_sync_falls_back_to_next_provider_and_writes_rows(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = CompanyProfileShadowSyncService(
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
        registry=CompanyProfileProviderRegistry(
            {
                "efinance": _EmptyProvider(),
                "baostock": _BaoProvider(),
            }
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["total_profiles_written"] == 1
    exchange_result = result["exchanges"][0]
    assert exchange_result["source"] == "baostock"
    assert exchange_result["attempted_sources"] == [
        "efinance:direct",
        "baostock:direct",
    ]

    with storage.get_connection() as conn:
        profile_count = conn.execute("SELECT COUNT(*) FROM company_profiles").fetchone()[0]
        audit_count = conn.execute("SELECT COUNT(*) FROM raw_payload_audit").fetchone()[0]

    assert profile_count == 1
    assert audit_count == 1


@pytest.mark.asyncio
async def test_company_profile_sync_degrades_when_no_provider_returns_profiles(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = CompanyProfileShadowSyncService(
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
        registry=CompanyProfileProviderRegistry(
            {
                "efinance": _EmptyProvider(),
            }
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "degraded"
    assert result["total_profiles_written"] == 0
    assert result["exchanges"][0]["status"] == "degraded"


@pytest.mark.asyncio
async def test_company_profile_sync_allows_optional_empty_bse(tmp_path):
    research_config = _build_research_config(tmp_path)
    research_config.modules["company_profile"]["optional_empty_exchanges"] = ["BSE"]
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = CompanyProfileShadowSyncService(
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
        registry=CompanyProfileProviderRegistry({"efinance": _EmptyProvider()}),
    )

    result = await service.sync(exchanges=["BSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["total_profiles_written"] == 0
    assert result["exchanges"][0]["status"] == "success"
