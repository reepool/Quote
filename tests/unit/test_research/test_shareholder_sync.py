import asyncio
import json
from datetime import date
from dataclasses import dataclass

import pytest

from research.providers.base import BaseShareholderProvider, ShareholderSnapshot
from research.providers.registry import ShareholderProviderRegistry
from research.shareholder_sync import ShareholderShadowSyncService
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


class _EmptyProvider(BaseShareholderProvider):
    source_name = "akshare"

    async def fetch_shareholder_snapshots(self, **kwargs):
        return []


class _CancelledProvider(BaseShareholderProvider):
    source_name = "akshare"

    async def fetch_shareholder_snapshots(self, **kwargs):
        raise asyncio.CancelledError()


class _ShareholderProvider(BaseShareholderProvider):
    source_name = "akshare"

    def __init__(
        self,
        source_name: str = "akshare",
        resolved_instrument_ids: list[str] | None = None,
        raw_payload: dict | None = None,
        coverage_scope: list[str] | None = None,
        control_owner_name: str = "中国贵州茅台酒厂（集团）有限责任公司",
        control_owner_ratio: float = 54.0,
    ):
        self.source_name = source_name
        self.resolved_instrument_ids = (
            None if resolved_instrument_ids is None else set(resolved_instrument_ids)
        )
        self.raw_payload = raw_payload or {"provider": self.source_name}
        self.coverage_scope = coverage_scope or [
            "holder_count",
            "top10_holders",
            "reference_only_ownership_clues",
        ]
        self.control_owner_name = control_owner_name
        self.control_owner_ratio = control_owner_ratio

    async def fetch_shareholder_snapshots(
        self,
        *,
        instruments,
        exchange,
        mode="direct",
        limit=None,
    ):
        selected = instruments[:limit] if limit is not None else instruments
        filtered = [
            instrument
            for instrument in selected
            if self.resolved_instrument_ids is None
            or instrument["instrument_id"] in self.resolved_instrument_ids
        ]
        if not filtered:
            return []

        return [
            ShareholderSnapshot(
                instrument_id=instrument["instrument_id"],
                symbol=instrument["symbol"],
                exchange=exchange,
                coverage_status="reference_only",
                holder_count=123456 if "holder_count" in self.coverage_scope else None,
                holder_count_report_date=(
                    "2026-03-31" if "holder_count" in self.coverage_scope else None
                ),
                top_holders_report_date=(
                    "2026-03-31" if "top10_holders" in self.coverage_scope else None
                ),
                top_holders_count=2 if "top10_holders" in self.coverage_scope else 0,
                top_holders_total_ratio=(
                    62.5 if "top10_holders" in self.coverage_scope else None
                ),
                control_owner_name=(
                    self.control_owner_name
                    if "reference_only_ownership_clues" in self.coverage_scope
                    else None
                ),
                control_owner_ratio=(
                    self.control_owner_ratio
                    if "reference_only_ownership_clues" in self.coverage_scope
                    else None
                ),
                source=self.source_name,
                source_mode=mode,
                snapshot_json={
                    "coverage_scope": self.coverage_scope,
                    "top_holders": (
                        [{"holder_name": "中国贵州茅台酒厂（集团）有限责任公司"}]
                        if "top10_holders" in self.coverage_scope
                        else []
                    ),
                    "ownership_clues": {
                        "control_owner_name": (
                            self.control_owner_name
                            if "reference_only_ownership_clues" in self.coverage_scope
                            else None
                        ),
                        "control_owner_ratio": (
                            self.control_owner_ratio
                            if "reference_only_ownership_clues" in self.coverage_scope
                            else None
                        ),
                    },
                },
                raw_payload=self.raw_payload,
            )
            for instrument in filtered
        ]


class _BatchThenSingleRecoveryProvider(BaseShareholderProvider):
    source_name = "akshare"

    def __init__(self, source_name: str = "akshare"):
        self.source_name = source_name
        self.call_sizes: list[int] = []

    async def fetch_shareholder_snapshots(
        self,
        *,
        instruments,
        exchange,
        mode="direct",
        limit=None,
    ):
        selected = instruments[:limit] if limit is not None else instruments
        self.call_sizes.append(len(selected))
        coverage_scope = (
            ["holder_count", "reference_only_ownership_clues"]
            if len(selected) > 1
            else ["holder_count", "top10_holders", "reference_only_ownership_clues"]
        )
        return [
            ShareholderSnapshot(
                instrument_id=instrument["instrument_id"],
                symbol=instrument["symbol"],
                exchange=exchange,
                coverage_status="reference_only",
                holder_count=123456,
                holder_count_report_date="2026-03-31",
                top_holders_report_date=(
                    "2026-03-31" if "top10_holders" in coverage_scope else None
                ),
                top_holders_count=2 if "top10_holders" in coverage_scope else 0,
                top_holders_total_ratio=62.5 if "top10_holders" in coverage_scope else None,
                control_owner_name="中国贵州茅台酒厂（集团）有限责任公司",
                control_owner_ratio=54.0,
                source=self.source_name,
                source_mode=mode,
                snapshot_json={
                    "coverage_scope": coverage_scope,
                    "top_holders": (
                        [{"holder_name": "中国贵州茅台酒厂（集团）有限责任公司"}]
                        if "top10_holders" in coverage_scope
                        else []
                    ),
                    "ownership_clues": {
                        "control_owner_name": "中国贵州茅台酒厂（集团）有限责任公司",
                        "control_owner_ratio": 54.0,
                    },
                },
                raw_payload={
                    "provider": self.source_name,
                    "call_size": len(selected),
                },
            )
            for instrument in selected
        ]


class _PartialModeRecordingProvider(_ShareholderProvider):
    supported_modes = {"direct", "proxy_patch"}

    def __init__(self):
        super().__init__(
            source_name="akshare",
            coverage_scope=["holder_count", "reference_only_ownership_clues"],
        )
        self.calls: list[tuple[str, int]] = []

    async def fetch_shareholder_snapshots(
        self,
        *,
        instruments,
        exchange,
        mode="direct",
        limit=None,
    ):
        selected = instruments[:limit] if limit is not None else instruments
        self.calls.append((mode, len(selected)))
        return await super().fetch_shareholder_snapshots(
            instruments=instruments,
            exchange=exchange,
            mode=mode,
            limit=limit,
        )


def _build_research_config(tmp_path) -> ResearchConfig:
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
        markets=["SSE"],
        modules={
            "shareholders": {
                "enabled": True,
                "delivery_mode": "free_best_effort",
                "snapshot_api_requires_mode": "paid_high_availability",
                "allowed_scope": [
                    "holder_count",
                    "top10_holders",
                    "reference_only_ownership_clues",
                ],
                "same_source_recovery_candidates": ["akshare:direct"],
                "same_source_recovery_batch_size": 1,
                "same_source_recovery_max_instruments": 20,
            }
        },
        routing={
            "shareholders": {
                "free_chain": [{"source": "akshare", "mode": "direct"}],
                "fallback_chain": [
                    {"source": "efinance", "mode": "direct"},
                    {"source": "cninfo", "mode": "direct"},
                ],
                "paid_chain": [
                    {"source": "akshare", "mode": "proxy_patch"},
                    {"source": "efinance", "mode": "proxy_patch"},
                ],
            }
        },
        sources={
            "akshare": {
                "enabled": True,
                "supports_proxy_patch": True,
                "cost_tier": "free",
                "proxy_patch": {"cost_tier": "paid"},
            },
            "efinance": {
                "enabled": True,
                "supports_proxy_patch": True,
                "cost_tier": "free",
            },
            "cninfo": {
                "enabled": True,
                "supports_proxy_patch": False,
                "cost_tier": "free",
            },
        },
    )


@pytest.mark.asyncio
async def test_shareholder_sync_writes_latest_snapshot(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = ShareholderShadowSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=ShareholderProviderRegistry({"akshare": _ShareholderProvider("akshare")}),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["total_snapshots_written"] == 1
    exchange_result = result["exchanges"][0]
    assert exchange_result["source"] == "akshare"
    assert exchange_result["attempted_sources"] == ["akshare:direct"]
    assert exchange_result["successful_sources"] == ["akshare:direct"]
    assert exchange_result["requested_instruments"] == 1
    assert exchange_result["resolved_instruments"] == 1
    assert exchange_result["missing_instruments"] == 0

    snapshot = storage.get_shareholder_snapshot("600519.SH")
    assert snapshot is not None
    assert snapshot["holder_count"] == 123456
    assert snapshot["coverage_status"] == "reference_only"


@pytest.mark.asyncio
async def test_shareholder_sync_degrades_when_no_provider_returns_rows(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = ShareholderShadowSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=ShareholderProviderRegistry({"akshare": _EmptyProvider()}),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "degraded"
    assert result["total_snapshots_written"] == 0
    assert result["exchanges"][0]["status"] == "degraded"


@pytest.mark.asyncio
async def test_shareholder_sync_marks_ingestion_run_failed_when_cancelled(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = ShareholderShadowSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=ShareholderProviderRegistry({"akshare": _CancelledProvider()}),
    )

    with pytest.raises(asyncio.CancelledError):
        await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    with storage.get_connection() as conn:
        row = conn.execute(
            """
            SELECT status, rows_written, error_message, metadata_json, completed_at
            FROM ingestion_runs
            WHERE job_name = 'shareholder_shadow_sync'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()

    assert row is not None
    assert row["status"] == "failed"
    assert row["rows_written"] == 0
    assert "cancelled before exchange completion" in row["error_message"]
    assert json.loads(row["metadata_json"])["cancelled"] is True
    assert row["completed_at"] is not None


@pytest.mark.asyncio
async def test_shareholder_sync_falls_back_from_akshare_to_cninfo(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = ShareholderShadowSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=ShareholderProviderRegistry(
            {
                "akshare": _EmptyProvider(),
                "efinance": _EmptyProvider(),
                "cninfo": _ShareholderProvider("cninfo"),
            }
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    exchange_result = result["exchanges"][0]
    assert exchange_result["source"] == "cninfo"
    assert exchange_result["attempted_sources"] == [
        "akshare:direct",
        "efinance:direct",
        "cninfo:direct",
    ]
    assert exchange_result["successful_sources"] == ["cninfo:direct"]


@pytest.mark.asyncio
async def test_shareholder_sync_continues_fallback_for_missing_instruments(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    instruments = [
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "name": "贵州茅台",
            "exchange": "SSE",
            "type": "stock",
            "is_active": True,
        },
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "name": "浦发银行",
            "exchange": "SSE",
            "type": "stock",
            "is_active": True,
        },
    ]
    service = ShareholderShadowSyncService(
        db_ops=_MockDbOps(instruments=instruments),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=ShareholderProviderRegistry(
            {
                "akshare": _ShareholderProvider(
                    "akshare",
                    resolved_instrument_ids=["600519.SH"],
                ),
                "efinance": _EmptyProvider(),
                "cninfo": _ShareholderProvider(
                    "cninfo",
                    resolved_instrument_ids=["600000.SH"],
                ),
            }
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=2)

    assert result["status"] == "success"
    assert result["total_snapshots_written"] == 2
    exchange_result = result["exchanges"][0]
    assert exchange_result["source"] == "akshare"
    assert exchange_result["mode"] == "direct"
    assert exchange_result["attempted_sources"] == [
        "akshare:direct",
        "efinance:direct",
        "cninfo:direct",
    ]
    assert exchange_result["successful_sources"] == [
        "akshare:direct",
        "cninfo:direct",
    ]
    assert exchange_result["requested_instruments"] == 2
    assert exchange_result["resolved_instruments"] == 2
    assert exchange_result["missing_instruments"] == 0
    assert exchange_result["snapshots_written"] == 2

    first_snapshot = storage.get_shareholder_snapshot("600519.SH")
    second_snapshot = storage.get_shareholder_snapshot("600000.SH")
    assert first_snapshot is not None
    assert first_snapshot["source"] == "akshare"
    assert second_snapshot is not None
    assert second_snapshot["source"] == "cninfo"


@pytest.mark.asyncio
async def test_shareholder_sync_merges_missing_scope_from_later_provider(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    instruments = [
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "name": "贵州茅台",
            "exchange": "SSE",
            "type": "stock",
            "is_active": True,
        }
    ]
    service = ShareholderShadowSyncService(
        db_ops=_MockDbOps(instruments=instruments),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=ShareholderProviderRegistry(
            {
                "akshare": _ShareholderProvider(
                    "akshare",
                    coverage_scope=["holder_count", "top10_holders"],
                ),
                "efinance": _EmptyProvider(),
                "cninfo": _ShareholderProvider(
                    "cninfo",
                    coverage_scope=["reference_only_ownership_clues"],
                ),
            }
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    exchange_result = result["exchanges"][0]
    assert exchange_result["successful_sources"] == [
        "akshare:direct",
        "cninfo:direct",
    ]
    snapshot = storage.get_shareholder_snapshot("600519.SH")
    assert snapshot is not None
    assert snapshot["source"] == "akshare"
    assert snapshot["top_holders_count"] == 2
    assert snapshot["control_owner_name"] == "中国贵州茅台酒厂（集团）有限责任公司"
    assert snapshot["snapshot"]["scope_sources"] == {
        "holder_count": "akshare:direct",
        "top10_holders": "akshare:direct",
        "reference_only_ownership_clues": "cninfo:direct",
    }


@pytest.mark.asyncio
async def test_shareholder_sync_force_merges_cninfo_control_owner(tmp_path):
    research_config = _build_research_config(tmp_path)
    research_config.modules["shareholders"]["force_merge_candidates"] = ["cninfo:direct"]
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    instruments = [
        {
            "instrument_id": "920489.BJ",
            "symbol": "920489",
            "name": "佳先股份",
            "exchange": "BSE",
            "type": "stock",
            "is_active": True,
        }
    ]
    service = ShareholderShadowSyncService(
        db_ops=_MockDbOps(instruments=instruments),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=ShareholderProviderRegistry(
            {
                "akshare": _ShareholderProvider(
                    "akshare",
                    control_owner_name="蚌埠能源集团有限公司",
                    control_owner_ratio=27.44,
                ),
                "efinance": _EmptyProvider(),
                "cninfo": _ShareholderProvider(
                    "cninfo",
                    coverage_scope=["reference_only_ownership_clues"],
                    control_owner_name="蚌埠市人民政府国有资产监督管理委员会",
                    control_owner_ratio=27.44,
                ),
            }
        ),
    )

    result = await service.sync(exchanges=["BSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    exchange_result = result["exchanges"][0]
    assert exchange_result["attempted_sources"] == [
        "akshare:direct",
        "cninfo:direct",
    ]
    assert exchange_result["successful_sources"] == [
        "akshare:direct",
        "cninfo:direct",
    ]

    snapshot = storage.get_shareholder_snapshot("920489.BJ")
    assert snapshot is not None
    assert snapshot["holder_count"] == 123456
    assert snapshot["top_holders_count"] == 2
    assert snapshot["control_owner_name"] == "蚌埠市人民政府国有资产监督管理委员会"
    assert snapshot["snapshot"]["scope_sources"] == {
        "holder_count": "akshare:direct",
        "top10_holders": "akshare:direct",
        "reference_only_ownership_clues": "cninfo:direct",
    }

    with storage.get_connection() as conn:
        row = conn.execute(
            """
            SELECT metadata_json
            FROM ingestion_runs
            WHERE domain = 'shareholders'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    assert row is not None
    metadata = json.loads(row["metadata_json"])
    assert metadata["force_merge_candidates"] == ["cninfo:direct"]


def test_shareholder_sync_merge_updates_holder_count_report_date_when_filling_value(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    service = ShareholderShadowSyncService(
        db_ops=_MockDbOps(instruments=[]),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=ShareholderProviderRegistry({}),
    )
    existing = ShareholderSnapshot(
        instrument_id="600115.SH",
        symbol="600115",
        exchange="SSE",
        holder_count=None,
        holder_count_report_date="2026-04-23",
        top_holders_report_date="2026-04-23",
        top_holders_count=10,
        control_owner_name="国务院国有资产监督管理委员会",
        source="akshare",
        source_mode="proxy_patch",
        snapshot_json={
            "coverage_scope": ["top10_holders", "reference_only_ownership_clues"],
            "holder_count": {"value": None, "report_date": "2026-04-23"},
            "top_holders": [{"holder_name": "中国东方航空集团有限公司"}],
            "ownership_clues": {
                "control_owner_name": "国务院国有资产监督管理委员会",
            },
            "scope_sources": {
                "top10_holders": "akshare:proxy_patch",
                "reference_only_ownership_clues": "cninfo:direct",
            },
        },
    )
    incoming = ShareholderSnapshot(
        instrument_id="600115.SH",
        symbol="600115",
        exchange="SSE",
        holder_count=163848,
        holder_count_report_date="2026-03-31",
        source="cninfo",
        source_mode="direct",
        snapshot_json={
            "coverage_scope": ["holder_count"],
            "holder_count": {"value": 163848, "report_date": "2026-03-31"},
        },
    )

    merged = service._merge_snapshots(existing, incoming)

    assert merged.holder_count == 163848
    assert merged.holder_count_report_date == "2026-03-31"
    assert merged.snapshot_json["holder_count"] == {
        "value": 163848,
        "report_date": "2026-03-31",
    }
    assert merged.snapshot_json["scope_sources"]["holder_count"] == "cninfo:direct"


@pytest.mark.asyncio
async def test_shareholder_sync_runs_same_source_recovery_for_missing_scope(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    instruments = [
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "name": "贵州茅台",
            "exchange": "SSE",
            "type": "stock",
            "is_active": True,
        },
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "name": "浦发银行",
            "exchange": "SSE",
            "type": "stock",
            "is_active": True,
        },
    ]
    recovery_provider = _BatchThenSingleRecoveryProvider()
    service = ShareholderShadowSyncService(
        db_ops=_MockDbOps(instruments=instruments),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=ShareholderProviderRegistry(
            {
                "akshare": recovery_provider,
                "efinance": _EmptyProvider(),
                "cninfo": _EmptyProvider(),
            }
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=2)

    assert result["status"] == "success"
    exchange_result = result["exchanges"][0]
    assert exchange_result["successful_sources"] == ["akshare:direct"]
    assert exchange_result["resolved_instruments"] == 2
    assert recovery_provider.call_sizes == [2, 1, 1]

    snapshot = storage.get_shareholder_snapshot("600519.SH")
    assert snapshot is not None
    assert snapshot["top_holders_count"] == 2
    with storage.get_connection() as conn:
        row = conn.execute(
            """
            SELECT metadata_json
            FROM ingestion_runs
            WHERE domain = 'shareholders'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    assert row is not None
    metadata = json.loads(row["metadata_json"])
    assert metadata["same_source_recovery_runs"] == 2
    assert metadata["same_source_recovery_attempted_instruments"] == 2
    assert metadata["same_source_recovery_resolved_instruments"] == 2


@pytest.mark.asyncio
async def test_shareholder_sync_skips_same_source_full_fallback_after_success(tmp_path):
    research_config = _build_research_config(tmp_path)
    research_config.budget.default_mode = "availability_first"
    research_config.budget.allow_paid_proxy = True
    research_config.modules["shareholders"][
        "skip_same_source_full_fallback_after_success"
    ] = True
    research_config.modules["shareholders"]["same_source_recovery_candidates"] = []
    research_config.modules["shareholders"]["force_merge_candidates"] = ["cninfo:direct"]
    research_config.routing["shareholders"] = {
        "free_chain": [],
        "paid_chain": [{"source": "akshare", "mode": "proxy_patch"}],
        "fallback_chain": [
            {"source": "cninfo", "mode": "direct"},
            {"source": "akshare", "mode": "direct"},
        ],
    }
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    instruments = [
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "name": "贵州茅台",
            "exchange": "SSE",
            "type": "stock",
            "is_active": True,
        },
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "name": "浦发银行",
            "exchange": "SSE",
            "type": "stock",
            "is_active": True,
        },
    ]
    akshare_provider = _PartialModeRecordingProvider()
    service = ShareholderShadowSyncService(
        db_ops=_MockDbOps(instruments=instruments),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=ShareholderProviderRegistry(
            {
                "akshare": akshare_provider,
                "cninfo": _EmptyProvider(),
            }
        ),
    )

    result = await service.sync(
        exchanges=["SSE"],
        limit_per_exchange=2,
        budget_mode="availability_first",
        allow_paid_proxy=True,
    )

    exchange_result = result["exchanges"][0]
    assert exchange_result["status"] == "degraded"
    assert exchange_result["attempted_sources"] == ["akshare:proxy_patch", "cninfo:direct"]
    assert akshare_provider.calls == [("proxy_patch", 2)]

    with storage.get_connection() as conn:
        row = conn.execute(
            """
            SELECT metadata_json
            FROM ingestion_runs
            WHERE domain = 'shareholders'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    assert row is not None
    metadata = json.loads(row["metadata_json"])
    assert metadata["skipped_sources"] == ["akshare:direct"]
    assert metadata["skip_same_source_full_fallback_after_success"] is True


@pytest.mark.asyncio
async def test_shareholder_sync_reports_degraded_when_some_instruments_remain_missing(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    instruments = [
        {
            "instrument_id": "600519.SH",
            "symbol": "600519",
            "name": "贵州茅台",
            "exchange": "SSE",
            "type": "stock",
            "is_active": True,
        },
        {
            "instrument_id": "600000.SH",
            "symbol": "600000",
            "name": "浦发银行",
            "exchange": "SSE",
            "type": "stock",
            "is_active": True,
        },
    ]
    service = ShareholderShadowSyncService(
        db_ops=_MockDbOps(instruments=instruments),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=ShareholderProviderRegistry(
            {
                "akshare": _ShareholderProvider(
                    "akshare",
                    resolved_instrument_ids=["600519.SH"],
                ),
                "efinance": _EmptyProvider(),
                "cninfo": _EmptyProvider(),
            }
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=2)

    assert result["status"] == "degraded"
    assert result["total_snapshots_written"] == 1
    exchange_result = result["exchanges"][0]
    assert exchange_result["status"] == "degraded"
    assert exchange_result["source"] == "akshare"
    assert exchange_result["successful_sources"] == ["akshare:direct"]
    assert exchange_result["requested_instruments"] == 2
    assert exchange_result["resolved_instruments"] == 1
    assert exchange_result["missing_instruments"] == 1
    assert exchange_result["missing_instrument_ids"] == ["600000.SH"]
    assert (
        "Missing required shareholder scope for 1 instruments"
        == exchange_result["error_message"]
    )

    first_snapshot = storage.get_shareholder_snapshot("600519.SH")
    second_snapshot = storage.get_shareholder_snapshot("600000.SH")
    assert first_snapshot is not None
    assert second_snapshot is None


@pytest.mark.asyncio
async def test_shareholder_sync_allows_optional_empty_bse(tmp_path):
    research_config = _build_research_config(tmp_path)
    research_config.markets = ["BSE"]
    research_config.modules["shareholders"]["optional_empty_exchanges"] = ["BSE"]
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = ShareholderShadowSyncService(
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
        registry=ShareholderProviderRegistry(
            {
                "akshare": _EmptyProvider(),
                "efinance": _EmptyProvider(),
                "cninfo": _EmptyProvider(),
            }
        ),
    )

    result = await service.sync(exchanges=["BSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    assert result["total_snapshots_written"] == 0
    exchange_result = result["exchanges"][0]
    assert exchange_result["status"] == "success"
    assert exchange_result["resolved_instruments"] == 1
    assert exchange_result["missing_instruments"] == 0


@pytest.mark.asyncio
async def test_shareholder_sync_serializes_date_like_raw_payloads(tmp_path):
    research_config = _build_research_config(tmp_path)
    storage = ResearchStorageManager(research_config)
    storage.initialize()

    service = ShareholderShadowSyncService(
        db_ops=_MockDbOps(
            instruments=[
                {
                    "instrument_id": "600519.SH",
                    "symbol": "600519",
                    "name": "贵州茅台",
                    "exchange": "SSE",
                    "type": "stock",
                    "is_active": True,
                }
            ]
        ),
        storage=storage,
        research_config=research_config,
        resolver=ResearchSourcePolicyResolver(research_config),
        registry=ShareholderProviderRegistry(
            {
                "akshare": _ShareholderProvider(
                    "akshare",
                    raw_payload={
                        "provider": "akshare",
                        "report_date": date(2026, 3, 31),
                    },
                )
            }
        ),
    )

    result = await service.sync(exchanges=["SSE"], limit_per_exchange=1)

    assert result["status"] == "success"
    with storage.get_connection() as conn:
        row = conn.execute(
            """
            SELECT payload_json
            FROM raw_payload_audit
            WHERE domain = 'shareholders' AND instrument_id = '600519.SH'
            """
        ).fetchone()

    assert row is not None
    payload = json.loads(row["payload_json"])
    assert payload["report_date"] == "2026-03-31"
