import sqlite3
from dataclasses import dataclass
from datetime import datetime

import pytest

from research.announcements import (
    AnnouncementRecord,
    AnnouncementRouteAttempt,
    AnnouncementRouteResult,
    AnnouncementScanResult,
    ProviderCursor,
    build_announcement_key,
)
from research.providers.base import BaseShareholderProvider, ShareholderSnapshot
from research.providers.registry import ShareholderProviderRegistry
from research.shareholder_announcement_filters import (
    ShareholderAnnouncementCandidate,
    shareholder_announcement_filter,
)
from research.shareholder_incremental_sync import (
    ShareholderIncrementalSyncService,
    compute_shareholder_content_hashes,
)
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


class _FakeAnnouncementService:
    def __init__(self, records):
        self.records = tuple(records)
        self.queries = []

    def acquire(self, query, *, selectors=None):
        self.queries.append(query)
        selected = []
        for record in self.records:
            reasons = []
            for selector in selectors or ():
                reasons.extend(selector(record) or ())
            if reasons:
                selected.append(record.with_selection_reasons(reasons))
        source_query = query.for_source("cninfo")
        scan_result = AnnouncementScanResult(
            source="cninfo",
            query=source_query,
            status="success",
            records=self.records,
            selected_records=tuple(selected),
            pages_scanned=1,
            requests_made=1,
            announcements_seen=len(self.records),
            max_published_at=max(
                (record.published_at for record in self.records if record.published_at),
                default=None,
            ),
            provider_cursor=ProviderCursor(kind="published_at", value="2026-04-30"),
            is_complete=True,
            stop_reason="completed",
        )
        attempt = AnnouncementRouteAttempt(
            source="cninfo",
            status="success",
            record_count=len(self.records),
            selected_count=len(selected),
            pages_scanned=1,
            stop_reason="completed",
        )
        return AnnouncementRouteResult(
            query=query,
            status="success",
            selected_source="cninfo",
            scan_result=scan_result,
            attempts=(attempt,),
        )


class _ShareholderProvider(BaseShareholderProvider):
    source_name = "cninfo"

    def __init__(self, holder_count=100):
        self.holder_count = holder_count
        self.calls = []

    async def fetch_shareholder_snapshots(self, *, instruments, exchange, mode="direct", limit=None):
        self.calls.append({"instruments": list(instruments), "exchange": exchange, "mode": mode})
        return [
            ShareholderSnapshot(
                instrument_id=instrument["instrument_id"],
                symbol=instrument["symbol"],
                exchange=exchange,
                holder_count=self.holder_count,
                holder_count_report_date="2026-03-31",
                top_holders_report_date="2026-03-31",
                top_holders_count=1,
                top_holders_total_ratio=50.0,
                control_owner_name="控股股东A",
                control_owner_ratio=50.0,
                source="cninfo",
                source_mode=mode,
                snapshot_json={
                    "coverage_scope": [
                        "holder_count",
                        "top10_holders",
                        "reference_only_ownership_clues",
                    ],
                    "holder_count": {"value": self.holder_count, "report_date": "2026-03-31"},
                    "top_holders": [
                        {
                            "rank": 1,
                            "holder_name": "控股股东A",
                            "holding_shares": 1000000,
                            "holding_ratio": 50.0,
                            "holder_type": "流通A股",
                            "change": "未变",
                            "report_date": "2026-03-31",
                        }
                    ],
                    "ownership_clues": {
                        "control_owner_name": "控股股东A",
                        "control_owner_ratio": 50.0,
                        "report_date": "2026-03-31",
                    },
                },
                raw_payload={"holder_count": self.holder_count},
            )
            for instrument in instruments[:limit]
        ]


def _build_config(tmp_path):
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
                "allowed_scope": [
                    "holder_count",
                    "top10_holders",
                    "reference_only_ownership_clues",
                ],
                "incremental_sync": {
                    "lookback_days": 7,
                    "overlap_days": 2,
                    "page_size": 30,
                    "max_pages_per_market": 5,
                    "max_candidates": 20,
                    "pending_recheck_days": 2,
                },
            }
        },
        routing={
            "shareholders": {
                "free_chain": [{"source": "cninfo", "mode": "direct"}],
                "paid_chain": [],
                "fallback_chain": [],
            }
        },
        sources={
            "cninfo": {
                "enabled": True,
                "supports_proxy_patch": False,
                "cost_tier": "free",
                "announcements": {
                    "markets": {
                        "SSE": {"market": "SSE", "column": "sse", "plate": "sh"}
                    }
                },
            }
        },
    )


@pytest.mark.asyncio
async def test_shareholder_incremental_sync_uses_common_announcement_service(tmp_path):
    config = _build_config(tmp_path)
    storage = ResearchStorageManager(config)
    storage.initialize()
    instrument = {
        "instrument_id": "600519.SH",
        "symbol": "600519",
        "name": "贵州茅台",
        "exchange": "SSE",
        "type": "stock",
        "is_active": True,
    }
    announcement_service = _FakeAnnouncementService(
        [
            AnnouncementRecord(
                source="cninfo",
                source_announcement_id="ann-common-1",
                announcement_key=build_announcement_key("cninfo", "ann-common-1"),
                title="贵州茅台2026年第一季度报告",
                published_at="2026-04-30T16:00:00+08:00",
                published_at_raw="2026-04-30",
                exchange="SSE",
                market="SSE",
                symbols=("600519",),
                raw_payload={"announcementId": "ann-common-1"},
            )
        ]
    )
    provider = _ShareholderProvider(holder_count=100)
    service = ShareholderIncrementalSyncService(
        db_ops=_MockDbOps([instrument]),
        storage=storage,
        research_config=config,
        resolver=ResearchSourcePolicyResolver(config),
        registry=ShareholderProviderRegistry({"cninfo": provider}),
        announcement_service=announcement_service,
    )

    result = await service.sync(exchanges=["SSE"], pending_recheck_days=0)

    assert result["status"] == "success"
    assert result["selected_announcements"] == 1
    assert result["snapshots_written"] == 1
    assert announcement_service.queries[0].purpose_key == service.purpose_key
    with sqlite3.connect(config.storage.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM announcement_scan_state").fetchone()[0] == 5
        audit = conn.execute(
            "SELECT source_announcement_id FROM announcement_audit"
        ).fetchone()
    assert audit == ("ann-common-1",)


def test_shareholder_scan_uses_periodic_report_and_ownership_streams(tmp_path):
    config = _build_config(tmp_path)
    storage = ResearchStorageManager(config)
    storage.initialize()
    announcement_service = _FakeAnnouncementService([])
    service = ShareholderIncrementalSyncService(
        db_ops=_MockDbOps([]),
        storage=storage,
        research_config=config,
        resolver=ResearchSourcePolicyResolver(config),
        registry=ShareholderProviderRegistry({"cninfo": _ShareholderProvider()}),
        announcement_service=announcement_service,
    )

    result = service._scan_announcements(
        exchanges=["SSE"],
        lookback_days=7,
        overlap_days=2,
        page_size=30,
        max_pages_per_market=40,
        all_instruments=[],
        run_id=None,
        dry_run=True,
    )

    assert [
        (query.scope.category, query.scope.keyword)
        for query in announcement_service.queries
    ] == [
        ("periodic_report", None),
        (None, "权益变动"),
        (None, "收购报告书"),
        (None, "要约收购"),
        (None, "股东持股变动"),
    ]
    assert all(
        query.scope.source_options.get("adaptive_pagination") is True
        for query in announcement_service.queries
    )
    assert result["errors"] == []
    assert result["selected_announcements"] == 0


@pytest.mark.asyncio
async def test_shareholder_incremental_default_keeps_all_announcement_candidates(tmp_path):
    config = _build_config(tmp_path)
    config.modules["shareholders"]["incremental_sync"].pop("max_candidates", None)
    storage = ResearchStorageManager(config)
    storage.initialize()
    instruments = [
        {
            "instrument_id": f"60000{index}.SH",
            "symbol": f"60000{index}",
            "name": f"测试{index}",
            "exchange": "SSE",
            "type": "stock",
            "is_active": True,
        }
        for index in range(3)
    ]
    announcement_service = _FakeAnnouncementService(
        [
            AnnouncementRecord(
                source="cninfo",
                source_announcement_id=f"ann-{instrument['symbol']}",
                announcement_key=build_announcement_key(
                    "cninfo", f"ann-{instrument['symbol']}"
                ),
                title=f"{instrument['name']}2026年半年度报告",
                published_at="2026-08-26T16:00:00+08:00",
                published_at_raw="2026-08-26",
                exchange="SSE",
                market="SSE",
                symbols=(instrument["symbol"],),
                raw_payload={"announcementId": f"ann-{instrument['symbol']}"},
            )
            for instrument in instruments
        ]
    )
    provider = _ShareholderProvider(holder_count=100)
    service = ShareholderIncrementalSyncService(
        db_ops=_MockDbOps(instruments),
        storage=storage,
        research_config=config,
        resolver=ResearchSourcePolicyResolver(config),
        registry=ShareholderProviderRegistry({"cninfo": provider}),
        announcement_service=announcement_service,
    )

    result = await service.sync(exchanges=["SSE"], pending_recheck_days=0, dry_run=True)

    assert result["candidate_instruments"] == 3
    assert result["selected_announcements"] == 3


def test_shareholder_hash_is_stable_for_reordered_top_holders():
    left = {
        "coverage_scope": ["top10_holders", "holder_count"],
        "holder_count": {"value": "100", "report_date": "2026-03-31"},
        "top_holders": [
            {"rank": 2, "holder_name": "B", "holding_ratio": "2.0"},
            {"rank": 1, "holder_name": "A", "holding_ratio": 1},
        ],
    }
    right = {
        "coverage_scope": ["holder_count", "top10_holders"],
        "holder_count": {"value": 100, "report_date": "2026-03-31"},
        "top_holders": [
            {"rank": 1, "holder_name": "A", "holding_ratio": 1.0},
            {"rank": 2, "holder_name": "B", "holding_ratio": 2},
        ],
    }

    assert (
        compute_shareholder_content_hashes(left)["content_hash"]
        == compute_shareholder_content_hashes(right)["content_hash"]
    )


def test_pending_recheck_deadline_does_not_extend_for_same_announcement():
    now = datetime.fromisoformat("2026-05-10T09:00:00+08:00")
    candidate = ShareholderAnnouncementCandidate(
        instrument_id="600519.SH",
        symbol="600519",
        exchange="SSE",
        announcement_ids=["ann-1"],
    )
    existing_manifest = {
        "metadata": {
            "announcement_ids": ["ann-1"],
            "first_pending_at": "2026-05-01T09:00:00+08:00",
        }
    }

    deadline = ShareholderIncrementalSyncService._pending_recheck_deadline(
        existing_manifest,
        candidate,
        now,
        pending_recheck_days=5,
    )

    assert deadline == datetime.fromisoformat("2026-05-06T09:00:00+08:00")
    assert deadline < now
