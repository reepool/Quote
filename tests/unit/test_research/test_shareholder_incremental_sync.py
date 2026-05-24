import sqlite3
from dataclasses import dataclass
from datetime import datetime

import pytest

from research.providers.base import BaseShareholderProvider, ShareholderSnapshot
from research.providers.cninfo_announcements import (
    CninfoAnnouncementRecord,
    CninfoAnnouncementScanConfig,
    CninfoAnnouncementScanResult,
    CninfoAnnouncementScanner,
)
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


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = []

    def post(self, url, data, headers, timeout):
        self.calls.append({"url": url, "data": dict(data), "timeout": timeout})
        return _FakeResponse(self.payloads.pop(0))


class _FakeAnnouncementScanner:
    def __init__(self, records):
        self.records = records
        self.calls = []

    def scan(self, config, *, filters=None):
        self.calls.append(config)
        selected = []
        for record in self.records:
            reasons = []
            for predicate in filters or []:
                reasons.extend(predicate(record) or [])
            if reasons:
                selected.append(
                    CninfoAnnouncementRecord(
                        announcement_id=record.announcement_id,
                        title=record.title,
                        announcement_time=record.announcement_time,
                        market=record.market,
                        column=record.column,
                        symbols=record.symbols,
                        raw_payload=record.raw_payload,
                        selection_reasons=sorted(set(reasons)),
                    )
                )
        return CninfoAnnouncementScanResult(
            config=config,
            records=self.records,
            selected_records=selected,
            pages_scanned=1,
            announcements_seen=len(self.records),
            max_announcement_time=max(
                (record.announcement_time for record in self.records if record.announcement_time),
                default=None,
            ),
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
                "announcement_scan": {
                    "markets": {
                        "SSE": {"market": "SSE", "column": "sse", "plate": "sh"}
                    }
                },
            }
        },
    )


def test_cninfo_announcement_scanner_paginates_and_filters():
    session = _FakeSession(
        [
            {
                "announcements": [
                    {
                        "announcementId": "a1",
                        "announcementTitle": "平安银行股份有限公司2026年第一季度报告",
                        "announcementTime": 1777392000000,
                        "secCode": "000001",
                        "secName": "平安银行",
                        "adjunctUrl": "x.pdf",
                    },
                    {
                        "announcementId": "a2",
                        "announcementTitle": "普通提示性公告",
                        "announcementTime": 1777391000000,
                        "secCode": "000002",
                    },
                ]
            },
            {"announcements": []},
        ]
    )
    scanner = CninfoAnnouncementScanner(session=session, request_interval_seconds=0)

    result = scanner.scan(
        CninfoAnnouncementScanConfig(
            purpose_key="test",
            market="SZSE",
            column="szse",
            plate="sz",
            start_date="2026-05-01",
            end_date="2026-05-23",
            page_size=2,
            max_pages=3,
        ),
        filters=[shareholder_announcement_filter],
    )

    assert result.pages_scanned == 2
    assert result.announcements_seen == 2
    assert len(result.selected_records) == 1
    assert result.selected_records[0].announcement_id == "a1"
    assert result.selected_records[0].selection_reasons == ["periodic_report"]
    assert session.calls[0]["data"]["column"] == "szse"
    assert session.calls[0]["data"]["plate"] == "sz"


@pytest.mark.asyncio
async def test_shareholder_incremental_sync_writes_then_skips_unchanged(tmp_path):
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
    scanner = _FakeAnnouncementScanner(
        [
            CninfoAnnouncementRecord(
                announcement_id="ann-1",
                title="贵州茅台2026年第一季度报告",
                announcement_time="2026-04-30T16:00:00",
                market="SSE",
                column="sse",
                symbols=["600519"],
                raw_payload={"announcementId": "ann-1"},
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
        announcement_scanner=scanner,
    )

    first = await service.sync(exchanges=["SSE"], pending_recheck_days=0)
    second = await service.sync(exchanges=["SSE"], pending_recheck_days=0)

    assert first["status"] == "success"
    assert first["snapshots_written"] == 1
    assert first["changed_instruments"] == 1
    assert second["status"] == "success"
    assert second["snapshots_written"] == 0
    assert second["unchanged_instruments"] == 1
    manifest = storage.get_shareholder_change_manifest("600519.SH")
    assert manifest is not None
    assert manifest["status"] == "unchanged"

    with sqlite3.connect(config.storage.db_path) as conn:
        raw_count = conn.execute(
            "SELECT COUNT(*) FROM raw_payload_audit WHERE domain = 'shareholders'"
        ).fetchone()[0]
    assert raw_count == 1


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
