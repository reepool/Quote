from research.providers.base import ShareholderSnapshot
from research.providers.cninfo_shareholders import CninfoShareholdersProvider
from research.shareholder_control_sync import persist_shareholder_control_changes
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


def _build_storage(tmp_path):
    config = ResearchConfig(
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
        modules={"shareholders": {"enabled": True}},
    )
    storage = ResearchStorageManager(config)
    storage.initialize()
    return storage


def _complete_snapshot():
    return ShareholderSnapshot(
        instrument_id="600519.SH",
        symbol="600519",
        exchange="SSE",
        holder_count=200,
        holder_count_report_date="2026-06-30",
        top_holders_report_date="2026-06-30",
        top_holders_count=1,
        top_holders_total_ratio=50.0,
        control_owner_name="贵州省国有资产监督管理委员会",
        control_owner_ratio=64.9,
        source="cninfo",
        source_mode="direct",
        snapshot_json={
            "coverage_scope": [
                "holder_count",
                "top10_holders",
                "reference_only_ownership_clues",
            ],
            "holder_count": {"value": 200, "report_date": "2026-06-30"},
            "top_holders": [
                {
                    "rank": 1,
                    "holder_name": "中国贵州茅台酒厂（集团）有限责任公司",
                    "holding_ratio": 50.0,
                    "report_date": "2026-06-30",
                }
            ],
            "ownership_clues": {
                "control_owner_name": "贵州省国有资产监督管理委员会",
                "control_owner_ratio": 64.9,
                "report_date": "2014-06-30",
            },
        },
        raw_payload={"seed": True},
    )


def test_persist_control_changes_writes_history_and_patches_existing_snapshot(tmp_path):
    storage = _build_storage(tmp_path)
    storage.upsert_shareholder_snapshot(_complete_snapshot())
    provider = CninfoShareholdersProvider(request_interval_seconds=0)
    provider._control_change_records = [
        {
            "source_symbol": "600519",
            "security_name": "贵州茅台",
            "change_date": "2014-06-30",
            "actual_controller_name": "贵州省国有资产监督管理委员会",
            "direct_controller_name": "中国贵州茅台酒厂(集团)有限责任公司",
            "control_type": "单独控制",
            "control_holding_shares": 74126.0,
            "control_holding_ratio": 64.9,
            "payload": {"变动日期": "2014-06-30"},
        },
        {
            "source_symbol": "600519",
            "security_name": "贵州茅台",
            "change_date": "2020-01-15",
            "actual_controller_name": "贵州省人民政府国有资产监督管理委员会",
            "direct_controller_name": "中国贵州茅台酒厂（集团）有限责任公司",
            "control_type": "单独控制",
            "control_holding_shares": 77882.0,
            "control_holding_ratio": 54.07,
            "payload": {"变动日期": "2020-01-15"},
        },
    ]

    stats = persist_shareholder_control_changes(
        storage=storage,
        provider=provider,
        instruments=[
            {
                "instrument_id": "600519.SH",
                "symbol": "600519",
                "exchange": "SSE",
                "type": "stock",
            }
        ],
        ingestion_run_id=None,
    )

    history = storage.list_shareholder_control_changes("600519.SH")
    stored = storage.get_shareholder_snapshot("600519.SH")
    assert stats["history_upserted"] == 2
    assert stats["snapshots_patched"] == 1
    assert [item["change_date"] for item in history] == ["2014-06-30", "2020-01-15"]
    assert history[1]["control_type"] == "单独控制"
    assert history[1]["direct_controller_name"] == "中国贵州茅台酒厂（集团）有限责任公司"
    assert stored["control_owner_name"] == "贵州省人民政府国有资产监督管理委员会"
    assert stored["control_owner_ratio"] == 54.07
    assert stored["snapshot"]["top_holders"][0]["holder_name"] == (
        "中国贵州茅台酒厂（集团）有限责任公司"
    )
    assert stored["snapshot"]["ownership_clues"] == {
        "control_owner_name": "贵州省人民政府国有资产监督管理委员会",
        "control_owner_ratio": 54.07,
        "report_date": "2020-01-15",
        "direct_controller_name": "中国贵州茅台酒厂（集团）有限责任公司",
        "control_type": "单独控制",
        "control_holding_shares": 77882.0,
    }
    assert stored["snapshot"]["scope_sources"]["reference_only_ownership_clues"] == (
        "cninfo:direct"
    )
