import json
from pathlib import Path

from utils.config_manager import UnifiedConfigManager


def test_fx_config_merges_into_research_module_path(tmp_path):
    (tmp_path / "10_research.json").write_text(
        json.dumps(
            {
                "research_config": {
                    "enabled": True,
                    "modules": {
                        "fx_market_data": {
                            "enabled": False,
                            "storage": {"database": "data/legacy_fx.db"},
                            "quality": {"max_stale_observation_days": 10},
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "12_fx.json").write_text(
        json.dumps(
            {
                "fx_config": {
                    "enabled": True,
                    "storage": {"database": "data/fx.db"},
                    "quality": {"source_conflict_tolerance_pct": 0.01},
                    "download_scopes": [
                        {
                            "scope_id": "rmb_core",
                            "series_ids": ["FX.USD_CNY.CFETS.MID.DAILY"],
                        }
                    ],
                }
            }
        ),
        encoding="utf-8",
    )

    manager = UnifiedConfigManager(str(tmp_path))
    research_config = manager.get_research_config()
    module_cfg = research_config.modules["fx_market_data"]

    assert module_cfg["enabled"] is True
    assert module_cfg["storage"]["database"] == "data/fx.db"
    assert module_cfg["quality"]["max_stale_observation_days"] == 10
    assert module_cfg["quality"]["source_conflict_tolerance_pct"] == 0.01
    assert module_cfg["download_scopes"][0]["scope_id"] == "rmb_core"
    assert any("fx_config duplicates" in item for item in manager.get_warnings())


def test_fx_rate_tasks_have_governance_prerequisites():
    config_path = Path(__file__).resolve().parents[3] / "config" / "05_scheduler.json"
    scheduler_config = json.loads(config_path.read_text(encoding="utf-8"))["scheduler_config"]
    jobs = scheduler_config["jobs"]

    backfill_deps = jobs["fx_rate_backfill"]["dependencies"]["pre_success"][0]["jobs"]
    assert [item["job_id"] for item in backfill_deps] == ["fx_master_sync", "fx_calendar_governance"]
    assert backfill_deps[1]["inherit"] == ["start_date", "end_date", "dry_run"]
    assert backfill_deps[1]["parameters"]["source_profiles"] == ["cfets_rmb_fixing"]

    sync_deps = jobs["fx_rate_sync"]["dependencies"]["pre_success"][0]["jobs"]
    assert [item["job_id"] for item in sync_deps] == ["fx_master_sync", "fx_calendar_governance"]
    assert sync_deps[1]["inherit"] == ["dry_run"]
    assert sync_deps[1]["parameters"]["source_profiles"] == ["cfets_rmb_fixing"]
