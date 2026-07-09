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
    first_phase_profiles = [
        "cfets_rmb_fixing",
        "cnh_market_aggregated_public",
        "fred_trade_weighted_dollar",
    ]

    backfill_deps = jobs["fx_rate_backfill"]["dependencies"]["pre_success"][0]["jobs"]
    assert [item["job_id"] for item in backfill_deps] == ["fx_master_sync", "fx_calendar_governance"]
    assert backfill_deps[1]["inherit"] == ["start_date", "end_date", "dry_run"]
    assert backfill_deps[1]["parameters"]["source_profiles"] == first_phase_profiles
    backfill_post = jobs["fx_rate_backfill"]["dependencies"]["post_success"][0]["jobs"]
    assert [item["job_id"] for item in backfill_post] == ["fx_calendar_governance"]
    assert backfill_post[0]["inherit"] == ["start_date", "end_date", "dry_run"]
    assert backfill_post[0]["parameters"]["source_profiles"] == first_phase_profiles

    sync_deps = jobs["fx_rate_sync"]["dependencies"]["pre_success"][0]["jobs"]
    assert [item["job_id"] for item in sync_deps] == ["fx_master_sync", "fx_calendar_governance"]
    assert jobs["fx_rate_sync"]["manual_only"] is False
    assert jobs["fx_rate_sync"]["trigger"] == {
        "type": "cron",
        "day_of_week": "mon-fri",
        "hour": 10,
        "minute": 45,
        "second": 0,
    }
    assert jobs["fx_rate_sync"]["parameters"]["scope_id"] == "rmb_core_download"
    assert jobs["fx_rate_sync"]["parameters"]["dry_run"] is False
    assert sync_deps[1]["inherit"] == ["dry_run"]
    assert sync_deps[1]["parameters"]["source_profiles"] == first_phase_profiles
    sync_post = jobs["fx_rate_sync"]["dependencies"]["post_success"][0]["jobs"]
    assert [item["job_id"] for item in sync_post] == [
        "fx_calendar_governance",
        "fx_derivation_sync",
        "fx_quality_check",
    ]
    assert sync_post[0]["inherit"] == ["dry_run"]
    assert sync_post[0]["parameters"]["source_profiles"] == first_phase_profiles
    assert sync_post[1]["inherit"] == ["dry_run"]
    assert jobs["fx_derivation_sync"]["enabled"] is True
    assert jobs["fx_derivation_sync"]["manual_only"] is True
    assert jobs["fx_derivation_sync"]["parameters"]["dry_run"] is False


def test_fx_config_has_offshore_rmb_spot_scope():
    config_path = Path(__file__).resolve().parents[3] / "config" / "12_fx.json"
    fx_config = json.loads(config_path.read_text(encoding="utf-8"))["fx_config"]
    scopes = {item["scope_id"]: item for item in fx_config["download_scopes"]}

    offshore_scope = scopes["rmb_offshore_spot"]
    assert offshore_scope["source_profiles"] == ["cnh_market_aggregated_public"]
    assert offshore_scope["market_scopes"] == ["offshore"]
    assert offshore_scope["rate_types"] == ["spot"]
    assert offshore_scope["series_ids"] == [
        "FX.USD_CNH.MARKET.SPOT.DAILY",
        "FX.EUR_CNH.MARKET.SPOT.DAILY",
        "FX.JPY_CNH.MARKET.SPOT.DAILY",
    ]
    assert offshore_scope["metadata"]["calendar_source_profiles"] == ["cnh_market_aggregated_public"]

    download_scope = scopes["rmb_core_download"]
    assert download_scope["source_profiles"] == [
        "cfets_rmb_fixing",
        "cnh_market_aggregated_public",
        "fred_trade_weighted_dollar",
    ]
    assert "FX.EUR_CNH.DERIVED.DAILY" not in download_scope["series_ids"]
    assert "FX.JPY_CNH.DERIVED.DAILY" not in download_scope["series_ids"]
    assert "FXI.DXY.ICE.DAILY" not in download_scope["series_ids"]
