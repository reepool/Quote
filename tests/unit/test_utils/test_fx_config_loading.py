import json

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
