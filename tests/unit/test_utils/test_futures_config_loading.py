import json

from utils.config_manager import UnifiedConfigManager


def test_futures_config_merges_into_legacy_research_module_path(tmp_path):
    (tmp_path / "10_research.json").write_text(
        json.dumps(
            {
                "research_config": {
                    "enabled": True,
                    "modules": {
                        "commodity_market_data": {
                            "enabled": False,
                            "storage": {"database": "data/legacy_futures.db"},
                            "sources": {
                                "exchange_official": {
                                    "enabled_exchanges": ["SHFE"],
                                    "timeout_seconds": 10,
                                }
                            },
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "11_futures.json").write_text(
        json.dumps(
            {
                "futures_config": {
                    "enabled": True,
                    "storage": {"database": "data/futures.db"},
                    "sources": {
                        "exchange_official": {
                            "enabled_exchanges": ["GFEX"],
                        }
                    },
                    "download_scopes": [
                        {
                            "scope_id": "gfex_all",
                            "exchanges": ["GFEX"],
                            "categories": ["all"],
                        }
                    ],
                }
            }
        ),
        encoding="utf-8",
    )

    manager = UnifiedConfigManager(str(tmp_path))
    research_config = manager.get_research_config()
    module_cfg = research_config.modules["commodity_market_data"]

    assert module_cfg["enabled"] is True
    assert module_cfg["storage"]["database"] == "data/futures.db"
    assert module_cfg["sources"]["exchange_official"]["timeout_seconds"] == 10
    assert module_cfg["sources"]["exchange_official"]["enabled_exchanges"] == ["GFEX"]
    assert module_cfg["download_scopes"][0]["scope_id"] == "gfex_all"
    assert any("futures_config duplicates" in item for item in manager.get_warnings())
