import json

from utils.config_manager import UnifiedConfigManager


def test_research_config_is_loaded_from_split_json_files(tmp_path):
    config_path = tmp_path / "10_research.json"
    config_path.write_text(
        json.dumps(
            {
                "research_config": {
                    "enabled": True,
                    "markets": ["SSE", "SZSE"],
                    "storage": {
                        "db_path": "data/research.db",
                        "shadow_mode": True,
                        "attach_quotes_db": True,
                        "quotes_db_path": "data/quotes.db",
                        "quotes_db_alias": "quotes",
                    },
                    "budget": {
                        "default_mode": "balanced",
                        "allow_paid_proxy": False,
                        "max_paid_candidates_per_domain": 2,
                    },
                    "modules": {
                        "company_profile": {"enabled": True},
                    },
                    "sources": {
                        "efinance": {
                            "enabled": False,
                            "supports_proxy_patch": True,
                        }
                    },
                    "routing": {
                        "company_profile": {
                            "free_chain": [
                                {"source": "efinance", "mode": "direct"}
                            ]
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    manager = UnifiedConfigManager(str(tmp_path))
    research_config = manager.get_research_config()

    assert research_config.enabled is True
    assert research_config.markets == ["SSE", "SZSE"]
    assert research_config.storage.db_path == "data/research.db"
    assert research_config.storage.shadow_mode is True
    assert research_config.budget.default_mode == "balanced"
    assert research_config.budget.max_paid_candidates_per_domain == 2
    assert research_config.modules["company_profile"]["enabled"] is True
    assert research_config.sources["efinance"]["supports_proxy_patch"] is True


def test_repository_research_config_has_explicit_shareholder_gate():
    manager = UnifiedConfigManager("config")
    research_config = manager.get_research_config()

    shareholders_config = research_config.modules["shareholders"]

    assert shareholders_config["enabled"] is True
    assert shareholders_config["delivery_mode"] == "paid_high_availability"
    assert (
        shareholders_config["snapshot_api_requires_mode"]
        == "paid_high_availability"
    )
    assert shareholders_config["allowed_scope"] == [
        "holder_count",
        "top10_holders",
        "reference_only_ownership_clues",
    ]


def test_repository_research_config_defaults_to_availability_first_with_paid_proxy():
    manager = UnifiedConfigManager("config")
    research_config = manager.get_research_config()

    assert research_config.budget.default_mode == "availability_first"
    assert research_config.budget.allow_paid_proxy is True
