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


def test_financial_statements_config_merges_defaults_without_changing_dict_api(tmp_path):
    config_path = tmp_path / "10_research.json"
    config_path.write_text(
        json.dumps(
            {
                "research_config": {
                    "enabled": True,
                    "storage": {
                        "db_path": "data/research.db",
                        "financials_db_path": "data/custom_financials.db",
                        "filings_archive_root": "data/custom_filings",
                    },
                    "modules": {
                        "financial_statements": {
                            "enabled": True,
                            "storage": {
                                "hot_quarter_window": 10,
                            },
                            "runtime": {
                                "max_concurrency": 1,
                            },
                        },
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    manager = UnifiedConfigManager(str(tmp_path))
    research_config = manager.get_research_config()
    financial_config = research_config.modules["financial_statements"]

    assert research_config.storage.financials_db_path == "data/custom_financials.db"
    assert research_config.storage.filings_archive_root == "data/custom_filings"
    assert financial_config["enabled"] is True
    assert financial_config["history"]["baseline_report_period"] == "2024Q1"
    assert financial_config["storage"]["hot_quarter_window"] == 10
    assert financial_config["storage"]["tier_maintenance"]["preserve_source_lineage"] is True
    assert financial_config["runtime"]["max_concurrency"] == 1
    assert financial_config["runtime"]["retry_attempts"] == 2
    assert financial_config["parser"]["alias_mapping_version"] == "core_financial_facts.v1"
    assert financial_config["official_structured_sources"]["candidates"][0]["source"] == "sse"


def test_repository_research_config_has_explicit_shareholder_gate():
    manager = UnifiedConfigManager("config")
    research_config = manager.get_research_config()

    shareholders_config = research_config.modules["shareholders"]
    shareholders_routing = research_config.routing["shareholders"]

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
    assert shareholders_routing["free_chain"] == [
        {"source": "cninfo", "mode": "direct"},
    ]
    assert shareholders_routing["paid_chain"] == [
        {"source": "akshare", "mode": "proxy_patch"},
    ]


def test_repository_research_config_defaults_to_availability_first_with_paid_proxy():
    manager = UnifiedConfigManager("config")
    research_config = manager.get_research_config()

    assert research_config.budget.default_mode == "availability_first"
    assert research_config.budget.allow_paid_proxy is True


def test_repository_research_config_has_financial_statement_rollout_config():
    manager = UnifiedConfigManager("config")
    research_config = manager.get_research_config()
    financial_config = research_config.modules["financial_statements"]

    assert research_config.storage.financials_db_path == "data/financials.db"
    assert (
        research_config.storage.filings_archive_root
        == "data/filings/financial_statements"
    )
    assert financial_config["history"]["baseline_report_period"] == "2024Q1"
    assert financial_config["history"]["rolling_min_quarters"] == 8
    assert financial_config["storage"]["hot_quarter_window"] == 12
    assert financial_config["storage"]["tier_maintenance"]["enabled"] is True
    assert financial_config["runtime"]["request_timeout_seconds"] == 20.0
    assert (
        financial_config["parser"]["parser_version"]
        == "financial_structured_filing.v1"
    )
    assert (
        financial_config["fallback_policy"]["do_not_overwrite_higher_priority_facts"]
        is True
    )
    candidate_sources = {
        item["source"]
        for item in financial_config["official_structured_sources"]["candidates"]
    }
    assert {"sse", "cninfo", "bse"} <= candidate_sources
    assert all(
        "manifest_url" in item and "endpoint_url" in item
        for item in financial_config["official_structured_sources"]["candidates"]
    )
    assert (
        research_config.sources["sse"]["financial_statements"]["status"]
        == "sse_structured_json_parser_ready_disabled"
    )
    assert (
        research_config.sources["akshare"]["financial_statements"]["parser_version"]
        == "akshare_financial_statements.v1"
    )
