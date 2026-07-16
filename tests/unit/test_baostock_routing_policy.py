import json
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def _load_json(relative_path: str):
    return json.loads((REPOSITORY_ROOT / relative_path).read_text(encoding="utf-8"))


def _assert_baostock_not_primary_in_market_routes(routing):
    for exchange_routes in routing.get("daily", {}).values():
        for source_chain in exchange_routes.values():
            assert not source_chain or source_chain[0] != "baostock"

    for source_chain in routing.get("instrument_list", {}).values():
        assert not source_chain or source_chain[0] != "baostock"

    for source_chain in routing.get("calendar", {}).values():
        assert not source_chain or source_chain[0] != "baostock"

    for factor_route in routing.get("factor", {}).values():
        assert factor_route.get("primary") != "baostock"


def test_repository_market_routes_keep_baostock_as_backup_only():
    data_config = _load_json("config/03_data.json")

    _assert_baostock_not_primary_in_market_routes(data_config["routing"])
    assert data_config["routing"]["calendar"]["a_stock"] == [
        "akshare",
        "baostock",
    ]
    for exchange in ("SSE", "SZSE"):
        factor_route = data_config["routing"]["factor"][exchange]
        assert factor_route["primary"] == "akshare"
        assert factor_route["fallback"] == "baostock"


def test_repository_research_routes_keep_baostock_after_primary_provider():
    research_config = _load_json("config/10_research.json")["research_config"]

    for domain in ("company_profile", "industry", "financial_summary"):
        source_chain = research_config["routing"][domain]["free_chain"]
        assert source_chain[0]["source"] == "pytdx"
        assert source_chain[1]["source"] == "baostock"


def test_config_template_does_not_promote_baostock_to_primary():
    template = _load_json("config/config-template.json.example")

    assert template["data_sources_config"]["akshare"]["enabled"] is True
    _assert_baostock_not_primary_in_market_routes(template["routing"])
