import json

import pytest

from scripts.dev_validation.validate_business_profile_structured_backfill import (
    build_isolated_config,
    load_selection,
)
from utils.config_manager import ResearchConfig


def test_load_selection_flattens_industry_groups_and_rejects_duplicates(tmp_path):
    selection = tmp_path / "selection.json"
    selection.write_text(
        json.dumps(
            {
                "as_of_date": "2026-07-18",
                "industries": {
                    "coal": {
                        "selected_issuers": [
                            {
                                "instrument_id": "601088.SH",
                                "exchange": "SSE",
                            }
                        ]
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    as_of_date, universe = load_selection(selection)

    assert as_of_date == "2026-07-18"
    assert universe == [
        {
            "instrument_id": "601088.SH",
            "exchange": "SSE",
            "industry_group": "coal",
        }
    ]

    payload = json.loads(selection.read_text(encoding="utf-8"))
    payload["industries"]["steel"] = payload["industries"]["coal"]
    selection.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate instruments"):
        load_selection(selection)


def test_isolated_config_requires_fresh_tmp_root_and_routes_all_writes(tmp_path):
    root = tmp_path / "isolated"
    config = build_isolated_config(
        ResearchConfig(),
        temp_root=root,
        max_instruments=30,
    )

    assert config.storage.db_path == str(root / "research.db")
    assert config.storage.financials_db_path == str(root / "financials.db")
    source = config.modules["business_profile_evidence"]["free_structured_sources"]
    assert source["enabled"] is True
    assert source["candidate_only"] is True
    assert source["runtime"]["raw_cache_root"] == str(root / "raw")

    (root / "occupied").write_text("x", encoding="utf-8")
    with pytest.raises(FileExistsError, match="must be empty"):
        build_isolated_config(
            ResearchConfig(),
            temp_root=root,
            max_instruments=30,
        )


def test_isolated_config_rejects_non_tmp_root():
    with pytest.raises(ValueError, match="child of /tmp"):
        build_isolated_config(
            ResearchConfig(),
            temp_root="/home/python/not-allowed",
            max_instruments=30,
        )
