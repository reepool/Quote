from research.business_profile_semantic_repair import BusinessProfileSemanticRepairService
from research.providers.base import ShareholderSnapshot
from research.storage import ResearchStorageManager
from utils.config_manager import ResearchBudgetConfig, ResearchConfig, ResearchStorageConfig


def _storage(tmp_path):
    config = ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"), shadow_mode=True,
            attach_quotes_db=False, quotes_db_path=str(tmp_path / "quotes.db"),
        ),
        budget=ResearchBudgetConfig(),
    )
    storage = ResearchStorageManager(config)
    storage.initialize()
    return storage


def test_repair_audit_is_read_only_and_apply_requires_explicit_scope(tmp_path):
    storage = _storage(tmp_path)
    storage.upsert_shareholder_snapshot(ShareholderSnapshot(
        instrument_id="600000.SH", symbol="600000", exchange="SSE",
        holder_count=10, holder_count_report_date="20260331", control_owner_name="第一大股东",
        source="efinance", snapshot_json={
            "coverage_scope": ["holder_count", "reference_only_ownership_clues"],
            "holder_count": {"value": 10, "report_date": "20260331"},
            "ownership_clues": {"control_owner_name": "第一大股东"},
        },
    ))
    service = BusinessProfileSemanticRepairService(storage)
    before = storage.get_shareholder_snapshot("600000.SH")

    audit = service.run(instrument_ids=["600000.SH"])

    assert audit["mode"] == "audit"
    assert audit["write_count"] == 0
    assert audit["network_access"] is False and audit["llm_access"] is False
    assert storage.get_shareholder_snapshot("600000.SH") == before
    assert {item["code"] for item in audit["instruments"][0]["issues"]} >= {
        "shareholder_noncanonical_report_date", "shareholder_inferred_controller"
    }

    try:
        service.run(apply=True)
    except ValueError as exc:
        assert "instrument_ids or all_scope" in str(exc)
    else:
        raise AssertionError("apply without an explicit scope must fail")


def test_repair_apply_normalizes_dates_preserves_scope_provenance_and_is_idempotent(tmp_path):
    storage = _storage(tmp_path)
    storage.upsert_shareholder_snapshot(ShareholderSnapshot(
        instrument_id="600000.SH", symbol="600000", exchange="SSE",
        holder_count=10, holder_count_report_date="20260331", source="composite",
        source_mode="per_scope", snapshot_json={
            "coverage_scope": ["holder_count"],
            "holder_count": {"value": 10, "report_date": "20260331"},
            "scope_raw_provenance": {
                "holder_count": {
                    "source": "cninfo", "source_mode": "direct",
                    "payload": {"raw_report_date": "20260331", "value": 10},
                }
            },
        },
    ))
    service = BusinessProfileSemanticRepairService(storage)

    applied = service.run(instrument_ids=["600000.SH"], apply=True)
    repaired = storage.get_shareholder_snapshot("600000.SH", include_snapshot=True)
    repeated = service.run(instrument_ids=["600000.SH"], apply=True)

    assert applied["change_counts"] == {"changed": 1}
    assert repaired["holder_count_report_date"] == "2026-03-31"
    assert repaired["snapshot"]["holder_count"]["report_date"] == "2026-03-31"
    provenance = repaired["snapshot"]["scope_raw_provenance"]["holder_count"]
    assert provenance["source"] == "cninfo"
    assert provenance["payload"] == {"raw_report_date": "20260331", "value": 10}
    assert repeated["change_counts"] == {"unchanged": 1}


def test_local_shareholder_projection_uses_snapshot_and_control_history_only(tmp_path):
    storage = _storage(tmp_path)
    storage.upsert_shareholder_snapshot(ShareholderSnapshot(
        instrument_id="600000.SH", symbol="600000", exchange="SSE",
        holder_count=12, holder_count_report_date="2026-03-31", top_holders_report_date="2026-03-31",
        source="cninfo", source_mode="direct", snapshot_json={
            "top_holders": [{"rank": 1, "holder_name": "股东甲", "report_date": "2026-03-31"}],
        },
    ))
    storage.upsert_shareholder_control_changes([{
        "instrument_id": "600000.SH", "symbol": "600000", "exchange": "SSE",
        "change_date": "2026-02-01", "actual_controller_name": "控制人甲",
        "control_type": "实际控制", "source": "cninfo", "source_mode": "direct",
    }])

    projection = storage.get_shareholder_profile_projection(
        "600000.SH", knowledge_cutoff="2099-01-01"
    )

    assert projection["status"] == "success"
    assert projection["top_holders"][0]["holder_name"] == "股东甲"
    assert projection["actual_controller"]["actual_controller_name"] == "控制人甲"
