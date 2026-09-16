from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.operations import (
    LEGACY_TASK_NAMES_NOT_CONNECTED,
    PUBLISHED_ACTIONS,
    PUBLISHED_TASK_NAME,
    apply_published_publication,
    record_published_operator_closure,
)
from research.company_profile.operator_closure import (
    CLI_INSTRUCTIONS,
    OPERATOR_ACTIONS,
    OPERATOR_CLOSURE_SCHEMA_VERSION,
    RETIRED_OPERATOR_ENTRIES,
    CompanyProfileOperatorClosureReport,
    operator_closure_schema_manifest,
    record_operator_closure_report,
)
from research.company_profile.publication import record_publication_control
from research.company_profile.runtime import COMMON_CORE_WRITER_NAME
from utils.config_manager import UnifiedConfigManager


def test_operator_closure_requires_publication_cutover(tmp_path):
    with pytest.raises(ValueError, match="publication cutover"):
        record_published_operator_closure(checkpoint_root=tmp_path / "checkpoints")


def test_operator_closure_records_published_entry_and_m4_backlog(tmp_path):
    checkpoint_root = tmp_path / "checkpoints"
    apply_published_publication(action="enable", checkpoint_root=checkpoint_root)
    payload = record_published_operator_closure(checkpoint_root=checkpoint_root)
    report = payload["operator_closure"]
    path = Path(payload["operator_closure_path"])

    assert payload["action"] == "operator_closure"
    assert payload["executed"] is False
    assert payload["database_deletion_authorized"] is False
    assert payload["legacy_writer_enabled"] is False
    assert payload["production_authorization"] == PRODUCTION_AUTHORIZATION
    assert report["schema_version"] == OPERATOR_CLOSURE_SCHEMA_VERSION
    assert report["publication_cutover_verified"] is True
    assert report["publication_state"] == "enabled"
    assert report["writer"] == COMMON_CORE_WRITER_NAME
    assert report["historical_modules_deleted"] is False
    assert report["official_pdfs_deleted"] is False
    assert report["raw_evidence_deleted"] is False
    assert report["operator_instructions"]["task_name"] == PUBLISHED_TASK_NAME
    assert tuple(report["operator_instructions"]["actions"]) == PUBLISHED_ACTIONS
    assert tuple(report["operator_instructions"]["cli"]) == CLI_INSTRUCTIONS
    assert [item["entry_name"] for item in report["retired_entries"]] == list(
        LEGACY_TASK_NAMES_NOT_CONNECTED
    )
    assert RETIRED_OPERATOR_ENTRIES == LEGACY_TASK_NAMES_NOT_CONNECTED
    assert OPERATOR_ACTIONS == PUBLISHED_ACTIONS
    assert {item["item_id"] for item in report["m4_backlog"]} == {
        "manufacturing_materials_package_not_production",
        "non_manufacturing_industry_packages_absent",
        "first_expansion_gates_unmet",
        "unassessed_source_semantics",
        "missing_assets_stay_in_denominator",
        "gold24_and_fixture_guards_not_live_quality",
    }
    assert all(item["execute_this_round"] is False for item in report["m4_backlog"])
    assert all(item["production_authorized"] is False for item in report["m4_backlog"])
    assert "operator_closure" not in PUBLISHED_ACTIONS
    assert path.is_file()


def test_operator_closure_stays_available_after_pause(tmp_path):
    checkpoint_root = tmp_path / "checkpoints"
    apply_published_publication(action="enable", checkpoint_root=checkpoint_root)
    apply_published_publication(action="pause", checkpoint_root=checkpoint_root)
    payload = record_published_operator_closure(checkpoint_root=checkpoint_root)

    assert payload["operator_closure"]["publication_state"] == "paused"
    assert payload["executed"] is False


def test_public_json_cannot_rewrite_catalog_or_authorize_deletion():
    enabled = record_operator_closure_report(record_publication_control("enable"))
    payload = json.loads(enabled.model_dump_json())
    payload["executed"] = True
    with pytest.raises(ValidationError):
        CompanyProfileOperatorClosureReport.model_validate(payload)
    payload["executed"] = False
    payload["database_deletion_authorized"] = True
    with pytest.raises(ValidationError):
        CompanyProfileOperatorClosureReport.model_validate(payload)
    payload["database_deletion_authorized"] = False
    payload["dcf_authorized"] = True
    with pytest.raises(ValidationError):
        CompanyProfileOperatorClosureReport.model_validate(payload)
    payload["dcf_authorized"] = False
    original_reason = payload["m4_backlog"][0]["reason"]
    payload["m4_backlog"][0]["reason"] = "rewritten backlog"
    with pytest.raises(ValidationError):
        CompanyProfileOperatorClosureReport.model_validate(payload)
    payload["m4_backlog"][0]["reason"] = original_reason
    payload["operator_instructions"]["actions"] = list(OPERATOR_ACTIONS) + [
        "business_profile_backfill"
    ]
    with pytest.raises(ValidationError):
        CompanyProfileOperatorClosureReport.model_validate(payload)
    payload["operator_instructions"]["actions"] = list(OPERATOR_ACTIONS)
    payload["retired_entries"][0]["entry_name"] = "invented_legacy_job"
    with pytest.raises(ValidationError):
        CompanyProfileOperatorClosureReport.model_validate(payload)


def test_operator_closure_owner_does_not_delete_or_call_legacy_writer():
    from research.company_profile import operator_closure

    source = inspect.getsource(operator_closure)
    assert "unlink" not in source
    assert "rmtree" not in source
    assert "DROP TABLE" not in source
    assert "from research.business_profile" not in source
    assert "openai" not in source.lower()
    manifest = operator_closure_schema_manifest()
    assert manifest["executed"] is False
    assert manifest["database_deletion_authorized"] is False
    assert manifest["historical_modules_deleted"] is False


def test_scheduler_copy_closes_leftover_entries():
    jobs = UnifiedConfigManager("config").get_scheduler_config().jobs
    leftover = (
        "business_profile_daily_incremental",
        "business_profile_backfill",
        "business_profile_backfill_control",
        "business_profile_semantic_repair",
        "company_profile_shadow_sync",
    )
    for name in leftover:
        assert jobs[name]["description"].startswith("已冻结")
    description = jobs["company_profile_common_core"]["description"]
    for action in PUBLISHED_ACTIONS:
        assert action in description
