from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from research.company_profile.legacy_retirement import (
    LEGACY_RETIREMENT_SCHEMA_VERSION,
    RETAINED_CLASSES,
    CompanyProfileLegacyRetirementReport,
    legacy_retirement_schema_manifest,
    record_legacy_retirement_report,
)
from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.operations import (
    PUBLISHED_ACTIONS,
    apply_published_publication,
    record_published_legacy_retirement,
)
from research.company_profile.publication import record_publication_control
from research.company_profile.runtime import COMMON_CORE_WRITER_NAME


def test_legacy_retirement_requires_publication_cutover(tmp_path):
    with pytest.raises(ValueError, match="publication cutover"):
        record_published_legacy_retirement(checkpoint_root=tmp_path / "checkpoints")


def test_legacy_retirement_lists_only_confirmed_invalid_items(tmp_path):
    checkpoint_root = tmp_path / "checkpoints"
    apply_published_publication(action="enable", checkpoint_root=checkpoint_root)
    payload = record_published_legacy_retirement(checkpoint_root=checkpoint_root)
    report = payload["legacy_retirement"]
    objects = [item["object"] for item in report["items"]]
    path = Path(payload["legacy_retirement_path"])

    assert payload["action"] == "legacy_retirement"
    assert payload["executed"] is False
    assert payload["database_deletion_authorized"] is False
    assert payload["legacy_writer_enabled"] is False
    assert payload["production_authorization"] == PRODUCTION_AUTHORIZATION
    assert report["schema_version"] == LEGACY_RETIREMENT_SCHEMA_VERSION
    assert report["dry_run"] is True
    assert report["executed"] is False
    assert report["reset_blocks_new_delivery"] is False
    assert report["publication_cutover_verified"] is True
    assert report["publication_state"] == "enabled"
    assert report["retained_classes"] == list(RETAINED_CLASSES)
    assert report["writer"] == COMMON_CORE_WRITER_NAME
    assert {item["item_id"] for item in report["items"]} == {
        "legacy_writer_llm_report",
        "legacy_writer_atomic_extraction",
        "legacy_exposure_fact_producer",
        "disconnected_legacy_task_names",
        "withdrawn_expanded_cohort_audits",
        "replaced_usable_mvp_roadmap",
        "legacy_semantic_artifact_table",
    }
    assert all(item["execute_this_round"] is False for item in report["items"])
    assert all(item["database_deletion"] is False for item in report["items"])
    assert "company_profile_common_core.v1" not in objects
    assert not any(".pdf" in item.lower() for item in objects)
    assert "business_profile_work_items" not in objects
    assert report["recovery_plan"]["do_not_reenable_legacy_writer"] is True
    assert report["recovery_plan"]["database_restore_not_required"] is True
    assert path.is_file()
    assert "legacy_retirement" not in PUBLISHED_ACTIONS


def test_legacy_retirement_stays_available_after_pause(tmp_path):
    checkpoint_root = tmp_path / "checkpoints"
    apply_published_publication(action="enable", checkpoint_root=checkpoint_root)
    apply_published_publication(action="pause", checkpoint_root=checkpoint_root)
    payload = record_published_legacy_retirement(checkpoint_root=checkpoint_root)

    assert payload["legacy_retirement"]["publication_state"] == "paused"
    assert payload["executed"] is False


def test_public_json_cannot_authorize_deletion_or_protected_targets():
    enabled = record_legacy_retirement_report(record_publication_control("enable"))
    payload = json.loads(enabled.model_dump_json())
    payload["executed"] = True
    with pytest.raises(ValidationError):
        CompanyProfileLegacyRetirementReport.model_validate(payload)
    payload["executed"] = False
    payload["database_deletion_authorized"] = True
    with pytest.raises(ValidationError):
        CompanyProfileLegacyRetirementReport.model_validate(payload)
    payload["database_deletion_authorized"] = False
    payload["reset_blocks_new_delivery"] = True
    with pytest.raises(ValidationError):
        CompanyProfileLegacyRetirementReport.model_validate(payload)
    payload["reset_blocks_new_delivery"] = False
    payload["dcf_authorized"] = True
    with pytest.raises(ValidationError):
        CompanyProfileLegacyRetirementReport.model_validate(payload)
    payload["dcf_authorized"] = False
    original_object = payload["items"][0]["object"]
    original_reason = payload["items"][0]["reason"]
    payload["items"][0]["object"] = "invented_legacy_writer.v9"
    with pytest.raises(ValidationError):
        CompanyProfileLegacyRetirementReport.model_validate(payload)
    payload["items"][0]["object"] = original_object
    payload["items"][0]["reason"] = "rewritten catalog"
    with pytest.raises(ValidationError):
        CompanyProfileLegacyRetirementReport.model_validate(payload)
    payload["items"][0]["reason"] = original_reason
    payload["items"][0]["object"] = "company_profile_common_core.v1"
    with pytest.raises(ValidationError):
        CompanyProfileLegacyRetirementReport.model_validate(payload)


def test_legacy_retirement_owner_does_not_delete_or_call_legacy_writer():
    from research.company_profile import legacy_retirement

    source = inspect.getsource(legacy_retirement)
    assert "unlink" not in source
    assert "rmtree" not in source
    assert "DROP TABLE" not in source
    assert "from research.business_profile" not in source
    assert "openai" not in source.lower()
    manifest = legacy_retirement_schema_manifest()
    assert manifest["executed"] is False
    assert manifest["database_deletion_authorized"] is False
    assert manifest["reset_blocks_new_delivery"] is False
