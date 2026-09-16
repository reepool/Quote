from __future__ import annotations

import asyncio
import inspect
import json

import pytest
from pydantic import ValidationError

from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.operations import (
    apply_published_publication,
    execute_published_task,
)
from research.company_profile.publication import (
    PUBLICATION_SCHEMA_VERSION,
    CompanyProfilePublicationControl,
    default_publication_control,
    publication_allows_new_writes,
    publication_schema_manifest,
    record_publication_control,
)
from research.company_profile.runtime import COMMON_CORE_WRITER_NAME
from tests.unit.test_research.test_business_profile_exposure_components import _storage


def test_enable_opens_only_company_facts_and_commodity_associations():
    enabled = record_publication_control("enable")

    assert enabled.schema_version == PUBLICATION_SCHEMA_VERSION
    assert enabled.production_authorization == PRODUCTION_AUTHORIZATION
    assert enabled.state == "enabled"
    assert enabled.writer == COMMON_CORE_WRITER_NAME
    assert enabled.reader == "company_profile_read_service.v1"
    assert enabled.declared_scopes == ("company_facts", "commodity_associations")
    assert enabled.active_scopes == ("company_facts", "commodity_associations")
    assert enabled.automatic_research_publication is True
    assert enabled.allows_new_writes is True
    assert enabled.legacy_writer_enabled is False
    assert enabled.dcf_authorized is False
    assert enabled.trading_authorized is False
    assert enabled.price_sensitivity_authorized is False
    assert publication_allows_new_writes(None) is True
    assert publication_allows_new_writes(default_publication_control()) is True

    payload = json.loads(enabled.model_dump_json())
    payload["dcf_authorized"] = True
    with pytest.raises(ValidationError):
        CompanyProfilePublicationControl.model_validate_json(json.dumps(payload))
    payload["dcf_authorized"] = False
    payload["legacy_writer_enabled"] = True
    with pytest.raises(ValidationError):
        CompanyProfilePublicationControl.model_validate_json(json.dumps(payload))
    payload["legacy_writer_enabled"] = False
    payload["declared_scopes"] = ["company_facts", "dcf_inputs"]
    with pytest.raises(ValidationError):
        CompanyProfilePublicationControl.model_validate_json(json.dumps(payload))


def test_pause_resume_and_rollback_stop_writes_without_enabling_legacy():
    enabled = record_publication_control("enable")
    paused = record_publication_control("pause", enabled)
    resumed = record_publication_control("resume", paused)
    rolled_back = record_publication_control("rollback", resumed)

    assert paused.state == "paused"
    assert paused.automatic_research_publication is False
    assert paused.active_scopes == ()
    assert paused.allows_new_writes is False
    assert paused.retains_saved_records is True
    assert resumed.state == "enabled"
    assert rolled_back.state == "rolled_back"
    assert rolled_back.allows_new_writes is False
    assert rolled_back.legacy_writer_enabled is False
    assert publication_allows_new_writes(paused) is False
    assert publication_allows_new_writes(rolled_back) is False
    with pytest.raises(ValueError, match="cannot pause"):
        record_publication_control("pause", rolled_back)
    reopened = record_publication_control("enable", rolled_back)
    assert reopened.state == "enabled"
    assert reopened.legacy_writer_enabled is False


def test_published_owner_persists_publication_and_blocks_paused_run(tmp_path):
    storage = _storage(tmp_path)
    enabled = apply_published_publication(
        action="enable",
        checkpoint_root=tmp_path / "checkpoints",
    )
    path = tmp_path / "checkpoints" / "reports" / f"{PUBLICATION_SCHEMA_VERSION}.json"

    assert enabled["action"] == "publication"
    assert enabled["state"] == "enabled"
    assert enabled["publication"]["automatic_research_publication"] is True
    assert enabled["legacy_writer_enabled"] is False
    assert enabled["production_authorization"] == PRODUCTION_AUTHORIZATION
    assert path.is_file()

    paused = apply_published_publication(
        action="pause",
        checkpoint_root=tmp_path / "checkpoints",
    )
    assert paused["publication"]["allows_new_writes"] is False
    with pytest.raises(ValueError, match="paused"):
        asyncio.run(
            execute_published_task(
                action="run",
                storage=storage,
                output_root=tmp_path / "output",
                checkpoint_root=tmp_path / "checkpoints",
            )
        )

    rolled_back = apply_published_publication(
        action="rollback",
        checkpoint_root=tmp_path / "checkpoints",
    )
    assert rolled_back["publication"]["state"] == "rolled_back"
    assert rolled_back["publication"]["retains_saved_records"] is True
    assert rolled_back["publication"]["legacy_writer_enabled"] is False
    with pytest.raises(ValueError, match="rolled_back"):
        asyncio.run(
            execute_published_task(
                action="run",
                storage=storage,
                output_root=tmp_path / "output",
                checkpoint_root=tmp_path / "checkpoints",
            )
        )
    queried = asyncio.run(
        execute_published_task(
            action="query",
            storage=storage,
            output_root=tmp_path / "output",
            checkpoint_root=tmp_path / "checkpoints",
        )
    )
    assert queried["action"] == "query"
    assert queried["publication"]["retains_saved_records"] is True
    assert queried["publication"]["legacy_writer_enabled"] is False


def test_publication_module_has_no_llm_or_legacy_writer_entry():
    from research.company_profile import publication

    source = inspect.getsource(publication)
    assert "openai" not in source.lower()
    assert "BusinessProfileExposureFactProducer" not in source
    assert "execute_published_task" not in source
    manifest = publication_schema_manifest()
    assert manifest["production_authorization"] == "not_authorized"
    assert manifest["legacy_writer_enabled"] is False
    assert manifest["declared_scopes"] == [
        "company_facts",
        "commodity_associations",
    ]
