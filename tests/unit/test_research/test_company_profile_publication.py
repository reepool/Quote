from __future__ import annotations

import asyncio
import inspect
import json
import threading
import time
from types import SimpleNamespace

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
    commit_research_profile_write,
    default_publication_control,
    publication_allows_new_writes,
    publication_schema_manifest,
    record_publication_control,
)
from research.company_profile.runtime import (
    COMMON_CORE_STORAGE_NAMESPACE,
    COMMON_CORE_WRITER_NAME,
    CompanyProfileResearchWriter,
    CompanyProfileStageRuntime,
    PublicationWritesStopped,
)
from tests.unit.test_research.test_business_profile_exposure_components import _storage
from tests.unit.test_research.test_company_profile_live_run import _registry
from tests.unit.test_research.test_company_profile_operations import _frontier
from tests.unit.test_research.test_company_profile_runtime import (
    SERVICE_OVERVIEW,
    _overview_page,
    _report,
    _RequestBoundOverviewProvider,
)


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


def _stub_runtime_record(work_id: str) -> SimpleNamespace:
    record = SimpleNamespace(
        storage_namespace=COMMON_CORE_STORAGE_NAMESPACE,
        writer=COMMON_CORE_WRITER_NAME,
        production_authorization=PRODUCTION_AUTHORIZATION,
        legacy_writers_invoked=(),
        work_id=work_id,
    )
    record.model_dump_json = lambda indent=2: "{}"
    return record


def test_writer_persist_rechecks_publication_before_write(tmp_path):
    checkpoint_root = tmp_path / "checkpoints"
    apply_published_publication(action="enable", checkpoint_root=checkpoint_root)
    writer = CompanyProfileResearchWriter(
        tmp_path / "output",
        checkpoint_root=checkpoint_root,
    )

    writer.persist(_stub_runtime_record("work-open"))
    apply_published_publication(action="pause", checkpoint_root=checkpoint_root)
    with pytest.raises(PublicationWritesStopped, match="stopped new writes"):
        writer.persist(_stub_runtime_record("work-paused"))

    assert (writer.output_root / "work-open.json").is_file()
    assert not (writer.output_root / "work-paused.json").exists()


def test_completed_pause_cannot_be_followed_by_in_flight_write(tmp_path):
    checkpoint_root = tmp_path / "checkpoints"
    apply_published_publication(action="enable", checkpoint_root=checkpoint_root)
    in_write = threading.Event()
    finish_write = threading.Event()
    order: list[str] = []
    committed: list[bool] = []
    profile = tmp_path / "profile.json"

    def held_write() -> None:
        order.append("write_acquired")
        in_write.set()
        assert finish_write.wait(timeout=2)
        profile.write_text("{}", encoding="utf-8")
        order.append("write_done")

    def do_write() -> None:
        committed.append(commit_research_profile_write(checkpoint_root, held_write))

    def do_pause() -> None:
        order.append("pause_start")
        apply_published_publication(action="pause", checkpoint_root=checkpoint_root)
        order.append("pause_done")

    writer = threading.Thread(target=do_write)
    pauser = threading.Thread(target=do_pause)
    writer.start()
    assert in_write.wait(timeout=2)
    pauser.start()
    time.sleep(0.1)
    assert pauser.is_alive()
    finish_write.set()
    writer.join(timeout=2)
    pauser.join(timeout=2)

    assert committed == [True]
    assert profile.is_file()
    assert order.index("write_done") < order.index("pause_done")
    assert commit_research_profile_write(
        checkpoint_root,
        lambda: (tmp_path / "after-pause.json").write_text("no", encoding="utf-8"),
    ) is False
    assert not (tmp_path / "after-pause.json").exists()


@pytest.mark.parametrize("stop_action", ["pause", "rollback"])
def test_in_flight_official_run_stops_publish_after_publication_switch(
    tmp_path, monkeypatch, stop_action
):
    storage = _storage(tmp_path)
    _frontier(storage)
    checkpoint_root = tmp_path / "checkpoints"
    monkeypatch.setattr(
        "research.company_profile.operations.load_official_task_candidate_registry",
        lambda **_kwargs: _registry(),
    )
    apply_published_publication(action="enable", checkpoint_root=checkpoint_root)
    provider = _RequestBoundOverviewProvider()
    original_extract = provider.extract

    def extract_and_switch_publication(request):
        apply_published_publication(
            action=stop_action,
            checkpoint_root=checkpoint_root,
        )
        return original_extract(request)

    provider.extract = extract_and_switch_publication
    report = _report(instrument_id="600000.SH", report_id="asset-pub-inflight")

    def load_pages(item):
        return {
            "report": report,
            "pages": (_overview_page(SERVICE_OVERVIEW),),
        }

    result = asyncio.run(
        execute_published_task(
            action="run",
            storage=storage,
            output_root=tmp_path / "output",
            checkpoint_root=checkpoint_root,
            page_source=load_pages,
            provider=provider,
            knowledge_cutoff="2026-08-30",
            instrument_ids=["600000.SH"],
            max_items=1,
            shared_asset_access=object(),
        )
    )
    records = list(
        (tmp_path / "output" / COMMON_CORE_STORAGE_NAMESPACE).glob("*.json")
    )
    publish = dict((result.get("drain") or {}).get("publish") or {})

    assert result["state"] == "paused"
    assert result["publication"]["state"] == (
        "rolled_back" if stop_action == "rollback" else "paused"
    )
    assert result["publication"]["allows_new_writes"] is False
    assert provider.extract_calls == 1
    assert records == []
    assert int(publish.get("completed") or 0) == 0
    blocked = "paused" if stop_action == "pause" else "rolled_back"
    with pytest.raises(ValueError, match=blocked):
        asyncio.run(
            execute_published_task(
                action="resume",
                storage=storage,
                output_root=tmp_path / "output",
                checkpoint_root=checkpoint_root,
                page_source=load_pages,
                provider=provider,
                knowledge_cutoff="2026-08-30",
                shared_asset_access=object(),
            )
        )


@pytest.mark.parametrize("stop_action", ["pause", "rollback"])
def test_publish_claimed_publication_stop_stays_resumable(
    tmp_path, monkeypatch, stop_action
):
    storage = _storage(tmp_path)
    _frontier(storage)
    checkpoint_root = tmp_path / "checkpoints"
    monkeypatch.setattr(
        "research.company_profile.operations.load_official_task_candidate_registry",
        lambda **_kwargs: _registry(),
    )
    apply_published_publication(action="enable", checkpoint_root=checkpoint_root)
    original_publish = CompanyProfileStageRuntime._publish

    def pause_after_publish_claimed(self, state):
        apply_published_publication(
            action=stop_action,
            checkpoint_root=checkpoint_root,
        )
        return original_publish(self, state)

    monkeypatch.setattr(
        CompanyProfileStageRuntime,
        "_publish",
        pause_after_publish_claimed,
    )
    provider = _RequestBoundOverviewProvider()
    report = _report(instrument_id="600000.SH", report_id="asset-pub-claimed")

    def load_pages(item):
        return {
            "report": report,
            "pages": (_overview_page(SERVICE_OVERVIEW),),
        }

    stopped = asyncio.run(
        execute_published_task(
            action="run",
            storage=storage,
            output_root=tmp_path / "output",
            checkpoint_root=checkpoint_root,
            page_source=load_pages,
            provider=provider,
            knowledge_cutoff="2026-08-30",
            instrument_ids=["600000.SH"],
            max_items=1,
            shared_asset_access=object(),
        )
    )
    with storage.get_connection() as conn:
        rows = conn.execute(
            "SELECT stage, status FROM business_profile_work_items"
        ).fetchall()
    records = list(
        (tmp_path / "output" / COMMON_CORE_STORAGE_NAMESPACE).glob("*.json")
    )

    assert stopped["state"] == "paused"
    assert records == []
    assert rows
    assert all(str(row["status"]) != "terminal_failure" for row in rows)
    assert any(str(row["stage"]) == "publish" for row in rows)

    monkeypatch.setattr(CompanyProfileStageRuntime, "_publish", original_publish)
    apply_published_publication(
        action="resume" if stop_action == "pause" else "enable",
        checkpoint_root=checkpoint_root,
    )
    resumed = asyncio.run(
        execute_published_task(
            action="resume",
            storage=storage,
            output_root=tmp_path / "output",
            checkpoint_root=checkpoint_root,
            page_source=load_pages,
            provider=provider,
            knowledge_cutoff="2026-08-30",
            shared_asset_access=object(),
        )
    )
    published = list(
        (tmp_path / "output" / COMMON_CORE_STORAGE_NAMESPACE).glob("*.json")
    )

    assert resumed["state"] == "completed"
    assert len(published) == 1


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
