from __future__ import annotations

import asyncio
import inspect
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

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
    refuse_retired_operator_entry,
    retired_operator_entry_notice,
)
from research.company_profile.publication import record_publication_control
from research.company_profile.runtime import COMMON_CORE_WRITER_NAME
from utils.config_manager import UnifiedConfigManager
from utils.task_manager.formatters import TaskManagerFormatters
from utils.task_manager.handlers import TaskManagerHandlers
from utils.task_manager.keyboards import TaskManagerKeyboards
from utils.task_manager.models import TaskStatus, TaskStatusInfo


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


def test_external_parse_layer_refuses_retired_job_ids_before_handlers():
    from scheduler.scheduler import TaskScheduler
    from scheduler.tasks import ScheduledTasks
    from utils.task_manager.handlers import TaskManagerHandlers

    looked_up: list[str] = []
    scheduler = SimpleNamespace(
        job_config_manager=SimpleNamespace(
            get_job_config=lambda job_id: looked_up.append(job_id)
        )
    )
    scheduler_cls = TaskScheduler.__closure__[0].cell_contents
    execute = AsyncMock(return_value=True)
    handler = TaskManagerHandlers(
        SimpleNamespace(
            logger=SimpleNamespace(
                info=lambda *args, **kwargs: None,
                error=lambda *args, **kwargs: None,
            ),
            task_scheduler=SimpleNamespace(
                execute_job_direct=execute,
                jobs={},
            ),
        )
    )

    for job_id in RETIRED_OPERATOR_ENTRIES:
        with pytest.raises(ValueError, match="disconnected"):
            refuse_retired_operator_entry(job_id)
        with pytest.raises(ValueError, match="disconnected"):
            asyncio.run(scheduler_cls.execute_job_direct(scheduler, job_id))
        assert asyncio.run(handler._execute_task_direct(chat_id=1, job_id=job_id)) is False
        assert getattr(ScheduledTasks, job_id) is not None

    refuse_retired_operator_entry(PUBLISHED_TASK_NAME)
    assert looked_up == []
    execute.assert_not_awaited()


def _silent_logger() -> SimpleNamespace:
    return SimpleNamespace(
        debug=lambda *args, **kwargs: None,
        info=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
    )


def _telegram_handler(
    *,
    job_configs: dict | None = None,
    execute_job_direct: AsyncMock | None = None,
    update_nested: AsyncMock | None = None,
) -> tuple[TaskManagerHandlers, SimpleNamespace]:
    manager = SimpleNamespace(
        logger=_silent_logger(),
        send_message=AsyncMock(),
        is_authorized=lambda chat_id: True,
        task_scheduler=SimpleNamespace(
            get_all_jobs_status=lambda: {"jobs": {}},
            execute_job_direct=execute_job_direct or AsyncMock(return_value=True),
            jobs={},
        ),
        job_config_manager=SimpleNamespace(job_configs=job_configs or {}),
        config_manager=SimpleNamespace(
            update_nested=update_nested or AsyncMock(return_value=True),
        ),
    )
    return TaskManagerHandlers(manager), manager


def test_retired_operator_entry_notice_names_the_published_entry():
    notice = retired_operator_entry_notice("business_profile_backfill")
    assert "入口已断开" in notice
    assert "business_profile_backfill" in notice
    assert PUBLISHED_TASK_NAME in notice
    with pytest.raises(ValueError, match="not a retired operator entry"):
        retired_operator_entry_notice(PUBLISHED_TASK_NAME)


@pytest.mark.asyncio
async def test_telegram_status_omits_retired_run_copy():
    jobs = {
        PUBLISHED_TASK_NAME: SimpleNamespace(
            enabled=False,
            description="company profile common core",
            parameters={},
        )
    }
    for name in RETIRED_OPERATOR_ENTRIES:
        jobs[name] = SimpleNamespace(
            enabled=False,
            description=f"已冻结 {name}",
            parameters={},
        )
    handler, _manager = _telegram_handler(job_configs=jobs)
    running, disabled, total = await handler._get_all_tasks_status()
    ids = {task.job_id for task in (*running, *disabled)}
    assert ids == {PUBLISHED_TASK_NAME}
    assert total == 1
    text = TaskManagerFormatters.format_task_status_summary(running, disabled, total)
    for name in RETIRED_OPERATOR_ENTRIES:
        assert f"/run {name}" not in text
    assert f"/run {PUBLISHED_TASK_NAME}" in text


@pytest.mark.asyncio
async def test_run_command_refuses_retired_before_enabled_prompt(monkeypatch):
    import utils

    looked_up: list[str] = []

    def get_nested(path, default=None):
        looked_up.append(path)
        return {"enabled": False, "description": "已冻结"}

    monkeypatch.setattr(utils.config_manager, "get_nested", get_nested)
    handler, manager = _telegram_handler()
    handler._execute_task_direct = AsyncMock(return_value=True)
    await handler.handle_run_command(
        SimpleNamespace(
            chat_id=1,
            sender_id=2,
            text="/run business_profile_backfill",
        )
    )
    message = manager.send_message.await_args.args[1]
    assert "入口已断开" in message
    assert "请先启用" not in message
    assert PUBLISHED_TASK_NAME in message
    handler._execute_task_direct.assert_not_awaited()
    manager.task_scheduler.execute_job_direct.assert_not_awaited()
    assert looked_up == []


@pytest.mark.asyncio
async def test_run_command_still_reaches_published_entry(monkeypatch):
    import utils

    monkeypatch.setattr(
        utils.config_manager,
        "get_nested",
        lambda path, default=None: {"enabled": True, "manual_only": True}
        if path == f"scheduler_config.jobs.{PUBLISHED_TASK_NAME}"
        else default,
    )
    handler, manager = _telegram_handler()
    handler._execute_task_direct = AsyncMock(return_value=True)
    await handler.handle_run_command(
        SimpleNamespace(
            chat_id=1,
            sender_id=2,
            text=f"/run {PUBLISHED_TASK_NAME}",
        )
    )
    messages = [call.args[1] for call in manager.send_message.await_args_list]
    assert all("入口已断开" not in message for message in messages)
    handler._execute_task_direct.assert_awaited_once()


@pytest.mark.asyncio
async def test_task_detail_and_enable_omit_retired_entry():
    handler, manager = _telegram_handler()
    await handler._show_task_detail_safe(1, "business_profile_backfill")
    detail = manager.send_message.await_args.args[1]
    assert "入口已断开" in detail
    assert "启用任务" not in detail

    retired = TaskStatusInfo(
        job_id="business_profile_backfill",
        description="已冻结",
        enabled=False,
        in_scheduler=False,
        status=TaskStatus.DISABLED,
        trigger_info=None,
    )
    retired_texts = [
        button["text"]
        for row in TaskManagerKeyboards.task_detail_menu(retired)
        for button in row
    ]
    retired_callbacks = [
        button["callback"]
        for row in TaskManagerKeyboards.task_detail_menu(retired)
        for button in row
    ]
    assert all("启用任务" not in text for text in retired_texts)
    assert all("立即执行" not in text for text in retired_texts)
    assert all(not callback.startswith("task_action:") for callback in retired_callbacks)

    published = TaskStatusInfo(
        job_id=PUBLISHED_TASK_NAME,
        description="common core",
        enabled=False,
        in_scheduler=False,
        status=TaskStatus.DISABLED,
        trigger_info=None,
    )
    published_texts = [
        button["text"]
        for row in TaskManagerKeyboards.task_detail_menu(published)
        for button in row
    ]
    assert any("启用任务" in text for text in published_texts)

    await handler._handle_task_action(1, "enable", "business_profile_backfill")
    assert "入口已断开" in manager.send_message.await_args.args[1]

    update = AsyncMock(return_value=True)
    action_handler, _manager = _telegram_handler(update_nested=update)
    with pytest.raises(Exception, match="disconnected"):
        await action_handler._perform_task_action("enable", "business_profile_backfill")
    update.assert_not_awaited()


def test_authoritative_requirements_match_connected_operator_entry():
    requirements = Path(
        "docs/development/company_profile_product_and_industry_semantic_requirements.md"
    ).read_text(encoding="utf-8")
    index = Path("docs/README.md").read_text(encoding="utf-8")

    assert "实现任务仍未完成" not in requirements
    assert "新增生产能力待实现" not in requirements
    assert "生产切换仍待实现" not in requirements
    assert "跨行业基础任务待接入" not in requirements
    assert "入口名在接通并测试后发布" not in requirements
    assert "未勾选 tasks 是实施缺口的权威清单" not in requirements
    assert "company_profile_common_core" in requirements
    assert "4.1—4.6" in index
