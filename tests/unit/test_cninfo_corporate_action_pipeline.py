import asyncio

import pytest
from unittest.mock import AsyncMock, Mock
from types import SimpleNamespace

from data_sources.cninfo_corporate_action_pipeline import (
    ArtifactPayload,
    CNINFO_PIPELINE_DEFAULT_LLM_CONCURRENCY,
    CNINFO_PIPELINE_MAX_LLM_CONCURRENCY,
    CninfoCorporateActionResolutionPipeline,
    CninfoCorporateActionPipelineConfig,
    CninfoDocumentPreparationStage,
    CninfoSerialPersistenceWriter,
    CorporateActionPipelineStage,
    ExtractionCasePayload,
    ParsedPageReference,
    PipelineIdentity,
    PersistenceCommand,
    ResolutionStagePayload,
    ResumeFingerprint,
    InventoryPayload,
    decide_stage_resume,
    SelectedAnnouncementPayload,
    TitleBundlePayload,
    assert_same_business_identity,
    normalize_cninfo_pipeline_exchanges,
    requires_semantic_verification,
)
from data_manager import DataManager


def _identity(stage=CorporateActionPipelineStage.INVENTORY, sequence=0):
    return PipelineIdentity(
        instrument_id="600108.SH",
        source_event_key="event-1",
        run_id="run-1",
        stage=stage,
        stage_sequence=sequence,
        source_profile="cninfo_dividend",
    )


def test_pipeline_config_bounds_resources_and_keeps_serial_rollback():
    config = CninfoCorporateActionPipelineConfig.from_mapping({})
    assert config.mode == "serial"
    assert config.llm_concurrency == CNINFO_PIPELINE_DEFAULT_LLM_CONCURRENCY == 15
    assert config.llm_requests_per_minute == 0
    assert config.document_parse_concurrency == 8
    assert config.writer_concurrency == 1
    assert config.verification_policy == "always"

    with pytest.raises(ValueError, match="document_parse_concurrency"):
        CninfoCorporateActionPipelineConfig.from_mapping({
            "document_parse_concurrency": 9,
        })
    with pytest.raises(ValueError, match="llm_concurrency"):
        CninfoCorporateActionPipelineConfig.from_mapping({
            "llm_concurrency": 51,
        })
    with pytest.raises(ValueError, match="llm_requests_per_minute"):
        CninfoCorporateActionPipelineConfig.from_mapping({
            "llm_requests_per_minute": -1,
        })
    with pytest.raises(ValueError, match="llm_requests_per_minute"):
        CninfoCorporateActionPipelineConfig.from_mapping({
            "llm_requests_per_minute": 0.5,
        })
    with pytest.raises(ValueError, match="llm_requests_per_minute"):
        CninfoCorporateActionPipelineConfig.from_mapping({
            "llm_requests_per_minute": True,
        })
    with pytest.raises(ValueError, match="writer_concurrency"):
        CninfoCorporateActionPipelineConfig.from_mapping({
            "writer_concurrency": 2,
        })
    with pytest.raises(ValueError, match="verification_policy"):
        CninfoCorporateActionPipelineConfig.from_mapping({
            "verification_policy": "never",
        })


def test_pipeline_excludes_bse_without_supplementing_cninfo():
    selected, excluded = normalize_cninfo_pipeline_exchanges(
        ["SSE", "SZSE", "BSE"]
    )
    assert selected == ("SSE", "SZSE")
    assert excluded == ("BSE",)


@pytest.mark.asyncio
async def test_data_manager_bse_only_resolution_is_explicitly_skipped():
    manager = DataManager()
    manager.db_ops = Mock()
    manager.db_ops.execute_read_query = AsyncMock()

    result = await manager.analyze_cninfo_corporate_action_candidates(
        start_date="2020-01-01",
        end_date="2026-07-22",
        exchanges=["BSE"],
        dry_run=True,
    )

    assert result["status"] == "dry_run"
    assert result["parameters"]["exchanges"] == []
    assert result["parameters"]["excluded_exchanges"] == ["BSE"]
    assert result["skip_reason"] == "cninfo_bse_not_supported"
    manager.db_ops.execute_read_query.assert_not_awaited()


@pytest.mark.asyncio
async def test_data_manager_closes_application_owned_llm_client():
    manager = DataManager()
    client = SimpleNamespace(close=AsyncMock())
    manager._llm_client = client
    manager.source_factory = None
    manager.db_ops = SimpleNamespace(db=None)

    await manager.close()

    client.close.assert_awaited_once()
    assert manager._llm_client is None


def test_identity_advances_without_changing_business_key():
    initial = _identity()
    advanced = initial.advance(
        CorporateActionPipelineStage.TITLE_CLASSIFICATION,
        input_hash="input-1",
        schema_version="title.v1",
    )
    assert_same_business_identity(initial, advanced)
    assert advanced.stage_sequence == 1
    assert advanced.source_event_key == initial.source_event_key

    with pytest.raises(ValueError, match="identity changed"):
        assert_same_business_identity(
            initial,
            PipelineIdentity(
                instrument_id="000001.SZ",
                source_event_key="event-2",
                run_id="run-1",
                stage=CorporateActionPipelineStage.DISCOVERY,
                stage_sequence=1,
            ),
        )


def test_title_bundle_rejects_duplicate_announcement_identity():
    with pytest.raises(ValueError, match="duplicate"):
        TitleBundlePayload(
            identity=_identity(),
            bundle_id="bundle-1",
            announcements=(
                {"announcement_id": "a-1", "title": "one"},
                {"announcement_id": "a-1", "title": "two"},
            ),
        )


def test_extraction_case_uses_artifact_and_page_references_not_bytes():
    identity = _identity(
        CorporateActionPipelineStage.DOCUMENT_PARSE, sequence=4
    )
    artifact = ArtifactPayload(
        identity=identity,
        announcement_id="announcement-1",
        artifact_hash="artifact-hash",
        artifact_ref="filings/corporate_actions/announcement-1.pdf",
        content_length=1024,
    )
    page = ParsedPageReference(
        announcement_id="announcement-1",
        artifact_hash="artifact-hash",
        page_number=1,
        text_hash="text-hash",
        extraction_method="native_text",
        quality_status="usable",
    )
    case = ExtractionCasePayload(
        identity=identity.advance(
            CorporateActionPipelineStage.SEMANTIC_EXTRACTION,
            input_hash="input-hash",
        ),
        artifacts=(artifact,),
        pages=(page,),
        context_ref="context:input-hash",
    )
    assert case.pages[0].text_hash == "text-hash"
    assert not hasattr(case, "document_bytes")


def test_resume_fingerprint_changes_when_any_version_or_artifact_changes():
    current = ResumeFingerprint(
        source_event_key="event-1",
        artifact_hashes=("artifact-1",),
        page_text_hashes=("page-1",),
        input_hash="input-1",
        prompt_version="prompt.v1",
        schema_version="schema.v1",
        model_policy="policy.v1",
    )
    assert current.matches(current)
    changed = ResumeFingerprint(
        source_event_key="event-1",
        artifact_hashes=("artifact-2",),
        page_text_hashes=("page-1",),
        input_hash="input-1",
        prompt_version="prompt.v1",
        schema_version="schema.v1",
        model_policy="policy.v1",
    )
    assert not current.matches(changed)
    assert decide_stage_resume(
        current,
        committed=current,
        committed_status="success",
    ).reuse is True
    assert decide_stage_resume(
        current,
        committed=changed,
        committed_status="success",
    ).reason == "input_or_version_changed"
    assert decide_stage_resume(
        current,
        committed=current,
        committed_status="failed",
    ).reuse is False
    assert decide_stage_resume(
        current,
        committed=current,
        committed_status="success",
        force_rerun=True,
    ).reason == "operator_forced_rerun"


def test_semantic_verification_policy_is_conservative():
    strong = {
        "analysis_status": "resolved_candidate",
        "event_stage": "implemented",
        "confidence": 0.99,
        "evidence": [{"evidence_id": "one"}],
        "conflicts": [],
        "warnings": [],
    }
    assert requires_semantic_verification("always", analysis=strong) is True
    assert requires_semantic_verification(
        "risk_based", analysis=strong
    ) is False
    assert requires_semantic_verification(
        "risk_based", analysis={**strong, "confidence": 0.8}
    ) is True


@pytest.mark.asyncio
async def test_persistence_writer_serializes_and_deduplicates_commands():
    active = 0
    peak = 0
    calls = []

    async def writer(command):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        calls.append(command.idempotency_key)
        await asyncio.sleep(0.01)
        active -= 1
        return {"status": "saved", "key": command.idempotency_key}

    persistence = CninfoSerialPersistenceWriter(writer=writer, queue_size=4)
    await persistence.start()
    identity = _identity(
        CorporateActionPipelineStage.PERSISTENCE, sequence=8
    )
    first = PersistenceCommand(
        identity=identity,
        command_type="save_analysis",
        idempotency_key="command-1",
        payload_ref="analysis:1",
        expected_input_hash="input-1",
    )
    second = PersistenceCommand(
        identity=identity,
        command_type="save_analysis",
        idempotency_key="command-2",
        payload_ref="analysis:2",
        expected_input_hash="input-2",
    )
    first_result, duplicate_result, second_result = await asyncio.gather(
        persistence.submit(first),
        persistence.submit(first),
        persistence.submit(second),
    )
    await persistence.close()

    assert peak == 1
    assert calls == ["command-1", "command-2"]
    assert first_result == duplicate_result
    assert second_result["key"] == "command-2"


@pytest.mark.asyncio
async def test_persistence_writer_dry_run_and_stale_identity_do_not_write():
    calls = []

    async def writer(command):
        calls.append(command)
        return {"status": "saved"}

    async def stale(command):
        return False

    persistence = CninfoSerialPersistenceWriter(
        writer=writer,
        identity_validator=stale,
    )
    await persistence.start()
    identity = _identity(
        CorporateActionPipelineStage.PERSISTENCE, sequence=8
    )
    dry_run = PersistenceCommand(
        identity=identity,
        command_type="save_analysis",
        idempotency_key="dry-run",
        payload_ref="analysis:dry",
        expected_input_hash="input-dry",
        dry_run=True,
    )
    assert (await persistence.submit(dry_run))["status"] == "dry_run"
    stale_command = PersistenceCommand(
        identity=identity,
        command_type="save_analysis",
        idempotency_key="stale",
        payload_ref="analysis:stale",
        expected_input_hash="input-stale",
    )
    with pytest.raises(RuntimeError, match="stale or superseded"):
        await persistence.submit(stale_command)
    await persistence.close()

    assert calls == []


@pytest.mark.asyncio
async def test_document_stage_separates_download_and_parse_limits():
    class Artifact:
        def __init__(self, announcement_id):
            self.announcement_id = announcement_id
            self.source_url = f"https://example.test/{announcement_id}.pdf"
            self.content_hash = f"hash-{announcement_id}"
            self.content_type = "application/pdf"
            self.content_length = 100
            self.archive_path = f"{announcement_id}/document.pdf"
            self.source = "cninfo"

    class Service:
        def __init__(self):
            self.download_active = 0
            self.download_peak = 0
            self.parse_active = 0
            self.parse_peak = 0

        def retrieve_and_archive(self, **kwargs):
            import time

            self.download_active += 1
            self.download_peak = max(
                self.download_peak, self.download_active
            )
            time.sleep(0.01)
            self.download_active -= 1
            return Artifact(kwargs["announcement_id"])

        def parse_artifact(self, artifact):
            import time
            from types import SimpleNamespace

            self.parse_active += 1
            self.parse_peak = max(self.parse_peak, self.parse_active)
            time.sleep(0.01)
            self.parse_active -= 1
            page = SimpleNamespace(
                page_number=1,
                text_hash=f"text-{artifact.announcement_id}",
                extraction_method="native_text",
                quality_status="usable",
            )
            return SimpleNamespace(
                announcement_id=artifact.announcement_id,
                source_url=artifact.source_url,
                content_hash=artifact.content_hash,
                content_type=artifact.content_type,
                content_length=artifact.content_length,
                archive_path=artifact.archive_path,
                source=artifact.source,
                pages=(page,),
                extraction_status="extracted",
            )

    service = Service()
    stage = CninfoDocumentPreparationStage(
        service=service,
        download_concurrency=4,
        document_parse_concurrency=2,
    )
    selected = [
        SelectedAnnouncementPayload(
            identity=_identity(
                CorporateActionPipelineStage.TITLE_CLASSIFICATION,
                sequence=2,
            ),
            announcement_id=f"announcement-{index}",
            title=f"title-{index}",
            published_at=None,
            attachment_url=f"https://example.test/{index}.pdf",
            announcement_role="implementation",
            classification_request_hash=f"request-{index}",
        )
        for index in range(8)
    ]
    results = await asyncio.gather(*(stage.prepare(item) for item in selected))

    assert service.download_peak == 4
    assert service.parse_peak == 2
    assert len(results) == 8
    assert all(not hasattr(item.artifact, "content") for item in results)


@pytest.mark.asyncio
async def test_document_stage_deduplicates_inflight_artifact_work():
    class Service:
        def __init__(self):
            self.download_calls = 0
            self.parse_calls = 0

        def retrieve_and_archive(self, **kwargs):
            import time

            self.download_calls += 1
            time.sleep(0.02)
            return SimpleNamespace(
                announcement_id=kwargs["announcement_id"],
                source_url=kwargs["source_url"],
                content_hash="same-hash",
                content_type="application/pdf",
                content_length=100,
                archive_path="ann-1/same-hash.pdf",
                source="cninfo",
            )

        def parse_artifact(self, artifact):
            self.parse_calls += 1
            return SimpleNamespace(
                announcement_id=artifact.announcement_id,
                source_url=artifact.source_url,
                content_hash=artifact.content_hash,
                content_type=artifact.content_type,
                content_length=artifact.content_length,
                archive_path=artifact.archive_path,
                source=artifact.source,
                extraction_status="extracted",
                pages=(SimpleNamespace(
                    page_number=1,
                    text_hash="page-hash",
                    extraction_method="native_text",
                    quality_status="usable",
                ),),
            )

    service = Service()
    stage = CninfoDocumentPreparationStage(
        service=service,
        download_concurrency=4,
        document_parse_concurrency=2,
    )
    selected = SelectedAnnouncementPayload(
        identity=_identity(
            CorporateActionPipelineStage.TITLE_CLASSIFICATION,
            sequence=2,
        ),
        announcement_id="ann-1",
        title="权益分派实施公告",
        published_at=None,
        attachment_url="https://example.test/ann-1.pdf",
        announcement_role="implementation",
        classification_request_hash="request-1",
    )

    first, second = await asyncio.gather(
        stage.prepare(selected),
        stage.prepare(selected),
    )

    assert first.artifact.artifact_hash == second.artifact.artifact_hash
    assert service.download_calls == 1
    assert service.parse_calls == 1
    assert stage.snapshot()["cached_artifacts"] == 1


@pytest.mark.asyncio
async def test_failed_persistence_remains_unacknowledged_and_can_retry():
    attempts = 0

    async def writer(command):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("database transaction rolled back")
        return {"status": "saved"}

    persistence = CninfoSerialPersistenceWriter(writer=writer)
    await persistence.start()
    command = PersistenceCommand(
        identity=_identity(
            CorporateActionPipelineStage.PERSISTENCE, sequence=8
        ),
        command_type="save_analysis",
        idempotency_key="retryable-command",
        payload_ref="analysis:retry",
        expected_input_hash="input-retry",
    )
    with pytest.raises(RuntimeError, match="rolled back"):
        await persistence.submit(command)
    assert await persistence.submit(command) == {"status": "saved"}
    await persistence.close()

    assert attempts == 2


@pytest.mark.asyncio
async def test_resolution_pipeline_routes_out_of_order_events_without_cross_linking():
    persisted = []

    async def prepare(item):
        key = item.identity.source_event_key
        if key == "slow-event":
            await asyncio.sleep(0.03)
        return ResolutionStagePayload(
            identity=item.identity.advance(
                CorporateActionPipelineStage.DOCUMENT_PARSE
            ),
            payload_ref=f"pages:{key}",
        )

    async def analyze(item):
        return ResolutionStagePayload(
            identity=item.identity.advance(
                CorporateActionPipelineStage.DETERMINISTIC_VALIDATION,
                input_hash=f"input:{item.identity.source_event_key}",
            ),
            payload_ref=f"analysis:{item.identity.source_event_key}",
        )

    async def persist(item):
        key = item.identity.source_event_key
        persisted.append((item.identity.instrument_id, key, item.payload_ref))
        return {"source_event_key": key}

    config = CninfoCorporateActionPipelineConfig.from_mapping({
        "mode": "async",
        "download_concurrency": 2,
        "llm_concurrency": 2,
        "progress_interval_seconds": 60,
    })
    pipeline = CninfoCorporateActionResolutionPipeline(
        config=config,
        prepare=prepare,
        analyze=analyze,
        persist=persist,
        logger=Mock(),
    )
    items = [
        InventoryPayload(
            identity=PipelineIdentity(
                instrument_id=instrument_id,
                source_event_key=event_key,
                run_id="run-async",
                stage=CorporateActionPipelineStage.INVENTORY,
                stage_sequence=0,
                source_profile="cninfo_dividend",
            ),
            observation={"instrument_id": instrument_id},
        )
        for instrument_id, event_key in (
            ("600108.SH", "slow-event"),
            ("000001.SZ", "fast-event"),
        )
    ]

    run = await pipeline.run(items)

    assert [item[1] for item in persisted] == ["fast-event", "slow-event"]
    assert persisted == [
        ("000001.SZ", "fast-event", "analysis:fast-event"),
        ("600108.SH", "slow-event", "analysis:slow-event"),
    ]
    assert all(
        outcome.status.value == "success" for outcome in run.terminal_outcomes
    )
    assert run.submitted == 2


@pytest.mark.asyncio
async def test_resolution_pipeline_serial_mode_and_duplicate_submission_are_safe():
    calls = []

    async def prepare(item):
        calls.append(("prepare", item.identity.source_event_key))
        return ResolutionStagePayload(
            identity=item.identity.advance(
                CorporateActionPipelineStage.DOCUMENT_PARSE
            ),
            payload_ref="pages:event-1",
        )

    async def analyze(item):
        calls.append(("analyze", item.identity.source_event_key))
        return ResolutionStagePayload(
            identity=item.identity.advance(
                CorporateActionPipelineStage.DETERMINISTIC_VALIDATION
            ),
            payload_ref="analysis:event-1",
        )

    async def persist(item):
        calls.append(("persist", item.identity.source_event_key))
        return {"status": "saved"}

    pipeline = CninfoCorporateActionResolutionPipeline(
        config=CninfoCorporateActionPipelineConfig.from_mapping({
            "mode": "serial"
        }),
        prepare=prepare,
        analyze=analyze,
        persist=persist,
        logger=Mock(),
    )
    item = InventoryPayload(identity=_identity(), observation={"value": 1})

    run = await pipeline.run([item, item])

    assert calls == [
        ("prepare", "event-1"),
        ("analyze", "event-1"),
        ("persist", "event-1"),
    ]
    assert run.submitted == 1
    assert run.duplicate_submissions == 1


@pytest.mark.asyncio
async def test_resolution_pipeline_rejects_cross_event_callback_output():
    async def prepare(item):
        return ResolutionStagePayload(
            identity=PipelineIdentity(
                instrument_id="000001.SZ",
                source_event_key="other-event",
                run_id=item.identity.run_id,
                stage=CorporateActionPipelineStage.DOCUMENT_PARSE,
                stage_sequence=item.identity.stage_sequence + 1,
                source_profile=item.identity.source_profile,
            ),
            payload_ref="pages:wrong",
        )

    pipeline = CninfoCorporateActionResolutionPipeline(
        config=CninfoCorporateActionPipelineConfig.from_mapping({
            "mode": "async",
            "progress_interval_seconds": 60,
        }),
        prepare=prepare,
        analyze=AsyncMock(),
        persist=AsyncMock(),
        logger=Mock(),
    )

    run = await pipeline.run([
        InventoryPayload(identity=_identity(), observation={"value": 1})
    ])

    assert run.terminal_outcomes[0].status.value == "terminal_failure"
    assert "identity changed" in run.terminal_outcomes[0].error_message
