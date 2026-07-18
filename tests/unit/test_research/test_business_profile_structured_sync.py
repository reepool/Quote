import asyncio
import gzip
import hashlib
import json
import sqlite3
from pathlib import Path

import research.business_profile_structured_sync as sync_module
from research.business_profile_governance import BusinessProfileRepository
from research.business_profile_structured_sync import (
    WRITE_OPERATOR_SWITCH,
    StructuredBusinessProfileSyncService,
)
from research.providers.akshare_business_profile import (
    COMPOSITION_SOURCE,
    INTRODUCTION_SOURCE,
    BusinessCompositionRow,
    BusinessIntroduction,
    StructuredBusinessProfileSnapshot,
    StructuredSourceResult,
)
from research.storage import ResearchStorageManager
from utils.config_manager import (
    ResearchBudgetConfig,
    ResearchConfig,
    ResearchStorageConfig,
)


def _config(tmp_path, *, enabled=True):
    return ResearchConfig(
        enabled=True,
        storage=ResearchStorageConfig(
            db_path=str(tmp_path / "research.db"),
            shadow_mode=True,
            attach_quotes_db=False,
            quotes_db_path=str(tmp_path / "quotes.db"),
            financials_db_path=str(tmp_path / "financials.db"),
            valuation_db_path=str(tmp_path / "valuation.db"),
            interests_db_path=str(tmp_path / "interests.db"),
        ),
        budget=ResearchBudgetConfig(),
        modules={
            "business_profile_evidence": {
                "free_structured_sources": {
                    "enabled": enabled,
                    "candidate_only": True,
                    "sources": [
                        {
                            "source": COMPOSITION_SOURCE,
                            "enabled": True,
                            "possible_row_cap": 200,
                        },
                        {
                            "source": INTRODUCTION_SOURCE,
                            "enabled": True,
                        },
                    ],
                    "runtime": {
                        "request_timeout_seconds": 2,
                        "request_interval_seconds": 0,
                        "retry_attempts": 1,
                        "retry_backoff_seconds": 0,
                        "max_instruments_per_run": 2,
                        "max_elapsed_seconds": 60,
                        "raw_cache_root": str(tmp_path / "raw"),
                        "checkpoint_root": str(tmp_path / "checkpoints"),
                    },
                }
            }
        },
    )


def _universe():
    return [
        {
            "instrument_id": "601088.SH",
            "industry_group": "coal",
            "exchange": "SSE",
        }
    ]


def _two_company_universe():
    return [
        *_universe(),
        {
            "instrument_id": "600188.SH",
            "industry_group": "coal",
            "exchange": "SSE",
        },
    ]


def _composition(instrument_id="601088.SH"):
    raw_payload = ({"ITEM_NAME": "动力煤"},)
    row = BusinessCompositionRow(
        instrument_id=instrument_id,
        report_period="2025-12-31",
        classification_type="product",
        item_name="动力煤",
        revenue=1000,
        revenue_ratio=0.8,
        cost=600,
        cost_ratio=0.75,
        profit=400,
        profit_ratio=0.9,
        gross_margin=0.4,
        source_row_hash="row-hash",
    )
    return StructuredSourceResult(
        source=COMPOSITION_SOURCE,
        status="success",
        payload_hash=_payload_hash(raw_payload),
        rows=(row,),
        raw_payload=raw_payload,
        elapsed_seconds=0.1,
    )


def _introduction(status="success"):
    if status == "failed":
        return StructuredSourceResult(
            source=INTRODUCTION_SOURCE,
            status="failed",
            payload_hash=None,
            diagnostics=("RuntimeError:temporary",),
            elapsed_seconds=0.2,
        )
    item = BusinessIntroduction(
        instrument_id="601088.SH",
        main_business="煤炭生产",
        product_types="煤炭",
        product_names="动力煤",
        business_scope=None,
        source_row_hash="intro-hash",
    )
    return StructuredSourceResult(
        source=INTRODUCTION_SOURCE,
        status="success",
        payload_hash="b" * 64,
        introduction=item,
        raw_payload=({"主营业务": "煤炭生产"},),
        elapsed_seconds=0.1,
    )


def _payload_hash(payload):
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class _ResumeProvider:
    def __init__(self):
        self.calls = []

    async def fetch(
        self,
        instrument_id,
        *,
        observed_at,
        sources,
        deadline_monotonic,
    ):
        self.calls.append(tuple(sources))
        composition = (
            _composition()
            if COMPOSITION_SOURCE in sources
            else StructuredSourceResult(
                source=COMPOSITION_SOURCE,
                status="skipped",
                payload_hash=None,
            )
        )
        introduction = (
            _introduction("failed" if len(self.calls) == 1 else "success")
            if INTRODUCTION_SOURCE in sources
            else StructuredSourceResult(
                source=INTRODUCTION_SOURCE,
                status="skipped",
                payload_hash=None,
            )
        )
        return StructuredBusinessProfileSnapshot(
            instrument_id=instrument_id,
            observed_at=observed_at,
            composition=composition,
            introduction=introduction,
        )


def test_checkpoint_resume_retries_only_failed_instrument_source(tmp_path):
    config = _config(tmp_path)
    storage = ResearchStorageManager(config)
    provider = _ResumeProvider()
    service = StructuredBusinessProfileSyncService(
        storage=storage,
        research_config=config,
        provider=provider,
    )
    checkpoint = tmp_path / "checkpoint.json"

    first = asyncio.run(
        service.sync(
            universe=_universe(),
            dry_run=True,
            checkpoint_path=checkpoint,
        )
    )
    second = asyncio.run(
        service.sync(
            universe=_universe(),
            dry_run=True,
            checkpoint_path=checkpoint,
            resume=True,
        )
    )

    assert first["status"] == "degraded"
    assert first["remaining_source_count"] == 1
    assert second["status"] == "success"
    assert second["completed"] is True
    assert provider.calls == [
        (COMPOSITION_SOURCE, INTRODUCTION_SOURCE),
        (INTRODUCTION_SOURCE,),
    ]


def test_candidate_write_uses_raw_cache_reference_and_remains_candidate_only(
    tmp_path,
):
    config = _config(tmp_path)
    storage = ResearchStorageManager(config)
    storage.initialize()

    class _Provider:
        async def fetch(
            self,
            instrument_id,
            *,
            observed_at,
            sources,
            deadline_monotonic,
        ):
            return StructuredBusinessProfileSnapshot(
                instrument_id=instrument_id,
                observed_at=observed_at,
                composition=_composition(),
                introduction=StructuredSourceResult(
                    source=INTRODUCTION_SOURCE,
                    status="skipped",
                    payload_hash=None,
                ),
            )

    service = StructuredBusinessProfileSyncService(
        storage=storage,
        research_config=config,
        provider=_Provider(),
    )
    report = asyncio.run(
        service.sync(
            universe=_universe(),
            sources=[COMPOSITION_SOURCE],
            dry_run=False,
            candidate_write=True,
            operator_switch=WRITE_OPERATOR_SWITCH,
            cache_raw_snapshots=True,
            checkpoint_path=tmp_path / "write.checkpoint.json",
        )
    )
    history = BusinessProfileRepository(storage).get_profile_history("601088.SH")

    assert report["status"] == "success"
    assert report["candidate_evidence_written"] == 1
    assert report["candidate_segments_written"] == 1
    assert report["dcf_leakage"]["status"] == "pass"
    assert history["value_chain_roles"] == []
    assert history["exposures"] == []
    evidence_metadata = history["evidence"][0]["metadata"]
    reference = evidence_metadata["raw_snapshot_reference"]
    assert evidence_metadata["raw_payload"] == []
    assert reference["payload_hash"] == _payload_hash(({"ITEM_NAME": "动力煤"},))
    with gzip.open(Path(reference["cache_path"]), "rt", encoding="utf-8") as file_obj:
        cached = json.load(file_obj)
    assert cached["raw_payload"] == [{"ITEM_NAME": "动力煤"}]
    assert report["raw_manifest_path"]
    source_report = report["sources"][COMPOSITION_SOURCE]
    assert source_report["raw_field_names"] == ["ITEM_NAME"]
    assert source_report["raw_field_non_empty_counts"] == {"ITEM_NAME": 1}
    assert source_report["introduction_count"] == 0

    second = asyncio.run(
        service.sync(
            universe=_universe(),
            sources=[COMPOSITION_SOURCE],
            dry_run=False,
            candidate_write=True,
            operator_switch=WRITE_OPERATOR_SWITCH,
            checkpoint_path=tmp_path / "write-again.checkpoint.json",
        )
    )
    assert second["candidate_evidence_written"] == 0
    assert second["candidate_segments_written"] == 0
    assert second["sources"][COMPOSITION_SOURCE]["payload_unchanged_count"] == 1
    assert second["sources"][COMPOSITION_SOURCE]["cache_existing_count"] == 1


def test_elapsed_limit_stops_before_next_instrument_and_leaves_resume_state(
    tmp_path,
):
    config = _config(tmp_path)
    storage = ResearchStorageManager(config)

    class _Provider:
        calls = 0

        async def fetch(
            self,
            instrument_id,
            *,
            observed_at,
            sources,
            deadline_monotonic,
        ):
            self.calls += 1
            composition = _composition(instrument_id)
            return StructuredBusinessProfileSnapshot(
                instrument_id=instrument_id,
                observed_at=observed_at,
                composition=composition,
                introduction=StructuredSourceResult(
                    source=INTRODUCTION_SOURCE,
                    status="skipped",
                    payload_hash=None,
                ),
            )

    class _Clock:
        def __init__(self):
            self.values = iter([0, 0, 0, 0, 61, 61])
            self.last = 0

        def __call__(self):
            self.last = next(self.values, self.last)
            return self.last

    provider = _Provider()
    service = StructuredBusinessProfileSyncService(
        storage=storage,
        research_config=config,
        provider=provider,
        clock=_Clock(),
    )
    report = asyncio.run(
        service.sync(
            universe=_two_company_universe(),
            sources=[COMPOSITION_SOURCE],
            dry_run=True,
            max_instruments=2,
            max_elapsed_seconds=60,
            checkpoint_path=tmp_path / "elapsed.checkpoint.json",
        )
    )

    assert report["status"] == "interrupted"
    assert report["stopped_reason"] == "max_elapsed_seconds"
    assert report["attempted_instruments"] == 1
    assert report["remaining_source_count"] == 1
    assert provider.calls == 1


def test_disabled_config_allows_explicit_dry_probe_but_never_candidate_write(
    tmp_path,
):
    config = _config(tmp_path, enabled=False)
    storage = ResearchStorageManager(config)

    class _Provider:
        async def fetch(
            self,
            instrument_id,
            *,
            observed_at,
            sources,
            deadline_monotonic,
        ):
            return StructuredBusinessProfileSnapshot(
                instrument_id=instrument_id,
                observed_at=observed_at,
                composition=_composition(),
                introduction=_introduction(),
            )

    service = StructuredBusinessProfileSyncService(
        storage=storage,
        research_config=config,
        provider=_Provider(),
    )
    report = asyncio.run(
        service.sync(
            universe=_universe(),
            dry_run=True,
            allow_disabled_dry_run=True,
            checkpoint_path=tmp_path / "probe.checkpoint.json",
        )
    )
    assert report["status"] == "success"

    try:
        asyncio.run(
            service.sync(
                universe=_universe(),
                dry_run=False,
                candidate_write=True,
                operator_switch=WRITE_OPERATOR_SWITCH,
            )
        )
    except RuntimeError as exc:
        assert "disabled" in str(exc)
    else:
        raise AssertionError("disabled config must reject candidate writes")


def test_manifest_failure_does_not_write_candidates_or_leave_run_open(
    tmp_path,
    monkeypatch,
):
    config = _config(tmp_path)
    storage = ResearchStorageManager(config)
    storage.initialize()

    class _Provider:
        async def fetch(
            self,
            instrument_id,
            *,
            observed_at,
            sources,
            deadline_monotonic,
        ):
            return StructuredBusinessProfileSnapshot(
                instrument_id=instrument_id,
                observed_at=observed_at,
                composition=_composition(),
                introduction=StructuredSourceResult(
                    source=INTRODUCTION_SOURCE,
                    status="skipped",
                    payload_hash=None,
                ),
            )

    def _fail_manifest(**_kwargs):
        raise OSError("manifest unavailable")

    monkeypatch.setattr(sync_module, "_write_raw_manifest", _fail_manifest)
    service = StructuredBusinessProfileSyncService(
        storage=storage,
        research_config=config,
        provider=_Provider(),
    )
    checkpoint = tmp_path / "manifest-failure.checkpoint.json"

    report = asyncio.run(
        service.sync(
            universe=_universe(),
            sources=[COMPOSITION_SOURCE],
            dry_run=False,
            candidate_write=True,
            operator_switch=WRITE_OPERATOR_SWITCH,
            checkpoint_path=checkpoint,
        )
    )

    with sqlite3.connect(config.storage.db_path) as conn:
        evidence_count = conn.execute(
            "SELECT COUNT(*) FROM business_profile_evidence"
        ).fetchone()[0]
        run = conn.execute(
            """
            SELECT status, completed_at
            FROM ingestion_runs
            WHERE domain = 'business_profile_structured'
            """
        ).fetchone()
    checkpoint_payload = json.loads(checkpoint.read_text(encoding="utf-8"))
    assert report["status"] == "degraded"
    assert report["remaining_source_count"] == 1
    assert evidence_count == 0
    assert run[0] == "degraded"
    assert run[1]
    assert checkpoint_payload["completed_source_keys"] == []


def test_candidate_run_measures_and_fails_on_governance_leakage(tmp_path):
    config = _config(tmp_path)
    storage = ResearchStorageManager(config)
    storage.initialize()

    class _Provider:
        async def fetch(
            self,
            instrument_id,
            *,
            observed_at,
            sources,
            deadline_monotonic,
        ):
            return StructuredBusinessProfileSnapshot(
                instrument_id=instrument_id,
                observed_at=observed_at,
                composition=_composition(),
                introduction=StructuredSourceResult(
                    source=INTRODUCTION_SOURCE,
                    status="skipped",
                    payload_hash=None,
                ),
            )

    service = StructuredBusinessProfileSyncService(
        storage=storage,
        research_config=config,
        provider=_Provider(),
    )
    original_write = service.writer.write

    def _leaking_write(*args, **kwargs):
        result = original_write(*args, **kwargs)
        with storage.get_connection() as conn:
            conn.execute(
                """
                UPDATE business_profile_evidence
                SET review_status = 'approved'
                WHERE instrument_id = '601088.SH'
                """
            )
            conn.commit()
        return result

    service.writer.write = _leaking_write
    report = asyncio.run(
        service.sync(
            universe=_universe(),
            sources=[COMPOSITION_SOURCE],
            dry_run=False,
            candidate_write=True,
            operator_switch=WRITE_OPERATOR_SWITCH,
            checkpoint_path=tmp_path / "leakage.checkpoint.json",
        )
    )

    assert report["status"] == "failed"
    assert report["dcf_leakage"]["approved_records_written"] == 1
    assert report["dcf_leakage"]["status"] == "fail"
    with sqlite3.connect(config.storage.db_path) as conn:
        run_status = conn.execute(
            """
            SELECT status
            FROM ingestion_runs
            WHERE domain = 'business_profile_structured'
            """
        ).fetchone()[0]
    assert run_status == "failed"


def test_max_elapsed_seconds_cancels_in_flight_provider(tmp_path):
    config = _config(tmp_path)
    storage = ResearchStorageManager(config)

    class _SlowProvider:
        async def fetch(
            self,
            instrument_id,
            *,
            observed_at,
            sources,
            deadline_monotonic,
        ):
            await asyncio.sleep(1)

    service = StructuredBusinessProfileSyncService(
        storage=storage,
        research_config=config,
        provider=_SlowProvider(),
    )
    report = asyncio.run(
        service.sync(
            universe=_universe(),
            dry_run=True,
            max_elapsed_seconds=0.02,
            checkpoint_path=tmp_path / "deadline.checkpoint.json",
        )
    )

    assert report["status"] == "interrupted"
    assert report["stopped_reason"] == "max_elapsed_seconds"
    assert report["elapsed_seconds"] < 0.2
