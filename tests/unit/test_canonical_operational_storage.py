import json
from datetime import date, datetime

import pytest

from database.connection import DatabaseManager
from database.models import (
    AdjustmentFactorCanonicalDB,
    AdjustmentFactorDecisionDB,
    AdjustmentFactorInstrumentStatusDB,
    AdjustmentFactorSeriesStatusDB,
    CorporateActionInstrumentStatusDB,
    InstrumentDB,
)
from database.operations import DatabaseOperations


async def _ops_for_tmp_db(tmp_path):
    manager = DatabaseManager(str(tmp_path / "canonical_ops.db"))
    manager.initialize()
    manager.create_tables()
    operations = DatabaseOperations(auto_initialize=False)
    operations.db = manager
    operations.engine = manager.sync_engine
    operations.async_engine = manager.async_engine
    operations.SessionLocal = manager.SessionLocal
    operations.AsyncSessionLocal = manager.TaskAsyncSessionLocal
    with manager.get_session() as session:
        session.add(InstrumentDB(
            instrument_id="000001.SZ",
            symbol="000001",
            name="Ping An Bank",
            exchange="SZSE",
            type="stock",
            currency="CNY",
            source="unit",
        ))
        session.commit()
    return manager, operations


def _decision():
    return {
        "instrument_id": "000001.SZ",
        "segment_id": "000001.SZ:1",
        "start_date": "1991-04-03",
        "end_date": "2026-07-31",
        "selected_source": "cninfo",
        "confidence": "high",
        "reason": "independent_consensus",
    }


@pytest.mark.asyncio
async def test_series_write_normalizes_decisions_and_keeps_report_compact(tmp_path):
    manager, operations = await _ops_for_tmp_db(tmp_path)
    try:
        await operations.upsert_adjustment_factor_series_status(
            "v1",
            {
                "status": "promoted",
                "promotion_eligible": True,
                "instrument_count": 1,
                "row_count": 2,
                "coverage_ratio": 1.0,
                "decisions": [_decision()],
            },
        )
        status = await operations.get_adjustment_factor_series_status("v1")
        quality = await operations.get_adjustment_factor_series_quality("v1")
        decisions = await operations.get_adjustment_factor_decisions(
            series_version="v1",
            instrument_id="000001.SZ",
        )

        assert "decisions" not in status
        assert status["decision_count"] == 1
        assert status["report_format"] == "canonical_summary_v2"
        assert quality["decision_storage"] == "adjustment_factor_decisions"
        assert quality["coverage_ratio"] == 1.0
        assert [item["segment_id"] for item in decisions] == ["000001.SZ:1"]
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_series_write_normalizes_nested_candidate_decisions(tmp_path):
    manager, operations = await _ops_for_tmp_db(tmp_path)
    try:
        await operations.upsert_adjustment_factor_series_status(
            "nested-write-v1",
            {
                "status": "promoted",
                "promotion_eligible": True,
                "candidate": {"decisions": [_decision()]},
            },
        )

        status = await operations.get_adjustment_factor_series_status(
            "nested-write-v1"
        )
        decisions = await operations.get_adjustment_factor_decisions(
            series_version="nested-write-v1",
            instrument_id="000001.SZ",
        )
        assert status["decision_count"] == 1
        assert "decisions" not in status.get("candidate", {})
        assert [item["segment_id"] for item in decisions] == ["000001.SZ:1"]
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_decision_migration_is_previewable_verified_and_idempotent(tmp_path):
    manager, operations = await _ops_for_tmp_db(tmp_path)
    try:
        report = {
            "status": "promoted",
            "promotion_eligible": True,
            "decisions": [_decision()],
        }
        with manager.get_session() as session:
            session.add(AdjustmentFactorSeriesStatusDB(
                series_version="legacy-v1",
                status="promoted",
                promotion_eligible=True,
                report_json=json.dumps(report),
            ))
            session.commit()

        preview = await operations.migrate_adjustment_factor_series_decisions(
            series_versions=["legacy-v1"],
            dry_run=True,
        )
        with manager.get_session() as session:
            assert session.query(AdjustmentFactorDecisionDB).count() == 0

        applied = await operations.migrate_adjustment_factor_series_decisions(
            series_versions=["legacy-v1"],
            dry_run=False,
            confirm=True,
        )
        repeated = await operations.migrate_adjustment_factor_series_decisions(
            series_versions=["legacy-v1"],
            dry_run=False,
            confirm=True,
        )

        assert preview["versions"][0]["status"] == "migrate"
        assert preview["status"] == "dry_run"
        assert applied["status"] == "success"
        assert applied["migrated_decisions"] == 1
        assert repeated["versions"][0]["status"] == "already_compact"
        status = await operations.get_adjustment_factor_series_status(
            "legacy-v1"
        )
        assert "decisions" not in status
        assert status["decision_count"] == 1
        assert applied["refreshed_summaries"] == 1
        assert repeated["refreshed_summaries"] == 1
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_summary_refresh_repairs_stale_scalars_without_changing_factors(
    tmp_path,
):
    manager, operations = await _ops_for_tmp_db(tmp_path)
    try:
        decision = _decision() | {"confidence": "low"}
        with manager.get_session() as session:
            session.add(AdjustmentFactorSeriesStatusDB(
                series_version="stable-v1",
                status="promoted",
                promotion_eligible=True,
                instrument_count=1,
                row_count=1,
                coverage_ratio=0.0,
                conflict_count=1,
                report_json=json.dumps({
                    "status": "promoted",
                    "promotion_eligible": True,
                    "instrument_count": 1,
                    "row_count": 1,
                    "coverage_ratio": 0.0,
                    "conflict_count": 1,
                    "overall_completeness": {"status": "partial"},
                    "decision_count": 1,
                }),
            ))
            session.add(AdjustmentFactorDecisionDB(
                **operations._adjustment_factor_decision_payload(
                    "stable-v1", decision
                )
            ))
            session.add(AdjustmentFactorInstrumentStatusDB(
                instrument_id="000001.SZ",
                series_version="stable-v1",
                source="canonical",
                coverage_status="complete_with_events",
                event_count=1,
            ))
            session.add(AdjustmentFactorCanonicalDB(
                instrument_id="000001.SZ",
                ex_date=datetime(2026, 7, 31),
                series_version="stable-v1",
                factor=0.98,
                cumulative_factor=1.0,
                selected_source="cninfo",
                source_profile="unit",
                quality_status="low",
                evidence_count=2,
            ))
            session.commit()

        preview = await operations.refresh_adjustment_factor_series_summaries(
            series_versions=["stable-v1"],
            dry_run=True,
        )
        preview_row = preview["versions"][0]
        assert preview_row["before"]["coverage_ratio"] == 0.0
        assert preview_row["after"]["coverage_ratio"] == 1.0
        assert preview_row["after"]["conflict_count"] == 0
        assert preview_row["after"]["low_confidence_segment_count"] == 1
        assert preview_row["factor_rows_unchanged"] is True
        stale = await operations.get_adjustment_factor_series_status_light(
            "stable-v1"
        )
        assert stale["coverage_ratio"] == 0.0

        applied = await operations.refresh_adjustment_factor_series_summaries(
            series_versions=["stable-v1"],
            dry_run=False,
            confirm=True,
        )
        status = await operations.get_adjustment_factor_series_status(
            "stable-v1"
        )
        light = await operations.get_adjustment_factor_series_status_light(
            "stable-v1"
        )
        factors = await operations.get_canonical_adjustment_factor_page(
            series_version="stable-v1",
            limit=10,
        )

        assert applied["refreshed_summaries"] == 1
        assert light["coverage_ratio"] == 1.0
        assert light["conflict_count"] == 0
        assert status["status"] == "promoted"
        assert status["overall_completeness"]["status"] == "success"
        assert status["low_confidence_segment_count"] == 1
        assert status["conflict_count"] == 0
        assert factors["total"] == 1
        assert factors["items"][0]["factor"] == 0.98
        assert factors["items"][0]["selected_source"] == "cninfo"
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_summary_refresh_counts_only_blocked_decisions_as_conflicts(tmp_path):
    manager, operations = await _ops_for_tmp_db(tmp_path)
    try:
        with manager.get_session() as session:
            session.add(AdjustmentFactorSeriesStatusDB(
                series_version="blocked-v1",
                status="candidate",
                report_json=json.dumps({"decision_count": 2}),
            ))
            for suffix, confidence in (
                ("blocked", "blocked"),
                ("historical", "historical_single_source"),
            ):
                decision = _decision() | {
                    "segment_id": f"000001.SZ:{suffix}",
                    "confidence": confidence,
                }
                session.add(AdjustmentFactorDecisionDB(
                    **operations._adjustment_factor_decision_payload(
                        "blocked-v1", decision
                    )
                ))
            session.add(AdjustmentFactorInstrumentStatusDB(
                instrument_id="000001.SZ",
                series_version="blocked-v1",
                source="canonical",
                coverage_status="incomplete",
                event_count=0,
            ))
            session.commit()

        result = await operations.refresh_adjustment_factor_series_summaries(
            series_versions=["blocked-v1"],
            dry_run=False,
            confirm=True,
        )
        after = result["versions"][0]["after"]

        assert after["coverage_ratio"] == 0.0
        assert after["conflict_count"] == 1
        assert after["historical_single_source_segment_count"] == 1
        assert after["overall_completeness"]["status"] == "partial"
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_summary_refresh_preserves_coverage_without_normalized_statuses(
    tmp_path,
):
    manager, operations = await _ops_for_tmp_db(tmp_path)
    try:
        with manager.get_session() as session:
            session.add(AdjustmentFactorSeriesStatusDB(
                series_version="legacy-coverage-v1",
                status="promoted",
                promotion_eligible=True,
                instrument_count=4,
                coverage_ratio=0.75,
                conflict_count=1,
                report_json=json.dumps({
                    "instrument_count": 4,
                    "coverage_ratio": 0.75,
                    "overall_completeness": {
                        "status": "partial",
                        "instrument_count": 4,
                        "complete_instrument_count": 3,
                        "incomplete_instrument_count": 1,
                    },
                }),
            ))
            session.add(AdjustmentFactorDecisionDB(
                **operations._adjustment_factor_decision_payload(
                    "legacy-coverage-v1",
                    _decision() | {"confidence": "low"},
                )
            ))
            session.commit()

        result = await operations.refresh_adjustment_factor_series_summaries(
            series_versions=["legacy-coverage-v1"],
            dry_run=False,
            confirm=True,
        )
        status = await operations.get_adjustment_factor_series_status(
            "legacy-coverage-v1"
        )

        assert result["versions"][0]["after"]["coverage_ratio"] == 0.75
        assert result["versions"][0]["after"]["conflict_count"] == 0
        assert result["versions"][0]["after"][
            "coverage_refresh_status"
        ] == "preserved_no_instrument_statuses"
        assert status["instrument_count"] == 4
        assert status["coverage_ratio"] == 0.75
        assert status["overall_completeness"]["status"] == "partial"
        assert status["coverage_refresh_status"] == (
            "preserved_no_instrument_statuses"
        )
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_decision_read_uses_legacy_report_until_migration(tmp_path):
    manager, operations = await _ops_for_tmp_db(tmp_path)
    try:
        with manager.get_session() as session:
            session.add(AdjustmentFactorSeriesStatusDB(
                series_version="legacy-read-v1",
                status="promoted",
                promotion_eligible=True,
                report_json=json.dumps({"decisions": [_decision()]}),
            ))
            session.commit()

        decisions = await operations.get_adjustment_factor_decisions(
            series_version="legacy-read-v1",
            instrument_id="000001.SZ",
        )
        quality = await operations.get_adjustment_factor_series_quality(
            "legacy-read-v1"
        )
        page = await operations.get_adjustment_factor_decision_page(
            series_version="legacy-read-v1",
            instrument_id="000001.SZ",
            confidence="high",
            limit=1,
            offset=0,
        )

        assert [item["segment_id"] for item in decisions] == ["000001.SZ:1"]
        assert decisions[0]["series_version"] == "legacy-read-v1"
        assert "decisions" not in quality
        assert quality["decision_count"] == 1
        assert page["total"] == 1
        assert page["returned"] == 1
        assert page["items"][0]["segment_id"] == "000001.SZ:1"
        assert page["items"][0]["series_version"] == "legacy-read-v1"
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_decision_migration_supports_nested_candidate_report(tmp_path):
    manager, operations = await _ops_for_tmp_db(tmp_path)
    try:
        report = {
            "status": "promoted",
            "promotion_eligible": True,
            "candidate": {"decisions": [_decision()]},
        }
        with manager.get_session() as session:
            session.add(AdjustmentFactorSeriesStatusDB(
                series_version="nested-v1",
                status="promoted",
                promotion_eligible=True,
                report_json=json.dumps(report),
            ))
            session.commit()

        applied = await operations.migrate_adjustment_factor_series_decisions(
            series_versions=["nested-v1"],
            dry_run=False,
            confirm=True,
        )

        status = await operations.get_adjustment_factor_series_status(
            "nested-v1"
        )
        decisions = await operations.get_adjustment_factor_decisions(
            series_version="nested-v1",
            instrument_id="000001.SZ",
        )
        assert applied["migrated_decisions"] == 1
        assert decisions[0]["segment_id"] == "000001.SZ:1"
        assert "decisions" not in status["candidate"]
        assert status["decision_count"] == 1
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_decision_migration_rejects_compact_report_with_missing_rows(tmp_path):
    manager, operations = await _ops_for_tmp_db(tmp_path)
    try:
        with manager.get_session() as session:
            session.add(AdjustmentFactorSeriesStatusDB(
                series_version="broken-v1",
                status="promoted",
                promotion_eligible=True,
                report_json=json.dumps({
                    "report_format": "canonical_summary_v2",
                    "decision_count": 1,
                }),
            ))
            session.commit()

        with pytest.raises(RuntimeError, match="compacted decision count"):
            await operations.migrate_adjustment_factor_series_decisions(
                series_versions=["broken-v1"],
                dry_run=True,
            )
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_success_watermark_is_not_erased_by_later_partial_attempt(tmp_path):
    manager, operations = await _ops_for_tmp_db(tmp_path)
    try:
        await operations.upsert_operational_watermark(
            watermark_name="a_share_quote_baostock_sina",
            status="success",
            attempted_through=date(2026, 7, 31),
            metadata={"run": 1},
        )
        await operations.upsert_operational_watermark(
            watermark_name="a_share_quote_baostock_sina",
            status="partial",
            attempted_through=date(2026, 8, 1),
            metadata={"run": 2},
        )

        row = await operations.get_operational_watermark(
            "a_share_quote_baostock_sina"
        )

        assert row["successful_through"].date() == date(2026, 7, 31)
        assert row["last_attempted_through"].date() == date(2026, 8, 1)
        assert row["last_status"] == "partial"
        assert row["metadata"] == {"run": 2}
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_retention_requires_confirmation_and_protects_active(tmp_path):
    manager, operations = await _ops_for_tmp_db(tmp_path)
    try:
        with manager.get_session() as session:
            for version in ("stable", "v1__staging__old", "v1__benchmark__old"):
                session.add(AdjustmentFactorSeriesStatusDB(
                    series_version=version,
                    status="promoted" if version == "stable" else "candidate",
                    promotion_eligible=version == "stable",
                    report_json="{}",
                    updated_at=datetime(2026, 1, 1),
                ))
            session.commit()

        preview = await operations.maintain_adjustment_factor_operational_storage(
            active_series_version="stable",
            keep_recent_staging=0,
            keep_recent_benchmarks=0,
            dry_run=True,
        )
        with pytest.raises(ValueError, match="confirm=true"):
            await operations.maintain_adjustment_factor_operational_storage(
                active_series_version="stable",
                keep_recent_staging=0,
                keep_recent_benchmarks=0,
                dry_run=False,
                confirm=False,
            )
        await operations.maintain_adjustment_factor_operational_storage(
            active_series_version="stable",
            keep_recent_staging=0,
            keep_recent_benchmarks=0,
            dry_run=False,
            confirm=True,
        )

        assert preview["candidate_versions"] == [
            "v1__benchmark__old",
            "v1__staging__old",
        ]
        assert await operations.list_adjustment_factor_series_versions() == [
            "stable"
        ]
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_retention_compacts_old_endpoint_status_but_keeps_latest(tmp_path):
    manager, operations = await _ops_for_tmp_db(tmp_path)
    try:
        with manager.get_session() as session:
            for requested_start, last_attempt in (
                (datetime(2020, 1, 1), datetime(2022, 1, 1)),
                (datetime(2021, 1, 1), datetime(2021, 1, 1)),
            ):
                session.add(CorporateActionInstrumentStatusDB(
                    instrument_id="000001.SZ",
                    source="cninfo",
                    source_profile="cninfo_structured_v1",
                    coverage_status="complete_with_events",
                    event_count=1,
                    requested_start_date=requested_start,
                    requested_end_date=datetime(2022, 1, 1),
                    last_attempt_at=last_attempt,
                ))
            session.commit()

        preview = await operations.maintain_adjustment_factor_operational_storage(
            active_series_version="stable",
            endpoint_status_retention_days=90,
            dry_run=True,
        )
        await operations.maintain_adjustment_factor_operational_storage(
            active_series_version="stable",
            endpoint_status_retention_days=90,
            dry_run=False,
            confirm=True,
        )

        with manager.get_session() as session:
            remaining = session.query(CorporateActionInstrumentStatusDB).all()
        assert preview["candidate_counts"]["endpoint_statuses"] == 1
        assert len(remaining) == 1
        assert remaining[0].requested_start_date == datetime(2020, 1, 1)
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_retention_preserves_old_full_history_endpoint_coverage(tmp_path):
    manager, operations = await _ops_for_tmp_db(tmp_path)
    try:
        with manager.get_session() as session:
            session.add_all([
                CorporateActionInstrumentStatusDB(
                    instrument_id="000001.SZ",
                    source="cninfo",
                    source_profile="cninfo_dividend",
                    coverage_status="complete_with_events",
                    event_count=10,
                    requested_start_date=datetime(1990, 12, 19),
                    requested_end_date=datetime(2026, 7, 1),
                    last_attempt_at=datetime(2022, 1, 1),
                ),
                CorporateActionInstrumentStatusDB(
                    instrument_id="000001.SZ",
                    source="cninfo",
                    source_profile="cninfo_dividend",
                    coverage_status="complete_no_events",
                    event_count=0,
                    requested_start_date=datetime(2026, 7, 2),
                    requested_end_date=datetime(2026, 7, 31),
                    last_attempt_at=datetime(2026, 7, 31),
                ),
            ])
            session.commit()

        preview = await operations.maintain_adjustment_factor_operational_storage(
            active_series_version="stable",
            endpoint_status_retention_days=90,
            dry_run=True,
        )
        await operations.maintain_adjustment_factor_operational_storage(
            active_series_version="stable",
            endpoint_status_retention_days=90,
            dry_run=False,
            confirm=True,
        )

        with manager.get_session() as session:
            remaining = session.query(CorporateActionInstrumentStatusDB).all()
        assert preview["candidate_counts"]["endpoint_statuses"] == 0
        assert len(remaining) == 2
        assert min(row.requested_start_date for row in remaining) == datetime(
            1990, 12, 19
        )
    finally:
        await manager.close_async()


@pytest.mark.asyncio
async def test_retention_does_not_replace_complete_coverage_with_partial(tmp_path):
    manager, operations = await _ops_for_tmp_db(tmp_path)
    try:
        with manager.get_session() as session:
            session.add_all([
                CorporateActionInstrumentStatusDB(
                    instrument_id="000001.SZ",
                    source="cninfo",
                    source_profile="cninfo_dividend",
                    coverage_status="complete_with_events",
                    event_count=10,
                    requested_start_date=datetime(1990, 12, 19),
                    requested_end_date=datetime(2026, 7, 1),
                    last_attempt_at=datetime(2022, 1, 1),
                ),
                CorporateActionInstrumentStatusDB(
                    instrument_id="000001.SZ",
                    source="cninfo",
                    source_profile="cninfo_dividend",
                    coverage_status="partial_missing_fields",
                    event_count=9,
                    requested_start_date=datetime(1990, 12, 19),
                    requested_end_date=datetime(2026, 7, 31),
                    last_attempt_at=datetime(2026, 7, 31),
                ),
            ])
            session.commit()

        preview = await operations.maintain_adjustment_factor_operational_storage(
            active_series_version="stable",
            endpoint_status_retention_days=90,
            dry_run=True,
        )
        await operations.maintain_adjustment_factor_operational_storage(
            active_series_version="stable",
            endpoint_status_retention_days=90,
            dry_run=False,
            confirm=True,
        )

        with manager.get_session() as session:
            remaining = session.query(CorporateActionInstrumentStatusDB).all()
        assert preview["candidate_counts"]["endpoint_statuses"] == 0
        assert {row.coverage_status for row in remaining} == {
            "complete_with_events",
            "partial_missing_fields",
        }
    finally:
        await manager.close_async()
