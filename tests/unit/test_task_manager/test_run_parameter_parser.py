from utils.task_manager.handlers import TaskManagerHandlers


def test_run_parameter_parser_preserves_numeric_one_and_zero():
    handler = TaskManagerHandlers.__new__(TaskManagerHandlers)

    params = handler._parse_run_runtime_parameters(
        [
            "chunk_size=1",
            "limit=0",
            "resume=false",
            "repair_pending_factor_quotes=true",
        ]
    )

    assert params["chunk_size"] == 1
    assert type(params["chunk_size"]) is int
    assert params["limit"] == 0
    assert type(params["limit"]) is int
    assert params["resume"] is False
    assert params["repair_pending_factor_quotes"] is True


def test_run_parameter_parser_splits_source_event_keys():
    handler = TaskManagerHandlers.__new__(TaskManagerHandlers)

    params = handler._parse_run_runtime_parameters(
        ["source_event_keys=event-1,event-2"]
    )

    assert params["source_event_keys"] == ["event-1", "event-2"]


def test_run_parameter_parser_types_business_profile_scope_lists():
    handler = TaskManagerHandlers.__new__(TaskManagerHandlers)

    params = handler._parse_run_runtime_parameters(
        [
            "field_families=structured_segments,tabular_operating_facts",
            "document_types=annual_report,annual_report_correction",
            "instrument_ids=600000.SH,000001.SZ",
            "selection_policy=latest_annual_only",
        ]
    )

    assert params["field_families"] == [
        "structured_segments",
        "tabular_operating_facts",
    ]
    assert params["document_types"] == [
        "annual_report",
        "annual_report_correction",
    ]
    assert params["instrument_ids"] == ["600000.SH", "000001.SZ"]
    assert params["selection_policy"] == "latest_annual_only"


def test_run_parameter_parser_types_continuous_backfill_controls():
    handler = TaskManagerHandlers.__new__(TaskManagerHandlers)

    params = handler._parse_run_runtime_parameters(
        [
            "continuous=true",
            "continuous_poll_seconds=15",
            "continuous_max_idle_cycles=4",
            "progress_report_interval_seconds=300",
            "action=stop",
            "reason=maintenance_window",
        ]
    )

    assert params == {
        "continuous": True,
        "continuous_poll_seconds": 15,
        "continuous_max_idle_cycles": 4,
        "progress_report_interval_seconds": 300,
        "action": "stop",
        "reason": "maintenance_window",
    }
