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
