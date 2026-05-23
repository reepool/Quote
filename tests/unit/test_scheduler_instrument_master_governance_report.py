from scheduler.tasks import _format_instrument_master_governance_summary


def test_format_instrument_master_governance_summary_success():
    summary = _format_instrument_master_governance_summary({
        "status": "fresh",
        "action": "reused_fresh_master",
        "summary": {
            "added_instruments": 0,
            "deactivated_instruments": 0,
            "active_count": 5519,
        },
        "warnings": [],
        "errors": [],
    })

    assert "状态: fresh (reused_fresh_master)" in summary
    assert "新增: 0，停用: 0，活跃合计: 5519" in summary


def test_format_instrument_master_governance_summary_skipped_warning_and_error():
    skipped = _format_instrument_master_governance_summary({
        "status": "skipped",
        "reason": "historical_current_master_governance_skipped",
        "summary": {},
        "warnings": ["HKEX: unsupported market for instrument master governance"],
        "errors": [],
    })
    failed = _format_instrument_master_governance_summary({
        "status": "error",
        "action": "synced",
        "summary": {"added_instruments": 0, "deactivated_instruments": 0, "active_count": 0},
        "warnings": [],
        "errors": ["SSE: instrument master sync timed out after 180s"],
    })

    assert "状态: skipped (historical_current_master_governance_skipped)" in skipped
    assert "警告: HKEX: unsupported market" in skipped
    assert "状态: error (synced)" in failed
    assert "错误: SSE: instrument master sync timed out after 180s" in failed
