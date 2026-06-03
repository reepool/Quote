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
    assert "新增: 0，停用: 0，停牌: 0，复活: 0，待复核: 0，活跃合计: 5519" in summary


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


def test_format_instrument_master_governance_summary_hkex_details():
    summary = _format_instrument_master_governance_summary({
        "status": "warning",
        "action": "synced",
        "summary": {
            "added_instruments": 0,
            "deactivated_instruments": 0,
            "suspended_instruments": 0,
            "reactivated_instruments": 1,
            "review_required": 3015,
            "active_count": 3020,
        },
        "exchanges": {
            "HKEX": {
                "mode": "audit_only",
                "official_active_count": 8,
                "official_delisted_count": 2,
                "supplemental_count": 5,
                "safe_write_preview_count": 4,
                "allowed_reactivation_count": 1,
                "allowed_suspension_count": 0,
                "source_usage": {
                    "hkex_securities_list": 8,
                    "hkexnews_active_list": 4,
                },
                "quote_availability": {
                    "no_local_quote_count": 1314,
                    "stale_local_quote_count": 1611,
                },
            }
        },
        "warnings": ["HKEX official securities-list source not configured"],
        "errors": [],
    })

    assert "HKEX: mode=audit_only，official_active=8，official_delisted=2，supplemental=5，safe_write候选=4" in summary
    assert "HKEX生命周期候选: 可复活=1，可停牌=0" in summary
    assert "HKEX源: hkex_securities_list:8，hkexnews_active_list:4" in summary
    assert "HKEX行情诊断: 无本地行情=1314，过旧=1611" in summary
