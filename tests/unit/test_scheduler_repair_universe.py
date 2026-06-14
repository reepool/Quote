from scheduler.tasks import _format_repair_universe_summary


def test_scheduler_formats_repair_universe_summary_concisely():
    summary = _format_repair_universe_summary({
        'mode': 'historical_backfill',
        'input_instrument_count': 100,
        'eligible_instrument_count': 80,
        'clipped_instrument_count': 5,
        'skipped_instrument_count': 20,
        'skipped_gap_segment_count': 30,
        'skipped_missing_days': 90,
        'reason_distribution': {
            'index_lifecycle_stale_no_quote': 18,
            'after_delisted_date': 2,
        },
        'current_master_refresh': {
            'requested': True,
            'status': 'success',
            'scopes': ['a_share_index'],
        },
        'samples': [
            {'instrument_id': f'00506{i}.SZ', 'reason': 'index_lifecycle_stale_no_quote'}
            for i in range(10)
        ],
    })

    assert '模式: historical_backfill' in summary
    assert '生命周期跳过=20' in summary
    assert 'segments=30' in summary
    assert '显式主数据刷新: success' in summary
    assert summary.count('index_lifecycle_stale_no_quote') <= 6
