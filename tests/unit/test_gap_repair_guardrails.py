from types import SimpleNamespace

from scheduler.tasks import _apply_hkex_gap_guard


def _gap(instrument_id: str, symbol: str, exchange: str, gap_days: int):
    return SimpleNamespace(
        instrument_id=instrument_id,
        symbol=symbol,
        exchange=exchange,
        gap_days=gap_days,
    )


def test_hkex_guard_skips_sparse_history_instrument():
    gaps = [
        _gap("01240.HK", "01240", "HKEX", 30),
        _gap("01240.HK", "01240", "HKEX", 25),
        _gap("01240.HK", "01240", "HKEX", 20),
        _gap("600000.SH", "600000", "SSE", 2),
    ]

    filtered, skipped_details, skipped_count = _apply_hkex_gap_guard(
        gaps,
        max_segments_per_instrument=2,
        max_missing_days_per_instrument=60,
    )

    assert len(filtered) == 1
    assert filtered[0].instrument_id == "600000.SH"
    assert skipped_count == 3
    assert skipped_details[0]["instrument_id"] == "01240.HK"


def test_hkex_guard_keeps_small_repairable_gap_sets():
    gaps = [
        _gap("00700.HK", "00700", "HKEX", 2),
        _gap("00700.HK", "00700", "HKEX", 3),
    ]

    filtered, skipped_details, skipped_count = _apply_hkex_gap_guard(
        gaps,
        max_segments_per_instrument=5,
        max_missing_days_per_instrument=10,
    )

    assert len(filtered) == 2
    assert skipped_details == []
    assert skipped_count == 0
