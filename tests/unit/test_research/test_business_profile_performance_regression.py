import threading
import time

from research.business_profile_async_production import BusinessProfileWriteCoordinator


BASELINE_DATE = "2026-08-09"
ITEM_COUNT = 8
MODEL_SECONDS = 0.01
WRITE_SECONDS = 0.001


def _run_benchmark(*, legacy_writer_holds_model: bool):
    coordinator = BusinessProfileWriteCoordinator(inter_write_seconds=0)
    initialization_count = ITEM_COUNT if legacy_writer_holds_model else 1
    started = time.perf_counter()

    def one_item():
        if legacy_writer_holds_model:
            with coordinator.write_scope():
                time.sleep(MODEL_SECONDS)
                time.sleep(WRITE_SECONDS)
            return
        time.sleep(MODEL_SECONDS)
        with coordinator.write_scope():
            time.sleep(WRITE_SECONDS)

    if legacy_writer_holds_model:
        for _ in range(ITEM_COUNT):
            one_item()
    else:
        threads = [threading.Thread(target=one_item) for _ in range(ITEM_COUNT)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2)
            assert not thread.is_alive()

    elapsed = time.perf_counter() - started
    snapshot = coordinator.snapshot()
    llm_throughput = ITEM_COUNT / elapsed if elapsed else 0.0
    return {
        "baseline_date": BASELINE_DATE,
        "initialization_count": initialization_count,
        "transaction_count": snapshot["write_transactions"],
        "writer_lock_duty": snapshot["writer_lock_duty"],
        "elapsed_seconds": elapsed,
        "llm_requests": ITEM_COUNT,
        "llm_throughput_per_second": llm_throughput,
    }


def test_current_async_boundary_beats_2026_08_09_legacy_architecture_baseline():
    baseline = _run_benchmark(legacy_writer_holds_model=True)
    current = _run_benchmark(legacy_writer_holds_model=False)

    assert baseline["baseline_date"] == current["baseline_date"] == BASELINE_DATE
    assert baseline["initialization_count"] == ITEM_COUNT
    assert current["initialization_count"] == 1
    assert baseline["transaction_count"] == current["transaction_count"] == ITEM_COUNT
    assert current["writer_lock_duty"] < baseline["writer_lock_duty"]
    assert current["elapsed_seconds"] < baseline["elapsed_seconds"] * 0.75
    assert current["llm_throughput_per_second"] > baseline["llm_throughput_per_second"] * 2
