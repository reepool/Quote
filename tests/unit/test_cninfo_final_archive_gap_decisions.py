from collections import Counter

from scripts.dev_validation import (
    apply_cninfo_final_archive_gap_decisions as batch,
)


def test_final_archive_decision_manifest_is_frozen_and_partitioned():
    event_keys = {item[1] for item in batch.DECISION_SPECS}
    manifest = {
        f"{event_key}|{row_hash}|{terminal_reason}"
        for _, event_key, row_hash, terminal_reason in batch.DECISION_SPECS
    }
    reason_counts = Counter(item[3] for item in batch.DECISION_SPECS)

    assert len(batch.DECISION_SPECS) == 10
    assert len(event_keys) == 10
    assert reason_counts == {
        "archive_gap_ignored": 8,
        "scope_mismatch": 2,
    }
    assert (
        batch.archive_batch._hash_lines(event_keys)
        == batch.EXPECTED_EVENT_KEYS_HASH
    )
    assert (
        batch.archive_batch._hash_lines(manifest)
        == batch.EXPECTED_MANIFEST_HASH
    )
