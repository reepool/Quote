import json

import pytest

from data_sources.a_share_factor_activation import (
    CANONICAL_DATASET,
    COMPOSITE_DATASET,
    FactorActivationError,
    load_factor_activation,
    write_factor_activation,
)


def test_factor_activation_round_trip_and_rollback(tmp_path):
    path = tmp_path / "activation.json"

    canonical = write_factor_activation(
        path,
        read_dataset=CANONICAL_DATASET,
        canonical_series_version="a_share_cninfo_primary_v1",
        reason="unit_promotion",
    )

    assert load_factor_activation(path) == canonical
    assert canonical.read_dataset == CANONICAL_DATASET

    composite = write_factor_activation(
        path,
        read_dataset=COMPOSITE_DATASET,
        canonical_series_version=None,
        reason="unit_rollback",
    )

    assert load_factor_activation(path) == composite
    assert composite.canonical_series_version is None
    assert not list(tmp_path.glob(".*.tmp"))


@pytest.mark.parametrize(
    "payload",
    [
        {
            "schema_version": "a_share_factor_activation_v1",
            "read_dataset": "canonical",
            "canonical_series_version": None,
            "updated_at": "2026-08-01T10:00:00+08:00",
            "reason": "missing_version",
        },
        {
            "schema_version": "a_share_factor_activation_v1",
            "read_dataset": "unknown",
            "canonical_series_version": None,
            "updated_at": "2026-08-01T10:00:00+08:00",
            "reason": "unknown_dataset",
        },
    ],
)
def test_factor_activation_rejects_invalid_payload(tmp_path, payload):
    path = tmp_path / "activation.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(FactorActivationError):
        load_factor_activation(path)


def test_factor_activation_rejects_duplicate_normalized_keys(tmp_path):
    path = tmp_path / "activation.json"
    path.write_text(
        """
        {
          "schema_version": "a_share_factor_activation_v1",
          "read_dataset": "canonical",
          "READ_DATASET": "baostock_sina_composite",
          "canonical_series_version": "v1",
          "updated_at": "2026-08-01T10:00:00+08:00",
          "reason": "duplicate"
        }
        """,
        encoding="utf-8",
    )

    with pytest.raises(FactorActivationError, match="duplicate normalized"):
        load_factor_activation(path)


def test_factor_activation_rejects_invalid_utf8_as_activation_error(tmp_path):
    path = tmp_path / "activation.json"
    path.write_bytes(b"\xff\xfe\x00")

    with pytest.raises(FactorActivationError, match="cannot load factor activation"):
        load_factor_activation(path)
