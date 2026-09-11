from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest
from run_external_validation import (
    CONTRACT_PATH,
    _load_contract,
    _validate_contract_payload,
    _validate_output_absent,
    _validate_predecessor,
    validate_inputs,
)


def test_exact_contract_admits_two_frozen_scopes_provider_free() -> None:
    contract = _load_contract(CONTRACT_PATH)
    _, selected, _ = validate_inputs(contract, require_output_absent=False)

    assert [(item[0]["sample_id"], item[0]["scope_id"]) for item in selected] == [
        ("manufacturing-materials-shadow-000055-2025", "segment_financials-01"),
        ("manufacturing-materials-shadow-000408-2025", "segment_financials-02"),
    ]
    assert all(item[2].production_authorization == "not_authorized" for item in selected)


def test_predecessor_receipt_is_bound_provider_free() -> None:
    contract = _load_contract(CONTRACT_PATH)
    _validate_predecessor(contract)


@pytest.mark.parametrize("field", ["case", "budget", "authorization", "target"])
def test_contract_drift_fails_before_provider(field: str) -> None:
    contract = deepcopy(_load_contract(CONTRACT_PATH))
    if field == "case":
        contract["cases"][0]["scope_id"] = "segment_financials-02"
    elif field == "budget":
        contract["provider"]["max_provider_calls"] = 13
    elif field == "authorization":
        contract["production_authorization"] = "authorized"
    else:
        contract["transfer_authorization"]["target"] = "other.example"

    with pytest.raises(ValueError):
        _validate_contract_payload(contract)


def test_implementation_hash_drift_fails_provider_free() -> None:
    contract = deepcopy(_load_contract(CONTRACT_PATH))
    contract["implementation_hashes"]["research/company_profile/stage5_provider.py"] = (
        "f" * 64
    )

    with pytest.raises(ValueError, match="implementation hash mismatch"):
        validate_inputs(contract, require_output_absent=False)


def test_existing_output_is_rejected(tmp_path: Path) -> None:
    output = tmp_path / "result.json"
    output.write_text("{}", encoding="utf-8")

    with pytest.raises(FileExistsError, match="already exists"):
        _validate_output_absent(output)
