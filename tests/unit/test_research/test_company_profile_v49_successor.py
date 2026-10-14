"""Successor identity preserves the verified official purchase input path."""

import pytest

from research.company_profile import core_evidence_selection as selection
from research.company_profile.execution import default_processing_identity
from tests.unit.test_research import test_company_profile_v48_metal_equipment as prior


def test_v49_keeps_default_and_five_cumulative_repairs():
    default = default_processing_identity()
    identity = {**default, "revenue_sentence_repair": "v49"}
    assert default.get("revenue_sentence_repair") != "v49"
    for name in (
        "revenue_sentence_repair_requested",
        "named_role_repair_requested",
        "service_operating_energy_requested",
        "core_answer_repair_requested",
        "source_delivery_repair_requested",
    ):
        assert getattr(selection, name)(identity)
        assert not getattr(selection, name)(default)
    assert default_processing_identity() == default


@pytest.mark.parametrize("instrument", ["600058.SH", "600089.SH"])
def test_v49_official_page_source_full_contract(tmp_path, monkeypatch, instrument):
    drive = prior._drive

    def successor_drive(*args, **kwargs):
        kwargs["repair_version"] = "v49"
        return drive(*args, **kwargs)

    monkeypatch.setattr(prior, "_drive", successor_drive)
    prior.test_official_owner_recovers_purchase_layout_before_full_delivery(
        tmp_path, monkeypatch, instrument
    )
