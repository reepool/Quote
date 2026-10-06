"""The formal repair identity must activate all five cumulative paths."""

import pytest

from research.company_profile.core_evidence_selection import (
    core_answer_repair_requested,
    named_role_repair_requested,
    revenue_sentence_repair_requested,
    service_operating_energy_requested,
    source_delivery_repair_requested,
)
from research.company_profile.execution import default_processing_identity


@pytest.mark.parametrize("version", ["v20", "v21", "v22", "v23", "v24"])
def test_formal_identity_activates_all_cumulative_repairs_without_changing_default(
    version,
):
    default = default_processing_identity()
    formal = {**default, "revenue_sentence_repair": version}
    for requested in (
        revenue_sentence_repair_requested,
        named_role_repair_requested,
        service_operating_energy_requested,
        core_answer_repair_requested,
        source_delivery_repair_requested,
    ):
        assert requested(formal)
        assert not requested(default)
