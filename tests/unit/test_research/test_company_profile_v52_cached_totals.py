"""The original PDF parser can place a total label at the previous row's end."""

import json
from pathlib import Path

from research.company_profile import core_evidence_selection as selection
from research.company_profile.execution import default_processing_identity
from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _drive
from tests.unit.test_research.test_company_profile_v51_park_fishery import (
    FIXTURE,
    match,
    negative_violations,
    role_match,
)


def test_cached_total_label_keeps_its_own_income_row():
    source = (
        "商品销售\n业务\n424,569.35 - - -49.75 - 合计\n"
        "449,240,562. 34\n355,491,553.9\n6\n"
        "20.87 33.06 105.81 减少 27.97\n个百分点"
    )
    joined = selection._join_native_income_cells(source)
    assert "合计\n" in joined or "合计 " in joined
    assert "- 合计" not in joined
    assert "449,240,562.34" in joined
    soft = selection._join_pdf_soft_breaks(source)
    assert "- 合计" not in soft


def test_actual_cached_mda_page_keeps_both_native_totals(tmp_path):
    f = json.loads(FIXTURE.read_text())["600082.SH"]
    cached = json.loads(
        (
            Path(__file__).resolve().parents[2]
            / "fixtures"
            / "company_profile_v52_cached_mda_page.json"
        ).read_text()
    )
    pages = [cached if p["page"] == 15 else p for p in f["pages"]]
    q, e = _drive(
        tmp_path, instrument="600082.SH", fixture=f, pages=pages, repair_version="v52"
    )
    assert q == e
    raw = list(
        _iter_accepted_raw(
            json.loads(
                next(
                    tmp_path.glob("company_profile_common_core.v1/checkpoints/*.json")
                ).read_text()
            )
        )
    )
    for kind in ("Segment", "Measurement"):
        assert all(
            any(r["object_type"] == kind and match(r, row) for r in raw)
            for row in f["source_rows"]
        )
    assert all(any(role_match(r, role) for r in raw) for role in f["native_roles"])
    assert all(
        not negative_violations(n, raw, q["dimensions"])
        for n in f["negative_conditions"]
    )


def test_v52_inherits_every_cumulative_switch_without_changing_default():
    default = default_processing_identity()
    successor = {**default, "revenue_sentence_repair": "v52"}
    for name in (
        "revenue_sentence_repair_requested",
        "named_role_repair_requested",
        "service_operating_energy_requested",
        "core_answer_repair_requested",
        "source_delivery_repair_requested",
    ):
        assert getattr(selection, name)(successor)
        assert not getattr(selection, name)(default)
    assert default_processing_identity() == default
