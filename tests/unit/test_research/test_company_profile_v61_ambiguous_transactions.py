"""An income row in a mixed goods/services table does not resolve its action."""

import json
from copy import deepcopy
from pathlib import Path

from research.company_profile import core_evidence_selection as selection
from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _drive
from tests.unit.test_research.test_company_profile_v60_utility_agriculture import (
    FIXTURE,
    co,
    match,
    pages,
)

AMBIGUOUS_ROWS = (
    ("其他", "甘肃黄羊河农工商（集团）有限责任公司", "216096.00"),
    ("工程施工/滴管材料", "甘肃省农垦建筑工程有限责任公司", "30213429.52"),
    ("工程施工/滴管材料", "农垦集团及其他所属公司", "22649188.61"),
    ("其他", "农垦集团及其他所属公司", "13904.59"),
)


def ambiguous_checks(records, profile, name, party, value):
    """Require positive retained income/evidence as well as no invented service."""
    incomes = [
        r
        for r in records
        if r["object_type"] in {"Segment", "Measurement"}
        and 120 in pages(r)
        and r["source_native"]["name"] == name
        and co(party) in co(r["source_native"].get("qualifier"))
        and str(r["source_native"].get("value")).replace(",", "") == value
        and r["source_native"].get("unit") == "元"
    ]
    retained = {r["object_type"] for r in incomes} == {"Segment", "Measurement"}
    retained = retained and all(
        any(
            e["page"] == 120 and name in co(e["anchor"]["bounded_quote"])
            for e in r["evidence"]
        )
        for r in incomes
    )
    actions = [
        r
        for r in records
        if r["object_type"] == "Activity"
        and 120 in pages(r)
        and r["source_native"]["name"] == name
        and co(party) in co(r["source_native"].get("qualifier"))
    ]
    forbidden = [
        r
        for r in actions
        if r.get("action") == "provides_service" or r.get("source_verb") == "提供"
    ]
    support = {x for d in profile["dimensions"] for x in d["supporting_record_ids"]}
    return retained, not forbidden and not any(
        r["record_id"] in support for r in actions
    )


def test_complete_p120_preserves_income_without_inventing_service(tmp_path):
    b = json.loads(FIXTURE.read_text())
    src = b["official_inputs"]["600108.SH"]
    ps = src["pages"]  # Full report context, including the complete p120 layout.
    q, e = _drive(
        tmp_path,
        instrument="600108.SH",
        fixture=src["report"],
        pages=ps,
        repair_version="v61",
    )
    assert q == e
    records = list(
        _iter_accepted_raw(
            json.loads(
                next(
                    (tmp_path / "company_profile_common_core.v1/checkpoints").glob(
                        "*.json"
                    )
                ).read_text()
            )
        )
    )
    for row in AMBIGUOUS_ROWS:
        assert ambiguous_checks(records, q, *row) == (True, True), row
        # An incorrect definite service must fail independently of positive contracts.
        bad = deepcopy(records)
        old = next(
            r
            for r in json.loads(
                (
                    Path(
                        "openspec/changes/deliver-company-profile-m4-v59-unseen-small-batch"
                    )
                    / "v60-formal-delivery-review-inputs/raw_600108.SH.json"
                ).read_text()
            )
            if r["object_type"] == "Activity"
            and r["action"] == "provides_service"
            and r["source_native"]["name"] == row[0]
            and co(row[1]) in co(r["source_native"].get("qualifier"))
        )
        bad.append(old)
        assert ambiguous_checks(bad, q, *row)[1] is False
    for name, action in [
        ("工程材料", "sells"),
        ("物业服务", "provides_service"),
        ("建筑工程", "purchases"),
        ("包装材料", "purchases"),
    ]:
        assert any(
            r["object_type"] == "Activity"
            and r["source_native"]["name"] == name
            and r["action"] == action
            for r in records
        ), (name, action)
    for v in b["contract"]["revenue_conditions"]["600108.SH"]:
        if v["page"] == 120:
            assert all(
                any(r["object_type"] == typ and match(r, v) for r in records)
                for typ in ["Segment", "Measurement"]
            ), v


def test_v61_retains_all_five_repairs_and_default_identity():
    from research.company_profile.execution import default_processing_identity

    before = default_processing_identity()
    identity = {**before, "revenue_sentence_repair": "v61"}
    assert all(
        getattr(selection, n)(identity)
        for n in [
            "revenue_sentence_repair_requested",
            "named_role_repair_requested",
            "service_operating_energy_requested",
            "core_answer_repair_requested",
            "source_delivery_repair_requested",
        ]
    )
    assert default_processing_identity() == before
