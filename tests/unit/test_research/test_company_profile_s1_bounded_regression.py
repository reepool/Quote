"""Twelve historical companies, frozen core checks under one v61 snapshot."""

import json
import re
from pathlib import Path

import pytest

from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _drive

CONTRACT = Path(
    "openspec/changes/prepare-company-profile-s1-s2-bounded-acceptance/s1-core-regression-contract.json"
)
CASES = json.loads(CONTRACT.read_text())["cases"]


def compact(value):
    return re.sub(r"\s+", "", value or "")


@pytest.mark.parametrize("case", CASES, ids=lambda c: c["instrument_id"])
def test_historical_core_accept_query_export(tmp_path, case):
    instrument = case["instrument_id"]
    if instrument == "600000.SH":
        from tests.unit.test_research.test_company_profile_owned_page_facts import (
            SPDB_INCOME,
            _spdb_pages,
        )
        from tests.unit.test_research.test_company_profile_runtime import _report

        source = _report(instrument_id=instrument).model_dump(mode="json")
        ps = [*_spdb_pages(), {"page": 71, "text": SPDB_INCOME, "readable": True}]
    else:
        b = json.loads(Path(case["fixture"]).read_text())
        source = b.get("official_inputs", b)[instrument]
        ps = source["pages"]
        if instrument == "600097.SH":
            ps = sorted(
                [
                    *ps,
                    json.loads(
                        Path(
                            "tests/fixtures/company_profile_v53_official_segment_conflict.json"
                        ).read_text()
                    ),
                ],
                key=lambda p: p["page"],
            )
        if instrument == "600082.SH":
            cached = json.loads(
                Path(
                    "tests/fixtures/company_profile_v52_cached_mda_page.json"
                ).read_text()
            )
            ps = [cached if p["page"] == 15 else p for p in ps]
    q, e = _drive(
        tmp_path,
        instrument=instrument,
        fixture=source.get("report", source),
        pages=ps,
        repair_version="v61",
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
    if instrument == "600000.SH":
        assert any("商业信贷" in d["excerpt"] for d in q["dimensions"])
        for name, value in [("营业收入", "173,964"), ("利息净收入", "120,483")]:
            assert any(
                r["source_native"]["name"] == name
                and r["source_native"].get("value") == value
                and r["source_native"].get("unit") == "百万元"
                for r in raw
            )
        assert not any(
            r["source_native"].get("value") in {"325,269", "186,233"}
            for r in raw
            if r["object_type"] in {"Measurement", "Segment"}
        )
        return
    assert all(d["answered"] and d["excerpt"] for d in q["dimensions"])
    for d in q["dimensions"]:
        for group in case["answer_rules"].get(d["dimension_id"], []):
            assert any(compact(t) in compact(d["excerpt"]) for t in group), (
                instrument,
                group,
            )
    if instrument in {"600094.SH", "600103.SH"}:
        from tests.unit.test_research.test_company_profile_v55_property_paper import (
            match,
            negative_violations,
            purchase_category_matches,
            rolematch,
        )

        assert all(purchase_category_matches(r) for r in raw)
    elif instrument in {"600100.SH", "600107.SH"}:
        from tests.unit.test_research.test_company_profile_v59_technology_apparel import (
            match,
            rolematch,
        )
    elif instrument in {"600088.SH", "600099.SH"}:
        from tests.unit.test_research.test_company_profile_v54_media_machinery import (
            action_match as rolematch,
        )
        from tests.unit.test_research.test_company_profile_v54_media_machinery import (
            match,
            negative_violations,
        )
    elif instrument in {"600082.SH", "600097.SH"}:
        from tests.unit.test_research.test_company_profile_v51_park_fishery import (
            match,
            negative_violations,
        )
        from tests.unit.test_research.test_company_profile_v51_park_fishery import (
            role_match as rolematch,
        )

        if instrument == "600097.SH":
            from tests.unit.test_research.test_company_profile_v53_source_conflict import (
                assert_conflict_boundary,
            )

            assert_conflict_boundary(raw, q)
    elif instrument == "600054.SH":
        from tests.unit.test_research.test_company_profile_v46_tourism_wine import (
            check_bodies,
        )
        from tests.unit.test_research.test_company_profile_v46_tourism_wine import (
            native_match as match,
        )

        check_bodies(source, q, instrument)
    else:
        # Assertions below use independent source-specific risk checks, not a second execution.
        if instrument == "600098.SH":
            for name in ["广东红海湾发电有限公司", "广东珠海金湾液化天然气有限公司"]:
                rs = [
                    r
                    for r in raw
                    if r["object_type"] == "Segment"
                    and r["source_native"]["name"] == name
                ]
                assert rs and all(
                    "联营企业" in r["source_native"]["header"]
                    and "原栏目冲突" in r["source_native"]["qualifier"]
                    for r in rs
                )
            assert not any(
                r.get("action") == "sells"
                and r["source_native"]["name"] in {"油品仓储", "燃气管网建设"}
                for r in raw
            )
        else:
            rs = [
                r
                for r in raw
                if r.get("action") == "purchases"
                and r["source_native"]["name"] == "研发劳务"
            ]
            assert len(rs) == 5 and all(
                r["source_native"]["header"] == "本期采购服务" for r in rs
            )
            assert not any(
                r["source_native"].get("value") == "250,550,820.65"
                for r in raw
                if r["object_type"] in {"Segment", "Measurement"}
            )
        revenue = next(
            d["excerpt"]
            for d in q["dimensions"]
            if d["dimension_id"] == "revenue_model"
        )
        assert (
            "实际利率" in revenue if instrument == "600098.SH" else "代理人" in revenue
        )
        return
    for row in case["revenue"]:
        assert any(r["object_type"] == "Segment" and match(r, row) for r in raw), row
    if instrument == "600054.SH":
        return
    for role in case["actions"]:
        assert any(
            r["object_type"] == "Activity" and rolematch(r, role) for r in raw
        ), role
    for n in case["negative"]:
        if instrument in {"600100.SH", "600107.SH"}:
            financial = [
                r for r in raw if r["object_type"] in {"Segment", "Measurement"}
            ]
            if instrument == "600107.SH":
                assert not any(r["source_native"]["name"] == "医疗" for r in financial)
            else:
                assert not any(
                    n["page"] in {e.get("page") for e in r["evidence"]}
                    and "成本" in (r["source_native"].get("header") or "")
                    for r in financial
                )
                assert not any(
                    r["source_native"].get("unit") in {"台", "套", "件", "吨"}
                    for r in financial
                    if n["page"] == 24
                )
        else:
            assert not negative_violations(n, raw, q["dimensions"])
