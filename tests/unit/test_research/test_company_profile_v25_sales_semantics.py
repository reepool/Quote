"""Full p39 lists: a rejected sale must not suppress other current sales."""

import json
import re

import pytest

from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_company_profile_v19_closure import _drive
from tests.unit.test_research.test_company_profile_v24_hydro_pharma import _fixture


@pytest.mark.parametrize("page_kind", ["pages", "independent_pages"])
@pytest.mark.parametrize(
    "list_prefix",
    [
        "尚未销售的主要产品有",
        "公司将销售的主要产品有",
        "公司未销售的主要产品有",
        "公司计划销售的主要产品有",
    ],
)
def test_non_current_sales_list_rejected_without_losing_other_sales(
    tmp_path, page_kind, list_prefix
):
    fixture = _fixture("600062.SH")
    page = next(p for p in fixture[page_kind] if p["page"] == 39)
    compact = re.sub(r"\s+", "", page["text"])
    assert compact.count("丙戊酸镁缓释片") == 1
    assert compact.count("主要产品有") == 1
    profiles = _drive(
        tmp_path,
        instrument="600062.SH",
        fixture=fixture,
        pages=[{**page, "text": page["text"].replace("主要产品有", list_prefix)}],
        repair_version="v25",
    )
    assert profiles[0] == profiles[1]
    checkpoint = json.loads(
        next(
            (tmp_path / "company_profile_common_core.v1/checkpoints").glob("*.json")
        ).read_text()
    )
    raw = {f["record_id"]: f for f in _iter_accepted_raw(checkpoint)}
    rejected = (
        "丙戊酸镁缓释片",
        "替尼泊苷注射液",
        "白消安注射液",
        "注射用牛肺表面活性剂",
        "小儿复方氨基酸注射液",
    )
    assert not any(
        f.get("action") == "sells"
        and any(name in f["object_name"] for name in rejected)
        for f in raw.values()
    )
    for profile in profiles:
        assert not any(
            any(name in (f["source_native_name"] or "") for name in rejected)
            for f in profile["accepted_facts"]
            if f["object_type"] == "Activity"
        )
        roles = profile["commodity_exposure"]["assessment"]["exposures"]
        assert not any(
            any(name in role["source_native_name"] for name in rejected)
            for role in roles
        )
        # 杜仲 has separate affirmed terminal-sales support on this same page.
        assert len(roles) == 14
        for name in (
            "复方利血平氨苯蝶啶片(0号)",
            "左炔诺孕酮片",
            "依诺肝素钠注射液",
            "复方杜仲健骨颗粒",
        ):
            assert any(name in role["source_native_name"] for role in roles)
        subsidiary = [
            raw[oid]
            for role in roles
            if role["source_native_name"] == "左炔诺孕酮片"
            for oid in role["source_record_ids"]
        ]
        assert subsidiary and all(f["source_actor"] == "华润紫竹" for f in subsidiary)
