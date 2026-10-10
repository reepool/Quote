"""Full official-page closure for the frozen telecom/pharma business slice."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from pathlib import Path

import pytest

from research.company_profile.execution import default_processing_identity
from research.company_profile.m4_next_batch import (
    M4NextBatchPlan,
    M4NextBatchReport,
    save_m4_next_batch_plan,
)
from research.company_profile.reads import _iter_accepted_raw
from tests.unit.test_research.test_business_profile_exposure_components import _storage
from tests.unit.test_research.test_business_profile_pdf_artifacts import _pdf_bytes
from tests.unit.test_research.test_company_profile_operations import (
    _OfficialAssetAccess,
    _service,
)
from tests.unit.test_research.test_company_profile_runtime import (
    _RequestBoundOverviewProvider,
)
from tests.unit.test_research.test_company_profile_v19_closure import _answer, _drive

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures/company_profile_v41_telecom_pharma_frozen_pages.json"
)


def compact(s):
    return re.sub(r"\s+", "", s or "").replace("（", "(").replace("）", ")")


def fixture(i):
    return json.loads(FIXTURE.read_text())[i]


def page_numbers(r):
    return {
        p for e in r["evidence"] for p in [e["page"], *e.get("continuation_pages", [])]
    }


def delivered(root, i, pages, repair_version="v41"):
    q, e = _drive(
        root,
        instrument=i,
        fixture=fixture(i),
        pages=pages,
        repair_version=repair_version,
    )
    assert q == e
    cp = json.loads(
        next(
            (root / "company_profile_common_core.v1/checkpoints").glob("*.json")
        ).read_text()
    )
    return q, list(_iter_accepted_raw(cp))


@pytest.mark.parametrize("drift", [False, True, "between_reads"])
@pytest.mark.parametrize("repair_version", ["v41", "v42"])
def test_frozen_asset_enters_real_owner_without_an_existing_frontier(
    tmp_path, drift, repair_version
):
    storage = _storage(tmp_path)
    pdf_path = tmp_path / "annual.pdf"
    pdf_path.write_bytes(
        _pdf_bytes(
            [
                "Principal Business and business model with enough native text for official page binding."
            ]
        )
    )
    digest = hashlib.sha256(pdf_path.read_bytes()).hexdigest()

    class Assets(_OfficialAssetAccess):
        reads = 0

        def get_effective_asset(self, instrument_id, **kwargs):
            self.reads += 1
            if drift == "between_reads" and self.reads >= 3:
                self.digest = "changed"
            return {
                **super().get_effective_asset(instrument_id, **kwargs),
                "report_id": self.announcement_id,
            }

    access = Assets(pdf_path=pdf_path, digest=digest, announcement_id="annual-2025")
    service = _service(
        tmp_path,
        storage,
        provider=_RequestBoundOverviewProvider(),
        shared_asset_access=access,
        processing_identity={
            **default_processing_identity(),
            "revenue_sentence_repair": repair_version,
        },
    )
    asset = access.get_effective_asset("600000.SH")
    plan = M4NextBatchPlan(
        plan_id="bound-local",
        knowledge_cutoff="2026-08-30",
        token_budget=50000,
        reports=(
            M4NextBatchReport(
                instrument_id="600000.SH",
                asset_id=asset["asset_id"],
                report_id=asset["report_id"],
                report_period=asset["report_period"],
                document_version="drifted" if drift is True else digest,
                disclosure_form="service",
            ),
            M4NextBatchReport(
                instrument_id="000100.SZ",
                asset_id="unrequested",
                report_id="unrequested",
                report_period="2025-12-31",
                document_version="unrequested",
                disclosure_form="manufacturing",
            ),
        ),
    )
    save_m4_next_batch_plan(service.checkpoint_root, plan)
    with storage.get_connection() as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM business_profile_announcement_frontier"
            ).fetchone()[0]
            == 0
        )
    if drift == "between_reads":
        with pytest.raises(ValueError, match="binding changed"):
            asyncio.run(
                service.execute(
                    "run",
                    instrument_ids=("600000.SH",),
                    knowledge_cutoff=plan.knowledge_cutoff,
                    max_items=1,
                )
            )
        with storage.get_connection() as conn:
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM business_profile_announcement_frontier"
                ).fetchone()[0]
                == 0
            )
        return
    result = asyncio.run(
        service.execute(
            "run",
            instrument_ids=("600000.SH",),
            knowledge_cutoff=plan.knowledge_cutoff,
            max_items=1,
        )
    )
    if drift:
        assert result["enqueue"]["inserted"] == 0
        with storage.get_connection() as conn:
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM business_profile_announcement_frontier"
                ).fetchone()[0]
                == 0
            )
        return
    assert result["state"] == "completed"
    assert set(result["drain"]) == {"acquire", "parse", "semantic", "verify", "publish"}
    assert result["enqueue"]["inserted"] == 1
    assert access.exact_calls
    q = asyncio.run(service.execute("query", instrument_ids=("600000.SH",)))
    e = asyncio.run(
        service.execute(
            "export",
            instrument_ids=("600000.SH",),
            output_directory=tmp_path / "export",
        )
    )
    assert q["profiles"] == e["profiles"]
    assert q["profiles"][0] == json.loads(Path(e["files"][0]).read_text())


@pytest.mark.parametrize("i", ["600050.SH", "600079.SH"])
@pytest.mark.parametrize("kind", ["pages", "independent_pages"])
@pytest.mark.parametrize("repair_version", ["v41", "v42"])
def test_full_source_business_bodies_income_and_named_actions(
    tmp_path, i, kind, repair_version
):
    f = fixture(i)
    q, raw = delivered(tmp_path, i, f[kind], repair_version)
    for dim, rule in f["answers"].items():
        text = compact(_answer(q, dim))
        missing = [
            g for g in rule["required_groups"] if not any(compact(t) in text for t in g)
        ]
        assert not missing, (dim, missing)
    for row in f["source_rows"]:
        dimension = {"recognition_timing": "revenue_timing"}.get(
            row["dimension"], row["dimension"]
        )
        matches = [
            r
            for r in raw
            if r["object_type"] == "Segment"
            and r["dimension"] == dimension
            and compact(r["source_native"]["name"])
            in set(map(compact, row["native_aliases"]))
            and compact(r["source_native"]["value"]) == compact(row["value"])
            and r["source_native"]["unit"] == row["unit"]
            and row["page"] in page_numbers(r)
            and (
                not row.get("counterparty")
                or compact(row["counterparty"])
                in compact(r["source_native"].get("qualifier"))
            )
        ]
        assert matches, (
            row["page"],
            row["name"],
            row["value"],
            row.get("counterparty"),
        )
        if row["source_actor"] == "公司及合并子公司":
            assert matches[0]["subject_scope"] == "consolidated_group", (
                row["page"],
                row["name"],
                matches[0]["subject_scope"],
            )
        assert any(
            r["object_type"] == "Measurement"
            and r["source_native"] == matches[0]["source_native"]
            for r in raw
        ), (row["page"], row["name"], matches[0]["source_native"])
    for role in f["native_roles"]:
        candidates = [
            r
            for r in raw
            if r["object_type"] == "Activity"
            and compact(r["source_native"]["name"])
            in set(map(compact, role["native_aliases"]))
            and r["action"] in role["source_actions"]
            and role["page"] in page_numbers(r)
            and (
                r["subject_scope"] == "consolidated_group"
                if role["source_actor"] == "公司及合并子公司"
                else r["source_actor"] == role["source_actor"]
            )
        ]
        assert candidates, (role["name"], role["source_actor"], role["page"])
        projected_role = {"raw_material_procurement": "raw_material_input"}.get(
            role["role"], role["role"]
        )
        assert any(
            r["record_id"] in x["source_record_ids"] and x["role"] == projected_role
            for r in candidates
            for x in q["commodity_exposure"]["assessment"]["exposures"]
        ), (role["name"], role["source_actor"], role["role"])
        if role["role"] == "raw_material_procurement":
            assert all(r["action"] == "purchases" for r in candidates)


@pytest.mark.parametrize("i", ["600050.SH", "600079.SH"])
@pytest.mark.parametrize("repair_version", ["v41", "v42"])
def test_original_nine_negative_conditions_and_true_rows_coexist(
    tmp_path, i, repair_version
):
    f = fixture(i)
    q, raw = delivered(tmp_path, i, f["pages"], repair_version)
    if i == "600050.SH":
        assert not any(
            r["object_type"] == "Activity"
            and "6G" in r["source_native"]["name"]
            and r.get("action") == "sells"
            for r in raw
        )
        assert not any(
            r["object_type"] in {"Segment", "Measurement"}
            and r["source_native"]["value"]
            in {"77,766,887,871", "11,224,366,415", "64,320,274,389", "3,872,932"}
            for r in raw
        )
    else:
        for party in [
            "乐福思（武汉）药业有限公司",
            "北京乐福思卫生用品有限公司",
            "武汉睿成创业投资管理有限公司",
        ]:
            assert not any(
                r["object_type"] in {"Segment", "Measurement"}
                and compact(party) in compact(r["source_native"].get("qualifier"))
                for r in raw
            )
        assert not any(
            r["object_type"] in {"Segment", "Measurement"}
            and (
                250 in page_numbers(r)
                or r["source_native"]["value"] == "2,484,021,317.47"
            )
            for r in raw
        )
        assert not any(
            r["object_type"] == "Segment"
            and r["dimension"] == "sales_mode"
            and any(e["page"] == 27 for e in r["evidence"])
            for r in raw
        )
        assert not any(
            r["object_type"] == "Activity"
            and r.get("action") == "sells"
            and r["source_native"]["name"] in {"甲泼尼龙", "屈螺酮", "睾酮"}
            for r in raw
        )
        assert not any("入营业利润" in (r.get("source_actor") or "") for r in raw)
        assert any(r["source_native"]["value"] == "-2,497,699.12" for r in raw)
    assert q["accepted_facts"]


@pytest.mark.parametrize("modifier", ["不销售", "拟销售"])
@pytest.mark.parametrize("repair_version", ["v41", "v42"])
def test_product_action_modifier_governs_its_own_full_source_branch(
    tmp_path, modifier, repair_version
):
    i = "600050.SH"
    f = fixture(i)
    pages = [
        {
            **p,
            "text": p["text"].replace(
                "重点产品销售同比翻", "重点产品" + modifier + "同比翻"
            ),
        }
        for p in f["pages"]
    ]
    q, raw = delivered(tmp_path, i, pages, repair_version)
    assert not any(
        r["object_type"] == "Activity"
        and r["source_native"]["name"] in {"视频云", "云桌面"}
        and r["action"] == "sells"
        for r in raw
    )
    assert any(
        r["object_type"] == "Activity"
        and r["source_native"]["name"] == "手机"
        and r["action"] == "sells"
        for r in raw
    )
    assert not any(
        x["source_native_name"] in {"视频云", "云桌面"} and x["role"] == "product_sales"
        for x in q["commodity_exposure"]["assessment"]["exposures"]
    )


@pytest.mark.parametrize("modifier", ["不销售", "拟销售"])
@pytest.mark.parametrize("repair_version", ["v41", "v42"])
def test_subsidiary_product_list_requires_its_own_current_sale(
    tmp_path, modifier, repair_version
):
    i = "600079.SH"
    f = fixture(i)
    pages = [
        {
            **p,
            "text": p["text"].replace(
                "研发、生产与销售的国家高新技术企业",
                "研发、生产且" + modifier + "的国家高新技术企业",
            ),
        }
        for p in f["pages"]
    ]
    q, raw = delivered(tmp_path, i, pages, repair_version)
    assert not any(
        r["object_type"] == "Activity"
        and r["source_actor"] == "湖北葛店人福药业有限责任公司"
        and r["source_native"]["name"] == "米索前列醇片"
        and r["action"] == "sells"
        for r in raw
    )
    assert any(
        r["object_type"] == "Activity"
        and r["source_actor"] == "新疆维吾尔药业有限责任公司"
        and r["source_native"]["name"] == "祖卡木颗粒"
        and r["action"] == "sells"
        for r in raw
    )
    assert not any(
        x["source_native_name"] == "米索前列醇片" and x["role"] == "product_sales"
        for x in q["commodity_exposure"]["assessment"]["exposures"]
    )


@pytest.mark.parametrize(
    "mutation",
    [
        None,
        "wrong_name",
        "wrong_qualifier",
        "wrong_header",
        "default_basis",
        "group_revenue",
    ],
)
def test_named_investee_revenue_does_not_become_group_income(mutation):
    from research.company_profile.acceptance_policy import (
        explicit_group_wording_subject_is_unsupported,
        usage_policy,
    )
    from research.company_profile.models import Measurement

    payload = {
        "record_id": "m",
        "field_id": "operating_revenue",
        "chapter_task": "extract_segment_financials",
        "report": {
            "instrument_id": "600050.SH",
            "report_id": "r",
            "document_version": "v",
            "report_period": "2024-12-31",
            "published_at": "2025-03-18",
        },
        "subject_scope": "business_segment",
        "subject_basis": "direct_source_wording",
        "reported_period": "2024-12-31",
        "period_type": "duration",
        "assertion_class": "reported_fact",
        "evidence": [
            {
                "evidence_id": "e",
                "report": {
                    "instrument_id": "600050.SH",
                    "report_id": "r",
                    "document_version": "v",
                    "report_period": "2024-12-31",
                    "published_at": "2025-03-18",
                },
                "page": 185,
                "section_title": "重要合营企业",
                "anchor": {
                    "anchor_type": "text",
                    "bounded_quote": "本集团披露重要合营企业 招联金融 本期营业收入 100 元",
                },
            }
        ],
        "source_native": {
            "name": "招联金融",
            "value": "100",
            "unit": "元",
            "header": "重要合营企业/本期营业收入",
            "qualifier": "合营企业：招联金融",
        },
        "metric_type": "operating_revenue",
        "logical_slot": "revenue",
        "measured_object": "招联金融",
    }
    if mutation == "wrong_name":
        payload["measured_object"] = "其他企业"
        payload["source_native"]["qualifier"] = "合营企业：其他企业"
    elif mutation == "wrong_qualifier":
        payload["source_native"]["qualifier"] = "合营企业：其他企业"
    elif mutation == "wrong_header":
        payload["source_native"]["header"] = "本集团营业收入"
    elif mutation == "default_basis":
        payload["subject_basis"] = "report_default_group_scope"
    elif mutation == "group_revenue":
        payload["evidence"][0]["anchor"]["bounded_quote"] = "本集团营业收入 100 元"
    record = Measurement.model_validate_json(json.dumps(payload))
    assert explicit_group_wording_subject_is_unsupported(record) == (
        mutation is not None
    )
    assert usage_policy(record)["consolidated_aggregation"] is False


@pytest.mark.parametrize(
    ("prefix", "expected_scope"),
    [("母公司财务报表", "issuer"), ("本公司单体口径", "unclear")],
)
@pytest.mark.parametrize("repair_version", ["v41", "v42"])
def test_operating_income_keeps_its_own_parent_scope(
    tmp_path, prefix, expected_scope, repair_version
):
    i = "600050.SH"
    f = fixture(i)
    pages = [
        {
            **p,
            "text": p["text"].replace("联通云收入", prefix + "：联通云收入")
            if p["page"] == 10
            else p["text"],
        }
        for p in f["pages"]
    ]
    _, raw = delivered(tmp_path, i, pages, repair_version)
    pair = [
        r
        for r in raw
        if r["object_type"] in {"Segment", "Measurement"}
        and r["source_native"]["name"] == "联通云"
        and compact(r["source_native"]["value"]) == "686"
        and 10 in page_numbers(r)
    ]
    assert {r["object_type"] for r in pair} == {"Segment", "Measurement"}
    assert all(r["subject_scope"] == expected_scope for r in pair)
