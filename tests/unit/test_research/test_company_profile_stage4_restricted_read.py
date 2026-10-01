from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

from research.company_profile.models import PRODUCTION_AUTHORIZATION
from research.company_profile.reads import CompanyProfileReadService
from research.company_profile.stage4_restricted_read import (
    FROZEN_CHAPTERS,
    FROZEN_REPORTS,
    FrozenChapterArtifacts,
    default_repo_root,
    project_stage4_restricted_views,
)

_COMPANIES = ("300750.SZ", "603659.SH", "920015.BJ", "302132.SZ")
_FACT_COUNTS = {
    "extract_material_inputs": {
        "300750.SZ": 7,
        "603659.SH": 9,
        "920015.BJ": 7,
        "302132.SZ": 0,
    },
    "extract_operating_quantities": {
        "300750.SZ": 8,
        "603659.SH": 23,
        "920015.BJ": 7,
        "302132.SZ": 0,
    },
    "extract_segment_financials": {
        "300750.SZ": 55,
        "603659.SH": 39,
        "920015.BJ": 62,
        "302132.SZ": 57,
    },
    "extract_counterparties_and_concentration": {
        "300750.SZ": 17,
        "603659.SH": 8,
        "920015.BJ": 14,
        "302132.SZ": 6,
    },
}


def _artifact_hashes(root: Path | None = None) -> dict[str, str]:
    base = root or default_repo_root()
    hashes: dict[str, str] = {}
    for chapter in FROZEN_CHAPTERS:
        replay = base / chapter.replay_dir
        paths = {
            "enqueue": replay / "enqueue.json",
            "run": replay / "run.json",
            "result": replay / chapter.result_name,
            "source_review": replay / "source_review.json",
        }
        for name, path in paths.items():
            hashes[f"{chapter.chapter_task}:{name}"] = hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
    return hashes


def _chapter(view: dict, chapter_task: str) -> dict:
    return next(
        item for item in view["chapters"] if item["chapter_task"] == chapter_task
    )


def test_query_delivers_four_companies_without_runtime_records(tmp_path):
    result = CompanyProfileReadService(tmp_path).query(list(_COMPANIES))
    views = {item["instrument_id"]: item for item in result["stage4_restricted_views"]}

    assert result["state"] == "not_found"
    assert result["profiles"] == []
    assert result["delivered"] == 0
    assert result["missing_instrument_ids"] == list(_COMPANIES)
    assert result["production_authorization"] == PRODUCTION_AUTHORIZATION
    assert set(views) == set(_COMPANIES)
    for instrument_id, view in views.items():
        identity = FROZEN_REPORTS[instrument_id]
        assert view["core_profile_complete"] is False
        assert view["production_authorization"] == "not_authorized"
        assert view["scale_quality_claim_allowed"] is False
        assert view["report"]["report_period"] == "2025-12-31"
        assert view["report"]["report_id"] == identity["report_id"]
        assert view["report"]["document_version"] == identity["document_version"]
        assert view["report"]["content_hash"] == identity["content_hash"]
        assert {item["chapter_task"] for item in view["gaps"]} == {
            "extract_business_overview",
            "extract_business_regime",
        }
        for chapter in view["chapters"]:
            assert chapter["delivered"] is True
            assert chapter["reason"] is None
            assert (
                len(chapter["facts"])
                == _FACT_COUNTS[chapter["chapter_task"]][instrument_id]
            )


def test_chengfei_material_input_stays_legal_empty():
    views = project_stage4_restricted_views(("302132.SZ",))
    chapter = _chapter(views[0], "extract_material_inputs")

    assert chapter["facts"] == []
    assert len(chapter["coverage"]) == 5
    assert {item["outcome"] for item in chapter["coverage"]} == {"legal_empty"}


def test_query_filters_one_company_and_keeps_native_fields(tmp_path):
    result = CompanyProfileReadService(tmp_path).query(("603659.SH",))
    views = result["stage4_restricted_views"]
    quantities = _chapter(views[0], "extract_operating_quantities")
    fact = quantities["facts"][0]

    assert result["profiles"] == []
    assert [item["instrument_id"] for item in views] == ["603659.SH"]
    assert len(quantities["facts"]) == 23
    assert fact["report_period"] == "2025-12-31"
    assert fact["unit"]
    assert fact["page"]
    assert fact["bounded_quote"]
    assert all(
        item["report_id"] == FROZEN_REPORTS["603659.SH"]["report_id"]
        for item in quantities["facts"]
    )


def test_other_period_rows_stay_outside_the_frozen_view(tmp_path):
    identity = FROZEN_REPORTS["300750.SZ"]
    payload = {
        "plan_version": FROZEN_CHAPTERS[1].plan_version,
        "chapter_task": "extract_operating_quantities",
        "reports": [
            {
                "sample_id": "year-2025",
                "instrument_id": "300750.SZ",
                "report_id": identity["report_id"],
                "document_version": identity["document_version"],
                "content_hash": identity["content_hash"],
            },
            {
                "sample_id": "year-2024",
                "instrument_id": "300750.SZ",
                "report_id": "asset-old",
                "document_version": "ver-old",
                "content_hash": "old-hash",
            },
        ],
        "facts": [
            {
                "sample_id": "year-2025",
                "record_id": "current",
                "report_id": identity["report_id"],
                "document_version": identity["document_version"],
                "report_period": "2025-12-31",
                "page": 26,
                "unit": "GWh",
                "value": 541,
            },
            {
                "sample_id": "year-2025",
                "record_id": "restated-old-period",
                "report_id": identity["report_id"],
                "document_version": identity["document_version"],
                "report_period": "2024-12-31",
                "page": 26,
                "unit": "GWh",
                "value": 1,
            },
            {
                "sample_id": "year-2024",
                "record_id": "prior-report",
                "report_id": "asset-old",
                "document_version": "ver-old",
                "report_period": "2024-12-31",
                "page": 1,
                "unit": "GWh",
                "value": 9,
            },
        ],
        "coverage": [],
    }
    binding = _binding_for_payload(tmp_path, FROZEN_CHAPTERS[1], payload)
    views = project_stage4_restricted_views(
        ("300750.SZ",),
        chapters=(binding,),
    )
    chapter = views[0]["chapters"][0]

    assert chapter["delivered"] is True
    assert [item["record_id"] for item in chapter["facts"]] == ["current"]
    assert views[0]["report"]["report_period"] == "2025-12-31"


def test_wrong_report_identity_does_not_borrow_another_year(tmp_path):
    payload = {
        "plan_version": FROZEN_CHAPTERS[1].plan_version,
        "chapter_task": "extract_operating_quantities",
        "reports": [
            {
                "sample_id": "year-2024",
                "instrument_id": "300750.SZ",
                "report_id": "asset-old",
                "document_version": "ver-old",
                "content_hash": "old-hash",
            }
        ],
        "facts": [
            {
                "sample_id": "year-2024",
                "record_id": "prior-report",
                "report_period": "2024-12-31",
                "value": 9,
            }
        ],
        "coverage": [],
    }
    binding = _binding_for_payload(tmp_path, FROZEN_CHAPTERS[1], payload)
    chapter = project_stage4_restricted_views(
        ("300750.SZ",),
        chapters=(binding,),
    )[0]["chapters"][0]

    assert chapter["delivered"] is False
    assert chapter["reason"] == "report_identity_mismatch"
    assert chapter["facts"] == []


def test_hash_mismatch_rejects_only_the_affected_chapter(tmp_path):
    source = default_repo_root() / FROZEN_CHAPTERS[0].replay_dir
    replay = tmp_path / "replay"
    shutil.copytree(source, replay)
    result_path = replay / FROZEN_CHAPTERS[0].result_name
    result_path.write_bytes(result_path.read_bytes() + b" ")
    damaged = FrozenChapterArtifacts(
        chapter_task=FROZEN_CHAPTERS[0].chapter_task,
        plan_version=FROZEN_CHAPTERS[0].plan_version,
        replay_dir=str(replay),
        result_name=FROZEN_CHAPTERS[0].result_name,
        enqueue_sha256=FROZEN_CHAPTERS[0].enqueue_sha256,
        run_sha256=FROZEN_CHAPTERS[0].run_sha256,
        result_sha256=FROZEN_CHAPTERS[0].result_sha256,
        source_review_sha256=FROZEN_CHAPTERS[0].source_review_sha256,
    )
    views = project_stage4_restricted_views(
        ("300750.SZ",),
        chapters=(damaged, FROZEN_CHAPTERS[1]),
    )
    chapters = {item["chapter_task"]: item for item in views[0]["chapters"]}

    assert chapters["extract_material_inputs"]["delivered"] is False
    assert chapters["extract_material_inputs"]["reason"] == "hash_mismatch:result"
    assert chapters["extract_material_inputs"]["facts"] == []
    assert chapters["extract_operating_quantities"]["delivered"] is True
    assert len(chapters["extract_operating_quantities"]["facts"]) == 8


def test_missing_artifact_rejects_only_the_affected_chapter(tmp_path):
    missing = FrozenChapterArtifacts(
        chapter_task=FROZEN_CHAPTERS[2].chapter_task,
        plan_version=FROZEN_CHAPTERS[2].plan_version,
        replay_dir=str(tmp_path / "absent"),
        result_name=FROZEN_CHAPTERS[2].result_name,
        enqueue_sha256=FROZEN_CHAPTERS[2].enqueue_sha256,
        run_sha256=FROZEN_CHAPTERS[2].run_sha256,
        result_sha256=FROZEN_CHAPTERS[2].result_sha256,
        source_review_sha256=FROZEN_CHAPTERS[2].source_review_sha256,
    )
    views = project_stage4_restricted_views(
        ("920015.BJ",),
        chapters=(missing, FROZEN_CHAPTERS[3]),
    )
    chapters = {item["chapter_task"]: item for item in views[0]["chapters"]}

    assert chapters["extract_segment_financials"]["delivered"] is False
    assert (
        chapters["extract_segment_financials"]["reason"] == "missing_artifact:enqueue"
    )
    assert chapters["extract_segment_financials"]["facts"] == []
    assert chapters["extract_counterparties_and_concentration"]["delivered"] is True
    assert len(chapters["extract_counterparties_and_concentration"]["facts"]) == 14


def test_repeated_query_does_not_write(tmp_path):
    before = _artifact_hashes()
    service = CompanyProfileReadService(tmp_path)
    first = service.query(("300750.SZ",))
    second = service.query(("300750.SZ",))

    assert first["stage4_restricted_views"] == second["stage4_restricted_views"]
    assert _artifact_hashes() == before
    assert list(tmp_path.rglob("*")) == []


def test_unrelated_company_does_not_receive_a_stage4_view(tmp_path):
    result = CompanyProfileReadService(tmp_path).query(("600000.SH",))

    assert result["stage4_restricted_views"] == []
    assert result["state"] == "not_found"
    assert result["profiles"] == []


def test_frozen_artifact_hashes_match_the_archive():
    actual = _artifact_hashes()
    expected = {
        f"{chapter.chapter_task}:{name}": getattr(chapter, f"{name}_sha256")
        for chapter in FROZEN_CHAPTERS
        for name in ("enqueue", "run", "result", "source_review")
    }

    assert actual == expected


def _binding_for_payload(
    tmp_path: Path,
    template: FrozenChapterArtifacts,
    payload: dict,
) -> FrozenChapterArtifacts:
    replay = tmp_path / "replay"
    result_dir = replay / Path(template.result_name).parent
    result_dir.mkdir(parents=True)
    result_path = replay / template.result_name
    encoded = json.dumps(payload).encode()
    result_path.write_bytes(encoded)
    enqueue = replay / "enqueue.json"
    run = replay / "run.json"
    review = replay / "source_review.json"
    enqueue.write_text("{}", encoding="utf-8")
    run.write_text("{}", encoding="utf-8")
    review.write_text("{}", encoding="utf-8")

    def digest(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    return FrozenChapterArtifacts(
        chapter_task=template.chapter_task,
        plan_version=template.plan_version,
        replay_dir=str(replay),
        result_name=template.result_name,
        enqueue_sha256=digest(enqueue),
        run_sha256=digest(run),
        result_sha256=digest(result_path),
        source_review_sha256=digest(review),
    )
