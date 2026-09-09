from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research.business_profile_section_selection import (
    SelectedSection,
    SelectedSectionArtifact,
)
from research.company_profile.models import ChapterTask
from research.company_profile.shadow_batch import ShadowSampleManifest
from research.company_profile.shadow_evidence import (
    ShadowEvidencePlanner,
    ShadowEvidencePlanningError,
    ShadowEvidencePreparer,
    ShadowPlanningFailureCode,
    build_shadow_preparation_audit,
    load_shadow_evidence_plan,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
CHANGE_ROOT = (
    REPOSITORY_ROOT
    / "openspec/changes/validate-manufacturing-materials-company-profile-shadow-batch"
)
SHADOW_MANIFEST = CHANGE_ROOT / "shadow-manifest.v1.json"


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _manifest() -> ShadowSampleManifest:
    return ShadowSampleManifest.model_validate_json(
        SHADOW_MANIFEST.read_text(encoding="utf-8")
    )


class _ManifestExtractor:
    def __init__(self, manifest: ShadowSampleManifest, *, mismatch: bool = False) -> None:
        self._by_path = {str(item.local_path): item for item in manifest.reports}
        self._mismatch = mismatch

    def extract_file(self, path: str | Path, *, source_file_id: str):
        report = self._by_path[str(path)]
        content_hash = "f" * 64 if self._mismatch else report.content_hash
        return SimpleNamespace(
            status="parsed",
            diagnostics={},
            source_content_hash=content_hash,
            page_count=report.page_count,
            artifact_hash=_hash(f"artifact:{report.sample_id}"),
            pages=tuple(
                SimpleNamespace(
                    page_number=page,
                    printed_page_label=str(page),
                    text=(
                        "主营业务 分部信息 产销量 原材料 前五名客户 经营模式 "
                        f"单位：吨 受控原文第{page}页"
                    ),
                    extraction_method="native_text",
                    native_text_status="extracted",
                    page_artifact_hash=_hash(f"page:{page}"),
                )
                for page in range(1, 5)
            ),
        )


class _Selector:
    def __init__(self, *, quality: str = "native", continuation_gap: bool = False) -> None:
        self._quality = quality
        self._continuation_gap = continuation_gap

    def select(self, **kwargs):
        term = next(iter(kwargs["hint_terms"]))
        sections = []
        pages = (2,) if self._continuation_gap else (1, 2, 3, 4)
        for page in pages:
            text = (
                "主营业务 分部信息 产销量 原材料 前五名客户 经营模式 "
                f"单位：吨 受控原文第{page}页"
            )
            if self._continuation_gap:
                text += "\n续表"
            sections.append(
                SelectedSection(
                    section_id=f"section-{page}",
                    page_number=page,
                    section_key="structured_hint",
                    text=text,
                    normalized_text=text,
                    normalized_start=0,
                    normalized_end=len(text),
                    page_hash=_hash(f"page:{page}"),
                    section_hash=_hash(f"section:{term}:{page}"),
                    selector_reasons=(f"structured_hint:{term}",),
                    quality=self._quality,
                )
            )
        return SelectedSectionArtifact(
            artifact_version="test",
            bundle={},
            sections=tuple(sections),
            previous_bundle_id=None,
            expansion_reason=None,
            artifact_hash=_hash(f"selection:{term}"),
        )


def test_shadow_planner_generates_six_bounded_hash_bound_chapters() -> None:
    manifest = _manifest()
    planner = ShadowEvidencePlanner(
        extractor=_ManifestExtractor(manifest),
        selector=_Selector(),
    )

    plan = planner.build(manifest)

    assert len(plan.reports) == 20
    for report in plan.reports:
        assert {task.chapter_task for task in report.tasks} == set(ChapterTask)
        assert all(
            len(scope.pages) <= 3
            and all(right == left + 1 for left, right in zip(scope.pages, scope.pages[1:]))
            for task in report.tasks
            for scope in task.request_scopes
        )
        selections = plan.scope_selections[report.sample_id]
        assert 6 <= len(selections) <= 9
        assert all(selection.page_hashes for selection in selections)
        assert all(selection.selector_reasons for selection in selections)
    assert plan.production_authorization == "not_authorized"
    prepared = ShadowEvidencePreparer(planner=planner).prepare(
        manifest=manifest,
        plan=plan,
    )
    audit = build_shadow_preparation_audit(
        plan,
        audit_id="shadow-preparation-test",
        prepared=prepared,
    )
    assert audit.report_count == 20
    assert audit.planned_report_count == 20
    assert audit.total_scope_count <= 180
    assert audit.evidence_traceability_rate == 1.0
    assert audit.recovered_report_count == 0
    assert audit.production_authorization == "not_authorized"


def test_shadow_planner_rejects_pdf_identity_mismatch() -> None:
    manifest = _manifest()
    planner = ShadowEvidencePlanner(
        extractor=_ManifestExtractor(manifest, mismatch=True),
        selector=_Selector(),
    )

    with pytest.raises(ShadowEvidencePlanningError) as caught:
        planner.build(manifest)

    assert caught.value.code == ShadowPlanningFailureCode.PDF_IDENTITY_MISMATCH


@pytest.mark.parametrize(
    ("selector", "expected"),
    [
        (_Selector(quality="low_text"), ShadowPlanningFailureCode.PAGE_UNREADABLE),
        (_Selector(continuation_gap=True), ShadowPlanningFailureCode.TABLE_CONTEXT_INCOMPLETE),
    ],
)
def test_shadow_planner_rejects_unusable_or_incomplete_evidence(selector, expected) -> None:
    manifest = _manifest()
    planner = ShadowEvidencePlanner(
        extractor=_ManifestExtractor(manifest),
        selector=selector,
    )

    with pytest.raises(ShadowEvidencePlanningError) as caught:
        planner.build(manifest)

    assert caught.value.code == expected


def test_shadow_plan_loader_rejects_answer_bearing_content(tmp_path: Path) -> None:
    manifest = _manifest()
    plan = ShadowEvidencePlanner(
        extractor=_ManifestExtractor(manifest),
        selector=_Selector(),
    ).build(manifest)
    payload = plan.model_dump(mode="json")
    payload["gold"] = {"expected_value": "40000"}
    path = tmp_path / "answer-bearing.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ShadowEvidencePlanningError) as caught:
        load_shadow_evidence_plan(path)

    assert caught.value.code == ShadowPlanningFailureCode.PLAN_INVALID
