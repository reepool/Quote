from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

from research.business_profile_section_selection import (
    SelectedSection,
    SelectedSectionArtifact,
)
from research.company_profile.shadow_batch import ShadowSampleManifest
from research.company_profile.shadow_batch_audit import (
    build_shadow_readiness_audit,
    build_shadow_review_package,
)
from research.company_profile.shadow_batch_service import (
    ManufacturingMaterialsShadowBatchService,
    ShadowBatchStore,
    load_shadow_batch_result,
    load_shadow_report_result,
)
from research.company_profile.shadow_evidence import (
    ShadowEvidencePlanner,
    ShadowEvidencePreparer,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
CHANGE_ROOT = (
    REPOSITORY_ROOT
    / "openspec/changes/archive/2026-09-09-validate-manufacturing-materials-company-profile-shadow-batch"
)
SHADOW_MANIFEST = CHANGE_ROOT / "shadow-manifest.v1.json"


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class _ManifestExtractor:
    def __init__(self, manifest: ShadowSampleManifest) -> None:
        self._by_path = {str(item.local_path): item for item in manifest.reports}

    def extract_file(self, path: str | Path, *, source_file_id: str):
        report = self._by_path[str(path)]
        return SimpleNamespace(
            status="parsed",
            diagnostics={},
            source_content_hash=report.content_hash,
            page_count=report.page_count,
            artifact_hash=_hash(f"artifact:{report.sample_id}"),
            pages=tuple(
                SimpleNamespace(
                    page_number=page,
                    printed_page_label=str(page),
                    text=(
                        "公司主要从事钢铁制造，主要业务包括钢材生产和销售。"
                        "分行业 营业收入 营业成本 毛利率；主要产品产销量 单位：吨；"
                        "公司生产所需主要原材料为铁矿石；前五名客户销售额占比；"
                        "公司业务、产品或服务发生重大变化：不适用。"
                    ),
                    extraction_method="native_text",
                    native_text_status="extracted",
                    page_artifact_hash=_hash(f"page:{page}"),
                )
                for page in range(1, 5)
            ),
        )


class _Selector:
    def select(self, **kwargs):
        term = next(iter(kwargs["hint_terms"]))
        text = (
            "公司主要从事钢铁制造，主要业务包括钢材生产和销售。"
            "分行业 营业收入 营业成本 毛利率；主要产品产销量 单位：吨；"
            "公司生产所需主要原材料为铁矿石；前五名客户销售额占比；"
            "公司业务、产品或服务发生重大变化：不适用。"
        )
        sections = tuple(
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
                quality="native",
            )
            for page in range(1, 5)
        )
        return SelectedSectionArtifact(
            artifact_version="test",
            bundle={},
            sections=sections,
            previous_bundle_id=None,
            expansion_reason=None,
            artifact_hash=_hash(f"selection:{term}"),
        )


def _admitted_inputs():
    manifest = ShadowSampleManifest.model_validate_json(
        SHADOW_MANIFEST.read_text(encoding="utf-8")
    )
    planner = ShadowEvidencePlanner(
        extractor=_ManifestExtractor(manifest),
        selector=_Selector(),
    )
    plan = planner.build(manifest)
    prepared = ShadowEvidencePreparer(planner=planner).prepare(
        manifest=manifest,
        plan=plan,
    )
    return manifest, plan, prepared


def test_shadow_batch_admits_twenty_without_weakening_legacy_bundle(
    tmp_path: Path,
) -> None:
    manifest, plan, prepared = _admitted_inputs()
    result, output_path = ManufacturingMaterialsShadowBatchService().run(
        batch_id="shadow-unit-success",
        primary_logical_profile="semantic_extraction__scorpio_gemini",
        manifest=manifest,
        evidence_plan=plan,
        prepared=prepared,
        store=ShadowBatchStore(tmp_path / "shadow"),
        provider_factory=lambda _scope: None,
    )

    assert result.completed_report_count == 20
    assert result.failed_report_count == 0
    assert result.production_authorization == "not_authorized"
    assert load_shadow_batch_result(output_path / "manifest.json") == result
    assert len(list((output_path / "reports").glob("*.json"))) == 20
    first = load_shadow_report_result(output_path / result.reports[0].relative_path)
    assert first.sample_id == manifest.reports[0].sample_id
    assert first.production_authorization == "not_authorized"
    assert json.loads((output_path / "manifest.json").read_text())["result_hash"]
    review = build_shadow_review_package(result, batch_directory=output_path)
    sampled = [
        item for item in review.rows if item.category in {"chapter_sample", "caveat"}
    ]
    assert len({(item.sample_id, item.chapter_task) for item in sampled}) == len(
        sampled
    )
    assert len(sampled) <= 20 * 6
    audit = build_shadow_readiness_audit(
        result,
        review,
        batch_directory=output_path,
        prepared_report_count=20,
        audit_id="shadow-unit-audit",
    )
    assert audit.readiness_decision == "hold"
    assert audit.execution_completion_rate == 0.0
    assert audit.evidence_traceability_rate == 1.0
    assert audit.production_authorization == "not_authorized"


def test_shadow_batch_keeps_report_failure_separate_and_continues(
    tmp_path: Path,
) -> None:
    manifest, plan, prepared = _admitted_inputs()
    failed_sample = manifest.reports[0].sample_id

    def provider_factory(scope):
        if scope.sample_id == failed_sample:
            raise RuntimeError("typed provider setup failure")

    result, output_path = ManufacturingMaterialsShadowBatchService().run(
        batch_id="shadow-unit-isolation",
        primary_logical_profile="semantic_extraction__scorpio_gemini",
        manifest=manifest,
        evidence_plan=plan,
        prepared=prepared,
        store=ShadowBatchStore(tmp_path / "shadow"),
        provider_factory=provider_factory,
    )

    assert result.completed_report_count == 19
    assert result.failed_report_count == 1
    failed = next(item for item in result.reports if item.status == "failed")
    assert failed.sample_id == failed_sample
    payload = load_shadow_report_result(output_path / failed.relative_path)
    assert payload.status == "failed"
    assert payload.diagnostics[0].code == "RuntimeError"
    assert all(
        item.sample_id != failed_sample
        for item in result.reports
        if item.status == "success"
    )


def test_shadow_batch_rejects_plan_manifest_mismatch_before_provider(
    tmp_path: Path,
) -> None:
    _manifest, plan, _prepared = _admitted_inputs()
    payload = plan.model_dump(mode="json")
    payload["sample_manifest_revision"] = "wrong-revision"
    payload["plan_hash"] = "0" * 64

    try:
        type(plan).model_validate(payload)
    except ValueError:
        pass
    else:  # pragma: no cover
        raise AssertionError("tampered shadow plan should be rejected")

    assert not (tmp_path / "shadow").exists()
