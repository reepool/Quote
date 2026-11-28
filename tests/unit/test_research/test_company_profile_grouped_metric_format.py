"""6.FY post-seal local format candidates; never new model quality or old scores."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from research.company_profile.workpaper import (
    CompanyProfileWorkpaper, WorkpaperFact, WorkpaperDraftResponse, _check_fact_source,
    bind_workpaper_draft, normalize_workpaper_draft, workpaper_elements,
)
from research.company_profile.workpaper_delivery import project_workpaper_delivery
from research.company_profile.workpaper_llm import run_workpaper, workpaper_processing_identity
from research.company_profile.workpaper_metric_format import (
    METRIC_FORMAT_FLAG, METRIC_FORMAT_POLICY, RAW_NULL_METRIC_FORMAT_POLICY,
    GROUPED_DECIMAL_METRIC_FORMAT_POLICY, normalize_directional_metric_format,
)
from research.company_profile.workpaper_review import review_workpaper, workpaper_hash
from research.company_profile.workpaper_sections import SectionPacket, build_section_packet, index_sections
from tests.unit.test_research.test_company_profile_workpaper import PAGES, _draft, _report, _selection


ROOT = Path(__file__).resolve().parents[3]
SAVED = ROOT / "openspec/changes/restore-company-profile-llm-led-generalization/workpaper-prospective20-closeout-20261128"


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("post-seal local format validation must not send a request")
    monkeypatch.setattr("socket.getaddrinfo", denied)
    monkeypatch.setattr("socket.create_connection", denied)


def fact(raw="1,234,567.80", value="1,234,567.80", **changes):
    return dict(fact_id="grouped", text="保留原表披露。", subject_scope="consolidated_group", period="2025",
        evidence_ids=["source-0001"], metrics=[{"name": "营业收入", "raw_value": raw, "value": value,
            "unit": "元", "source_unit": "元", "currency": "CNY", "subject_scope": "consolidated_group", "period": "2025",
            **changes}])


@pytest.mark.parametrize("raw,value,expected", [
    ("18,945,220,585", "18,945,220,585", "18945220585"),
    ("12,601,193,497.11", "12,601,193,497.11", "12601193497.11"),
    ("-5,021,297,876.69", "-5,021,297,876.69", "-5021297876.69"),
    ("+1234.5600", "+1,234.5600", "+1234.5600"),
    ("1,234.00元", "1,234.00", "1234.00"),
    ("1234.00", "1,234.00", "1234.00"),
])
def test_proper_grouping_changes_only_value_preserving_sign_and_decimal(raw, value, expected):
    original = fact(raw, value)
    before = deepcopy(original)
    result, notes = normalize_directional_metric_format(original, policy=GROUPED_DECIMAL_METRIC_FORMAT_POLICY)
    assert original == before
    assert result == {**original, "metrics": [{**original["metrics"][0], "value": expected}]}
    assert len(notes) == 1 and "非新的模型源核或语义接受" in notes[0]
    assert f"value={value}→{expected}" in notes[0]
    WorkpaperFact.model_validate(result)


@pytest.mark.parametrize("policy", [None, METRIC_FORMAT_POLICY, RAW_NULL_METRIC_FORMAT_POLICY])
def test_previous_policies_keep_grouping_rejection_and_unchanged_input(policy):
    original = fact()
    result, notes = normalize_directional_metric_format(original, policy=policy)
    assert result == original and not notes
    with pytest.raises(ValueError, match="decimal strings"):
        WorkpaperFact.model_validate(result)
    assert workpaper_processing_identity(real=False)[METRIC_FORMAT_FLAG] == RAW_NULL_METRIC_FORMAT_POLICY


@pytest.mark.parametrize("raw,value,updates", [
    ("1,234.00", "12,34.00", {}), ("1,234.00", "1,23,4.00", {}),
    ("1,234.00", "01,234.00", {}), ("1,234.00", "1,234.00e0", {}),
    ("1,234.00", "1,234.00\n5", {}), ("1,234.00", "1,234 .00", {}),
    ("1,234.00", "1，234.00", {}), ("1,234.00", "(1,234.00)", {}),
    ("1,234.00", "-1,234.00", {}), ("1,234.00", "1,235.00", {}),
    ("12,34.00", "1,234.00", {}), ("1,23,4.00", "1,234.00", {}),
    ("1,\n234.00", "1,234.00", {}), ("1,234e0", "1,234.00", {}),
    ("1,234.00元万元", "1,234.00", {}),
    ("1,234.00", "1,234.00", {"unit": "万元"}),
    ("1,234.00", "1,234.00", {"unit": None}),
    ("1,234.00", "1,234.00", {"qualifier": "approximate"}),
    ("1,234.00", "1,234.00", {"source_unit": False}),
    ("1,234.00%", "1,234.00", {"unit": "百分点", "source_unit": "%"}),
])
def test_invalid_grouping_value_sign_and_unit_are_not_normalized(raw, value, updates):
    original = fact(raw, value, **updates)
    result, notes = normalize_directional_metric_format(original, policy=GROUPED_DECIMAL_METRIC_FORMAT_POLICY)
    assert result == original and not notes
    with pytest.raises(ValueError):
        WorkpaperFact.model_validate(result)


def test_v3_retains_v2_raw_null_accounting_and_directional_behavior():
    samples = [fact("原文待标准化", None), fact("(1,234.00)", "-1234.00"),
        fact("下降0.71%", "-0.71", unit="%", source_unit="%")]
    for original in samples:
        assert normalize_directional_metric_format(original, policy=GROUPED_DECIMAL_METRIC_FORMAT_POLICY) == (
            normalize_directional_metric_format(original, policy=RAW_NULL_METRIC_FORMAT_POLICY))


def test_grouped_conversion_still_requires_original_typed_fields():
    for update in ({"subject_scope": "unknown_scope"}, {"currency": False}, {"period": ""}):
        result, _ = normalize_directional_metric_format(fact(**update), policy=GROUPED_DECIMAL_METRIC_FORMAT_POLICY)
        with pytest.raises(ValueError):
            WorkpaperFact.model_validate(result)


def source_material(**metric_updates):
    pages = tuple({**page, "text": ("单位：元\n" if page["page"] == 3 else "")
        + page["text"].replace("100.00", "1,234,567.80")} for page in PAGES)
    packet = build_section_packet(_report(), pages, index_sections(pages), _selection(index_sections(pages)))
    payload = _draft(packet).model_dump(mode="json")
    candidate = payload["elements"]["scale_and_mix"]["facts"][0]
    candidate["text"] = "报告表列本期营业收入1,234,567.80元。"
    candidate["metrics"][0].update(raw_value="1,234,567.80", value="1,234,567.80", source_unit="元", **metric_updates)
    return pages, packet, payload


@pytest.mark.parametrize("failure", [None, "value", "sign", "source_unit", "source_grouping", "unknown_evidence", "coordinates"])
def test_extraction_normalizer_keeps_full_source_binding(failure):
    pages, packet, payload = source_material()
    candidate = payload["elements"]["scale_and_mix"]["facts"][0]
    if failure == "value":
        candidate["metrics"][0].update(raw_value="9,234,567.80", value="9,234,567.80")
    elif failure == "sign":
        candidate["metrics"][0].update(raw_value="-1,234,567.80", value="-1,234,567.80")
    elif failure == "source_unit":
        candidate["metrics"][0].update(unit="万元", source_unit="万元")
    elif failure == "unknown_evidence":
        candidate["evidence_ids"] = ["unknown"]
    elif failure == "source_grouping":
        pages = tuple({**p, "text": p["text"].replace("1,234,567.80", "12,34,567.80")} for p in pages)
        packet = build_section_packet(_report(), pages, index_sections(pages), _selection(index_sections(pages)))
    old = WorkpaperDraftResponse.model_validate(normalize_workpaper_draft(payload, metric_format_policy=RAW_NULL_METRIC_FORMAT_POLICY))
    assert not old.elements.scale_and_mix.facts
    draft = WorkpaperDraftResponse.model_validate(normalize_workpaper_draft(payload, metric_format_policy=GROUPED_DECIMAL_METRIC_FORMAT_POLICY))
    if failure == "unknown_evidence":
        with pytest.raises(ValueError, match="outside"):
            bind_workpaper_draft(draft, report=packet.report, reading_scope=packet.reading_scope, evidence=packet.evidence)
        return
    paper = bind_workpaper_draft(draft, report=packet.report, reading_scope=packet.reading_scope,
        evidence=packet.evidence, origin="controlled_fixture")
    if failure == "coordinates":
        pages = tuple({**p, "text": "altered page"} for p in pages)
        with pytest.raises(ValueError, match="source span"):
            paper.validate_pages(pages)
    elif failure is None:
        paper.validate_pages(pages)
        assert paper.elements.scale_and_mix.facts[0].metrics[0].value == "1234567.80"
        assert project_workpaper_delivery(paper)["facts"][1]["review_basis"] == "not_reviewed"
    else:
        assert not paper.elements.scale_and_mix.facts
        assert paper.elements.business_structure.facts


def saved_case(stock):
    api_path = SAVED / "execution" / stock / "run/first-api.json"
    if not api_path.is_file():
        pytest.skip("sealed 6.FY original source corpus not installed")
    api = json.loads(api_path.read_text())
    base = SAVED / "raw20/company_profile_common_core.v1"
    record_path = base / (api["base_work_id"] + ".json")
    record = json.loads(record_path.read_text())
    checkpoint = json.loads((base / "checkpoints" / record_path.name).read_text())
    paper = CompanyProfileWorkpaper.model_validate(record["workpaper"])
    packet = SectionPacket.model_validate(checkpoint["workpaper_packet"])
    receipts = {r["purpose"]: r for r in record["workpaper_receipts"]}
    for receipt in receipts.values():
        assert hashlib.sha256(receipt["raw_content"].encode()).hexdigest() == receipt["response_hash"]
        assert json.loads(receipt["raw_content"]) == receipt["response_payload"]
    return record, checkpoint, paper, packet, receipts


@pytest.mark.parametrize("stock,affected,grouped_metrics", [
    ("002463.SZ", {"SC-01", "SC-02", "SC-03", "SC-04", "SC-05", "CC-01", "CC-02"}, 11),
    ("601098.SH", {"sm-01", "sm-02", "sm-03", "sm-04", "sm-05", "sm-06", "sm-07", "sm-08", "sm-09", "sm-10", "cc-01"}, 13),
])
def test_sealed_real_extraction_caller_restores_only_local_unreviewed_candidates(stock, affected, grouped_metrics):
    from research.business_profile_pdf_artifacts import BusinessProfilePdfArtifactExtractor

    record, checkpoint, old_paper, packet, receipts = saved_case(stock)
    pdf = Path(checkpoint["source_pdf_path"])
    if not pdf.is_file():
        pytest.skip("official PDF is not locally installed")
    assert hashlib.sha256(pdf.read_bytes()).hexdigest() == old_paper.report.document_version
    native = BusinessProfilePdfArtifactExtractor().extract_file(pdf)
    pages = tuple({"page": p.page_number, "text": p.text,
                   "readable": p.native_text_status == "extracted" and bool(p.text.strip())} for p in native.pages)
    class ReplayOriginal:
        offline_only = True
        def __init__(self):
            self.workpaper_receipts = []
        def workpaper_request(self, **kwargs):
            receipt = deepcopy(receipts[kwargs["purpose"]])
            assert kwargs["request_id"] == receipt["request_id"]
            if kwargs["purpose"] == "extract_profile_workpaper":
                assert kwargs["payload"]["source_text"] == packet.model_text()
                assert kwargs["payload"]["reading_scope"] == packet.reading_scope.model_dump(mode="json")
            self.workpaper_receipts.append(receipt)
            raw = deepcopy(receipt["response_payload"])
            return normalize_workpaper_draft(raw) if kwargs["purpose"] == "extract_profile_workpaper" else raw
    provider = ReplayOriginal()
    identity = {**record["execution"]["input_identity"]["processing_identity"], "execution_mode": "offline_fake",
                METRIC_FORMAT_FLAG: GROUPED_DECIMAL_METRIC_FORMAT_POLICY}
    result = run_workpaper(report=old_paper.report, pages=pages, processing_identity=identity,
        provider=provider, work_id=record["work_id"], source_pdf_path=str(pdf))
    assert result.packet == packet
    current = {f.fact_id: f for group in workpaper_elements(result.workpaper).values() for f in group.facts}
    previous = {f.fact_id for group in workpaper_elements(old_paper).values() for f in group.facts}
    assert set(current) - previous == affected
    assert result.workpaper.quality_review is None and result.workpaper.use_review is None
    assert result.workpaper.origin == "controlled_fixture"
    assert len(result.receipts) == 2 and result.receipts == tuple(receipts.values())
    assert result.workpaper.generation == old_paper.generation
    notes = [note for group in workpaper_elements(result.workpaper).values() for note in group.limitations if "程序格式规范化" in note]
    assert len(notes) == grouped_metrics
    for f in project_workpaper_delivery(result.workpaper)["facts"]:
        if f["fact_id"] in affected:
            assert f["review_basis"] == "not_reviewed"
            assert f["model_review_stage"] is None
    result.workpaper.validate_pages(result.pages)
    # This test creates no release/union: existing automatic final stays authoritative.
    final = json.loads((SAVED / "execution" / stock / "run/automatic-final-api.json").read_text())
    assert any(f["review_basis"] == "model_source_check" for f in final["facts"])


@pytest.mark.parametrize("case", ["original", "grouped", "wrong_value", "wrong_sign", "wrong_unit", "bad_group", "bad_raw", "unknown_evidence"])
def test_saved_real_repair_caller_checks_original_source_without_reusing_old_final(case):
    from utils.llm.errors import LlmError
    from research.company_profile.reads import _record_processing_identity
    from research.company_profile.runtime import CompanyProfileRuntimeRecordV3

    record, _, paper, packet, _ = saved_case("002463.SZ")
    final_api = json.loads((SAVED / "execution/002463.SZ/run/automatic-final-api.json").read_text())
    reviewed = CompanyProfileRuntimeRecordV3.model_validate_json((SAVED / "raw20/company_profile_common_core.v1" /
        (final_api["latest_reading_work_id"] + ".json")).read_text())
    assert workpaper_hash(paper) == reviewed.workpaper.quality_review.original_draft_hash
    receipts = {r["purpose"]: deepcopy(r) for r in reviewed.workpaper_receipts}
    repair = receipts["repair_profile_workpaper"]
    original_hash = repair["response_hash"]
    count = 0
    if case != "original":
        for candidate in repair["response_payload"]["repaired_facts"]:
            for metric in candidate.get("metrics", []):
                raw = metric["raw_value"]
                if metric.get("value") is not None and "," in raw:
                    metric["value"] = raw
                    count += 1
        assert count == 5
        first = repair["response_payload"]["repaired_facts"][0]
        metric = first["metrics"][0]
        if case == "wrong_value":
            metric["value"] = "18,945,220,586"
        elif case == "wrong_sign":
            metric.update(raw_value="-18,945,220,585", value="-18,945,220,585")
        elif case == "wrong_unit":
            metric["source_unit"] = "万元"
        elif case == "bad_group":
            metric["value"] = "18,945,22,0585"
        elif case == "bad_raw":
            metric["raw_value"] = "18,945,22,0585"
        elif case == "unknown_evidence":
            first["evidence_ids"] = ["not-in-supplied-source"]
        repair["raw_content"] = json.dumps(repair["response_payload"], ensure_ascii=False)
        repair["response_hash"] = hashlib.sha256(repair["raw_content"].encode()).hexdigest()
        assert repair["response_hash"] != original_hash  # This is a controlled format variant, not old model bytes.
    class SavedStages:
        offline_only = True
        def __init__(self):
            self.workpaper_receipts, self.calls = [], []
        def workpaper_request(self, **kwargs):
            self.calls.append(kwargs)
            if kwargs["purpose"] == "verify_profile_workpaper_repair":
                raise LlmError(code="deadline_exceeded", message="offline candidate boundary: no new final")
            receipt = deepcopy(receipts[kwargs["purpose"]])
            assert kwargs["request_id"] == receipt["request_id"]
            assert kwargs["payload"]["source_text"] == packet.model_text()
            self.workpaper_receipts.append(receipt)
            return receipt["response_payload"]
    identity = {**_record_processing_identity(reviewed), "execution_mode": "offline_fake",
                METRIC_FORMAT_FLAG: GROUPED_DECIMAL_METRIC_FORMAT_POLICY}
    provider = SavedStages()
    result, kept = review_workpaper(paper, packet=packet, provider=provider, work_id=reviewed.work_id, identity=identity)
    assert result.quality_review.status == "incomplete"
    bad_candidate = case not in {"original", "grouped"}
    candidates = [f.revised_fact for f in result.quality_review.findings
                  if f.revised_fact is not None and f.resolution == "verification_failed"]
    assert len(candidates) == (6 if bad_candidate else 7)
    assert all(f.resolution != "corrected_pending_trust" for f in result.quality_review.findings)
    assert not any(c.purpose == "verify_profile_workpaper_repair" for c in result.quality_review.calls)
    for candidate in candidates:
        _check_fact_source(candidate, {e.evidence_id: e for e in packet.evidence}, check_units=True)
    sent = provider.calls[-1]["payload"]["candidates"]
    assert len(sent) == len(candidates) and all("," not in m["value"] for c in sent for m in c["metrics"] if m.get("value") is not None)
    if bad_candidate:
        assert all(c["fact_id"] != "review-add:F-OM-REV-01" for c in sent)
        rejected = next(f for f in result.quality_review.findings if f.finding_id == "F-OM-REV-01")
        assert rejected.resolution == "repair_rejected"
    assert next(r for r in kept if r["purpose"] == "repair_profile_workpaper")["response_hash"] == repair["response_hash"]
    assert len([note for note in result.standardization_notes if "程序格式规范化" in note]) == count - int(bad_candidate)
    # Even a newly normalized source-bound proposal cannot borrow the old accepted final.
    assert not any(f["review_basis"] == "model_source_check" and f["model_review_stage"] == "final"
                   for f in project_workpaper_delivery(result)["facts"])
