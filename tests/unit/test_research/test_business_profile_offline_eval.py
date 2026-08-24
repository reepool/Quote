import json
from pathlib import Path

from research.business_profile_semantic_extraction import (
    _validate_batch_verification_response,
)


def test_offline_evaluation_manifest_is_local_and_covers_required_industries():
    root = (
        Path(__file__).parents[2] / "fixtures" / "business_profile_offline_eval" / "v1"
    )
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "business_profile_offline_eval.v1"
    assert {item["industry"] for item in manifest["cases"]} == {
        "制造业",
        "能源",
        "医药",
        "金融",
        "消费",
        "矿业",
        "多业务集团",
    }
    for item in manifest["cases"]:
        payload = json.loads((root / item["file"]).read_text(encoding="utf-8"))
        assert payload["case_id"] == item["case_id"]
        assert payload["sections"]
        assert "expected" in payload


def test_batch_verification_accepts_partial_targets_for_resumable_replay():
    checks = {
        "subject": True,
        "action": True,
        "object": True,
        "scope": True,
        "period": True,
        "evidence": True,
    }
    _validate_batch_verification_response(
        {
            "decisions": [
                {
                    "target_id": "activity-1",
                    "decision": "supported",
                    "checks": checks,
                    "failed_aspects": [],
                    "reason_zh": "公告证据完整支持该业务断言",
                }
            ]
        },
        expected_ids=("activity-1", "activity-2"),
    )


def test_offline_corpus_reports_expected_label_coverage_without_network():
    root = (
        Path(__file__).parents[2] / "fixtures" / "business_profile_offline_eval" / "v1"
    )
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    expected_records = 0
    ambiguous_cases = 0
    for item in manifest["cases"]:
        payload = json.loads((root / item["file"]).read_text(encoding="utf-8"))
        expected = payload["expected"]
        expected_records += len(expected.get("activities", []))
        expected_records += len(expected.get("relationships", []))
        ambiguous_cases += int(bool(expected.get("expected_ambiguity")))
    assert len(manifest["cases"]) == 7
    assert expected_records > 0
    assert ambiguous_cases == 1
