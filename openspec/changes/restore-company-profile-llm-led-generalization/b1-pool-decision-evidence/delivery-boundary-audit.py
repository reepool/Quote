"""Independent offline challenges for 176e10d0; never calls a model."""

from research.company_profile.contracts import ReportSemanticResponseV3
from research.company_profile.report_interpretation import (
    layered_processing_identity,
    research_views,
)
from tests.unit.test_research.test_company_profile_llm_led import (
    _one_batch_provider,
    _report,
    _run,
    _span_measurement,
)

PAGE = {"page": 1, "text": "公司经营有线电视网络传输。", "readable": True}


def test_rejected_native_dependency_cannot_remain_verified():
    report = _report()
    native = _span_measurement(report, "rev-in", "公司经营有线电视网络传输", 1)
    base = _one_batch_provider(
        report, claim_update={"supporting_record_ids": ("rev-in",)}
    )

    class Provider:
        offline_only = True

        def interpret(self, request):
            response = base.interpret(request)
            if request.purpose == "verify_views":
                assert [item.record_id for item in request.records] == ["rev-in"]
                import json

                payload = response.model_dump(mode="json")
                payload["checks"].append(
                    {
                        "target_type": "record",
                        "target_id": "rev-in",
                        "status": "unclear",
                        "explanation": "source conflict",
                    }
                )
                return ReportSemanticResponseV3.model_validate_json(json.dumps(payload))
            return response

    result = _run(
        Provider(),
        report=report,
        pages=(PAGE,),
        structured_records=(native,),
        processing_identity=layered_processing_identity(),
    )
    assert "record_semantic_conflict:rev-in" in result.gaps
    assert not any(item.record_id == "rev-in" for item in result.records)
    claim = result.claims[0]
    rendered = research_views(result)["views"]["company_profile"]["claims"][0]
    assert claim.status == "unclear", (claim.status, rendered["review_state"])
    assert rendered["review_state"] != "verified"


def test_same_page_changed_condition_does_not_reuse_previous_batch():
    report = _report()
    first = _run(
        _one_batch_provider(report),
        report=report,
        pages=(PAGE,),
        processing_identity=layered_processing_identity(),
    )
    saved = [
        item
        for item in first.call_receipts
        if item.get("status") == "success"
        and item.get("purpose") in {"synthesize_views", "verify_views"}
    ]
    assert len(saved) == 2
    identity = layered_processing_identity()
    identity["resumed_receipts"] = saved
    base = _one_batch_provider(report)

    class Provider:
        offline_only = True

        def interpret(self, request):
            response = base.interpret(request)
            if (
                request.purpose == "source_inventory"
                and request.source_scope != "page_map"
            ):
                return response.model_copy(
                    update={
                        "source_conditions": tuple(
                            item.model_copy(
                                update={"condition_id": "different-condition"}
                            )
                            for item in response.source_conditions
                        )
                    }
                )
            return response

    result = _run(
        Provider(), report=report, pages=(PAGE,), processing_identity=identity
    )
    assert result.source_conditions[0].condition_id.endswith(":different-condition")
    assert any(item.purpose == "synthesize_views" for item in base.requests), (
        [(item.purpose, item.source_scope) for item in base.requests],
        [(claim.condition_ids, claim.status) for claim in result.claims],
    )
