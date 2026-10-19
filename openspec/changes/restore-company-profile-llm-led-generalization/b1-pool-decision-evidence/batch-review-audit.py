import pytest

from research.company_profile.models import TextAnchor
from research.company_profile.report_interpretation import research_views
from tests.unit.test_research import test_company_profile_llm_led as submitted


def submitted_case(monkeypatch):
    """Reuse the submitted test's exact setup without its success assertions."""
    captured = {}

    class Captured(Exception):
        pass

    def capture(provider, **kwargs):
        captured.update(provider=provider, kwargs=kwargs)
        raise Captured

    with monkeypatch.context() as local:
        local.setattr(submitted, "_run", capture)
        with pytest.raises(Captured):
            submitted.test_a_verified_batch_remains_when_the_next_synthesis_fails()
    return captured["provider"], captured["kwargs"]


@pytest.mark.parametrize("defect", ["wrong_quote", "missing_record", "verify_failure"])
def test_batch_hard_constraints_before_promotion(monkeypatch, defect):
    provider, kwargs = submitted_case(monkeypatch)
    interpret = provider.interpret

    def respond(request):
        if defect == "verify_failure" and request.purpose == "verify_views":
            raise ValueError("review_injected_verification_failure")
        response = interpret(request)
        if request.purpose != "synthesize_views" or not response.claims:
            return response
        claim = response.claims[0]
        if defect == "wrong_quote":
            evidence = claim.evidence[0].model_copy(
                update={"anchor": TextAnchor(bounded_quote="原文没有的盈利预测")}
            )
            claim = claim.model_copy(update={"evidence": (evidence,)})
        elif defect == "missing_record":
            claim = claim.model_copy(
                update={"supporting_record_ids": ("missing-native-record",)}
            )
        return response.model_copy(update={"claims": (claim,)})

    provider.interpret = respond
    result = submitted._run(provider, **kwargs)
    rendered = research_views(result)["views"]["company_profile"]["claims"]
    assert result.real_semantic_complete is False
    assert all(item["review_state"] != "verified" for item in rendered), rendered


def test_submitted_test_reaches_the_second_synthesis(monkeypatch):
    run = submitted._run
    second_calls = []

    def traced(provider, **kwargs):
        interpret = provider.interpret

        def respond(request):
            if request.purpose == "synthesize_views" and any(
                item.condition_id.endswith("cond-b")
                for item in request.source_conditions
            ):
                second_calls.append(request.request_id)
            return interpret(request)

        provider.interpret = respond
        return run(provider, **kwargs)

    monkeypatch.setattr(submitted, "_run", traced)
    submitted.test_a_verified_batch_remains_when_the_next_synthesis_fails()
    assert second_calls, "second synthesis was never attempted"
