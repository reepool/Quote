import asyncio
from dataclasses import asdict
from types import SimpleNamespace

import pytest

from scripts.dev_validation import benchmark_llm_orchestration_live as benchmark
from scripts.dev_validation.benchmark_llm_orchestration_live import (
    acceptance_reasons,
    build_controlled_config,
)
from utils.config_manager import config_manager


def _accepted_result():
    return {
        "concurrency": 10,
        "successes": 10,
        "rate_limits": 0,
        "provider_5xx": 0,
        "timeouts": 0,
        "parse_failures": 0,
        "schema_failures": 0,
        "failover_exhausted": 0,
        "first_event_count": 10,
        "identity_ok": True,
        "registry_empty_after_shutdown": True,
        "transport_active_after_shutdown": 0,
        "fd_delta": 0,
        "dispatch_counts": {
            "pipio:grok-4.5": 8,
            "pipio:gpt-5.6-luna": 2,
        },
        "configured_weight_ratio": {
            "pipio:grok-4.5": 0.75,
            "pipio:gpt-5.6-luna": 0.25,
        },
    }


def test_live_acceptance_gate_requires_clean_complete_stage():
    assert acceptance_reasons(_accepted_result()) == []
    failed = {**_accepted_result(), "successes": 9, "provider_5xx": 1}
    assert acceptance_reasons(failed) == [
        "not_all_requests_succeeded",
        "nonzero_provider_5xx",
    ]


def test_live_acceptance_gate_rejects_missing_measurements_and_identity():
    failed = {
        **_accepted_result(),
        "first_event_count": 9,
        "identity_ok": False,
    }

    assert acceptance_reasons(failed) == [
        "missing_first_event_measurements",
        "request_identity_mismatch",
    ]


def test_controlled_live_config_is_in_memory_and_quota_bounded():
    base = config_manager.get_llm_config()
    original = asdict(base)
    controlled = build_controlled_config(
        base,
        logical_profile="semantic_extraction",
        concurrency=10,
        confirmed_quota_scope="independent",
        confirmed_per_source_concurrency=10,
        confirmed_provider_rpm=10,
        timeout_seconds=120,
    )

    assert controlled.enabled is True
    assert controlled.pools["shared_semantic"].enabled is True
    assert controlled.pools["shared_semantic"].total_concurrency == 10
    assert all(
        controlled.profiles[name].enabled
        for name in (
            "semantic_extraction__pipio_grok",
            "semantic_extraction__pipio_luna",
        )
    )
    for resource_name in ("pipio:grok", "pipio:luna"):
        resource = controlled.provider_resources[resource_name]
        assert resource.hard_max_concurrency == 10
        assert resource.requests_per_minute == 10
    for profile_name in (
        "semantic_extraction__pipio_grok",
        "semantic_extraction__pipio_luna",
    ):
        profile = controlled.profiles[profile_name]
        assert profile.max_concurrency == 10
        assert profile.requests_per_minute == 0
    assert asdict(base) == original


@pytest.mark.parametrize(
    ("source_limit", "rpm", "message"),
    [
        (9, 10, "full pool borrowing"),
        (10, 9, "full stage"),
    ],
)
def test_controlled_live_config_rejects_unconfirmed_capacity(
    source_limit, rpm, message
):
    with pytest.raises(ValueError, match=message):
        build_controlled_config(
            config_manager.get_llm_config(),
            logical_profile="semantic_extraction",
            concurrency=10,
            confirmed_quota_scope="independent",
            confirmed_per_source_concurrency=source_limit,
            confirmed_provider_rpm=rpm,
            timeout_seconds=120,
        )


@pytest.mark.parametrize(
    ("quota_scope", "message"),
    [
        ("shared", "one provider resource"),
        ("unknown", "independent or shared"),
    ],
)
def test_controlled_live_config_validates_confirmed_quota_scope(
    quota_scope, message
):
    with pytest.raises(ValueError, match=message):
        build_controlled_config(
            config_manager.get_llm_config(),
            logical_profile="semantic_extraction",
            concurrency=10,
            confirmed_quota_scope=quota_scope,
            confirmed_per_source_concurrency=10,
            confirmed_provider_rpm=10,
            timeout_seconds=120,
        )


def test_staged_live_gate_stops_after_first_failed_level(monkeypatch):
    calls = []

    async def fake_run_stage(base, **kwargs):
        concurrency = kwargs["concurrency"]
        calls.append(concurrency)
        return {"concurrency": concurrency, "accepted": concurrency < 25}

    monkeypatch.setattr(benchmark, "run_stage", fake_run_stage)
    monkeypatch.setattr(
        benchmark.config_manager,
        "get_llm_config",
        lambda: object(),
    )
    args = SimpleNamespace(
        confirm_live=True,
        concurrency=[10, 25, 50],
        profile="semantic_extraction",
        confirmed_quota_scope="independent",
        confirmed_per_source_concurrency=50,
        confirmed_provider_rpm=50,
        timeout_seconds=120,
    )

    result = asyncio.run(benchmark.run_stages(args))

    assert calls == [10, 25]
    assert result["executed_stages"] == [10, 25]
    assert result["all_executed_stages_accepted"] is False


@pytest.mark.parametrize(
    ("confirm_live", "levels", "message"),
    [
        (False, [10], "--confirm-live"),
        (True, [25, 10], "unique and increasing"),
        (True, [10, 10], "unique and increasing"),
    ],
)
def test_staged_live_gate_rejects_unsafe_invocation(
    confirm_live, levels, message
):
    args = SimpleNamespace(
        confirm_live=confirm_live,
        concurrency=levels,
        profile="semantic_extraction",
        confirmed_quota_scope="independent",
        confirmed_per_source_concurrency=50,
        confirmed_provider_rpm=50,
        timeout_seconds=120,
    )

    with pytest.raises(ValueError, match=message):
        asyncio.run(benchmark.run_stages(args))
